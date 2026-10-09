"""kb-service NATS rebuild consumer (P1 #24, B6).

Consumes messages from ``ani.tasks.kb.rebuild.v1`` (routed there by the
OutboxDispatcher's ``subject_overrides`` for event_type ``kb.rebuild``)
and executes the full-KB re-parse: every ready/failed document in the
KB is reset and re-parsed sequentially.

Transport is a JetStream durable push consumer (ManualAck) on the
ANI_TASKS WorkQueue stream — at-least-once delivery, per the deploy
contract (component-contracts/nats.yaml) and the Go message_bus
Subscribe path. Ack semantics (see app/consumers/jetstream.py):
handler success → Ack, invalid JSON → Ack (poison pill swallowed; the
durable outbox row stays as the operator's source of truth), unhandled
crash → neither Ack nor Nak (silent for ack_wait redelivery — the
redelivery takes over the orphaned operation via acquire_run's
non-terminal re-acquire). While a rebuild runs, an InProgress heartbeat
(ack_wait/3) renews the delivery lease so a long rebuild is never
redelivered mid-run.

Default OFF — started only when ``settings.kb_rebuild_consumer_enabled``
is True (main.py gates startup on this flag, mirroring the parse
consumer rollout).

Design decisions (B6 plan):
  - Serial execution (Semaphore(1), D5): a rebuild is a heavy long job;
    documents are processed one at a time so one rebuild never competes
    with itself for pool connections or rag-engine capacity.
  - Per-document reset (D1): each document is reset via
    ``reset_for_reparse_in_tx`` immediately before its
    ``process_document`` call — NOT in one bulk UPDATE. Queries keep
    being served from the existing (old) chunks/vector index while the
    rebuild runs; the orchestrator's built-in re-entrant cleanup removes
    the old chunks per document.
  - The orchestrator's ready-skip (parse_status == 'ready') would defeat
    the rebuild — the reset to 'pending' happens first, so the skip
    never fires; the orchestrator itself is untouched (zero changes
    below this layer).
  - No checkpoint/resume (D5): a crashed rebuild leaves the kb_operations
    row running (or the KB stuck rebuilding); the client retries with a
    fresh idempotency key (revive semantics: a failed operation is not
    replayed on the same key, SPEC §5.4).

Payload shape (from kb-service outbox_events, RebuildKB servicer)::

    {"kb_id", "tenant_id", "task_id"}

``task_id`` is the id of the kb_operations row written by RebuildKB
(KB-SPLIT-P1B #051 — the shared async_tasks table is no longer written).

Idempotency (at-least-once): a redelivered rebuild message is gated
by ``acquire_run`` (non-terminal → running conditional UPDATE). If the
operation is still pending (first delivery crashed before marking), or
running (consumer died mid-run — the JetStream redelivery itself is the
takeover signal, since a live handler holds the delivery lease via
InProgress heartbeats), the rebuild runs; if the operation is already
terminal, the message is a duplicate and skipped entirely. Within
one run,
``list_documents_for_rebuild`` only returns ready/failed docs, so
already-rebuilt (ready) docs are re-reset and re-parsed — this is
the point of a rebuild. Documents deleted between snapshot and
processing are skipped (reset matches 0 rows / process_document
returns early).

KB status lifecycle: the consumer is the only writer of the
rebuilding → active exit transition (``set_status_in_tx``), in a
``finally`` so a crashed handler still restores the KB to active (the
kb_operations row stays failed; the KB must not stay locked forever).
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Protocol, runtime_checkable

import asyncpg

from app.consumers import jetstream
from app.core.config import settings
from app.observability import metrics
from app.observability.correlation import corr
from app.repositories import operation as operation_repo
from app.repositories import document as doc_repo
from app.repositories import knowledge_base as kb_repo

logger = logging.getLogger(__name__)

# D5: serial — one document at a time, no concurrent rebuild documents.
REBUILD_MAX_CONCURRENCY = 1

# JetStream durable identity (Go model-import-worker style constants:
# AckWait 30m — a full-KB rebuild can legitimately take longer than a
# single document parse; MaxDeliver 3 — enough redeliveries for one
# crash-restart cycle, without an endless retry loop; MaxAckPending 1 —
# the durable's delivery backpressure matches the serial handler).
REBUILD_DURABLE = "kb-rebuild-consumer"
REBUILD_ACK_WAIT = 30 * 60          # seconds
REBUILD_MAX_DELIVER = 3


@runtime_checkable
class _ParseOrchestrator(Protocol):
    """Subset of ParseOrchestrator used by the rebuild consumer."""

    async def process_document(
        self,
        *,
        tenant_id: str,
        kb_id: str,
        doc_id: str,
        storage_path: str,
        file_name: str,
        file_type: str,
        chunk_size: int,
        vector_store_id: str,
        embedding_model: str = "",
        generate_model: str = "",
        embed_runtime_endpoint: str = "",
        generate_runtime_endpoint: str = "",
    ) -> None: ...


class RebuildConsumer:
    """NATS consumer that executes full-KB rebuilds sequentially.

    Lifecycle mirrors ParseConsumer::

        consumer = RebuildConsumer(
            nats_client=nc, db_pool=pool, orchestrator=parse_orchestrator,
            subject="ani.tasks.kb.rebuild.v1",
        )
        await consumer.start()   # subscribe
        ...
        await consumer.stop()   # unsubscribe + drain in-flight
    """

    def __init__(
        self,
        *,
        nats_client: Any,
        db_pool: asyncpg.Pool,
        orchestrator: _ParseOrchestrator,
        subject: str,
        max_concurrency: int = REBUILD_MAX_CONCURRENCY,
        vector_store: Any | None = None,
        embedder: Any | None = None,
        inference_service_client: Any | None = None,
    ) -> None:
        self._nats = nats_client
        self._pool = db_pool
        self._orchestrator = orchestrator
        self._subject = subject
        self._max_concurrency = max_concurrency
        # D1 向量自管（issue-056）：注入后在 rebuild 开始时重建 collection
        # （先删后建，schema 维度随当前 embedding 模型）。None = 跳过重建，
        # 退化为逐文档向量替换（orchestrator 内置的 reparse 清理）。
        self._vector_store = vector_store
        # rag-engine Embed 客户端，用于重建前的维度探测；None 时回落
        # env EMBEDDING_DIM。
        self._embedder = embedder
        # Resolves (tenant_id, served_model_name) → runtime_endpoint so rebuild
        # parses direct-connect to the tenant's inference service. Optional:
        # when None, endpoints stay empty and rag-engine uses its default base.
        self._inference_service_client = inference_service_client
        self._subscription = None
        self._semaphore: asyncio.Semaphore | None = None
        self._pending: set[asyncio.Task] = set()
        self._stopped = True

    async def _resolve_runtime_endpoint(
        self, *, tenant_id: str, model: str
    ) -> tuple[str, str]:
        """Resolve (runtime endpoint, canonical served_model_name) for ``model``.

        Returns ("", "") when no client is configured or resolution fails; the
        rag-engine then falls back to its default base URL and the caller keeps
        the original model name.
        """
        model = (model or "").strip()
        if not model or self._inference_service_client is None:
            return "", ""
        try:
            resolved = await self._inference_service_client.resolve_endpoint(
                tenant_id=tenant_id, served_model_name=model,
            )
            return resolved.base_url, resolved.served_model_name or model
        except Exception as exc:  # noqa: BLE001 — degrade to rag-engine default
            logger.warning(
                "rebuild_consumer: failed to resolve runtime endpoint for model "
                "%r (tenant %s), falling back to rag-engine default: %s",
                model, tenant_id, exc,
            )
            return "", ""

    async def start(self) -> None:
        """Subscribe to the rebuild subject and begin consuming."""
        self._stopped = False
        self._semaphore = asyncio.Semaphore(self._max_concurrency)
        js = self._nats.jetstream()
        await jetstream.ensure_ani_tasks_stream(js)
        self._subscription = await jetstream.subscribe_durable(
            js,
            subject=self._subject,
            durable=REBUILD_DURABLE,
            cb=self._on_msg,
            ack_wait=REBUILD_ACK_WAIT,
            max_deliver=REBUILD_MAX_DELIVER,
            max_ack_pending=self._max_concurrency,
        )
        logger.info(
            "rebuild_consumer: subscribed to %s (durable=%s)",
            self._subject, REBUILD_DURABLE,
        )

    async def stop(self, timeout: float = 5.0) -> None:
        """Unsubscribe and drain in-flight rebuild tasks."""
        self._stopped = True
        if self._subscription is not None:
            try:
                await self._subscription.unsubscribe()
            except Exception:  # noqa: BLE001 — best-effort
                pass
            self._subscription = None
        if self._pending:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._pending, return_exceptions=True),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "rebuild_consumer: drain timed out, cancelling %d "
                    "lingering tasks", len(self._pending),
                )
                # cancel-then-await: cancelling only requests interruption;
                # each task is awaited so cleanup (KB restore in finally,
                # ack/nak) actually runs before stop() returns. A bare
                # cancel without await leaves the tasks orphaned on a
                # closing event loop.
                for task in self._pending:
                    task.cancel()
                await asyncio.gather(*self._pending, return_exceptions=True)
            self._pending.clear()

    async def _on_msg(self, msg: Any) -> None:
        """NATS callback: spawn a bounded task per rebuild message."""
        if self._stopped:
            return
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrency)
        task = asyncio.create_task(self._handle(msg))
        self._pending.add(task)
        task.add_done_callback(self._on_task_done)
        metrics.set_consumer_inflight("rebuild", len(self._pending))

    def _on_task_done(self, task: asyncio.Task) -> None:
        """Drop a finished task from ``_pending`` and refresh the gauge."""
        self._pending.discard(task)
        metrics.set_consumer_inflight("rebuild", len(self._pending))

    async def _handle(self, msg: Any) -> None:
        """Process one rebuild message with bounded concurrency.

        JetStream ack policy (ManualAck; see jetstream.py):
          - invalid payload → Ack: it can never succeed on redelivery
            (poison pill); the durable outbox row keeps the audit trail;
          - any normal return of ``process_message`` (including task
            close-outs with status='failed') → Ack: the operation row is
            the source of truth and a redelivery would just gate-skip;
          - unhandled crash → neither Ack nor Nak. The message goes
            silent and JetStream redelivers after ack_wait (30 min) —
            the redelivery is the orphan-takeover signal: acquire_run
            re-acquires the running row and the rebuild re-runs. A Nak
            instead would redeliver immediately and re-run the rebuild
            on a tight loop (bounded only by MaxDeliver) — hammering
            upstreams that may still be down is exactly what the silent
            ack_wait gap avoids.
        """
        async with self._semaphore:  # type: ignore[union-attr]
            # KB-SPLIT-P3C (issue-063): count JetStream redeliveries (the
            # orphan-takeover / lease-expiry signal). Metadata is best-effort.
            try:
                meta = msg.metadata
                if meta is not None and getattr(meta, "num_delivered", 1) > 1:
                    metrics.inc_consumer_retry("rebuild")
            except Exception:  # noqa: BLE001 — metadata is best-effort
                pass
            try:
                payload = json.loads(msg.data.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError(
                        f"payload is {type(payload).__name__}, expected object"
                    )
            except Exception as exc:  # noqa: BLE001
                logger.error("rebuild_consumer: invalid message payload: %s", exc)
                await jetstream.ack(msg)
                return
            # Renew the JetStream delivery lease while the handler runs
            # (in_progress every ack_wait/3, mirroring the Go heartbeat
            # goroutine) so a long rebuild is never redelivered mid-run.
            heartbeat = asyncio.create_task(
                jetstream.heartbeat_loop(msg, REBUILD_ACK_WAIT / 3)
            )
            try:
                await self.process_message(payload)
                await jetstream.ack(msg)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception(
                    "rebuild_consumer: unhandled crash; leaving message "
                    "unacked for ack_wait redelivery (orphan takeover "
                    "via expired lease)"
                )
            finally:
                # Stop renewing the delivery lease before the outcome is
                # final; on crash paths this is what lets JetStream
                # schedule the redelivery at all.
                heartbeat.cancel()
                try:
                    await heartbeat
                except asyncio.CancelledError:
                    pass

    async def process_message(self, payload: dict[str, Any]) -> None:
        """Run the full-KB rebuild for one task payload.

        Payload shape (from the RebuildKB outbox event)::

            {"kb_id", "tenant_id", "task_id"}

        Task lifecycle: acquire_run on pickup → complete_operation
        (completed | failed) with the outcome. Per-document progress
        writes died with the shared async_tasks table (ADR 0001 §D4) —
        task progress is visible externally only through outbox events.
        Terminal semantics (B6 plan):
          - all documents failed → status='failed' (client retries with a
            fresh idempotency key);
          - any success → status='completed' with failed_doc_ids in the
            result (partial failure is visible, not silent).
        """
        kb_id = payload.get("kb_id", "")
        tenant_id = payload.get("tenant_id", "")
        task_id = payload.get("task_id") or ""
        # KB-SPLIT-P3C (issue-063): correlation suffix for every log line in
        # this rebuild run (kb-level; per-doc lines extend it with doc_id).
        corr_ctx = corr(kb_id=kb_id, run_id=task_id)
        if not kb_id or not tenant_id:
            logger.error(
                "rebuild_consumer: missing required fields (kb_id=%s tenant_id=%s) %s",
                kb_id, tenant_id, corr_ctx,
            )
            return

        if not task_id:
            # Outbox event published without a task ref (shouldn't happen
            # — RebuildKB always creates the row first). Process anyway;
            # only progress/close-out are skipped.
            logger.warning(
                "rebuild_consumer: no task_id in payload for kb %s; "
                "running rebuild without task tracking %s", kb_id, corr_ctx,
            )

        # 1. mark the operation running (no-op without a task_id). The
        #    conditional UPDATE (non-terminal → running) is the idempotency
        #    gate for at-least-once redelivery: a duplicate delivery finds
        #    the operation already completed/failed and returns False —
        #    skip BEFORE entering the try/finally so we neither re-run
        #    the rebuild nor touch the KB status transition. A 'running'
        #    row re-acquires: the redelivery proves the previous consumer
        #    died (a live handler holds the delivery lease via heartbeats).
        if task_id:
            try:
                async with self._pool.acquire() as conn:
                    marked = await operation_repo.acquire_run(
                        conn, tenant_id=tenant_id, operation_id=task_id
                    )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "rebuild_consumer: acquire_run failed for operation %s: %s",
                    task_id, exc,
                )
            else:
                if not marked:
                    logger.warning(
                        "rebuild_consumer: operation %s for kb %s is already "
                        "terminal (duplicate delivery or already processed); "
                        "skipping rebuild %s", task_id, kb_id, corr_ctx,
                    )
                    return

        kb_restored = False
        failed_doc_ids: list[str] = []
        succeeded = 0
        try:
            # 2. resolve KB metadata (vector store / embedding model /
            #    chunk_size drive every document parse).
            async with self._pool.acquire() as conn:
                kb_row = await kb_repo.get_kb(
                    conn, tenant_id=tenant_id, kb_id=kb_id
                )
            if not kb_row:
                logger.warning(
                    "rebuild_consumer: kb %s not found, skipping rebuild %s",
                    kb_id, corr_ctx,
                )
                if task_id:
                    await self._complete(
                        tenant_id, task_id, status="failed",
                        result={"error": "knowledge base not found"},
                        failed_doc_ids=[],
                    )
                return
            vector_store_id = str(kb_row.get("vector_store_id") or "")
            embedding_model = str(kb_row.get("embedding_model") or "")
            generate_model = str(kb_row.get("default_inference_service") or "")
            chunk_size = kb_row.get("chunk_size")
            if chunk_size is None:
                chunk_size = 1024  # NOT NULL column; defensive fallback
            if not vector_store_id:
                logger.error(
                    "rebuild_consumer: kb %s has no vector_store_id, "
                    "skipping rebuild %s", kb_id, corr_ctx,
                )
                if task_id:
                    await self._complete(
                        tenant_id, task_id, status="failed",
                        result={"error": "kb has no vector_store_id"},
                        failed_doc_ids=[],
                    )
                return

            # 2b. D1 向量自管（issue-056）：rebuild 重建 collection — 先删后建，
            #     schema 维度随当前 embedding 模型探测（失败回落 env
            #     EMBEDDING_DIM），随后逐文档重新灌入。重建失败直接终止本次
            #     rebuild（任务 failed，KB 状态经 finally 恢复 active）；不
            #     继续解析，避免旧向量被删后文档半新半旧。
            if self._vector_store is not None:
                dim = 0
                try:
                    if self._embedder is not None:
                        _, dim = await self._embedder.embed(
                            texts=["dimension probe"], model=embedding_model
                        )
                except Exception as exc:  # noqa: BLE001 — 探测仅降级
                    logger.warning(
                        "rebuild_consumer: embedding dimension probe failed "
                        "(kb=%s model=%s), falling back to KB_EMBEDDING_DIM=%s: %s",
                        kb_id, embedding_model, settings.embedding_dim, exc,
                    )
                if dim <= 0:
                    dim = settings.embedding_dim
                try:
                    await self._vector_store.recreate_collection(
                        name=vector_store_id, dim=dim
                    )
                    logger.info(
                        "rebuild_consumer: recreated milvus collection %s "
                        "(dim=%d) for kb %s %s",
                        vector_store_id, dim, kb_id, corr_ctx,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "rebuild_consumer: milvus collection recreate failed "
                        "for kb %s: %s %s", kb_id, exc, corr_ctx,
                    )
                    if task_id:
                        await self._complete(
                            tenant_id, task_id, status="failed",
                            result={"error": f"milvus collection recreate failed: {exc}"},
                            failed_doc_ids=[],
                        )
                    return

            # Resolve the tenant's inference-service endpoints per model:
            # embedding uses embedding_model, summary generation uses the KB's
            # chat model. The canonical served_model_name replaces the config
            # name for the OpenAI request body.
            embed_runtime_endpoint, resolved_embed_model = (
                await self._resolve_runtime_endpoint(
                    tenant_id=tenant_id, model=embedding_model,
                )
            )
            generate_runtime_endpoint, resolved_generate_model = (
                await self._resolve_runtime_endpoint(
                    tenant_id=tenant_id, model=generate_model,
                )
            )

            # 3. snapshot eligible doc ids (ready/failed, soft-deleted
            #    excluded). Later deletions are skipped per-doc below.
            async with self._pool.acquire() as conn:
                doc_ids = await doc_repo.list_documents_for_rebuild(
                    conn, tenant_id=tenant_id, kb_id=kb_id
                )
            total = len(doc_ids)
            logger.info(
                "rebuild_consumer: rebuilding kb %s (%d documents) %s",
                kb_id, total, corr_ctx,
            )

            # 4. sequential per-document rebuild (D1: reset then parse —
            #    the reset makes the orchestrator's ready-skip inert).
            for idx, doc_id in enumerate(doc_ids, start=1):
                doc_ctx = corr(kb_id=kb_id, doc_id=doc_id, run_id=task_id)
                async with self._pool.acquire() as conn:
                    doc_row = await doc_repo.get_document(
                        conn, tenant_id=tenant_id, kb_id=kb_id, doc_id=doc_id,
                    )
                if not doc_row:
                    # Deleted between snapshot and processing — skip.
                    logger.info(
                        "rebuild_consumer: doc %s vanished, skipping %s",
                        doc_id, doc_ctx,
                    )
                    continue
                # Re-check eligibility: the doc may have been deleted
                # (soft-deleted marker) or gone mid-pipeline after the
                # snapshot; both are out of rebuild scope.
                status = doc_row.get("parse_status")
                if status not in ("ready", "failed"):
                    logger.info(
                        "rebuild_consumer: doc %s parse_status=%s (mid-pipeline?), "
                        "skipping %s", doc_id, status, doc_ctx,
                    )
                    continue

                # Reset (own transaction) then parse. The reset clears the
                # ready state so process_document's skip never fires.
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        reset = await doc_repo.reset_for_reparse_in_tx(
                            conn, tenant_id=tenant_id, kb_id=kb_id, doc_id=doc_id,
                        )
                if not reset:
                    logger.info(
                        "rebuild_consumer: doc %s reset matched 0 rows, "
                        "skipping %s", doc_id, doc_ctx,
                    )
                    continue

                try:
                    await self._orchestrator.process_document(
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        doc_id=doc_id,
                        storage_path=str(doc_row.get("storage_path") or ""),
                        file_name=str(doc_row.get("file_name") or ""),
                        file_type=str(doc_row.get("file_type") or ""),
                        chunk_size=int(chunk_size),
                        vector_store_id=vector_store_id,
                        embedding_model=resolved_embed_model or embedding_model,
                        generate_model=resolved_generate_model or generate_model,
                        embed_runtime_endpoint=embed_runtime_endpoint,
                        generate_runtime_endpoint=generate_runtime_endpoint,
                        run_id=task_id,
                    )
                    succeeded += 1
                except Exception as exc:  # noqa: BLE001 — per-doc isolation
                    # The orchestrator swallows its own errors (writes
                    # failed); this catches transport-level failures only.
                    logger.exception(
                        "rebuild_consumer: rebuild parse failed for doc %s: %s %s",
                        doc_id, exc, doc_ctx,
                    )
                    failed_doc_ids.append(doc_id)
        finally:
            # 6. restore the KB to active — MUST run even on crash paths,
            #    otherwise the KB stays locked (rebuilding) forever.
            #    Wrapped in a transaction: set_status_in_tx is an in_tx
            #    repo function (does NOT open its own transaction), so the
            #    caller must supply the tx block it participates in (#054:
            #    the old "RLS context needs a tx block" rationale is gone —
            #    set_tenant_context is a pure validator now; the tx remains
            #    the right shape for an in_tx function).
            try:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        restored = await kb_repo.set_status_in_tx(
                            conn,
                            tenant_id=tenant_id,
                            kb_id=kb_id,
                            from_status="rebuilding",
                            to_status="active",
                        )
                kb_restored = restored
                if not restored:
                    logger.warning(
                        "rebuild_consumer: kb %s was not in 'rebuilding' "
                        "on exit (deleted or already active); no restore "
                        "needed %s", kb_id, corr_ctx,
                    )
            except Exception:  # noqa: BLE001 — best-effort restore
                logger.exception(
                    "rebuild_consumer: failed to restore kb %s to active; "
                    "it stays rebuilding (operator intervention needed) %s",
                    kb_id, corr_ctx,
                )

        # 7. terminal task state (after the finally-side KB restore so the
        #    KB is active by the time the client polls the task).
        if task_id:
            status = "failed" if (failed_doc_ids and succeeded == 0) else "completed"
            await self._complete(
                tenant_id, task_id, status=status,
                result={
                    "total": len(failed_doc_ids) + succeeded,
                    "succeeded": succeeded,
                    "failed_doc_ids": failed_doc_ids,
                },
                failed_doc_ids=failed_doc_ids,
            )
        logger.info(
            "rebuild_consumer: kb %s rebuild done (succeeded=%d failed=%d "
            "kb_restored=%s) %s",
            kb_id, succeeded, len(failed_doc_ids), kb_restored, corr_ctx,
        )

    async def _complete(
        self,
        tenant_id: str,
        task_id: str,
        *,
        status: str,
        result: dict[str, Any],
        failed_doc_ids: list[str],
    ) -> None:
        """Close the kb_operations row with the rebuild outcome (best-effort)."""
        try:
            async with self._pool.acquire() as conn:
                await operation_repo.complete_operation(
                    conn,
                    tenant_id=tenant_id,
                    operation_id=task_id,
                    result=result,
                    status=status,
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "rebuild_consumer: failed to close task %s (%d failed docs): %s %s",
                task_id, len(failed_doc_ids), exc, corr(run_id=task_id),
            )


def build_rebuild_consumer(
    *,
    nats_client: Any,
    db_pool: asyncpg.Pool,
    orchestrator: _ParseOrchestrator,
    subject: str,
    max_concurrency: int = REBUILD_MAX_CONCURRENCY,
    vector_store: Any | None = None,
    embedder: Any | None = None,
    inference_service_client: Any | None = None,
) -> RebuildConsumer:
    """Factory for constructing a RebuildConsumer (called from main.py).

    Kept as a separate function so main.py can conditionally build the
    consumer only when ``settings.kb_rebuild_consumer_enabled`` is True.
    """
    return RebuildConsumer(
        nats_client=nats_client,
        db_pool=db_pool,
        orchestrator=orchestrator,
        subject=subject,
        max_concurrency=max_concurrency,
        vector_store=vector_store,
        embedder=embedder,
        inference_service_client=inference_service_client,
    )
