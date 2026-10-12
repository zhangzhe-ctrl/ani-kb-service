"""KBService gRPC servicer (SPEC §2.4, §4.1, §6.1).

US-009 wires the repositories + rag-engine client into the 10 P0 RPCs.
CreateKB / DeleteKB / DeleteDocument 自管向量：直连独立 Milvus
（app/vector_store/，KB-SPLIT-P1G issue-056, ADR 0001 §D1），不再调用
Core vector-stores API。上传链路为预签名直传 + CompleteDocumentUpload
（KB-SPLIT-P1I issue-058, ADR 0001 §D1）；Query 负责 kb_messages 持久化
+ Redis 会话缓存。
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import queue
import threading
import uuid
from datetime import datetime
from typing import Any

import asyncpg
import grpc
from google.protobuf import empty_pb2
from google.protobuf import timestamp_pb2

from app.generated.common.v1 import common_pb2
from app.generated.kb.v1 import kb_service_pb2 as kb_pb
from app.generated.kb.v1 import kb_service_pb2_grpc as pb_grpc
from app.observability import metrics
from app.observability.correlation import corr
from app.rag_engine.client import RagEngineError
from app.repositories import audit as audit_repo
from app.repositories import chunk as chunk_repo
from app.repositories import document as document_repo
from app.repositories import knowledge_base as kb_repo
from app.repositories import message as message_repo
from app.repositories import operation as operation_repo
from app.repositories import permission as permission_repo
from app.repositories.cursor import InvalidCursorError
from app.core.config import settings
from app.object_store import (
    MinioObjectStore,
    ObjectNotFoundError,
    ObjectStoreError,
    get_default_object_store,
)
from app.services.contracts import QueryResult
from app.vector_store import MilvusVectorStore, VectorStoreError, get_default_vector_store

logger = logging.getLogger(__name__)


# ── async bridge for gRPC ThreadPoolExecutor worker threads ──────────────────
# gRPC servicer methods run in a ThreadPoolExecutor worker thread. asyncpg
# connections are bound to the event loop they were created on, so sharing
# a pool created on the uvicorn loop across gRPC threads causes
# "Future attached to a different loop" errors.
#
# Fix: a single dedicated event loop runs on a background thread. The pool,
# Redis client, and all async work live on that loop. gRPC worker threads
# submit coroutines via run_coroutine_threadsafe and block on the result.
_grpc_loop: asyncio.AbstractEventLoop | None = None
_grpc_loop_thread: threading.Thread | None = None


def _start_grpc_loop():
    """Start the dedicated gRPC event loop on a background thread."""
    global _grpc_loop, _grpc_loop_thread
    if _grpc_loop is not None:
        return
    ready = threading.Event()

    def _run_loop():
        global _grpc_loop
        _grpc_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_grpc_loop)
        ready.set()
        _grpc_loop.run_forever()

    _grpc_loop_thread = threading.Thread(target=_run_loop, daemon=True, name="grpc-async-loop")
    _grpc_loop_thread.start()
    ready.wait(timeout=5)


def _run_async(coro):
    """Submit a coroutine to the dedicated gRPC event loop and block on it.

    The loop is shared across all gRPC worker threads so asyncpg connections
    from the pool always run on the same loop they were created on.
    """
    if _grpc_loop is None:
        _start_grpc_loop()
    future = asyncio.run_coroutine_threadsafe(coro, _grpc_loop)
    return future.result()


def _run_async_bg(coro):
    """Submit a coroutine to the gRPC loop without blocking (fire-and-forget).

    Used by _default_session_cache which opens async resources that
    need to live on the gRPC loop.
    """
    if _grpc_loop is None:
        _start_grpc_loop()
    return asyncio.run_coroutine_threadsafe(coro, _grpc_loop)


def _ts(dt: datetime | str | None) -> timestamp_pb2.Timestamp:
    """Convert a datetime to a protobuf Timestamp.

    Also accepts ISO strings: idempotency-replay results are stored in
    kb_operations.result (JSONB), where datetimes come back as strings.
    """
    ts = timestamp_pb2.Timestamp()
    if dt is None:
        return ts
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    ts.FromDatetime(dt)
    return ts


def _vector_store_name(kb_id: str) -> str:
    """Derive the Milvus collection name from a kb_id (ADR §一-1.2).

    D1 向量自管（issue-056）：同名即 kb 自己的 Milvus collection 名，
    CreateKB 以此落库 vector_store_id，不经 Core vector-stores 登记。
    """
    return f"kb_{kb_id.replace('-', '')}"


# Upload size ceiling for the kb-owned bucket (CompleteDocumentUpload gate).
# SPEC §6.1 DOC_TOO_LARGE / UX「文件大小超限（上限 100MB）」— the console
# pre-validates but a direct PUT bypasses it, so the business layer enforces
# the same 100MB bound before registering the row.
MAX_UPLOAD_BYTES = 100 * 1024 * 1024

# Allowed values for the ListKBs status filter (ListKBsRequest.status).
# 'deleted' rows are always excluded from listing regardless of the filter.
_KB_LIST_STATUSES = frozenset({"active", "rebuilding"})


class KBServiceServicer(pb_grpc.KBServiceServicer):
    """KBService servicer: 10 P0 RPCs wired (US-009) + 3 P1 UNIMPLEMENTED."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool | None = None,
        session_cache_factory: Any | None = None,
        retrieve_service_factory: Any | None = None,
        rag_engine_grpc_client_factory: Any | None = None,
        vector_store: Any | None = None,
        object_store: Any | None = None,
        inference_service_client_factory: Any | None = None,
    ) -> None:
        # When pool is None the servicer still serves RPCs that don't need DB
        # (used by the skeleton tests in test_grpc_server.py). DB-backed RPCs
        # abort with FAILED_PRECONDITION when pool is unset.
        self._pool = pool
        # session_cache_factory() -> SessionCache | None; injected for testing.
        # Returns None when Redis is unavailable (Query degrades to DB-only).
        self._session_cache_factory = session_cache_factory or _default_session_cache
        # retrieve_service_factory(tenant_id) -> RetrieveService; injected for
        # testing. In production, constructed from settings in main.py.
        self._retrieve_service_factory = retrieve_service_factory
        # rag_engine_grpc_client_factory() -> RagEngineGRPCClient; injected for
        # testing. In production, constructed from settings in main.py.
        self._rag_engine_grpc_client_factory = rag_engine_grpc_client_factory
        # D1 向量自管（issue-056）：直连独立 Milvus 的 vector store 客户端。
        # 生产为进程级单例（懒连接）；注入点供测试使用。
        self._vector_store: MilvusVectorStore = (
            vector_store if vector_store is not None else get_default_vector_store()
        )
        # D1 对象自管（issue-058）：kb 自有 MinIO bucket 的对象存储客户端
        # （预签名 PUT 签发 + HeadObject 校验）。生产为进程级单例（懒连接）；
        # 注入点供测试使用。
        self._object_store: MinioObjectStore = (
            object_store if object_store is not None else get_default_object_store()
        )
        # inference_service_client_factory() -> InferenceServiceGRPCClient;
        # injected for testing. In production, constructed from settings in
        # main.py. Used to resolve (tenant_id, served_model_name) → the
        # published inference service's runtime_endpoint, which is then passed
        # through to rag-engine (Embed/Generate) so the stateless RPCs connect
        # directly to the tenant's own service.
        self._inference_service_client_factory = inference_service_client_factory
        # Per-tenant orchestrator cache. Each tenant gets its own
        # QueryOrchestrator instance backed by a tenant-scoped
        # RetrieveService. This preserves multi-tenant isolation — a single
        # global orchestrator would cross tenant boundaries.
        self._orchestrators: dict[str, Any] = {}

    async def _resolve_runtime_endpoint(
        self, *, tenant_id: str, model: str
    ) -> tuple[str, str]:
        """Resolve a published model to (runtime endpoint, canonical name).

        The selected model IS the tenant boundary (``inference_services`` has a
        per-tenant uniqueness on ``served_model_name``), so resolving
        ``(tenant_id, served_model_name)`` via the internal
        ``InferenceEndpointResolver/ResolveInternalEndpoint`` RPC yields
        exactly one service. The returned ``base_url`` is passed through to
        rag-engine (Embed/Generate) so the stateless RPCs connect directly to
        the tenant's own service — never through the AI Gateway — and the
        canonical ``served_model_name`` MUST be used as the ``model`` in the
        OpenAI request body.

        Degrades to ("", "") (rag-engine then uses its server-default endpoint
        and the caller keeps the original model name) when the model is empty,
        no client is wired, or resolution fails. This BEST-EFFORT behaviour is
        only for the non-Q&A chains: the CreateKB dimension probe (which must
        not block KB creation) and the background document consumers. The
        Q&A path (Query/Retrieve) instead uses
        ``_resolve_required_runtime_endpoint`` which never silently falls back.
        A failure is logged at warning level with model + tenant so it is not
        silent. ``base_url`` is a runtime snapshot: it is re-resolved on every
        call, never cached.
        """
        model = (model or "").strip()
        if not model or self._inference_service_client_factory is None:
            return "", ""
        try:
            client = self._inference_service_client_factory()
            resolved = await client.resolve_endpoint(
                tenant_id=tenant_id, served_model_name=model
            )
            return resolved.base_url, resolved.served_model_name or model
        except Exception as exc:  # noqa: BLE001 — degrade to rag-engine default
            logger.warning(
                "kb-service: failed to resolve runtime endpoint for model %r "
                "(tenant %s), falling back to rag-engine default: %s",
                model, tenant_id, exc,
            )
            return "", ""

    async def _resolve_required_runtime_endpoint(
        self, *, tenant_id: str, model: str, role: str, context
    ) -> tuple[str, str]:
        """Resolve a Q&A model to (runtime endpoint, canonical name) or abort.

        Unlike the best-effort ``_resolve_runtime_endpoint``, the Q&A path
        (Query/Retrieve) must NEVER silently fall back to a server-default
        endpoint — the selected model IS the tenant boundary, and hitting an
        empty endpoint produces an opaque rag-engine failure. This variant
        therefore:

        * aborts ``INVALID_ARGUMENT`` when no model is configured (neither the
          request nor the KB ``default_inference_service`` supplied one) —
          三级回落链 request → KB 默认 → 报错;
        * aborts ``FAILED_PRECONDITION`` when the model cannot be resolved to a
          running inference service (unpublished / not found / endpoint error).

        ``role`` ("chat" / "embedding") labels the failing leg in messages.
        When no resolver factory is wired (skeleton/test wiring) there is
        nothing to enforce, so this degrades to the best-effort behaviour.
        """
        model = (model or "").strip()
        if self._inference_service_client_factory is None:
            return await self._resolve_runtime_endpoint(
                tenant_id=tenant_id, model=model
            )
        if not model:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"no {role} model configured: pass inference_service_name in "
                f"the request or set the knowledge base default inference "
                f"service",
            )
            return "", ""  # unreachable; for type checkers
        try:
            client = self._inference_service_client_factory()
            resolved = await client.resolve_endpoint(
                tenant_id=tenant_id, served_model_name=model
            )
            return resolved.base_url, resolved.served_model_name or model
        except Exception as exc:  # noqa: BLE001 — surface, never degrade
            logger.warning(
                "kb-service: failed to resolve %s runtime endpoint for model "
                "%r (tenant %s): %s",
                role, model, tenant_id, exc,
            )
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"failed to resolve {role} model {model!r}: {exc}",
            )
            return "", ""  # unreachable; for type checkers

    # ── 10 P0 RPCs ───────────────────────────────────────────────────────────

    def CreateKB(self, request, context):
        """CreateKB: idempotent KB + Core vector-store (SPEC §6.1)."""
        return _run_async(self._create_kb(request, context))

    async def _create_kb(self, request, context) -> kb_pb.KnowledgeBase:
        # 1. validate idempotency_key
        idem = getattr(request, "idempotency_key", "") or ""
        # Note: proto3 CreateKBRequest has no idempotency_key field; it lives
        # on the kb_operations side. We generate a deterministic one from the
        # tenant+name if absent so retries are safe.
        tenant_id = request.tenant_id or ""
        if not tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
        # Tenant existence check is application-level only (KB-SPLIT-P1D
        # #053, ADR §五-阶段1-4): the cross-library FK to tenants(id) is
        # gone from the kb baseline DDL (migrations/011), so no database-level
        # tenant validation exists. kb trusts the authenticated tenant
        # context injected by the Gateway (D2 boundary convergence) — the
        # format check below is the only guard kb itself performs.
        try:
            uuid.UUID(tenant_id)
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "tenant_id must be a valid UUID",
            )
            return
        if not request.name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "name is required")

        # 0. validate KB config ranges (SPEC §6.1 / openapi v1.yaml)
        cs = request.chunk_size or 1024
        if cs < 1 or cs > 8192:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"chunk_size must be in [1, 8192], got {request.chunk_size}",
            )
        tk = request.top_k or 5
        if tk < 1 or tk > 20:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"top_k must be in [1, 20], got {request.top_k}",
            )
        st = request.score_threshold
        if st < 0 or st > 1:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"score_threshold must be in [0, 1], got {request.score_threshold}",
            )

        if self._pool is None:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "DB pool not configured (skeleton mode)",
            )
            return  # unreachable; for type checkers

        idem_key = idem or f"create_kb:{tenant_id}:{request.name}"

        # 2. idempotency replay: return existing result
        async with self._pool.acquire() as conn:
            existing = await operation_repo.find_by_idempotency_key(
                conn,
                tenant_id=tenant_id,
                idempotency_key=idem_key,
                operation_type="kb.create",
            )
            if existing and existing.get("result"):
                # result is JSONB; asyncpg may return it as a str or already-
                # parsed dict depending on the codec config. Normalize.
                result = existing["result"]
                if isinstance(result, str):
                    result = json.loads(result)
                # Replay only while the recorded KB is still alive. The
                # name-fallback key above is shared by every same-name
                # create in this tenant, so a soft-deleted KB's record
                # must not shadow the re-create: get_kb hides deleted rows
                # (None), and falling through to the INSERT path builds
                # the new KB instead of returning a stale snapshot
                # (bug: delete → same-name re-create returned the old id
                # and the new KB never showed up in the list).
                replay_id = str(result.get("id") or "")
                replay_kb = (
                    await kb_repo.get_kb(
                        conn, tenant_id=tenant_id, kb_id=replay_id
                    )
                    if replay_id
                    else None
                )
                if replay_kb is not None:
                    return _kb_row_to_pb(result)
                # deleted/unknown KB → skip replay, re-create below

            # 3. INSERT knowledge_bases
            # Name-collision 23505 → ALREADY_EXISTS. Two paths lead here:
            # (a) another active KB in this tenant already uses the name;
            # (b) a prior attempt committed the kb INSERT but crashed before
            #     the kb_operations record was written, so the replay check
            #     above found nothing and this retry re-inserts. Both must
            #     surface as a clean 409, not UNKNOWN.
            #     (Distinct from the kb_operations poison-key 23505 handled in
            #     step 5 below, which self-heals by reusing the row.)
            # No SAVEPOINT needed: create_kb runs in its own transaction, so
            # the connection is clean when the exception is caught.
            # embedding_model 解析：优先请求体显式值，缺省时回落 env 的
            # KB_EMBEDDING_MODEL（SiliconFlow 只认全名前缀，如
            # "BAAI/bge-m3"）。两者都为空 = 未配置所选向量模型 → 拒绝
            # 建库（INVALID_ARGUMENT），不再静默使用默认模型。只求值一次，
            # DB 行与向量库探测共用同一变量，防止两处表达式的单边漂移。
            embedding_model = (
                request.embedding_model or settings.embedding_model
            )
            if not embedding_model:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "embedding_model is required: provide it in the request "
                    "or set KB_EMBEDDING_MODEL",
                )
                return  # unreachable; for type checkers
            try:
                async with conn.transaction():
                    kb_row = await kb_repo.create_kb(
                        conn,
                        tenant_id=tenant_id,
                        name=request.name,
                        description=request.description,
                        embedding_model=embedding_model,
                        chunk_size=request.chunk_size or 1024,
                        top_k=request.top_k or 5,
                        # 未显式传入时落库存 0（表示未设置；运行时由 rag-engine 的
                        # DEFAULT_SCORE_THRESHOLD 兜底），而不是硬编码 0.3。
                        score_threshold=request.score_threshold or 0.0,
                        retrieval_mode=request.retrieval_mode or "hybrid",
                        # 建库时选定的默认推理模型；空串落库为 NULL（未设置），
                        # 问答时由三级回落链解析（request → KB 行 → ""，
                        # rag-engine 端由 settings.vllm_model 接管）。
                        default_inference_service=request.default_inference_service or "",
                    )
                    # audit kb.create (plan §6.3) — same transaction as the kb
                    # INSERT, so the audit row exists iff the KB does.
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=str(kb_row["id"]),
                        action="kb.create",
                        actor_user_id=_actor_user_id(context),
                        before_state=None,
                        after_state=_kb_audit_snapshot(kb_row),
                    )
            except asyncpg.UniqueViolationError:
                # UNIQUE(tenant_id, name) hit — mirrors the mapping in
                # _update_kb (Gateway maps to HTTP 409). No failure audit:
                # the kb INSERT failed, so no knowledge_bases row exists
                # to satisfy kb_audit_log.kb_id (NOT NULL FK) — a create-
                # failure audit row is structurally impossible (deviation
                # logged in the P1 plan, B8 §6.3).
                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS,
                    "knowledge base name already exists in this tenant",
                )
                return  # unreachable; for type checkers
            kb_id = str(kb_row["id"])

        # 4. D1 向量自管（issue-056）：直连独立 Milvus 建 collection，
        # 不再调用 Core POST /vector-stores（下线知会在 #065）。
        # collection 名沿用 vector_store_name = "kb_{kb_id}" 派生，并以同名
        # 字符串作为 vector_store_id 落库——下游 parse/rebuild/retrieve 消费者
        # 把 vector_store_id 当 collection 标识使用，语义平移、调用面零改动。
        # 维度探测：用选定的 embedding 模型实测一条短文本，取返回的
        # dimension 作为 collection 维度——collection 随模型自动切换，
        # 不再固定读 env 的 EMBEDDING_DIM。探测失败（网络异常/模型名
        # 无效等）不阻断建库：回落 env 值并记 warning。
        probe_dim = 0
        try:
            if self._rag_engine_grpc_client_factory is not None:
                rag_engine_grpc = self._rag_engine_grpc_client_factory()
            else:
                rag_engine_grpc = _default_rag_engine_grpc_client()
            # Resolve the embedding service endpoint from the selected
            # embedding model (model = tenant boundary) so the probe connects
            # directly to the tenant's own service. The canonical
            # served_model_name is used as the request-body model.
            probe_endpoint, probe_model = await self._resolve_runtime_endpoint(
                tenant_id=tenant_id, model=embedding_model
            )
            _, probe_dim = await rag_engine_grpc.embed(
                texts=["dimension probe"], model=probe_model or embedding_model,
                runtime_endpoint=probe_endpoint,
            )
        except Exception as exc:  # noqa: BLE001 — 探测仅降级，不阻断建库
            logger.warning(
                "embedding dimension probe failed (model=%s), "
                "falling back to KB_EMBEDDING_DIM=%s: %s",
                embedding_model,
                settings.embedding_dim,
                exc,
            )
        dimension = probe_dim or settings.embedding_dim
        collection_name = _vector_store_name(kb_id)
        try:
            await self._vector_store.create_collection(
                name=collection_name, dim=dimension, metric="COSINE"
            )
            async with self._pool.acquire() as conn:
                await kb_repo.set_vector_store_id(
                    conn,
                    tenant_id=tenant_id,
                    kb_id=kb_id,
                    vector_store_id=collection_name,
                )
        except VectorStoreError as e:
            # Best-effort cleanup: soft-delete the KB row so retries can
            # re-create. We don't abort here on cleanup failure.
            async with self._pool.acquire() as conn:
                await kb_repo.soft_delete_kb(conn, tenant_id=tenant_id, kb_id=kb_id)
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"Milvus collection creation failed: {e}",
            )
            return  # unreachable

        # 5. write kb_operations(idempotency_key, result=kb) for replay
        # Poison-key self-heal: a prior attempt that crashed between create
        # and complete leaves a pending row with result=NULL; the replay
        # check above skips it, so this INSERT hits the UNIQUE constraint.
        # Reuse that pending row and complete it (self-heal).
        async with self._pool.acquire() as conn:
            try:
                task_row = await operation_repo.create_operation(
                    conn,
                    tenant_id=tenant_id,
                    idempotency_key=idem_key,
                    operation_type="kb.create",
                    resource_type="knowledge_base",
                    resource_id=kb_id,
                    payload={"kb_id": kb_id, "name": request.name},
                    status="pending",
                )
            except asyncpg.UniqueViolationError:
                existing = await operation_repo.find_by_idempotency_key(
                    conn,
                    tenant_id=tenant_id,
                    idempotency_key=idem_key,
                    operation_type="kb.create",
                )
                if existing is None:
                    # Row vanished between INSERT and SELECT; surface it.
                    raise
                task_row = existing
            await operation_repo.complete_operation(
                conn,
                tenant_id=tenant_id,
                operation_id=str(task_row["id"]),
                result=kb_row,
            )

        return _kb_row_to_pb(kb_row)

    def GetKB(self, request, context):
        return _run_async(self._get_kb(request, context))

    async def _get_kb(self, request, context) -> kb_pb.KnowledgeBase:
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        async with self._pool.acquire() as conn:
            row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
        if not row:
            context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
            return
        return _kb_row_to_pb(row)

    def ListKBs(self, request, context):
        return _run_async(self._list_kbs(request, context))

    async def _list_kbs(self, request, context) -> kb_pb.ListKBsResponse:
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        status = (request.status or "").strip()
        if status and status not in _KB_LIST_STATUSES:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "status must be one of: " + ", ".join(sorted(_KB_LIST_STATUSES)),
            )
            return
        limit = request.page.limit or 20
        cursor = request.page.cursor or None
        async with self._pool.acquire() as conn:
            rows, total = await kb_repo.list_kbs(
                conn, tenant_id=request.tenant_id, limit=limit, cursor=cursor,
                status=status,
            )
        kbs = [_kb_row_to_pb(r) for r in rows]
        next_cursor = str(rows[-1]["id"]) if rows and len(rows) >= limit else ""
        return kb_pb.ListKBsResponse(
            kbs=kbs,
            meta=common_pb2.CursorPageMeta(total=total, next_cursor=next_cursor),
        )

    def UpdateKB(self, request, context):
        return _run_async(self._update_kb(request, context))

    async def _update_kb(self, request, context) -> kb_pb.KnowledgeBase:
        # 1. validate idempotency_key / kb_id / tenant_id (SPEC §5.1)
        if not request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required"
            )
            return
        try:
            uuid.UUID(request.idempotency_key.strip())
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key must be a uuid"
            )
            return
        if not request.kb_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "kb_id is required")
            return
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return

        tenant_id = request.tenant_id
        idem_key = request.idempotency_key

        # 2-4. single transaction: idempotency replay check, UPDATE, and the
        # kb_operations idempotency record (insert + complete) all commit
        # atomically — no partial failure can leave a poison pending row
        # behind (same pattern as CompleteDocumentUpload, SPEC §6.1 US-010).
        # find_by_idempotency_key / update_kb each open conn.transaction(),
        # which inside this outer transaction degrades to a SAVEPOINT
        # (asyncpg nested-tx semantics); create_operation_in_tx /
        # complete_operation_in_tx are plain. Nothing commits until the outer
        # transaction commits. Audit (kb.update) writes in the same tx.
        actor = _actor_user_id(context)
        intent = {"name": request.name, "description": request.description}
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # 2. idempotency replay: return the recorded result row
                    existing = await operation_repo.find_by_idempotency_key(
                        conn,
                        tenant_id=tenant_id,
                        idempotency_key=idem_key,
                        operation_type="kb.update",
                    )
                    if existing and existing.get("result"):
                        result = existing["result"]
                        if isinstance(result, str):
                            result = json.loads(result)
                        return _kb_row_to_pb(result)

                    # audit before_state: the row as it stands pre-UPDATE
                    # (None when the tenant filter hides the KB → the 404 abort below).
                    before_row = await kb_repo.get_kb(
                        conn, tenant_id=tenant_id, kb_id=request.kb_id
                    )
                    if before_row and before_row.get("status") == "rebuilding":
                        # B6 (#24): write ops are mutually exclusive with a
                        # running full-KB rebuild (queries stay served).
                        context.abort(
                            grpc.StatusCode.FAILED_PRECONDITION,
                            "knowledge base is rebuilding",
                        )
                        return  # unreachable; for type checkers

                    # 3. UPDATE (empty name/description keep current values)
                    try:
                        kb_row = await kb_repo.update_kb(
                            conn,
                            tenant_id=tenant_id,
                            kb_id=request.kb_id,
                            name=request.name,
                            description=request.description,
                        )
                    except asyncpg.UniqueViolationError:
                        # UNIQUE(tenant_id, name) hit: name collides with
                        # another KB in the same tenant → ALREADY_EXISTS
                        # (Gateway maps to 409).
                        context.abort(
                            grpc.StatusCode.ALREADY_EXISTS,
                            "knowledge base name already exists in this tenant",
                        )
                        return
                    if kb_row is None:
                        context.abort(
                            grpc.StatusCode.NOT_FOUND, "knowledge base not found"
                        )
                        return

                    # audit kb.update (plan §6.3) — same transaction as the
                    # UPDATE itself.
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=request.kb_id,
                        action="kb.update",
                        actor_user_id=actor,
                        before_state=_kb_audit_snapshot(before_row),
                        after_state=_kb_audit_snapshot(kb_row),
                    )

                    # 4. write kb_operations idempotency record (result = updated row)
                    # Poison-key self-heal: a prior attempt that crashed between
                    # create_operation and complete_operation leaves a pending row with
                    # result=NULL; the replay check above skips it, so the INSERT
                    # here hits UNIQUE(tenant_id, idempotency_key). Reuse that
                    # pending row and complete it — the retry becomes a replay.
                    # The nested transaction (SAVEPOINT) keeps the outer tx usable
                    # after the violated constraint aborts the inner one. Note:
                    # distinct from the name-collision 23505 above, which is a
                    # genuine ALREADY_EXISTS.
                    try:
                        async with conn.transaction():
                            task_row = await operation_repo.create_operation_in_tx(
                                conn,
                                tenant_id=tenant_id,
                                idempotency_key=idem_key,
                                operation_type="kb.update",
                                resource_type="knowledge_base",
                                resource_id=request.kb_id,
                                payload={"kb_id": request.kb_id, "name": request.name},
                                status="pending",
                            )
                    except asyncpg.UniqueViolationError:
                        existing = await operation_repo.find_by_idempotency_key(
                            conn,
                            tenant_id=tenant_id,
                            idempotency_key=idem_key,
                            operation_type="kb.update",
                        )
                        if existing is None:
                            # A concurrent writer raced the row away between
                            # INSERT and SELECT; surface as UNKNOWN rather
                            # than masking it.
                            raise
                        task_row = existing
                    await operation_repo.complete_operation_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        operation_id=str(task_row["id"]),
                        result=kb_row,
                    )
        except Exception:
            # failure audit kb.update (plan §6.3): 404/409 business
            # rejections are recorded; INVALID_ARGUMENT never reaches here
            # (validated before the acquire block). The business tx has
            # already rolled back, so the audit lands in its own tx.
            info = _audit_failure_info(context)
            if info is not None:
                await _record_failure_audit(
                    self._pool,
                    tenant_id=tenant_id,
                    kb_id=request.kb_id,
                    action="kb.update",
                    intent=intent,
                    error_code=info[0],
                    error_msg=info[1],
                    actor_user_id=actor,
                )
            raise

        # 5. return updated row
        return _kb_row_to_pb(kb_row)

    def DeleteKB(self, request, context):
        return _run_async(self._delete_kb(request, context))

    async def _delete_kb(self, request, context) -> empty_pb2.Empty:
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        tenant_id = request.tenant_id
        kb_id = request.kb_id
        actor = _actor_user_id(context)
        corr_ctx = corr(request_id=_request_id(context), kb_id=kb_id)

        # 1. Delete the KB row + dependents; fetch persisted vector_store_id
        #    for Core cleanup. KB-SPLIT-P1D (#053, ADR §五-阶段1-4): the
        #    cross-library tenants(id) ON DELETE CASCADE FK is gone from the
        #    kb baseline DDL, so the cascade is kb's own responsibility —
        #    kb_repo.delete_kb_cascade hard-deletes kb_messages/kb_sessions/
        #    kb_documents and the FK-less kb_chunks (no cascade from
        #    kb_documents — review fix #1), then soft-deletes the
        #    knowledge_bases row in one transaction
        #    (vector/object cleanup stays best-effort below, owned by
        #    #056/#058). audit kb.delete (plan §6.3): before = the KB row
        #    being removed, after = NULL; written in the same transaction as
        #    the delete. 404 failure audit: skipped — a missing KB row cannot
        #    satisfy the kb_audit_log.kb_id FK, and there is no meaningful
        #    before_state to record (deviation logged in the P1 plan, B8 §6.3).
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                kb_row = await kb_repo.get_kb(
                    conn, tenant_id=tenant_id, kb_id=kb_id
                )
                if not kb_row:
                    context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                    return
                # 删除优先于重建（原 B6 #24 互斥 guard 已移除）：rebuilding KB
                # 可删——重建卡死时 KB 不能被永久锁死（线上 409 事故）。安全性
                # 由 consumer 侧已有 guards 兜底：开头 get_kb（过滤 deleted）
                # 未命中 → skip + task failed；退出转换 rebuilding→active 是
                # 条件 UPDATE（set_status_in_tx），KB 已删则 no-op；逐文档
                # 处理前 re-check 文档行。OpenAPI 契约 DELETE 亦仅声明
                # 204/404（无 409）。
                await audit_repo.insert_audit_in_tx(
                    conn,
                    tenant_id=tenant_id,
                    kb_id=kb_id,
                    action="kb.delete",
                    actor_user_id=actor,
                    before_state=_kb_audit_snapshot(kb_row),
                    after_state=None,
                )
                deleted = await kb_repo.delete_kb_cascade(
                    conn, tenant_id=tenant_id, kb_id=kb_id
                )
        if not deleted:
            context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
            return

        # C-fix (best-effort): drop this KB's kb.create idempotency replay
        # rows. The create fallback key create_kb:{tenant}:{name} is shared
        # by every same-name create in the tenant, so a surviving row would
        # make the next same-name create replay this KB's stale snapshot
        # (old id returned, nothing inserted). The _create_kb replay guard
        # also skips deleted KBs, so a failure here only leaves a stale row
        # for that guard to bypass — not a correctness break.
        try:
            async with self._pool.acquire() as conn:
                await operation_repo.delete_kb_create_replay_records(
                    conn, tenant_id=tenant_id, kb_id=kb_id
                )
        except Exception as e:  # noqa: BLE001 — cleanup is best-effort
            logger.warning(
                "kb-service: DeleteKB kb_id=%s kb.create replay-record "
                "cleanup failed (best-effort): %s %s", kb_id, e, corr_ctx,
            )

        # 2. D1 向量自管（issue-056）：直连 Milvus 删 collection — best-effort.
        # vector_store_id 现存的是 collection 名（CreateKB 落库的
        # "kb_{kb_id}" 派生名）。
        vector_store_id = str(kb_row.get("vector_store_id") or "")
        if not vector_store_id:
            logger.warning(
                "kb-service: DeleteKB kb_id=%s has no persisted vector_store_id; "
                "skipping Milvus collection cleanup %s", kb_id, corr_ctx,
            )
        else:
            try:
                await self._vector_store.drop_collection(name=vector_store_id)
            except VectorStoreError as e:
                # best-effort: KB is already soft-deleted; vector cleanup can be
                # retried by a reconciler.
                logger.warning(
                    "kb-service: DeleteKB kb_id=%s Milvus collection cleanup failed "
                    "(best-effort): %s %s", kb_id, e, corr_ctx,
                )

        # 3. D1 对象自管（issue-059, ADR §D1-删除）：删全前缀 {kb_id}/ — best-effort.
        # 覆盖该 KB 下所有文档正本 + 图片产物（两路径都挂 {kb_id}/ 前缀，
        # 与 #053 的 CASCADE 语义流程衔接：DB 行已硬删，对象清理兜底）。
        try:
            await self._object_store.remove_objects_by_prefix(prefix=f"{kb_id}/")
        except ObjectStoreError as e:
            logger.warning(
                "kb-service: DeleteKB kb_id=%s object cleanup failed "
                "(best-effort): %s %s", kb_id, e, corr_ctx,
            )

        return empty_pb2.Empty()

    def GetDocumentUploadURL(self, request, context):
        return _run_async(self._get_document_upload_url(request, context))

    async def _get_document_upload_url(
        self, request, context
    ) -> kb_pb.GetDocumentUploadURLResponse:
        """GetDocumentUploadURL — kb 自有 bucket 预签名 PUT 签发（#058, ADR §D1）。

        D1 对象自管（issue-058）：kb 不再经 Core objects 中转——直接对 kb
        自有 bucket（settings.minio_bucket）签发预签名 PUT URL（对外视角
        publicEndpoint/NodePort，前端浏览器直传），key 规则
        ``{kb_id}/{doc_id}``（正本，无文件名后缀）。kb_documents 登记后移
        到 CompleteDocumentUpload（HeadObject 校验后写 pending 行），本端点
        只预留 doc_id 并返回签发的 URL。
        """
        if not request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required"
            )
            return
        try:
            uuid.UUID(request.idempotency_key.strip())
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key must be a uuid"
            )
            return
        # validate file_type (SPEC §6.2)
        allowed = {"pdf", "docx", "xlsx", "pptx", "md", "txt"}
        if request.file_type not in allowed:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"file_type must be one of {sorted(allowed)}",
            )
            return
        # validate tenant_id / kb_id UUIDs — symmetric with CompleteDocumentUpload
        # (a non-UUID kb_id would otherwise build a malformed storage_path and
        # only surface as a FK error at complete time).
        if not request.tenant_id or not request.kb_id:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "tenant_id and kb_id are required",
            )
            return
        for field, value in (("tenant_id", request.tenant_id), ("kb_id", request.kb_id)):
            try:
                uuid.UUID(value)
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"{field} must be a valid UUID",
                )
                return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return

        # 0. verify the KB exists BEFORE resolving an upload URL. Otherwise a
        # non-existent kb_id produces a presigned URL pointing at an object
        # under a dead KB (and the complete-time registration trips the
        # kb_documents.kb_id FK with a 500 instead of a clean 404).
        # Failure audit doc.create (plan §6.3): NOT_FOUND / FAILED_PRECONDITION
        # are business failures recorded with the request intent; the KB row
        # exists on those paths, so the kb_audit_log.kb_id FK is satisfiable.
        # ObjectStoreError (MinIO down / presign failure) is deliberately NOT
        # audited (not a business rule rejection) — same treatment as the
        # Core-down UNAVAILABLE path it replaces.
        actor = _actor_user_id(context)
        intent: dict[str, Any] | None = None
        try:
            async with self._pool.acquire() as conn:
                kb_row = await kb_repo.get_kb(
                    conn, tenant_id=request.tenant_id, kb_id=request.kb_id
                )
            if kb_row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                return
            if kb_row.get("status") == "rebuilding":
                # B6 (#24): uploads are mutually exclusive with a running
                # full-KB rebuild (doc.create failure audits this path).
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "knowledge base is rebuilding",
                )
                return

            doc_id = str(uuid.uuid4())
            # D1 key rule (ADR §D1): 正本 {kb_id}/{doc_id} — 无文件名后缀，
            # 同一 doc 的重传覆盖同一 key，产物 key 由 #059 规则派生。
            storage_path = f"{request.kb_id}/{doc_id}"
            intent = _doc_intent_snapshot(request, storage_path=storage_path)

            # 1. presign PUT against kb's own bucket — public/NodePort view
            #    (frontend-reachable), TTL from settings (default 15 min).
            try:
                upload_url = await self._object_store.presigned_put_object(
                    key=storage_path
                )
                metrics.inc_presign("presign_put", True)
            except ObjectStoreError as e:
                metrics.inc_presign("presign_put", False)
                context.abort(
                    grpc.StatusCode.UNAVAILABLE, f"MinIO presign failed: {e}"
                )
                return
        except Exception:
            info = _audit_failure_info(context)
            if info is not None:
                await _record_failure_audit(
                    self._pool,
                    tenant_id=request.tenant_id,
                    kb_id=request.kb_id,
                    action="doc.create",
                    intent=intent,
                    error_code=info[0],
                    error_msg=info[1],
                    actor_user_id=actor,
                )
            raise

        # 2. No kb_documents write here (D1 three-step flow): the row is
        #    registered by CompleteDocumentUpload after the frontend's direct
        #    PUT is verified via HeadObject. The reserved doc_id + storage
        #    key are returned to the client for that follow-up call.
        return kb_pb.GetDocumentUploadURLResponse(
            doc_id=doc_id,
            upload_url=upload_url,
            storage_path=storage_path,
        )

    def GetObjectURL(self, request, context):
        return _run_async(self._get_object_url(request, context))

    async def _get_object_url(
        self, request, context
    ) -> kb_pb.GetObjectURLResponse:
        """GetObjectURL — kb 自管对象的预签名 GET 签发（#059, ADR §D1-取用）。

        图片产物取用（前端契约不变：对外只感知 Gateway）：Gateway 把
        ``GET /knowledge-bases/{kb_id}/objects?object_id=`` 转发到本 RPC，
        kb 签发**对外视角**（NodePort）预签名 GET URL 后原样回传；调用方
        （前端浏览器）凭 URL 直连 MinIO 取字节，**Gateway 与 kb 都不
        中转载荷**。签发的 key 由 object_id 解析而来，object_id 语义与
        markdown 占位 ``[图片: caption](object_id)`` 中的字面一致：
        ``{bucket}/{key}``（bucket 为 kb 自有 bucket，key 形如
        ``{kb_id}/{doc_id}/images/{uuid}.{ext}``）。

        归属校验：object_id 的 bucket 必须等于 kb 自有 bucket，且 key 必须
        以 ``{kb_id}/`` 为前缀——否则拒绝（跨 KB 越权取用）。
        """
        tenant_id = request.tenant_id or ""
        kb_id = request.kb_id or ""
        object_id = request.object_id or ""
        if not tenant_id or not kb_id or not object_id:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "tenant_id, kb_id and object_id are required",
            )
            return
        for field, value in (("tenant_id", tenant_id), ("kb_id", kb_id)):
            try:
                uuid.UUID(value)
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"{field} must be a valid UUID",
                )
                return
        # object_id 语义 = {bucket}/{key}（ADR §D1 line 137）：bucket 为
        # kb 自有 bucket，key 为 kb 前缀下的对象路径。只按第一个 "/" 切分
        # （bucket 名不含斜杠），key 保留原始斜杠结构。
        bucket, sep, key = object_id.partition("/")
        if not sep or not bucket or not key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "object_id must be in {bucket}/{key} form",
            )
            return
        if bucket != self._object_store.bucket:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "object does not belong to the kb-owned bucket",
            )
            return
        if not key.startswith(f"{kb_id}/"):
            # 跨 KB 越权取用：object_id 的 key 前缀不属于请求的 kb_id。
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "object does not belong to the requested knowledge base",
            )
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return

        # KB existence gate: a deleted/unknown KB must yield NOT_FOUND rather
        # than signing a URL for an object under a dead KB (mirrors the
        # presign-PUT endpoint's ordering).
        async with self._pool.acquire() as conn:
            kb_row = await kb_repo.get_kb(
                conn, tenant_id=tenant_id, kb_id=kb_id
            )
        if kb_row is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
            return

        # presign GET — public/NodePort view: the URL is opened by the
        # end-user's browser, which lives outside the cluster (mirrors the
        # presign-PUT endpoint and the Core Go SignedDownloadURL precedent).
        try:
            url = await self._object_store.presigned_get_object(
                key=key, view="public"
            )
            metrics.inc_presign("presign_get", True)
        except ObjectStoreError as e:
            metrics.inc_presign("presign_get", False)
            context.abort(
                grpc.StatusCode.UNAVAILABLE, f"MinIO presign failed: {e}"
            )
            return
        return kb_pb.GetObjectURLResponse(
            url=url,
            expires_seconds=self._object_store.presign_ttl_seconds,
        )

    def CompleteDocumentUpload(self, request, context):
        return _run_async(self._complete_document_upload(request, context))

    async def _complete_document_upload(
        self, request, context
    ) -> common_pb2.AsyncTaskRef:
        """CompleteDocumentUpload — HeadObject 校验 + 登记 + 解析触发（#058, ADR §D1）。

        D1 上传三步流的第三步：① GetDocumentUploadURL 签发（本文件）→
        ② 前端直传 MinIO → ③ 本端点。结构上是原子 outbox 事务
        （同 _reparse_document），两处关键差异：

        - 前置 HeadObject 校验：kb_documents 登记前先 stat_object 内网
          视角校验对象真实存在——不存在/大小为 0 时拒绝登记
          （FAILED_PRECONDITION，不写行不进 outbox，防止前端"假上传"
          产生永远 pending 的幽灵文档行）。file_size_bytes 取 HeadObject
          实测值（比请求自报值可信：请求值仅作 intent 参考）。
        - doc 行在本端点创建（create_document）而非更新：presign 端点
          只预留 doc_id 不落库（D1 三步流），kb_documents + doc.create
          audit + kb_operations + outbox_events 在同一事务内首次写入。

        outbox payload 携带真实 file_name（学 reparse：notify 的 "" 是
        历史妥协——parse consumer 需要 file_name 做扩展名路由）。
        """
        if not request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required"
            )
            return
        try:
            uuid.UUID(request.idempotency_key.strip())
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key must be a uuid"
            )
            return
        # validate file_type (SPEC §6.2 — same whitelist as presign)
        allowed = {"pdf", "docx", "xlsx", "pptx", "md", "txt"}
        if request.file_type not in allowed:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"file_type must be one of {sorted(allowed)}",
            )
            return
        if not request.file_name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "file_name is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        tenant_id = request.tenant_id or ""
        kb_id = request.kb_id or ""
        doc_id = request.doc_id or ""
        if not tenant_id or not kb_id or not doc_id:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "tenant_id, kb_id and doc_id are required",
            )
            return
        for field, value in (("tenant_id", tenant_id), ("kb_id", kb_id), ("doc_id", doc_id)):
            try:
                uuid.UUID(value)
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"{field} must be a valid UUID",
                )
                return
        idem_key = request.idempotency_key
        storage_path = f"{kb_id}/{doc_id}"

        # audit doc.create (plan §6.3): intent for the failure path.
        # _doc_intent_snapshot reads upload-request fields (file_size_bytes
        # among them) that the complete proto does not carry — build the
        # intent inline here instead.
        actor = _actor_user_id(context)
        intent: dict[str, Any] = {
            "doc_id": doc_id,
            "file_name": request.file_name,
            "file_type": request.file_type,
            "checksum_sha256": request.checksum_sha256,
            "storage_path": storage_path,
            "kb_id": kb_id,
        }
        try:
            # 1. HeadObject 校验（内网视角 stat）——登记的前置门槛。
            #    对象不存在 → FAILED_PRECONDITION（业务拒绝：客户端流程
            #    违约——未直传就调 complete，审计 doc.create 意图）；
            #    MinIO 网络失败 → UNAVAILABLE（基础设施故障，不进
            #    failure audit，同 presign 端点处理）。
            try:
                stat = await self._object_store.stat_object(key=storage_path)
            except ObjectNotFoundError:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "uploaded object not found or empty — "
                    "complete must follow a successful direct PUT",
                )
                return  # unreachable; for type checkers
            except ObjectStoreError as e:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE, f"MinIO HeadObject failed: {e}"
                )
                return
            file_size_bytes = int(stat.get("size") or 0)
            if file_size_bytes <= 0:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "uploaded object not found or empty — "
                    "complete must follow a successful direct PUT",
                )
                return  # unreachable; for type checkers
            if file_size_bytes > MAX_UPLOAD_BYTES:
                # SPEC §6.1 DOC_TOO_LARGE: hard cap at the business layer even
                # though the frontend also pre-validates — a direct PUT can
                # bypass the console check, and an oversized object would
                # otherwise be registered and fed to the parser.
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"uploaded object exceeds the {MAX_UPLOAD_BYTES}-byte limit",
                )
                return  # unreachable; for type checkers

            async with self._pool.acquire() as conn:
                # 2. idempotent replay: a prior complete for this doc that
                #    reached pending/completed returns the same AsyncTaskRef
                #    (SPEC §6.4 replay semantics, notify/reparse pattern).
                existing = await operation_repo.find_by_idempotency_key(
                    conn,
                    tenant_id=tenant_id,
                    idempotency_key=idem_key,
                    operation_type="kb.parse",
                )
                if existing and existing.get("status") in ("pending", "completed"):
                    # P3-4: idempotent replay is its own outcome, not a real
                    # first-upload success — keep it out of the success rate.
                    metrics.inc_presign_idempotent("complete")
                    return common_pb2.AsyncTaskRef(
                        task_id=str(existing["id"]),
                        task_type=existing.get("operation_type") or "kb.parse",
                        status=existing.get("status") or "pending",
                        location_url="",
                    )

                # 2.5. duplicate-doc guard: the same (kb, doc) completed
                #      before under a DIFFERENT idempotency key (client lost
                #      the key / replayed the presign response late). The
                #      doc row already exists → create_document would trip
                #      the pk UNIQUE and surface as UNKNOWN; reject cleanly.
                #      A soft-deleted row (parse_status='failed' +
                #      error_message='deleted') reads as None → falls
                #      through to create, which is the correct re-upload
                #      path over a deleted doc id.
                dup_doc = await document_repo.get_document(
                    conn, tenant_id=tenant_id, kb_id=kb_id, doc_id=doc_id
                )
                if dup_doc is not None:
                    context.abort(
                        grpc.StatusCode.ALREADY_EXISTS,
                        "document already registered — "
                        "retry the original complete call or request a fresh "
                        "upload URL for a new doc_id",
                    )
                    return  # unreachable; for type checkers

                # 3. single transaction: kb_documents (create, pending) +
                #    doc.create audit + kb_operations + outbox_events.
                #    Repo helpers here do not open their own transactions.
                from app.repositories import outbox as outbox_repo

                async with conn.transaction():
                    # a0. KB gate — B6 (#24): a KB under full rebuild rejects
                    #     new parses (checked inside the tx so the read is
                    #     consistent with the write below).
                    kb_row = await kb_repo.get_kb(
                        conn, tenant_id=tenant_id, kb_id=kb_id
                    )
                    if kb_row is None:
                        context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                        return  # unreachable; for type checkers
                    if kb_row.get("status") == "rebuilding":
                        context.abort(
                            grpc.StatusCode.FAILED_PRECONDITION,
                            "knowledge base is rebuilding",
                        )
                        return  # unreachable; for type checkers

                    # a. create the kb_documents row (D1 three-step flow:
                    #    registration deferred from presign to here; the
                    #    pre-reserved doc_id from GetDocumentUploadURL is
                    #    written explicitly). object_id stays None — the
                    #    object lives in kb's own bucket now, addressed by
                    #    storage_path; there is no Core object UUID.
                    #
                    #    Race guard: the dup_doc pre-check above and this
                    #    INSERT are not atomic — a concurrent complete for the
                    #    same (kb, doc) under a DIFFERENT key can slip in
                    #    between. The pk UNIQUE then trips; the nested
                    #    transaction (SAVEPOINT) keeps the outer tx usable and
                    #    we map the collision to the same ALREADY_EXISTS the
                    #    pre-check raises, instead of surfacing as UNKNOWN.
                    try:
                        async with conn.transaction():
                            await document_repo.create_document(
                                conn,
                                tenant_id=tenant_id,
                                kb_id=kb_id,
                                file_name=request.file_name,
                                file_type=request.file_type,
                                file_size_bytes=file_size_bytes,
                                storage_path=storage_path,
                                checksum_sha256=request.checksum_sha256,
                                custom_metadata=_parse_metadata(request.custom_metadata),
                                doc_id=doc_id,
                            )
                    except asyncpg.UniqueViolationError:
                        context.abort(
                            grpc.StatusCode.ALREADY_EXISTS,
                            "document already registered — "
                            "retry the original complete call or request a fresh "
                            "upload URL for a new doc_id",
                        )
                        return  # unreachable; for type checkers

                    # b. insert kb_operations row for idempotent replay +
                    #    status tracking, with UNIQUE race self-heal (same
                    #    pattern as _reparse_document):
                    #    a concurrent complete with the same key can win the
                    #    INSERT between the replay check above and here. The
                    #    nested transaction (SAVEPOINT) keeps the outer tx
                    #    usable; re-lookup then replays the winner's task.
                    #    A failed row is revived to pending (notify pattern):
                    #    the doc row exists by then, so revive is the only
                    #    non-dead end for a same-key retry.
                    publish_event = True
                    task_id = ""
                    task_type = "kb.parse"
                    task_status = "pending"
                    try:
                        async with conn.transaction():
                            task_row = await operation_repo.create_operation_in_tx(
                                conn,
                                tenant_id=tenant_id,
                                idempotency_key=idem_key,
                                operation_type="kb.parse",
                                resource_type="kb_document",
                                resource_id=doc_id,
                                payload={
                                    "doc_id": doc_id,
                                    "kb_id": kb_id,
                                    "object_id": "",
                                },
                                status="pending",
                            )
                        task_id = str(task_row["id"])
                    except asyncpg.UniqueViolationError:
                        existing = await operation_repo.find_by_idempotency_key(
                            conn,
                            tenant_id=tenant_id,
                            idempotency_key=idem_key,
                            operation_type="kb.parse",
                        )
                        if existing is None:
                            raise
                        task_id = str(existing["id"])
                        task_type = existing.get("operation_type") or "kb.parse"
                        task_status = existing.get("status") or "pending"
                        if task_status == "failed":
                            revived = await operation_repo.revive_operation_in_tx(
                                conn,
                                tenant_id=tenant_id,
                                operation_id=task_id,
                            )
                            if revived is not None:
                                task_status = "pending"
                            else:
                                publish_event = False
                        else:
                            # pending/completed: the concurrent winner's tx
                            # already published the outbox event — replay it,
                            # do not publish a second one.
                            publish_event = False

                    # c. audit doc.create (plan §6.3) — same transaction,
                    #    after_state links task_id (doc.parse lifecycle
                    #    audits follow the notify template downstream).
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        action="doc.create",
                        actor_user_id=actor,
                        before_state=None,
                        after_state={**intent, "doc_id": doc_id, "task_id": task_id},
                    )

                    # d. outbox event — the parse pipeline trigger. payload
                    #    mirrors the notify/reparse template with real values
                    #    (file_name from the request, storage_path from the
                    #    D1 key rule; object_id "" — no Core object anymore).
                    if publish_event:
                        kb_chunk_size = kb_row.get("chunk_size") or 1024
                        await outbox_repo.insert_event(
                            conn,
                            tenant_id=tenant_id,
                            aggregate_type="kb_documents",
                            aggregate_id=doc_id,
                            event_type="kb.parse",
                            payload={
                                "doc_id": doc_id,
                                "kb_id": kb_id,
                                "storage_path": storage_path,
                                "tenant_id": tenant_id,
                                "file_name": request.file_name,
                                "object_id": "",
                                "chunk_size": kb_chunk_size,
                                # Lets the parse consumer close this kb_operations
                                # row when the doc reaches a terminal parse_status.
                                "task_id": task_id,
                            },
                        )
        except Exception:
            # failure audit doc.create (plan §6.3): business rejections
            # (HeadObject empty object, KB missing/rebuilding) are recorded;
            # INVALID_ARGUMENT aborts above, before this try block;
            # UNAVAILABLE (MinIO down) is not an audited code.
            metrics.inc_presign("complete", False)
            info = _audit_failure_info(context)
            if info is not None:
                await _record_failure_audit(
                    self._pool,
                    tenant_id=tenant_id,
                    kb_id=kb_id,
                    action="doc.create",
                    intent=intent,
                    error_code=info[0],
                    error_msg=info[1],
                    actor_user_id=actor,
                )
            raise

        metrics.inc_presign("complete", True)
        return common_pb2.AsyncTaskRef(
            task_id=task_id,
            task_type=task_type,
            status=task_status,
            location_url="",
        )

    def GetDocument(self, request, context):
        return _run_async(self._get_document(request, context))

    async def _get_document(self, request, context) -> kb_pb.KBDocument:
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        async with self._pool.acquire() as conn:
            row = await document_repo.get_document(
                conn,
                tenant_id=request.tenant_id,
                kb_id=request.kb_id,
                doc_id=request.doc_id,
            )
        if not row:
            context.abort(grpc.StatusCode.NOT_FOUND, "document not found")
            return
        return _doc_row_to_pb(row)

    def ListDocuments(self, request, context):
        return _run_async(self._list_documents(request, context))

    async def _list_documents(self, request, context) -> kb_pb.ListDocumentsResponse:
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        limit = request.page.limit or 20
        cursor = request.page.cursor or None
        async with self._pool.acquire() as conn:
            # KB existence gate: a deleted/unknown KB must yield NOT_FOUND, not
            # an empty list (SPEC §6.1 ListDocuments 404 semantics).
            kb_row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
            if not kb_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                return
            rows, total = await document_repo.list_documents(
                conn,
                tenant_id=request.tenant_id,
                kb_id=request.kb_id,
                parse_status=request.parse_status or None,
                limit=limit,
                cursor=cursor,
            )
        docs = [_doc_row_to_pb(r) for r in rows]
        next_cursor = str(rows[-1]["id"]) if rows and len(rows) >= limit else ""
        return kb_pb.ListDocumentsResponse(
            documents=docs,
            meta=common_pb2.CursorPageMeta(total=total, next_cursor=next_cursor),
        )

    def DeleteDocument(self, request, context):
        return _run_async(self._delete_document(request, context))

    async def _delete_document(self, request, context) -> empty_pb2.Empty:
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        # 1. soft-delete document + delete chunks + fetch persisted vector_store_id
        #    audit doc.delete (plan §6.3): before = the document row being
        #    removed, after = NULL; written in the same transaction as the
        #    soft-delete and chunk cleanup. Failure audits are best-effort:
        #    the kb-404 abort cannot satisfy the kb_audit_log.kb_id FK (no
        #    KB row) and is swallowed by _record_failure_audit (debug); the
        #    doc-404 abort records normally (the KB gate passed, so the KB
        #    row exists).
        actor = _actor_user_id(context)
        corr_ctx = corr(
            request_id=_request_id(context),
            kb_id=request.kb_id,
            doc_id=request.doc_id,
        )
        intent: dict[str, Any] = {"doc_id": request.doc_id}
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    kb_row = await kb_repo.get_kb(
                        conn, tenant_id=request.tenant_id, kb_id=request.kb_id
                    )
                    if not kb_row:
                        context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                        return
                    if kb_row.get("status") == "rebuilding":
                        # B6 (#24): doc deletes are mutually exclusive with a
                        # running full-KB rebuild (the rebuild consumer skips
                        # docs that vanish mid-run; soft-deleted rows are
                        # excluded from the rebuild snapshot).
                        context.abort(
                            grpc.StatusCode.FAILED_PRECONDITION,
                            "knowledge base is rebuilding",
                        )
                        return
                    # before snapshot: the row as it stands pre-delete. None
                    # when the doc is already soft-deleted (get_document
                    # filters the 'deleted' marker) — a repeated delete
                    # records with before=NULL, mirroring the no-row case.
                    doc_row = await document_repo.get_document(
                        conn,
                        tenant_id=request.tenant_id,
                        kb_id=request.kb_id,
                        doc_id=request.doc_id,
                    )
                    deleted = await document_repo.soft_delete_document(
                        conn,
                        tenant_id=request.tenant_id,
                        kb_id=request.kb_id,
                        doc_id=request.doc_id,
                    )
                    if not deleted:
                        context.abort(grpc.StatusCode.NOT_FOUND, "document not found")
                        return
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=request.tenant_id,
                        kb_id=request.kb_id,
                        action="doc.delete",
                        actor_user_id=actor,
                        before_state=_doc_audit_snapshot(doc_row),
                        after_state=None,
                    )
                    await chunk_repo.delete_chunks_by_doc(
                        conn,
                        tenant_id=request.tenant_id,
                        kb_id=request.kb_id,
                        doc_id=request.doc_id,
                    )
        except Exception:
            # failure audit doc.delete (plan §6.3): 404 business rejections
            # are recorded; INVALID_ARGUMENT never reaches here (validated
            # by the proto/route layer). The business tx has already rolled
            # back, so the audit lands in its own fresh transaction.
            info = _audit_failure_info(context)
            if info is not None:
                await _record_failure_audit(
                    self._pool,
                    tenant_id=request.tenant_id,
                    kb_id=request.kb_id,
                    action="doc.delete",
                    intent=intent,
                    error_code=info[0],
                    error_msg=info[1],
                    actor_user_id=actor,
                )
            raise
        # 2. D1 向量自管（issue-056）：直连 Milvus 按 doc_id 删向量 — best-effort.
        # vector_store_id 现存的是 collection 名（CreateKB 落库的
        # "kb_{kb_id}" 派生名）。
        vector_store_id = str(kb_row.get("vector_store_id") or "")
        if not vector_store_id:
            logger.warning(
                "kb-service: DeleteDocument kb_id=%s has no persisted vector_store_id; "
                "skipping Milvus vector cleanup %s", request.kb_id, corr_ctx,
            )
        else:
            try:
                await self._vector_store.delete(
                    collection=vector_store_id,
                    filter_expr=f'metadata["doc_id"] == "{request.doc_id}"',
                )
            except VectorStoreError as e:
                # best-effort vector cleanup; document already soft-deleted.
                logger.warning(
                    "kb-service: DeleteDocument kb_id=%s doc_id=%s Milvus vector "
                    "cleanup failed (best-effort): %s %s",
                    request.kb_id, request.doc_id, e, corr_ctx,
                )
        # 3. D1 对象自管（issue-059, ADR §D1-删除）：删正本 key + 图片前缀 — best-effort.
        # 正本 key 优先取持久化的 storage_path（#058 登记值 {kb_id}/{doc_id}），
        # 缺省回退同规则；图片产物前缀 {kb_id}/{doc_id}/images/（解析管线写入）。
        original_key = str((doc_row or {}).get("storage_path") or "") or (
            f"{request.kb_id}/{request.doc_id}"
        )
        try:
            await self._object_store.remove_object(key=original_key)
        except ObjectStoreError as e:
            logger.warning(
                "kb-service: DeleteDocument kb_id=%s doc_id=%s original object "
                "cleanup failed (best-effort): %s %s",
                request.kb_id, request.doc_id, e, corr_ctx,
            )
        try:
            await self._object_store.remove_objects_by_prefix(
                prefix=f"{request.kb_id}/{request.doc_id}/images/"
            )
        except ObjectStoreError as e:
            logger.warning(
                "kb-service: DeleteDocument kb_id=%s doc_id=%s image objects "
                "cleanup failed (best-effort): %s %s",
                request.kb_id, request.doc_id, e, corr_ctx,
            )
        return empty_pb2.Empty()

    def Query(self, request, context):
        return _run_async(self._query(request, context))

    async def _query(self, request, context) -> kb_pb.QueryResponse:
        """Query — kb_messages persistence + Redis session cache (SPEC §6.1, US-010).

        1. validate idempotency_key
        2. resolve session_id (empty → new UUID)
        3. INSERT kb_messages(role='user', content=question)
        4. Redis: RPUSH user_msg; EXPIRE 24h; LTRIM 20
        5. call rag-engine Query (gRPC-intent client)
        6. INSERT kb_messages(role='assistant', content=answer, sources)
        7. Redis: RPUSH assistant_msg; LTRIM 20
        8. return QueryResponse
        """
        if not request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required"
            )
            return
        try:
            uuid.UUID(request.idempotency_key.strip())
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key must be a uuid"
            )
            return
        if not request.question:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "question is required"
            )
            return

        tenant_id = request.tenant_id or ""
        kb_id = request.kb_id or ""
        if not tenant_id or not kb_id:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "tenant_id and kb_id are required",
            )
            return

        # 2. resolve / create session id (SPEC §6.1 step 2)
        session_id = request.session_id or str(uuid.uuid4())

        # 2.5. Load the KB row BEFORE persisting session/message so a
        # query against a non-existent kb_id returns NOT_FOUND instead of
        # tripping the kb_sessions.kb_id FK constraint (→ 500). This also
        # yields the KB's top_k / score_threshold / retrieval_mode once.
        kb_row = None
        if self._pool is not None:
            try:
                async with self._pool.acquire() as conn:
                    kb_row = await kb_repo.get_kb(
                        conn, tenant_id=tenant_id, kb_id=kb_id
                    )
            except Exception:  # noqa: BLE001 — degrade to defaults below
                logger.warning("kb-service: failed to load KB config, using defaults", exc_info=True)
                kb_row = None
        if kb_row is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND, "knowledge base not found"
            )
            return

        kb_cfg = {
            "top_k": kb_row.get("top_k") or 5,
            # 未设置(0)时透传给 rag-engine，由 DEFAULT_SCORE_THRESHOLD 兜底。
            "score_threshold": kb_row.get("score_threshold") or 0.0,
            "retrieval_mode": kb_row.get("retrieval_mode") or "hybrid",
            "embedding_model": kb_row.get("embedding_model") or "",
            "default_inference_service": str(
                kb_row.get("default_inference_service") or ""
            ),
        }

        # 3-4. persist user message + Redis cache (best-effort).
        # create_session + insert_message(user) run in a single transaction so
        # a partial user-message write can't survive a crash mid-RPC (SPEC §6.1).
        if self._pool is not None:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await message_repo.create_session_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        session_id=session_id,
                    )
                    await message_repo.insert_message_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        session_id=session_id,
                        role="user",
                        content=request.question,
                    )

        cache = self._session_cache_factory()
        if cache is not None:
            await cache.append_message(
                session_id=session_id, role="user", content=request.question
            )

        # 5. Resolve retrieval configuration from the KB row (loaded at
        #    step 2.5 above). Client request values override the KB config
        #    when explicitly provided.
        top_k = request.top_k if request.top_k else kb_cfg["top_k"]
        score_threshold = (
            request.score_threshold if request.score_threshold != 0
            else kb_cfg["score_threshold"]
        )
        retrieval_mode = (request.retrieval_mode or kb_cfg["retrieval_mode"] or "hybrid")

        # 6. QueryOrchestrator: retrieve → gates → Generate RPC. A failure
        # here must not leave a half-record: the user message was already
        # persisted (steps 3-4), so record the failure trail (assistant
        # error placeholder + kb.query audit row) before re-raising.
        try:
            result = await self._query_new_path(
                tenant_id=tenant_id,
                kb_id=kb_id,
                question=request.question,
                session_id=session_id,
                top_k=top_k,
                score_threshold=score_threshold,
                retrieval_mode=retrieval_mode,
                inference_service_name=(
                    request.inference_service_name
                    or kb_cfg["default_inference_service"]
                    or ""
                ),
                vector_store_id=str(kb_row.get("vector_store_id") or ""),
                embedding_model=kb_cfg["embedding_model"],
                cache=cache,
                context=context,
            )
        except Exception as e:
            await self._on_query_failure(
                context=context,
                tenant_id=tenant_id,
                kb_id=kb_id,
                session_id=session_id,
                question=request.question,
                cache=cache,
                error=e,
            )
            raise
        answer = result.answer
        sources = result.sources
        input_tokens = result.input_tokens
        output_tokens = result.output_tokens

        # 6-7. persist assistant message + Redis cache (best-effort).
        await self._persist_assistant(
            tenant_id=tenant_id, session_id=session_id,
            answer=answer, sources=sources,
            input_tokens=input_tokens, output_tokens=output_tokens,
            cache=cache,
        )

        # Q&A audit: record the turn's content into the operation history
        # (kb_audit_log, action kb.query). Best-effort, after the response
        # data is durable — see _record_query_audit.
        await self._record_query_audit(
            context=context,
            tenant_id=tenant_id,
            kb_id=kb_id,
            session_id=session_id,
            question=request.question,
            answer=answer,
            sources=sources,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

        # 8. build response (session_id may have been newly created).
        source_chunks = [
            kb_pb.SourceChunk(
                doc_id=s.get("doc_id", ""),
                file_name=s.get("file_name", ""),
                page=s.get("page", 0),
                content=s.get("content", ""),
                score=s.get("score", 0.0),
            )
            for s in sources
        ]
        return kb_pb.QueryResponse(
            answer=answer,
            sources=source_chunks,
            session_id=session_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

    # ── Plan step 10 (issue-038): Retrieve server-streaming RPC ───────────

    def Retrieve(self, request, context):
        """Retrieve — server-streaming RAG: retrieve → sources → tokens → done.

        Plan §10.2: Gateway SSE switches to this gRPC stream when
        ``KB_SSE_USE_NEW_PATH=True``. The handler orchestrates retrieve →
        three no-result gates → GenerateStream, yielding RetrieveEvent
        messages (token* → sources → done), matching the legacy SSE event
        sequence.

        Session management and persistence mirror Query RPC: persist user
        message first, load history (includes current-turn user), stream
        GenerateStream, then persist assistant message.

        gRPC server-streaming RPC methods must be sync generators (yield
        RetrieveEvent). The async orchestrator runs on the dedicated
        gRPC event loop; events are bridged via a thread-safe queue so the
        sync generator can yield them to the gRPC transport.
        """
        yield from self._retrieve_stream_sync(request, context)

    def _retrieve_stream_sync(self, request, context):
        """Sync generator wrapper: bridge async _retrieve_stream to gRPC.

        Runs the async generator on the dedicated gRPC event loop and
        bridges events through a thread-safe queue. A sentinel (None)
        signals completion; exceptions are re-raised in the sync thread
        so gRPC can map them to status codes.

        Cancellation safety (S1/S2): if the gRPC client disconnects mid-stream
        the sync generator stops iterating (GeneratorExit), which cancels the
        pending ``run_coroutine_threadsafe`` future — the ``_runner`` coroutine
        receives ``CancelledError`` and stops the LLM generation + DB work.
        ``ev_q.get`` uses a timeout so the sync thread cannot hang forever if
        the event loop dies without producing the sentinel.
        """
        if _grpc_loop is None:
            _start_grpc_loop()

        ev_q: queue.Queue = queue.Queue()
        loop = _grpc_loop
        # Max wait for a single event; the gRPC query timeout is 120s on the
        # gateway side, so 130s gives a grace margin before we give up.
        _EVENT_TIMEOUT = 130.0
        future: asyncio.Future | None = None

        async def _runner():
            try:
                async for event in self._retrieve_stream(request, context):
                    ev_q.put(event)
            except Exception as exc:  # noqa: BLE001 — propagate to sync side
                ev_q.put(exc)
            finally:
                ev_q.put(None)  # sentinel: stream complete

        try:
            future = asyncio.run_coroutine_threadsafe(_runner(), loop)

            while True:
                try:
                    item = ev_q.get(timeout=_EVENT_TIMEOUT)
                except queue.Empty:
                    # S2: event loop died or runner hung — cancel and abort.
                    if future is not None and not future.done():
                        future.cancel()
                    context.abort(
                        grpc.StatusCode.DEADLINE_EXCEEDED,
                        "retrieve stream timed out waiting for event",
                    )
                    return
                if item is None:
                    break
                if isinstance(item, BaseException):
                    if isinstance(item, grpc.RpcError):
                        raise item
                    # Map common exceptions to gRPC status codes.
                    if isinstance(item, RagEngineError):
                        context.abort(
                            grpc.StatusCode.UNAVAILABLE,
                            f"backend unavailable: {item}",
                        )
                        return
                    raise item
                yield item
        except GeneratorExit:
            # S1: client disconnected — cancel the runner coroutine so the
            # LLM generation / DB work stops instead of leaking.
            if future is not None and not future.done():
                future.cancel()
            raise

    async def _retrieve_stream(self, request, context):
        """Async generator: orchestrate retrieve → gates → GenerateStream.

        Yields RetrieveEvent messages (token* → sources → done). Session
        management mirrors Query RPC (issue-038 AC 2).
        """
        # 1. validate request (mirror Query validation).
        if not request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required"
            )
            return
        try:
            uuid.UUID(request.idempotency_key.strip())
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key must be a uuid"
            )
            return
        if not request.question:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "question is required")
            return
        tenant_id = request.tenant_id or ""
        kb_id = request.kb_id or ""
        if not tenant_id or not kb_id:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "tenant_id and kb_id are required",
            )
            return

        # 2. resolve / create session id.
        session_id = request.session_id or str(uuid.uuid4())

        # 2.5. Load KB row (NOT_FOUND gate, mirror Query).
        kb_row = None
        if self._pool is not None:
            try:
                async with self._pool.acquire() as conn:
                    kb_row = await kb_repo.get_kb(
                        conn, tenant_id=tenant_id, kb_id=kb_id
                    )
            except Exception:  # noqa: BLE001
                logger.warning("kb-service: failed to load KB config, using defaults", exc_info=True)
                kb_row = None
        if kb_row is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
            return

        kb_cfg = {
            "top_k": kb_row.get("top_k") or 5,
            "score_threshold": kb_row.get("score_threshold") or 0.0,
            "retrieval_mode": kb_row.get("retrieval_mode") or "hybrid",
            "embedding_model": kb_row.get("embedding_model") or "",
            "default_inference_service": str(
                kb_row.get("default_inference_service") or ""
            ),
        }

        # 3-4. persist user message + Redis cache (mirror Query).
        if self._pool is not None:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await message_repo.create_session_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        session_id=session_id,
                    )
                    await message_repo.insert_message_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        session_id=session_id,
                        role="user",
                        content=request.question,
                    )

        cache = self._session_cache_factory()
        if cache is not None:
            await cache.append_message(
                session_id=session_id, role="user", content=request.question
            )

        # 5. Resolve retrieval config (client overrides KB defaults).
        top_k = request.top_k if request.top_k else kb_cfg["top_k"]
        score_threshold = (
            request.score_threshold if request.score_threshold != 0
            else kb_cfg["score_threshold"]
        )
        retrieval_mode = request.retrieval_mode or kb_cfg["retrieval_mode"] or "hybrid"
        inference_service_name = (
            request.inference_service_name
            or kb_cfg["default_inference_service"]
            or ""
        )
        vector_store_id = str(kb_row.get("vector_store_id") or "")

        # 6. Load chat history (includes current-turn user, already persisted).
        history = await self._load_history(
            tenant_id=tenant_id,
            session_id=session_id,
            cache=cache,
        )

        # 7. Build or reuse per-tenant orchestrator (mirror _query_new_path).
        from app.services.query_orchestrator import (
            QueryOrchestrator,
            StreamDoneEvent,
            StreamNoResultEvent,
            StreamSourcesEvent,
            StreamTokenEvent,
        )

        orch = self._orchestrators.get(tenant_id)
        if orch is None:
            if self._retrieve_service_factory is not None:
                retrieve_service = self._retrieve_service_factory(tenant_id)
            else:
                retrieve_service = _default_retrieve_service(tenant_id, self._pool)

            if self._rag_engine_grpc_client_factory is not None:
                rag_engine_grpc = self._rag_engine_grpc_client_factory()
            else:
                rag_engine_grpc = _default_rag_engine_grpc_client()

            orch = QueryOrchestrator(
                retrieve_service=retrieve_service,
                rag_engine_client=rag_engine_grpc,
            )
            self._orchestrators[tenant_id] = orch

        # 7.5. Resolve per-model runtime endpoints (embed vs chat are distinct
        #      inference services → distinct endpoints). Strict on the Q&A
        #      path: a missing/unresolvable model aborts instead of silently
        #      falling back to the rag-engine server default.
        embed_endpoint, embed_model = await self._resolve_required_runtime_endpoint(
            tenant_id=tenant_id, model=kb_cfg["embedding_model"],
            role="embedding", context=context,
        )
        generate_endpoint, generate_model = await self._resolve_required_runtime_endpoint(
            tenant_id=tenant_id, model=inference_service_name, role="chat",
            context=context,
        )

        # 8. Delegate to orchestrator.query_stream (single source of gate
        # logic). Failures before or mid-stream get the same failure trail
        # as the synchronous Query path (assistant error placeholder +
        # kb.query audit row) before the exception propagates.
        answer = ""
        final_sources: list[dict[str, Any]] = []
        final_input_tokens = 0
        final_output_tokens = 0

        async def _stream_with_failure_trail():
            # Thin wrapper so a failure before/inside the stream records
            # the failure trail (assistant error placeholder + audit row)
            # without re-indenting the consumer loop below.
            try:
                async for ev in orch.query_stream(
                    tenant_id=tenant_id,
                    kb_id=kb_id,
                    question=request.question,
                    session_id=session_id,
                    top_k=top_k,
                    score_threshold=score_threshold,
                    retrieval_mode=retrieval_mode,
                    inference_service_name=generate_model or inference_service_name,
                    vector_store_id=vector_store_id,
                    embedding_model=embed_model or kb_cfg["embedding_model"],
                    history=history,
                    embed_runtime_endpoint=embed_endpoint,
                    generate_runtime_endpoint=generate_endpoint,
                ):
                    yield ev
            except Exception as e:
                await self._on_query_failure(
                    context=context,
                    tenant_id=tenant_id,
                    kb_id=kb_id,
                    session_id=session_id,
                    question=request.question,
                    cache=cache,
                    error=e,
                )
                raise

        async for ev in _stream_with_failure_trail():
            if isinstance(ev, StreamTokenEvent):
                answer += ev.content
                yield kb_pb.RetrieveEvent(
                    token=kb_pb.RetrieveTokenEvent(content=ev.content)
                )
            elif isinstance(ev, StreamNoResultEvent):
                answer = ev.answer
                final_input_tokens = ev.input_tokens
                final_output_tokens = ev.output_tokens
                yield kb_pb.RetrieveEvent(
                    token=kb_pb.RetrieveTokenEvent(content=ev.answer)
                )
            elif isinstance(ev, StreamSourcesEvent):
                final_sources = ev.sources
                source_chunks = [
                    kb_pb.SourceChunk(
                        doc_id=str(s.get("doc_id", "")),
                        file_name=str(s.get("file_name", "")),
                        page=int(s.get("page", 0) or 0),
                        content=str(s.get("content", "")),
                        score=float(s.get("score", 0.0) or 0.0),
                    )
                    for s in ev.sources
                ]
                yield kb_pb.RetrieveEvent(
                    sources=kb_pb.RetrieveSourcesEvent(sources=source_chunks)
                )
            elif isinstance(ev, StreamDoneEvent):
                final_input_tokens = ev.input_tokens
                final_output_tokens = ev.output_tokens
                yield kb_pb.RetrieveEvent(
                    done=kb_pb.RetrieveDoneEvent(
                        input_tokens=ev.input_tokens,
                        output_tokens=ev.output_tokens,
                        session_id=ev.session_id,
                    )
                )

        # 9. Persist assistant message + Redis cache (mirror Query).
        await self._persist_assistant(
            tenant_id=tenant_id, session_id=session_id,
            answer=answer, sources=final_sources,
            input_tokens=final_input_tokens, output_tokens=final_output_tokens,
            cache=cache,
        )

        # Q&A audit (mirror Query): record the streamed turn's content into
        # the operation history. Best-effort — the stream is already complete.
        await self._record_query_audit(
            context=context,
            tenant_id=tenant_id,
            kb_id=kb_id,
            session_id=session_id,
            question=request.question,
            answer=answer,
            sources=final_sources,
            input_tokens=final_input_tokens,
            output_tokens=final_output_tokens,
        )

    async def _persist_assistant(
        self, *, tenant_id: str, session_id: str,
        answer: str, sources: list[dict[str, Any]],
        input_tokens: int, output_tokens: int, cache: Any,
    ) -> None:
        """Persist assistant message to DB + Redis (shared by Query and Retrieve)."""
        if self._pool is not None:
            async with self._pool.acquire() as conn:
                await message_repo.insert_message(
                    conn,
                    tenant_id=tenant_id,
                    session_id=session_id,
                    role="assistant",
                    content=answer,
                    source_chunks=sources,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                )
        if cache is not None:
            await cache.append_message(
                session_id=session_id,
                role="assistant",
                content=answer,
                sources=sources,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )

    async def _record_query_audit(
        self, *, context: Any, tenant_id: str, kb_id: str, session_id: str,
        question: str, answer: str, sources: list[dict[str, Any]],
        input_tokens: int, output_tokens: int,
    ) -> None:
        """Audit one Q&A turn into kb_audit_log (action ``kb.query``).

        The operation history previously never recorded Q&A — the turn's
        question/answer (truncated) plus token usage now land in the KB
        audit trail so ListKBAuditLogs shows Q&A activity. Best-effort:
        audit write failures are logged, never propagated — a Query /
        Retrieve response must not fail because of the audit trail (the
        Q&A itself is already persisted to kb_messages by _persist_assistant).
        """
        if self._pool is None:
            return
        try:
            after_state = {
                "session_id": session_id,
                "question": _audit_text_truncate(question),
                "answer": _audit_text_truncate(answer),
                "source_count": len(sources),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        action="kb.query",
                        actor_user_id=_actor_user_id(context),
                        before_state=None,
                        after_state=after_state,
                    )
        except Exception as e:  # noqa: BLE001 — best-effort audit
            logger.warning(
                "kb-service: failed to persist query audit (kb_id=%s "
                "session_id=%s): %s %s", kb_id, session_id, e,
                corr(request_id=_request_id(context), kb_id=kb_id),
            )

    async def _on_query_failure(
        self, *, context: Any, tenant_id: str, kb_id: str,
        session_id: str, question: str, cache: Any, error: Exception,
    ) -> None:
        """Best-effort failure trail for a failed Q&A turn (Query + Retrieve).

        A failed turn previously left a half-record: the user message was
        already persisted (steps 3-4) but no assistant message and no audit
        row ever appeared — the session detail showed an unanswered
        question and the operation history showed nothing at all. Both
        gaps are filled here (never raising):
          - a role='assistant' error placeholder message in kb_messages +
            Redis — the session detail shows WHY there is no answer and
            the multi-turn history stays complete;
          - a ``kb.query`` audit row with error_code ``QA_FAILED`` + the
            error text (operation-history visibility, mirrors the parse
            result audit's PARSE_FAILED).
        The caller re-raises the original exception afterwards, so the
        RPC error semantics are unchanged.
        """
        error_text = str(error) or repr(error)
        await self._persist_assistant_error(
            tenant_id=tenant_id,
            session_id=session_id,
            error_message=error_text,
            cache=cache,
        )
        await self._record_query_failure_audit(
            context=context,
            tenant_id=tenant_id,
            kb_id=kb_id,
            session_id=session_id,
            question=question,
            error_message=error_text,
        )

    async def _persist_assistant_error(
        self, *, tenant_id: str, session_id: str,
        error_message: str, cache: Any,
    ) -> None:
        """Persist an assistant error placeholder (best-effort, never raises).

        The placeholder content is user-facing (session detail) and feeds
        the next turn's chat history; the raw error text is truncated to
        the same 500-char cap used by the audit trail.

        The raw error text (e.g. an AioRpcError repr full of ``<...>``
        angle brackets) is wrapped in a fenced code block: chat frontends
        render assistant messages as markdown/HTML, and a bare ``<...>``
        sequence would be swallowed as an HTML tag — hiding the failure
        reason. Inside a code fence the text is displayed verbatim.
        """
        content = (
            "回答生成失败：\n```\n"
            + _audit_text_truncate(error_message)
            + "\n```"
        )
        try:
            if self._pool is not None:
                async with self._pool.acquire() as conn:
                    await message_repo.insert_message(
                        conn,
                        tenant_id=tenant_id,
                        session_id=session_id,
                        role="assistant",
                        content=content,
                        source_chunks=[],
                        input_tokens=0,
                        output_tokens=0,
                    )
            if cache is not None:
                await cache.append_message(
                    session_id=session_id,
                    role="assistant",
                    content=content,
                    sources=[],
                    input_tokens=0,
                    output_tokens=0,
                )
        except Exception as e:  # noqa: BLE001 — best-effort placeholder
            logger.warning(
                "kb-service: failed to persist assistant error placeholder "
                "(session_id=%s): %s", session_id, e,
            )

    async def _record_query_failure_audit(
        self, *, context: Any, tenant_id: str, kb_id: str,
        session_id: str, question: str, error_message: str,
    ) -> None:
        """Audit a failed Q&A turn (action ``kb.query``, QA_FAILED).

        Mirrors _record_query_audit but records the failure: answer is
        empty, source_count/tokens are 0, and the error text rides in
        error_msg (visible in the operation history's failure display).
        Best-effort — never raises.
        """
        if self._pool is None:
            return
        try:
            after_state = {
                "session_id": session_id,
                "question": _audit_text_truncate(question),
                "answer": "",
                "source_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
            }
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        action="kb.query",
                        actor_user_id=_actor_user_id(context),
                        before_state=None,
                        after_state=after_state,
                        error_code="QA_FAILED",
                        error_msg=_audit_text_truncate(error_message),
                    )
        except Exception as e:  # noqa: BLE001 — best-effort audit
            logger.warning(
                "kb-service: failed to persist query failure audit "
                "(kb_id=%s session_id=%s): %s %s", kb_id, session_id, e,
                corr(request_id=_request_id(context), kb_id=kb_id),
            )

    # ── Plan step 8A: new path helpers (flag=true) ───────────────────────────

    async def _load_history(
        self,
        *,
        tenant_id: str,
        session_id: str,
        cache: Any,
        limit: int = 20,
    ) -> list[dict[str, str]]:
        """Load chat history for the current session (Plan step 8A).

        Priority: Redis cache (``LRANGE key -limit -1`` → most recent N in
        chronological order, matching legacy ChatMemoryBuffer token_limit
        behavior) → fallback to DB ``list_session_messages`` (oldest N).

        The current-turn user message was already persisted to Redis (step
        3-4 of ``_query``) BEFORE this call, so the Redis history INCLUDES
        the current-turn user. The Generate RPC appends ``question`` as the
        final USER message, so the current-turn user appears twice — this
        matches the legacy ContextChatEngine behavior and is intentional.

        Returns a list of ``{role, content}`` dicts in chronological order
        (oldest-first), ready to pass as ``history`` to Generate RPC.
        """
        # 1. Try Redis first (most recent N, chronological order).
        if cache is not None:
            msgs = await cache.list_recent_messages(
                session_id=session_id, limit=limit
            )
            if msgs:
                return [
                    {"role": str(m.get("role", "")), "content": str(m.get("content", ""))}
                    for m in msgs
                ]

        # 2. Fallback to DB (oldest N by created_at ASC).
        if self._pool is not None:
            async with self._pool.acquire() as conn:
                rows = await message_repo.list_session_messages(
                    conn,
                    tenant_id=tenant_id,
                    session_id=session_id,
                    limit=limit,
                )
            return [
                {"role": r.get("role", ""), "content": r.get("content", "")}
                for r in rows
            ]

        return []

    async def _query_new_path(
        self,
        *,
        tenant_id: str,
        kb_id: str,
        question: str,
        session_id: str,
        top_k: int,
        score_threshold: float,
        retrieval_mode: str,
        inference_service_name: str,
        vector_store_id: str,
        embedding_model: str = "",
        cache: Any,
        context: Any,
    ) -> QueryResult:
        """QueryOrchestrator: retrieve → gates → Generate RPC.

        Returns a ``QueryResult`` dataclass (answer, sources, session_id,
        input_tokens, output_tokens).
        """
        from app.services.query_orchestrator import QueryOrchestrator

        # 1. Load chat history (includes current-turn user, already persisted).
        history = await self._load_history(
            tenant_id=tenant_id,
            session_id=session_id,
            cache=cache,
        )

        # 2. Build or reuse the per-tenant orchestrator.
        #    Each tenant gets its own QueryOrchestrator backed by a
        #    tenant-scoped RetrieveService. This preserves multi-tenant isolation.
        orch = self._orchestrators.get(tenant_id)
        if orch is None:
            if self._retrieve_service_factory is not None:
                retrieve_service = self._retrieve_service_factory(tenant_id)
            else:
                retrieve_service = _default_retrieve_service(tenant_id, self._pool)

            if self._rag_engine_grpc_client_factory is not None:
                rag_engine_grpc = self._rag_engine_grpc_client_factory()
            else:
                rag_engine_grpc = _default_rag_engine_grpc_client()

            orch = QueryOrchestrator(
                retrieve_service=retrieve_service,
                rag_engine_client=rag_engine_grpc,
            )
            self._orchestrators[tenant_id] = orch

        # 3. Resolve per-model runtime endpoints. The embedding model and the
        #    chat model are two distinct inference services with distinct
        #    endpoints; resolve each from its own model name so the embed and
        #    generate legs connect to the right service. The Q&A path is
        #    strict: a missing/unresolvable model aborts instead of silently
        #    falling back to the rag-engine server default (三级回落链
        #    request → KB 默认 → 报错).
        embed_endpoint, embed_model = await self._resolve_required_runtime_endpoint(
            tenant_id=tenant_id, model=embedding_model, role="embedding",
            context=context,
        )
        generate_endpoint, generate_model = await self._resolve_required_runtime_endpoint(
            tenant_id=tenant_id, model=inference_service_name, role="chat",
            context=context,
        )

        # 4. Run the orchestrator.
        result = await orch.query(
            tenant_id=tenant_id,
            kb_id=kb_id,
            question=question,
            session_id=session_id,
            top_k=top_k,
            score_threshold=score_threshold,
            retrieval_mode=retrieval_mode,
            inference_service_name=generate_model or inference_service_name,
            vector_store_id=vector_store_id,
            embedding_model=embed_model or embedding_model,
            history=history,
            embed_runtime_endpoint=embed_endpoint,
            generate_runtime_endpoint=generate_endpoint,
        )

        return result

    # ── B2 servicers (kb-api-completion, issue-045) ────────────────────────────

    def ListDocumentChunks(self, request, context):
        return _run_async(self._list_document_chunks(request, context))

    async def _list_document_chunks(
        self, request, context
    ) -> kb_pb.ListDocumentChunksResponse:
        """ListDocumentChunks — keyset-paged chunk details (SPEC §5.1 #11).

        get_kb gate → get_document (soft-delete filtered) →
        list_chunks_by_doc_paged (ORDER BY id ASC, id > $cursor).
        """
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        # limit 1–100 (default 50), chunk_type whitelist (SPEC §5.2).
        limit = request.page.limit or 50
        if not 1 <= limit <= 100:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "limit must be between 1 and 100"
            )
            return
        allowed_chunk_types = {"child", "parent", "doc_summary"}
        if request.chunk_type and request.chunk_type not in allowed_chunk_types:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"chunk_type must be one of {sorted(allowed_chunk_types)}",
            )
            return
        cursor = request.page.cursor or None

        async with self._pool.acquire() as conn:
            kb_row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
            if not kb_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                return
            doc_row = await document_repo.get_document(
                conn,
                tenant_id=request.tenant_id,
                kb_id=request.kb_id,
                doc_id=request.doc_id,
            )
            if not doc_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "document not found")
                return
            try:
                rows = await chunk_repo.list_chunks_by_doc_paged(
                    conn,
                    tenant_id=request.tenant_id,
                    kb_id=request.kb_id,
                    doc_id=request.doc_id,
                    chunk_type=request.chunk_type or None,
                    limit=limit,
                    cursor=cursor,
                )
            except InvalidCursorError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "invalid page cursor"
                )
                return
        items = [_chunk_row_to_pb(r) for r in rows]
        next_cursor = str(rows[-1]["id"]) if rows and len(rows) >= limit else ""
        return kb_pb.ListDocumentChunksResponse(items=items, next_cursor=next_cursor)

    def GetSessionMessages(self, request, context):
        return _run_async(self._get_session_messages(request, context))

    async def _get_session_messages(
        self, request, context
    ) -> kb_pb.GetSessionMessagesResponse:
        """GetSessionMessages — session message replay (SPEC §5.1 #17).

        get_kb gate → get_session ownership check (session must belong to the
        path kb_id → NOT_FOUND otherwise) → list_session_messages_paged
        (ORDER BY created_at ASC, id ASC composite keyset).
        """
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        limit = request.page.limit or 100
        if not 1 <= limit <= 100:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "limit must be between 1 and 100"
            )
            return
        cursor = request.page.cursor or None

        async with self._pool.acquire() as conn:
            kb_row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
            if not kb_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                return
            session_row = await message_repo.get_session(
                conn,
                tenant_id=request.tenant_id,
                kb_id=request.kb_id,
                session_id=request.session_id,
            )
            if not session_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "session not found")
                return
            try:
                rows = await message_repo.list_session_messages_paged(
                    conn,
                    tenant_id=request.tenant_id,
                    session_id=request.session_id,
                    limit=limit,
                    cursor=cursor,
                )
            except InvalidCursorError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "invalid page cursor"
                )
                return
        items = [_session_message_row_to_pb(r) for r in rows]
        next_cursor = (
            _message_cursor(rows[-1]) if rows and len(rows) >= limit else ""
        )
        return kb_pb.GetSessionMessagesResponse(items=items, next_cursor=next_cursor)

    def DeleteSession(self, request, context):
        return _run_async(self._delete_session(request, context))

    async def _delete_session(self, request, context) -> empty_pb2.Empty:
        """DeleteSession — idempotent session removal (SPEC §5.1 #18).

        get_kb gate → single-transaction delete (messages then session with
        kb_id ownership check) → after commit, best-effort cache delete. A
        missing session still returns Empty (idempotent 204); only a missing
        KB returns NOT_FOUND.

        Audit: session.delete is a hard, irreversible delete of the session
        AND its messages, so a successful deletion records a session.delete
        audit row in the same transaction — before_state carries the session
        summary + message_count (the only surviving record of what was
        destroyed), after_state marks it gone. A missing-session no-op
        (idempotent retry) records nothing, mirroring doc.delete's handling
        of repeated deletes.
        """
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return

        actor = _actor_user_id(context)
        async with self._pool.acquire() as conn:
            kb_row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
            if not kb_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                return
            # Read the pre-delete snapshot and count inside the same outer
            # transaction as the delete + audit so the audit row and the
            # destructive write commit atomically.
            async with conn.transaction():
                session_row = await message_repo.get_session(
                    conn,
                    tenant_id=request.tenant_id,
                    kb_id=request.kb_id,
                    session_id=request.session_id,
                )
                if session_row is None:
                    # Missing session: idempotent no-op — no audit row, so a
                    # retried delete does not accumulate duplicate history.
                    return empty_pb2.Empty()
                message_count = await message_repo.count_session_messages(
                    conn,
                    tenant_id=request.tenant_id,
                    session_id=request.session_id,
                )
                before_state = {
                    "session_id": request.session_id,
                    "title": session_row.get("title") or "",
                    "user_id": str(session_row.get("user_id") or ""),
                    "message_count": message_count,
                    "created_at": _cursor_ts(session_row.get("created_at")),
                }
                # delete_session opens its own nested transaction (a
                # savepoint under the outer one).
                deleted = await message_repo.delete_session(
                    conn,
                    tenant_id=request.tenant_id,
                    kb_id=request.kb_id,
                    session_id=request.session_id,
                )
                if deleted:
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=request.tenant_id,
                        kb_id=request.kb_id,
                        action="session.delete",
                        actor_user_id=actor,
                        before_state=before_state,
                        after_state={"session_id": request.session_id, "deleted": True},
                    )

        # Best-effort Redis DEL after the DB transaction commits (SPEC §5.1 #18
        # step 4): on failure the 24h TTL expires the stale entries anyway.
        cache = None
        try:
            cache = self._session_cache_factory()
        except Exception as e:  # noqa: BLE001 — best-effort cache
            logger.warning("session cache factory failed: %s", e)
        if cache is not None:
            try:
                await cache.delete_session(session_id=request.session_id)
            except Exception as e:  # noqa: BLE001 — best-effort cache
                logger.warning(
                    "session cache delete failed after DeleteSession "
                    "(TTL will expire): %s", e,
                )
        return empty_pb2.Empty()

    def ReparseDocument(self, request, context):
        return _run_async(self._reparse_document(request, context))

    async def _reparse_document(
        self, request, context
    ) -> common_pb2.AsyncTaskRef:
        """ReparseDocument — reset + re-enqueue parse (SPEC §5.1 #12, issue-047).

        Structurally identical to _complete_document_upload's atomic outbox
        transaction (kb_documents reset + kb_operations + outbox_events), with
        two deliberate divergences:
        - the idempotency key is the client-supplied request.idempotency_key
          (the proto has the field; notify synthesizes one because its proto
          does not);
        - the outbox payload's storage_path/file_name come from the DB
          document row, not the request (reparse has no upload request);
          notify's file_name is always "" while reparse carries the real one.

        Guards: KB missing/rebuilding → NOT_FOUND/FAILED_PRECONDITION; doc
        missing/soft-deleted/ready → NOT_FOUND/FAILED_PRECONDITION. The
        parse_orchestrator's built-in re-entrant cleanup (delete_chunks_by_doc
        + best-effort Core vector delete) removes the old chunks downstream —
        no code change below this layer.
        """
        if not request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required"
            )
            return
        try:
            uuid.UUID(request.idempotency_key.strip())
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key must be a uuid"
            )
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return

        tenant_id = request.tenant_id or ""
        kb_id = request.kb_id or ""
        doc_id = request.doc_id or ""
        if not tenant_id or not kb_id or not doc_id:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "tenant_id, kb_id and doc_id are required",
            )
            return
        idem_key = request.idempotency_key

        # audit doc.reparse (plan §6.3): intent for the failure path; 404 /
        # FAILED_PRECONDITION business rejections (KB missing/rebuilding,
        # doc missing/ready, failed-task-key) are recorded — the KB gate
        # passing means the kb_audit_log.kb_id FK is satisfiable, and the
        # KB-missing abort is swallowed by _record_failure_audit (no KB
        # row). INVALID_ARGUMENT aborts above, before this try block.
        actor = _actor_user_id(context)
        intent: dict[str, Any] = {"doc_id": doc_id, "kb_id": kb_id}
        try:
            async with self._pool.acquire() as conn:
                # 1. idempotent replay: pending/completed task with the same key
                #    → return the same AsyncTaskRef (a failed task is NOT
                #    replayed — the client must submit a fresh key, SPEC §5.4).
                existing = await operation_repo.find_by_idempotency_key(
                    conn,
                    tenant_id=tenant_id,
                    idempotency_key=idem_key,
                    operation_type="kb.reparse",
                )
                if existing and existing.get("status") in ("pending", "completed"):
                    return common_pb2.AsyncTaskRef(
                        task_id=str(existing["id"]),
                        task_type=existing.get("operation_type") or "kb.reparse",
                        status=existing.get("status") or "pending",
                        location_url="",
                    )

                # 2. KB gate: missing (or tenant-filter-hidden) → NOT_FOUND; rebuilding →
                #    FAILED_PRECONDITION (mutually exclusive with a PUT config
                #    triggered rebuild).
                kb_row = await kb_repo.get_kb(
                    conn, tenant_id=tenant_id, kb_id=kb_id
                )
                if not kb_row:
                    context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                    return
                if kb_row.get("status") == "rebuilding":
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "knowledge base is rebuilding",
                    )
                    return

                # 3. document gate: missing/soft-deleted → NOT_FOUND; ready →
                #    FAILED_PRECONDITION (guard against accidental re-parse of a
                #    healthy doc; no force field in the contract — YAGNI).
                doc_row = await document_repo.get_document(
                    conn, tenant_id=tenant_id, kb_id=kb_id, doc_id=doc_id
                )
                if not doc_row:
                    context.abort(grpc.StatusCode.NOT_FOUND, "document not found")
                    return
                if doc_row.get("parse_status") == "ready":
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "document is ready; reparse is for failed documents",
                    )
                    return

                # 4. single transaction: reset doc row + kb_operations + outbox_events
                #    (same atomic shape as CompleteDocumentUpload; repo helpers
                #    here do not open their own transactions).
                from app.repositories import outbox as outbox_repo

                async with conn.transaction():
                    updated = await document_repo.reset_for_reparse_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        doc_id=doc_id,
                    )
                    if not updated:
                        context.abort(grpc.StatusCode.NOT_FOUND, "document not found")
                        return  # unreachable; for type checkers

                    # UNIQUE race self-heal (same pattern as _update_kb /
                    # _complete_document_upload): a concurrent reparse with the
                    # same idempotency key can win the INSERT between the
                    # replay check above and here. The nested transaction
                    # (SAVEPOINT) keeps the outer tx usable after the violated
                    # constraint aborts the inner one; re-lookup then either
                    # replays the winner's task or rejects the retry.
                    publish_event = True
                    task_id = ""
                    try:
                        async with conn.transaction():
                            task_row = await operation_repo.create_operation_in_tx(
                                conn,
                                tenant_id=tenant_id,
                                idempotency_key=idem_key,
                                operation_type="kb.reparse",
                                resource_type="kb_document",
                                resource_id=doc_id,
                                payload={
                                    "doc_id": doc_id,
                                    "kb_id": kb_id,
                                    "object_id": doc_row.get("object_id") or "",
                                },
                                status="pending",
                            )
                        task_id = str(task_row["id"])
                    except asyncpg.UniqueViolationError:
                        existing = await operation_repo.find_by_idempotency_key(
                            conn,
                            tenant_id=tenant_id,
                            idempotency_key=idem_key,
                            operation_type="kb.reparse",
                        )
                        if existing is None:
                            # A concurrent writer raced the row away between
                            # INSERT and SELECT; surface as UNKNOWN rather
                            # than masking it.
                            raise
                        if existing.get("status") == "failed":
                            # SPEC §5.4: a failed task must NOT be replayed on
                            # the same key — the client must submit a fresh
                            # idempotency_key. abort() rolls back the whole
                            # outer transaction (including the doc reset),
                            # so the failed state stays intact.
                            context.abort(
                                grpc.StatusCode.FAILED_PRECONDITION,
                                "task already failed with this idempotency_key; "
                                "retry with a new key",
                            )
                            return  # unreachable; for type checkers
                        # pending/completed: the concurrent winner's tx already
                        # published the outbox event — replay it, publish nothing.
                        task_id = str(existing["id"])
                        publish_event = False

                    # audit doc.reparse (plan §6.3) — same transaction as the
                    # reset, written AFTER the task row so after_state can
                    # link task_id: the parse consumer later flips THIS row in
                    # place at terminal parse_status (update_parse_result_in_tx)
                    # instead of inserting a second result row, so the
                    # operation history shows one entry per operation.
                    # before = the row as it stood (the failed state the gate
                    # validated); after = the row with the reset parse
                    # lifecycle (pending, error/parsed_at cleared, chunk_count
                    # 0 — mirrors reset_for_reparse_in_tx) + the task link.
                    after_row = dict(doc_row)
                    after_row["parse_status"] = "pending"
                    after_row["error_message"] = None
                    after_row["chunk_count"] = 0
                    after_snapshot = _doc_audit_snapshot(after_row) or {}
                    after_snapshot["task_id"] = task_id
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        action="doc.reparse",
                        actor_user_id=actor,
                        before_state=_doc_audit_snapshot(doc_row),
                        after_state=after_snapshot,
                    )

                    if publish_event:
                        # Payload mirrors the notify template; storage_path/file_name
                        # come from the DB row (reparse has no request-side values).
                        await outbox_repo.insert_event(
                            conn,
                            tenant_id=tenant_id,
                            aggregate_type="kb_documents",
                            aggregate_id=doc_id,
                            event_type="kb.reparse",
                            payload={
                                "doc_id": doc_id,
                                "kb_id": kb_id,
                                "storage_path": doc_row.get("storage_path") or "",
                                "tenant_id": tenant_id,
                                "file_name": doc_row.get("file_name") or "",
                                "object_id": doc_row.get("object_id") or "",
                                "chunk_size": kb_row.get("chunk_size") or 1024,
                                # Lets the parse consumer close this kb_operations
                                # row when the doc reaches a terminal parse_status
                                # (prevents tasks stuck pending forever).
                                "task_id": task_id,
                            },
                        )
        except Exception:
            # failure audit doc.reparse (plan §6.3): the business rejections
            # listed above are recorded; the replay branch returns (not an
            # exception) and the failed-key UNIQUE abort rolls the business
            # tx back before the audit lands in its own fresh transaction.
            info = _audit_failure_info(context)
            if info is not None:
                await _record_failure_audit(
                    self._pool,
                    tenant_id=tenant_id,
                    kb_id=kb_id,
                    action="doc.reparse",
                    intent=intent,
                    error_code=info[0],
                    error_msg=info[1],
                    actor_user_id=actor,
                )
            raise

        return common_pb2.AsyncTaskRef(
            task_id=task_id,
            task_type="kb.reparse",
            status="pending",
            location_url="",
        )

    async def _trigger_rebuild_in_tx(
        self,
        conn: asyncpg.Connection,
        context,
        *,
        tenant_id: str,
        kb_id: str,
        kb_row: dict[str, Any],
        idem_key: str,
        actor: str | None,
    ) -> common_pb2.AsyncTaskRef:
        """Same-transaction rebuild orchestration shared by RebuildKB (B6,
        P1 #24) and UpdateKBConfig (B7, P1 #23): the conditional
        active→rebuilding status transition (the authoritative mutual
        exclusion), the kb.rebuild audit row, the pending kb.rebuild async
        task (SAVEPOINT UNIQUE-race self-heal), and the kb.rebuild outbox
        event.

        Must run INSIDE the caller's outer transaction — every write here
        commits or rolls back with the caller's own writes (UpdateKBConfig
        pairs this with its config UPDATE so a failed trigger rolls the
        config change back too). ``kb_row`` supplies the rebuild payload:
        RebuildKB passes the gate-read row, UpdateKBConfig passes the row
        RETURNed by update_config_in_tx (the NEW embedding settings the
        rebuild must apply). Any abort() here rolls the whole caller
        transaction back before the failure audit lands in its own tx.
        """
        from app.repositories import outbox as outbox_repo

        # Conditional active→rebuilding: the authoritative mutual
        # exclusion. A concurrent rebuild (or any state change between
        # the caller's read and here) makes this UPDATE match 0 rows →
        # FAILED_PRECONDITION.
        transitioned = await kb_repo.set_status_in_tx(
            conn,
            tenant_id=tenant_id,
            kb_id=kb_id,
            from_status="active",
            to_status="rebuilding",
        )
        if not transitioned:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "knowledge base is rebuilding or not active",
            )
            return  # unreachable; for type checkers

        # audit kb.rebuild (plan §6.3) — same transaction as the status
        # transition. before = the active state the gate validated;
        # after = rebuilding.
        await audit_repo.insert_audit_in_tx(
            conn,
            tenant_id=tenant_id,
            kb_id=kb_id,
            action="kb.rebuild",
            actor_user_id=actor,
            before_state={"status": "active"},
            after_state={"status": "rebuilding"},
        )

        # UNIQUE race self-heal (same pattern as _reparse_document): a
        # concurrent rebuild with the same idempotency key can win the
        # INSERT between the caller's replay check and here. The nested
        # SAVEPOINT keeps the outer tx usable; the loser either replays
        # the winner's task (pending/completed) or rejects the poisoned
        # key (failed).
        publish_event = True
        task_id = ""
        try:
            async with conn.transaction():
                task_row = await operation_repo.create_operation_in_tx(
                    conn,
                    tenant_id=tenant_id,
                    idempotency_key=idem_key,
                    operation_type="kb.rebuild",
                    resource_type="knowledge_base",
                    resource_id=kb_id,
                    payload={
                        "kb_id": kb_id,
                        "embedding_model": kb_row.get("embedding_model") or "",
                        "chunk_size": kb_row.get("chunk_size") or 1024,
                    },
                    status="pending",
                )
            task_id = str(task_row["id"])
        except asyncpg.UniqueViolationError:
            existing = await operation_repo.find_by_idempotency_key(
                conn,
                tenant_id=tenant_id,
                idempotency_key=idem_key,
                operation_type="kb.rebuild",
            )
            if existing is None:
                # A concurrent writer raced the row away between INSERT and
                # SELECT; surface as UNKNOWN rather than masking.
                raise
            if existing.get("status") == "failed":
                # SPEC §5.4: a failed task must NOT be replayed on the
                # same key — abort() rolls back the whole outer
                # transaction INCLUDING the rebuilding transition, so the
                # KB returns to active and the failed state stays intact.
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "task already failed with this idempotency_key; "
                    "retry with a new key",
                )
                return  # unreachable; for type checkers
            # pending/completed: the concurrent winner's tx already
            # transitioned + published — replay, publish nothing.
            task_id = str(existing["id"])
            publish_event = False

        if publish_event:
            await outbox_repo.insert_event(
                conn,
                tenant_id=tenant_id,
                aggregate_type="knowledge_bases",
                aggregate_id=kb_id,
                event_type="kb.rebuild",
                payload={
                    "kb_id": kb_id,
                    "tenant_id": tenant_id,
                    # Lets the rebuild consumer close the
                    # kb_operations row (terminal status) —
                    # prevents rows stuck pending.
                    "task_id": task_id,
                },
            )

        return common_pb2.AsyncTaskRef(
            task_id=task_id,
            task_type="kb.rebuild",
            status="pending",
            location_url="",
        )

    # ── P1 RPC: full-KB rebuild (B6, P1 #24) ──────────────────────────────────

    def RebuildKB(self, request, context):
        return _run_async(self._rebuild_kb(request, context))

    async def _rebuild_kb(
        self, request, context
    ) -> common_pb2.AsyncTaskRef:
        """RebuildKB — full-KB re-parse of all ready/failed docs (P1 #24).

        202-style async semantics, structurally mirroring _reparse_document's
        atomic outbox transaction with three divergences:
        - the aggregate is the KB, not a document: the conditional
          active→rebuilding status transition (set_status_in_tx) IS the
          mutual exclusion — a second concurrent rebuild finds status=
          'rebuilding' and aborts FAILED_PRECONDITION;
        - no per-document reset here: the rebuild consumer (rebuild_consumer)
          snapshots doc ids and resets each document just before re-parsing
          it (reset_for_reparse_in_tx), so queries keep being served from
          the existing index while the rebuild runs;
        - the outbox event type 'kb.rebuild' is routed by the dispatcher
          to the dedicated rebuild subject (subject_overrides).

        Guards: KB missing → NOT_FOUND; KB already rebuilding →
        FAILED_PRECONDITION (both the gate read and the conditional UPDATE
        race window are covered — the loser's UPDATE matches 0 rows).
        """
        if not request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required"
            )
            return
        try:
            uuid.UUID(request.idempotency_key.strip())
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key must be a uuid"
            )
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return

        tenant_id = request.tenant_id or ""
        kb_id = request.kb_id or ""
        if not tenant_id or not kb_id:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "tenant_id and kb_id are required",
            )
            return
        idem_key = request.idempotency_key

        # audit kb.rebuild (plan §6.3): intent for the failure path; the
        # KB gate passing means the kb_audit_log.kb_id FK is satisfiable.
        actor = _actor_user_id(context)
        intent: dict[str, Any] = {"kb_id": kb_id}
        try:
            async with self._pool.acquire() as conn:
                # 1. idempotent replay: pending/completed task with the same
                #    key → return the same AsyncTaskRef (a failed task is NOT
                #    replayed — the client submits a fresh key, SPEC §5.4).
                existing = await operation_repo.find_by_idempotency_key(
                    conn,
                    tenant_id=tenant_id,
                    idempotency_key=idem_key,
                    operation_type="kb.rebuild",
                )
                if existing and existing.get("status") in ("pending", "completed"):
                    return common_pb2.AsyncTaskRef(
                        task_id=str(existing["id"]),
                        task_type=existing.get("operation_type") or "kb.rebuild",
                        status=existing.get("status") or "pending",
                        location_url="",
                    )

                # 2. KB gate: missing (or tenant-filter-hidden) → NOT_FOUND. The
                #    rebuilding check happens in the conditional UPDATE below
                #    (authoritative) — this read is only the fast path.
                kb_row = await kb_repo.get_kb(
                    conn, tenant_id=tenant_id, kb_id=kb_id
                )
                if not kb_row:
                    context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                    return

                # 3. single transaction: KB status transition + audit +
                #    kb_operations + outbox_events (atomic outbox shape) —
                #    _trigger_rebuild_in_tx owns the whole three-write
                #    orchestration (shared with UpdateKBConfig, B7 #23).
                async with conn.transaction():
                    return await self._trigger_rebuild_in_tx(
                        conn,
                        context,
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        kb_row=kb_row,
                        idem_key=idem_key,
                        actor=actor,
                    )
        except Exception:
            # failure audit kb.rebuild (plan §6.3): business rejections are
            # recorded; the replay branch returns (not an exception) and the
            # failed-key abort rolls the business tx back before the audit
            # lands in its own fresh transaction.
            info = _audit_failure_info(context)
            if info is not None:
                await _record_failure_audit(
                    self._pool,
                    tenant_id=tenant_id,
                    kb_id=kb_id,
                    action="kb.rebuild",
                    intent=intent,
                    error_code=info[0],
                    error_msg=info[1],
                    actor_user_id=actor,
                )
            raise

        # unreachable: both success paths return inside the try block
        # (replay → the recorded task, fresh → _trigger_rebuild_in_tx).

    def ListKBCitations(self, request, context):
        return _run_async(self._list_kb_citations(request, context))

    async def _list_kb_citations(
        self, request, context
    ) -> kb_pb.ListKBCitationsResponse:
        """ListKBCitations — expand source_chunks into citations (SPEC §5.1 #15).

        Pages assistant messages with non-empty source_chunks (message
        granularity), then expands each message's JSON in Python: per (message,
        doc) group the citation takes the highest-score source. The citation
        id is uuid5-derived — deterministic, replay-stable, uuid-formatted.
        """
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        limit = request.page.limit or 20
        if not 1 <= limit <= 100:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "limit must be between 1 and 100"
            )
            return
        cursor = request.page.cursor or None

        async with self._pool.acquire() as conn:
            kb_row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
            if not kb_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                return
            try:
                rows = await message_repo.list_citation_messages_paged(
                    conn,
                    tenant_id=request.tenant_id,
                    kb_id=request.kb_id,
                    limit=limit,
                    cursor=cursor,
                )
            except InvalidCursorError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "invalid page cursor"
                )
                return

        items: list[kb_pb.KBCitation] = []
        for row in rows:
            message_id = str(row["id"])
            session_id = str(row["session_id"])
            created_at = row["created_at"]
            # Malformed JSON on one message must not blow up the whole page
            # (SPEC §5.4): skip the message instead.
            try:
                sources = json.loads(row["source_chunks"])
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(sources, list) or not sources:
                continue
            # Group by doc_id keeping the highest-score source per doc.
            best: dict[str, dict[str, Any]] = {}
            for src in sources:
                if not isinstance(src, dict):
                    continue
                doc_id = str(src.get("doc_id", ""))
                if not doc_id:
                    continue
                # Non-numeric score/page on one source must not blow up the
                # whole page (SPEC §5.4): skip the source instead.
                try:
                    score = float(src.get("score", 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
                if doc_id not in best or score > float(best[doc_id].get("score", 0.0) or 0.0):
                    best[doc_id] = src
            for doc_id, src in best.items():
                try:
                    page = int(src.get("page", 0) or 0)
                    score = float(src.get("score", 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
                items.append(
                    kb_pb.KBCitation(
                        id=str(uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"ani:kb:citation:{request.kb_id}:{message_id}:{doc_id}",
                        )),
                        kb_id=request.kb_id,
                        doc_id=doc_id,
                        file_name=str(src.get("file_name", "")),
                        page=page,
                        content=str(src.get("content", "")),
                        score=score,
                        created_at=_ts(created_at),
                        message_id=message_id,
                        session_id=session_id,
                    )
                )
        next_cursor = (
            _message_cursor(rows[-1]) if rows and len(rows) >= limit else ""
        )
        return kb_pb.ListKBCitationsResponse(items=items, next_cursor=next_cursor)

    def ListKBSessions(self, request, context):
        return _run_async(self._list_kb_sessions(request, context))

    async def _list_kb_sessions(
        self, request, context
    ) -> kb_pb.ListKBSessionsResponse:
        """ListKBSessions — aggregated session list (SPEC §5.1 #16).

        get_kb gate → list_sessions aggregate SQL (message_count /
        last_active_at / last_query) → KBSession mapping with a composite
        keyset cursor (created_at DESC, id DESC).
        """
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        limit = request.page.limit or 20
        if not 1 <= limit <= 100:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "limit must be between 1 and 100"
            )
            return
        cursor = request.page.cursor or None

        async with self._pool.acquire() as conn:
            kb_row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
            if not kb_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                return
            try:
                rows = await message_repo.list_sessions(
                    conn,
                    tenant_id=request.tenant_id,
                    kb_id=request.kb_id,
                    limit=limit,
                    cursor=cursor,
                )
            except InvalidCursorError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "invalid page cursor"
                )
                return
        items = [
            kb_pb.KBSession(
                id=str(r["id"]),
                kb_id=request.kb_id,
                message_count=int(r.get("message_count") or 0),
                last_query=str(r.get("last_query") or ""),
                created_at=_ts(r.get("created_at")),
                last_active_at=_ts(r.get("last_active_at")),
            )
            for r in rows
        ]
        next_cursor = (
            _session_cursor(rows[-1]) if rows and len(rows) >= limit else ""
        )
        return kb_pb.ListKBSessionsResponse(items=items, next_cursor=next_cursor)

    # ── P1 RPC: permissions read/write pair (B4, plan §2.5) ────────────────────

    def GetKBPermissions(self, request, context):
        return _run_async(self._get_kb_permissions(request, context))

    async def _get_kb_permissions(self, request, context) -> kb_pb.KBPermissions:
        # 1. validate (read path: no idempotency)
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if not request.kb_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "kb_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return

        async with self._pool.acquire() as conn:
            # 2. KB existence check (tenant-filtered query, same semantics as
            # GetKB: a deleted KB is 404, not defaults)
            kb_row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
            if not kb_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                return
            # 3. read the permission row; no row → defaults (contract: not 404)
            perm = await permission_repo.get_permissions(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
        # 4. updated_at falls back to kb.created_at when no permission row
        # exists — avoids leaking the proto Timestamp zero value (1970).
        updated_at = perm.get("updated_at") or kb_row.get("created_at")
        return kb_pb.KBPermissions(
            kb_id=request.kb_id,
            public_read=bool(perm["public_read"]),
            allowed_user_ids=perm["allowed_user_ids"],
            updated_at=_ts(updated_at),
        )

    def GetKBConfig(self, request, context):
        return _run_async(self._get_kb_config(request, context))

    async def _get_kb_config(self, request, context) -> kb_pb.KBConfig:
        # 1. validate (read path: no idempotency)
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if not request.kb_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "kb_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return

        # 2. Tenant-filtered read; a deleted KB is 404 (same semantics as GetKB).
        async with self._pool.acquire() as conn:
            row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
        if not row:
            context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
            return

        # 3. column-direct mapping (B5 #22; retrieval_mode aligns the
        #    contract fix: vector | hybrid | keyword) — shared with
        #    UpdateKBConfig's response and replay paths (_kb_config_msg).
        return _kb_config_msg(request.tenant_id, request.kb_id, row)

    # ── P1 RPC: config update + paired rebuild (B7, P1 #23) ──────────────────

    def UpdateKBConfig(self, request, context):
        return _run_async(self._update_kb_config(request, context))

    async def _update_kb_config(
        self, request, context
    ) -> kb_pb.UpdateKBConfigResponse:
        """UpdateKBConfig — explicit-partial config update (B7, P1 #23).

        Tri-state request fields (proto3 optional + BoolValue): only
        fields explicitly carried in the request are change candidates;
        an absent field keeps its current value (unlike UpdateKB's
        COALESCE "empty keeps current"). A patch that changes nothing
        is 400 "no effective change". Changing embedding_model or
        chunk_size invalidates existing vectors, so the same transaction
        pairs the config UPDATE with a full-KB rebuild (via
        _trigger_rebuild_in_tx) — a failed trigger rolls the config
        change back. The rebuild's idempotency key is derived from the
        config key ("rebuild:<uuid>"), so a config replay can never
        re-trigger the rebuild either. 200 carries the new config plus
        the AsyncTaskRef of the paired rebuild (unset when none — the
        wire form of the contract's nullable rebuild_task).
        """
        # 1. validate idempotency_key / kb_id / tenant_id (align UpdateKB)
        if not request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required"
            )
            return
        try:
            uuid.UUID(request.idempotency_key.strip())
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key must be a uuid"
            )
            return
        if not request.kb_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "kb_id is required")
            return
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return

        # 2. value ranges on the explicitly-carried fields (align CreateKB;
        #    embedding_model is accepted as any non-empty string — the model
        #    catalog (ListKBModels) is deferred beyond P1).
        if request.HasField("chunk_size") and not (
            1 <= request.chunk_size <= 8192
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"chunk_size must be in [1, 8192], got {request.chunk_size}",
            )
            return
        if request.HasField("top_k") and not (1 <= request.top_k <= 20):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"top_k must be in [1, 20], got {request.top_k}",
            )
            return
        if request.HasField("score_threshold") and not (
            0 <= request.score_threshold <= 1
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"score_threshold must be in [0, 1], got {request.score_threshold}",
            )
            return
        if request.HasField("retrieval_mode") and request.retrieval_mode not in (
            "vector",
            "hybrid",
            "keyword",
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "retrieval_mode must be one of ['hybrid', 'keyword', 'vector'], "
                f"got {request.retrieval_mode}",
            )
            return
        if request.HasField("embedding_model") and not (
            request.embedding_model.strip()
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "embedding_model must not be empty"
            )
            return
        if self._pool is None:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured"
            )
            return

        tenant_id = request.tenant_id
        kb_id = request.kb_id
        idem_key = request.idempotency_key

        # 3. single transaction (same shape as UpdateKB): replay check,
        #    KB gate, config UPDATE, the kb.config.update audit, the
        #    kb.config.update idempotency record (insert + complete), and
        #    — when the embedding settings changed — the paired rebuild
        #    trigger, all commit atomically. A failed trigger (rebuilding
        #    race, poisoned rebuild key) rolls the config change back with
        #    everything else.
        actor = _actor_user_id(context)
        intent: dict[str, Any] = {"kb_id": kb_id}
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # 3a. idempotency replay: the recorded result carries the
                    # config row + the paired rebuild ref (None when the
                    # change did not touch the embedding settings).
                    existing = await operation_repo.find_by_idempotency_key(
                        conn,
                        tenant_id=tenant_id,
                        idempotency_key=idem_key,
                        operation_type="kb.config.update",
                    )
                    if existing and existing.get("result"):
                        result = existing["result"]
                        if isinstance(result, str):
                            result = json.loads(result)
                        # None → the field stays unset: the wire form of
                        # the contract's nullable rebuild_task.
                        rebuild_ref_pb = None
                        if result.get("rebuild_task"):
                            rt = result["rebuild_task"]
                            rebuild_ref_pb = common_pb2.AsyncTaskRef(
                                task_id=str(rt.get("task_id", "")),
                                task_type=str(rt.get("task_type", "kb.rebuild")),
                                status=str(rt.get("status", "pending")),
                                location_url=str(rt.get("location_url", "")),
                            )
                        return kb_pb.UpdateKBConfigResponse(
                            config=_kb_config_msg(tenant_id, kb_id, result["config"]),
                            rebuild_task=rebuild_ref_pb,
                        )

                    # 3b. KB gate: missing (or tenant-filter-hidden) → NOT_FOUND;
                    #     rebuilding → FAILED_PRECONDITION (B6 mutex).
                    kb_row = await kb_repo.get_kb(
                        conn, tenant_id=tenant_id, kb_id=kb_id
                    )
                    if not kb_row:
                        context.abort(
                            grpc.StatusCode.NOT_FOUND, "knowledge base not found"
                        )
                        return
                    if kb_row.get("status") == "rebuilding":
                        context.abort(
                            grpc.StatusCode.FAILED_PRECONDITION,
                            "knowledge base is rebuilding",
                        )
                        return

                    # 3c. explicit-change detection: HasField = "explicitly
                    #     carried" (tri-state); value comparison = "effective".
                    #     embedding_model / chunk_size changes additionally
                    #     invalidate existing vectors → paired rebuild.
                    patch: dict[str, Any] = {}
                    rebuild_needed = False
                    if request.HasField("embedding_model") and (
                        request.embedding_model.strip()
                        != (kb_row.get("embedding_model") or "")
                    ):
                        patch["embedding_model"] = request.embedding_model.strip()
                        rebuild_needed = True
                    if request.HasField("chunk_size") and request.chunk_size != (
                        kb_row.get("chunk_size") or 0
                    ):
                        patch["chunk_size"] = request.chunk_size
                        rebuild_needed = True
                    if request.HasField("ocr_enabled") and (
                        request.ocr_enabled.value
                        != bool(kb_row.get("ocr_enabled"))
                    ):
                        patch["ocr_enabled"] = request.ocr_enabled.value
                    if request.HasField("top_k") and request.top_k != (
                        kb_row.get("top_k") or 0
                    ):
                        patch["top_k"] = request.top_k
                    if request.HasField("score_threshold") and (
                        not math.isclose(
                            request.score_threshold,
                            float(kb_row.get("score_threshold") or 0.0),
                            rel_tol=1e-6,
                            abs_tol=1e-9,
                        )
                    ):
                        patch["score_threshold"] = request.score_threshold
                    if request.HasField("retrieval_mode") and (
                        request.retrieval_mode
                        != (kb_row.get("retrieval_mode") or "")
                    ):
                        patch["retrieval_mode"] = request.retrieval_mode
                    if not patch:
                        context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT, "no effective change"
                        )
                        return

                    intent = {"kb_id": kb_id, "patch": patch,
                              "rebuild_needed": rebuild_needed}

                    # 3d. config UPDATE (explicit-partial: patch keys only).
                    updated = await kb_repo.update_config_in_tx(
                        conn, tenant_id=tenant_id, kb_id=kb_id, patch=patch
                    )
                    if updated is None:
                        context.abort(
                            grpc.StatusCode.NOT_FOUND, "knowledge base not found"
                        )
                        return

                    # 3e. paired rebuild, same transaction — takes the NEW
                    #     embedding settings from the row just RETURNed.
                    #     Derived key "rebuild:<uuid>": a replayed config
                    #     request can never re-trigger it.
                    rebuild_ref = None
                    if rebuild_needed:
                        rebuild_ref = await self._trigger_rebuild_in_tx(
                            conn,
                            context,
                            tenant_id=tenant_id,
                            kb_id=kb_id,
                            kb_row=updated,
                            idem_key=f"rebuild:{idem_key}",
                            actor=actor,
                        )

                    # 3f. audit kb.config.update (plan §6.3) — same
                    #     transaction as the config UPDATE itself.
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=kb_id,
                        action="kb.config.update",
                        actor_user_id=actor,
                        before_state=_kb_audit_snapshot(kb_row),
                        after_state=_kb_audit_snapshot(updated),
                    )

                    # 3g. write kb_operations idempotency record. Poison-key
                    #     self-heal: same pattern as UpdateKB — a prior
                    #     crash between create_operation and complete_operation
                    #     leaves a pending row the replay check skips; the
                    #     SAVEPOINT keeps the outer tx usable after the
                    #     UNIQUE violation, and the retry completes the
                    #     reused row.
                    try:
                        async with conn.transaction():
                            task_row = await operation_repo.create_operation_in_tx(
                                conn,
                                tenant_id=tenant_id,
                                idempotency_key=idem_key,
                                operation_type="kb.config.update",
                                resource_type="knowledge_base",
                                resource_id=kb_id,
                                payload={"kb_id": kb_id, "patch": patch,
                                         "rebuild_needed": rebuild_needed},
                                status="pending",
                            )
                    except asyncpg.UniqueViolationError:
                        existing = await operation_repo.find_by_idempotency_key(
                            conn,
                            tenant_id=tenant_id,
                            idempotency_key=idem_key,
                            operation_type="kb.config.update",
                        )
                        if existing is None:
                            # A concurrent writer raced the row away between
                            # INSERT and SELECT; surface as UNKNOWN rather
                            # than masking.
                            raise
                        task_row = existing
                    rebuild_ref_dict = (
                        {
                            "task_id": rebuild_ref.task_id,
                            "task_type": rebuild_ref.task_type,
                            "status": rebuild_ref.status,
                            "location_url": rebuild_ref.location_url,
                        }
                        if rebuild_ref is not None
                        else None
                    )
                    await operation_repo.complete_operation_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        operation_id=str(task_row["id"]),
                        result={
                            "config": updated,
                            "rebuild_task": rebuild_ref_dict,
                        },
                    )
        except Exception:
            # failure audit kb.config.update (plan §6.3): business
            # rejections (404/409/412) are recorded; INVALID_ARGUMENT never
            # reaches here (validated before the acquire block). The
            # business tx has already rolled back, so the audit lands in
            # its own tx.
            info = _audit_failure_info(context)
            if info is not None:
                await _record_failure_audit(
                    self._pool,
                    tenant_id=tenant_id,
                    kb_id=kb_id,
                    action="kb.config.update",
                    intent=intent,
                    error_code=info[0],
                    error_msg=info[1],
                    actor_user_id=actor,
                )
            raise

        # 4. 200 carries the new config + the paired rebuild ref (None →
        #    unset on the wire: the contract's nullable rebuild_task, only
        #    present when the change touched embedding/chunk settings).
        return kb_pb.UpdateKBConfigResponse(
            config=_kb_config_msg(tenant_id, kb_id, updated),
            rebuild_task=rebuild_ref,
        )

    def UpdateKBPermissions(self, request, context):
        return _run_async(self._update_kb_permissions(request, context))

    async def _update_kb_permissions(self, request, context) -> kb_pb.KnowledgeBase:
        # 1. validate idempotency_key / kb_id / tenant_id (align UpdateKB)
        if not request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required"
            )
            return
        try:
            uuid.UUID(request.idempotency_key.strip())
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key must be a uuid"
            )
            return
        if not request.kb_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "kb_id is required")
            return
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        # allowed_user_ids: each must be a valid uuid; dedupe preserving order
        allowed_user_ids: list[str] = []
        seen_user_ids: set[str] = set()
        for raw in request.allowed_user_ids:
            uid = raw.strip()
            try:
                uuid.UUID(uid)
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "allowed_user_ids entries must be uuids",
                )
                return
            if uid not in seen_user_ids:
                seen_user_ids.add(uid)
                allowed_user_ids.append(uid)
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return

        tenant_id = request.tenant_id
        idem_key = request.idempotency_key

        # 2. single transaction: replay check, KB existence, permission
        # upsert, and the idempotency record commit atomically (UpdateKB
        # pattern; nested conn.transaction() degrades to SAVEPOINT)
        # audit kb.permissions.update (plan §6.3): before = the permission
        # row pre-upsert (defaults when no row exists), after = the
        # requested {public_read, allowed_user_ids}; written in the same
        # transaction as the upsert. Failure audits: the 404 abort fires
        # after the KB gate passed (KB row exists → FK satisfiable);
        # INVALID_ARGUMENT aborts above, before this try block.
        actor = _actor_user_id(context)
        intent: dict[str, Any] = {
            "public_read": request.public_read,
            "allowed_user_ids": list(allowed_user_ids),
        }
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # 2a. idempotency replay: return the recorded KB row
                    existing = await operation_repo.find_by_idempotency_key(
                        conn,
                        tenant_id=tenant_id,
                        idempotency_key=idem_key,
                        operation_type="kb.perm.update",
                    )
                    if existing and existing.get("result"):
                        result = existing["result"]
                        if isinstance(result, str):
                            result = json.loads(result)
                        return _kb_row_to_pb(result)

                    # 2b. KB existence check (deleted KB → NOT_FOUND)
                    kb_row = await kb_repo.get_kb(
                        conn, tenant_id=tenant_id, kb_id=request.kb_id
                    )
                    if kb_row is None:
                        context.abort(
                            grpc.StatusCode.NOT_FOUND, "knowledge base not found"
                        )
                        return
                    if kb_row.get("status") == "rebuilding":
                        # B6 (#24): permission writes are mutually exclusive
                        # with a running full-KB rebuild (reads via
                        # GetKBPermissions stay served).
                        context.abort(
                            grpc.StatusCode.FAILED_PRECONDITION,
                            "knowledge base is rebuilding",
                        )
                        return  # unreachable; for type checkers

                    # before snapshot: the permission row as it stands
                    # (defaults when no row exists yet — get_permissions
                    # never 404s, mirroring the read contract).
                    before_perm = await permission_repo.get_permissions(
                        conn, tenant_id=tenant_id, kb_id=request.kb_id
                    )

                    # 2c. UPSERT permission row
                    await permission_repo.upsert_permissions_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=request.kb_id,
                        public_read=request.public_read,
                        allowed_user_ids=allowed_user_ids,
                    )

                    # audit kb.permissions.update (plan §6.3) — same
                    # transaction as the upsert.
                    await audit_repo.insert_audit_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        kb_id=request.kb_id,
                        action="kb.permissions.update",
                        actor_user_id=actor,
                        before_state={
                            "public_read": before_perm.get("public_read"),
                            "allowed_user_ids": before_perm.get("allowed_user_ids") or [],
                        },
                        after_state=dict(intent),
                    )

                    # 2d. reuse the KB snapshot from step 2b as the replay
                    # result — the permission upsert only touches
                    # kb_permissions, so the knowledge_bases row is unchanged
                    # within this transaction (no second SELECT needed)

                    # 2e. idempotency record (poison-key self-heal, align
                    # UpdateKB step 4)
                    try:
                        async with conn.transaction():
                            task_row = await operation_repo.create_operation_in_tx(
                                conn,
                                tenant_id=tenant_id,
                                idempotency_key=idem_key,
                                operation_type="kb.perm.update",
                                resource_type="knowledge_base",
                                resource_id=request.kb_id,
                                payload={
                                    "public_read": request.public_read,
                                    "allowed_user_ids": allowed_user_ids,
                                },
                                status="pending",
                            )
                    except asyncpg.UniqueViolationError:
                        existing = await operation_repo.find_by_idempotency_key(
                            conn,
                            tenant_id=tenant_id,
                            idempotency_key=idem_key,
                            operation_type="kb.perm.update",
                        )
                        if existing is None:
                            raise
                        task_row = existing
                    await operation_repo.complete_operation_in_tx(
                        conn,
                        tenant_id=tenant_id,
                        operation_id=str(task_row["id"]),
                        result=kb_row,
                    )
        except Exception:
            # failure audit kb.permissions.update (plan §6.3): the 404
            # business rejection is recorded. The business tx has already
            # rolled back, so the audit lands in its own fresh transaction.
            info = _audit_failure_info(context)
            if info is not None:
                await _record_failure_audit(
                    self._pool,
                    tenant_id=tenant_id,
                    kb_id=request.kb_id,
                    action="kb.permissions.update",
                    intent=intent,
                    error_code=info[0],
                    error_msg=info[1],
                    actor_user_id=actor,
                )
            raise

        # 3. return KB snapshot (contract returns KnowledgeBase)
        return _kb_row_to_pb(kb_row)

    # ── P1 RPC: KB audit log read (B8, plan §6.4) ──────────────────────────────

    def ListKBAuditLogs(self, request, context):
        return _run_async(self._list_kb_audit_logs(request, context))

    async def _list_kb_audit_logs(
        self, request, context
    ) -> kb_pb.ListKBAuditLogsResponse:
        """ListKBAuditLogs — KB management-plane audit trail (P1 #21).

        get_kb gate → keyset-paginated audit rows (created_at DESC, id
        DESC) → AuditLogEntry mapping. before/after_state come back from
        asyncpg as JSON strings by default (no jsonb codec on the pools)
        and are passed through, re-serialized only if a codec ever returns
        dicts (the proto carries them as JSON strings, same as
        KBChunk.custom_metadata).
        """
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        limit = request.page.limit or 20
        if not 1 <= limit <= 100:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "limit must be between 1 and 100"
            )
            return
        cursor = request.page.cursor or None

        async with self._pool.acquire() as conn:
            kb_row = await kb_repo.get_kb(
                conn, tenant_id=request.tenant_id, kb_id=request.kb_id
            )
            if not kb_row:
                context.abort(grpc.StatusCode.NOT_FOUND, "knowledge base not found")
                return
            try:
                rows = await audit_repo.list_logs(
                    conn,
                    tenant_id=request.tenant_id,
                    kb_id=request.kb_id,
                    limit=limit,
                    cursor=cursor,
                )
            except InvalidCursorError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "invalid page cursor"
                )
                return
        items = [
            kb_pb.AuditLogEntry(
                id=str(r["id"]),
                kb_id=str(r["kb_id"]),
                actor_user_id=str(r["actor_user_id"]) if r["actor_user_id"] else "",
                action=str(r["action"] or ""),
                before_state=_audit_state_json(r.get("before_state")),
                after_state=_audit_state_json(r.get("after_state")),
                error_code=str(r.get("error_code") or ""),
                error_msg=str(r.get("error_msg") or ""),
                created_at=_ts(r.get("created_at")),
            )
            for r in rows
        ]
        next_cursor = (
            _audit_cursor(rows[-1]) if rows and len(rows) >= limit else ""
        )
        return kb_pb.ListKBAuditLogsResponse(items=items, next_cursor=next_cursor)

    # ── P1C RPC: kb_operations progress query (#052, ADR 0001 §D4-2) ────────────

    def ListOperations(self, request, context):
        return _run_async(self._list_operations(request, context))

    async def _list_operations(
        self, request, context
    ) -> kb_pb.ListOperationsResponse:
        """ListOperations — kb_operations progress query (P1C #052).

        Temporary progress-visibility surface (ADR 0001 §D4-2): kb no longer
        writes the shared async_tasks table and task-service (which will
        subscribe to kb outbox events and take over task display, ADR
        §五-阶段6) is not ready yet, so operators can see kb operation
        status through this RPC in the interim. Keyset pagination over
        (created_at DESC, id DESC) — same convention as ListKBAuditLogs;
        tenant isolation is the repository's explicit tenant_id filter.
        payload/result JSONB pass through as JSON strings.
        """
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        limit = request.page.limit or 20
        if not 1 <= limit <= 100:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "limit must be between 1 and 100"
            )
            return
        cursor = request.page.cursor or None
        resource_id = request.resource_id or None
        if resource_id:
            try:
                uuid.UUID(resource_id)
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "resource_id must be a UUID"
                )
                return

        async with self._pool.acquire() as conn:
            try:
                rows = await operation_repo.list_operations(
                    conn,
                    tenant_id=request.tenant_id,
                    operation_type=request.operation_type or None,
                    resource_id=resource_id,
                    status=request.status or None,
                    limit=limit,
                    cursor=cursor,
                )
            except InvalidCursorError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "invalid page cursor"
                )
                return
        items = [_operation_row_to_pb(r) for r in rows]
        next_cursor = (
            _operation_cursor(rows[-1]) if rows and len(rows) >= limit else ""
        )
        return kb_pb.ListOperationsResponse(items=items, next_cursor=next_cursor)

    def GetOperation(self, request, context):
        return _run_async(self._get_operation(request, context))

    async def _get_operation(self, request, context) -> kb_pb.KBOperation:
        """GetOperation — single kb_operation lookup (P1C #052).

        Same isolation as ListOperations (explicit tenant filter): an
        operation of another tenant is invisible → NOT_FOUND.
        """
        if not request.tenant_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tenant_id is required")
            return
        if self._pool is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "DB pool not configured")
            return
        try:
            uuid.UUID(request.operation_id)
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "operation_id must be a UUID"
            )
            return
        async with self._pool.acquire() as conn:
            row = await operation_repo.get_operation(
                conn, tenant_id=request.tenant_id, operation_id=request.operation_id
            )
        if not row:
            context.abort(grpc.StatusCode.NOT_FOUND, "operation not found")
            return
        return _operation_row_to_pb(row)


# ── helpers ───────────────────────────────────────────────────────────────────


def _default_session_cache() -> Any:
    """Build a SessionCache from app settings, or None if Redis is unavailable.

    In production main.py builds the cache once at startup and injects it via
    session_cache_factory, so this default is only used by tests / skeleton
    mode / direct servicer construction without main.py. We construct a fresh
    instance per call (no module-global singleton) to preserve test isolation
    — a module-level cache would leak state across tests and block them from
    mocking the factory. Query degrades to DB-only when Redis is down
    (SPEC §7.3).
    """
    from app.core.config import settings
    from app.session.cache import SessionCache

    try:
        import redis.asyncio as aioredis

        client = aioredis.from_url(settings.redis_url, decode_responses=False)
        return SessionCache(redis=client)
    except Exception as e:  # noqa: BLE001 — best-effort cache wiring
        logger.warning("Redis session cache unavailable (Query will be DB-only): %s", e)
        return None


def _default_rag_engine_grpc_client() -> Any:
    """Build a RagEngineGRPCClient from app settings (production default).

    Used when no factory was injected. In production, main.py constructs
    the client once at startup and injects it via
    ``rag_engine_grpc_client_factory``.

    This fallback creates a module-level singleton so the gRPC channel is
    not re-created on every request (avoids channel leak / connection storm).
    """
    global _default_grpc_client_instance
    if _default_grpc_client_instance is None:
        from app.rag_engine.client import RagEngineGRPCClient

        _default_grpc_client_instance = RagEngineGRPCClient(
            addr=settings.rag_engine_grpc_addr
        )
    return _default_grpc_client_instance


def _default_inference_service_client() -> Any:
    """Build an InferenceServiceGRPCClient from app settings.

    Used when no factory was injected. In production, main.py constructs the
    client once at startup and injects it via
    ``inference_service_client_factory``.

    Module-level singleton so the gRPC channel is not re-created per request
    (the channel itself is created lazily on first async call, binding to the
    caller's loop).
    """
    global _default_inference_client_instance
    if _default_inference_client_instance is None:
        from app.inference_service.client import InferenceServiceGRPCClient

        _default_inference_client_instance = InferenceServiceGRPCClient(
            addr=settings.inference_service_grpc_addr
        )
    return _default_inference_client_instance


def _default_retrieve_service(tenant_id: str, pool: Any) -> Any:
    """Build a RetrieveService from app settings (production default).

    Used when no factory was injected. In production, main.py constructs
    the service once at startup and injects it via
    ``retrieve_service_factory``.

    Reuses the module-level ``_default_rag_engine_grpc_client`` singleton so
    the gRPC channel is shared, not re-created per request.
    """
    from app.services.retrieve_service import RetrieveService
    from app.vector_store import get_default_vector_store

    rag_engine = _default_rag_engine_grpc_client()
    return RetrieveService(
        db_pool=pool,
        vector_store=get_default_vector_store(),
        rag_engine_client=rag_engine,
    )


# Module-level singleton for the fallback gRPC client (avoids channel leak).
_default_grpc_client_instance: Any = None

# Module-level singleton for the fallback inference-service client.
_default_inference_client_instance: Any = None


def _kb_bucket_id(kb_id: str) -> str:
    """Derive the MinIO bucket id for a KB.

    Convention: a single shared kb-docs bucket per deployment. The Core
    object-store manages the bucket; kb-service just uses it.
    """
    return "kb-docs"


def _parse_metadata(raw: str) -> dict[str, Any] | None:
    """Parse the custom_metadata JSON string from the proto request."""
    if not raw:
        return None

    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def _kb_row_to_pb(row: dict[str, Any]) -> kb_pb.KnowledgeBase:
    """Convert a knowledge_bases repository row to a proto KnowledgeBase."""
    return kb_pb.KnowledgeBase(
        tenant_id=str(row.get("tenant_id", "")),
        id=str(row.get("id", "")),
        name=row.get("name") or "",
        description=row.get("description") or "",
        embedding_model=row.get("embedding_model") or "",
        chunk_size=row.get("chunk_size") or 0,
        top_k=row.get("top_k") or 0,
        score_threshold=row.get("score_threshold") or 0.0,
        retrieval_mode=row.get("retrieval_mode") or "",
        default_inference_service=str(
            row.get("default_inference_service") or ""
        ),
        status=row.get("status") or "",
        doc_count=row.get("doc_count") or 0,
        created_at=_ts(row.get("created_at")),
        updated_at=_ts(row.get("updated_at")),
    )


def _kb_config_msg(tenant_id: str, kb_id: str, row: dict[str, Any]) -> kb_pb.KBConfig:
    """Convert a knowledge_bases row (or its JSON-serialized replay form)
    to a proto KBConfig (B5 #22 / B7 #23).

    Shared by GetKBConfig and UpdateKBConfig (fresh + replay paths);
    retrieval_mode aligns the contract fix: vector | hybrid | keyword.
    """
    return kb_pb.KBConfig(
        tenant_id=tenant_id,
        kb_id=kb_id,
        embedding_model=row.get("embedding_model") or "",
        chunk_size=row.get("chunk_size") or 0,
        ocr_enabled=bool(row.get("ocr_enabled")),
        top_k=row.get("top_k") or 0,
        score_threshold=float(row.get("score_threshold") or 0.0),
        retrieval_mode=row.get("retrieval_mode") or "hybrid",
    )


def _doc_row_to_pb(row: dict[str, Any]) -> kb_pb.KBDocument:
    """Convert a kb_documents repository row to a proto KBDocument."""
    metadata = row.get("custom_metadata")
    if isinstance(metadata, (dict, list)):
        metadata_str = json.dumps(metadata, default=str)
    else:
        metadata_str = str(metadata) if metadata else ""
    return kb_pb.KBDocument(
        tenant_id=str(row.get("tenant_id", "")),
        kb_id=str(row.get("kb_id", "")),
        id=str(row.get("id", "")),
        file_name=row.get("file_name") or "",
        file_type=row.get("file_type") or "",
        file_size_bytes=row.get("file_size_bytes") or 0,
        parse_status=row.get("parse_status") or "",
        chunk_count=row.get("chunk_count") or 0,
        error_message=row.get("error_message") or "",
        custom_metadata=metadata_str,
        created_at=_ts(row.get("created_at")),
        parsed_at=_ts(row.get("parsed_at")),
    )


def _chunk_row_to_pb(row: dict[str, Any]) -> kb_pb.KBChunk:
    """Convert a kb_chunks repository row to a proto KBChunk."""
    metadata = row.get("custom_metadata")
    if isinstance(metadata, (dict, list)):
        metadata_str = json.dumps(metadata, default=str)
    else:
        metadata_str = str(metadata) if metadata else ""
    return kb_pb.KBChunk(
        id=str(row.get("id", "")),
        doc_id=str(row.get("doc_id", "")),
        kb_id=str(row.get("kb_id", "")),
        parent_chunk_id=str(row.get("parent_chunk_id") or ""),
        chunk_type=row.get("chunk_type") or "",
        content=row.get("content") or "",
        parent_content=row.get("parent_content") or "",
        page_number=row.get("page_number") or 0,
        content_type=row.get("content_type") or "",
        token_count=row.get("token_count") or 0,
        custom_metadata=metadata_str,
        created_at=_ts(row.get("created_at")),
        file_name=row.get("file_name") or "",
    )


def _session_message_row_to_pb(row: dict[str, Any]) -> kb_pb.KBSessionMessage:
    """Convert a kb_messages repository row to a proto KBSessionMessage."""
    return kb_pb.KBSessionMessage(
        id=str(row.get("id", "")),
        session_id=str(row.get("session_id", "")),
        role=row.get("role") or "",
        content=row.get("content") or "",
        source_chunks=str(row.get("source_chunks") or ""),
        input_tokens=row.get("input_tokens") or 0,
        output_tokens=row.get("output_tokens") or 0,
        duration_ms=row.get("duration_ms") or 0,
        created_at=_ts(row.get("created_at")),
    )


def _message_cursor(row: dict[str, Any]) -> str:
    """Encode a composite keyset cursor for kb_messages (created_at, id).

    The message id is a random UUID, so created_at alone cannot break ties —
    the id ASC component guarantees a stable playback order. The timestamp
    uses the ``Z`` suffix (not ``+00:00``): cursors round-trip through URL
    query strings where ``+`` is decoded as a space, which would corrupt the
    cursor on the next request. ``Z`` is parsed fine by
    parse_composite_cursor (datetime.fromisoformat).
    """
    return f"{_cursor_ts(row['created_at'])}|{row['id']}"


def _session_cursor(row: dict[str, Any]) -> str:
    """Encode a composite keyset cursor for kb_sessions (created_at, id)."""
    return f"{_cursor_ts(row['created_at'])}|{row['id']}"


def _audit_cursor(row: dict[str, Any]) -> str:
    """Encode a composite keyset cursor for kb_audit_log (created_at, id)."""
    return f"{_cursor_ts(row['created_at'])}|{row['id']}"


def _operation_cursor(row: dict[str, Any]) -> str:
    """Encode a composite keyset cursor for kb_operations (created_at, id)."""
    return f"{_cursor_ts(row['created_at'])}|{row['id']}"


def _operation_row_to_pb(row: dict[str, Any]) -> kb_pb.KBOperation:
    """Map a kb_operations row dict to the KBOperation proto (#052).

    payload/result JSONB come back from asyncpg as JSON strings by default
    (no jsonb codec on the pools) and pass through; the dict branch stays
    defensive in case a codec is registered later (same as audit state).
    """
    return kb_pb.KBOperation(
        tenant_id=str(row["tenant_id"]),
        id=str(row["id"]),
        idempotency_key=str(row["idempotency_key"]),
        operation_type=str(row["operation_type"]),
        resource_id=str(row["resource_id"]) if row["resource_id"] else "",
        status=str(row["status"]),
        payload=_audit_state_json(row["payload"]),
        result=_audit_state_json(row.get("result")),
        started_at=_ts(row.get("started_at")),
        completed_at=_ts(row.get("completed_at")),
        created_at=_ts(row["created_at"]),
        updated_at=_ts(row["updated_at"]),
    )


def _audit_state_json(state: Any) -> str:
    """Serialize an audit before/after_state JSONB value to a JSON string.

    asyncpg returns JSONB as a plain string by default (no jsonb codec is
    registered on the pools), so the str branch is the production path;
    the dict branch stays defensive in case a codec is registered later.
    The proto field is a plain string field — same treatment as
    KBChunk.custom_metadata. None → "" (creation-type has no before_state,
    deletion-type has no after_state).
    """
    if state is None:
        return ""
    if isinstance(state, str):
        return state
    return json.dumps(state, default=str)


def _actor_user_id(context: Any) -> str | None:
    """Extract the calling user id from gRPC metadata ``x-user-id``.

    Set by the Gateway on management-plane calls (same header as the
    tenant-write path). Returns None when absent or not a valid uuid —
    internal system actors (parse/rebuild consumers) call without it,
    yielding the NULL actor_user_id audit rows. The metadata lookup is
    defensive: test fakes may not implement invocation_metadata().
    """
    try:
        metadata = context.invocation_metadata() or ()
    except Exception:
        return None
    for key, value in metadata:
        if key == "x-user-id":
            try:
                uuid.UUID(str(value))
            except ValueError:
                return None
            return str(value)
    return None


def _request_id(context: Any) -> str | None:
    """Extract the Gateway request id from gRPC metadata ``x-request-id``.

    Gateway sets this on every proxied call (same header it writes into its
    own structured logs) so a kb-service log line can be joined back to the
    originating request. Returns None when absent — internal system callers
    (parse/rebuild consumers) have no request context. The metadata lookup is
    defensive: test fakes may not implement invocation_metadata().
    """
    try:
        metadata = context.invocation_metadata() or ()
    except Exception:
        return None
    for key, value in metadata:
        if key == "x-request-id":
            value = str(value)
            return value or None
    return None


# gRPC status codes that constitute a *business failure* for audit
# purposes (plan §6.3): the request was well-formed but a business rule
# rejected it. INVALID_ARGUMENT (bad request shape, rejected before any
# business logic) is not audited. FAILED_PRECONDITION covers both
# business-rule rejections (KB rebuilding, doc ready, poisoned retry key —
# audited) and system-not-ready states (skeleton pool, missing bucket);
# the latter cannot be audited anyway because the pool is None, which
# makes _record_failure_audit a no-op.
_AUDITED_FAILURE_CODES = frozenset({
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.ALREADY_EXISTS,
    grpc.StatusCode.FAILED_PRECONDITION,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
})


def _audit_failure_info(context: Any) -> tuple[str, str] | None:
    """Return ``(code_name, details)`` for an aborted, audited failure.

    ``context.abort()`` raises a bare ``Exception`` after setting the state
    (grpc/_server.py) — the status details live on the context, not in the
    exception message. Failure-audit wrappers therefore catch broadly and
    read the already-set code/details off the context to decide whether the
    failure is auditable. Test fakes may not implement code()/details() —
    treated as not auditable.
    """
    try:
        code = context.code()
    except Exception:
        return None
    if code not in _AUDITED_FAILURE_CODES:
        return None
    try:
        details = str(context.details() or "")
    except Exception:
        details = ""
    return code.name, details


async def _record_failure_audit(
    pool: asyncpg.Pool | None,
    *,
    tenant_id: str,
    kb_id: str,
    action: str,
    intent: dict[str, Any] | None,
    error_code: str,
    error_msg: str,
    actor_user_id: str | None,
) -> None:
    """Best-effort audit row for a failed write attempt (plan §6.3).

    Runs AFTER the business transaction rolled back, in its own fresh
    transaction, so the failure record survives. Audit write failures
    are swallowed — never mask the business error being re-raised to the
    caller. The kb_audit_log.kb_id FK (ON DELETE CASCADE) rejects rows
    when the target KB itself does not exist (e.g. a 404 abort) —
    expected: the audit write is best-effort and those failures are only
    visible via this debug log.
    """
    if pool is None:
        return
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await audit_repo.insert_audit_in_tx(
                    conn,
                    tenant_id=tenant_id,
                    kb_id=kb_id,
                    action=action,
                    actor_user_id=actor_user_id,
                    before_state=intent,
                    after_state=None,
                    error_code=error_code,
                    error_msg=error_msg,
                )
    except Exception as e:
        logger.debug(
            "kb-service: failed to persist failure audit (action=%s kb_id=%s "
            "error_code=%s): %s", action, kb_id, error_code, e,
        )


def _kb_audit_snapshot(kb_row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Project a KB row onto the audited key fields (plan §6.3 snapshots).

    Creates/updates audit the whole KB config; deletes audit the row being
    removed. Returns None for a missing row so JSON NULL vs {"empty"} stay
    distinguishable in the audit payload.
    """
    if kb_row is None:
        return None
    return {
        "name": kb_row.get("name"),
        "description": kb_row.get("description"),
        "embedding_model": kb_row.get("embedding_model"),
        "chunk_size": kb_row.get("chunk_size"),
        "ocr_enabled": kb_row.get("ocr_enabled"),
        "top_k": kb_row.get("top_k"),
        "score_threshold": kb_row.get("score_threshold"),
        "retrieval_mode": kb_row.get("retrieval_mode"),
        "default_inference_service": kb_row.get("default_inference_service"),
        "status": kb_row.get("status"),
        "doc_count": kb_row.get("doc_count"),
    }


# Audit text cap: Q&A content is truncated before it lands in kb_audit_log
# so a long question/answer cannot bloat the audit trail rows.
_AUDIT_TEXT_MAX = 500


def _audit_text_truncate(text: str | None) -> str:
    text = text or ""
    return text if len(text) <= _AUDIT_TEXT_MAX else text[:_AUDIT_TEXT_MAX]


def _doc_audit_snapshot(doc_row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Project a document row onto the audited key fields (plan §6.3).

    doc.create audits the document intent (file identity + storage);
    doc.parse / doc.delete / doc.reparse audit the row's parse lifecycle
    state at the time of the operation.
    """
    if doc_row is None:
        return None
    return {
        "doc_id": str(doc_row.get("id") or doc_row.get("doc_id") or ""),
        "file_name": doc_row.get("file_name"),
        "file_type": doc_row.get("file_type"),
        "file_size_bytes": doc_row.get("file_size_bytes"),
        "storage_path": doc_row.get("storage_path"),
        "object_id": doc_row.get("object_id"),
        "parse_status": doc_row.get("parse_status"),
        "chunk_count": doc_row.get("chunk_count"),
    }


def _doc_intent_snapshot(request: Any, *, storage_path: str = "") -> dict[str, Any]:
    """Build the doc.create intent snapshot from the upload request.

    Used both as the success-path after_state and the failure-path
    before_state ("intent") — a failed create leaves no DB row, so the
    intent is the only record of what was attempted. storage_path is the
    servicer-derived object-store key, passed in because the request does
    not carry it.
    """
    return {
        "file_name": request.file_name,
        "file_type": request.file_type,
        "file_size_bytes": request.file_size_bytes,
        "checksum_sha256": request.checksum_sha256,
        "storage_path": storage_path,
        "kb_id": request.kb_id,
    }


def _cursor_ts(dt: datetime) -> str:
    """ISO timestamp with the URL-safe ``Z`` suffix for cursor encoding."""
    return dt.isoformat().replace("+00:00", "Z")
