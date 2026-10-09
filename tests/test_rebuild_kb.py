"""Tests for RebuildKB servicer + rebuild-supporting repositories (P1 #24, B6).

Mirrors test_reparse_document.py (mock asyncpg conn, no real DB):

Servicer (_rebuild_kb):
- Success: KB active→rebuilding via set_status_in_tx (conditional UPDATE);
  audit row (action='kb.rebuild', before=active/after=rebuilding);
  kb_operations row (operation_type='kb.rebuild', payload carries
  kb_id/embedding model/chunk_size); outbox event 'kb.rebuild' with
  task_id in the payload; returns an AsyncTaskRef (pending).
- Guards: KB missing → NOT_FOUND; KB already rebuilding (gate read) →
  FAILED_PRECONDITION; conditional UPDATE matching 0 rows (race loser) →
  FAILED_PRECONDITION; missing idempotency_key → INVALID_ARGUMENT;
  no pool → FAILED_PRECONDITION.
- Idempotency: same key pending/completed → same task_id replayed (no
  transition, no inserts); failed task same key → FAILED_PRECONDITION
  (poisoned key); UNIQUE-race pending winner → replay, no outbox event.

Repositories:
- set_status_in_tx: conditional UPDATE binds from/to status, RLS set.
- list_documents_for_rebuild: scope (ready/failed, soft-deleted excluded)
  and deterministic ordering.
- operation.acquire_run / complete_operation: conditional SQL (non-terminal
  takeover; terminal-state guard on completion).

Mutex matrix (B6 #24): the six write servicers reject a rebuilding KB with
FAILED_PRECONDITION — covered per-servicer here via the shared kb_row status
('rebuilding' rows short-circuit before any business write).
"""
import asyncio
import json
import os
import sys
import uuid
from contextlib import asynccontextmanager

import asyncpg
import grpc
import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.api.grpc_server import KBServiceServicer
from app.generated.kb.v1 import kb_service_pb2 as kb_pb

TENANT_ID = "11111111-1111-1111-1111-111111111111"
KB_ID = "22222222-2222-2222-2222-222222222222"
TASK_ID = "44444444-4444-4444-4444-444444444444"


def _kb_row(**over):
    row = {
        "id": uuid.UUID(KB_ID),
        "tenant_id": uuid.UUID(TENANT_ID),
        "name": "kb",
        "status": "active",
        "chunk_size": 512,
        "embedding_model": "bge-m3",
        "vector_store_id": "vs_kb_2222222222222222222222222",
    }
    row.update(over)
    return row


def _op_row(task_id=TASK_ID, status="pending", operation_type="kb.rebuild"):
    return {
        "id": uuid.UUID(task_id),
        "operation_type": operation_type,
        "status": status,
    }


class _ServicerContext:
    """grpc.ServicerContext stand-in — abort raises with the code embedded."""

    def __init__(self):
        self.aborted = None

    def abort(self, code, message):
        self.aborted = (code, message)
        raise RuntimeError(f"aborted: {code} {message}")

    # _audit_failure_info reads code()/details() off aborted contexts; a
    # raising fake keeps the failure-audit path a no-op (same as
    # test_reparse_document.py).
    def code(self):
        raise RuntimeError("not a real context")

    def details(self):
        raise RuntimeError("not a real context")


class _RebuildConn:
    """Recording fake conn for the RebuildKB flow.

    Dispatches by SQL keywords: kb_operations idempotency lookups,
    knowledge_bases reads (KB gate), the conditional status UPDATE
    (``transition_result`` feeds set_status_in_tx), kb_operations +
    outbox_events + kb_audit_log inserts.

    UNIQUE-race simulation (mirrors _UpdateKBMockConn): ``insert_raise_exc``
    makes the kb_operations INSERT raise; the retry find then returns
    ``race_task`` instead of ``existing_task``.
    """

    def __init__(
        self,
        *,
        existing_task=None,
        kb_row="default",
        transition_result="UPDATE 1",
        insert_raise_exc=None,
        race_task=None,
    ):
        self.existing_task = existing_task
        self._kb_row = _kb_row() if kb_row == "default" else kb_row
        self.transition_result = transition_result
        self.insert_raise_exc = insert_raise_exc
        self.race_task = race_task
        self._find_calls = 0
        self.execute_calls: list[tuple] = []
        self.fetchrow_calls: list[tuple] = []

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self

        return _tx()

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        if sql.startswith("SET LOCAL"):
            return None
        if "UPDATE knowledge_bases" in sql and "status = $2" in sql:
            return self.transition_result
        if "UPDATE kb_operations" in sql:
            return "UPDATE 1"
        return "UPDATE 1"

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        if "FROM kb_operations" in sql and "idempotency_key" in sql:
            self._find_calls += 1
            task = self.existing_task
            if (
                task is not None
                and len(args) >= 3
                and args[2] is not None
                and task.get("operation_type") != args[2]
            ):
                task = None
            if self._find_calls > 1:
                return self.race_task
            return task
        if "FROM knowledge_bases" in sql:
            return self._kb_row
        if "INSERT INTO kb_operations" in sql:
            if self.insert_raise_exc is not None:
                raise self.insert_raise_exc
            return _op_row()
        if "INSERT INTO kb_outbox_events" in sql:
            return {"id": 1}
        if "INSERT INTO kb_audit_log" in sql:
            return {"id": uuid.uuid4()}
        return None

    async def fetch(self, sql, *args):
        return []

    async def fetchval(self, sql, *args):
        return None


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


def _make_servicer(conn):
    return KBServiceServicer(pool=_Pool(conn))


def _req(**over):
    defaults = dict(
        tenant_id=TENANT_ID,
        kb_id=KB_ID,
        idempotency_key=str(uuid.uuid4()),
    )
    defaults.update(over)
    return kb_pb.RebuildKBRequest(**defaults)


# ── servicer: success path ───────────────────────────────────────────────────


async def test_rebuild_success_transitions_status_writes_audit_task_outbox():
    conn = _RebuildConn()
    servicer = _make_servicer(conn)
    idem = str(uuid.uuid4())

    result = await servicer._rebuild_kb(_req(idempotency_key=idem), _ServicerContext())

    assert result.task_id == TASK_ID
    assert result.task_type == "kb.rebuild"
    assert result.status == "pending"

    # #054: no RLS/GUC statement — tenant isolation is the explicit
    # tenant_id bound into every business statement below.
    assert not any("SET LOCAL" in s for s, _ in conn.execute_calls)

    # a. conditional status transition: active → rebuilding, bound params.
    transition_calls = [
        (s, a) for s, a in conn.execute_calls
        if "UPDATE knowledge_bases" in s and "status = $2" in s
    ]
    assert len(transition_calls) == 1
    sql, args = transition_calls[0]
    assert "WHERE id = $1 AND tenant_id = $4 AND status = $3" in sql
    assert args == (uuid.UUID(KB_ID), "rebuilding", "active", uuid.UUID(TENANT_ID))

    # b. audit row: action='kb.rebuild', before=active, after=rebuilding.
    #    Positional args: (tenant_id, kb_id, actor_user_id, action,
    #    before_state, after_state, error_code, error_msg).
    audit_calls = [
        (s, a) for s, a in conn.fetchrow_calls if "INSERT INTO kb_audit_log" in s
    ]
    assert len(audit_calls) == 1
    args = audit_calls[0][1]
    assert args[0] == uuid.UUID(TENANT_ID)
    assert args[1] == uuid.UUID(KB_ID)
    assert args[3] == "kb.rebuild"
    assert json.loads(args[4]) == {"status": "active"}
    assert json.loads(args[5]) == {"status": "rebuilding"}


# ── servicer: guards ──────────────────────────────────────────────────────────


async def test_rebuild_kb_missing_not_found():
    conn = _RebuildConn(kb_row=None)
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._rebuild_kb(_req(), ctx)
    assert ctx.aborted is not None
    assert ctx.aborted[0] == grpc.StatusCode.NOT_FOUND
    # No status transition / task / outbox write happened.
    assert not any("UPDATE knowledge_bases" in s for s, _ in conn.execute_calls)
    assert not any("INSERT INTO kb_operations" in s for s, _ in conn.fetchrow_calls)


async def test_rebuild_kb_already_rebuilding_gate_rejected():
    """A KB already rebuilding: the gate read sees 'rebuilding' (not None —
    no 404) and the authoritative rejection comes from the conditional
    UPDATE: status is not 'active' so 0 rows match → FAILED_PRECONDITION.
    (The gate read itself only guards existence; D3 keeps the mutex in
    the conditional UPDATE so the read-UPDATE race window is closed.)"""
    conn = _RebuildConn(
        kb_row=_kb_row(status="rebuilding"),
        transition_result="UPDATE 0",
    )
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._rebuild_kb(_req(), ctx)
    assert ctx.aborted is not None
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    # The losing transaction wrote nothing durable.
    assert not any("INSERT INTO kb_operations" in s for s, _ in conn.fetchrow_calls)
    assert not any("INSERT INTO kb_outbox_events" in s for s, _ in conn.fetchrow_calls)


async def test_rebuild_conditional_update_race_loser_rejected():
    """Authoritative mutex: the gate read saw 'active' but a concurrent
    rebuild won the transition — the conditional UPDATE matches 0 rows →
    FAILED_PRECONDITION (the race window between read and UPDATE)."""
    conn = _RebuildConn(transition_result="UPDATE 0")
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._rebuild_kb(_req(), ctx)
    assert ctx.aborted is not None
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    # The losing transaction wrote nothing durable.
    assert not any("INSERT INTO kb_operations" in s for s, _ in conn.fetchrow_calls)
    assert not any("INSERT INTO kb_outbox_events" in s for s, _ in conn.fetchrow_calls)


async def test_rebuild_missing_idempotency_key_invalid_argument():
    conn = _RebuildConn()
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await servicer._rebuild_kb(_req(idempotency_key=""), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.INVALID_ARGUMENT


async def test_rebuild_bad_idempotency_key_invalid_argument():
    conn = _RebuildConn()
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await servicer._rebuild_kb(_req(idempotency_key="not-a-uuid"), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.INVALID_ARGUMENT


async def test_rebuild_no_pool_failed_precondition():
    servicer = KBServiceServicer()
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._rebuild_kb(_req(), ctx)
    assert ctx.aborted is not None


# ── servicer: idempotency ─────────────────────────────────────────────────────


async def test_rebuild_same_key_pending_replays_same_task_id():
    conn = _RebuildConn(existing_task=_op_row(status="pending"))
    servicer = _make_servicer(conn)
    result = await servicer._rebuild_kb(_req(), _ServicerContext())
    assert result.task_id == TASK_ID
    assert result.status == "pending"
    assert result.task_type == "kb.rebuild"
    # No transition / task / outbox write ran.
    assert not any("UPDATE knowledge_bases" in s for s, _ in conn.execute_calls)
    assert not any("INSERT INTO kb_operations" in s for s, _ in conn.fetchrow_calls)
    assert not any("INSERT INTO kb_outbox_events" in s for s, _ in conn.fetchrow_calls)


async def test_rebuild_same_key_completed_replays_same_task_id():
    conn = _RebuildConn(existing_task=_op_row(status="completed"))
    servicer = _make_servicer(conn)
    result = await servicer._rebuild_kb(_req(), _ServicerContext())
    assert result.task_id == TASK_ID
    assert result.status == "completed"


async def test_rebuild_find_filters_by_task_type():
    """A row recorded by a DIFFERENT operation (e.g. kb.reparse) under the
    same tenant + key must NOT replay — the rebuild proceeds down the full
    path (transition + INSERT)."""
    conn = _RebuildConn(
        existing_task=_op_row(status="pending", operation_type="kb.reparse")
    )
    servicer = _make_servicer(conn)
    result = await servicer._rebuild_kb(_req(), _ServicerContext())
    assert result.task_type == "kb.rebuild"
    assert result.task_id == TASK_ID
    assert any("UPDATE knowledge_bases" in s for s, _ in conn.execute_calls)
    assert any("INSERT INTO kb_operations" in s for s, _ in conn.fetchrow_calls)
    # The replay lookup itself carried the operation_type filter.
    find_calls = [
        (s, a) for s, a in conn.fetchrow_calls
        if "FROM kb_operations" in s and "idempotency_key" in s
    ]
    assert len(find_calls) >= 1
    sql, args = find_calls[0]
    assert "operation_type = $3" in sql
    assert args[2] == "kb.rebuild"


async def test_rebuild_failed_task_same_key_poison_rejected():
    """A failed task is NOT replayed on the same key (SPEC §5.4): the UNIQUE
    self-heal finds the failed row → FAILED_PRECONDITION; the abort rolls
    back the whole transaction INCLUDING the rebuilding transition."""
    conn = _RebuildConn(
        existing_task=_op_row(status="failed"),
        insert_raise_exc=asyncpg.UniqueViolationError(
            'duplicate key value violates unique constraint '
            '"kb_operations_tenant_id_idempotency_key_key"'
        ),
        race_task=_op_row(status="failed"),
    )
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._rebuild_kb(_req(), ctx)
    assert ctx.aborted is not None
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    # No second outbox event on the rejected retry.
    assert not any("INSERT INTO kb_outbox_events" in s for s, _ in conn.fetchrow_calls)


async def test_rebuild_unique_race_pending_replays_winner_task():
    """TOCTOU self-heal: a concurrent rebuild with the same key commits
    between the check and the INSERT. The re-lookup returns the winner's
    pending row → same task_id replayed, no second outbox event."""
    conn = _RebuildConn(
        insert_raise_exc=asyncpg.UniqueViolationError(
            'duplicate key value violates unique constraint '
            '"kb_operations_tenant_id_idempotency_key_key"'
        ),
        race_task=_op_row(status="pending"),
    )
    servicer = _make_servicer(conn)
    result = await servicer._rebuild_kb(_req(), _ServicerContext())
    assert result.task_id == TASK_ID
    assert result.status == "pending"
    # The transition ran (outer tx continues past the SAVEPOINT abort)…
    assert any("UPDATE knowledge_bases" in s for s, _ in conn.execute_calls)
    # …but no outbox event: the winner's tx already published one.
    assert not any("INSERT INTO kb_outbox_events" in s for s, _ in conn.fetchrow_calls)


# ── repository: set_status_in_tx ──────────────────────────────────────────────


async def test_set_status_in_tx_sql_and_args():
    from app.repositories import knowledge_base as kb_repo

    conn = _RebuildConn()
    updated = await kb_repo.set_status_in_tx(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID,
        from_status="active", to_status="rebuilding",
    )
    assert updated is True
    sql, args = conn.execute_calls[-1]
    assert "UPDATE knowledge_bases" in sql
    assert "SET status = $2" in sql
    assert "WHERE id = $1 AND tenant_id = $4 AND status = $3" in sql
    assert args == (uuid.UUID(KB_ID), "rebuilding", "active", uuid.UUID(TENANT_ID))
    # #054: no GUC statement — isolation is the tenant_id = $4 filter above
    # (set_status_in_tx does not open its own transaction — the caller's tx
    # is the boundary).
    assert not any("SET LOCAL" in s for s, _ in conn.execute_calls)


async def test_set_status_in_tx_row_missing_returns_false():
    from app.repositories import knowledge_base as kb_repo

    conn = _RebuildConn(transition_result="UPDATE 0")
    updated = await kb_repo.set_status_in_tx(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID,
        from_status="rebuilding", to_status="active",
    )
    assert updated is False


# ── repository: list_documents_for_rebuild ────────────────────────────────────


async def test_list_documents_for_rebuild_scope_and_order():
    from app.repositories import document as doc_repo

    conn = _RebuildConn()

    async def fake_fetch(sql, *args):
        conn.execute_calls.append((sql, args))
        return [{"id": uuid.UUID(TASK_ID)}, {"id": uuid.UUID(TENANT_ID)}]

    conn.fetch = fake_fetch
    ids = await doc_repo.list_documents_for_rebuild(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID
    )
    assert ids == [TASK_ID, TENANT_ID]
    sql, args = conn.execute_calls[-1]
    assert "FROM kb_documents" in sql
    assert "parse_status IN ('ready', 'failed')" in sql
    assert "NOT (parse_status = 'failed' AND error_message = 'deleted')" in sql
    assert "ORDER BY created_at ASC, id ASC" in sql
    # #054: explicit tenant filter (RLS/GUC is gone).
    assert "tenant_id = $2" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))


# ── repository: acquire_run / complete_operation ─────────────────────────────


async def test_acquire_run_sql_and_args():
    from app.repositories import operation as op_repo

    conn = _RebuildConn()
    ok = await op_repo.acquire_run(conn, tenant_id=TENANT_ID, operation_id=TASK_ID)
    assert ok is True
    sql, args = conn.execute_calls[-1]
    assert "UPDATE kb_operations" in sql
    assert "status = 'running'" in sql
    assert "started_at = now()" in sql
    # Two acquire paths share one conditional: any non-terminal row is
    # acquirable — pending first-pickup OR running orphan takeover (the
    # previous consumer crashed without acking; JetStream redelivery is
    # the takeover signal).
    assert "status NOT IN ('completed', 'failed')" in sql
    # #054: explicit tenant filter (RLS/GUC is gone).
    assert "tenant_id = $2" in sql
    assert args == (uuid.UUID(TASK_ID), uuid.UUID(TENANT_ID))


async def test_acquire_run_takeover_semantics():
    """Row-state driven conditional (mock returns UPDATE 0/1 like real PG
    would): a terminal row is never re-acquired (duplicate delivery of an
    already-processed rebuild is skipped); a pending/running row IS
    acquirable (first pickup or orphan takeover)."""
    from app.repositories import operation as op_repo

    class _TakeoverConn(_RebuildConn):
        def __init__(self, result, **kw):
            super().__init__(**kw)
            self._mark_result = result

        async def execute(self, sql, *args):
            call = (sql, args)
            self.execute_calls.append(call)
            if sql.startswith("SET LOCAL"):
                return None
            if (
                "UPDATE kb_operations" in sql
                and "started_at = now()" in sql
            ):
                return self._mark_result
            if "UPDATE knowledge_bases" in sql and "status = $2" in sql:
                return self.transition_result
            return "UPDATE 1"

    # Terminal row (completed): duplicate delivery skipped.
    conn = _TakeoverConn("UPDATE 0")
    ok = await op_repo.acquire_run(conn, tenant_id=TENANT_ID, operation_id=TASK_ID)
    assert ok is False

    # Pending / running orphan: acquirable.
    conn = _TakeoverConn("UPDATE 1")
    ok = await op_repo.acquire_run(conn, tenant_id=TENANT_ID, operation_id=TASK_ID)
    assert ok is True


async def test_complete_operation_sql_has_terminal_state_guard():
    """complete_operation_in_tx refuses to overwrite a terminal row: the
    UPDATE carries ``status NOT IN ('completed','failed')`` so interleaved
    at-least-once deliveries can't clobber each other's terminal state.
    The first completion wins; a second UPDATE matches 0 rows → False."""
    from app.repositories import operation as op_repo

    conn = _RebuildConn()
    ok = await op_repo.complete_operation(
        conn, tenant_id=TENANT_ID, operation_id=TASK_ID,
        result={"total": 1}, status="completed",
    )
    assert ok is True
    sql, args = conn.execute_calls[-1]
    assert "SET status = $2" in sql
    assert "completed_at = now()" in sql
    assert "status NOT IN" in sql
    for terminal in ("'completed'", "'failed'"):
        assert terminal in sql
    assert args[0] == uuid.UUID(TASK_ID)
    assert args[1] == "completed"


# ── rebuild event payload contract ────────────────────────────────────────────


async def test_rebuild_outbox_event_carries_task_id():
    """The kb.rebuild outbox payload carries task_id so the rebuild consumer
    can advance/close the kb_operations row (prevents tasks stuck pending)."""
    conn = _RebuildConn()
    servicer = _make_servicer(conn)
    await servicer._rebuild_kb(_req(), _ServicerContext())

    insert_outbox = [
        (s, a) for s, a in conn.fetchrow_calls if "INSERT INTO kb_outbox_events" in s
    ]
    assert len(insert_outbox) == 1
    sql, args = insert_outbox[0]
    assert args[0] == "knowledge_bases"     # aggregate_type
    assert args[1] == uuid.UUID(KB_ID)       # aggregate_id
    assert args[2] == "kb.rebuild"           # event_type
    assert args[3] == uuid.UUID(TENANT_ID)
    payload = json.loads(args[4])
    assert payload == {
        "kb_id": KB_ID,
        "tenant_id": TENANT_ID,
        "task_id": TASK_ID,
    }


async def test_rebuild_task_payload_records_kb_settings():
    """The kb_operations payload snapshot carries the KB's embedding model
    and chunk_size at request time (the consumer re-reads the KB row
    anyway — this snapshot is for operation forensics). resource_type has
    no dedicated column and is folded into the payload."""
    conn = _RebuildConn()
    servicer = _make_servicer(conn)
    await servicer._rebuild_kb(_req(), _ServicerContext())

    insert_task = [
        (s, a) for s, a in conn.fetchrow_calls if "INSERT INTO kb_operations" in s
    ]
    assert len(insert_task) == 1
    sql, args = insert_task[0]
    assert args[0] == uuid.UUID(TENANT_ID)
    assert args[1]  # idempotency_key (client uuid)
    assert args[2] == "kb.rebuild"          # operation_type
    assert args[3] == uuid.UUID(KB_ID)      # resource_id
    payload = json.loads(args[5])
    assert payload["kb_id"] == KB_ID
    assert payload["embedding_model"] == "bge-m3"
    assert payload["chunk_size"] == 512
    assert payload["resource_type"] == "knowledge_base"


# ── mutex matrix: write servicers vs a rebuilding KB ─────────────────────────
#
# B6 #24: while a full-KB rebuild runs (KB status='rebuilding'), every
# write servicer short-circuits at its KB gate with FAILED_PRECONDITION
# ("knowledge base is rebuilding") before any business write. The gate
# read is a plain get_kb — the 'rebuilding' row must survive the earlier
# idempotency-replay lookup (existing_task=None keeps the flow honest).


def _rebuilding_conn():
    return _RebuildConn(kb_row=_kb_row(status="rebuilding"))


async def test_mutex_update_kb_rejected_while_rebuilding():
    conn = _rebuilding_conn()
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._update_kb(
            kb_pb.UpdateKBRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID,
                idempotency_key=str(uuid.uuid4()), name="x",
            ),
            ctx,
        )
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    # No KB UPDATE ran (the gate precedes the business write).
    assert not any(
        "UPDATE knowledge_bases" in s and "RETURNING" in s
        for s, _ in conn.fetchrow_calls
    )


async def test_mutex_delete_kb_rejected_while_rebuilding():
    conn = _rebuilding_conn()
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._delete_kb(
            kb_pb.DeleteKBRequest(tenant_id=TENANT_ID, kb_id=KB_ID), ctx
        )
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    # No soft-delete UPDATE, no audit insert.
    assert not any("status = 'deleted'" in s for s, _ in conn.execute_calls)
    assert not any("INSERT INTO kb_audit_log" in s for s, _ in conn.fetchrow_calls)


async def test_mutex_upload_url_rejected_while_rebuilding():
    """GetDocumentUploadURL: the gate aborts BEFORE the presigned-PUT
    signing, so no object-store round-trip happens at all."""
    store_calls: list[str] = []

    class _RecordingStore:
        async def presigned_put_object(self, **kwargs):
            store_calls.append("presigned_put_object")
            raise AssertionError("must not be reached")

    conn = _rebuilding_conn()
    servicer = KBServiceServicer(pool=_Pool(conn), object_store=_RecordingStore())
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._get_document_upload_url(
            kb_pb.GetDocumentUploadURLRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID, file_name="a.pdf",
                file_type="pdf", file_size_bytes=1024,
                checksum_sha256="abc", idempotency_key=str(uuid.uuid4()),
            ),
            ctx,
        )
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    assert store_calls == []
    # No document row reserved.
    assert not any("INSERT INTO kb_documents" in s for s, _ in conn.fetchrow_calls)


class _FakeUploadStore:
    """Minimal object store for the complete-path mutex test: HeadObject
    succeeds (size 1024) so the flow reaches the in-tx KB gate."""

    async def stat_object(self, *, key):
        return {"size": 1024}


def _complete_req(**over):
    req = dict(
        tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=str(uuid.uuid4()),
        file_name="a.pdf", file_type="pdf",
        checksum_sha256="abc123", custom_metadata="",
        idempotency_key=str(uuid.uuid4()),
    )
    req.update(over)
    return kb_pb.CompleteDocumentUploadRequest(**req)


async def test_mutex_complete_document_upload_rejected_while_rebuilding():
    """CompleteDocumentUpload: the KB gate sits INSIDE the atomic write tx;
    a rebuilding KB aborts before any doc/parse-status write or outbox
    event (a document completing upload cannot join the rebuild)."""
    conn = _rebuilding_conn()
    servicer = KBServiceServicer(pool=_Pool(conn), object_store=_FakeUploadStore())
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._complete_document_upload(_complete_req(), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    assert not any("INSERT INTO kb_documents" in s for s, _ in conn.fetchrow_calls)
    assert not any("INSERT INTO kb_operations" in s for s, _ in conn.fetchrow_calls)
    assert not any("INSERT INTO kb_outbox_events" in s for s, _ in conn.fetchrow_calls)


async def test_mutex_delete_document_rejected_while_rebuilding():
    conn = _rebuilding_conn()
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._delete_document(
            kb_pb.DeleteDocumentRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=str(uuid.uuid4()),
            ),
            ctx,
        )
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    assert not any("DELETE FROM kb_chunks" in s for s, _ in conn.execute_calls)
    assert not any("INSERT INTO kb_audit_log" in s for s, _ in conn.fetchrow_calls)


async def test_mutex_update_kb_permissions_rejected_while_rebuilding():
    conn = _rebuilding_conn()
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._update_kb_permissions(
            kb_pb.UpdateKBPermissionsRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID,
                idempotency_key=str(uuid.uuid4()), public_read=True,
            ),
            ctx,
        )
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    # No permission upsert, no audit.
    assert not any("INSERT INTO kb_permissions" in s for s, _ in conn.execute_calls)
    assert not any("INSERT INTO kb_audit_log" in s for s, _ in conn.fetchrow_calls)
