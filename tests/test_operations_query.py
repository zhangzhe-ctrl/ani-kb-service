"""Tests for the ListOperations / GetOperation query RPCs (KB-SPLIT-P1C,
issue-052, ADR 0001 §D4-2).

These two RPCs are the temporary progress-visibility surface over
kb_operations until task-service subscribes to kb outbox events and takes
over task display (ADR §五-阶段6). Covered here:

Repository SQL contracts (mock conn, no real DB):
- list_operations: explicit tenant_id = $1 filter (issue-052 AC — the
  durable isolation once #054 replaces RLS), optional operation_type /
  resource_id / status filters, composite keyset cursor
  (created_at, id) DESC, plain LIMIT.
- invalid cursor → InvalidCursorError (servicer maps to INVALID_ARGUMENT).

Servicer behavior (fake pool, same pattern as test_audit_logging.py):
- ListOperations: item mapping (JSONB payload/result → JSON strings,
  NULL resource_id → "", timestamps), next_cursor boundary (exactly
  ``limit`` rows → cursor; fewer → ""), status filter passthrough,
  missing tenant → INVALID_ARGUMENT, no pool → FAILED_PRECONDITION,
  invalid cursor → INVALID_ARGUMENT.
- GetOperation: hit maps the row, miss → NOT_FOUND.
- Cross-tenant isolation (stateful fake): tenant B's list is empty and
  tenant B's GetOperation of a tenant-A row is NOT_FOUND.
"""
import os
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import grpc
import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.api.grpc_server import KBServiceServicer
from app.generated.common.v1 import common_pb2
from app.generated.kb.v1 import kb_service_pb2 as kb_pb
from app.repositories import operation as op_repo
from app.repositories.cursor import InvalidCursorError

TENANT_A = "11111111-1111-1111-1111-111111111111"
TENANT_B = "22222222-2222-2222-2222-222222222222"
OP_ID = "44444444-4444-4444-4444-444444444444"
KB_ID = "33333333-3333-3333-3333-333333333333"


def _op_row(op_id=OP_ID, tenant=TENANT_A, status="running", result=None, **over):
    row = {
        "id": uuid.UUID(str(op_id)),
        "tenant_id": uuid.UUID(tenant),
        "idempotency_key": "key-1",
        "operation_type": "kb.rebuild",
        "resource_id": uuid.UUID(KB_ID),
        "status": status,
        "payload": '{"resource_type": "knowledge_base"}',
        "result": result,
        "started_at": datetime(2026, 9, 1, 0, 0, 1, tzinfo=timezone.utc),
        "completed_at": None,
        "created_at": datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 9, 1, 0, 0, 2, tzinfo=timezone.utc),
    }
    row.update(over)
    return row


class _RecordingConn:
    """Recording fake conn: canned fetch()/fetchrow() results + call capture."""

    def __init__(self, *, list_rows=None, get_row="unset"):
        self._list_rows = list_rows or []
        self._get_row = None if get_row == "unset" else get_row
        self.fetch_calls: list[tuple] = []
        self.fetchrow_calls: list[tuple] = []

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self

        return _tx()

    async def execute(self, sql, *args):
        return "SET LOCAL"

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        return self._list_rows

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        return self._get_row


# ── repository SQL contracts ──────────────────────────────────────────────────


async def test_list_operations_sql_tenant_filter_and_pagination():
    conn = _RecordingConn(list_rows=[])
    rows = await op_repo.list_operations(conn, tenant_id=TENANT_A)
    assert rows == []
    sql, args = conn.fetch_calls[0]
    assert "FROM kb_operations" in sql
    # Issue-052 AC: explicit tenant filter (durable beyond the RLS context).
    assert "WHERE tenant_id = $1" in sql
    assert "ORDER BY created_at DESC, id DESC" in sql
    assert "LIMIT $2" in sql
    assert args == (uuid.UUID(TENANT_A), 20)


async def test_list_operations_filters_and_cursor_args():
    conn = _RecordingConn(list_rows=[])
    cursor = f"{datetime(2026, 9, 1, tzinfo=timezone.utc).isoformat()}|{OP_ID}"
    await op_repo.list_operations(
        conn,
        tenant_id=TENANT_A,
        operation_type="kb.rebuild",
        resource_id=KB_ID,
        status="completed",
        limit=50,
        cursor=cursor,
    )
    sql, args = conn.fetch_calls[0]
    assert "operation_type = $2" in sql
    assert "resource_id = $3" in sql
    assert "status = $4" in sql
    # Composite keyset over (created_at, id).
    assert "(created_at, id) < ($5, $6)" in sql
    assert "LIMIT $7" in sql
    assert args[0] == uuid.UUID(TENANT_A)
    assert args[1] == "kb.rebuild"
    assert args[2] == uuid.UUID(KB_ID)
    assert args[3] == "completed"
    assert args[4] == datetime(2026, 9, 1, tzinfo=timezone.utc)
    assert args[5] == uuid.UUID(OP_ID)
    assert args[6] == 50


async def test_list_operations_invalid_cursor_raises():
    conn = _RecordingConn(list_rows=[])
    with pytest.raises(InvalidCursorError):
        await op_repo.list_operations(
            conn, tenant_id=TENANT_A, cursor="not-a-cursor"
        )


# ── servicer fakes ────────────────────────────────────────────────────────────


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


class _ServicerContext:
    """grpc.ServicerContext stand-in — abort raises (same as audit tests)."""

    def __init__(self):
        self.aborted = None

    def abort(self, code, message):
        self.aborted = (code, message)
        raise RuntimeError(f"aborted: {code} {message}")

    def invocation_metadata(self):
        return ()


def _make_servicer(conn):
    return KBServiceServicer(pool=_Pool(conn))


def _list_req(**over):
    defaults = dict(tenant_id=TENANT_A, page=common_pb2.CursorPageRequest(limit=20))
    defaults.update(over)
    return kb_pb.ListOperationsRequest(**defaults)


def _get_req(**over):
    defaults = dict(tenant_id=TENANT_A, operation_id=OP_ID)
    defaults.update(over)
    return kb_pb.GetOperationRequest(**defaults)


# ── ListOperations behavior ───────────────────────────────────────────────────


async def test_list_operations_maps_items():
    row = _op_row(result='{"total": 7}')
    conn = _RecordingConn(list_rows=[row])
    servicer = _make_servicer(conn)

    resp = await servicer._list_operations(_list_req(), _ServicerContext())

    assert len(resp.items) == 1
    e = resp.items[0]
    assert e.tenant_id == TENANT_A
    assert e.id == OP_ID
    assert e.idempotency_key == "key-1"
    assert e.operation_type == "kb.rebuild"
    assert e.resource_id == KB_ID
    assert e.status == "running"
    assert e.payload == '{"resource_type": "knowledge_base"}'   # JSON string passthrough
    assert e.result == '{"total": 7}'
    assert e.started_at.ToJsonString().startswith("2026-09-01")
    assert e.completed_at.ToJsonString() == "1970-01-01T00:00:00Z"  # NULL → unset
    assert e.created_at.ToJsonString().startswith("2026-09-01")


async def test_list_operations_next_cursor_boundary():
    rows = [
        _op_row(
            op_id=uuid.uuid5(uuid.NAMESPACE_OID, str(i)),
            created_at=datetime(2026, 9, 1, 0, 0, i, tzinfo=timezone.utc),
        )
        for i in range(1, 3)
    ]
    conn = _RecordingConn(list_rows=rows)
    servicer = _make_servicer(conn)

    resp = await servicer._list_operations(
        _list_req(page=common_pb2.CursorPageRequest(limit=2)), _ServicerContext()
    )
    assert len(resp.items) == 2
    expected = f"{rows[-1]['created_at'].isoformat().replace('+00:00', 'Z')}|{rows[-1]['id']}"
    assert resp.next_cursor == expected

    # Fewer rows than limit → no next page.
    conn2 = _RecordingConn(list_rows=[_op_row()])
    servicer2 = _make_servicer(conn2)
    resp2 = await servicer2._list_operations(
        _list_req(page=common_pb2.CursorPageRequest(limit=5)), _ServicerContext()
    )
    assert resp2.next_cursor == ""


async def test_list_operations_filters_passthrough(monkeypatch):
    """operation_type / resource_id / status filters and limit reach the
    repository as keyword arguments."""
    row = _op_row(status="failed")
    conn = _RecordingConn()
    servicer = _make_servicer(conn)
    captured = {}

    async def spy(_conn, **kwargs):
        captured.update(kwargs)
        return [row]

    monkeypatch.setattr(op_repo, "list_operations", spy)

    resp = await servicer._list_operations(
        _list_req(status="failed", operation_type="kb.rebuild", resource_id=KB_ID),
        _ServicerContext(),
    )
    assert captured["status"] == "failed"
    assert captured["operation_type"] == "kb.rebuild"
    assert captured["resource_id"] == KB_ID
    assert captured["limit"] == 20
    assert resp.items[0].status == "failed"


async def test_list_operations_empty():
    conn = _RecordingConn(list_rows=[])
    servicer = _make_servicer(conn)

    resp = await servicer._list_operations(_list_req(), _ServicerContext())
    assert list(resp.items) == []
    assert resp.next_cursor == ""


async def test_list_operations_missing_tenant_invalid_argument():
    servicer = _make_servicer(_RecordingConn())
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError):
        await servicer._list_operations(_list_req(tenant_id=""), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.INVALID_ARGUMENT


async def test_list_operations_no_pool_failed_precondition():
    servicer = KBServiceServicer()
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError):
        await servicer._list_operations(_list_req(), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION


async def test_list_operations_invalid_cursor_invalid_argument():
    conn = _RecordingConn(list_rows=[])
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError):
        await servicer._list_operations(
            _list_req(page=common_pb2.CursorPageRequest(limit=20, cursor="bad")),
            ctx,
        )
    assert ctx.aborted == (grpc.StatusCode.INVALID_ARGUMENT, "invalid page cursor")


async def test_list_operations_limit_out_of_bounds():
    servicer = _make_servicer(_RecordingConn())
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError):
        await servicer._list_operations(
            _list_req(page=common_pb2.CursorPageRequest(limit=101)), ctx
        )
    assert ctx.aborted[0] == grpc.StatusCode.INVALID_ARGUMENT


# ── GetOperation behavior ─────────────────────────────────────────────────────


async def test_get_operation_hit_maps_row():
    conn = _RecordingConn(get_row=_op_row(status="completed", result='{"ok": 1}'))
    servicer = _make_servicer(conn)

    op = await servicer._get_operation(_get_req(), _ServicerContext())

    assert op.id == OP_ID
    assert op.status == "completed"
    assert op.result == '{"ok": 1}'
    sql, args = conn.fetchrow_calls[0]
    # Explicit tenant filter (issue-052 AC) on top of the RLS context.
    assert "WHERE id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(OP_ID), uuid.UUID(TENANT_A))


async def test_get_operation_miss_not_found():
    conn = _RecordingConn(get_row=None)
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()

    with pytest.raises(RuntimeError):
        await servicer._get_operation(_get_req(), ctx)
    assert ctx.aborted == (grpc.StatusCode.NOT_FOUND, "operation not found")


async def test_get_operation_missing_tenant_invalid_argument():
    servicer = _make_servicer(_RecordingConn())
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError):
        await servicer._get_operation(_get_req(tenant_id=""), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.INVALID_ARGUMENT


async def test_get_operation_malformed_id_invalid_argument():
    """Malformed operation_id → INVALID_ARGUMENT, not UNKNOWN (review fix #2)."""
    conn = _RecordingConn(get_row=_op_row())
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()

    with pytest.raises(RuntimeError):
        await servicer._get_operation(_get_req(operation_id="not-a-uuid"), ctx)
    assert ctx.aborted == (
        grpc.StatusCode.INVALID_ARGUMENT,
        "operation_id must be a UUID",
    )
    assert conn.fetchrow_calls == []  # rejected before reaching the repo


async def test_list_operations_malformed_resource_id_invalid_argument():
    """Malformed resource_id filter → INVALID_ARGUMENT, not UNKNOWN (review fix #2)."""
    conn = _RecordingConn(list_rows=[_op_row()])
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()

    with pytest.raises(RuntimeError):
        await servicer._list_operations(_list_req(resource_id="not-a-uuid"), ctx)
    assert ctx.aborted == (
        grpc.StatusCode.INVALID_ARGUMENT,
        "resource_id must be a UUID",
    )
    assert conn.fetch_calls == []  # rejected before reaching the repo


# ── cross-tenant isolation (stateful fake) ────────────────────────────────────


class _StatefulTenantConn:
    """Stateful fake simulating the two isolation layers of kb_operations.

    - list (explicit tenant filter): fetch() filters stored rows by the
      tenant_id arg ($1) — the issue-052 explicit filter.
    - get (RLS context): fetchrow() returns a row only when its tenant
      matches the SET LOCAL app.current_tenant_id seen on this connection,
      mirroring the kb_operations_self RLS policy.
    """

    def __init__(self, rows):
        self.rows = rows
        self.current_tenant: uuid.UUID | None = None

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self

        return _tx()

    async def execute(self, sql, *args):
        if sql.startswith("SET LOCAL app.current_tenant_id = '"):
            self.current_tenant = uuid.UUID(
                sql.split("'")[1]
            )
        return "SET LOCAL"

    async def fetch(self, sql, *args):
        tenant = args[0]
        return [r for r in self.rows if r["tenant_id"] == tenant]

    async def fetchrow(self, sql, *args):
        op_id = args[0]
        for r in self.rows:
            if r["id"] == op_id:
                # RLS: the row is visible only within its own tenant context.
                return r if r["tenant_id"] == self.current_tenant else None
        return None


async def test_cross_tenant_list_returns_empty_and_get_not_found():
    """AC (issue-052): a cross-tenant list is empty and a cross-tenant
    GetOperation is NOT_FOUND — tenant B never sees tenant A's operations."""
    rows = [_op_row(), _op_row("55555555-5555-5555-5555-555555555555", tenant=TENANT_B)]
    conn = _StatefulTenantConn(rows)

    list_a = await op_repo.list_operations(conn, tenant_id=TENANT_A)
    assert {str(r["id"]) for r in list_a} == {OP_ID}
    list_b = await op_repo.list_operations(conn, tenant_id=TENANT_B)
    assert {str(r["id"]) for r in list_b} == {
        "55555555-5555-5555-5555-555555555555"
    }

    # Tenant A's operation is invisible to tenant B's RLS context.
    got_b_on_a = await op_repo.get_operation(
        conn, tenant_id=TENANT_B, operation_id=OP_ID
    )
    assert got_b_on_a is None

    servicer = _make_servicer(conn)
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError):
        await servicer._get_operation(_get_req(tenant_id=TENANT_B), ctx)
    assert ctx.aborted == (grpc.StatusCode.NOT_FOUND, "operation not found")
