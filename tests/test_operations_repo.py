"""Tests for the kb_operations repository + migration DDL (KB-SPLIT-P1B,
issue-051, ADR 0001 §D4).

kb_operations is kb-local idempotency state: it replaces the shared
async_tasks table for CreateKB / UpdateKB / CompleteDocumentUpload /
ReparseDocument / RebuildKB / UpdateKBConfig / UpdateKBPermissions replay
(SPEC §6.1, §6.4). kb no longer writes async_tasks; task progress is
visible externally only through outbox events.

Mirrors test_rebuild_kb.py (mock asyncpg conn, no real DB):

Migration DDL (010_kb_operations.sql contract):
- Composite UNIQUE(tenant_id, idempotency_key) — the same client key in
  different tenants never collides or replays each other's results.
- status CHECK (pending/running/completed/failed); operation_type /
  status / result columns exist (issue-051 column requirements).
- RLS enabled + forced, double PERMISSIVE policy shape (same as 009).

Repository SQL contracts:
- find_by_idempotency_key: tenant + key + operation_type filter.
- create_operation_in_tx: column order binds (tenant, key, operation_type,
  resource_id, status, payload); resource_type folded into payload.
- complete_operation_in_tx: terminal-state guard (completed/failed).
- acquire_run: non-terminal guard (pending pickup OR running orphan
  takeover under JetStream redelivery — no lease column).
- revive_operation_in_tx: failed-only reset to pending.
- delete_kb_create_replay_records: kb.create rows deleted by resource.

Behavior (stateful fake conn):
- Cross-tenant isolation: the same idempotency_key in different tenants
  coexists (composite UNIQUE) and never replays the other tenant's row.
"""
import json
import os
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)

from app.repositories import operation as op_repo

TENANT_A = "11111111-1111-1111-1111-111111111111"
TENANT_B = "22222222-2222-2222-2222-222222222222"
OP_ID = "44444444-4444-4444-4444-444444444444"
KB_ID = "33333333-3333-3333-3333-333333333333"
MIGRATION = Path(_SERVICE_ROOT) / "migrations" / "010_kb_operations.sql"


def _op_row(op_id=OP_ID, tenant=TENANT_A, status="pending", result=None):
    return {
        "id": uuid.UUID(op_id),
        "tenant_id": uuid.UUID(tenant),
        "idempotency_key": "key-1",
        "operation_type": "kb.create",
        "resource_id": uuid.UUID(KB_ID),
        "status": status,
        "payload": {"resource_type": "knowledge_base"},
        "result": result,
        "started_at": None,
        "completed_at": None,
        "created_at": None,
        "updated_at": None,
    }


class _RecordingConn:
    """Recording fake conn: returns canned rows by SQL keyword."""

    def __init__(self, *, find_row=None, revive_row="unset", execute_result="UPDATE 1"):
        self.find_row = find_row
        self.revive_row = None if revive_row == "unset" else revive_row
        self._revive_set = revive_row != "unset"
        self.execute_result = execute_result
        self.execute_calls: list[tuple] = []
        self.fetchrow_calls: list[tuple] = []

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self

        return _tx()

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        return self.execute_result

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        if "FROM kb_operations" in sql and "idempotency_key" in sql:
            return self.find_row
        if "UPDATE kb_operations" in sql and "RETURNING" in sql:
            return self.revive_row if self._revive_set else _op_row()
        return None

    async def fetch(self, sql, *args):
        return []

    async def fetchval(self, sql, *args):
        return None


# ── migration DDL contract ────────────────────────────────────────────────────


def test_migration_has_composite_unique_constraint():
    sql = MIGRATION.read_text(encoding="utf-8")
    # The composite constraint is the issue-051 AC: the same client key in
    # different tenants never collides (and can never replay each other).
    assert "CONSTRAINT kb_operations_tenant_id_idempotency_key_key" in sql
    assert "UNIQUE (tenant_id, idempotency_key)" in sql


def test_migration_has_required_columns_and_status_check():
    sql = MIGRATION.read_text(encoding="utf-8")
    for col in ("operation_type", "status", "result", "resource_id",
                "idempotency_key", "payload"):
        assert f"{col}" in sql, f"missing column {col}"
    assert "CHECK (status IN ('pending', 'running', 'completed', 'failed'))" in sql
    # kb.create replay-row cleanup + #052 resource filtering.
    assert "idx_kb_operations_resource" in sql
    assert "ON kb_operations(tenant_id, operation_type, resource_id)" in sql


def test_migration_enables_rls_with_double_policy_shape():
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "ENABLE ROW LEVEL SECURITY" in sql
    assert "FORCE ROW LEVEL SECURITY" in sql
    # Same effective shape as 009 kb_outbox_events (platform_bypass + self).
    assert "CREATE POLICY kb_operations_platform_bypass ON kb_operations" in sql
    assert "CREATE POLICY kb_operations_self ON kb_operations" in sql
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON kb_operations TO ani_app" in sql


# ── find_by_idempotency_key ───────────────────────────────────────────────────


async def test_find_by_idempotency_key_sql_and_args():
    conn = _RecordingConn(find_row=None)
    row = await op_repo.find_by_idempotency_key(
        conn, tenant_id=TENANT_A, idempotency_key="key-1",
        operation_type="kb.update",
    )
    assert row is None
    sql, args = conn.fetchrow_calls[0]
    assert "FROM kb_operations" in sql
    assert "tenant_id = $1 AND idempotency_key = $2" in sql
    # The operation_type filter keeps cross-operation key reuse from
    # replaying another operation's result.
    assert "($3::text IS NULL OR operation_type = $3)" in sql
    assert args == (uuid.UUID(TENANT_A), "key-1", "kb.update")


# ── create_operation_in_tx ────────────────────────────────────────────────────


async def test_create_operation_in_tx_sql_and_arg_order():
    conn = _RecordingConn(execute_result="UPDATE 1")
    # INSERT goes through fetchrow (RETURNING); stub a row return.
    conn.fetchrow_calls.clear()
    original_fetchrow = conn.fetchrow

    async def fetchrow(sql, *args):
        conn.fetchrow_calls.append((sql, args))
        if "INSERT INTO kb_operations" in sql:
            return _op_row()
        return await original_fetchrow(sql, *args)

    conn.fetchrow = fetchrow

    row = await op_repo.create_operation_in_tx(
        conn,
        tenant_id=TENANT_A,
        idempotency_key="key-1",
        operation_type="kb.rebuild",
        resource_type="knowledge_base",
        resource_id=KB_ID,
        payload={"kb_id": KB_ID, "chunk_size": 512},
        status="pending",
    )
    assert row["id"] == uuid.UUID(OP_ID)
    sql, args = conn.fetchrow_calls[0]
    assert "INSERT INTO kb_operations" in sql
    assert args[0] == uuid.UUID(TENANT_A)
    assert args[1] == "key-1"
    assert args[2] == "kb.rebuild"
    assert args[3] == uuid.UUID(KB_ID)
    assert args[4] == "pending"
    payload = json.loads(args[5])
    assert payload["kb_id"] == KB_ID
    # resource_type has no dedicated column — folded into the payload.
    assert payload["resource_type"] == "knowledge_base"
    # Caller payload keys are never overwritten by the fold.
    conn.fetchrow_calls.clear()
    await op_repo.create_operation_in_tx(
        conn, tenant_id=TENANT_A, idempotency_key="key-2",
        operation_type="kb.parse", resource_type="kb_document",
        payload={"resource_type": "explicit-wins"},
    )
    payload2 = json.loads(conn.fetchrow_calls[0][1][5])
    assert payload2["resource_type"] == "explicit-wins"


async def test_create_operation_in_tx_resource_id_optional():
    conn = _RecordingConn()
    original_fetchrow = conn.fetchrow

    async def fetchrow(sql, *args):
        conn.fetchrow_calls.append((sql, args))
        if "INSERT INTO kb_operations" in sql:
            return _op_row()
        return await original_fetchrow(sql, *args)

    conn.fetchrow = fetchrow
    await op_repo.create_operation_in_tx(
        conn, tenant_id=TENANT_A, idempotency_key="key-3",
        operation_type="kb.update",
    )
    sql, args = conn.fetchrow_calls[0]
    assert args[3] is None  # no resource_id → NULL
    assert json.loads(args[5]) == {}  # empty payload → {}


# ── complete_operation_in_tx ──────────────────────────────────────────────────


async def test_complete_operation_in_tx_sql_and_terminal_guard():
    conn = _RecordingConn(execute_result="UPDATE 1")
    ok = await op_repo.complete_operation_in_tx(
        conn, tenant_id=TENANT_A, operation_id=OP_ID,
        result={"total": 1}, status="completed",
    )
    assert ok is True
    sql, args = conn.execute_calls[-1]
    assert "UPDATE kb_operations" in sql
    assert "SET status = $2" in sql
    assert "completed_at = now()" in sql
    assert "status NOT IN ('completed', 'failed')" in sql
    assert args[0] == uuid.UUID(OP_ID)
    assert args[1] == "completed"
    assert json.loads(args[2]) == {"total": 1}


async def test_complete_operation_in_tx_zero_rows_returns_false():
    conn = _RecordingConn(execute_result="UPDATE 0")
    ok = await op_repo.complete_operation_in_tx(
        conn, tenant_id=TENANT_A, operation_id=OP_ID, status="failed",
    )
    assert ok is False


# ── acquire_run ───────────────────────────────────────────────────────────────


async def test_acquire_run_sql_and_args():
    conn = _RecordingConn(execute_result="UPDATE 1")
    ok = await op_repo.acquire_run(conn, tenant_id=TENANT_A, operation_id=OP_ID)
    assert ok is True
    sql, args = conn.execute_calls[-1]
    assert "UPDATE kb_operations" in sql
    assert "status = 'running'" in sql
    assert "started_at = now()" in sql
    # No lease column: one conditional covers pending first-pickup AND
    # running orphan takeover (JetStream redelivery is the takeover signal).
    assert "status NOT IN ('completed', 'failed')" in sql
    assert "lease_until" not in sql
    # #054: explicit tenant filter (RLS/GUC is gone).
    assert "tenant_id = $2" in sql
    assert args == (uuid.UUID(OP_ID), uuid.UUID(TENANT_A))


async def test_acquire_run_terminal_row_returns_false():
    conn = _RecordingConn(execute_result="UPDATE 0")
    ok = await op_repo.acquire_run(conn, tenant_id=TENANT_A, operation_id=OP_ID)
    assert ok is False


# ── revive_operation_in_tx ────────────────────────────────────────────────────


async def test_revive_operation_in_tx_failed_only_conditional():
    conn = _RecordingConn(revive_row=_op_row(status="pending"))
    row = await op_repo.revive_operation_in_tx(
        conn, tenant_id=TENANT_A, operation_id=OP_ID,
    )
    assert row is not None
    sql, args = conn.fetchrow_calls[0]
    assert "UPDATE kb_operations" in sql
    assert "SET status = 'pending'" in sql
    assert "result = NULL" in sql
    assert "completed_at = NULL" in sql
    # Only a failed row revives — a concurrent status change between the
    # caller's lookup and this UPDATE is detected, not overwritten.
    # #054: explicit tenant filter (RLS/GUC is gone).
    assert "WHERE id = $1 AND tenant_id = $2 AND status = 'failed'" in sql
    assert args == (uuid.UUID(OP_ID), uuid.UUID(TENANT_A))


async def test_revive_operation_in_tx_raced_out_returns_none():
    conn = _RecordingConn(revive_row=None)
    row = await op_repo.revive_operation_in_tx(
        conn, tenant_id=TENANT_A, operation_id=OP_ID,
    )
    assert row is None


# ── get_operation / delete_kb_create_replay_records ──────────────────────────


async def test_get_operation_selects_by_id():
    conn = _RecordingConn(find_row=None)
    conn.find_row = _op_row()
    row = await op_repo.get_operation(conn, tenant_id=TENANT_A, operation_id=OP_ID)
    assert row["id"] == uuid.UUID(OP_ID)
    sql, args = conn.fetchrow_calls[0]
    assert "FROM kb_operations" in sql
    # Explicit tenant filter (issue-052 AC) on top of the RLS context.
    assert "WHERE id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(OP_ID), uuid.UUID(TENANT_A))


async def test_delete_kb_create_replay_records_sql_and_args():
    conn = _RecordingConn()
    await op_repo.delete_kb_create_replay_records(
        conn, tenant_id=TENANT_A, kb_id=KB_ID,
    )
    sql, args = conn.execute_calls[-1]
    assert "DELETE FROM kb_operations" in sql
    assert "operation_type = 'kb.create'" in sql
    assert "resource_id = $2" in sql
    assert args == (uuid.UUID(TENANT_A), uuid.UUID(KB_ID))


# ── cross-tenant isolation (stateful fake conn) ───────────────────────────────


class _StatefulOpsConn:
    """Stateful fake conn simulating kb_operations' composite UNIQUE.

    Rows live in a dict keyed by (tenant_id, idempotency_key) — exactly the
    UNIQUE(tenant_id, idempotency_key) index — so the same key in different
    tenants coexists while a duplicate within one tenant raises
    UniqueViolationError, like real PG.
    """

    def __init__(self):
        self.rows: dict[tuple[uuid.UUID, str], dict] = {}

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self

        return _tx()

    async def execute(self, sql, *args):
        return "UPDATE 1"

    async def fetchrow(self, sql, *args):
        if "FROM kb_operations" in sql and "idempotency_key" in sql:
            tenant, key = args[0], args[1]
            return self.rows.get((tenant, key))
        if "INSERT INTO kb_operations" in sql:
            tenant, key, op_type, resource_id, status = args[:5]
            pair = (tenant, key)
            if pair in self.rows:
                raise asyncpg.UniqueViolationError(
                    "duplicate key value violates unique constraint "
                    '"kb_operations_tenant_id_idempotency_key_key"'
                )
            row = {
                "id": uuid.uuid4(),
                "tenant_id": tenant,
                "idempotency_key": key,
                "operation_type": op_type,
                "resource_id": resource_id,
                "status": status,
                "payload": json.loads(args[5]),
                "result": None,
            }
            self.rows[pair] = row
            return row
        return None

    async def fetch(self, sql, *args):
        return []

    async def fetchval(self, sql, *args):
        return None


async def test_same_key_different_tenants_coexist_and_isolate():
    """AC (issue-051): the composite UNIQUE(tenant_id, idempotency_key)
    lets tenant A and tenant B use the same client key without colliding,
    and each tenant's replay lookup only ever sees its own row."""
    conn = _StatefulOpsConn()

    row_a = await op_repo.create_operation_in_tx(
        conn, tenant_id=TENANT_A, idempotency_key="shared-key",
        operation_type="kb.create", resource_type="knowledge_base",
        resource_id=KB_ID, payload={"kb_id": KB_ID},
    )
    # Same key, different tenant: INSERT succeeds (no UNIQUE collision).
    row_b = await op_repo.create_operation_in_tx(
        conn, tenant_id=TENANT_B, idempotency_key="shared-key",
        operation_type="kb.create", resource_type="knowledge_base",
        resource_id=KB_ID, payload={"kb_id": KB_ID},
    )
    assert row_a["id"] != row_b["id"]
    assert row_a["tenant_id"] == uuid.UUID(TENANT_A)
    assert row_b["tenant_id"] == uuid.UUID(TENANT_B)

    # Replay lookups are tenant-scoped: A never sees B's row and vice versa.
    found_a = await op_repo.find_by_idempotency_key(
        conn, tenant_id=TENANT_A, idempotency_key="shared-key",
        operation_type="kb.create",
    )
    found_b = await op_repo.find_by_idempotency_key(
        conn, tenant_id=TENANT_B, idempotency_key="shared-key",
        operation_type="kb.create",
    )
    assert found_a["id"] == row_a["id"]
    assert found_b["id"] == row_b["id"]

    # Same tenant + same key: the composite UNIQUE fires (duplicate insert).
    import pytest

    with pytest.raises(asyncpg.UniqueViolationError):
        await op_repo.create_operation_in_tx(
            conn, tenant_id=TENANT_A, idempotency_key="shared-key",
            operation_type="kb.create", resource_id=KB_ID,
        )


async def test_duplicate_key_within_tenant_raises_unique_violation():
    conn = _StatefulOpsConn()
    await op_repo.create_operation_in_tx(
        conn, tenant_id=TENANT_A, idempotency_key="dup",
        operation_type="kb.create",
    )
    import pytest

    with pytest.raises(asyncpg.UniqueViolationError):
        await op_repo.create_operation_in_tx(
            conn, tenant_id=TENANT_A, idempotency_key="dup",
            operation_type="kb.create",
        )
