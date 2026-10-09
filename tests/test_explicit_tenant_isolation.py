"""Cross-tenant negative tests for explicit tenant isolation (issue-054, KB-SPLIT-P1E).

[ADR §五-阶段1-5] RLS is gone; every repository query must carry an
explicit, parameterized ``tenant_id = $N`` filter. For every resource
(KB / Document / Session / Message / Chunk / Permission / AuditLog /
Operation) this file proves at unit level that another tenant's rows —
even ones sharing the same kb_id / doc_id / session_id / chunk_id /
operation_id UUIDs — are unreachable:

  1. Behaviour: when the DB applies the filter and no row is visible to
     the caller (the realistic cross-tenant case: the row exists but
     belongs to tenant B), each GET returns None / [] / defaults, each
     UPDATE/DELETE reports 0 rows (False / 0) — the values the servicer
     maps to NOT_FOUND / empty-set / idempotent-204.
  2. Durable guarantee: the SQL actually sent to the database carries the
     tenant filter with the caller's tenant UUID bound at the right
     positional parameter — so the DB *cannot* return tenant B's rows.
  3. The old RLS GUC is dead: no statement anywhere sets
     ``app.current_tenant_id`` / ``SET LOCAL`` (rls.set_tenant_context is
     a pure validation no-op).

Uses recording fake asyncpg connections — no real Postgres required
(same _RecordingConn pattern as test_b2_chunks_sessions_citations.py).
"""
import os
import sys
import uuid
from contextlib import asynccontextmanager

import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)

from app.repositories import audit as audit_repo
from app.repositories import chunk as chunk_repo
from app.repositories import document as document_repo
from app.repositories import knowledge_base as kb_repo
from app.repositories import message as message_repo
from app.repositories import operation as operation_repo
from app.repositories import permission as permission_repo
from app.repositories import rls as rls_repo

# Tenant A = the caller whose context every query must be scoped to.
TENANT_ID = "11111111-1111-1111-1111-111111111111"
# Tenant B = owns the row being probed; must never be reachable from A.
OTHER_TENANT_ID = "99999999-9999-9999-9999-999999999999"
KB_ID = "22222222-2222-2222-2222-222222222222"
DOC_ID = "33333333-3333-3333-3333-333333333333"
SESSION_ID = "44444444-4444-4444-4444-444444444444"
MSG_ID = "55555555-5555-5555-5555-555555555555"
CHUNK_ID = "66666666-6666-6666-6666-666666666666"
OP_ID = "88888888-8888-8888-8888-888888888888"
IDEMPOTENCY_KEY = "req-054-cross-tenant"


# ── recording fake conn ──────────────────────────────────────────────────────


class _RecordingConn:
    """Records fetch/fetchrow/execute/fetchval SQL + args; canned returns.

    Defaults model the cross-tenant case: the DB sees the explicit
    ``tenant_id`` filter, finds no visible row, and returns nothing.
    ``execute_result`` defaults to "UPDATE 1" but negative UPDATE/DELETE
    tests pass "UPDATE 0" / "DELETE 0" (filter matched 0 rows).
    """

    def __init__(
        self,
        *,
        rows=None,
        fetchrow_row=None,
        fetchval_result=None,
        execute_result="UPDATE 1",
    ):
        self._rows = rows or []
        self._fetchrow_row = fetchrow_row
        self._fetchval_result = fetchval_result
        self._execute_result = execute_result
        self.fetch_calls: list[tuple] = []
        self.fetchrow_calls: list[tuple] = []
        self.execute_calls: list[tuple] = []
        self.fetchval_calls: list[tuple] = []

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self

        return _tx()

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        return self._rows

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        if self._fetchrow_row is not None:
            return self._fetchrow_row
        return None

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        return self._execute_result

    async def fetchval(self, sql, *args):
        self.fetchval_calls.append((sql, args))
        return self._fetchval_result


def _assert_no_guc(conn: _RecordingConn) -> None:
    """#054: the RLS GUC is gone — nothing may touch app.current_tenant_id."""
    for sql, _ in (
        *conn.execute_calls,
        *conn.fetch_calls,
        *conn.fetchrow_calls,
        *conn.fetchval_calls,
    ):
        assert "app.current_tenant_id" not in sql
        assert "SET LOCAL" not in sql


# ── KB resource ─────────────────────────────────────────────────────────────


async def test_get_kb_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)  # filter hides tenant B's row
    row = await kb_repo.get_kb(conn, tenant_id=TENANT_ID, kb_id=KB_ID)
    assert row is None  # → servicer NOT_FOUND
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE id = $1 AND tenant_id = $2 AND status <> 'deleted'" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_list_kbs_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[], fetchval_result=0)
    rows, total = await kb_repo.list_kbs(conn, tenant_id=TENANT_ID)
    assert rows == [] and total == 0  # → empty ListKBs response
    total_sql, total_args = conn.fetchval_calls[0]
    assert "WHERE tenant_id = $1 AND status <> 'deleted'" in total_sql
    assert total_args == (uuid.UUID(TENANT_ID), "")
    page_sql, page_args = conn.fetch_calls[0]
    assert "WHERE tenant_id = $1 AND status <> 'deleted'" in page_sql
    assert page_args == (uuid.UUID(TENANT_ID), "", 20)
    _assert_no_guc(conn)


async def test_update_kb_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)
    row = await kb_repo.update_kb(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, name="hijacked"
    )
    assert row is None  # → NOT_FOUND, tenant B's name untouched
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE id = $1 AND tenant_id = $4 AND status <> 'deleted'" in sql
    assert args == (uuid.UUID(KB_ID), "hijacked", "", uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_soft_delete_kb_cross_tenant_returns_false():
    conn = _RecordingConn(execute_result="UPDATE 0")
    deleted = await kb_repo.soft_delete_kb(conn, tenant_id=TENANT_ID, kb_id=KB_ID)
    assert deleted is False  # → NOT_FOUND, no cross-tenant tombstone
    sql, args = conn.execute_calls[0]
    assert "WHERE id = $1 AND tenant_id = $2 AND status <> 'deleted'" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_delete_kb_cascade_cross_tenant_all_five_statements_scoped():
    conn = _RecordingConn(execute_result="UPDATE 0")
    deleted = await kb_repo.delete_kb_cascade(conn, tenant_id=TENANT_ID, kb_id=KB_ID)
    assert deleted is False  # final KB UPDATE matched 0 rows
    assert len(conn.execute_calls) == 5  # 4 child deletes + KB soft-delete
    # messages delete: outer + subquery both tenant-scoped
    msg_sql, msg_args = conn.execute_calls[0]
    assert "WHERE tenant_id = $2" in msg_sql
    assert "WHERE kb_id = $1 AND tenant_id = $2" in msg_sql
    assert msg_args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    # sessions / documents / chunks deletes: single-line, tenant-scoped
    for idx, table in ((1, "kb_sessions"), (2, "kb_documents"), (3, "kb_chunks")):
        sql, args = conn.execute_calls[idx]
        assert f"DELETE FROM {table} WHERE kb_id = $1 AND tenant_id = $2" in sql
        assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    kb_sql, kb_args = conn.execute_calls[4]
    assert "WHERE id = $1 AND tenant_id = $2 AND status <> 'deleted'" in kb_sql
    assert kb_args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_get_kb_status_cross_tenant_returns_none():
    conn = _RecordingConn(fetchval_result=None)
    status = await kb_repo.get_kb_status(conn, tenant_id=TENANT_ID, kb_id=KB_ID)
    assert status is None
    sql, args = conn.fetchval_calls[0]
    assert "WHERE id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_set_status_in_tx_cross_tenant_returns_false():
    conn = _RecordingConn(execute_result="UPDATE 0")
    ok = await kb_repo.set_status_in_tx(
        conn,
        tenant_id=TENANT_ID,
        kb_id=KB_ID,
        from_status="rebuilding",
        to_status="active",
    )
    assert ok is False  # tenant B's KB stays in its own lifecycle
    sql, args = conn.execute_calls[0]
    assert "WHERE id = $1 AND tenant_id = $4 AND status = $3" in sql
    assert args == (uuid.UUID(KB_ID), "active", "rebuilding", uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_update_config_in_tx_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)
    row = await kb_repo.update_config_in_tx(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, patch={"top_k": 10}
    )
    assert row is None
    # patch of 1 column → id=$1, top_k=$2, tenant filter is $3 (appended last)
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE id = $1 AND tenant_id = $3 AND status <> 'deleted'" in sql
    assert args == (uuid.UUID(KB_ID), 10, uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


# ── Document resource ────────────────────────────────────────────────────────


async def test_get_document_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)
    row = await document_repo.get_document(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID
    )
    assert row is None
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE id = $1 AND kb_id = $2 AND tenant_id = $3" in sql
    assert args == (uuid.UUID(DOC_ID), uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_list_documents_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[], fetchval_result=0)
    rows, total = await document_repo.list_documents(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID
    )
    assert rows == [] and total == 0
    total_sql, total_args = conn.fetchval_calls[0]
    assert "WHERE kb_id = $1 AND tenant_id = $2" in total_sql
    assert total_args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    page_sql, page_args = conn.fetch_calls[0]
    assert "WHERE kb_id = $1 AND tenant_id = $2" in page_sql
    assert page_args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID), 20)
    _assert_no_guc(conn)


async def test_update_parse_status_in_tx_cross_tenant_returns_false():
    conn = _RecordingConn(execute_result="UPDATE 0")
    ok = await document_repo.update_parse_status_in_tx(
        conn, tenant_id=TENANT_ID, doc_id=DOC_ID, parse_status="ready"
    )
    assert ok is False  # tenant B's parse pipeline untouched
    sql, args = conn.execute_calls[0]
    assert "WHERE id = $1 AND tenant_id = $5" in sql
    assert args[-1] == uuid.UUID(TENANT_ID)
    assert args[0] == uuid.UUID(DOC_ID)
    _assert_no_guc(conn)


async def test_reset_for_reparse_in_tx_cross_tenant_returns_false():
    conn = _RecordingConn(execute_result="UPDATE 0")
    ok = await document_repo.reset_for_reparse_in_tx(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID
    )
    assert ok is False
    sql, args = conn.execute_calls[0]
    assert "WHERE id = $1 AND kb_id = $2 AND tenant_id = $3" in sql
    assert args == (uuid.UUID(DOC_ID), uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_soft_delete_document_cross_tenant_returns_false():
    conn = _RecordingConn(execute_result="UPDATE 0")
    ok = await document_repo.soft_delete_document(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID
    )
    assert ok is False
    sql, args = conn.execute_calls[0]
    assert "WHERE id = $1 AND kb_id = $2 AND tenant_id = $3" in sql
    assert args == (uuid.UUID(DOC_ID), uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_list_documents_for_rebuild_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[])
    doc_ids = await document_repo.list_documents_for_rebuild(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID
    )
    assert doc_ids == []  # rebuild of a KB sees no foreign docs
    sql, args = conn.fetch_calls[0]
    assert "WHERE kb_id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


# ── Session resource ─────────────────────────────────────────────────────────


async def test_get_session_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)
    row = await message_repo.get_session(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, session_id=SESSION_ID
    )
    assert row is None
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE id = $1 AND kb_id = $2 AND tenant_id = $3" in sql
    assert args == (uuid.UUID(SESSION_ID), uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_delete_session_cross_tenant_returns_false():
    conn = _RecordingConn(fetchval_result=None)  # sessions DELETE ... RETURNING → no row
    ok = await message_repo.delete_session(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, session_id=SESSION_ID
    )
    assert ok is False  # idempotent 204, tenant B's session intact
    # messages delete: outer tenant filter + tenant-scoped session subquery
    msg_sql, msg_args = conn.execute_calls[0]
    assert "WHERE tenant_id = $3" in msg_sql
    assert "WHERE id = $1 AND kb_id = $2 AND tenant_id = $3" in msg_sql
    assert msg_args == (uuid.UUID(SESSION_ID), uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    # sessions delete: tenant filter on the row itself
    sess_sql, sess_args = conn.fetchval_calls[0]
    assert "WHERE id = $1 AND kb_id = $2 AND tenant_id = $3" in sess_sql
    assert sess_args == (uuid.UUID(SESSION_ID), uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_list_sessions_cross_tenant_join_and_subquery_scoped():
    conn = _RecordingConn(rows=[])
    rows = await message_repo.list_sessions(conn, tenant_id=TENANT_ID, kb_id=KB_ID)
    assert rows == []
    sql, args = conn.fetch_calls[0]
    # aggregate JOIN carries the tenant pairing...
    assert "ON m.session_id = s.id AND m.tenant_id = s.tenant_id" in sql
    # ...the correlated last_query subquery too...
    assert "WHERE m2.session_id = s.id AND m2.tenant_id = s.tenant_id" in sql
    # ...and the outer WHERE scopes both ids + tenant.
    assert "WHERE s.kb_id = $1 AND s.tenant_id = $2" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID), 20)
    _assert_no_guc(conn)


# ── Message resource ─────────────────────────────────────────────────────────


async def test_list_session_messages_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[])
    rows = await message_repo.list_session_messages(
        conn, tenant_id=TENANT_ID, session_id=SESSION_ID
    )
    assert rows == []
    sql, args = conn.fetch_calls[0]
    assert "WHERE session_id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(SESSION_ID), uuid.UUID(TENANT_ID), 20)
    _assert_no_guc(conn)


async def test_list_session_messages_paged_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[])
    rows = await message_repo.list_session_messages_paged(
        conn, tenant_id=TENANT_ID, session_id=SESSION_ID
    )
    assert rows == []
    sql, args = conn.fetch_calls[0]
    assert "WHERE session_id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(SESSION_ID), uuid.UUID(TENANT_ID), 100)
    _assert_no_guc(conn)


async def test_count_session_messages_cross_tenant_returns_zero():
    conn = _RecordingConn(fetchval_result=0)
    count = await message_repo.count_session_messages(
        conn, tenant_id=TENANT_ID, session_id=SESSION_ID
    )
    assert count == 0
    sql, args = conn.fetchval_calls[0]
    assert "WHERE session_id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(SESSION_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_list_citation_messages_paged_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[])
    rows = await message_repo.list_citation_messages_paged(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID
    )
    assert rows == []
    sql, args = conn.fetch_calls[0]
    # the JOIN pairs messages with sessions tenant-wise...
    assert "ON s.id = m.session_id AND s.tenant_id = m.tenant_id" in sql
    # ...so messages under a session of another tenant never appear.
    assert "WHERE s.kb_id = $1 AND s.tenant_id = $2 AND m.role = 'assistant'" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID), 20)
    _assert_no_guc(conn)


# ── Chunk resource ───────────────────────────────────────────────────────────


async def test_get_chunk_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)
    row = await chunk_repo.get_chunk(conn, tenant_id=TENANT_ID, chunk_id=CHUNK_ID)
    assert row is None
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(CHUNK_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_get_parent_chunk_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)
    row = await chunk_repo.get_parent_chunk(
        conn, tenant_id=TENANT_ID, parent_chunk_id=CHUNK_ID
    )
    assert row is None  # hybrid-retrieval backfill can't read tenant B
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE id = $1 AND chunk_type = 'parent' AND tenant_id = $2" in sql
    assert args == (uuid.UUID(CHUNK_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_list_parent_chunks_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[])
    rows = await chunk_repo.list_parent_chunks(
        conn, tenant_id=TENANT_ID, doc_id=DOC_ID
    )
    assert rows == []
    sql, args = conn.fetch_calls[0]
    assert "WHERE doc_id = $1 AND chunk_type = 'parent' AND tenant_id = $2" in sql
    assert args == (uuid.UUID(DOC_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_list_chunks_by_doc_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[])
    rows = await chunk_repo.list_chunks_by_doc(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID
    )
    assert rows == []
    sql, args = conn.fetch_calls[0]
    assert "WHERE kb_id = $1 AND doc_id = $2 AND tenant_id = $3" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(DOC_ID), uuid.UUID(TENANT_ID), 100)
    _assert_no_guc(conn)


async def test_count_chunks_by_doc_cross_tenant_returns_zero():
    conn = _RecordingConn(fetchval_result=0)
    count = await chunk_repo.count_chunks_by_doc(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID
    )
    assert count == 0
    sql, args = conn.fetchval_calls[0]
    assert "WHERE kb_id = $1 AND doc_id = $2 AND tenant_id = $3" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(DOC_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_delete_chunks_by_doc_cross_tenant_deletes_zero():
    conn = _RecordingConn(execute_result="DELETE 0")
    deleted = await chunk_repo.delete_chunks_by_doc(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID
    )
    assert deleted == 0  # reparse idempotency never wipes tenant B's chunks
    sql, args = conn.execute_calls[0]
    assert "WHERE kb_id = $1 AND doc_id = $2 AND tenant_id = $3" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(DOC_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_keyword_search_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[])
    rows = await chunk_repo.keyword_search(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, query="知识库检索", limit=5
    )
    assert rows == []
    # [0] is the pg_trgm similarity_threshold set_config — NOT a tenant GUC.
    sql, args = conn.fetch_calls[0]
    # token count varies with jieba availability; tenant is bound after
    # the tokens and kb_id, before limit: args = (*tokens, kb, tenant, limit)
    n_tokens = len(args) - 3
    assert n_tokens >= 1
    assert f"tenant_id = ${n_tokens + 2}" in sql
    assert args[-3] == uuid.UUID(KB_ID)
    assert args[-2] == uuid.UUID(TENANT_ID)
    assert args[-1] == 5
    _assert_no_guc(conn)


# ── Permission resource ─────────────────────────────────────────────────────


async def test_get_permissions_cross_tenant_returns_defaults():
    conn = _RecordingConn(fetchrow_row=None)  # tenant B's row invisible
    perms = await permission_repo.get_permissions(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID
    )
    # contract: no row → defaults, never 404 and never tenant B's ACL
    assert perms == {
        "kb_id": KB_ID,
        "public_read": False,
        "allowed_user_ids": [],
        "updated_at": None,
    }
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE kb_id = $1" in sql
    assert "AND tenant_id = $2" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_upsert_permissions_in_tx_conflict_arm_tenant_guarded():
    conn = _RecordingConn()
    # Upsert against a kb_id owned by tenant B: the INSERT violates the
    # composite key, and the DO UPDATE arm's guard must refuse the flip.
    await permission_repo.upsert_permissions_in_tx(
        conn,
        tenant_id=TENANT_ID,
        kb_id=KB_ID,
        public_read=True,
        allowed_user_ids=[],
    )
    sql, args = conn.execute_calls[0]
    assert "ON CONFLICT (kb_id) DO UPDATE" in sql
    assert "WHERE kb_permissions.tenant_id = $2" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID), True, [])
    _assert_no_guc(conn)


# ── AuditLog resource ────────────────────────────────────────────────────────


async def test_list_logs_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[])
    rows = await audit_repo.list_logs(conn, tenant_id=TENANT_ID, kb_id=KB_ID)
    assert rows == []
    sql, args = conn.fetch_calls[0]
    assert "WHERE kb_id = $1" in sql
    assert "AND tenant_id = $2" in sql
    assert args == (uuid.UUID(KB_ID), uuid.UUID(TENANT_ID), 20)
    _assert_no_guc(conn)


async def test_update_parse_result_in_tx_cross_tenant_updates_zero():
    conn = _RecordingConn(execute_result="UPDATE 0")
    updated = await audit_repo.update_parse_result_in_tx(
        conn,
        tenant_id=TENANT_ID,
        kb_id=KB_ID,
        action="doc.parse",
        task_id=OP_ID,
        result_overlay={"parse_status": "ready"},
        error_code=None,
        error_msg=None,
    )
    assert updated == 0  # tenant B's audit trail is not rewritten
    sql, args = conn.execute_calls[0]
    assert "WHERE tenant_id = $1" in sql
    assert "AND kb_id = $2" in sql
    assert args[0] == uuid.UUID(TENANT_ID)
    _assert_no_guc(conn)


# ── Operation resource ──────────────────────────────────────────────────────


async def test_find_by_idempotency_key_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)
    row = await operation_repo.find_by_idempotency_key(
        conn, tenant_id=TENANT_ID, idempotency_key=IDEMPOTENCY_KEY
    )
    assert row is None  # tenant B's replay row never serves tenant A
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE tenant_id = $1 AND idempotency_key = $2" in sql
    assert args[0] == uuid.UUID(TENANT_ID)
    assert args[1] == IDEMPOTENCY_KEY
    _assert_no_guc(conn)


async def test_get_operation_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)
    row = await operation_repo.get_operation(
        conn, tenant_id=TENANT_ID, operation_id=OP_ID
    )
    assert row is None
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(OP_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_complete_operation_in_tx_cross_tenant_returns_false():
    conn = _RecordingConn(execute_result="UPDATE 0")
    ok = await operation_repo.complete_operation_in_tx(
        conn, tenant_id=TENANT_ID, operation_id=OP_ID, result={"done": True}
    )
    assert ok is False  # tenant B's operation can't be completed from A
    sql, args = conn.execute_calls[0]
    assert "WHERE id = $1 AND tenant_id = $4" in sql
    assert args == (uuid.UUID(OP_ID), "completed", '{"done": true}', uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_acquire_run_cross_tenant_returns_false():
    conn = _RecordingConn(execute_result="UPDATE 0")
    ok = await operation_repo.acquire_run(conn, tenant_id=TENANT_ID, operation_id=OP_ID)
    assert ok is False  # rebuild pickup never hijacks tenant B's operation
    sql, args = conn.execute_calls[0]
    assert "WHERE id = $1 AND tenant_id = $2" in sql
    assert args == (uuid.UUID(OP_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_revive_operation_in_tx_cross_tenant_returns_none():
    conn = _RecordingConn(fetchrow_row=None)
    row = await operation_repo.revive_operation_in_tx(
        conn, tenant_id=TENANT_ID, operation_id=OP_ID
    )
    assert row is None
    sql, args = conn.fetchrow_calls[0]
    assert "WHERE id = $1 AND tenant_id = $2 AND status = 'failed'" in sql
    assert args == (uuid.UUID(OP_ID), uuid.UUID(TENANT_ID))
    _assert_no_guc(conn)


async def test_list_operations_cross_tenant_returns_empty():
    conn = _RecordingConn(rows=[])
    rows = await operation_repo.list_operations(conn, tenant_id=TENANT_ID)
    assert rows == []
    sql, args = conn.fetch_calls[0]
    assert "WHERE tenant_id = $1" in sql
    assert args == (uuid.UUID(TENANT_ID), 20)
    _assert_no_guc(conn)


# ── the GUC itself is dead ───────────────────────────────────────────────────


async def test_set_tenant_context_emits_no_sql():
    """#054: set_tenant_context is a pure validation no-op — no GUC write."""
    conn = _RecordingConn()
    await rls_repo.set_tenant_context(conn, TENANT_ID)
    assert conn.execute_calls == []
    assert conn.fetch_calls == []
    assert conn.fetchrow_calls == []
    assert conn.fetchval_calls == []
    # malformed tenant ids are still rejected before any query is built
    with pytest.raises(ValueError):
        await rls_repo.set_tenant_context(conn, "not-a-uuid")
