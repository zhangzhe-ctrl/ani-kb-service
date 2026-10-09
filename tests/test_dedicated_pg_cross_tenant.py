"""Real-PostgreSQL cross-tenant negative tests (issue-055, KB-SPLIT-P1F).

[ADR 0001 §五-阶段1-5 / §五-阶段1-验收] tests/test_explicit_tenant_isolation.py
(#054) proved with fakes that every repository query carries the explicit
``tenant_id = $N`` filter. This file proves the SAME story against a real
dedicated-PostgreSQL instance, on the schema a fresh kb database gets from
replaying migrations 000-014 from zero, exercised under the
least-privilege application role (ani_app_user) — covering A+B layer tables,
kb_operations, and kb_outbox_events.

Test layout (per resource): tenant B owns a row; tenant A then issues every
GET/UPDATE/DELETE repository call. Each must observe None / empty list /
defaults / False / 0 — the values the servicer maps to NOT_FOUND / empty-set /
idempotent-204 — and tenant B's row must survive untouched in the database
(durable check via a second read as tenant B).

outbox is covered per its documented semantics (outbox.py):
  * insert_event is tenant-bound (row lands with tenant B's id when B writes,
    with A's when A writes — each tenant's event belongs to itself);
  * list_undispatched / mark_dispatched / mark_dispatched_batch are
    DELIBERATELY cross-tenant (the dispatcher polls all tenants) — the real
    test asserts that dispatcher behavior works on the dedicated instance
    (A's and B's events are both listed and both dispatchable) while no
    application repository path ever exposes another tenant's business rows.

Gating: runs ONLY when KB_TEST_ADMIN_DATABASE_URL is set — the URL of a
superuser/admin connection used to (1) create a throwaway database,
(2) replay migrations 000-014 from zero (NOTE: this replay is executed
by the ADMIN, not by SET ROLE ani_migrator — the migrator-role workflow
is NOT covered here; migration 000 needs CREATEROLE so it always stays
in the admin phase anyway. Covering the migrator identity is deferred
to the migration-pipeline issue. The role baseline itself (000) grants
ani_migrator CONNECT + ALL PRIVILEGES on the database plus
USAGE/CREATE on schema public, so a SET ROLE replay is not blocked by
PG15+ public-schema ACLs — but it remains unverified in this file),
(3) grant ani_app_user the CONNECT + table privileges the
migrations already install, and (4) set its password. The tests themselves
then connect AS ani_app_user — proving the least-privilege role really can
run the whole workload (every INSERT succeeds through the sequence grants of
migration 014), and that cross-tenant isolation holds with no RLS anywhere.

Follows the env-gated real-PG precedent of
make test-inference-control-plane-postgres (INFERENCE_TEST_DATABASE_URL):
absent env → module-level pytest.skip, so `make test` stays green without a
provisioned instance (the evidence lands in docs/execution/status.md as
pass / fail / not_verified).

Usage on a reachable admin PG (dev example — the dedicated instance from
deploy/docker/docker-compose.kb-pg.yml maps it on 127.0.0.1:5433):
    KB_TEST_ADMIN_DATABASE_URL=postgres://ani:ani_dev_password@localhost:5433/postgres?sslmode=disable \
    python -m pytest tests/test_dedicated_pg_cross_tenant.py -v
"""
from __future__ import annotations

import os
import sys
import uuid
from urllib.parse import urlsplit, urlunsplit

import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _SERVICE_ROOT not in sys.path:
    sys.path.insert(0, _SERVICE_ROOT)

import asyncpg

from app.repositories import audit as audit_repo
from app.repositories import chunk as chunk_repo
from app.repositories import document as document_repo
from app.repositories import knowledge_base as kb_repo
from app.repositories import message as message_repo
from app.repositories import operation as operation_repo
from app.repositories import outbox as outbox_repo
from app.repositories import permission as permission_repo

ADMIN_URL = os.environ.get("KB_TEST_ADMIN_DATABASE_URL")
pytestmark = [
    pytest.mark.skipif(
        not ADMIN_URL,
        reason="KB_TEST_ADMIN_DATABASE_URL not set — real dedicated-PG tests need a provisioned admin instance (see module docstring)",
    ),
    # One event loop for the whole module: the shared asyncpg connection is
    # created in the module-scoped `migrated_db` fixture and must not be
    # bounced between per-function loops (asyncpg binds to one loop).
    pytest.mark.asyncio(loop_scope="module"),
]

# ── identities ──────────────────────────────────────────────────────────────
TENANT_A = "11111111-1111-1111-1111-111111111111"
TENANT_B = "99999999-9999-9999-9999-999999999999"

MIGRATIONS_DIR = os.path.join(_SERVICE_ROOT, "migrations")

# Test-scoped app role password (the admin sets it before the app connects).
APP_USER = "ani_app_user"
APP_PASSWORD = "kb_test_app_password"

# ── throwaway database plumbing ─────────────────────────────────────────────
_TEST_DB = f"kb_test_055_{uuid.uuid4().hex[:8]}"
_ADMIN_POOL = None          # admin connection (DDL + teardown)
APP_CONN: asyncpg.Connection | None = None  # shared ani_app_user connection
# Snapshot of pg_authid.rolpassword for ani_app_user taken BEFORE the test
# ALTER ROLE — passwords live at the instance (cluster) level and survive
# the DROP DATABASE teardown, so the original hash must be restored in the
# fixture teardown. None = the role genuinely has no password; the flag
# distinguishes "snapshotted as NULL" from "never snapshotted".
_SAVED_APP_PASSWORD_HASH: str | None = None
_PASSWORD_HASH_SAVED = False


async def _admin_execute(sql: str) -> None:
    global _ADMIN_POOL
    if _ADMIN_POOL is None:
        _ADMIN_POOL = await asyncpg.create_pool(dsn=ADMIN_URL, min_size=1, max_size=1)
    async with _ADMIN_POOL.acquire() as conn:
        await conn.execute(sql)


async def _close_admin_pool() -> None:
    global _ADMIN_POOL
    if _ADMIN_POOL is not None:
        await _ADMIN_POOL.close()
        _ADMIN_POOL = None


async def _restore_app_password() -> None:
    """Restore ani_app_user's pg_authid.rolpassword as snapshotted by
    _provision (no-op when the snapshot never ran). Roles and passwords
    are CLUSTER-level state that outlives the throwaway database, so the
    fixture teardown must put the original back. ALTER ROLE ... PASSWORD
    accepts an already-hashed SCRAM/md5 verifier and stores it verbatim,
    making the restore byte-precise; PASSWORD NULL covers the genuinely
    passwordless case."""
    if not _PASSWORD_HASH_SAVED:
        return
    if _SAVED_APP_PASSWORD_HASH is None:
        await _admin_execute(f"ALTER ROLE {APP_USER} WITH PASSWORD NULL")
    else:
        # Verifiers are base64/$/:/digits — no single quotes in practice;
        # ''-escape defensively anyway.
        esc = _SAVED_APP_PASSWORD_HASH.replace("'", "''")
        await _admin_execute(f"ALTER ROLE {APP_USER} WITH PASSWORD '{esc}'")


def _db_url(db_name: str, *, app_role: bool = False) -> str:
    """ADMIN_URL rebuilt against ``db_name``, PRESERVING any query params
    (e.g. ``?sslmode=disable``). With ``app_role`` the auth switches to the
    least-privilege role via URL userinfo — asyncpg does NOT honor
    user/password in the query string, so the credentials must go into the
    authority part of the DSN."""
    parts = urlsplit(ADMIN_URL)
    netloc = parts.netloc
    if app_role:
        host_part = netloc.rsplit("@", 1)[-1]  # strip admin userinfo
        netloc = f"{APP_USER}:{APP_PASSWORD}@{host_part}"
    return urlunsplit((parts.scheme, netloc, f"/{db_name}", parts.query, ""))


async def _replay_migrations(admin_conn: asyncpg.Connection) -> None:
    """Replay migrations/000-014 in sorted() order; assert ZERO errors.

    This is the [ADR §五-阶段1-验收] replay-from-zero check itself: the
    whole baseline (outbox 009, kb_operations 010, B-layer tables 011,
    isolation rework 012, index 013, sequence grants 014) must build a
    working schema on an empty dedicated database in one pass.
    """
    files = sorted(f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql"))
    assert files, "no migration files found"
    for mf in files:
        with open(os.path.join(MIGRATIONS_DIR, mf), encoding="utf-8") as f:
            sql = f.read()
        try:
            await admin_conn.execute(sql)
        except Exception as e:  # noqa: BLE001 — surface file+error, fail fast
            raise AssertionError(f"replay of {mf} failed on a fresh database: {e}") from e


async def _provision() -> asyncpg.Connection:
    """Create throwaway DB, replay migrations from zero, return an
    ani_app_user connection to it."""
    global _SAVED_APP_PASSWORD_HASH, _PASSWORD_HASH_SAVED

    admin = await asyncpg.connect(ADMIN_URL)
    try:
        # Snapshot the app role's password verifier BEFORE the test ALTER
        # ROLE overwrites it — pg_authid is instance-level state that the
        # DROP DATABASE teardown never touches. Reading rolpassword
        # requires superuser, which this admin connection is.
        row = await admin.fetchrow(
            "SELECT rolpassword FROM pg_authid WHERE rolname = $1", APP_USER
        )
        _SAVED_APP_PASSWORD_HASH = row["rolpassword"] if row is not None else None
        _PASSWORD_HASH_SAVED = True

        await admin.execute(f'DROP DATABASE IF EXISTS "{_TEST_DB}"')
        # Isolation-level CREATE DATABASE (cannot run inside a transaction).
        await admin.execute(f'CREATE DATABASE "{_TEST_DB}"')
    finally:
        await admin.close()

    try:
        admin = await asyncpg.connect(_db_url(_TEST_DB))
        try:
            await _replay_migrations(admin)
            # Migrations assume the roles exist with a password (deployment
            # secret / compose init script in real deployments). Test instance:
            # the admin sets it now (the migrator identity is NOLOGIN and not
            # covered by this file — see module docstring).
            await admin.execute(
                f"ALTER ROLE {APP_USER} WITH PASSWORD '{APP_PASSWORD}'"
            )
        finally:
            await admin.close()
        return await asyncpg.connect(_db_url(_TEST_DB, app_role=True))
    except Exception:
        # yield-fixture semantics: if setup fails before the yield, the
        # teardown never runs — so a failure past CREATE DATABASE must
        # clean up the throwaway DB (and the cluster-level password it
        # already changed) itself before re-raising.
        await _restore_app_password()
        await _admin_execute(f'DROP DATABASE IF EXISTS "{_TEST_DB}"')
        await _close_admin_pool()
        raise


@pytest.fixture(scope="module")
async def migrated_db():
    global APP_CONN, _ADMIN_POOL
    APP_CONN = await _provision()
    yield APP_CONN
    await APP_CONN.close()
    APP_CONN = None
    await _admin_execute(f'DROP DATABASE IF EXISTS "{_TEST_DB}"')
    # Cluster-level state the DROP DATABASE above did NOT revert: put the
    # role's original password verifier back before closing the admin pool.
    await _restore_app_password()
    await _close_admin_pool()


# ── test data: tenant B owns a full hierarchy ────────────────────────────────
KB_B = "22222222-2222-2222-2222-222222222222"
DOC_B = "33333333-3333-3333-3333-333333333333"
SESSION_B = "44444444-4444-4444-4444-444444444444"
MSG_B = "55555555-5555-5555-5555-555555555555"
CHUNK_B = "66666666-6666-6666-6666-666666666666"
OP_B = "88888888-8888-8888-8888-888888888888"
IDEMPOTENCY_KEY = "req-055-cross-tenant"


@pytest.fixture(scope="module")
async def tenant_b_world(migrated_db):
    """Seed tenant B's full hierarchy (KB → doc → session → message →
    chunk + permission + audit + operation + outbox event) via the REAL
    repositories on the REAL database as the application role."""
    conn = migrated_db
    # create_kb generates its own id; the fixture needs the fixed KB_B id
    # for the direct hierarchy inserts below. Reuse the generated row via
    # the soft-delete-then-reinsert dance: the partial unique index
    # (tenant_id, name) WHERE status <> 'deleted' lets the fixed-id insert
    # reuse the name once the generated row is soft-deleted (fixture writes
    # only; the negative tests below use repositories exclusively).
    await kb_repo.create_kb(
        conn, tenant_id=TENANT_B, name="tenant-b-kb", embedding_model="BAAI/bge-m3"
    )
    kbs, _total = await kb_repo.list_kbs(conn, tenant_id=TENANT_B)
    generated_kb = str(kbs[0]["id"])
    await kb_repo.soft_delete_kb(conn, tenant_id=TENANT_B, kb_id=generated_kb)
    async with conn.transaction():
        await conn.execute(
            """
            INSERT INTO knowledge_bases (id, tenant_id, name, embedding_model)
            VALUES ($1, $2, 'tenant-b-kb', 'BAAI/bge-m3')
            """,
            uuid.UUID(KB_B),
            uuid.UUID(TENANT_B),
        )
    # Documents (create_document accepts doc_id explicitly).
    await document_repo.create_document(
        conn,
        tenant_id=TENANT_B,
        kb_id=KB_B,
        doc_id=DOC_B,
        file_name="b.pdf",
        file_type="pdf",
        file_size_bytes=10,
        storage_path="kb/b.pdf",
        checksum_sha256="b" * 64,
    )
    # Session (create_session accepts session_id) + message (no explicit id
    # parameter: insert_message_in_tx doesn't accept msg id — so write the
    # message via the repo and pin MSG_B through a direct fixture insert).
    await message_repo.create_session(
        conn, tenant_id=TENANT_B, kb_id=KB_B, session_id=SESSION_B, title="b-session"
    )
    async with conn.transaction():
        await message_repo.insert_message_in_tx(
            conn, tenant_id=TENANT_B, session_id=SESSION_B,
            role="user", content="tenant b question",
        )
        await conn.execute(
            """
            INSERT INTO kb_messages (id, session_id, tenant_id, role, content)
            VALUES ($1, $2, $3, 'assistant', 'tenant b answer')
            """,
            uuid.UUID(MSG_B),
            uuid.UUID(SESSION_B),
            uuid.UUID(TENANT_B),
        )
    # Chunk (write_chunks takes explicit chunk ids via the dicts).
    await chunk_repo.write_chunks(
        conn,
        tenant_id=TENANT_B,
        kb_id=KB_B,
        doc_id=DOC_B,
        file_name="b.pdf",
        parents=[{"chunk_id": CHUNK_B, "content": "tenant b parent chunk",
                   "content_type": "text", "page_number": 1, "token_count": 5}],
        children=[],
    )
    # Permission / audit / operation / outbox.
    async with conn.transaction():
        await permission_repo.upsert_permissions_in_tx(
            conn, tenant_id=TENANT_B, kb_id=KB_B,
            public_read=False, allowed_user_ids=[],
        )
        await audit_repo.insert_audit_in_tx(
            conn, tenant_id=TENANT_B, kb_id=KB_B, action="kb.create",
        )
    op = await operation_repo.create_operation(
        conn, tenant_id=TENANT_B, idempotency_key=IDEMPOTENCY_KEY,
        operation_type="kb.update", resource_id=KB_B,
    )
    # op is the repo-generated idempotency-record row; OP_B below is the
    # separate fixed-id row the negative tests probe (both stay pending).
    async with conn.transaction():
        await conn.execute(
            """
            INSERT INTO kb_operations (id, tenant_id, idempotency_key,
                                       operation_type, status)
            VALUES ($1, $2, 'req-055-op-b', 'kb.update', 'pending')
            """,
            uuid.UUID(OP_B),
            uuid.UUID(TENANT_B),
        )
    async with conn.transaction():
        await outbox_repo.insert_event(
            conn, tenant_id=TENANT_B, aggregate_type="kb",
            aggregate_id=KB_B, event_type="doc.uploaded.v2",
            payload={"doc": DOC_B},
        )
    return {"kb": KB_B, "doc": DOC_B, "session": SESSION_B, "msg": MSG_B,
            "chunk": CHUNK_B, "op": OP_B}


# ── B-layer: KB ──────────────────────────────────────────────────────────────


async def test_get_kb_cross_tenant_returns_none(migrated_db, tenant_b_world):
    row = await kb_repo.get_kb(migrated_db, tenant_id=TENANT_A, kb_id=KB_B)
    assert row is None  # servicer → NOT_FOUND


async def test_list_kbs_cross_tenant_returns_empty(migrated_db, tenant_b_world):
    rows, total = await kb_repo.list_kbs(migrated_db, tenant_id=TENANT_A)
    assert rows == [] and total == 0


async def test_update_kb_cross_tenant_returns_none(migrated_db, tenant_b_world):
    row = await kb_repo.update_kb(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B, name="hijacked"
    )
    assert row is None
    # tenant B's name untouched (durable)
    b_row = await kb_repo.get_kb(migrated_db, tenant_id=TENANT_B, kb_id=KB_B)
    assert b_row["name"] == "tenant-b-kb"


async def test_soft_delete_kb_cross_tenant_returns_false(migrated_db, tenant_b_world):
    ok = await kb_repo.soft_delete_kb(migrated_db, tenant_id=TENANT_A, kb_id=KB_B)
    assert ok is False
    b_row = await kb_repo.get_kb(migrated_db, tenant_id=TENANT_B, kb_id=KB_B)
    assert b_row["status"] == "active"


async def test_update_kb_config_cross_tenant_returns_none(migrated_db, tenant_b_world):
    row = await kb_repo.update_config_in_tx(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B, patch={"top_k": 1}
    )
    assert row is None
    b_row = await kb_repo.get_kb(migrated_db, tenant_id=TENANT_B, kb_id=KB_B)
    assert b_row["top_k"] == 5


# ── B-layer: Document ────────────────────────────────────────────────────────


async def test_get_document_cross_tenant_returns_none(migrated_db, tenant_b_world):
    row = await document_repo.get_document(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B, doc_id=DOC_B
    )
    assert row is None


async def test_list_documents_cross_tenant_returns_empty(migrated_db, tenant_b_world):
    rows, total = await document_repo.list_documents(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B
    )
    assert rows == [] and total == 0


async def test_soft_delete_document_cross_tenant_returns_false(migrated_db, tenant_b_world):
    ok = await document_repo.soft_delete_document(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B, doc_id=DOC_B
    )
    assert ok is False


async def test_update_parse_status_cross_tenant_returns_false(migrated_db, tenant_b_world):
    ok = await document_repo.update_parse_status_in_tx(
        migrated_db, tenant_id=TENANT_A, doc_id=DOC_B, parse_status="ready"
    )
    assert ok is False


# ── B-layer: Session / Message ───────────────────────────────────────────────


async def test_get_session_cross_tenant_returns_none(migrated_db, tenant_b_world):
    row = await message_repo.get_session(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B, session_id=SESSION_B
    )
    assert row is None


async def test_list_sessions_cross_tenant_returns_empty(migrated_db, tenant_b_world):
    rows = await message_repo.list_sessions(migrated_db, tenant_id=TENANT_A, kb_id=KB_B)
    assert rows == []


async def test_list_session_messages_cross_tenant_returns_empty(migrated_db, tenant_b_world):
    rows = await message_repo.list_session_messages(
        migrated_db, tenant_id=TENANT_A, session_id=SESSION_B
    )
    assert rows == []


async def test_delete_session_cross_tenant_returns_false(migrated_db, tenant_b_world):
    ok = await message_repo.delete_session(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B, session_id=SESSION_B
    )
    assert ok is False
    b_row = await message_repo.get_session(
        migrated_db, tenant_id=TENANT_B, kb_id=KB_B, session_id=SESSION_B
    )
    assert b_row is not None


# ── B-layer: Chunk ───────────────────────────────────────────────────────────


async def test_get_chunk_cross_tenant_returns_none(migrated_db, tenant_b_world):
    row = await chunk_repo.get_chunk(migrated_db, tenant_id=TENANT_A, chunk_id=CHUNK_B)
    assert row is None


async def test_list_chunks_by_doc_cross_tenant_returns_empty(migrated_db, tenant_b_world):
    rows = await chunk_repo.list_chunks_by_doc(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B, doc_id=DOC_B
    )
    assert rows == []


async def test_list_parent_chunks_cross_tenant_returns_empty(migrated_db, tenant_b_world):
    rows = await chunk_repo.list_parent_chunks(
        migrated_db, tenant_id=TENANT_A, doc_id=DOC_B
    )
    assert rows == []


async def test_delete_chunks_by_doc_cross_tenant_deletes_zero(migrated_db, tenant_b_world):
    n = await chunk_repo.delete_chunks_by_doc(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B, doc_id=DOC_B
    )
    assert n == 0
    # tenant B's chunk survives
    b_row = await chunk_repo.get_chunk(migrated_db, tenant_id=TENANT_B, chunk_id=CHUNK_B)
    assert b_row is not None


# ── Permission ───────────────────────────────────────────────────────────────


async def test_get_permissions_cross_tenant_returns_defaults(migrated_db, tenant_b_world):
    perms = await permission_repo.get_permissions(
        migrated_db, tenant_id=TENANT_A, kb_id=KB_B
    )
    # defaults — never tenant B's ACL, never 404
    assert perms == {
        "kb_id": KB_B,
        "public_read": False,
        "allowed_user_ids": [],
        "updated_at": None,
    }


async def test_upsert_permissions_cross_tenant_cannot_flip(migrated_db, tenant_b_world):
    """The ON CONFLICT DO UPDATE arm carries the tenant guard: A's upsert
    on B's kb_id must not flip B's row (conflict → guard fails → 0 rows)."""
    async with migrated_db.transaction():
        await permission_repo.upsert_permissions_in_tx(
            migrated_db, tenant_id=TENANT_A, kb_id=KB_B,
            public_read=True, allowed_user_ids=[],
        )
    b_perms = await permission_repo.get_permissions(
        migrated_db, tenant_id=TENANT_B, kb_id=KB_B
    )
    assert b_perms["public_read"] is False  # B's ACL untouched


# ── AuditLog ─────────────────────────────────────────────────────────────────


async def test_list_logs_cross_tenant_returns_empty(migrated_db, tenant_b_world):
    rows = await audit_repo.list_logs(migrated_db, tenant_id=TENANT_A, kb_id=KB_B)
    assert rows == []


# ── kb_operations ─────────────────────────────────────────────────────────────


async def test_find_by_idempotency_key_cross_tenant_returns_none(migrated_db, tenant_b_world):
    row = await operation_repo.find_by_idempotency_key(
        migrated_db, tenant_id=TENANT_A, idempotency_key=IDEMPOTENCY_KEY
    )
    assert row is None  # tenant B's replay result never serves tenant A


async def test_get_operation_cross_tenant_returns_none(migrated_db, tenant_b_world):
    row = await operation_repo.get_operation(
        migrated_db, tenant_id=TENANT_A, operation_id=OP_B
    )
    assert row is None


async def test_list_operations_cross_tenant_returns_empty(migrated_db, tenant_b_world):
    rows = await operation_repo.list_operations(migrated_db, tenant_id=TENANT_A)
    assert rows == []


async def test_complete_operation_cross_tenant_returns_false(migrated_db, tenant_b_world):
    ok = await operation_repo.complete_operation_in_tx(
        migrated_db, tenant_id=TENANT_A, operation_id=OP_B, result={"x": 1}
    )
    assert ok is False
    b_row = await operation_repo.get_operation(
        migrated_db, tenant_id=TENANT_B, operation_id=OP_B
    )
    assert b_row["status"] == "pending"  # B's lifecycle untouched


async def test_acquire_run_cross_tenant_returns_false(migrated_db, tenant_b_world):
    ok = await operation_repo.acquire_run(
        migrated_db, tenant_id=TENANT_A, operation_id=OP_B
    )
    assert ok is False


# ── kb_outbox_events (documented cross-tenant dispatcher semantics) ─────────


async def test_outbox_insert_event_is_tenant_bound(migrated_db, tenant_b_world):
    """insert_event writes the caller's tenant on the row — A inserts its
    own event; B's earlier event stays B's."""
    async with migrated_db.transaction():
        ev_a = await outbox_repo.insert_event(
            migrated_db, tenant_id=TENANT_A, aggregate_type="kb",
            aggregate_id=KB_B, event_type="doc.uploaded.v2", payload={},
        )
    row = await migrated_db.fetchrow(
        "SELECT tenant_id FROM kb_outbox_events WHERE id = $1", ev_a
    )
    assert str(row["tenant_id"]) == TENANT_A


async def test_outbox_dispatcher_sees_all_tenants_and_marks_dispatched(migrated_db, tenant_b_world):
    """list_undispatched / mark_dispatched(_batch) are deliberately
    cross-tenant (dispatcher semantics since 009) — proven against the
    dedicated instance: both A's and B's pending events are listed and
    marked, and only pending rows are ever returned."""
    before = await outbox_repo.list_undispatched(migrated_db, limit=100)
    tenants = {str(r["tenant_id"]) for r in before}
    assert TENANT_A in tenants and TENANT_B in tenants

    ids = [r["id"] for r in before]
    marked = await outbox_repo.mark_dispatched_batch(
        migrated_db, event_ids=ids
    )
    assert marked == len(ids)

    after = await outbox_repo.list_undispatched(migrated_db, limit=100)
    assert all(r["id"] not in ids for r in after)
    # mark_dispatched re-marks by primary key (idempotent UPDATE 1 — the
    # published flag is not part of the WHERE guard, by design since 009)
    assert await outbox_repo.mark_dispatched(migrated_db, event_id=ids[0]) is True


# ── least-privilege role really works (app workload under ani_app_user) ───────


async def test_app_role_ran_the_whole_fixture_workload(migrated_db, tenant_b_world):
    """The shared fixture connection IS ani_app_user — every seeding write
    above (INSERT on all 9 tables incl. the BIGSERIAL outbox insert via the
    014 sequence grant) and every negative read below already proves the
    least-privilege role supports the full application workload. Explicit
    sanity: the session role is ani_app_user, NOT a superuser."""
    role = await migrated_db.fetchval("SELECT current_user")
    assert role == APP_USER
    superuser = await migrated_db.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname = $1", APP_USER)
    assert superuser is False
