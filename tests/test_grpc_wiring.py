"""Tests for gRPC server wiring (issue-007 / US-009).

Verifies:
- CreateKB creates the dedicated Milvus collection kb_{kb_id} (issue-056 D1,
  direct pymilvus — no Core vector-store round trip) and writes
  knowledge_bases + kb_operations.
- DeleteKB soft-deletes KB and drops the KB's Milvus collection (best-effort).
- The 10 P0 RPCs are still declared on the servicer (regression of issue-006).
- The 3 P1 RPCs still return UNIMPLEMENTED.

These tests use a mock asyncpg pool so no real DB is required. They focus on
the wiring logic (correct vector_store_id derived, errors mapped) rather than
the SQL behavior (covered by the repository layer + integration tests).
"""
import os
import sys
import uuid
from concurrent import futures
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import grpc
import httpx
import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.api.grpc_server import KBServiceServicer, _vector_store_name
from app.generated.common.v1 import common_pb2
from app.generated.kb.v1 import kb_service_pb2 as kb_pb
from app.generated.kb.v1 import kb_service_pb2_grpc as kb_grpc


TENANT_ID = "11111111-1111-1111-1111-111111111111"
KB_ID = "22222222-2222-2222-2222-222222222222"


# ── regression: all 13 RPCs declared ──────────────────────────────────────────

P0_RPCS = [
    "CreateKB", "GetKB", "ListKBs", "DeleteKB",
    "GetDocumentUploadURL", "CompleteDocumentUpload",
    "GetDocument", "ListDocuments", "DeleteDocument", "Query",
]
P1_RPCS = ["ListKBCitations", "ListKBSessions", "UpdateKBPermissions", "RebuildKB"]


def test_servicer_still_declares_all_rpcs():
    servicer = KBServiceServicer()
    for name in P0_RPCS + P1_RPCS:
        assert hasattr(servicer, name), f"servicer missing RPC {name}"


def test_vector_store_name_derivation():
    name = _vector_store_name(KB_ID)
    assert name == f"kb_{KB_ID.replace('-', '')}"
    assert "-" not in name


# ── mock helpers ──────────────────────────────────────────────────────────────


class _MockConn:
    """Minimal asyncpg.Connection mock for repository calls."""

    def __init__(self):
        self._rows: dict = {}
        self._next_id = 0

    def transaction(self):
        # Return an async context manager
        @asynccontextmanager
        async def _tx():
            yield self
        return _tx()

    async def execute(self, sql, *args):
        return "UPDATE 1"

    async def fetchrow(self, sql, *args):
        return None

    async def fetch(self, sql, *args):
        return []

    async def fetchval(self, sql, *args):
        return 0


class _MockPool:
    """Minimal asyncpg.Pool mock returning _MockConn on acquire."""

    @asynccontextmanager
    async def acquire(self):
        yield _MockConn()


class _FakeVectorStore:
    """MilvusVectorStore fake for D1 direct-vector paths (issue-056)."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    async def create_collection(self, **kwargs):
        self.calls.append(("create_collection", kwargs))

    async def drop_collection(self, **kwargs):
        self.calls.append(("drop_collection", kwargs))
        return True

    async def recreate_collection(self, **kwargs):
        self.calls.append(("recreate_collection", kwargs))

    async def insert(self, **kwargs):
        self.calls.append(("insert", kwargs))

    async def search(self, **kwargs):
        self.calls.append(("search", kwargs))
        return []

    async def delete(self, **kwargs):
        self.calls.append(("delete", kwargs))


# ── gRPC server fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    kb_grpc.add_KBServiceServicer_to_server(KBServiceServicer(), server)
    port = server.add_insecure_port("[::]:0")
    server.start()
    yield f"localhost:{port}"
    server.stop(grace=None)


@pytest.fixture
def stub(grpc_server):
    return kb_grpc.KBServiceStub(grpc.insecure_channel(grpc_server))


# ── skeleton mode: no pool → FAILED_PRECONDITION ─────────────────────────────


def test_get_kb_without_pool_returns_failed_precondition(stub):
    """Without a DB pool, GetKB returns FAILED_PRECONDITION (not UNIMPLEMENTED)."""
    with pytest.raises(grpc.RpcError) as exc:
        stub.GetKB(kb_pb.GetKBRequest(tenant_id=TENANT_ID, kb_id=KB_ID))
    # US-009 changes skeleton from UNIMPLEMENTED to FAILED_PRECONDITION when
    # the servicer is constructed without a pool (test/skeleton mode).
    assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_list_kbs_without_pool_returns_failed_precondition(stub):
    with pytest.raises(grpc.RpcError) as exc:
        stub.ListKBs(kb_pb.ListKBsRequest(tenant_id=TENANT_ID, page=common_pb2.CursorPageRequest(limit=20)))
    assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_delete_kb_without_pool_returns_failed_precondition(stub):
    with pytest.raises(grpc.RpcError) as exc:
        stub.DeleteKB(kb_pb.DeleteKBRequest(tenant_id=TENANT_ID, kb_id=KB_ID))
    assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION


# ── P1 RPCs: B2 RPCs wired; permissions pair wired in B4 ─────────────────────


def test_update_kb_permissions_b4_wired_not_unimplemented(stub):
    """B4 (kb-p1-plan §2.5): UpdateKBPermissions is implemented. Constructed
    without a pool it must return FAILED_PRECONDITION — never UNIMPLEMENTED
    (that would mean the P1 stub still shadows the servicer)."""
    with pytest.raises(grpc.RpcError) as exc:
        stub.UpdateKBPermissions(
            kb_pb.UpdateKBPermissionsRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID, idempotency_key=str(uuid.uuid4())
            )
        )
    assert exc.value.code() != grpc.StatusCode.UNIMPLEMENTED
    assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_get_kb_permissions_b4_wired_not_unimplemented(stub):
    """B4 (kb-p1-plan §2.5): GetKBPermissions is implemented; without a pool
    it returns FAILED_PRECONDITION — never UNIMPLEMENTED."""
    with pytest.raises(grpc.RpcError) as exc:
        stub.GetKBPermissions(kb_pb.GetKBPermissionsRequest(tenant_id=TENANT_ID, kb_id=KB_ID))
    assert exc.value.code() != grpc.StatusCode.UNIMPLEMENTED
    assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_b2_rpcs_wired_not_unimplemented(stub):
    """ListKBCitations/ListKBSessions are implemented in B2 (issue-045).

    Constructed without a pool they must return FAILED_PRECONDITION — never
    UNIMPLEMENTED (that would mean the P1 stub still shadows the servicer).
    """
    for rpc, req in [
        ("ListKBCitations", kb_pb.ListKBCitationsRequest(tenant_id=TENANT_ID, kb_id=KB_ID)),
        ("ListKBSessions", kb_pb.ListKBSessionsRequest(tenant_id=TENANT_ID, kb_id=KB_ID)),
        ("ListDocumentChunks", kb_pb.ListDocumentChunksRequest(tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=str(uuid.uuid4()))),
        ("GetSessionMessages", kb_pb.GetSessionMessagesRequest(tenant_id=TENANT_ID, kb_id=KB_ID, session_id=str(uuid.uuid4()))),
        ("DeleteSession", kb_pb.DeleteSessionRequest(tenant_id=TENANT_ID, kb_id=KB_ID, session_id=str(uuid.uuid4()))),
    ]:
        with pytest.raises(grpc.RpcError) as exc:
            getattr(stub, rpc)(req)
        assert exc.value.code() != grpc.StatusCode.UNIMPLEMENTED
        assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_b2_rpcs_wired_not_unimplemented(stub):
    """ListKBCitations/ListKBSessions are implemented in B2 (issue-045).

    Constructed without a pool they must return FAILED_PRECONDITION — never
    UNIMPLEMENTED (that would mean the P1 stub still shadows the servicer).
    """
    for rpc, req in [
        ("ListKBCitations", kb_pb.ListKBCitationsRequest(tenant_id=TENANT_ID, kb_id=KB_ID)),
        ("ListKBSessions", kb_pb.ListKBSessionsRequest(tenant_id=TENANT_ID, kb_id=KB_ID)),
        ("ListDocumentChunks", kb_pb.ListDocumentChunksRequest(tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=str(uuid.uuid4()))),
        ("GetSessionMessages", kb_pb.GetSessionMessagesRequest(tenant_id=TENANT_ID, kb_id=KB_ID, session_id=str(uuid.uuid4()))),
        ("DeleteSession", kb_pb.DeleteSessionRequest(tenant_id=TENANT_ID, kb_id=KB_ID, session_id=str(uuid.uuid4()))),
    ]:
        with pytest.raises(grpc.RpcError) as exc:
            getattr(stub, rpc)(req)
        assert exc.value.code() != grpc.StatusCode.UNIMPLEMENTED
        assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION


# ── CreateKB validation ───────────────────────────────────────────────────────


def test_create_kb_missing_tenant_returns_invalid_argument(stub):
    with pytest.raises(grpc.RpcError) as exc:
        stub.CreateKB(kb_pb.CreateKBRequest(name="kb1"))
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_create_kb_missing_name_returns_invalid_argument(stub):
    with pytest.raises(grpc.RpcError) as exc:
        stub.CreateKB(kb_pb.CreateKBRequest(tenant_id=TENANT_ID))
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_create_kb_missing_embedding_model_returns_invalid_argument(monkeypatch):
    """request.embedding_model 与 settings.embedding_model 均为空 → 拒绝建库
    （INVALID_ARGUMENT），不再静默使用默认向量模型。校验位于幂等重放检查之后
    （重放优先），故需带 pool 的 servicer 才能走到该分支。"""
    from app.core import config

    monkeypatch.setattr(config.settings, "embedding_model", "")
    servicer = KBServiceServicer(pool=_MockPool())
    req = kb_pb.CreateKBRequest(tenant_id=TENANT_ID, name="kb-no-embed")

    import asyncio

    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        asyncio.new_event_loop().run_until_complete(
            servicer._create_kb(req, _StubContext())
        )


# ── CreateKB name conflict → ALREADY_EXISTS ───────────────────────────────────


def test_create_kb_name_conflict_returns_already_exists():
    """CreateKB hits UNIQUE(tenant_id, name) on the knowledge_bases INSERT.
    Two paths: (a) a genuine name conflict with an active KB, or (b) a retry
    whose prior attempt committed the kb INSERT but crashed before the
    kb_operations record was written (replay check finds nothing, so this
    retry re-inserts). Both must surface ALREADY_EXISTS, not UNKNOWN."""
    class _ConflictConn:
        """Minimal conn: the replay find returns nothing; the kb INSERT
        raises UniqueViolationError (SQLSTATE 23505)."""

        def __init__(self):
            self.events: list[str] = []

        def transaction(self):
            @asynccontextmanager
            async def _tx():
                yield self
            return _tx()

        async def execute(self, sql, *args):
            return "UPDATE 1"

        async def fetchrow(self, sql, *args):
            if "INSERT INTO knowledge_bases" in sql:
                self.events.append("insert_kb")
                raise asyncpg.UniqueViolationError(
                    'duplicate key value violates unique constraint '
                    '"knowledge_bases_tenant_id_name_key"'
                )
            if "FROM kb_operations" in sql and "idempotency_key" in sql:
                self.events.append("find_idempotency")
                return None  # no replay record (crash window / fresh conflict)
            return None

        async def fetch(self, sql, *args):
            return []

        async def fetchval(self, sql, *args):
            return 0

    class _SingleConnPool:
        @asynccontextmanager
        async def acquire(self):
            yield _ConflictConn()

    servicer = KBServiceServicer(pool=_SingleConnPool())
    req = kb_pb.CreateKBRequest(
        tenant_id=TENANT_ID, name="kb-taken", embedding_model="bge-m3"
    )

    import asyncio

    with pytest.raises(RuntimeError, match="ALREADY_EXISTS"):
        asyncio.new_event_loop().run_until_complete(
            servicer._create_kb(req, _StubContext())
        )
    # The abort happens before any write — a 409 path must stay side-effect free.

# ── CreateKB poison-key self-heal ────────────────────────────────────────────


def test_create_kb_poison_key_self_heals():
    """A prior CreateKB attempt crashed between create_task and complete_task,
    leaving a kb_operations row (pending, result=NULL). The replay check skips
    it (no result), the KB insert + Core vector-store call re-run, and the
    task INSERT now violates UNIQUE(tenant_id, idempotency_key). The servicer
    must reuse the pending row, complete it, and return success."""
    poison_task_id = uuid.uuid4()

    class _PoisonConn:
        """Minimal conn for the CreateKB poison-key flow.

        find_by_idempotency_key: first call returns the stale pending row
        (no result → no replay); the post-violation re-find returns it too.
        create_task INSERT raises UniqueViolationError.
        """

        def __init__(self):
            self.events: list[str] = []

        def transaction(self):
            @asynccontextmanager
            async def _tx():
                yield self
            return _tx()

        async def execute(self, sql, *args):
            return "UPDATE 1"

        async def fetchrow(self, sql, *args):
            if "INSERT INTO knowledge_bases" in sql:
                self.events.append("insert_kb")
                return {
                    "id": uuid.uuid4(),
                    "tenant_id": uuid.UUID(TENANT_ID),
                    "name": "kb-poison",
                    "description": "",
                    "embedding_model": "bge-m3",
                    "chunk_size": 1024,
                    "top_k": 5,
                    "score_threshold": 0.0,
                    "retrieval_mode": "hybrid",
                    "status": "active",
                    "doc_count": 0,
                }
            if "FROM kb_operations" in sql and "idempotency_key" in sql:
                self.events.append("find_idempotency")
                return {
                    "id": poison_task_id,
                    "status": "pending",  # stale: never completed
                    "result": None,
                }
            if "INSERT INTO kb_operations" in sql:
                self.events.append("insert_kb_operations")
                raise asyncpg.UniqueViolationError(
                    'duplicate key value violates unique constraint '
                    '"kb_operations_tenant_id_idempotency_key_key"'
                )
            if "INSERT INTO kb_audit_log" in sql:
                self.events.append("insert_audit")
                return {"id": uuid.uuid4()}
            return None

        async def fetch(self, sql, *args):
            return []

        async def fetchval(self, sql, *args):
            return 0

    class _SingleConnPool:
        @asynccontextmanager
        async def acquire(self):
            yield _PoisonConn()

    servicer = KBServiceServicer(
        pool=_SingleConnPool(),
        vector_store=_FakeVectorStore(),
    )
    req = kb_pb.CreateKBRequest(
        tenant_id=TENANT_ID, name="kb-poison", embedding_model="bge-m3"
    )

    import asyncio

    result = asyncio.new_event_loop().run_until_complete(
        servicer._create_kb(req, _StubContext())
    )

    # Retry self-heals: returns the KB row instead of UNKNOWN.
    assert result.name == "kb-poison"


class _StubContext:
    """grpc.ServicerContext stand-in (abort raises)."""

    def abort(self, code, message):
        raise RuntimeError(f"aborted: {code} {message}")


# ── CreateKB: replay of a soft-deleted KB must re-create (B-fix) ─────────────


def test_create_kb_replay_of_deleted_kb_recreates():
    """Bug: delete → same-name re-create returned the deleted KB's old id
    and the new KB never appeared in the list. The name-fallback key
    create_kb:{tenant}:{name} is shared by every same-name create in the
    tenant, so the replay check finds the deleted KB's completed task row.
    The guard must skip the stale replay (get_kb hides deleted rows →
    None) and take the INSERT path: a NEW kb id, not the deleted KB's
    snapshot. The Core idempotency keys must also be derived per-request
    (create_vs:{tenant}:{kb_id}), never the shared fallback key."""
    new_kb_id = uuid.uuid4()

    class _ReplayDeletedConn:
        """find_by_idempotency_key → a completed task whose result is the
        DELETED KB's snapshot (id = the OLD kb id); get_kb → None
        (soft-deleted rows are hidden); the kb INSERT → a fresh row."""

        def __init__(self):
            self.events: list[str] = []

        def transaction(self):
            @asynccontextmanager
            async def _tx():
                yield self
            return _tx()

        async def execute(self, sql, *args):
            return "UPDATE 1"

        async def fetchrow(self, sql, *args):
            if "FROM kb_operations" in sql and "idempotency_key" in sql:
                self.events.append("find_idempotency")
                return {
                    "id": uuid.uuid4(),
                    "status": "completed",
                    # JSONB snapshot of the deleted KB (OLD id).
                    "result": {
                        "id": KB_ID,
                        "tenant_id": TENANT_ID,
                        "name": "kb-replay",
                        "status": "deleted",
                    },
                }
            if "INSERT INTO knowledge_bases" in sql:
                self.events.append("insert_kb")
                return {
                    "id": new_kb_id,
                    "tenant_id": uuid.UUID(TENANT_ID),
                    "name": "kb-replay",
                    "description": "",
                    "embedding_model": "bge-m3",
                    "chunk_size": 1024,
                    "top_k": 5,
                    "score_threshold": 0.0,
                    "retrieval_mode": "hybrid",
                    "status": "active",
                    "doc_count": 0,
                }
            if "INSERT INTO kb_audit_log" in sql:
                self.events.append("insert_audit")
                return {"id": uuid.uuid4()}
            if "INSERT INTO kb_operations" in sql:
                self.events.append("insert_kb_operations")
                return {"id": uuid.uuid4(), "status": "pending"}
            if "FROM knowledge_bases" in sql:
                # get_kb guard: the recorded KB is soft-deleted → hidden.
                self.events.append("get_kb_deleted")
                return None
            return None

        async def fetch(self, sql, *args):
            return []

        async def fetchval(self, sql, *args):
            return 0

    conn = _ReplayDeletedConn()

    class _SharedPool:
        @asynccontextmanager
        async def acquire(self):
            yield conn

    vs = _FakeVectorStore()
    servicer = KBServiceServicer(
        pool=_SharedPool(), vector_store=vs
    )
    req = kb_pb.CreateKBRequest(
        tenant_id=TENANT_ID, name="kb-replay", embedding_model="bge-m3"
    )

    import asyncio

    result = asyncio.new_event_loop().run_until_complete(
        servicer._create_kb(req, _StubContext())
    )

    # The re-create took the INSERT path — not the deleted KB's snapshot.
    assert "insert_kb" in conn.events
    assert str(result.id) != KB_ID
    assert str(result.id) == str(new_kb_id)
    assert result.name == "kb-replay"
    # D1 (issue-056): the dedicated Milvus collection is derived from the
    # NEW kb id — never the deleted KB's collection. Dim falls back to
    # EMBEDDING_DIM because the rag-engine probe fails in this test env.
    from app.core.config import settings

    create_calls = [c for c in vs.calls if c[0] == "create_collection"]
    assert len(create_calls) == 1
    assert create_calls[0][1]["name"] == _vector_store_name(str(new_kb_id))
    assert create_calls[0][1]["dim"] == settings.embedding_dim


# ── CreateKB: same-key replay returns the recorded result (issue-051 AC) ─────


def test_create_kb_same_key_replays_recorded_result():
    """AC (issue-051): a repeated CreateKB with the same idempotency_key
    returns the recorded kb_operations result without re-executing — no
    Core vector-store call, no knowledge_bases INSERT, no audit row. The
    replay guard (get_kb → live row) passes, so the recorded snapshot is
    the response."""
    recorded_kb_id = str(uuid.uuid4())
    recorded_row = {
        "id": recorded_kb_id,
        "tenant_id": TENANT_ID,
        "name": "kb-replay",
        "description": "",
        "embedding_model": "bge-m3",
        "chunk_size": 1024,
        "top_k": 5,
        "score_threshold": 0.0,
        "retrieval_mode": "hybrid",
        "status": "active",
        "doc_count": 0,
    }

    class _ReplayConn:
        def __init__(self):
            self.sqls: list[str] = []

        def transaction(self):
            @asynccontextmanager
            async def _tx():
                yield self
            return _tx()

        async def execute(self, sql, *args):
            self.sqls.append(sql)
            return "UPDATE 1"

        async def fetchrow(self, sql, *args):
            self.sqls.append(sql)
            if "FROM kb_operations" in sql and "idempotency_key" in sql:
                return {
                    "id": uuid.uuid4(),
                    "status": "completed",
                    "result": recorded_row,
                }
            if "FROM knowledge_bases" in sql:
                # replay guard: the recorded KB is still alive
                return {"id": uuid.UUID(recorded_kb_id), "status": "active"}
            return None

        async def fetch(self, sql, *args):
            return []

        async def fetchval(self, sql, *args):
            return 0

    class _Pool:
        def __init__(self, conn):
            self._conn = conn

        @asynccontextmanager
        async def acquire(self):
            yield self._conn

    conn = _ReplayConn()

    servicer = KBServiceServicer(pool=_Pool(conn))
    import asyncio

    result = asyncio.new_event_loop().run_until_complete(
        servicer._create_kb(
            # CreateKBRequest carries no idempotency_key; the servicer falls
            # back to the deterministic create_kb:{tenant}:{name} key.
            kb_pb.CreateKBRequest(
                tenant_id=TENANT_ID, name="kb-replay", embedding_model="bge-m3"
            ),
            _StubContext(),
        )
    )

    assert result.id == recorded_kb_id
    assert result.name == "kb-replay"
    # Nothing re-executed: no business INSERT.
    assert not any("INSERT INTO" in s for s in conn.sqls)
    # The replay lookup itself filtered by operation_type='kb.create'.
    assert any(
        "FROM kb_operations" in s and "operation_type = $3" in s
        for s in conn.sqls
    )


# ── DeleteKB: kb.create replay-record cleanup (C-fix) ────────────────────────


def test_delete_kb_cleans_create_replay_records():
    """DeleteKB must drop the deleted KB's kb.create kb_operations replay
    rows (C-fix): the create fallback key create_kb:{tenant}:{name} is
    shared by every same-name create, so a surviving row replays the
    deleted KB's snapshot on the next same-name create. The cleanup runs
    after the soft-delete, is best-effort, and does not block the Core
    vector-store cleanup."""
    class _DeleteConn:
        def __init__(self):
            self.execute_calls: list[tuple] = []
            self._kb_row = {
                "id": uuid.UUID(KB_ID),
                "tenant_id": uuid.UUID(TENANT_ID),
                "name": "kb",
                "description": "desc",
                "embedding_model": "bge-m3",
                "chunk_size": 512,
                "top_k": 5,
                "score_threshold": 0.3,
                "retrieval_mode": "hybrid",
                "status": "active",
                "doc_count": 0,
                "default_inference_service": None,
                "vector_store_id": "77777777-7777-7777-7777-777777777777",
            }

        def transaction(self):
            @asynccontextmanager
            async def _tx():
                yield self
            return _tx()

        async def execute(self, sql, *args):
            self.execute_calls.append((sql, args))
            if sql.startswith("SET LOCAL"):
                return None
            if "DELETE FROM kb_operations" in sql:
                return "DELETE 1"
            return "UPDATE 1"

        async def fetchrow(self, sql, *args):
            if "INSERT INTO kb_audit_log" in sql:
                return {"id": uuid.uuid4()}
            if "FROM knowledge_bases" in sql:
                return self._kb_row
            return None

        async def fetch(self, sql, *args):
            return []

        async def fetchval(self, sql, *args):
            return 0

    conn = _DeleteConn()

    class _SharedPool:
        @asynccontextmanager
        async def acquire(self):
            yield conn

    vs = _FakeVectorStore()

    class _FakeObjectStore:
        """Hermetic stand-in so DeleteKB's D1 object cleanup (issue-059)
        does not reach a real MinIO."""

        def __init__(self):
            self.removed_prefixes: list[str] = []

        async def remove_objects_by_prefix(self, **kwargs):
            self.removed_prefixes.append(kwargs["prefix"])
            return 0

    store = _FakeObjectStore()
    servicer = KBServiceServicer(
        pool=_SharedPool(),
        vector_store=vs, object_store=store,
    )
    req = kb_pb.DeleteKBRequest(tenant_id=TENANT_ID, kb_id=KB_ID)

    import asyncio

    result = asyncio.new_event_loop().run_until_complete(
        servicer._delete_kb(req, _StubContext())
    )
    assert result is not None

    # The soft-delete ran…
    soft_delete = [
        (s, a) for s, a in conn.execute_calls
        if "UPDATE knowledge_bases" in s and "status = 'deleted'" in s
    ]
    assert len(soft_delete) == 1
    # …and the kb.create replay rows were deleted afterwards.
    replay_delete = [
        (s, a) for s, a in conn.execute_calls
        if "DELETE FROM kb_operations" in s
    ]
    assert len(replay_delete) == 1
    sql, args = replay_delete[0]
    assert "operation_type = 'kb.create'" in sql
    assert args == (uuid.UUID(TENANT_ID), uuid.UUID(KB_ID))
    assert conn.execute_calls.index(soft_delete[0]) < conn.execute_calls.index(
        replay_delete[0]
    )
    # D1: the KB's persisted Milvus collection is dropped (best-effort) —
    # the kb row's vector_store_id is now the collection name.
    assert any(
        c[0] == "drop_collection"
        and c[1]["name"] == "77777777-7777-7777-7777-777777777777"
        for c in vs.calls
    )
