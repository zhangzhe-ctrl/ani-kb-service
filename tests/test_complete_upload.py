"""Tests for the D1 upload three-step flow in the servicer (issue-058, ADR §D1).

Verifies GetDocumentUploadURL (presign) + CompleteDocumentUpload wiring:

1. Presign: kb-own bucket key rule `{kb_id}/{doc_id}` (no Core round trip,
   no kb_documents write — registration deferred to complete), presigned
   URL from the injected object store, doc row NOT created.
2. Complete: HeadObject-gated single transaction writes kb_documents +
   doc.create audit + kb_operations + kb_outbox_events; outbox payload
   carries the real file_name (parse consumer needs it for extension
   routing) and the HeadObject-measured file_size_bytes (trusted over the
   request self-report).
3. HeadObject rejection: object not found (ObjectNotFoundError) aborts
   FAILED_PRECONDITION BEFORE any DB write — no ghost pending row, no
   outbox event; empty object (size 0) same. MinIO down (plain
   ObjectStoreError) aborts UNAVAILABLE without a write.
4. Idempotent replay: a prior complete with the same key returns the
   recorded task; a duplicate (kb, doc) under a different key aborts
   ALREADY_EXISTS before create_document trips the pk UNIQUE.

No real DB/MinIO — recording fake conn (test_us010_wiring.py pattern) +
fake object store.
"""
import asyncio
import json
import os
import sys
import uuid
from contextlib import asynccontextmanager

import grpc
import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.api.grpc_server import KBServiceServicer
from app.generated.common.v1 import common_pb2
from app.generated.kb.v1 import kb_service_pb2 as kb_pb
from app.object_store import ObjectNotFoundError, ObjectStoreError

TENANT_ID = "11111111-1111-1111-1111-111111111111"
KB_ID = "22222222-2222-2222-2222-222222222222"
DOC_ID = "33333333-3333-3333-3333-333333333333"


# ── fakes ─────────────────────────────────────────────────────────────────────


class _FakeContext:
    """grpc.ServicerContext stand-in — abort records and raises."""

    def __init__(self, *, metadata=None):
        self.aborted = None
        self._metadata = metadata or ()

    def abort(self, code, message):
        self.aborted = (code, message)
        raise RuntimeError(f"aborted: {code} {message}")

    def code(self):
        return self.aborted[0] if self.aborted else None

    def details(self):
        return self.aborted[1] if self.aborted else ""

    def invocation_metadata(self):
        return self._metadata


class _FakeObjectStore:
    """Recording fake of MinioObjectStore's servicer-facing surface."""

    def __init__(self, *, stat=None, stat_exc=None, presign_url="http://minio/put"):
        self.calls: list[tuple] = []
        self._stat = stat if stat is not None else {"size": 1024}
        self._stat_exc = stat_exc
        self._presign_url = presign_url

    async def presigned_put_object(self, *, key):
        self.calls.append(("presign", key))
        return self._presign_url

    async def stat_object(self, *, key):
        self.calls.append(("stat", key))
        if self._stat_exc is not None:
            raise self._stat_exc
        return dict(self._stat)


class _CompleteMockConn:
    """Recording fake conn for the complete flow (notify pattern, extended
    with the kb_documents INSERT dispatch the complete path adds).

    Business-row knobs: existing_task (idempotency replay), dup_doc
    (duplicate-doc guard), kb_row (KB gate; None → NOT_FOUND),
    insert_raise_exc (UNIQUE race), race_task (post-race re-lookup row)."""

    def __init__(
        self,
        *,
        kb_row="default",
        existing_task=None,
        dup_doc=None,
        insert_raise_exc=None,
        race_task=None,
    ):
        self._kb_row = (
            {
                "id": uuid.UUID(KB_ID), "tenant_id": uuid.UUID(TENANT_ID),
                "name": "kb", "status": "active", "chunk_size": 512,
            }
            if kb_row == "default" else kb_row
        )
        self.existing_task = existing_task
        self.dup_doc = dup_doc
        self.insert_raise_exc = insert_raise_exc
        self.race_task = race_task
        self._find_calls = 0
        self._doc_reads = 0
        self.events: list[tuple] = []

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            self.events.append(("BEGIN",))
            try:
                yield self
            finally:
                self.events.append(("COMMIT",))

        return _tx()

    async def execute(self, sql, *args):
        if "SET LOCAL" in sql:
            self.events.append(("set_tenant", args))
            return None
        return "UPDATE 1"

    async def fetchrow(self, sql, *args):
        # find_by_idempotency_key → existing task or None
        if "FROM kb_operations" in sql and "idempotency_key" in sql:
            self.events.append(("find_idempotency", args))
            self._find_calls += 1
            if self._find_calls > 1:
                return self.race_task
            return self.existing_task
        # get_kb → KB gate row (missing → the 404 abort in the tx)
        if "FROM knowledge_bases" in sql:
            self.events.append(("get_kb", args))
            return self._kb_row
        # get_document → duplicate-doc guard; None → proceed to create
        if "FROM kb_documents" in sql:
            self.events.append(("get_document", args))
            self._doc_reads += 1
            return self.dup_doc
        # revive_operation_in_tx (failed-row revival on the race path)
        if "UPDATE kb_operations" in sql and "RETURNING" in sql:
            self.events.append(("revive_kb_operations", args))
            return {"id": uuid.uuid4(), "operation_type": "kb.parse", "status": "pending"}
        # create_operation_in_tx INSERT ... RETURNING
        if "INSERT INTO kb_operations" in sql:
            self.events.append(("insert_kb_operations", args))
            if self.insert_raise_exc is not None:
                raise self.insert_raise_exc
            return {"id": uuid.UUID(DOC_ID), "operation_type": "kb.parse", "status": "pending"}
        # create_document INSERT ... RETURNING (the D1 registration write)
        if "INSERT INTO kb_documents" in sql:
            self.events.append(("insert_kb_documents", args))
            return {"id": uuid.UUID(DOC_ID)}
        # audit insert (same tx as the business write)
        if "INSERT INTO kb_audit_log" in sql:
            self.events.append(("insert_audit", args))
            return {"id": uuid.uuid4()}
        # outbox event insert
        if "INSERT INTO kb_outbox_events" in sql:
            self.events.append(("insert_outbox_events", args))
            return {"id": 1}
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


def _make_servicer(conn, object_store):
    return KBServiceServicer(pool=_Pool(conn), object_store=object_store)


def _complete_req(**over):
    req = {
        "tenant_id": TENANT_ID, "kb_id": KB_ID, "doc_id": DOC_ID,
        "file_name": "a.pdf", "file_type": "pdf",
        "checksum_sha256": "abc123", "custom_metadata": "",
        "idempotency_key": str(uuid.uuid4()),
    }
    req.update(over)
    return kb_pb.CompleteDocumentUploadRequest(**req)


def _kinds(conn):
    return [e[0] for e in conn.events]


# ── presign: key rule + deferred registration ─────────────────────────────────


async def test_presign_uses_kb_own_key_and_defers_registration():
    """GetDocumentUploadURL: doc_id reserved + presigned against the kb-own
    bucket key `{kb_id}/{doc_id}`; NO kb_documents write (D1 three-step —
    registration is deferred to complete), no Core round trip."""
    conn = _CompleteMockConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    result = await servicer._get_document_upload_url(
        kb_pb.GetDocumentUploadURLRequest(
            tenant_id=TENANT_ID, kb_id=KB_ID, file_name="a.pdf",
            file_type="pdf", file_size_bytes=1024, checksum_sha256="abc",
            idempotency_key=str(uuid.uuid4()),
        ),
        ctx,
    )

    assert result.doc_id
    # D1 key rule: {kb_id}/{doc_id}, no filename suffix.
    assert result.storage_path == f"{KB_ID}/{result.doc_id}"
    assert result.upload_url == "http://minio/put"
    # Presign was served by kb's own object store (not Core).
    assert store.calls == [("presign", f"{KB_ID}/{result.doc_id}")]
    # Registration deferred: no kb_documents INSERT on the presign path.
    assert "insert_kb_documents" not in _kinds(conn)


async def test_presign_minio_down_maps_unavailable_not_audited():
    """Presign infra failure → UNAVAILABLE, no doc.create failure audit
    (same treatment as the Core-down path it replaces)."""
    from app.api.grpc_server import ObjectStoreError as _OSE  # same class

    class _FailingStore(_FakeObjectStore):
        async def presigned_put_object(self, *, key):
            raise ObjectStoreError("presign failed")

    conn = _CompleteMockConn()
    servicer = _make_servicer(conn, _FailingStore())
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="UNAVAILABLE"):
        await servicer._get_document_upload_url(
            kb_pb.GetDocumentUploadURLRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID, file_name="a.pdf",
                file_type="pdf", file_size_bytes=1024,
                idempotency_key=str(uuid.uuid4()),
            ),
            ctx,
        )
    assert ctx.aborted[0] == grpc.StatusCode.UNAVAILABLE
    # No audit insert (UNAVAILABLE is not an audited failure code).
    assert "insert_audit" not in _kinds(conn)


async def test_presign_kb_missing_maps_not_found():
    """KB existence gate precedes presign: missing KB → NOT_FOUND (404) not
    a presigned URL for an object under a dead KB."""
    conn = _CompleteMockConn(kb_row=None)
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._get_document_upload_url(
            kb_pb.GetDocumentUploadURLRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID, file_name="a.pdf",
                file_type="pdf", file_size_bytes=1024,
                idempotency_key=str(uuid.uuid4()),
            ),
            ctx,
        )
    assert ctx.aborted[0] == grpc.StatusCode.NOT_FOUND
    assert store.calls == []  # no presign for a dead KB


# ── complete: HeadObject-gated registration transaction ───────────────────────


async def test_complete_writes_doc_task_audit_outbox_in_one_tx():
    """AC: complete 登记流转。HeadObject passes → a single transaction
    writes kb_documents (pending) + kb_operations + kb_outbox_events, and
    the doc.create audit lands in the SAME transaction."""
    conn = _CompleteMockConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    result = await servicer._complete_document_upload(_complete_req(), ctx)

    # HeadObject ran against the D1 key before anything else.
    assert store.calls == [("stat", f"{KB_ID}/{DOC_ID}")]
    assert result.task_type == "kb.parse"
    assert result.status == "pending"
    assert result.task_id  # non-empty

    kinds = _kinds(conn)
    # The four writes, in D1 order: doc row → task → audit → outbox.
    write_order = [k for k in kinds if k in (
        "insert_kb_documents", "insert_kb_operations", "insert_audit",
        "insert_outbox_events",
    )]
    assert write_order == [
        "insert_kb_documents", "insert_kb_operations", "insert_audit",
        "insert_outbox_events",
    ]
    # Outbox payload: real file_name + HeadObject-measured size task chain.
    outbox = [e for e in conn.events if e[0] == "insert_outbox_events"][0]
    payload = json.loads(outbox[1][4])
    assert payload["file_name"] == "a.pdf"
    assert payload["storage_path"] == f"{KB_ID}/{DOC_ID}"
    assert payload["object_id"] == ""
    assert payload["task_id"] == DOC_ID  # fake conn returns DOC_ID as task id


async def test_complete_registers_headobject_measured_size():
    """file_size_bytes comes from the HeadObject stat (trusted), not from
    any client self-report — the complete proto carries no size field."""
    conn = _CompleteMockConn()
    store = _FakeObjectStore(stat={"size": 2048})
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    await servicer._complete_document_upload(_complete_req(), ctx)

    insert = [e for e in conn.events if e[0] == "insert_kb_documents"][0]
    # create_document args: (id, kb_id, tenant_id, file_name, file_type,
    # file_size_bytes, storage_path, checksum, metadata, object_id) —
    # positional order of the INSERT in repositories/document.py.
    assert insert[1][5] == 2048


async def test_complete_object_not_found_rejects_registration():
    """AC: HeadObject 校验失败拒绝登记。ObjectNotFoundError →
    FAILED_PRECONDITION BEFORE any DB write: no ghost pending row, no
    outbox, no task; the doc.create failure audit records the intent."""
    conn = _CompleteMockConn()
    store = _FakeObjectStore(stat_exc=ObjectNotFoundError("object not found"))
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._complete_document_upload(_complete_req(), ctx)

    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    kinds = _kinds(conn)
    assert "insert_kb_documents" not in kinds
    assert "insert_kb_operations" not in kinds
    assert "insert_outbox_events" not in kinds
    # Business rejection is audited (FAILED_PRECONDITION is an audited code).
    audits = [e for e in conn.events if e[0] == "insert_audit"]
    assert len(audits) == 1
    assert audits[0][1][3] == "doc.create"      # action
    assert audits[0][1][6] == "FAILED_PRECONDITION"  # error_code


async def test_complete_empty_object_rejects_registration():
    """Size-0 object (empty PUT) → same FAILED_PRECONDITION rejection."""
    conn = _CompleteMockConn()
    store = _FakeObjectStore(stat={"size": 0})
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._complete_document_upload(_complete_req(), ctx)

    kinds = _kinds(conn)
    assert "insert_kb_documents" not in kinds
    assert "insert_outbox_events" not in kinds


async def test_complete_minio_down_maps_unavailable_without_audit():
    """MinIO unreachable (non-NoSuchKey) → UNAVAILABLE infrastructure
    failure; NOT the FAILED_PRECONDITION business rejection, and no
    failure audit (same code family as the presign path)."""
    conn = _CompleteMockConn()
    store = _FakeObjectStore(stat_exc=ObjectStoreError("connection refused"))
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="UNAVAILABLE"):
        await servicer._complete_document_upload(_complete_req(), ctx)

    assert ctx.aborted[0] == grpc.StatusCode.UNAVAILABLE
    assert "insert_kb_documents" not in _kinds(conn)
    assert "insert_audit" not in _kinds(conn)  # infra failure not audited


# ── complete: idempotency / duplicates ────────────────────────────────────────


async def test_complete_replay_returns_recorded_task():
    """A retry with the same idempotency key replays the recorded
    AsyncTaskRef without re-writing the doc row or the outbox."""
    existing = {
        "id": uuid.uuid4(), "operation_type": "kb.parse", "status": "pending",
    }
    conn = _CompleteMockConn(existing_task=existing)
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    result = await servicer._complete_document_upload(_complete_req(), ctx)

    assert str(result.task_id) == str(existing["id"])
    assert result.status == "pending"
    kinds = _kinds(conn)
    assert "insert_kb_documents" not in kinds
    assert "insert_kb_operations" not in kinds
    assert "insert_outbox_events" not in kinds


async def test_complete_duplicate_doc_different_key_already_exists():
    """Same (kb, doc) completed before under a DIFFERENT key: the doc row
    exists → ALREADY_EXISTS (clean reject) instead of the create_document
    pk UNIQUE violation surfacing as gRPC UNKNOWN."""
    conn = _CompleteMockConn(dup_doc={"id": uuid.UUID(DOC_ID)})
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="ALREADY_EXISTS"):
        await servicer._complete_document_upload(_complete_req(), ctx)

    assert ctx.aborted[0] == grpc.StatusCode.ALREADY_EXISTS
    kinds = _kinds(conn)
    assert "insert_kb_documents" not in kinds
    assert "insert_outbox_events" not in kinds


async def test_complete_kb_missing_in_tx_maps_not_found():
    """KB gate inside the tx: missing KB → NOT_FOUND, no doc/task/outbox
    write (the doc.create failure audit records the intent)."""
    conn = _CompleteMockConn(kb_row=None)
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._complete_document_upload(_complete_req(), ctx)

    assert ctx.aborted[0] == grpc.StatusCode.NOT_FOUND
    kinds = _kinds(conn)
    assert "insert_kb_documents" not in kinds
    assert "insert_outbox_events" not in kinds
    audits = [e for e in conn.events if e[0] == "insert_audit"]
    assert len(audits) == 1
    assert audits[0][1][3] == "doc.create"
    assert audits[0][1][6] == "NOT_FOUND"


async def test_complete_unique_race_pending_replays_winner():
    """TOCTOU self-heal (notify pattern): the pre-check sees no task, a
    concurrent complete commits in between, the UNIQUE violation is
    caught in a SAVEPOINT and the winner's task is replayed — no second
    outbox event."""
    import asyncpg

    winner = uuid.uuid4()
    conn = _CompleteMockConn(
        insert_raise_exc=asyncpg.UniqueViolationError(
            'duplicate key value violates unique constraint '
            '"kb_operations_tenant_id_idempotency_key_key"'
        ),
        race_task={"id": winner, "operation_type": "kb.parse", "status": "pending"},
    )
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    result = await servicer._complete_document_upload(_complete_req(), ctx)

    assert str(result.task_id) == str(winner)
    assert result.status == "pending"
    kinds = _kinds(conn)
    # The doc row registered (outer tx survives the SAVEPOINT abort)…
    assert "insert_kb_documents" in kinds
    # …but no second outbox event for the winner's already-published one.
    assert "insert_outbox_events" not in kinds
