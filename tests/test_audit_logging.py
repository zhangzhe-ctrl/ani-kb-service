"""Tests for KB audit logging (B8 #21, plan §6.3/§6.4).

Verifies the audit contract of the 8 instrumented write paths plus the
ListKBAuditLogs read RPC:

- Success paths: INSERT INTO kb_audit_log runs in the SAME transaction as
  the business write, with the exact action / before_state / after_state
  snapshots (JSON payload contract, plan §6.3).
- Failure paths (business failures only): after the aborted business tx
  rolls back, the audit lands in a FRESH transaction with error_code /
  error_msg and the request intent as before_state (_record_failure_audit).
- Actor: x-user-id metadata (valid uuid → actor_user_id bound; invalid /
  missing → NULL). Test fakes without invocation_metadata() also yield
  NULL (defensive helper).
- Read RPC: keyset pagination (items mapping, next_cursor boundary),
  KB 404 gate, invalid cursor, limit bounds, empty tenant, no pool.
- Idempotent replay: no second audit INSERT.

No real DB — recording fake conn (same pattern as test_reparse_document.py
/ test_update_kb.py). fetchrow dispatches by table keywords; every audit
INSERT is captured with its full args tuple.
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

TENANT_ID = "11111111-1111-1111-1111-111111111111"
KB_ID = "22222222-2222-2222-2222-222222222222"
DOC_ID = "33333333-3333-3333-3333-333333333333"
USER_ID = "66666666-6666-6666-6666-666666666666"
TASK_ID = "44444444-4444-4444-4444-444444444444"


def _kb_row(**over):
    row = {
        "id": uuid.UUID(KB_ID),
        "tenant_id": uuid.UUID(TENANT_ID),
        "name": "kb",
        "description": "desc",
        "embedding_model": "bge-m3",
        "chunk_size": 512,
        "ocr_enabled": False,
        "top_k": 5,
        "score_threshold": 0.3,
        "retrieval_mode": "hybrid",
        "status": "active",
        "doc_count": 0,
        "vector_store_id": "77777777-7777-7777-7777-777777777777",
        "created_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
    }
    row.update(over)
    return row


def _doc_row(**over):
    row = {
        "id": uuid.UUID(DOC_ID),
        "kb_id": uuid.UUID(KB_ID),
        "tenant_id": uuid.UUID(TENANT_ID),
        "file_name": "a.pdf",
        "file_type": "pdf",
        "file_size_bytes": 1024,
        "storage_path": f"kb-docs/{KB_ID}/{DOC_ID}/a.pdf",
        "checksum_sha256": "abc123",
        "parse_status": "failed",
        "chunk_count": 3,
        "error_message": "boom",
        "object_id": "55555555-5555-5555-5555-555555555555",
    }
    row.update(over)
    return row


def _task_row(task_id=TASK_ID, status="pending", operation_type="kb.parse"):
    return {
        "id": uuid.UUID(task_id),
        "operation_type": operation_type,
        "status": status,
    }


# ── context fakes ──────────────────────────────────────────────────────────────


class _ServicerContext:
    """grpc.ServicerContext stand-in — abort raises (no code()/details()).

    Mirrors the fakes in test_reparse_document.py / test_update_kb.py: the
    audit failure helper treats a context without code() as "not auditable"
    (defensive), so this fake pins that behavior.
    """

    def __init__(self):
        self.aborted = None

    def abort(self, code, message):
        self.aborted = (code, message)
        raise RuntimeError(f"aborted: {code} {message}")

    def invocation_metadata(self):
        return ()


class _AuditServicerContext:
    """ServicerContext with code()/details()/invocation_metadata().

    abort() records (code, message) and raises; code()/details() return the
    recorded values so _audit_failure_info sees the already-set state —
    the same contract as the real grpc context (abort sets state, then
    raises; the wrapper reads it off the context).
    """

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


# ── mock conn family ──────────────────────────────────────────────────────────


class _AuditConn:
    """Recording fake conn covering every write path's repository SQL.

    fetchrow dispatches by table keywords (kb_operations lookups, knowledge_
    bases / kb_documents / kb_permissions reads, INSERT ... RETURNING);
    fetchval is unused by these paths. `audit_calls` records every
    INSERT INTO kb_audit_log args tuple:

        args[0]=tenant uuid, args[1]=kb uuid, args[2]=actor uuid|None,
        args[3]=action, args[4]=before_state JSON|None, args[5]=after_state
        JSON|None, args[6]=error_code|None, args[7]=error_msg|None

    Business-row knobs: kb_row (get_kb gate), doc_row (get_document),
    existing_task (idempotency replay), perm_row (get_permissions before
    snapshot), update_result / soft_delete_result (UPDATE row counts).
    """

    def __init__(
        self,
        *,
        kb_row="default",
        doc_row="default",
        existing_task=None,
        perm_row=None,
        update_result="UPDATE 1",
        soft_delete_result="UPDATE 1",
    ):
        self._kb_row = _kb_row() if kb_row == "default" else kb_row
        self._doc_row = _doc_row() if doc_row == "default" else doc_row
        self.existing_task = existing_task
        self._perm_row = perm_row
        self.update_result = update_result
        self.soft_delete_result = soft_delete_result
        self.audit_calls: list[tuple] = []
        self.fetchrow_calls: list[tuple] = []
        self.execute_calls: list[tuple] = []
        self.fetch_calls: list[tuple] = []
        # TX nesting: the audit INSERT must run INSIDE the business tx on
        # success paths (same-transaction contract) — track open tx depth.
        self._tx_depth = 0
        self.audit_tx_depths: list[int] = []

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            self._tx_depth += 1
            try:
                yield self
            finally:
                self._tx_depth -= 1

        return _tx()

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        if sql.startswith("SET LOCAL"):
            return None
        if "UPDATE knowledge_bases" in sql and "status = 'deleted'" in sql:
            return self.soft_delete_result
        if "UPDATE knowledge_bases" in sql:
            return self.update_result
        if "UPDATE kb_documents" in sql:
            return self.update_result
        if "UPDATE kb_operations" in sql:
            return "UPDATE 1"
        if "INSERT INTO kb_permissions" in sql:
            return None
        if "DELETE FROM kb_chunks" in sql:
            return "DELETE 1"
        return "UPDATE 1"

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        if "INSERT INTO kb_audit_log" in sql:
            self.audit_calls.append(args)
            self.audit_tx_depths.append(self._tx_depth)
            return {"id": uuid.uuid4()}
        if "FROM kb_operations" in sql and "idempotency_key" in sql:
            return self.existing_task
        # update_kb: UPDATE ... RETURNING (fetchrow form) — echo the row
        # back with the non-empty name/description applied (COALESCE+NULLIF
        # semantics: empty means "keep current"). kb_row=None models RLS
        # hiding the row: the UPDATE matches nothing and RETURNING yields
        # no row (update_kb → None → the servicer's 404 abort).
        if "UPDATE knowledge_bases" in sql and "RETURNING" in sql:
            if self._kb_row is None:
                return None
            row = dict(self._kb_row)
            if len(args) > 1 and args[1]:
                row["name"] = args[1]
            if len(args) > 2 and args[2]:
                row["description"] = args[2]
            return row
        if "FROM knowledge_bases" in sql:
            return self._kb_row
        if "FROM kb_documents" in sql:
            return self._doc_row
        if "FROM kb_permissions" in sql:
            return self._perm_row
        if "INSERT INTO kb_operations" in sql:
            return _task_row()
        if "INSERT INTO kb_outbox_events" in sql:
            return {"id": 1}
        if "INSERT INTO knowledge_bases" in sql:
            return _kb_row()
        if "INSERT INTO kb_documents" in sql:
            return _doc_row()
        return None

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        return []

    async def fetchval(self, sql, *args):
        return 0


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


def _make_servicer(conn, *, vector_store=None, object_store=None):
    return KBServiceServicer(
        pool=_Pool(conn), vector_store=vector_store,
        object_store=object_store,
    )


class _FakeObjectStore:
    """MinioObjectStore fake for D1 object paths (issue-058/059): records
    presigned_put_object + remove_object/remove_objects_by_prefix calls
    instead of hitting a real MinIO endpoint."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self._removed: list[str] = []

    async def presigned_put_object(self, **kwargs):
        self.calls.append(("presigned_put_object", kwargs))
        return f"http://minio.test/put?{kwargs['key']}"

    async def remove_object(self, **kwargs):
        self.calls.append(("remove_object", kwargs))
        self._removed.append(kwargs["key"])
        return True

    async def remove_objects_by_prefix(self, **kwargs):
        self.calls.append(("remove_objects_by_prefix", kwargs))
        return 0


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


# ── helpers ────────────────────────────────────────────────────────────────────


def _audit_json(state):
    import json
    return json.loads(state) if state is not None else None


def _user_metadata(user=USER_ID):
    return (("x-user-id", user),)


# ════════════════════════════════════════════════════════════════════════════
# A. Write paths — success audit (same-transaction, exact snapshots)
# ════════════════════════════════════════════════════════════════════════════


async def test_kb_create_success_writes_audit_in_same_tx():
    conn = _AuditConn(existing_task=None)
    servicer = _make_servicer(conn, vector_store=_FakeVectorStore())
    ctx = _AuditServicerContext(metadata=_user_metadata())

    result = await servicer._create_kb(
        kb_pb.CreateKBRequest(
            tenant_id=TENANT_ID, name="kb", embedding_model="bge-m3"
        ),
        ctx,
    )

    assert result.name == "kb"
    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[0] == uuid.UUID(TENANT_ID)
    assert args[1] == uuid.UUID(KB_ID)
    assert args[2] == uuid.UUID(USER_ID)          # x-user-id metadata
    assert args[3] == "kb.create"
    assert args[4] is None                         # before: creation
    after = _audit_json(args[5])
    assert after == {
        "name": "kb", "description": "desc", "embedding_model": "bge-m3",
        "chunk_size": 512, "ocr_enabled": False, "top_k": 5,
        "score_threshold": 0.3, "retrieval_mode": "hybrid", "status": "active",
        "doc_count": 0,
        "default_inference_service": None,  # 未设置 → NULL 快照
    }
    assert args[6] is None and args[7] is None     # no error on success
    # Same transaction as the kb INSERT (audit commits iff the KB does).
    assert conn.audit_tx_depths[0] >= 1


async def test_kb_update_success_writes_before_and_after_snapshots():
    conn = _AuditConn()
    servicer = _make_servicer(conn)

    result = await servicer._update_kb(
        kb_pb.UpdateKBRequest(
            tenant_id=TENANT_ID, kb_id=KB_ID, idempotency_key=str(uuid.uuid4()),
            name="kb2",
        ),
        _ServicerContext(),
    )

    assert result.name == "kb2"
    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[3] == "kb.update"
    assert args[2] is None                         # no x-user-id → NULL actor
    before = _audit_json(args[4])
    after = _audit_json(args[5])
    assert before["name"] == "kb"                   # pre-UPDATE row
    assert after["name"] == "kb2"                   # post-UPDATE row
    assert conn.audit_tx_depths[0] >= 1


async def test_kb_delete_success_writes_before_snapshot_only():
    conn = _AuditConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(
        conn, vector_store=_FakeVectorStore(), object_store=store,
    )

    result = await servicer._delete_kb(
        kb_pb.DeleteKBRequest(tenant_id=TENANT_ID, kb_id=KB_ID),
        _AuditServicerContext(metadata=_user_metadata()),
    )

    assert result is not None
    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[3] == "kb.delete"
    assert args[2] == uuid.UUID(USER_ID)
    before = _audit_json(args[4])
    assert before["name"] == "kb"                   # the row being removed
    assert args[5] is None                          # after: deletion
    # Audit precedes the soft-delete UPDATE in the same tx.
    assert conn.audit_tx_depths[0] >= 1
    # The soft-delete UPDATE ran (executed after the audit insert).
    assert any(
        "UPDATE knowledge_bases" in s and "status = 'deleted'" in s
        for s, _ in conn.execute_calls
    )
    # D1 object cleanup (issue-059): full {kb_id}/ prefix sweep — the KB's
    # originals + image products all live under that prefix.
    assert ("remove_objects_by_prefix", {"prefix": f"{KB_ID}/"}) in store.calls


async def test_doc_create_success_writes_intent_snapshot():
    """D1 three-step flow (issue-058): the presign endpoint only reserves
    doc_id + signs the PUT URL — no kb_documents write and no doc.create
    audit lands here (both moved to CompleteDocumentUpload, covered by
    test_complete_upload.py). This pins the presign side of the split:
    zero DB writes on success, key rule {kb_id}/{doc_id}."""
    conn = _AuditConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, object_store=store)

    result = await servicer._get_document_upload_url(
        kb_pb.GetDocumentUploadURLRequest(
            tenant_id=TENANT_ID, kb_id=KB_ID, file_name="a.pdf", file_type="pdf",
            file_size_bytes=1024, checksum_sha256="abc123",
            idempotency_key=str(uuid.uuid4()),
        ),
        _ServicerContext(),
    )

    assert result.doc_id
    # No audit, no document/task/outbox write on the presign success path.
    assert conn.audit_calls == []
    assert not any(
        sql.startswith("INSERT INTO") for sql, _ in conn.fetchrow_calls
    )
    # The presigned PUT went to the kb-owned bucket under the D1 key rule.
    assert store.calls == [
        ("presigned_put_object", {"key": f"{KB_ID}/{result.doc_id}"})
    ]
    assert result.storage_path == f"{KB_ID}/{result.doc_id}"


async def test_doc_delete_success_writes_before_snapshot_only():
    conn = _AuditConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(
        conn, vector_store=_FakeVectorStore(), object_store=store,
    )

    result = await servicer._delete_document(
        kb_pb.DeleteDocumentRequest(tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID),
        _AuditServicerContext(metadata=_user_metadata()),
    )

    assert result is not None
    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[3] == "doc.delete"
    assert args[2] == uuid.UUID(USER_ID)
    before = _audit_json(args[4])
    assert before["doc_id"] == DOC_ID               # the row being removed
    assert before["file_name"] == "a.pdf"
    assert before["parse_status"] == "failed"
    assert args[5] is None                          # after: deletion
    assert conn.audit_tx_depths[0] >= 1
    # Chunk cleanup ran in the same tx.
    assert any("DELETE FROM kb_chunks" in s for s, _ in conn.execute_calls)
    # D1 object cleanup (issue-059): original by key (persisted storage_path) +
    # image products by {kb_id}/{doc_id}/images/ prefix.
    assert store.calls == [
        ("remove_object", {"key": f"kb-docs/{KB_ID}/{DOC_ID}/a.pdf"}),
        ("remove_objects_by_prefix", {"prefix": f"{KB_ID}/{DOC_ID}/images/"}),
    ]


async def test_doc_reparse_success_writes_reset_snapshots():
    conn = _AuditConn(doc_row=_doc_row(parse_status="failed", chunk_count=3))
    servicer = _make_servicer(conn)

    result = await servicer._reparse_document(
        kb_pb.ReparseDocumentRequest(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            idempotency_key=str(uuid.uuid4()),
        ),
        _ServicerContext(),
    )

    assert result.status == "pending"
    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[3] == "doc.reparse"
    before = _audit_json(args[4])
    after = _audit_json(args[5])
    assert before["parse_status"] == "failed"       # the failed state
    assert before["chunk_count"] == 3
    assert after["parse_status"] == "pending"       # reset lifecycle
    assert after["chunk_count"] == 0                # reset to 0 literal
    assert conn.audit_tx_depths[0] >= 1


async def test_kb_permissions_update_success_writes_perm_snapshots():
    conn = _AuditConn(perm_row={"kb_id": KB_ID, "public_read": False,
                                "allowed_user_ids": [], "updated_at": None})
    servicer = _make_servicer(conn)

    result = await servicer._update_kb_permissions(
        kb_pb.UpdateKBPermissionsRequest(
            tenant_id=TENANT_ID, kb_id=KB_ID,
            idempotency_key=str(uuid.uuid4()),
            public_read=True, allowed_user_ids=[USER_ID],
        ),
        _ServicerContext(),
    )

    assert result.name == "kb"
    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[3] == "kb.permissions.update"
    before = _audit_json(args[4])
    after = _audit_json(args[5])
    assert before == {"public_read": False, "allowed_user_ids": []}
    assert after == {"public_read": True, "allowed_user_ids": [USER_ID]}
    assert conn.audit_tx_depths[0] >= 1


async def test_idempotent_replay_writes_no_audit():
    """kb.update replay (recorded result): the business tx never runs, so
    no audit INSERT either. A replay is not a state change."""
    conn = _AuditConn(
        existing_task={**_task_row(status="completed", operation_type="kb.update"),
                       "result": _kb_row()}
    )
    servicer = _make_servicer(conn)

    result = await servicer._update_kb(
        kb_pb.UpdateKBRequest(
            tenant_id=TENANT_ID, kb_id=KB_ID, idempotency_key=str(uuid.uuid4()),
            name="kb2",
        ),
        _ServicerContext(),
    )
    assert result.name == "kb"                      # replayed row
    assert conn.audit_calls == []


# ════════════════════════════════════════════════════════════════════════════
# B. Write paths — failure audit (business failures only)
# ════════════════════════════════════════════════════════════════════════════


async def test_kb_update_kb_missing_records_failure_audit_in_fresh_tx():
    """404 business rejection: after the business tx rolls back, the audit
    lands in its own FRESH transaction (depth back to 1 — only the audit
    tx itself, no business tx nesting)."""
    conn = _AuditConn(kb_row=None)
    servicer = _make_servicer(conn)
    ctx = _AuditServicerContext(metadata=_user_metadata())

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._update_kb(
            kb_pb.UpdateKBRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID, idempotency_key=str(uuid.uuid4()),
                name="kb2",
            ),
            ctx,
        )
    assert ctx.aborted is not None

    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[3] == "kb.update"
    assert args[2] == uuid.UUID(USER_ID)
    before = _audit_json(args[4])
    assert before == {"name": "kb2", "description": ""}   # the request intent
    assert args[5] is None                                # no after on failure
    assert args[6] == "NOT_FOUND"                         # error_code
    assert "not found" in args[7].lower()                 # error_msg
    # FRESH transaction: only the audit's own tx was open (the aborted
    # business tx is gone). This is the plan §6.3 failure contract.
    assert conn.audit_tx_depths[0] == 1


async def test_doc_delete_kb_missing_records_failure_audit():
    conn = _AuditConn(kb_row=None)
    servicer = _make_servicer(conn)
    ctx = _AuditServicerContext()

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._delete_document(
            kb_pb.DeleteDocumentRequest(tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID),
            ctx,
        )
    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[3] == "doc.delete"
    before = _audit_json(args[4])
    assert before == {"doc_id": DOC_ID}             # intent
    assert args[6] == "NOT_FOUND"
    assert conn.audit_tx_depths[0] == 1


async def test_doc_create_kb_missing_records_failure_audit():
    conn = _AuditConn(kb_row=None)
    servicer = _make_servicer(conn, object_store=_FakeObjectStore())
    ctx = _AuditServicerContext()

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._get_document_upload_url(
            kb_pb.GetDocumentUploadURLRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID, file_name="a.pdf", file_type="pdf",
                file_size_bytes=1024, idempotency_key=str(uuid.uuid4()),
            ),
            ctx,
        )
    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[3] == "doc.create"
    # intent is None on this path (snapshot built after the KB gate passed)
    assert args[4] is None
    assert args[6] == "NOT_FOUND"


async def test_kb_permissions_update_kb_missing_records_failure_audit():
    conn = _AuditConn(kb_row=None)
    servicer = _make_servicer(conn)
    ctx = _AuditServicerContext()

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._update_kb_permissions(
            kb_pb.UpdateKBPermissionsRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID,
                idempotency_key=str(uuid.uuid4()),
                public_read=True,
            ),
            ctx,
        )
    assert len(conn.audit_calls) == 1
    args = conn.audit_calls[0]
    assert args[3] == "kb.permissions.update"
    before = _audit_json(args[4])
    assert before == {"public_read": True, "allowed_user_ids": []}
    assert args[6] == "NOT_FOUND"


async def test_invalid_argument_not_audited():
    """400-class validation rejections fire BEFORE the business tx and are
    never audited (user decision: business failures only)."""
    conn = _AuditConn()
    servicer = _make_servicer(conn)
    ctx = _AuditServicerContext()

    # kb.update: missing idempotency_key → INVALID_ARGUMENT, no audit.
    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await servicer._update_kb(
            kb_pb.UpdateKBRequest(tenant_id=TENANT_ID, kb_id=KB_ID, name="x"),
            ctx,
        )
    # list read: bad cursor → INVALID_ARGUMENT (no write, no audit).
    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await servicer._list_kb_audit_logs(
            kb_pb.ListKBAuditLogsRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID,
                page=common_pb2.CursorPageRequest(limit=5, cursor="garbage"),
            ),
            ctx,
        )
    assert conn.audit_calls == []


async def test_failure_audit_skipped_without_code_details():
    """Test fakes without code()/details() (the pre-B8 fake family): the
    failure wrapper reads state defensively — not auditable, no crash."""
    conn = _AuditConn(kb_row=None)
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()   # no code()/details()

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._update_kb(
            kb_pb.UpdateKBRequest(
                tenant_id=TENANT_ID, kb_id=KB_ID, idempotency_key=str(uuid.uuid4()),
                name="kb2",
            ),
            ctx,
        )
    assert ctx.aborted is not None
    assert conn.audit_calls == []


# ════════════════════════════════════════════════════════════════════════════
# C. Actor attribution
# ════════════════════════════════════════════════════════════════════════════


async def test_actor_invalid_uuid_metadata_yields_null():
    """x-user-id present but not a uuid → actor None (system actor)."""
    conn = _AuditConn()
    servicer = _make_servicer(conn)

    await servicer._update_kb(
        kb_pb.UpdateKBRequest(
            tenant_id=TENANT_ID, kb_id=KB_ID, idempotency_key=str(uuid.uuid4()),
            name="kb2",
        ),
        _AuditServicerContext(metadata=(("x-user-id", "not-a-uuid"),)),
    )
    assert conn.audit_calls[0][2] is None


async def test_actor_missing_metadata_yields_null():
    """No x-user-id at all (internal system consumer) → NULL actor."""
    conn = _AuditConn()
    servicer = _make_servicer(conn)

    await servicer._update_kb(
        kb_pb.UpdateKBRequest(
            tenant_id=TENANT_ID, kb_id=KB_ID, idempotency_key=str(uuid.uuid4()),
            name="kb2",
        ),
        _AuditServicerContext(),   # metadata=() default
    )
    assert conn.audit_calls[0][2] is None


# ════════════════════════════════════════════════════════════════════════════
# D. ListKBAuditLogs read RPC
# ════════════════════════════════════════════════════════════════════════════


def _audit_log_row(i=1, **over):
    row = {
        "id": uuid.uuid4(),
        "kb_id": uuid.UUID(KB_ID),
        "actor_user_id": uuid.UUID(USER_ID),
        "action": "kb.update",
        "before_state": {"name": "kb"},
        "after_state": {"name": "kb2"},
        "error_code": None,
        "error_msg": None,
        "created_at": datetime(2026, 9, 1, 0, 0, i, tzinfo=timezone.utc),
    }
    row.update(over)
    return row


class _ListAuditConn(_AuditConn):
    """_AuditConn with a preset fetch() result feeding list_logs."""

    def __init__(self, rows):
        super().__init__()
        self._audit_rows = rows

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        if "FROM kb_audit_log" in sql:
            return self._audit_rows
        return []


def _list_req(**over):
    defaults = dict(tenant_id=TENANT_ID, kb_id=KB_ID,
                    page=common_pb2.CursorPageRequest(limit=20))
    defaults.update(over)
    return kb_pb.ListKBAuditLogsRequest(**defaults)


async def test_list_kb_audit_logs_maps_items():
    rows = [_audit_log_row(1), _audit_log_row(2)]
    conn = _ListAuditConn(rows)
    servicer = _make_servicer(conn)

    resp = await servicer._list_kb_audit_logs(_list_req(), _ServicerContext())

    assert len(resp.items) == 2
    e = resp.items[0]
    assert e.kb_id == KB_ID
    assert e.actor_user_id == USER_ID
    assert e.action == "kb.update"
    assert e.before_state == '{"name": "kb"}'        # dict → JSON string
    assert e.after_state == '{"name": "kb2"}'
    assert e.error_code == ""                        # None → ""
    assert e.error_msg == ""
    assert e.created_at.ToJsonString().startswith("2026-09-01")


async def test_list_kb_audit_logs_next_cursor_boundary():
    # Exactly limit rows → next_cursor encodes the last row's keyset.
    rows = [_audit_log_row(i) for i in range(1, 3)]
    conn = _ListAuditConn(rows)
    servicer = _make_servicer(conn)

    resp = await servicer._list_kb_audit_logs(
        _list_req(page=common_pb2.CursorPageRequest(limit=2)), _ServicerContext()
    )
    assert len(resp.items) == 2
    expected = f"{rows[-1]['created_at'].isoformat().replace('+00:00', 'Z')}|{rows[-1]['id']}"
    assert resp.next_cursor == expected

    # Fewer rows than limit → no next page.
    conn2 = _ListAuditConn([_audit_log_row(1)])
    servicer2 = _make_servicer(conn2)
    resp2 = await servicer2._list_kb_audit_logs(
        _list_req(page=common_pb2.CursorPageRequest(limit=5)), _ServicerContext()
    )
    assert resp2.next_cursor == ""


async def test_list_kb_audit_logs_empty():
    conn = _ListAuditConn([])
    servicer = _make_servicer(conn)

    resp = await servicer._list_kb_audit_logs(_list_req(), _ServicerContext())
    assert list(resp.items) == []
    assert resp.next_cursor == ""


async def test_list_kb_audit_logs_state_serialization_variants():
    """_audit_state_json: None → '', str passthrough, dict → json."""
    rows = [_audit_log_row(1, before_state=None, after_state='{"x": 1}')]
    conn = _ListAuditConn(rows)
    servicer = _make_servicer(conn)

    resp = await servicer._list_kb_audit_logs(_list_req(), _ServicerContext())
    assert resp.items[0].before_state == ""
    assert resp.items[0].after_state == '{"x": 1}'


async def test_list_kb_audit_logs_kb_missing_not_found():
    conn = _ListAuditConn([])
    conn._kb_row = None
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._list_kb_audit_logs(_list_req(), ctx)
    assert ctx.aborted is not None
    assert conn.fetch_calls == []                   # no list query ran


async def test_list_kb_audit_logs_invalid_cursor_invalid_argument():
    conn = _ListAuditConn([])
    servicer = _make_servicer(conn)
    ctx = _ServicerContext()

    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await servicer._list_kb_audit_logs(
            _list_req(page=common_pb2.CursorPageRequest(cursor="not-a-cursor")),
            ctx,
        )
    assert ctx.aborted is not None


async def test_list_kb_audit_logs_limit_bounds():
    conn = _ListAuditConn([])
    servicer = _make_servicer(conn)

    # 0 is the proto default → `limit or 20` yields the default 20 (covered
    # by test_list_kb_audit_logs_default_limit_20); only out-of-range
    # values are rejected.
    for bad in (101, -1):
        ctx = _ServicerContext()
        with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
            await servicer._list_kb_audit_logs(
                _list_req(page=common_pb2.CursorPageRequest(limit=bad)), ctx
            )
        assert ctx.aborted is not None


async def test_list_kb_audit_logs_default_limit_20():
    """limit unset (proto default 0) → 20; the fetch binds limit=20."""
    conn = _ListAuditConn([])
    servicer = _make_servicer(conn)

    await servicer._list_kb_audit_logs(
        kb_pb.ListKBAuditLogsRequest(tenant_id=TENANT_ID, kb_id=KB_ID),
        _ServicerContext(),
    )
    sql, args = conn.fetch_calls[-1]
    assert args[-1] == 20


async def test_list_kb_audit_logs_missing_tenant_invalid_argument():
    servicer = KBServiceServicer()
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await servicer._list_kb_audit_logs(
            kb_pb.ListKBAuditLogsRequest(kb_id=KB_ID), ctx
        )
    assert ctx.aborted is not None


async def test_list_kb_audit_logs_no_pool_failed_precondition():
    servicer = KBServiceServicer()
    ctx = _ServicerContext()
    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._list_kb_audit_logs(
            kb_pb.ListKBAuditLogsRequest(tenant_id=TENANT_ID, kb_id=KB_ID),
            ctx,
        )
    assert ctx.aborted is not None


# ════════════════════════════════════════════════════════════════════════════
# E. Repository: insert_audit_in_tx SQL/args contract
# ════════════════════════════════════════════════════════════════════════════


async def test_insert_audit_in_tx_binds_all_columns():
    from app.repositories import audit as audit_repo

    conn = _AuditConn()
    log_id = await audit_repo.insert_audit_in_tx(
        conn,
        tenant_id=TENANT_ID,
        kb_id=KB_ID,
        action="kb.update",
        actor_user_id=USER_ID,
        before_state={"name": "kb"},
        after_state={"name": "kb2"},
        error_code="NOT_FOUND",
        error_msg="kb gone",
    )
    assert log_id                                   # uuid string
    sql, args = conn.fetchrow_calls[-1]
    assert "INSERT INTO kb_audit_log" in sql
    assert "jsonb" in sql
    assert args[0] == uuid.UUID(TENANT_ID)
    assert args[1] == uuid.UUID(KB_ID)
    assert args[2] == uuid.UUID(USER_ID)
    assert args[3] == "kb.update"
    import json
    assert json.loads(args[4]) == {"name": "kb"}
    assert json.loads(args[5]) == {"name": "kb2"}
    assert args[6] == "NOT_FOUND"
    assert args[7] == "kb gone"
    # #054: no RLS/GUC statement is issued anymore — tenant isolation is
    # the tenant_id bound as args[0] of the INSERT itself.
    assert not any(
        s.startswith("SET LOCAL") for s, _ in conn.execute_calls
    )


async def test_insert_audit_in_tx_null_actor_and_states():
    from app.repositories import audit as audit_repo

    conn = _AuditConn()
    await audit_repo.insert_audit_in_tx(
        conn, tenant_id=TENANT_ID, kb_id=KB_ID, action="kb.create"
    )
    sql, args = conn.fetchrow_calls[-1]
    assert args[2] is None                          # no actor → NULL
    assert args[4] is None and args[5] is None      # no states → NULL
    assert args[6] is None and args[7] is None
