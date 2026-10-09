"""Tests for the kb-service ParseOrchestrator (issue-032 / Plan step 5).

Covers all Acceptance Criteria:
- Full pipeline: pending → parsing → rag-engine.Parse → image upload →
  indexing → summary → Embed → Milvus vector insert → kb_chunks write → ready
- Internal-view (ClusterIP) presigned GET URL signed by kb for the kb-owned
  bucket and passed to rag-engine.Parse (kb-service does not download document
  bytes; ADR §D1 line 138)
- Image direct-upload into the kb-owned bucket, markdown link
  [图片: 图片]({bucket}/{key}) embedded in parent content (ADR §D1 line 137)
- Embed RPC embeds child chunks + summary separately (summary NOT in
  child_chunks to avoid double-write)
- Direct-Milvus insert with pre-computed vectors; metadata includes chunk_id,
  chunk_type, parent_content, parent_chunk_id, page_number, content_type,
  doc_id, file_name
- write_chunks receives parents + children + summaries separately
- State machine: pending → parsing → indexing → ready | failed
- _generate_summary: best-effort, first 3 parent blocks, Generate RPC with
  the same English prompt template as legacy SummaryService (instructs LLM
  to match content language); failure → None, no block
- _sanitize_error: redacts paths/tokens, truncates 500 chars
- Equivalence test: same input → same kb_chunks row count and content

Uses fakes for MinioObjectStore, RagEngine client, asyncpg pool — no real
services. chunk_repo.write_chunks and doc_repo.update_parse_status are mocked.
"""
import json
import os
import sys
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.services import parse_orchestrator as po_module
from app.services.contracts import ParseOrchestratorProtocol
from app.services.parse_orchestrator import (
    ParseOrchestrator,
    SUMMARY_PARENT_COUNT,
    _sanitize_error,
)


TENANT_ID = "11111111-1111-1111-1111-111111111111"
KB_ID = "22222222-2222-2222-2222-222222222222"
DOC_ID = "33333333-3333-3333-3333-333333333333"
# #058 D1: kb-owned object key of the original document ({kb_id}/{doc_id}).
STORAGE_PATH = f"{KB_ID}/{DOC_ID}"
VECTOR_STORE_ID = "vs_kb_2222222222222222222222222"
PARENT_ID = "55555555-5555-5555-5555-555555555551"
CHILD_A = "66666666-6666-6666-6666-666666666661"
CHILD_B = "66666666-6666-6666-6666-666666666662"
IMAGE_ID = "77777777-7777-7777-7777-777777777771"
# kb-owned bucket name (object path prefix in the markdown image link).
BUCKET_NAME = "ani-kb-docs"
# Internal-view (ClusterIP) presigned GET URL signed by kb for rag-engine.
PRESIGNED_GET_URL = (
    "http://minio.kb.svc:9000/ani-kb-docs/"
    "22222222-2222-2222-2222-222222222222/"
    "33333333-3333-3333-3333-333333333333?X-Amz-Signature=secret"
)


# ── Fakes ────────────────────────────────────────────────────────────────────


class _FakeRagEngine:
    """Fake RagEngineGRPCClient: parse / embed / generate."""

    def __init__(
        self,
        *,
        chunks=None,
        vectors=None,
        dimension=4,
        summary_answer="这是一个约两百字以上的摘要。" * 10,
    ):
        self._chunks = chunks or []
        self._vectors = vectors or [[0.1, 0.2, 0.3, 0.4]]
        self._dimension = dimension
        self._summary_answer = summary_answer
        self.parse_calls: list[dict] = []
        self.embed_calls: list[dict] = []
        self.generate_calls: list[dict] = []

    async def parse(
        self,
        *,
        download_url: str,
        file_name: str,
        file_type: str,
        chunk_size: int = 1024,
    ) -> list[dict]:
        self.parse_calls.append({
            "download_url": download_url,
            "file_name": file_name,
            "file_type": file_type,
            "chunk_size": chunk_size,
        })
        return [dict(c) for c in self._chunks]

    async def embed(
        self, *, texts: list[str], model: str = "", runtime_endpoint: str = ""
    ) -> tuple[list[list[float]], int]:
        self.embed_calls.append({
            "texts": list(texts),
            "model": model,
            "runtime_endpoint": runtime_endpoint,
        })
        # Return one vector per text (cycle if fewer pre-built vectors).
        out = []
        for i in range(len(texts)):
            out.append(list(self._vectors[i % len(self._vectors)]))
        return out, self._dimension

    async def generate(
        self,
        *,
        question: str,
        session_id: str = "",
        context=None,
        history=None,
        inference_service_name: str = "",
        max_tokens: int = 2048,
        runtime_endpoint: str = "",
    ) -> dict:
        self.generate_calls.append({
            "question": question,
            "session_id": session_id,
            "context": context or [],
            "history": history or [],
            "inference_service_name": inference_service_name,
            "max_tokens": max_tokens,
            "runtime_endpoint": runtime_endpoint,
        })
        return {
            "answer": self._summary_answer,
            "input_tokens": 10,
            "output_tokens": 20,
            "session_id": session_id,
        }


class _FakeObjectStore:
    """Fake kb-owned MinioObjectStore (#058 D1 对象自管).

    Mirrors the subset of MinioObjectStore used by ParseOrchestrator:
    ``presigned_get_object`` (internal-view URL for the original document),
    ``put_object`` (image artifact direct upload) and the ``bucket`` property
    (markdown link prefix ``{bucket}/{key}``).
    """

    def __init__(
        self,
        *,
        get_url=PRESIGNED_GET_URL,
        bucket=BUCKET_NAME,
        raise_on_get=None,
        raise_on_put=None,
    ):
        self._get_url = get_url
        self._bucket = bucket
        self._raise_on_get = raise_on_get
        self._raise_on_put = raise_on_put
        self.get_calls: list[str] = []
        self.put_calls: list[dict] = []

    @property
    def bucket(self) -> str:
        return self._bucket

    async def presigned_get_object(self, *, key, expires_seconds=None):
        self.get_calls.append(key)
        if self._raise_on_get is not None:
            raise self._raise_on_get
        return self._get_url

    async def put_object(self, *, key, content, content_type=None):
        if self._raise_on_put is not None:
            raise self._raise_on_put
        self.put_calls.append({
            "key": key,
            "content_type": content_type,
            "content": content,
            "size": len(content),
        })


class _FakeVectorStore:
    """MilvusVectorStore fake for D1 direct-Milvus paths (issue-056)."""

    def __init__(self):
        self.inserts: list[dict] = []
        self.deletes: list[dict] = []

    async def insert(self, *, collection, documents):
        self.inserts.append({"collection": collection, "documents": documents})

    async def delete(self, *, collection, filter_expr):
        self.deletes.append({"collection": collection, "filter_expr": filter_expr})

    async def search(self, **kwargs):
        return []

    async def create_collection(self, **kwargs):
        pass

    async def drop_collection(self, **kwargs):
        return True

    async def recreate_collection(self, **kwargs):
        pass


class _FakeConn:
    """Minimal fake asyncpg Connection for doc_repo + chunk_repo calls."""

    def __init__(self):
        self.executed: list[tuple] = []

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self
        return _tx()


class _FakePool:
    """Fake asyncpg Pool yielding a shared _FakeConn."""

    def __init__(self, conn=None):
        self._conn = conn or _FakeConn()

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


# ── Builders ────────────────────────────────────────────────────────────────


def _parent(chunk_id=PARENT_ID, content="parent block text", page=1):
    return {
        "chunk_id": chunk_id,
        "content": content,
        "content_type": "text",
        "page_number": page,
        "parent_content": "",
        "parent_chunk_id": "",
        "chunk_type": "parent",
        "metadata_json": "{}",
        "image_bytes": b"",
        "image_format": "",
    }


def _child(chunk_id=CHILD_A, content="child chunk text", page=1,
           parent_content="parent block text", parent_chunk_id=PARENT_ID):
    return {
        "chunk_id": chunk_id,
        "content": content,
        "content_type": "text",
        "page_number": page,
        "parent_content": parent_content,
        "parent_chunk_id": parent_chunk_id,
        "chunk_type": "child",
        "metadata_json": "{}",
        "image_bytes": b"",
        "image_format": "",
    }


def _image_chunk(chunk_id=IMAGE_ID, content="[图片](placeholder)",
                 image_bytes=b"\x89PNG fake", image_format="png"):
    return {
        "chunk_id": chunk_id,
        "content": content,
        "content_type": "image",
        "page_number": 0,
        "parent_content": "",
        "parent_chunk_id": "",
        "chunk_type": "image",
        "metadata_json": "{}",
        "image_bytes": image_bytes,
        "image_format": image_format,
    }


def _make_service(*, rag_engine=None, object_store=None, pool=None):
    """Build a ParseOrchestrator with fakes and mocked repos."""
    rag = rag_engine or _FakeRagEngine()
    store = object_store or _FakeObjectStore()
    p = pool or _FakePool()
    svc = ParseOrchestrator(
        db_pool=p,
        object_store=store,
        rag_engine_client=rag,
        vector_store=_FakeVectorStore(),
    )
    return svc, rag, store, p


def _patch_repos(*, existing_doc=None):
    """Patch doc_repo.update_parse_status, doc_repo.get_document,
    chunk_repo.write_chunks, and chunk_repo.delete_chunks_by_doc.

    Returns (patch_status, patch_write, patch_get_doc, patch_delete,
    mock_status, mock_write, mock_get_doc, mock_delete) for use in
    ``with`` blocks.
    """
    mock_status = AsyncMock(return_value=True)
    mock_write = AsyncMock(return_value=3)
    mock_get_doc = AsyncMock(return_value=existing_doc)
    mock_delete = AsyncMock(return_value=0)
    return (
        patch("app.services.parse_orchestrator.doc_repo.update_parse_status", mock_status),
        patch("app.services.parse_orchestrator.chunk_repo.write_chunks", mock_write),
        patch("app.services.parse_orchestrator.doc_repo.get_document", mock_get_doc),
        patch("app.services.parse_orchestrator.chunk_repo.delete_chunks_by_doc", mock_delete),
        mock_status,
        mock_write,
        mock_get_doc,
        mock_delete,
    )


# ── Tests ───────────────────────────────────────────────────────────────────


# AC: ParseOrchestrator satisfies ParseOrchestratorProtocol
def test_parse_orchestrator_satisfies_protocol():
    svc, _, _, _ = _make_service()
    assert isinstance(svc, ParseOrchestratorProtocol)


# AC: pending → parsing → indexing → ready
@pytest.mark.asyncio
async def test_full_pipeline_success_state_transitions():
    chunks = [_parent(), _child()]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID,
            kb_id=KB_ID,
            doc_id=DOC_ID,
            storage_path=STORAGE_PATH,
            file_name="a.pdf",
            file_type="pdf",
            chunk_size=1024,
            vector_store_id=VECTOR_STORE_ID,
        )

    # State transitions: parsing, indexing, ready (in order)
    status_calls = [c.kwargs["parse_status"] for c in mock_status.call_args_list]
    assert status_calls == ["parsing", "indexing", "ready"]
    # ready call includes chunk_count = write_chunks return value (total rows:
    # parents + children + summaries, matching legacy parse_worker line 499)
    ready_call = mock_status.call_args_list[-1]
    assert ready_call.kwargs["chunk_count"] == 3  # mock_write returns 3


# AC: internal-view presigned GET URL signed by kb, passed to rag-engine.Parse
@pytest.mark.asyncio
async def test_presigned_get_url_passed_to_parse_rpc():
    chunks = [_parent(), _child()]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, _, _, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # kb signs the internal-view GET URL for the kb-owned object key
    assert store.get_calls == [STORAGE_PATH]
    assert rag.parse_calls[0]["download_url"] == PRESIGNED_GET_URL
    assert rag.parse_calls[0]["file_name"] == "a.pdf"
    assert rag.parse_calls[0]["file_type"] == "pdf"
    assert rag.parse_calls[0]["chunk_size"] == 1024


# AC: Image direct-upload to kb bucket, markdown link embedded in parent content
@pytest.mark.asyncio
async def test_image_upload_embeds_link_in_parent():
    img = _image_chunk()
    chunks = [_parent(), _child(), img]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # Image direct-uploaded into the kb-owned bucket
    assert len(store.put_calls) == 1
    up = store.put_calls[0]
    # Key follows legacy ImageUploader format: {kb_id}/{doc_id}/images/{uuid}.{ext}
    assert up["key"].startswith(f"{KB_ID}/{DOC_ID}/images/")
    assert up["key"].endswith(".png")
    assert up["content_type"] == "image/png"
    assert up["size"] > 0

    # The markdown link [图片: 图片](oss_url) should be appended to the
    # last parent's content (mirrors legacy parse_service._build_image_placeholder).
    # The URL is the kb-owned object path "{bucket}/{key}" (ADR §D1 line 137).
    write_call = mock_write.call_args
    parents_arg = write_call.kwargs["parents"]
    children_arg = write_call.kwargs["children"]
    # Image chunk should NOT be in parents or children
    parent_ids = [p["chunk_id"] for p in parents_arg]
    child_ids = [c["chunk_id"] for c in children_arg]
    assert IMAGE_ID not in parent_ids
    assert IMAGE_ID not in child_ids
    # The last parent's content should contain the image markdown link
    last_parent = parents_arg[-1]
    assert "[图片: 图片](" in last_parent["content"]
    # URL should be kb-owned object path "ani-kb-docs/{kb_id}/{doc_id}/images/{uuid}.png"
    assert f"{BUCKET_NAME}/{KB_ID}/{DOC_ID}/images/" in last_parent["content"]
    assert ".png" in last_parent["content"]


# AC: Embed RPC embeds child chunks + summary separately
#     (summary NOT in child_chunks to avoid double-write)
@pytest.mark.asyncio
async def test_embed_separates_summary_from_children():
    chunks = [_parent(), _child(CHILD_A), _child(CHILD_B)]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # embed called once with child texts + summary text
    assert len(rag.embed_calls) == 1
    embedded_texts = rag.embed_calls[0]["texts"]
    # 2 children + 1 summary = 3 texts
    assert len(embedded_texts) == 3
    # First two are child contents
    assert embedded_texts[0] == "child chunk text"
    assert embedded_texts[1] == "child chunk text"
    # Third is the summary answer
    assert embedded_texts[2] == rag._summary_answer

    # write_chunks called with parents + children (no summary in children)
    write_call = mock_write.call_args
    children_arg = write_call.kwargs["children"]
    summaries_arg = write_call.kwargs["summaries"]
    assert len(children_arg) == 2  # only 2 children, no summary
    assert summaries_arg is not None
    assert len(summaries_arg) == 1  # summary passed separately


# AC: Milvus insert with pre-computed vectors; metadata includes all fields
@pytest.mark.asyncio
async def test_milvus_insert_vector_metadata():
    chunks = [_parent(), _child(CHILD_A)]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, _, _, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    assert len(svc._vector_store.inserts) == 1
    ins = svc._vector_store.inserts[0]
    assert ins["collection"] == VECTOR_STORE_ID

    # 1 child + 1 summary = 2 documents inserted
    assert len(ins["documents"]) == 2

    # Check the child document metadata has all required fields
    child_doc = ins["documents"][0]
    assert child_doc["id"] == CHILD_A
    assert child_doc["content"] == "child chunk text"
    assert "vector" in child_doc
    assert len(child_doc["vector"]) == 4  # dimension from fake
    md = child_doc["metadata"]
    assert md["chunk_id"] == CHILD_A
    assert md["chunk_type"] == "child"
    assert md["parent_content"] == "parent block text"
    assert md["parent_chunk_id"] == PARENT_ID
    assert md["page_number"] == "1"
    assert md["content_type"] == "text"
    assert md["doc_id"] == DOC_ID
    assert md["file_name"] == "a.pdf"

    # Summary document metadata — chunk_type must be "doc_summary" (not "child")
    summary_doc = ins["documents"][1]
    assert summary_doc["metadata"]["chunk_type"] == "doc_summary"
    assert summary_doc["metadata"]["doc_id"] == DOC_ID


# AC: write_chunks receives parents + children + summaries separately
@pytest.mark.asyncio
async def test_write_chunks_separate_lists():
    chunks = [_parent(PARENT_ID, "parent text"), _child(CHILD_A), _child(CHILD_B)]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    write_call = mock_write.call_args
    assert write_call.kwargs["tenant_id"] == TENANT_ID
    assert write_call.kwargs["kb_id"] == KB_ID
    assert write_call.kwargs["doc_id"] == DOC_ID
    assert write_call.kwargs["file_name"] == "a.pdf"
    assert len(write_call.kwargs["parents"]) == 1
    assert len(write_call.kwargs["children"]) == 2
    assert write_call.kwargs["summaries"] is not None
    assert len(write_call.kwargs["summaries"]) == 1


# AC: State machine → failed on exception (with sanitized error_msg)
@pytest.mark.asyncio
async def test_failure_marks_doc_failed_with_sanitized_error():
    rag = _FakeRagEngine(chunks=[])

    # Override parse to raise
    async def failing_parse(**kwargs):
        raise RuntimeError(
            "download failed from /tmp/secret/path token=abc123 "
            + "x" * 600
        )

    rag.parse = failing_parse
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # First call: parsing; second call: failed
    status_calls = [c.kwargs["parse_status"] for c in mock_status.call_args_list]
    assert status_calls == ["parsing", "failed"]
    failed_call = mock_status.call_args_list[-1]
    error_msg = failed_call.kwargs["error_message"]
    # Sanitized: paths redacted, truncated to 500 chars
    assert "[redacted]" in error_msg, f"expected [redacted] in error_msg, got: {error_msg}"
    assert "/tmp/secret" not in error_msg, f"original path leaked: {error_msg}"
    assert "abc123" not in error_msg, f"token value leaked: {error_msg}"
    assert len(error_msg) <= 500


# AC: _generate_summary — best-effort, first 3 parents, prompt format
@pytest.mark.asyncio
async def test_summary_uses_first_3_parents_and_prompt():
    parents = [
        _parent("p1", "内容一"),
        _parent("p2", "内容二"),
        _parent("p3", "内容三"),
        _parent("p4", "内容四"),  # should be excluded (only first 3)
    ]
    rag = _FakeRagEngine()
    svc = ParseOrchestrator(
        db_pool=_FakePool(),
        object_store=_FakeObjectStore(),
        rag_engine_client=rag,
    )

    result = await svc._generate_summary(parents)

    assert result is not None
    assert result["content"] == rag._summary_answer
    assert result["content_type"] == "text"
    assert result["page_number"] == 1
    assert result["parent_chunk_id"] is None
    assert result["parent_content"] is None
    assert result["token_count"] >= 1

    # Generate called with the correct prompt (first 3 parents joined).
    # The prompt uses the same English template as legacy SummaryService
    # (summary_service.py lines 53-56) that instructs the LLM to match the
    # content's language.
    assert len(rag.generate_calls) == 1
    gen_call = rag.generate_calls[0]
    prompt = gen_call["question"]
    assert "Summarize the following content in 200-460 characters" in prompt
    assert "Use the same language as the content" in prompt
    assert "内容一" in prompt
    assert "内容二" in prompt
    assert "内容三" in prompt
    assert "内容四" not in prompt  # 4th parent excluded
    assert gen_call["context"] == []
    assert gen_call["history"] == []
    assert gen_call["max_tokens"] == po_module.SUMMARY_SAFE_CHARS
    assert len(result["content"]) <= po_module.SUMMARY_SAFE_CHARS  # Bug B clamp


# AC: _generate_summary — failure returns None, does not block
@pytest.mark.asyncio
async def test_summary_failure_returns_none():
    rag = _FakeRagEngine()
    # Override generate to raise
    async def failing_generate(**kwargs):
        raise RuntimeError("vLLM unavailable")

    rag.generate = failing_generate
    svc = ParseOrchestrator(
        db_pool=_FakePool(),
        object_store=_FakeObjectStore(),
        rag_engine_client=rag,
    )

    result = await svc._generate_summary([_parent()])
    assert result is None


# AC: _generate_summary — no parents → None
@pytest.mark.asyncio
async def test_summary_no_parents_returns_none():
    rag = _FakeRagEngine()
    svc = ParseOrchestrator(
        db_pool=_FakePool(),
        object_store=_FakeObjectStore(),
        rag_engine_client=rag,
    )
    result = await svc._generate_summary([])
    assert result is None
    # Generate should NOT have been called
    assert len(rag.generate_calls) == 0


# AC: _generate_summary — empty parent content → None
@pytest.mark.asyncio
async def test_summary_empty_parent_content_returns_none():
    rag = _FakeRagEngine()
    svc = ParseOrchestrator(
        db_pool=_FakePool(),
        object_store=_FakeObjectStore(),
        rag_engine_client=rag,
    )
    result = await svc._generate_summary([_parent(PARENT_ID, "")])
    assert result is None


# AC: _generate_summary — empty answer → None
@pytest.mark.asyncio
async def test_summary_empty_answer_returns_none():
    rag = _FakeRagEngine(summary_answer="   ")
    svc = ParseOrchestrator(
        db_pool=_FakePool(),
        object_store=_FakeObjectStore(),
        rag_engine_client=rag,
    )
    result = await svc._generate_summary([_parent()])
    assert result is None


# AC: _sanitize_error — redacts paths/tokens, truncates 500 chars
def test_sanitize_error_redacts_paths_and_tokens():
    msg = "failed at /tmp/secret/file.txt with token=abc123 bearer XYZ"
    result = _sanitize_error(msg)
    assert "/tmp/secret/file.txt" not in result
    assert "abc123" not in result
    assert "[redacted]" in result


def test_sanitize_error_redacts_windows_paths():
    msg = "failed at C:\\Users\\PC\\secrets\\key.pem with token=xyz"
    result = _sanitize_error(msg)
    assert "C:\\Users\\PC\\secrets\\key.pem" not in result
    assert "[redacted]" in result


def test_sanitize_error_redacts_presigned_url_params():
    msg = "download failed: X-Amz-Signature=abc123&X-Amz-Credential=AKIAxyz&expires=12345"
    result = _sanitize_error(msg)
    assert "abc123" not in result
    assert "AKIAxyz" not in result
    assert "[redacted]" in result


def test_sanitize_error_truncates_to_500():
    msg = "x" * 600
    result = _sanitize_error(msg)
    assert len(result) == 500


def test_sanitize_error_preserves_short_messages():
    msg = "parse error: invalid PDF"
    result = _sanitize_error(msg)
    assert result == "parse error: invalid PDF"


# AC: Equivalence — same input → same kb_chunks row count and content
# This test verifies that given the same Parse RPC output, the orchestrator
# produces the same write_chunks call (same parents, children, summaries).
@pytest.mark.asyncio
async def test_equivalence_same_input_same_output():
    chunks = [
        _parent(PARENT_ID, "parent content A"),
        _child(CHILD_A, "child content A", parent_content="parent content A"),
        _child(CHILD_B, "child content B", parent_content="parent content A"),
    ]
    rag = _FakeRagEngine(chunks=chunks)

    # Run twice with the same input
    results: list[dict] = []
    for _ in range(2):
        svc, _, _, _ = _make_service(rag_engine=_FakeRagEngine(chunks=chunks))
        p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()
        with p_status, p_write, p_get, p_delete:
            await svc.process_document(
                tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
                storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
                chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
            )
        wc = mock_write.call_args
        results.append({
            "parents": [(p["chunk_id"], p["content"]) for p in wc.kwargs["parents"]],
            "children": [(c["chunk_id"], c["content"]) for c in wc.kwargs["children"]],
            "summaries_len": len(wc.kwargs["summaries"]) if wc.kwargs["summaries"] else 0,
            "summaries_content": [s["content"] for s in (wc.kwargs["summaries"] or [])],
        })

    # Both runs produce identical kb_chunks content
    assert results[0] == results[1]
    # Row count: 1 parent + 2 children + 1 summary = 4
    total_rows_0 = len(results[0]["parents"]) + len(results[0]["children"]) + results[0]["summaries_len"]
    assert total_rows_0 == 4


# AC: No children → no embed, no vector insert, but summary still attempted
@pytest.mark.asyncio
async def test_no_children_still_generates_summary():
    chunks = [_parent()]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # embed called with just the summary text (0 children + 1 summary)
    assert len(rag.embed_calls) == 1
    assert len(rag.embed_calls[0]["texts"]) == 1  # only summary

    # Milvus insert called with 1 document (the summary)
    assert len(svc._vector_store.inserts) == 1
    assert len(svc._vector_store.inserts[0]["documents"]) == 1

    # write_chunks: 1 parent, 0 children, 1 summary
    wc = mock_write.call_args
    assert len(wc.kwargs["parents"]) == 1
    assert len(wc.kwargs["children"]) == 0
    assert len(wc.kwargs["summaries"]) == 1


# AC: Summary disabled when generate fails → embed only children, no summary
@pytest.mark.asyncio
async def test_summary_failure_no_summary_in_write_or_embed():
    chunks = [_parent(), _child(CHILD_A)]
    rag = _FakeRagEngine(chunks=chunks)

    async def failing_generate(**kwargs):
        raise RuntimeError("vLLM down")

    rag.generate = failing_generate
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # embed called with only child text (no summary)
    assert len(rag.embed_calls) == 1
    assert len(rag.embed_calls[0]["texts"]) == 1  # only 1 child

    # Milvus insert: 1 document (child only)
    assert len(svc._vector_store.inserts) == 1
    assert len(svc._vector_store.inserts[0]["documents"]) == 1

    # write_chunks: no summaries
    wc = mock_write.call_args
    assert wc.kwargs["summaries"] is None or len(wc.kwargs["summaries"]) == 0


# AC: presign GET signing failure → failed status
@pytest.mark.asyncio
async def test_presign_get_failure_marks_failed():
    chunks = [_parent(), _child()]
    rag = _FakeRagEngine(chunks=chunks)
    store = _FakeObjectStore(raise_on_get=RuntimeError("presign GET failed"))
    svc, rag, store, pool = _make_service(rag_engine=rag, object_store=store)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    status_calls = [c.kwargs["parse_status"] for c in mock_status.call_args_list]
    assert "failed" in status_calls


# AC: image put_object failure → failed status
@pytest.mark.asyncio
async def test_put_object_failure_marks_failed():
    chunks = [_parent(), _child(), _image_chunk()]
    rag = _FakeRagEngine(chunks=chunks)
    store = _FakeObjectStore(raise_on_put=RuntimeError("put failed"))
    svc, rag, store, pool = _make_service(rag_engine=rag, object_store=store)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    status_calls = [c.kwargs["parse_status"] for c in mock_status.call_args_list]
    assert "failed" in status_calls


# AC: Image-only document (e.g. scanned PDF): no parents/children from Parse —
#     a parent block + one child per image link must be synthesized so the
#     document is viewable (not "ready with 0 chunks")
@pytest.mark.asyncio
async def test_image_only_synthesizes_parent_and_image_children():
    chunks = [_image_chunk(), _image_chunk("i2")]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="scan.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # Both images direct-uploaded into the kb-owned bucket (#058 D1 对象自管)
    assert len(store.put_calls) == 2

    # One parent block carrying all image links + one child per link
    wc = mock_write.call_args
    parents = wc.kwargs["parents"]
    children = wc.kwargs["children"]
    assert len(parents) == 1
    assert len(children) == 2

    parent = parents[0]
    assert parent["chunk_type"] == "parent"
    assert parent["content_type"] == "image"
    assert parent["parent_chunk_id"] is None
    assert parent["content"].count("[图片: 图片](") == 2
    assert f"{BUCKET_NAME}/" in parent["content"]

    for child in children:
        assert child["chunk_type"] == "child"
        assert child["content_type"] == "image"
        assert child["parent_chunk_id"] == parent["chunk_id"]
        assert child["parent_content"] == parent["content"]
        # Each child carries exactly one image link
        assert child["content"].count("[图片: 图片](") == 1
        assert child["content"] in parent["content"]

    # Children (not the parent) are embedded and vector-inserted; no summary
    assert len(rag.embed_calls) == 1
    assert rag.embed_calls[0]["texts"] == [c["content"] for c in children]
    # D1 向量自管（issue-056）：直连 Milvus insert（不再经 Core insertVector）
    assert len(svc._vector_store.inserts) == 1
    inserted = svc._vector_store.inserts[0]["documents"]
    assert len(inserted) == 2
    for doc, child in zip(inserted, children):
        assert doc["metadata"]["content_type"] == "image"
        assert doc["metadata"]["chunk_type"] == "child"
        assert doc["metadata"]["parent_chunk_id"] == parent["chunk_id"]

    # Summary skipped: parent content is just image links, nothing to summarize
    assert len(rag.generate_calls) == 0
    assert wc.kwargs["summaries"] in (None, [])

    # Document becomes ready
    status_calls = [c.kwargs["parse_status"] for c in mock_status.call_args_list]
    assert status_calls == ["parsing", "indexing", "ready"]


# AC: Image with no image_bytes is skipped (no upload)
@pytest.mark.asyncio
async def test_no_image_bytes_skips_upload():
    # image chunk with empty bytes
    img = _image_chunk(image_bytes=b"")
    chunks = [_parent(), _child(), img]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, _, _, _, _ = _patch_repos()

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # No upload calls (image_bytes was empty)
    assert len(store.put_calls) == 0


# AC: SUMMARY_PARENT_COUNT is 3 (matches rag-engine SummaryService)
def test_summary_parent_count_is_3():
    assert SUMMARY_PARENT_COUNT == 3


# ── Additional robustness tests (review fixes) ──────────────────────────────


# tenant_id guard: empty tenant_id → ValueError before any DB call
@pytest.mark.asyncio
async def test_empty_tenant_id_raises():
    svc, _, _, _ = _make_service()
    with pytest.raises(ValueError, match="tenant_id must not be empty"):
        await svc.process_document(
            tenant_id="", kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )


# Idempotency: already-ready document → skip (no parse, no write)
@pytest.mark.asyncio
async def test_idempotency_skips_already_ready():
    chunks = [_parent(), _child()]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    # existing doc with parse_status="ready"
    existing_doc = {"parse_status": "ready", "id": DOC_ID}
    p_status, p_write, p_get, p_delete, mock_status, mock_write, mock_get, _ = _patch_repos(
        existing_doc=existing_doc,
    )

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # get_document was called to check status
    assert mock_get.call_count == 1
    # No parse, no embed, no write, no vector insert
    assert len(rag.parse_calls) == 0
    assert len(rag.embed_calls) == 0
    assert len(svc._vector_store.inserts) == 0
    assert mock_write.call_count == 0
    # No status update (no parsing/indexing/ready transitions)
    assert mock_status.call_count == 0


# Idempotency: non-ready document → proceeds with pipeline
@pytest.mark.asyncio
async def test_idempotency_proceeds_when_not_ready():
    chunks = [_parent(), _child()]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    # existing doc with parse_status="pending"
    existing_doc = {"parse_status": "pending", "id": DOC_ID}
    p_status, p_write, p_get, p_delete, mock_status, mock_write, mock_get, _ = _patch_repos(
        existing_doc=existing_doc,
    )

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # Pipeline proceeded: parse called, status transitions happened
    assert len(rag.parse_calls) == 1
    status_calls = [c.kwargs["parse_status"] for c in mock_status.call_args_list]
    assert status_calls == ["parsing", "indexing", "ready"]


# Reparse idempotency: prior kb_chunks must be deleted before re-writing
@pytest.mark.asyncio
async def test_reparse_deletes_prior_chunks():
    """Reparse of a failed doc must delete prior chunks before writing new ones."""
    chunks = [_parent(), _child()]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    existing_doc = {"parse_status": "failed", "id": DOC_ID}
    p_status, p_write, p_get, p_delete, mock_status, mock_write, mock_get, mock_delete = _patch_repos(
        existing_doc=existing_doc,
    )

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # delete_chunks_by_doc was called before write_chunks
    assert mock_delete.called, "expected delete_chunks_by_doc to be called"
    delete_call = mock_delete.call_args
    assert delete_call.kwargs["doc_id"] == DOC_ID
    assert delete_call.kwargs["tenant_id"] == TENANT_ID
    # Milvus vector delete also called (best-effort cleanup on reparse)
    assert len(svc._vector_store.deletes) == 1
    assert svc._vector_store.deletes[0]["collection"] == VECTOR_STORE_ID
    assert DOC_ID in svc._vector_store.deletes[0]["filter_expr"]
    # write_chunks still called (new chunks written after delete)
    assert mock_write.called
    # Pipeline proceeded to ready
    status_calls = [c.kwargs["parse_status"] for c in mock_status.call_args_list]
    assert status_calls == ["parsing", "indexing", "ready"]


# Summary chunk_type must be "doc_summary" in the chunk dict returned by
# _generate_summary (so vector metadata picks it up correctly)
@pytest.mark.asyncio
async def test_summary_chunk_has_doc_summary_type():
    svc = ParseOrchestrator(
        db_pool=_FakePool(),
        object_store=_FakeObjectStore(),
        rag_engine_client=_FakeRagEngine(),
    )
    result = await svc._generate_summary([_parent()])
    assert result is not None
    assert result["chunk_type"] == "doc_summary"


# update_parse_status returns False → doc not found/deleted → skip pipeline
# (mirrors rag-engine parse_worker #r5)
@pytest.mark.asyncio
async def test_update_returns_false_skips_pipeline():
    chunks = [_parent(), _child()]
    rag = _FakeRagEngine(chunks=chunks)
    svc, rag, store, pool = _make_service(rag_engine=rag)
    p_status, p_write, p_get, p_delete, mock_status, mock_write, mock_get, _ = _patch_repos()
    # Make update_parse_status return False (no row matched)
    mock_status.return_value = False

    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID, kb_id=KB_ID, doc_id=DOC_ID,
            storage_path=STORAGE_PATH, file_name="a.pdf", file_type="pdf",
            chunk_size=1024, vector_store_id=VECTOR_STORE_ID,
        )

    # Pipeline aborted: no parse, no embed, no write, no vector insert
    assert len(rag.parse_calls) == 0
    assert len(rag.embed_calls) == 0
    assert len(svc._vector_store.inserts) == 0
    assert mock_write.call_count == 0
    # Only one status update attempted (parsing), no indexing/ready
    assert mock_status.call_count == 1
    assert mock_status.call_args.kwargs["parse_status"] == "parsing"
