"""Write-read roundtrip tests for the D1 self-managed Milvus path
(issue-057 / ADR §一-1.2 + §一-1.4).

Pins the #057 acceptance criteria not yet covered by tests:
- Roundtrip: ParseOrchestrator.process_document inserts vectors directly
  into the (fake) self-managed Milvus; RetrieveService.retrieve reads
  them back via direct search — the SAME shared store instance on both
  sides, with no Core vector-stores anywhere in between.
- Reparse cleanup: re-running process_document for the same doc_id issues
  a Milvus delete (filter on metadata["doc_id"]) BEFORE the new insert,
  so no stale vectors remain after reparse (old kb_chunks rows and old
  vectors are deleted up front; write_chunks has no ON CONFLICT).

Fakes: one shared in-memory vector store implementing insert/delete/search
with real storage + real cosine ranking (search output shape aligned with
milvus_client.py: {id, score, content, metadata}; zero-similarity docs are
omitted — a test simplification, Milvus returns all top_k); fake RagEngine
(parse/embed/generate); fake kb-owned object store (presigned GET for the
document original + put_object for image products — the #058 D1 path never
touches Core objects); doc/chunk repo calls are patched, same as
test_parse_orchestrator.py.
"""
import math
import os
import sys
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.services.parse_orchestrator import ParseOrchestrator
from app.services.retrieve_service import RetrieveService


TENANT_ID = "11111111-1111-1111-1111-111111111111"
KB_ID = "22222222-2222-2222-2222-222222222222"
DOC_ID = "33333333-3333-3333-3333-333333333333"
DOC_ID_2 = "33333333-3333-3333-3333-333333333334"
# #058 D1: kb-owned object key of the original document ({kb_id}/{doc_id}).
STORAGE_PATH = f"{KB_ID}/{DOC_ID}"
BUCKET_NAME = "ani-kb-docs"
PRESIGNED_GET_URL = f"http://minio.internal/{BUCKET_NAME}/{STORAGE_PATH}?token=t"
COLLECTION = "kb_22222222222222222222222222222222"
PARENT_ID = "55555555-5555-5555-5555-555555555551"
CHILD_A = "66666666-6666-6666-6666-666666666661"
CHILD_B = "66666666-6666-6666-6666-666666666662"
CHILD_NEW_A = "66666666-6666-6666-6666-666666666671"
CHILD_NEW_B = "66666666-6666-6666-6666-666666666672"
DOC2_PARENT_ID = "77777777-7777-7777-7777-777777777771"
DOC2_CHILD = "88888888-8888-8888-8888-888888888881"
SUMMARY_TEXT = "这是一个用于往返测试的文档摘要。"

# Orthogonal unit vectors: cosine(query, target) is exactly 1.0 / 0.0 /
# 0.7071... — deterministic ranking without float noise.
V_CHILD_A = [1.0, 0.0, 0.0, 0.0]
V_CHILD_B = [0.0, 1.0, 0.0, 0.0]
V_SUMMARY = [0.0, 0.0, 1.0, 0.0]
V_NEW_A = [0.0, 0.0, 0.0, 1.0]
V_NEW_B = [1.0, 1.0, 0.0, 0.0]  # cosine with V_CHILD_A = 1/sqrt(2)

_VEC_BY_ID = {
    CHILD_A: V_CHILD_A,
    CHILD_B: V_CHILD_B,
    CHILD_NEW_A: V_NEW_A,
    CHILD_NEW_B: V_NEW_B,
}


# ── Shared fakes ─────────────────────────────────────────────────────────────


class _RoundtripVectorStore:
    """In-memory fake MilvusVectorStore with real storage + cosine ranking.

    Used as the SAME instance by both ParseOrchestrator (insert/delete)
    and RetrieveService (search) — that is what "write-read roundtrip"
    means. ``ops`` records the global call order so tests can assert
    delete-before-insert on reparse.
    """

    def __init__(self):
        self._store: dict[str, dict[str, dict]] = {}  # collection -> {id: doc}
        self.inserts: list[dict] = []
        self.deletes: list[dict] = []
        self.search_calls: list[dict] = []
        self.ops: list[str] = []  # ordered "insert" / "delete" markers

    async def insert(self, *, collection, documents):
        self.inserts.append({"collection": collection, "documents": documents})
        self.ops.append("insert")
        bucket = self._store.setdefault(collection, {})
        for doc in documents:
            bucket[str(doc["id"])] = {
                "id": doc["id"],
                "content": doc["content"],
                "vector": list(doc["vector"]),
                "metadata": dict(doc.get("metadata") or {}),
            }

    async def delete(self, *, collection, filter_expr):
        """Apply the filter semantics parse_orchestrator uses:
        ``metadata["doc_id"] == "{doc_id}"`` removes that doc's vectors."""
        self.deletes.append({"collection": collection, "filter_expr": filter_expr})
        self.ops.append("delete")
        bucket = self._store.get(collection, {})
        marker = 'metadata["doc_id"] == "'
        if marker not in filter_expr:
            return
        doc_id = filter_expr.split(marker, 1)[1].split('"', 1)[0]
        for key, doc in list(bucket.items()):
            if doc["metadata"].get("doc_id") == doc_id:
                bucket.pop(key, None)

    async def search(self, *, collection, vector, top_k, filter_expr=None):
        self.search_calls.append({
            "collection": collection,
            "vector": list(vector),
            "top_k": top_k,
            "filter_expr": filter_expr,
        })
        scored = []
        for doc in self._store.get(collection, {}).values():
            score = _cosine(doc["vector"], vector)
            if score > 0:  # test simplification: skip zero/negative sims
                scored.append((score, doc))
        scored.sort(key=lambda t: t[0], reverse=True)  # stable on ties
        return [
            {
                "id": doc["id"],
                "score": score,
                "content": doc["content"],
                "metadata": dict(doc["metadata"]),
            }
            for score, doc in scored[:top_k]
        ]

    async def create_collection(self, **kwargs):
        pass

    async def drop_collection(self, **kwargs):
        return True

    async def recreate_collection(self, **kwargs):
        pass

    def stored_ids(self, collection) -> set[str]:
        return set(self._store.get(collection, {}).keys())

    def stored_docs(self, collection) -> list[dict]:
        return list(self._store.get(collection, {}).values())


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


class _ParseRagEngine:
    """Fake RagEngine for the parse side: parse returns canned chunks,
    embed returns the pre-built vector for each input text position,
    generate returns a long summary answer."""

    def __init__(self, *, chunks, vectors, dimension=4):
        self._chunks = chunks
        self._vectors = vectors
        self._dimension = dimension
        self.embed_texts: list[list[str]] = []

    async def parse(self, *, download_url, file_name, file_type, chunk_size=1024):
        return [dict(c) for c in self._chunks]

    async def embed(self, *, texts, model="", runtime_endpoint=""):
        self.embed_texts.append(list(texts))
        out = [list(self._vectors[i % len(self._vectors)]) for i in range(len(texts))]
        return out, self._dimension

    async def generate(self, *, question, session_id="", context=None,
                       history=None, inference_service_name="", max_tokens=2048,
                       runtime_endpoint=""):
        return {
            "answer": SUMMARY_TEXT * 10,
            "input_tokens": 10,
            "output_tokens": 20,
            "session_id": session_id,
        }


class _QueryRagEngine:
    """Fake RagEngine for the retrieve side: embed returns one fixed
    query vector."""

    def __init__(self, vector, dimension=4):
        self._vector = vector
        self._dimension = dimension

    async def embed(self, *, texts, model="", runtime_endpoint=""):
        return [list(self._vector)], self._dimension


class _FakeObjectStore:
    """Fake kb-owned MinioObjectStore — only the methods the D1 parse path
    uses (presigned GET for the original + put_object for image products).
    Core objects/vector stores are never touched."""

    def __init__(self, *, bucket=BUCKET_NAME, get_url=PRESIGNED_GET_URL):
        self._bucket = bucket
        self._get_url = get_url
        self.get_calls: list[str] = []
        self.put_calls: list[dict] = []

    @property
    def bucket(self) -> str:
        return self._bucket

    async def presigned_get_object(self, *, key, expires_seconds=None):
        self.get_calls.append(key)
        return self._get_url

    async def put_object(self, *, key, content, content_type=None):
        self.put_calls.append({
            "key": key,
            "content_type": content_type,
            "content": content,
            "size": len(content),
        })


class _FakeConn:
    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self
        return _tx()


class _FakePool:
    def __init__(self):
        self._conn = _FakeConn()

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


class _FakeParentLookup:
    """Fake _ParentLookup — only the doc_summary backfill (lookup_parents)
    is reachable in these tests because child parent_content is always
    denormalized at write time."""

    def __init__(self, parents_by_doc=None):
        self._parents_by_doc = parents_by_doc or {}

    async def lookup_parent(self, *, tenant_id, parent_chunk_id):
        return None

    async def lookup_parents(self, *, tenant_id, doc_id):
        return [dict(p) for p in self._parents_by_doc.get(doc_id, [])]


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


def _parse_engine(chunks):
    """Build the parse-side RagEngine fake with vectors aligned to the
    embed order: [children..., summary]."""
    children = [c for c in chunks if c.get("chunk_type") == "child"]
    vectors = [_VEC_BY_ID.get(c["chunk_id"], [0.5, 0.5, 0.5, 0.5]) for c in children]
    vectors.append(V_SUMMARY)  # summary is embedded last
    return _ParseRagEngine(chunks=chunks, vectors=vectors)


def _patch_repos(*, existing_doc=None):
    """Patch the four repo calls parse_orchestrator makes (same as
    test_parse_orchestrator._patch_repos)."""
    mock_status = AsyncMock(return_value=True)
    mock_write = AsyncMock(return_value=3)
    mock_get_doc = AsyncMock(return_value=existing_doc)
    mock_delete = AsyncMock(return_value=0)
    return (
        patch("app.services.parse_orchestrator.doc_repo.update_parse_status", mock_status),
        patch("app.services.parse_orchestrator.chunk_repo.write_chunks", mock_write),
        patch("app.services.parse_orchestrator.doc_repo.get_document", mock_get_doc),
        patch("app.services.parse_orchestrator.chunk_repo.delete_chunks_by_doc", mock_delete),
    )


async def _run_parse(store, rag_engine, *, doc_id=DOC_ID, existing_doc=None,
                     storage_path=None):
    """Run process_document once against the shared store."""
    svc = ParseOrchestrator(
        db_pool=_FakePool(),
        object_store=_FakeObjectStore(),
        rag_engine_client=rag_engine,
        vector_store=store,
    )
    p_status, p_write, p_get, p_delete = _patch_repos(existing_doc=existing_doc)
    with p_status, p_write, p_get, p_delete:
        await svc.process_document(
            tenant_id=TENANT_ID,
            kb_id=KB_ID,
            doc_id=doc_id,
            storage_path=storage_path or f"{KB_ID}/{doc_id}",
            file_name="a.pdf",
            file_type="pdf",
            chunk_size=1024,
            vector_store_id=COLLECTION,
        )


def _make_retrieve_service(store, *, query_vector, parents_by_doc=None):
    """Build a RetrieveService reading from the SAME shared store."""
    svc = RetrieveService(
        db_pool=_FakePool(),
        vector_store=store,
        rag_engine_client=_QueryRagEngine(vector=query_vector),
        parent_lookup=_FakeParentLookup(parents_by_doc=parents_by_doc),
    )
    return svc


async def _retrieve(store, *, query_vector, retrieval_mode="vector",
                    top_k=5, parents_by_doc=None):
    svc = _make_retrieve_service(store, query_vector=query_vector,
                                 parents_by_doc=parents_by_doc)
    return await svc.retrieve(
        tenant_id=TENANT_ID,
        kb_id=KB_ID,
        question="roundtrip question",
        top_k=top_k,
        retrieval_mode=retrieval_mode,
        vector_store_id=COLLECTION,
    )


# ── Tests ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_roundtrip_write_then_vector_search():
    """Write via process_document, read back via retrieve("vector") from
    the same shared store: insert metadata fidelity, exact-cosine hit,
    parent-content replacement, page str→int, and summary roundtrip."""
    store = _RoundtripVectorStore()
    await _run_parse(store, _parse_engine([_parent(), _child(CHILD_A), _child(CHILD_B)]))

    # ── Write side: 2 children + 1 doc_summary, one insert batch < 100 ──
    assert store.ops == ["delete", "insert"]  # unconditional cleanup first
    assert len(store.inserts) == 1
    assert store.inserts[0]["collection"] == COLLECTION
    docs = store.inserts[0]["documents"]
    types = sorted(d["metadata"]["chunk_type"] for d in docs)
    assert types == ["child", "child", "doc_summary"]
    by_id = {d["id"]: d for d in docs}
    assert {d["id"] for d in docs if d["metadata"]["chunk_type"] == "child"} == {
        CHILD_A, CHILD_B,
    }
    # Summary chunk_id is a random uuid4 per run — pinned by chunk_type above.
    summary_doc = next(d for d in docs if d["metadata"]["chunk_type"] == "doc_summary")
    assert summary_doc["id"] not in {CHILD_A, CHILD_B}
    # Metadata written exactly as retrieve will need it (parse §7):
    child_a = by_id[CHILD_A]
    assert child_a["content"] == "child chunk text"
    assert child_a["vector"] == V_CHILD_A
    meta = child_a["metadata"]
    assert meta["doc_id"] == DOC_ID
    assert meta["file_name"] == "a.pdf"
    assert meta["chunk_id"] == CHILD_A
    assert meta["page_number"] == "1"  # stored as str
    assert meta["parent_content"] == "parent block text"
    assert meta["parent_chunk_id"] == PARENT_ID

    # ── Read side: query with CHILD_A's exact vector → cosine 1.0 ──
    sources, max_score = await _retrieve(store, query_vector=V_CHILD_A)
    assert store.search_calls[0]["collection"] == COLLECTION
    assert store.search_calls[0]["top_k"] == 10  # retrieve asks top_k * 2
    assert store.search_calls[0]["vector"] == V_CHILD_A
    assert max_score == pytest.approx(1.0)
    assert [s["chunk_id"] for s in sources] == [CHILD_A]
    hit = sources[0]
    assert hit["doc_id"] == DOC_ID
    assert hit["file_name"] == "a.pdf"
    assert hit["chunk_type"] == "child"
    assert hit["page"] == 1  # page_number roundtrip: written str, returned int
    # Parent replacement (retrieve _return_parent_and_dedup): child content
    # is swapped for the parent block; chunk_id stays the child's.
    assert hit["content"] == "parent block text"
    assert hit["parent_chunk_id"] == PARENT_ID


@pytest.mark.asyncio
async def test_roundtrip_summary_query_backfills_parent():
    """Query with the summary's vector: the doc_summary chunk written by
    parse is retrievable; empty parent_content triggers the PG backfill
    path (lookup_parents), matching legacy retrieve semantics."""
    store = _RoundtripVectorStore()
    await _run_parse(store, _parse_engine([_parent(), _child(CHILD_A), _child(CHILD_B)]))

    sources, max_score = await _retrieve(
        store, query_vector=V_SUMMARY,
        parents_by_doc={DOC_ID: [{"content": "parent block text"}]},
    )
    assert max_score == pytest.approx(1.0)
    assert len(sources) == 1
    summary = sources[0]
    assert summary["chunk_type"] == "doc_summary"
    assert summary["doc_id"] == DOC_ID
    # doc_summary content is NOT parent-replaced; parent_content is
    # backfilled from the parent blocks.
    assert summary["content"]
    assert summary["parent_content"] == "parent block text"


@pytest.mark.asyncio
async def test_roundtrip_hybrid_reads_same_store():
    """Hybrid mode reads the same shared store: vector leg hits the
    written chunk (keyword leg patched empty), parent replacement applies
    after RRF fusion, and max_score = max(vector cosine)."""
    from app.services import retrieve_service as retrieve_module

    store = _RoundtripVectorStore()
    await _run_parse(store, _parse_engine([_parent(), _child(CHILD_A), _child(CHILD_B)]))

    async def _fake_keyword_search(conn, *, tenant_id, kb_id, query, limit=10):
        return []

    with patch.object(retrieve_module.chunk_repo, "keyword_search", _fake_keyword_search):
        sources, max_score = await _retrieve(store, query_vector=V_CHILD_A,
                                             retrieval_mode="hybrid")
    assert max_score == pytest.approx(1.0)
    assert {s["chunk_id"] for s in sources} == {CHILD_A}
    child_a = sources[0]
    assert child_a["content"] == "parent block text"
    assert child_a["chunk_type"] == "child"


@pytest.mark.asyncio
async def test_reparse_deletes_old_vectors_before_rewrite():
    """AC: reparse issues the Milvus doc-scoped delete BEFORE the new
    insert, and no stale vectors remain — querying with the OLD chunk's
    exact vector surfaces only new-run chunks."""
    store = _RoundtripVectorStore()

    # Run 1 (fresh doc): children A/B + summary. The unconditional
    # cleanup delete matches nothing on the empty store.
    await _run_parse(store, _parse_engine([_parent(), _child(CHILD_A), _child(CHILD_B)]))
    old_ids = store.stored_ids(COLLECTION)
    assert len(old_ids) == 3  # 2 children + 1 summary
    assert len(store.deletes) == 1

    # Run 2 (reparse, doc previously failed → idempotency lets it run):
    # new chunk ids, same doc_id.
    await _run_parse(
        store,
        _parse_engine([_parent(), _child(CHILD_NEW_A), _child(CHILD_NEW_B)]),
        existing_doc={"parse_status": "failed", "id": DOC_ID},
    )

    # Every run is delete-then-insert; run 2's delete removes run 1's rows.
    assert store.ops == ["delete", "insert", "delete", "insert"]
    for d in store.deletes:
        assert d["collection"] == COLLECTION
        assert d["filter_expr"] == f'metadata["doc_id"] == "{DOC_ID}"'

    # Final state: only the new run's chunks (old children AND the old
    # run's random summary uuid are gone).
    final = store.stored_ids(COLLECTION)
    assert CHILD_A not in final
    assert CHILD_B not in final
    assert old_ids.isdisjoint(final)
    assert CHILD_NEW_A in final
    assert CHILD_NEW_B in final
    assert sorted(
        d["metadata"]["chunk_type"] for d in store.stored_docs(COLLECTION)
    ) == ["child", "child", "doc_summary"]

    # Search with the OLD child's exact vector: if any old vector had
    # survived the delete, CHILD_A would resurface with cosine 1.0.
    # Instead only the new run's positively-correlated chunk appears.
    sources, _ = await _retrieve(store, query_vector=V_CHILD_A, top_k=10)
    assert {s["chunk_id"] for s in sources} == {CHILD_NEW_B}  # cos = 1/√2


@pytest.mark.asyncio
async def test_reparse_delete_is_doc_scoped():
    """Doc-scoped delete: reparse of doc A never removes doc B's vectors
    from the same kb collection."""
    store = _RoundtripVectorStore()

    chunks_doc1 = [_parent(), _child(CHILD_A), _child(CHILD_B)]
    await _run_parse(store, _parse_engine(chunks_doc1), doc_id=DOC_ID)

    chunks_doc2 = [
        _parent(chunk_id=DOC2_PARENT_ID, content="doc2 parent"),
        _child(DOC2_CHILD, content="doc2 child", parent_content="doc2 parent",
               parent_chunk_id=DOC2_PARENT_ID),
    ]
    await _run_parse(store, _parse_engine(chunks_doc2), doc_id=DOC_ID_2)

    # Reparse doc1 with a fresh child set.
    await _run_parse(
        store,
        _parse_engine([_parent(content="doc1 new parent"),
                       _child(CHILD_NEW_A, content="doc1 new child",
                              parent_content="doc1 new parent")]),
        doc_id=DOC_ID,
        existing_doc={"parse_status": "failed", "id": DOC_ID},
    )

    assert store.deletes[-1]["filter_expr"] == f'metadata["doc_id"] == "{DOC_ID}"'
    final = store.stored_ids(COLLECTION)
    # Doc1's old children are gone; its new child is present.
    assert CHILD_A not in final
    assert CHILD_B not in final
    assert CHILD_NEW_A in final
    # Doc2's child AND summary survived doc1's reparse delete.
    assert DOC2_CHILD in final
    doc2_docs = [d for d in store.stored_docs(COLLECTION)
                 if d["metadata"]["doc_id"] == DOC_ID_2]
    assert {d["metadata"]["chunk_type"] for d in doc2_docs} == {"child", "doc_summary"}


@pytest.mark.asyncio
async def test_delete_then_search_returns_nothing():
    """Stale-leak regression pin: after the doc-scoped delete, searching
    with that doc's exact chunk vector returns no sources and max_score 0."""
    store = _RoundtripVectorStore()
    await _run_parse(store, _parse_engine([_parent(), _child(CHILD_A)]))
    assert store.stored_ids(COLLECTION)

    await store.delete(
        collection=COLLECTION,
        filter_expr=f'metadata["doc_id"] == "{DOC_ID}"',
    )

    sources, max_score = await _retrieve(store, query_vector=V_CHILD_A)
    assert sources == []
    assert max_score == 0.0
