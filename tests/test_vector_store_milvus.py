"""Tests for the D1 dedicated-Milvus client (issue-056, ADR §D1).

Verifies MilvusVectorStore against a fake AsyncMilvusClient (no real Milvus):

- Collection lifecycle: create (explicit schema + AUTOINDEX/COSINE index,
  dim bound into the FLOAT_VECTOR field), idempotent create when the
  collection already exists, idempotent drop (False when absent),
  recreate = drop-then-create ordering.
- Data plane: insert rows shape ({id, vector, content, metadata}, defaults
  filled), search call parameters (collection/limit/filter/output_fields)
  and Core-shape output mapping ({id, score, content, metadata} with score
  = COSINE distance passthrough), delete filter expression.
- Error wrapping: every pymilvus failure surfaces as VectorStoreError with
  the operation and collection name in the message.
- Lazy connection: construction performs no IO; the client is created on
  first operation from MILVUS_ADDR/MILVUS_TOKEN (addr without scheme gets
  http:// prepended); an injected ``client`` bypasses creation entirely.
- Event-loop isolation: the production client is pooled per running loop
  (kb's dual-loop process shares one store between the uvicorn loop and the
  dedicated gRPC loop; a grpc.aio channel binds to its first-using loop, so
  cross-loop reuse raised "attached to a different loop" — real-Milvus
  smoke regression, see development-records/KB-SPLIT-P1G.md).
"""
import asyncio
import os
import sys

import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.vector_store import MilvusVectorStore, VectorStoreError
from app.vector_store.milvus_client import normalize_milvus_uri


COLLECTION = "kb_22222222222222222222222222222222"


# ── fake AsyncMilvusClient ────────────────────────────────────────────────────


class _FakeSchema:
    def __init__(self):
        self.fields: list[tuple] = []

    def add_field(self, name, dtype, **kwargs):
        self.fields.append((name, dtype, kwargs))


class _FakeIndexParams:
    def __init__(self):
        self.indexes: list[dict] = []

    def add_index(self, **kwargs):
        self.indexes.append(kwargs)


class _FakeAsyncMilvusClient:
    """Recording fake of the AsyncMilvusClient surface MilvusVectorStore uses.

    ``fail_on`` maps method name → exception; the method raises it instead of
    executing (error-wrapping tests).
    """

    def __init__(self, *, exists=(), fail_on=None, search_results=None):
        self.calls: list[tuple[str, dict]] = []
        self._exists = set(exists)
        self._fail_on = dict(fail_on or {})
        self._search_results = search_results

    def _maybe_fail(self, name):
        exc = self._fail_on.get(name)
        if exc is not None:
            raise exc

    def create_schema(self, auto_id, enable_dynamic_field):
        self.calls.append((
            "create_schema",
            {"auto_id": auto_id, "enable_dynamic_field": enable_dynamic_field},
        ))
        return _FakeSchema()

    def prepare_index_params(self):
        return _FakeIndexParams()

    async def has_collection(self, name):
        self._maybe_fail("has_collection")
        self.calls.append(("has_collection", {"name": name}))
        return name in self._exists

    async def create_collection(self, name, schema=None, index_params=None):
        self._maybe_fail("create_collection")
        self.calls.append((
            "create_collection",
            {"name": name, "schema": schema, "index_params": index_params},
        ))
        self._exists.add(name)

    async def drop_collection(self, collection_name):
        self._maybe_fail("drop_collection")
        self.calls.append(("drop_collection", {"collection_name": collection_name}))
        self._exists.discard(collection_name)

    async def insert(self, collection_name, data):
        self._maybe_fail("insert")
        self.calls.append(("insert", {"collection_name": collection_name, "data": data}))

    async def search(self, **kwargs):
        self._maybe_fail("search")
        self.calls.append(("search", kwargs))
        return self._search_results

    async def delete(self, collection_name, filter):
        self._maybe_fail("delete")
        self.calls.append(("delete", {"collection_name": collection_name, "filter": filter}))

    async def close(self):
        self.calls.append(("close", {}))


def _make_store(client):
    return MilvusVectorStore(addr="localhost:19530", client=client)


def _calls(client, name):
    return [kw for n, kw in client.calls if n == name]


# ── normalize_milvus_uri ──────────────────────────────────────────────────────


def test_normalize_uri_adds_scheme():
    assert normalize_milvus_uri("localhost:19530") == "http://localhost:19530"
    assert normalize_milvus_uri(" 10.10.1.66:31950 ") == "http://10.10.1.66:31950"


def test_normalize_uri_keeps_scheme():
    assert normalize_milvus_uri("https://milvus:19530") == "https://milvus:19530"
    assert normalize_milvus_uri("http://localhost:19530") == "http://localhost:19530"


# ── collection lifecycle ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_collection_builds_explicit_schema_and_index():
    client = _FakeAsyncMilvusClient()
    store = _make_store(client)

    await store.create_collection(name=COLLECTION, dim=512, metric="COSINE")

    creates = _calls(client, "create_collection")
    assert len(creates) == 1
    schema = creates[0]["schema"]
    index_params = creates[0]["index_params"]
    # Explicit schema: pk + float vector (dim-bound) + content + metadata JSON.
    assert ("create_schema", {"auto_id": False, "enable_dynamic_field": False}) in client.calls
    field_names = [f[0] for f in schema.fields]
    assert field_names == ["id", "vector", "content", "metadata"]
    pk = schema.fields[0]
    assert pk[1].name == "VARCHAR" and pk[2].get("is_primary") is True
    vec = schema.fields[1]
    assert vec[1].name == "FLOAT_VECTOR" and vec[2]["dim"] == 512
    # AUTOINDEX + COSINE on the vector field.
    assert index_params.indexes == [
        {"field_name": "vector", "index_type": "AUTOINDEX", "metric_type": "COSINE"}
    ]


@pytest.mark.asyncio
async def test_create_collection_idempotent_when_already_exists():
    client = _FakeAsyncMilvusClient(exists=[COLLECTION])
    store = _make_store(client)

    await store.create_collection(name=COLLECTION, dim=512)  # must not raise

    assert _calls(client, "create_collection") == []
    assert len(_calls(client, "has_collection")) == 1


@pytest.mark.asyncio
async def test_create_collection_rejects_non_positive_dim():
    client = _FakeAsyncMilvusClient()
    store = _make_store(client)

    with pytest.raises(VectorStoreError, match="invalid embedding dimension"):
        await store.create_collection(name=COLLECTION, dim=0)
    assert client.calls == []  # no Milvus round trip


@pytest.mark.asyncio
async def test_drop_collection_returns_true_and_drops():
    client = _FakeAsyncMilvusClient(exists=[COLLECTION])
    store = _make_store(client)

    assert await store.drop_collection(name=COLLECTION) is True
    assert _calls(client, "drop_collection") == [{"collection_name": COLLECTION}]


@pytest.mark.asyncio
async def test_drop_collection_idempotent_when_absent():
    client = _FakeAsyncMilvusClient()
    store = _make_store(client)

    assert await store.drop_collection(name=COLLECTION) is False
    assert _calls(client, "drop_collection") == []


@pytest.mark.asyncio
async def test_recreate_collection_drops_then_creates():
    client = _FakeAsyncMilvusClient(exists=[COLLECTION])
    store = _make_store(client)

    await store.recreate_collection(name=COLLECTION, dim=768)

    names = [n for n, _ in client.calls]
    assert names.index("drop_collection") < names.index("create_collection")
    assert len(_calls(client, "create_collection")) == 1
    assert _calls(client, "create_collection")[0]["schema"].fields[1][2]["dim"] == 768


# ── data plane ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_insert_maps_documents_to_rows_with_defaults():
    client = _FakeAsyncMilvusClient()
    store = _make_store(client)
    documents = [
        {"id": "c1", "content": "text", "vector": [0.1, 0.2], "metadata": {"doc_id": "d1"}},
        {"id": "c2", "vector": [0.3, 0.4]},  # content/metadata defaults
    ]

    await store.insert(collection=COLLECTION, documents=documents)

    inserts = _calls(client, "insert")
    assert len(inserts) == 1
    assert inserts[0]["collection_name"] == COLLECTION
    assert inserts[0]["data"] == [
        {"id": "c1", "vector": [0.1, 0.2], "content": "text", "metadata": {"doc_id": "d1"}},
        {"id": "c2", "vector": [0.3, 0.4], "content": "", "metadata": {}},
    ]


@pytest.mark.asyncio
async def test_search_passthrough_params_and_core_shape_output():
    client = _FakeAsyncMilvusClient(
        search_results=[[
            {"id": "c1", "distance": 0.92, "entity": {"content": "text", "metadata": {"doc_id": "d1"}}},
            {"id": "c2", "distance": 0.10, "entity": {}},
        ]]
    )
    store = _make_store(client)

    items = await store.search(
        collection=COLLECTION, vector=[0.5, 0.6], top_k=5,
        filter_expr='metadata["doc_id"] == "d1"',
    )

    searches = _calls(client, "search")
    assert len(searches) == 1
    assert searches[0]["collection_name"] == COLLECTION
    assert searches[0]["data"] == [[0.5, 0.6]]
    assert searches[0]["limit"] == 5
    assert searches[0]["filter"] == 'metadata["doc_id"] == "d1"'
    assert searches[0]["output_fields"] == ["content", "metadata"]
    # Core searchVectorStore items shape; score = COSINE distance passthrough.
    assert items == [
        {"id": "c1", "score": 0.92, "content": "text", "metadata": {"doc_id": "d1"}},
        {"id": "c2", "score": 0.10, "content": "", "metadata": {}},
    ]


@pytest.mark.asyncio
async def test_search_empty_filter_and_none_results():
    client = _FakeAsyncMilvusClient(search_results=None)
    store = _make_store(client)

    items = await store.search(collection=COLLECTION, vector=[0.1], top_k=3)
    assert items == []
    assert _calls(client, "search")[0]["filter"] == ""


@pytest.mark.asyncio
async def test_delete_sends_filter_expression():
    client = _FakeAsyncMilvusClient()
    store = _make_store(client)
    expr = 'metadata["doc_id"] == "d1"'

    await store.delete(collection=COLLECTION, filter_expr=expr)

    assert _calls(client, "delete") == [
        {"collection_name": COLLECTION, "filter": expr}
    ]


# ── error wrapping ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "op,kwargs,fail_method,exists",
    [
        ("create_collection", {"name": COLLECTION, "dim": 4}, "has_collection", ()),
        ("create_collection", {"name": COLLECTION, "dim": 4}, "create_collection", ()),
        ("drop_collection", {"name": COLLECTION}, "drop_collection", (COLLECTION,)),
        ("insert", {"collection": COLLECTION, "documents": [{"id": "x", "vector": [0.1]}]}, "insert", ()),
        ("search", {"collection": COLLECTION, "vector": [0.1], "top_k": 1}, "search", ()),
        ("delete", {"collection": COLLECTION, "filter_expr": "x == '1'"}, "delete", ()),
    ],
)
async def test_pymilvus_failures_wrap_as_vector_store_error(op, kwargs, fail_method, exists):
    client = _FakeAsyncMilvusClient(exists=exists, fail_on={fail_method: RuntimeError("boom")})
    store = _make_store(client)

    with pytest.raises(VectorStoreError) as exc:
        await getattr(store, op)(**kwargs)
    # Message carries the operation and the collection name; the cause is chained.
    assert COLLECTION in str(exc.value)
    assert "boom" in str(exc.value)
    assert exc.value.__cause__ is not None


# ── lazy connection / injected client ─────────────────────────────────────────


def test_injected_client_bypasses_creation(monkeypatch):
    def _boom(*args, **kwargs):
        raise AssertionError("AsyncMilvusClient must not be constructed")

    import pymilvus
    monkeypatch.setattr(pymilvus, "AsyncMilvusClient", _boom)
    store = MilvusVectorStore(addr="localhost:19530", client=_FakeAsyncMilvusClient())
    assert store._get_client() is store._client


def test_lazy_client_created_on_first_operation_with_normalized_uri(monkeypatch):
    created: dict = {}

    class _SpyClient(_FakeAsyncMilvusClient):
        def __init__(self, **kwargs):
            super().__init__()
            created.update(kwargs)

    import pymilvus
    monkeypatch.setattr(pymilvus, "AsyncMilvusClient", _SpyClient)

    store = MilvusVectorStore(addr="10.0.0.1:19530", token="tok", timeout=5.0)
    assert created == {}  # construction performs no IO

    asyncio.run(store.delete(collection=COLLECTION, filter_expr="x == '1'"))
    assert created == {
        "uri": "http://10.0.0.1:19530",
        "token": "tok",
        "timeout": 5.0,
    }


def test_default_store_passes_settings_timeout(monkeypatch):
    """get_default_vector_store wires settings.milvus_timeout into the client.

    直连后 kb 与 Milvus 之间无中间层超时兜底：单例必须把 MILVUS_TIMEOUT
    传给懒建的 AsyncMilvusClient（否则 pymilvus 默认无限等待）。
    """
    import pymilvus

    import app.vector_store.milvus_client as m

    created: dict = {}

    class _SpyClient(_FakeAsyncMilvusClient):
        def __init__(self, **kwargs):
            super().__init__()
            created.update(kwargs)

    monkeypatch.setattr(pymilvus, "AsyncMilvusClient", _SpyClient)
    monkeypatch.setattr(m, "_default_vector_store", None)
    monkeypatch.setattr(m.settings, "milvus_timeout", 7.5)

    store = m.get_default_vector_store()
    asyncio.run(store.delete(collection=COLLECTION, filter_expr="x == '1'"))
    assert created["timeout"] == 7.5


@pytest.mark.asyncio
async def test_aclose_closes_client_once_and_noop_without_client():
    client = _FakeAsyncMilvusClient()
    store = _make_store(client)
    await store.aclose()
    await store.aclose()  # second close is a no-op (client already released)
    assert _calls(client, "close") == [{}]

    empty = MilvusVectorStore(addr="localhost:19530")
    await empty.aclose()  # never connected → no-op, no error


# ── event-loop isolation (per-loop client pool) ───────────────────────────────


def _spy_client_class(record: list):
    class _SpyClient(_FakeAsyncMilvusClient):
        def __init__(self, **kwargs):
            super().__init__()
            record.append(self)

    return _SpyClient


def _patch_pymilvus(monkeypatch, cls):
    import pymilvus

    monkeypatch.setattr(pymilvus, "AsyncMilvusClient", cls)


@pytest.mark.asyncio
async def test_same_loop_reuses_one_client_across_operations(monkeypatch):
    """同一 running loop 的多次操作复用同一个 client（只建一次）。"""
    created: list = []
    _patch_pymilvus(monkeypatch, _spy_client_class(created))

    store = MilvusVectorStore(addr="localhost:19530")
    await store.delete(collection=COLLECTION, filter_expr="x == '1'")
    await store.insert(collection=COLLECTION, documents=[{"id": "x", "vector": [0.1]}])
    await store.search(collection=COLLECTION, vector=[0.1], top_k=1)

    assert len(created) == 1
    assert store._get_client() is created[0]


def test_cross_loop_creates_one_client_per_loop(monkeypatch):
    """回归：跨 loop 复用单 client 会抛 "attached to a different loop"。

    每个事件循环必须得到自己的 client：loop A 首操作建一个，同 loop 复用；
    loop B 首操作另建一个。
    """
    created: list = []
    _patch_pymilvus(monkeypatch, _spy_client_class(created))

    store = MilvusVectorStore(addr="localhost:19530")

    async def _op():
        await store.delete(collection=COLLECTION, filter_expr="x == '1'")

    loop_a = asyncio.new_event_loop()
    try:
        loop_a.run_until_complete(_op())
        loop_a.run_until_complete(_op())  # same loop → reuse
    finally:
        loop_a.close()
    assert len(created) == 1

    loop_b = asyncio.new_event_loop()
    try:
        loop_b.run_until_complete(_op())  # different loop → own client
    finally:
        loop_b.close()
    assert len(created) == 2


def test_aclose_closes_only_current_loop_client(monkeypatch):
    """aclose 只关调用方 loop 的 client；其他 loop 的不受影响，且幂等。"""
    created: list = []
    _patch_pymilvus(monkeypatch, _spy_client_class(created))

    store = MilvusVectorStore(addr="localhost:19530")

    async def _op():
        await store.delete(collection=COLLECTION, filter_expr="x == '1'")

    async def _close():
        await store.aclose()
        await store.aclose()  # second close is a no-op for this loop

    loop_a = asyncio.new_event_loop()
    try:
        loop_a.run_until_complete(_op())
        loop_a.run_until_complete(_close())
    finally:
        loop_a.close()
    assert _calls(created[0], "close") == [{}]

    loop_b = asyncio.new_event_loop()
    try:
        loop_b.run_until_complete(_op())  # fresh client for loop B, untouched
        loop_b.run_until_complete(_op())  # and still usable after loop-A close
    finally:
        loop_b.close()
    assert len(created) == 2
    assert _calls(created[1], "close") == []
