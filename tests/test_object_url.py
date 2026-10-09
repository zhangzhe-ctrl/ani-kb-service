"""Tests for GetObjectURL in the servicer (issue-059, ADR §D1-取用).

图片产物取用闭环：Gateway 把 ``GET /knowledge-bases/{kb_id}/objects?object_id=``
转发到本 RPC，kb 签发**对外视角**（NodePort）预签名 GET URL 后回传；
调用方（前端浏览器）凭 URL 直连 MinIO 取字节。object_id 语义 = ``{bucket}/{key}``，与
markdown 占位 ``[图片: caption](object_id)`` 中的字面一致。

覆盖点：
1. object_id 解析正确：``{bucket}/{key}`` 只按第一个 "/" 切分，key 保留
   原始斜杠结构（``{kb_id}/{doc_id}/images/{uuid}.{ext}``）。
2. 成功：签发的 key 等于解析出的 key，回传 presign GET URL + TTL。
3. 归属校验：bucket ≠ kb 自有 bucket → FAILED_PRECONDITION；
   key 前缀 ≠ ``{kb_id}/`` → FAILED_PRECONDITION（跨 KB 越权取用）。
4. 格式校验：object_id 无 "/" 或 bucket/key 为空 → INVALID_ARGUMENT；
   非 UUID tenant_id/kb_id → INVALID_ARGUMENT；缺字段 → INVALID_ARGUMENT。
5. KB 存在性门禁：未知 KB → NOT_FOUND，且不签发 URL。
6. 基础设施故障：presign 抛 ObjectStoreError → UNAVAILABLE。

No real DB/MinIO — recording fake conn + fake object store.
"""
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
from app.generated.kb.v1 import kb_service_pb2 as kb_pb
from app.object_store import ObjectStoreError

TENANT_ID = "11111111-1111-1111-1111-111111111111"
KB_ID = "22222222-2222-2222-2222-222222222222"
DOC_ID = "33333333-3333-3333-3333-333333333333"
BUCKET = "ani-kb-docs"
IMAGE_KEY = f"{KB_ID}/{DOC_ID}/images/44444444-4444-4444-4444-444444444444.png"


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

    def __init__(self, *, bucket=BUCKET, presign_url="http://minio/get", ttl=900,
                 presign_exc=None):
        self._bucket = bucket
        self._presign_url = presign_url
        self._ttl = ttl
        self._presign_exc = presign_exc
        self.calls: list[tuple] = []

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def presign_ttl_seconds(self) -> int:
        return self._ttl

    async def presigned_get_object(self, *, key, expires_seconds=None,
                                   view="internal"):
        self.calls.append(("presign_get", key, view))
        if self._presign_exc is not None:
            raise self._presign_exc
        return f"{self._presign_url}?{key}"


class _MockConn:
    """Recording fake conn — only the get_kb row read matters here."""

    def __init__(self, *, kb_row="default"):
        self._kb_row = (
            {"id": uuid.UUID(KB_ID), "tenant_id": uuid.UUID(TENANT_ID),
             "name": "kb", "status": "active"}
            if kb_row == "default" else kb_row
        )

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self

        return _tx()

    async def fetchrow(self, sql, *args):
        if "FROM knowledge_bases" in sql:
            return self._kb_row
        return None

    async def fetch(self, sql, *args):
        return []

    async def fetchval(self, sql, *args):
        return 0

    async def execute(self, sql, *args):
        return "UPDATE 0"


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


def _make_servicer(conn, object_store):
    return KBServiceServicer(pool=_Pool(conn), object_store=object_store)


def _req(**over):
    req = {"tenant_id": TENANT_ID, "kb_id": KB_ID,
           "object_id": f"{BUCKET}/{IMAGE_KEY}"}
    req.update(over)
    return kb_pb.GetObjectURLRequest(**req)


# ── success: object_id parse + public-view presign ────────────────────────────


async def test_get_object_url_parses_object_id_and_signs_key():
    """AC: 图片 object_id 取用解析正确。object_id ``{bucket}/{key}`` 只按
    第一个 "/" 切分，key 保留原始斜杠结构；签发对外视角 presign GET
    （view="public"，前端浏览器集群外可达），回传 URL + TTL。"""
    conn = _MockConn()
    store = _FakeObjectStore(ttl=600)
    servicer = _make_servicer(conn, store)

    result = await servicer._get_object_url(_req(), _FakeContext())

    # The key handed to MinIO is exactly the object_id's key part (image key),
    # signed against the public view (frontend fetch, browser outside cluster).
    assert store.calls == [("presign_get", IMAGE_KEY, "public")]
    assert result.url == f"http://minio/get?{IMAGE_KEY}"
    assert result.expires_seconds == 600


# ── ownership: bucket + kb prefix ─────────────────────────────────────────────


async def test_get_object_url_rejects_foreign_bucket():
    """bucket ≠ kb 自有 bucket → FAILED_PRECONDITION，且不签发 URL。"""
    conn = _MockConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._get_object_url(
            _req(object_id=f"other-bucket/{KB_ID}/{DOC_ID}/a.pdf"), ctx
        )
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    assert store.calls == []


async def test_get_object_url_rejects_cross_kb_key_prefix():
    """key 前缀 ≠ {kb_id}/ → FAILED_PRECONDITION（跨 KB 越权取用）。"""
    other_kb = "55555555-5555-5555-5555-555555555555"
    conn = _MockConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        await servicer._get_object_url(
            _req(object_id=f"{BUCKET}/{other_kb}/{DOC_ID}/images/x.png"), ctx
        )
    assert ctx.aborted[0] == grpc.StatusCode.FAILED_PRECONDITION
    assert store.calls == []


# ── format validation ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("object_id", [BUCKET, f"{BUCKET}/", "/{key}".format(key=IMAGE_KEY)])
async def test_get_object_url_rejects_malformed_object_id(object_id):
    """无 "/" 或 bucket/key 为空 → INVALID_ARGUMENT。"""
    conn = _MockConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await servicer._get_object_url(_req(object_id=object_id), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.INVALID_ARGUMENT
    assert store.calls == []


async def test_get_object_url_rejects_missing_required_fields():
    """缺 tenant_id/kb_id/object_id → INVALID_ARGUMENT。"""
    conn = _MockConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await servicer._get_object_url(_req(object_id=""), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.INVALID_ARGUMENT


async def test_get_object_url_rejects_non_uuid_kb_id():
    """kb_id 非 UUID → INVALID_ARGUMENT。"""
    conn = _MockConn()
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await servicer._get_object_url(_req(kb_id="not-a-uuid"), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.INVALID_ARGUMENT
    assert store.calls == []


# ── KB existence gate + infra failure ─────────────────────────────────────────


async def test_get_object_url_kb_missing_maps_not_found():
    """未知 KB → NOT_FOUND（不为死 KB 下的对象签发 URL）。"""
    conn = _MockConn(kb_row=None)
    store = _FakeObjectStore()
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._get_object_url(_req(), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.NOT_FOUND
    assert store.calls == []


async def test_get_object_url_presign_failure_maps_unavailable():
    """presign 基础设施故障 → UNAVAILABLE。"""
    conn = _MockConn()
    store = _FakeObjectStore(presign_exc=ObjectStoreError("minio down"))
    servicer = _make_servicer(conn, store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="UNAVAILABLE"):
        await servicer._get_object_url(_req(), ctx)
    assert ctx.aborted[0] == grpc.StatusCode.UNAVAILABLE
