"""Tests for the D1 kb-owned-MinIO client (issue-058, ADR §D1).

Verifies MinioObjectStore against a fake minio SDK client (no real MinIO):

- presigned_put_object: public/NodePort view client is used (NOT the
  internal one — dual-view contract, ADR §一-1.6), TTL from settings /
  expires_seconds override, ttl<=0 rejected locally.
- presigned_get_object: INTERNAL (ClusterIP) view client is used — the
  rag-engine download URL contract (ADR §D1 line 138), NOT the public one.
- put_object: internal view client, streamed via asyncio.to_thread (real
  network IO), content_type defaults to octet-stream.
- stat_object: internal view client, {size, etag, content_type,
  last_modified} shape; NoSuchKey wraps as ObjectNotFoundError (business
  rejection for the complete endpoint) while network errors stay plain
  ObjectStoreError (infrastructure failure).
- Error wrapping: presign failures surface as ObjectStoreError with
  bucket/key in the message; the cause is chained.
- normalize_minio_endpoint: scheme stripped, bare host:port passthrough.
- Lazy client: construction performs no IO (minio.Minio not imported /
  constructed); injected clients bypass creation entirely.
"""
import asyncio
import os
import sys
from datetime import datetime, timezone

import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.object_store import (
    MinioObjectStore,
    ObjectNotFoundError,
    ObjectStoreError,
)
from app.object_store.minio_client import normalize_minio_endpoint
from minio.error import S3Error as _RealS3Error


BUCKET = "ani-kb-docs"
KEY = "22222222-2222-2222-2222-222222222222/33333333-3333-3333-3333-333333333333"


class _NoSuchKeyS3Error(_RealS3Error):
    """Real minio S3Error subclass carrying code='NoSuchKey' — exercises
    the production _is_no_such_key discrimination (isinstance + code).
    `code` is a read-only property over `_code`, so set the private attr
    before super().__init__ (which renders the message from the fields)."""

    def __init__(self, code):
        self._code = code
        super().__init__(
            code, "fake stat failure", KEY, "req-1", "host-1", None
        )


class _FakeStat:
    size = 1024
    etag = "abc123"
    content_type = "application/pdf"
    last_modified = datetime(2026, 9, 1, tzinfo=timezone.utc)


class _FakeMinioClient:
    """Recording fake of the minio SDK surface MinioObjectStore uses."""

    def __init__(self, *, stat=None, stat_exc=None, presign_exc=None,
                 put_exc=None, remove_exc=None, list_exc=None, objects=None):
        self.calls: list[tuple] = []
        self._stat = stat
        self._stat_exc = stat_exc
        self._presign_exc = presign_exc
        self._put_exc = put_exc
        self._remove_exc = remove_exc
        self._list_exc = list_exc
        self._objects = objects or []

    def presigned_put_object(self, bucket, key, expires):
        self.calls.append(("presigned_put_object", bucket, key, expires))
        if self._presign_exc is not None:
            raise self._presign_exc
        return "http://minio.public/put"

    def presigned_get_object(self, bucket, key, expires):
        self.calls.append(("presigned_get_object", bucket, key, expires))
        if self._presign_exc is not None:
            raise self._presign_exc
        return "http://minio.internal/get"

    def put_object(self, bucket, key, data, length, content_type=None):
        self.calls.append(
            ("put_object", bucket, key, data.read(), length, content_type)
        )
        if self._put_exc is not None:
            raise self._put_exc

    def stat_object(self, bucket, key):
        self.calls.append(("stat_object", bucket, key))
        if self._stat_exc is not None:
            raise self._stat_exc
        return self._stat or _FakeStat()

    def remove_object(self, bucket, key):
        self.calls.append(("remove_object", bucket, key))
        if self._remove_exc is not None:
            raise self._remove_exc

    def list_objects(self, bucket, prefix=None, recursive=False):
        self.calls.append(("list_objects", bucket, prefix, recursive))
        if self._list_exc is not None:
            raise self._list_exc
        return [  # mimic the SDK's lazy iterator of object records
            _FakeObject(name) for name in self._objects if name.startswith(prefix or "")
        ]


class _FakeObject:
    def __init__(self, object_name):
        self.object_name = object_name


def _make_store(client=None, public_client=None, **over):
    return MinioObjectStore(
        bucket=BUCKET, client=client, public_client=public_client, **over
    )


# ── normalize_minio_endpoint ──────────────────────────────────────────────────


def test_normalize_endpoint_strips_scheme():
    assert normalize_minio_endpoint("http://localhost:9000") == "localhost:9000"
    assert normalize_minio_endpoint("https://10.10.1.66:31950") == "10.10.1.66:31950"


def test_normalize_endpoint_bare_passthrough():
    assert normalize_minio_endpoint("localhost:9000") == "localhost:9000"
    assert normalize_minio_endpoint(" 10.10.1.66:31950 ") == "10.10.1.66:31950"


# ── presigned_put_object ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_presign_uses_public_view_client_and_default_ttl():
    """presign goes to the PUBLIC (NodePort) client — dual-view contract."""
    public = _FakeMinioClient()
    store = _make_store(public_client=public, presign_ttl_seconds=900)

    url = await store.presigned_put_object(key=KEY)

    assert url == "http://minio.public/put"
    assert public.calls == [
        ("presigned_put_object", BUCKET, KEY, public.calls[0][3])
    ]
    # TTL default from constructor.
    assert public.calls[0][3].total_seconds() == 900


@pytest.mark.asyncio
async def test_presign_ttl_override_beats_constructor_default():
    public = _FakeMinioClient()
    store = _make_store(public_client=public, presign_ttl_seconds=900)

    await store.presigned_put_object(key=KEY, expires_seconds=60)

    assert public.calls[0][3].total_seconds() == 60


@pytest.mark.asyncio
async def test_presign_rejects_non_positive_ttl_locally():
    """ttl<=0 is a config error — rejected before any MinIO round trip."""
    public = _FakeMinioClient()
    store = _make_store(public_client=public, presign_ttl_seconds=0)

    with pytest.raises(ObjectStoreError, match="invalid presign TTL"):
        await store.presigned_put_object(key=KEY)
    assert public.calls == []  # no round trip

    with pytest.raises(ObjectStoreError, match="invalid presign TTL"):
        await store.presigned_put_object(key=KEY, expires_seconds=-1)
    assert public.calls == []


@pytest.mark.asyncio
async def test_presign_failure_wraps_as_object_store_error():
    public = _FakeMinioClient(presign_exc=RuntimeError("boom"))
    store = _make_store(public_client=public)

    with pytest.raises(ObjectStoreError) as exc:
        await store.presigned_put_object(key=KEY)
    # Message carries bucket + key; the cause is chained.
    assert BUCKET in str(exc.value)
    assert KEY in str(exc.value)
    assert "boom" in str(exc.value)
    assert exc.value.__cause__ is not None


# ── presigned_get_object ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_presign_get_uses_internal_view_client_and_default_ttl():
    """presign GET defaults to the INTERNAL (ClusterIP) client — the rag-engine
    download contract (ADR §D1 line 138); the public view is never used."""
    internal = _FakeMinioClient()
    public = _FakeMinioClient()
    store = _make_store(client=internal, public_client=public,
                        presign_ttl_seconds=900)

    url = await store.presigned_get_object(key=KEY)

    assert url == "http://minio.internal/get"
    assert internal.calls == [
        ("presigned_get_object", BUCKET, KEY, internal.calls[0][3])
    ]
    assert internal.calls[0][3].total_seconds() == 900
    assert public.calls == []  # public view untouched


@pytest.mark.asyncio
async def test_presign_get_public_view_uses_public_client():
    """view="public" signs against the PUBLIC (NodePort) client — the frontend
    image-fetch contract (browser lives outside the cluster); the internal
    view must not be touched."""
    internal = _FakeMinioClient()
    public = _FakeMinioClient()
    store = _make_store(client=internal, public_client=public,
                        presign_ttl_seconds=900)

    url = await store.presigned_get_object(key=KEY, view="public")

    assert url == "http://minio.internal/get"  # fake returns a fixed URL
    assert public.calls == [
        ("presigned_get_object", BUCKET, KEY, public.calls[0][3])
    ]
    assert public.calls[0][3].total_seconds() == 900
    assert internal.calls == []  # internal view untouched


@pytest.mark.asyncio
async def test_presign_get_rejects_unknown_view():
    store = _make_store(client=_FakeMinioClient(),
                        public_client=_FakeMinioClient())

    with pytest.raises(ObjectStoreError, match="invalid presign view"):
        await store.presigned_get_object(key=KEY, view="sideways")


@pytest.mark.asyncio
async def test_presign_get_ttl_override_and_rejects_non_positive():
    internal = _FakeMinioClient()
    store = _make_store(client=internal, presign_ttl_seconds=900)

    await store.presigned_get_object(key=KEY, expires_seconds=60)
    assert internal.calls[0][3].total_seconds() == 60

    with pytest.raises(ObjectStoreError, match="invalid presign TTL"):
        await store.presigned_get_object(key=KEY, expires_seconds=0)
    assert len(internal.calls) == 1  # no round trip for the bad TTL


@pytest.mark.asyncio
async def test_presign_get_failure_wraps_as_object_store_error():
    internal = _FakeMinioClient(presign_exc=RuntimeError("boom"))
    store = _make_store(client=internal)

    with pytest.raises(ObjectStoreError) as exc:
        await store.presigned_get_object(key=KEY)
    assert "presign GET" in str(exc.value)
    assert BUCKET in str(exc.value)
    assert KEY in str(exc.value)
    assert exc.value.__cause__ is not None


# ── put_object ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_put_object_streams_bytes_via_internal_client(monkeypatch):
    """put_object uses the INTERNAL client and bridges the synchronous SDK
    call through asyncio.to_thread (real network IO)."""
    internal = _FakeMinioClient()
    store = _make_store(client=internal)

    seen_threads: list = []
    real_to_thread = asyncio.to_thread

    async def _spy(fn, *args, **kwargs):
        seen_threads.append(fn)
        return await real_to_thread(fn, *args, **kwargs)

    monkeypatch.setattr("app.object_store.minio_client.asyncio.to_thread", _spy)

    await store.put_object(key=KEY, content=b"PNGDATA",
                           content_type="image/png")

    assert seen_threads == [internal.put_object]  # went through to_thread
    call = internal.calls[0]
    assert call[0] == "put_object"
    assert call[1] == BUCKET
    assert call[2] == KEY
    assert call[3] == b"PNGDATA"      # BytesIO wrapped, read back verbatim
    assert call[4] == len(b"PNGDATA")
    assert call[5] == "image/png"


@pytest.mark.asyncio
async def test_put_object_content_type_defaults_to_octet_stream():
    internal = _FakeMinioClient()
    store = _make_store(client=internal)

    await store.put_object(key=KEY, content=b"x")

    assert internal.calls[0][5] == "application/octet-stream"


@pytest.mark.asyncio
async def test_put_object_failure_wraps_as_object_store_error():
    internal = _FakeMinioClient(put_exc=RuntimeError("boom"))
    store = _make_store(client=internal)

    with pytest.raises(ObjectStoreError) as exc:
        await store.put_object(key=KEY, content=b"x")
    assert BUCKET in str(exc.value)
    assert KEY in str(exc.value)
    assert exc.value.__cause__ is not None


# ── stat_object ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stat_uses_internal_view_client_and_maps_shape():
    """stat goes to the INTERNAL (ClusterIP) client and maps the stat row."""
    internal = _FakeMinioClient()
    store = _make_store(client=internal)

    stat = await store.stat_object(key=KEY)

    assert internal.calls == [("stat_object", BUCKET, KEY)]
    assert stat == {
        "size": 1024,
        "etag": "abc123",
        "content_type": "application/pdf",
        "last_modified": datetime(2026, 9, 1, tzinfo=timezone.utc),
    }


@pytest.mark.asyncio
async def test_stat_no_such_key_wraps_as_object_not_found():
    """NoSuchKey is a BUSINESS rejection (complete endpoint: FAILED_PRECONDITION),
    distinct from infrastructure failures (ObjectStoreError → UNAVAILABLE)."""
    internal = _FakeMinioClient(stat_exc=_NoSuchKeyS3Error("NoSuchKey"))
    store = _make_store(client=internal)

    with pytest.raises(ObjectNotFoundError) as exc:
        await store.stat_object(key=KEY)
    assert "object not found" in str(exc.value)
    # It is still an ObjectStoreError subclass.
    assert isinstance(exc.value, ObjectStoreError)
    assert exc.value.__cause__ is not None


@pytest.mark.asyncio
async def test_stat_network_error_stays_object_store_error():
    """Non-NoSuchKey failures stay plain ObjectStoreError — both a real
    S3Error with a different code AND a network error that only carries
    the code attr by shape (no accidental NoSuchKey promotion)."""
    for exc in (
        RuntimeError("connection refused"),
        _NoSuchKeyS3Error("InternalError"),
    ):
        internal = _FakeMinioClient(stat_exc=exc)
        store = _make_store(client=internal)

        with pytest.raises(ObjectStoreError) as caught:
            await store.stat_object(key=KEY)
        assert not isinstance(caught.value, ObjectNotFoundError)


# ── lifecycle: delete cleanup (issue-059, ADR §D1-删除) ───────────────────────


@pytest.mark.asyncio
async def test_remove_object_uses_internal_client_and_is_idempotent():
    """remove_object deletes a single key via the INTERNAL client; a missing
    object is not an error (S3 DELETE is idempotent) → always True."""
    internal = _FakeMinioClient()
    store = _make_store(client=internal)

    assert await store.remove_object(key=KEY) is True
    assert internal.calls == [("remove_object", BUCKET, KEY)]


@pytest.mark.asyncio
async def test_remove_object_failure_wraps_as_object_store_error():
    internal = _FakeMinioClient(remove_exc=RuntimeError("boom"))
    store = _make_store(client=internal)

    with pytest.raises(ObjectStoreError) as exc:
        await store.remove_object(key=KEY)
    assert BUCKET in str(exc.value)
    assert KEY in str(exc.value)
    assert exc.value.__cause__ is not None


@pytest.mark.asyncio
async def test_remove_objects_by_prefix_lists_then_deletes_and_counts():
    """remove_objects_by_prefix enumerates the prefix (recursive) then removes
    each object, returning the deleted count — the document image-prefix and
    KB-wide cleanup path (issue-059)."""
    kb = "22222222-2222-2222-2222-222222222222"
    doc = "33333333-3333-3333-3333-333333333333"
    prefix = f"{kb}/{doc}/images/"
    objects = [
        f"{prefix}aaa.png",
        f"{prefix}bbb.jpg",
        f"{kb}/{doc}",          # 正本，不带 images/ 前缀 — 不应被本前缀枚举命中
    ]
    internal = _FakeMinioClient(objects=objects)
    store = _make_store(client=internal)

    count = await store.remove_objects_by_prefix(prefix=prefix)

    assert count == 2
    assert internal.calls[0] == ("list_objects", BUCKET, prefix, True)
    removed = [c for c in internal.calls if c[0] == "remove_object"]
    assert set(removed) == {
        ("remove_object", BUCKET, f"{prefix}aaa.png"),
        ("remove_object", BUCKET, f"{prefix}bbb.jpg"),
    }


@pytest.mark.asyncio
async def test_remove_objects_by_prefix_empty_prefix_returns_zero():
    """No objects under the prefix → zero deletions, no remove calls (the
    common case for a document with no images)."""
    internal = _FakeMinioClient(objects=["other/thing"])
    store = _make_store(client=internal)

    assert await store.remove_objects_by_prefix(prefix=f"{KEY}/images/") == 0
    assert not [c for c in internal.calls if c[0] == "remove_object"]


@pytest.mark.asyncio
async def test_remove_objects_by_prefix_list_failure_wraps():
    internal = _FakeMinioClient(list_exc=RuntimeError("boom"))
    store = _make_store(client=internal)

    with pytest.raises(ObjectStoreError) as exc:
        await store.remove_objects_by_prefix(prefix="p/")
    assert "list" in str(exc.value)
    assert exc.value.__cause__ is not None


@pytest.mark.asyncio
async def test_presign_ttl_seconds_property_exposes_default():
    store = _make_store(presign_ttl_seconds=900)
    assert store.presign_ttl_seconds == 900


# ── lazy construction / injected clients ──────────────────────────────────────


def test_injected_clients_bypass_creation(monkeypatch):
    """Construction performs no IO; injected fakes are used directly."""

    def _boom(*args, **kwargs):
        raise AssertionError("minio.Minio must not be constructed")

    import minio as _minio_module
    monkeypatch.setattr(_minio_module, "Minio", _boom)

    store = MinioObjectStore(
        bucket=BUCKET,
        client=_FakeMinioClient(),
        public_client=_FakeMinioClient(),
    )
    # No construction until an operation — and injected clients never build.
    asyncio.run(store.presigned_put_object(key=KEY))


def test_public_endpoint_falls_back_to_internal_when_empty():
    """public_endpoint empty (the default) → falls back to the internal
    endpoint — single-node / dev deployments where both views coincide."""
    store = MinioObjectStore(
        endpoint="localhost:9000", public_endpoint="", bucket=BUCKET
    )
    assert store._public_endpoint == "localhost:9000"
