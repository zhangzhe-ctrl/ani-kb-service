"""proto 契约固化测试（KB-SPLIT-P2 / issue #060, ADR 0001 §四 #7、§五-阶段2）。

把拆分事务中的 proto 契约固化为可执行断言（对应 docs/specs/proto-contract.md）：

1. pin（对上）：kb.v1 + common.v1 归 kb 仓库，`services/kb-service/proto/`
   为单一真源；Go 侧镜像 `api/proto/` 必须逐字节一致（sha256）。
2. pin（对 rag）：`rag.v1` 与 rag-engine 共同 pin 同一版本——两侧源
   `services/kb-service/proto/rag/v1/rag.proto` 与
   `ai/rag-engine/app/grpc/rag.proto` 逐字节一致；RagEngine 服务面
   （Parse/Embed/Generate/GenerateStream）与 EmbedResponse 扁平 float
   数组约定（vectors_flat=1 / dimension=2 / count=3）固化。
3. 上传契约（D1）：预签名 PUT 时效（默认 900s、非正数本地拒绝）、key 规则
   `{kb_id}/{doc_id}`、双视角 URL 语义（PUT→public；GET internal vs public）。
4. D2（暂缓，零改动）：仅覆盖**租户上下文传递正确性**——过滤按请求携带的
   tenant_id 生效、缺失 metadata 不导致越权；不对具体 header 形态强断言。
5. RPC 面（ADR §一-1.3）：gRPC server 全量 RPC 与 proto service 一致
   （28 个，含 #052 ListOperations/GetOperation）。

不引入新依赖：文件哈希用 hashlib；proto 反射用生成物 DESCRIPTOR。
"""
import hashlib
import os
import sys
import uuid
from contextlib import asynccontextmanager

import grpc
import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_REPO_ROOT = os.path.abspath(os.path.join(_SERVICE_ROOT, "..", ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.api.grpc_server import KBServiceServicer
from app.generated.common.v1 import common_pb2
from app.generated.kb.v1 import kb_service_pb2 as kb_pb
from app.generated.kb.v1 import kb_service_pb2_grpc as kb_grpc
from app.object_store import MinioObjectStore, ObjectStoreError
from app.rag_engine import rag_pb2

TENANT_ID = "11111111-1111-1111-1111-111111111111"
KB_ID = "22222222-2222-2222-2222-222222222222"
DOC_ID = "33333333-3333-3333-3333-333333333333"
IDEMPOTENCY_KEY = "44444444-4444-4444-4444-444444444444"
BUCKET = "ani-kb-docs"

# 单一真源（kb-service 侧）与其镜像（对 rag-engine / Go 侧）。
_PIN = {
    "kb.v1": (
        os.path.join(_SERVICE_ROOT, "proto", "kb", "v1", "kb_service.proto"),
        os.path.join(_REPO_ROOT, "api", "proto", "kb", "v1", "kb_service.proto"),
    ),
    "common.v1": (
        os.path.join(_SERVICE_ROOT, "proto", "common", "v1", "common.proto"),
        os.path.join(_REPO_ROOT, "api", "proto", "common", "v1", "common.proto"),
    ),
    "rag.v1": (
        os.path.join(_SERVICE_ROOT, "proto", "rag", "v1", "rag.proto"),
        os.path.join(_REPO_ROOT, "ai", "rag-engine", "app", "grpc", "rag.proto"),
    ),
    # 内部推理端点解析（inference.internal.v1）：文件目录跟随上游 Go 生成物
    # 路径 inference/resolver/v1（服务 InferenceEndpointResolver，上游 #196 起
    # 旧 control 兼容路径双侧移除），proto 包名保持 inference.internal.v1。
    "inference.internal.v1": (
        os.path.join(
            _SERVICE_ROOT, "proto", "inference", "resolver", "v1",
            "inference_endpoint_resolver.proto",
        ),
        os.path.join(
            _REPO_ROOT, "api", "proto", "inference", "resolver", "v1",
            "inference_endpoint_resolver.proto",
        ),
    ),
}


def _sha256(path):
    with open(path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


# ── 1/2. pin：两侧源逐字节一致（byte-identical）─────────────────────────────


@pytest.mark.parametrize("name", sorted(_PIN))
def test_proto_source_pinned_byte_identical_across_repos(name):
    """pin = 两侧源 byte-identical。任一侧漂移，本契约测试失败。"""
    source, mirror = _PIN[name]
    assert os.path.isfile(source), f"missing kb-service source: {source}"
    if not os.path.isfile(mirror):
        # KB-SPLIT-P5A（issue #068）：镜像位于 ANI monorepo（api/proto/、
        # ai/rag-engine/），独立仓库检出内不存在。镜像侧一致性由 monorepo
        # 消费方（Gateway Go 生成物 / rag-engine）在依赖方切换（#069）时保证；
        # 本仓库内 pin 语义由「生成物 ↔ proto 源一致」测试组继续强制。
        pytest.skip(f"mirror not present in standalone checkout: {mirror}")
    assert _sha256(source) == _sha256(mirror), (
        f"{name} drifted: {source} != {mirror} — sync both sides before regenerating"
    )


def test_kb_service_proto_dir_is_single_source():
    """对上 proto（kb.v1 + common.v1）、rag.v1 与 inference.internal.v1
    （resolver）源均在 kb-service/proto/ 下。"""
    for name in ("kb.v1", "common.v1", "rag.v1", "inference.internal.v1"):
        source, _ = _PIN[name]
        assert source.startswith(os.path.join(_SERVICE_ROOT, "proto") + os.sep)


# ── 1b. 生成物与 proto 源一致（防「改源忘重生成」漂移）────────────────────────

# 运行时加载的是 app/generated/* 与 app/rag_engine/rag_pb2*；上面的源↔源 pin
# 无法发现「源已改、生成物未重生成」的漂移。本组按 docs/specs/proto-contract.md
# §3.2/§3.3 用锁定版本重生成后逐字节比对 tracked 产物（§3.4 可复现性的强制版）。

# tracked 生成物 → 重生成落点由 _regen_artifacts 映射
_GENERATED_ARTIFACTS = (
    "app/generated/kb/v1/kb_service_pb2.py",
    "app/generated/kb/v1/kb_service_pb2.pyi",
    "app/generated/kb/v1/kb_service_pb2_grpc.py",
    "app/generated/common/v1/common_pb2.py",
    "app/generated/common/v1/common_pb2.pyi",
    "app/generated/common/v1/common_pb2_grpc.py",
    "app/rag_engine/rag_pb2.py",
    "app/rag_engine/rag_pb2.pyi",
    "app/rag_engine/rag_pb2_grpc.py",
    "app/generated/inference/resolver/v1/inference_endpoint_resolver_pb2.py",
    "app/generated/inference/resolver/v1/inference_endpoint_resolver_pb2.pyi",
    "app/generated/inference/resolver/v1/inference_endpoint_resolver_pb2_grpc.py",
)


def _run_protoc(out_dir, include, files):
    """就地调用 grpc_tools.protoc（版本锁定见 requirements.txt）。"""
    import grpc_tools
    import grpc_tools.protoc as _protoc

    # 进程内调用不会自动挂载 grpc_tools 内置的 well-known proto（google/protobuf/*），
    # 须显式加入 include 路径（`python -m grpc_tools.protoc` 由 CLI 隐式完成）。
    well_known = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
    args = [f"-I{include}", f"-I{well_known}",
            f"--python_out={out_dir}", f"--pyi_out={out_dir}",
            f"--grpc_python_out={out_dir}", *files]
    rc = _protoc.main(["protoc", *args])
    assert rc == 0, f"protoc failed (rc={rc}) for {files}"


def _regen_artifacts(tmp_path):
    """按 §3.2 生成到 tmp，并按 §3.3 修正 rag_pb2_grpc 相对导入。返回 name→path。"""
    proto_dir = os.path.join(_SERVICE_ROOT, "proto")
    gen = tmp_path / "generated"
    rag = tmp_path / "rag"
    gen.mkdir()
    rag.mkdir()

    _run_protoc(
        str(gen), proto_dir,
        [os.path.join(proto_dir, "kb", "v1", "kb_service.proto"),
         os.path.join(proto_dir, "common", "v1", "common.proto")],
    )
    _run_protoc(
        str(rag), os.path.join(proto_dir, "rag", "v1"),
        [os.path.join(proto_dir, "rag", "v1", "rag.proto")],
    )
    # inference.internal.v1 → app/generated/（同构；文件目录 resolver/v1
    # 随上游 Go 生成物路径，Python 包路径跟随文件路径）。
    _run_protoc(
        str(gen), proto_dir,
        [os.path.join(proto_dir, "inference", "resolver", "v1",
                      "inference_endpoint_resolver.proto")],
    )

    rag_grpc = rag / "rag_pb2_grpc.py"
    blob = rag_grpc.read_bytes().replace(
        b"import rag_pb2 as rag__pb2", b"from . import rag_pb2 as rag__pb2"
    )
    rag_grpc.write_bytes(blob)  # 字节模式：避免 Windows 换行改写破坏比对

    return {
        "app/generated/kb/v1/kb_service_pb2.py": gen / "kb" / "v1" / "kb_service_pb2.py",
        "app/generated/kb/v1/kb_service_pb2.pyi": gen / "kb" / "v1" / "kb_service_pb2.pyi",
        "app/generated/kb/v1/kb_service_pb2_grpc.py": gen / "kb" / "v1" / "kb_service_pb2_grpc.py",
        "app/generated/common/v1/common_pb2.py": gen / "common" / "v1" / "common_pb2.py",
        "app/generated/common/v1/common_pb2.pyi": gen / "common" / "v1" / "common_pb2.pyi",
        "app/generated/common/v1/common_pb2_grpc.py": gen / "common" / "v1" / "common_pb2_grpc.py",
        "app/rag_engine/rag_pb2.py": rag / "rag_pb2.py",
        "app/rag_engine/rag_pb2.pyi": rag / "rag_pb2.pyi",
        "app/rag_engine/rag_pb2_grpc.py": rag_grpc,
        "app/generated/inference/resolver/v1/inference_endpoint_resolver_pb2.py": (
            gen / "inference" / "resolver" / "v1"
            / "inference_endpoint_resolver_pb2.py"
        ),
        "app/generated/inference/resolver/v1/inference_endpoint_resolver_pb2.pyi": (
            gen / "inference" / "resolver" / "v1"
            / "inference_endpoint_resolver_pb2.pyi"
        ),
        "app/generated/inference/resolver/v1/inference_endpoint_resolver_pb2_grpc.py": (
            gen / "inference" / "resolver" / "v1"
            / "inference_endpoint_resolver_pb2_grpc.py"
        ),
    }


def test_generated_artifacts_match_proto_source(tmp_path):
    """全部 tracked 生成物须与 proto 源同步（改了源必须重新生成，§3.4 可复现性）。

    单次重生成后循环比对，避免每份产物各跑一次 protoc。
    """
    pytest.importorskip("grpc_tools", reason="grpcio-tools 未安装，跳过生成物一致性校验")

    regen = _regen_artifacts(tmp_path)
    drifted = []
    for artifact in _GENERATED_ARTIFACTS:
        tracked_path = os.path.join(_SERVICE_ROOT, *artifact.split("/"))
        assert os.path.isfile(tracked_path), f"missing tracked artifact: {tracked_path}"
        if _sha256(tracked_path) != _sha256(str(regen[artifact])):
            drifted.append(artifact)

    assert drifted == [], (
        f"生成物与 proto 源不同步：{drifted}——按 docs/specs/proto-contract.md "
        f"§3.2/§3.3 重新生成并提交（源改了必须重生成，否则运行时加载的是旧契约）"
    )


# ── 2. rag.v1：服务面 + EmbedResponse 扁平数组约定 ──────────────────────────


def test_rag_service_surface_is_parse_embed_generate_stream():
    """rag proto 与 rag-engine 共同 pin 的 4 个 stateless RPC。"""
    methods = set(rag_pb2.DESCRIPTOR.services_by_name["RagEngine"].methods_by_name)
    assert methods == {"Parse", "Embed", "Generate", "GenerateStream"}


def test_embed_response_field_numbers_pinned():
    """EmbedResponse 字段号固化：vectors_flat=1 / dimension=2 / count=3。"""
    fields = rag_pb2.EmbedResponse.DESCRIPTOR.fields_by_name
    assert fields["vectors_flat"].number == 1
    assert fields["dimension"].number == 2
    assert fields["count"].number == 3
    # 扁平化前提：vectors_flat 是 repeated float（非嵌套）
    assert fields["vectors_flat"].is_repeated
    assert fields["vectors_flat"].type == fields["vectors_flat"].TYPE_FLOAT


def test_embed_response_flattened_array_wire_roundtrip():
    """扁平数组约定：vectors_flat[i * dimension + j] = 第 i 个文本第 j 维。

    契约固化在 wire 层：序列化 → 反序列化后按约定切片可无损还原二维向量。
    """
    vectors = [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]]
    dim = 4
    flat = [v for vec in vectors for v in vec]

    wire = rag_pb2.EmbedResponse(vectors_flat=flat, dimension=dim, count=2)
    parsed = rag_pb2.EmbedResponse()
    parsed.ParseFromString(wire.SerializeToString())

    assert parsed.dimension == dim
    assert parsed.count == 2
    # 契约切片：vectors[i] = flat[i*dim:(i+1)*dim]
    restored = [list(parsed.vectors_flat[i * dim:(i + 1) * dim]) for i in range(parsed.count)]
    for got, want in zip(restored, vectors):
        assert got == pytest.approx(want)


# ── 3. 上传契约（D1）：时效 / key 规则 / 双视角 URL ─────────────────────────


class _FakeMinioClient:
    """记录 presign 调用的 minio SDK fake（双视角各注入一个）。"""

    def __init__(self, url):
        self.calls: list[tuple] = []
        self._url = url

    def presigned_put_object(self, bucket, key, expires):
        self.calls.append(("put", bucket, key, expires.total_seconds()))
        return self._url

    def presigned_get_object(self, bucket, key, expires):
        self.calls.append(("get", bucket, key, expires.total_seconds()))
        return self._url


@pytest.mark.asyncio
async def test_upload_presign_rejects_non_positive_ttl_locally():
    """时效非正数在本地拒绝（不发起 MinIO 往返）。"""
    public = _FakeMinioClient("http://minio/put")
    store = MinioObjectStore(bucket=BUCKET, presign_ttl_seconds=0, public_client=public)
    with pytest.raises(ObjectStoreError, match="invalid presign TTL"):
        await store.presigned_put_object(key=f"{KB_ID}/{DOC_ID}")
    assert public.calls == []


@pytest.mark.asyncio
async def test_upload_key_rule_is_kb_id_slash_doc_id():
    """key 规则（ADR §D1）：正本 ``{kb_id}/{doc_id}``，无文件名后缀。"""
    public = _FakeMinioClient("http://minio/put")
    store = MinioObjectStore(bucket=BUCKET, presign_ttl_seconds=900, public_client=public)

    await store.presigned_put_object(key=f"{KB_ID}/{DOC_ID}")

    _verb, _bucket, key, _ttl = public.calls[0]
    assert key == f"{KB_ID}/{DOC_ID}"


@pytest.mark.asyncio
async def test_upload_dual_view_semantics():
    """双视角 URL 语义：PUT→public（前端直传）；GET internal→internal
    （rag-engine 集群内下载），GET public→public（浏览器取图片产物）。"""
    internal = _FakeMinioClient("http://minio.internal")
    public = _FakeMinioClient("http://minio.public")
    store = MinioObjectStore(
        bucket=BUCKET, presign_ttl_seconds=900,
        client=internal, public_client=public,
    )
    key = f"{KB_ID}/{DOC_ID}/images/55555555-5555-5555-5555-555555555555.png"

    await store.presigned_put_object(key=f"{KB_ID}/{DOC_ID}")
    assert [c[0] for c in public.calls] == ["put"]
    assert internal.calls == []

    internal.calls.clear()
    await store.presigned_get_object(key=key)  # 默认 internal
    assert [c[0] for c in internal.calls] == ["get"]
    assert public.calls[-1][0] == "put"  # public 未被 GET 触碰

    public.calls.clear()
    await store.presigned_get_object(key=key, view="public")
    assert [c[0] for c in public.calls] == ["get"]
    assert internal.calls[-1][0] == "get"


@pytest.mark.asyncio
async def test_upload_get_presign_rejects_unknown_view():
    """未知视角 → ObjectStoreError（契约封闭性）。"""
    store = MinioObjectStore(
        bucket=BUCKET, client=_FakeMinioClient("i"), public_client=_FakeMinioClient("p")
    )
    with pytest.raises(ObjectStoreError, match="invalid presign view"):
        await store.presigned_get_object(key=f"{KB_ID}/{DOC_ID}", view="sideways")


# ── 3b. 上传契约：servicer 侧签发端点（第一步）────────────────────────────────


class _FakeContext:
    """grpc.ServicerContext stand-in — abort 记录并 raise。"""

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


class _RecordingObjectStore:
    """记录 presign 的 object store fake（签发端点只用到 presign）。"""

    def __init__(self):
        self.calls: list[tuple] = []

    async def presigned_put_object(self, *, key):
        self.calls.append(("presign", key))
        return "http://minio/put"


class _RecordingConn:
    """记录 get_kb 查询参数（tenant_id/kb_id）的 fake conn。"""

    def __init__(self, *, kb_row="default"):
        self.get_kb_calls: list[tuple] = []
        self._kb_row = (
            {
                "id": uuid.UUID(KB_ID), "tenant_id": uuid.UUID(TENANT_ID),
                "name": "kb", "status": "active", "chunk_size": 512,
            }
            if kb_row == "default" else kb_row
        )

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            yield self

        return _tx()

    async def fetchrow(self, sql, *args):
        if "FROM knowledge_bases" in sql:
            # get_kb binds ($1=kb_id, $2=tenant_id); 记录顺序即 SQL 参数顺序。
            self.get_kb_calls.append(tuple(str(a) for a in args))
            # 过滤正确性：请求 tenant 必须与行 tenant 一致，否则 DB 不可见。
            if self._kb_row is None:
                return None
            if len(args) >= 2 and str(args[1]) != str(self._kb_row["tenant_id"]):
                return None
            return self._kb_row
        return None

    async def fetch(self, sql, *args):
        return []

    async def fetchval(self, sql, *args):
        return 0

    async def execute(self, sql, *args):
        return "UPDATE 1"


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


def _upload_req(**over):
    req = {
        "tenant_id": TENANT_ID, "kb_id": KB_ID, "file_name": "a.pdf",
        "file_type": "pdf", "idempotency_key": IDEMPOTENCY_KEY,
    }
    req.update(over)
    return kb_pb.GetDocumentUploadURLRequest(**req)


@pytest.mark.asyncio
async def test_upload_presign_uses_kb_own_key_and_defers_registration():
    """签发端点：key = {kb_id}/{doc_id}，仅 presign，不写 kb_documents。"""
    conn = _RecordingConn()
    store = _RecordingObjectStore()
    servicer = KBServiceServicer(pool=_Pool(conn), object_store=store)

    resp = await servicer._get_document_upload_url(_upload_req(), _FakeContext())

    assert store.calls == [("presign", f"{KB_ID}/{resp.doc_id}")]
    assert resp.storage_path == f"{KB_ID}/{resp.doc_id}"
    assert resp.upload_url == "http://minio/put"


@pytest.mark.asyncio
async def test_upload_key_rule_has_no_filename_suffix():
    """key 规则不含文件名后缀（非遗漏）：仅 {kb_id}/{doc_id}。"""
    conn = _RecordingConn()
    store = _RecordingObjectStore()
    servicer = KBServiceServicer(pool=_Pool(conn), object_store=store)

    resp = await servicer._get_document_upload_url(
        _upload_req(file_name="weird name.v2.PDF"), _FakeContext()
    )

    assert resp.storage_path == f"{KB_ID}/{resp.doc_id}"
    assert "weird" not in resp.storage_path


# ── 4. D2：租户上下文传递正确性（不对 header 形态强断言）────────────────────


@pytest.mark.asyncio
async def test_tenant_context_propagates_to_repo_query():
    """租户上下文传递正确性：请求携带的 tenant_id 原样传给仓储查询。"""
    conn = _RecordingConn()
    servicer = KBServiceServicer(pool=_Pool(conn), object_store=_RecordingObjectStore())

    await servicer._get_document_upload_url(_upload_req(), _FakeContext())

    assert conn.get_kb_calls, "repository query not issued"
    # 位置断言：get_kb 绑定 ($1=kb_id, $2=tenant_id)——参数互换会被本断言捕获，
    # 集合断言则漏检（契约测试的目的正是「传递正确」而非「都传了」）。
    assert conn.get_kb_calls[0] == (KB_ID, TENANT_ID)


@pytest.mark.asyncio
async def test_tenant_isolation_other_tenant_kb_is_not_found():
    """越权防线：以 tenant B 身份请求 tenant A 的 KB → NOT_FOUND，不签发 URL。"""
    other_tenant = "99999999-9999-9999-9999-999999999999"
    conn = _RecordingConn()
    store = _RecordingObjectStore()
    servicer = KBServiceServicer(pool=_Pool(conn), object_store=store)
    ctx = _FakeContext()

    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        await servicer._get_document_upload_url(
            _upload_req(tenant_id=other_tenant), ctx
        )
    assert ctx.aborted[0] == grpc.StatusCode.NOT_FOUND
    assert store.calls == []  # 未签发任何 URL


@pytest.mark.asyncio
async def test_tenant_context_without_metadata_does_not_escalate():
    """D2 零改动：无 x-user-id metadata（内部调用者）不改变租户过滤语义——
    过滤仍按请求 tenant_id 生效，不因缺失 metadata 而放大可见范围。"""
    conn = _RecordingConn()
    servicer = KBServiceServicer(pool=_Pool(conn), object_store=_RecordingObjectStore())

    await servicer._get_document_upload_url(
        _upload_req(), _FakeContext(metadata=())
    )

    assert conn.get_kb_calls[0][1] == TENANT_ID


# ── 5. RPC 面：gRPC server 全量 RPC 与 proto service 一致（ADR §一-1.3）────


def _proto_kb_rpcs() -> set[str]:
    return set(kb_pb.DESCRIPTOR.services_by_name["KBService"].methods_by_name)


def test_proto_kb_service_declares_all_rpcs():
    """kb.v1 服务面含 #052/#059 新增（ListOperations/GetOperation/GetObjectURL）。"""
    rpcs = _proto_kb_rpcs()
    assert {"ListOperations", "GetOperation"} <= rpcs
    assert "GetObjectURL" in rpcs
    assert len(rpcs) == 27


def test_grpc_servicer_covers_every_proto_rpc():
    """gRPC server 全量 RPC 面与 proto 一致：proto 的每个 RPC 都有 handler。"""
    servicer = KBServiceServicer()
    proto_rpcs = _proto_kb_rpcs()
    missing = [n for n in sorted(proto_rpcs) if not callable(getattr(servicer, n, None))]
    assert missing == [], f"servicer missing handlers for: {missing}"


def test_grpc_servicer_subclasses_generated_base():
    assert isinstance(KBServiceServicer(), kb_grpc.KBServiceServicer)


def test_common_tenant_context_field_numbers_pinned():
    """common.v1.TenantContext 字段号固化（对上契约，D2 零改动沿用）。"""
    fields = common_pb2.TenantContext.DESCRIPTOR.fields_by_name
    assert fields["tenant_id"].number == 1
    assert fields["user_id"].number == 2
    assert fields["roles"].number == 3
