"""KB-SPLIT-P4D（issue #067）专项回归套件 — 复用 #066 独立环境 e2e 基建。

五类专项回归（在 #066 全链路环境上，全真实依赖）：
  AC① 降级路径回归：Redis 挂 → 会话 DB-only /readyz 仍 ok；NATS 挂 → outbox
      积压恢复后 at-least-once 追平 /readyz degraded（#063 门控语义）。
  AC② 重解析幂等回归：每次重跑生成新 run_id（kb_operations 新行）；旧 kb_chunks
      行与旧向量显式清理无残留（DB chunk_id 集合 + Milvus 向量计数双验证）。
  AC③ 跨租户负向回归：另一租户 GET/UPDATE/DELETE → 404/空集；覆盖
      KB/Document/Session/Message/Chunk/Permission/Operation（Operation 走 gRPC）。
  AC④ D1 专项：预签名直传全链路 — key 越权拒绝（篡改 key 签名失效 + GetObjectURL
      三档拒绝）、过期 URL 拒绝（MINIO_PRESIGN_TTL_SECONDS=2 时效生效）、图片按
      {kb_id}/{doc_id}/images/ 前缀批量删除。
  AC⑤ D4 专项：重复 RPC（CreateKB/UpdateKB/Reparse）命中 kb_operations 幂等回放，
      不重复执行副作用（操作行数 / outbox 事件数 / KB 行数 / 向量数核对）。

证据分级（AC⑥）：pass / fail / not_verified + 五层次区分
（源码级 < 真实依赖级 < 正式进程级 < 产品链路级 < 部署级），
结果由本套件 SUMMARY 输出后人工誊入 docs/execution/status.md。

运行前置（同 #066）：
  - 服务器 kb-service scale 0（避免共享 NATS durable 消费竞争）；
  - repo/bin/ani-gateway.exe 已构建；
  - embedding SSH 隧道可达（套件自动建立/复用 127.0.0.1:18081）。
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
import urllib.parse
import uuid
from pathlib import Path
from typing import Any

# ── 复用 #066 e2e 基建（该文件有 __main__ 守卫，可安全 import） ─────────────────
_HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "test_issue066_e2e", _HERE / "test_issue066_e2e.py")
m066 = importlib.util.module_from_spec(_spec)
sys.modules["test_issue066_e2e"] = m066
_spec.loader.exec_module(m066)

# ── 覆盖 #066 模块级常量：日志与 tag 独立于 #066 产物 ──────────────────────────
m066.E2E_LOG = m066.KB_SVC / "tests" / "e2e" / "e2e_issue067_result.log"
m066.E2E_TAG = f"e2e067-{int(time.time())}"

E2E_TAG = m066.E2E_TAG
TENANT_ID = m066.TENANT_ID
OTHER_TENANT_ID = m066.OTHER_TENANT_ID
GRPC_PORT = 50053
H_OTHER = {"Content-Type": "application/json",
           "X-Dev-Tenant-ID": OTHER_TENANT_ID}


# m066._start 写死 e2e066 stdout 日志名 → 换成 067 后缀（进程管理逻辑一致）
def _start_067(args, env, cwd, name):
    full_env = os.environ.copy()
    full_env.update(env)
    m066.log(f"Starting {name}: {' '.join(args[:3])}... (cwd={cwd})")
    log_file = open(m066.E2E_LOG.parent / f"{name}_e2e067.stdout.log", "w",
                    encoding="utf-8")
    p = subprocess.Popen(
        args, env=full_env, cwd=str(cwd), stdout=log_file,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    m066._procs.append(p)
    m066.log(f"  {name} started: pid={p.pid}")
    return p


m066._start = _start_067

log, log_json = m066.log, m066.log_json
check, check_not_verified = m066.check, m066.check_not_verified


# ══════════════════════════════════════════════════════════════════════════════
# #067 专属辅助（PG / Milvus / MinIO / gRPC / 进程）
# ══════════════════════════════════════════════════════════════════════════════
def _readyz() -> tuple[int, dict]:
    st, text = m066.http_get_text("http://localhost:8002/readyz", "GET /readyz")
    try:
        return st, (json.loads(text) if text else {})
    except json.JSONDecodeError:
        return st, {}


def _wait_ports_closed(timeout: float = 30.0) -> bool:
    """等待 kb-service 双端口（8002/50053）真正释放后再启新实例。

    Windows taskkill 后端口释放有延迟：旧实例残留在监听时，新实例 bind 失败
    崩溃、而探针（_wait_http/_wait_port）会打到旧进程 —— AC① 降级探针首跑
    即因此打到主实例 /readyz（session_cache=True）。必须先等端口关闭。
    """
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        alive = False
        for port in (8002, GRPC_PORT):
            s = socket.socket()
            s.settimeout(0.5)
            try:
                s.connect(("localhost", port))
                alive = True
            except OSError:
                pass
            finally:
                s.close()
        if not alive:
            return True
        time.sleep(1)
    log("  WARNING: ports 8002/50053 still occupied after kill")
    return False


def _kill_main_kb() -> None:
    """停止主 kb-service 实例并等待端口释放（降级探针/换 env 重启共用）。"""
    if m066._main_kb_proc is not None:
        m066._kill_proc(m066._main_kb_proc)
        if m066._main_kb_proc in m066._procs:
            m066._procs.remove(m066._main_kb_proc)
        m066._main_kb_proc = None
    _wait_ports_closed()


def _healthz_status() -> int:
    st, _ = m066.http_get_text("http://localhost:8002/health", "GET /health")
    return st


async def _fetch_chunk_ids(kb_id: str, doc_id: str) -> list[str]:
    import asyncpg
    conn = await asyncpg.connect(m066.KB_PG_URL)
    try:
        rows = await conn.fetch(
            "SELECT id::text AS id FROM kb_chunks "
            " WHERE kb_id=$1 AND doc_id=$2",
            uuid.UUID(kb_id), uuid.UUID(doc_id))
        return [r["id"] for r in rows]
    finally:
        await conn.close()


async def _fetch_operations_rows(tenant_id: str, operation_type: str,
                                 resource_id: str) -> list[dict]:
    import asyncpg
    conn = await asyncpg.connect(m066.KB_PG_URL)
    try:
        rows = await conn.fetch(
            "SELECT id::text AS id, idempotency_key, operation_type, status, "
            "       resource_id::text AS resource_id "
            "  FROM kb_operations "
            " WHERE tenant_id=$1 AND operation_type=$2 AND resource_id=$3 "
            " ORDER BY created_at",
            uuid.UUID(tenant_id), operation_type, uuid.UUID(resource_id))
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def _fetch_outbox_events(aggregate_id: str, event_type: str) -> list[dict]:
    import asyncpg
    conn = await asyncpg.connect(m066.KB_PG_URL)
    try:
        rows = await conn.fetch(
            "SELECT id, event_type, published, payload, created_at "
            "  FROM kb_outbox_events "
            " WHERE aggregate_id=$1 AND event_type=$2 ORDER BY id",
            uuid.UUID(aggregate_id), event_type)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def _seed_doc_failed(doc_id: str) -> bool:
    """测试种子：把文档置 failed 以触发 reparse 门（failed 才可重解析）。

    reparse 链路本身（幂等键 / kb_operations / outbox / 清理 / 重跑）全真实；
    失败态由种子构造——生产中失败来自真实解析异常（AC②-0 已用 garbage pdf
    验证自然失败路径）。reset_for_reparse_in_tx 只排除
    error_message='deleted'，种子 error_message 不影响重置。
    """
    import asyncpg
    conn = await asyncpg.connect(m066.KB_PG_URL)
    try:
        res = await conn.execute(
            "UPDATE kb_documents SET parse_status='failed', "
            "       error_message='p4d-seed: forced failed to arm the reparse gate'"
            " WHERE id=$1 AND tenant_id=$2",
            uuid.UUID(doc_id), uuid.UUID(TENANT_ID))
        return res.endswith("1")
    finally:
        await conn.close()


def _wait_doc_leaves(doc_id: str, status: str, timeout: float = 90.0) -> bool:
    """等待文档 parse_status 离开某状态（规避「reparse 提交前旧终态被误读为本轮
    终态」的竞态：消费方真正重置后状态才会变化）。"""
    import asyncio
    import asyncpg

    async def _w() -> bool:
        conn = await asyncpg.connect(m066.KB_PG_URL)
        try:
            deadline = asyncio.get_event_loop().time() + timeout
            while asyncio.get_event_loop().time() < deadline:
                row = await conn.fetchrow(
                    "SELECT parse_status FROM kb_documents "
                    " WHERE id=$1 AND tenant_id=$2",
                    uuid.UUID(doc_id), uuid.UUID(TENANT_ID))
                if row and row["parse_status"] != status:
                    return True
                await asyncio.sleep(1)
            return False
        finally:
            await conn.close()

    return m066._run_async(_w())


def _milvus_doc_vector_count(kb_id: str, doc_id: str) -> int:
    """D1 向量自管：collection 中 metadata["doc_id"]==doc_id 的向量条数（-1 不可得）。"""
    from pymilvus import AsyncMilvusClient
    from app.vector_store.milvus_client import normalize_milvus_uri

    coll = "kb_" + kb_id.replace("-", "")

    async def _count() -> int:
        client = AsyncMilvusClient(uri=normalize_milvus_uri(m066.MILVUS_ADDR))
        try:
            if not await client.has_collection(coll):
                return -1
            try:
                await client.flush(coll)
            except Exception:  # noqa: BLE001 — flush 失败退回 query
                pass
            rows = await client.query(
                collection_name=coll,
                filter=f'metadata["doc_id"] == "{doc_id}"',
                output_fields=["count(*)"])
            return int(rows[0].get("count(*)", 0) or 0) if rows else 0
        finally:
            try:
                await client.close()
            except Exception:  # noqa: BLE001
                pass

    try:
        return m066._run_async(_count())
    except Exception as e:  # noqa: BLE001
        log(f"  milvus doc vector count failed: {e!r}")
        return -1


# 向量↔chunk 关系候选（实测本环境 summary chunk 也被嵌入：vec = child +
# doc_summary）。从基线归纳「哪些 chunk 类型被嵌入」，再应用到重跑后的 stats。
_VEC_SUBSETS: dict[str, Any] = {
    "child": lambda s: s.get("child", 0),
    "child+parent": lambda s: s.get("child", 0) + s.get("parent", 0),
    "child+summary": lambda s: s.get("child", 0) + s.get("doc_summary", 0),
    "child+parent+summary": lambda s: s.get("child", 0) + s.get("parent", 0)
                                     + s.get("doc_summary", 0),
}


def _vec_subset(stats: dict, vec: int) -> str | None:
    """从基线 (stats, vec) 归纳向量↔chunk 关系子集名；不可归纳返回 None。"""
    if vec <= 0:
        return None
    for name, fn in _VEC_SUBSETS.items():
        if vec == fn(stats):
            return name
    return None


def _list_object_keys(prefix: str) -> list[str]:
    from minio import Minio
    c = Minio(m066.MINIO_ENDPOINT, access_key=m066.MINIO_AK,
              secret_key=m066.MINIO_SK, secure=False)
    return [obj.object_name for obj in
            c.list_objects(m066.MINIO_BUCKET, prefix=prefix, recursive=True)]


def _grpc_reparse(kb_id: str, doc_id: str, idem_key: str) -> dict:
    """gRPC 直连 ReparseDocument → AsyncTaskRef dict（真实依赖级，不走 gateway）。"""
    pb, stub = m066._grpc_stub_on_port(GRPC_PORT)
    resp = stub.ReparseDocument(pb.ReparseDocumentRequest(
        tenant_id=TENANT_ID, kb_id=kb_id, doc_id=doc_id,
        idempotency_key=idem_key), timeout=60)
    return {"task_id": resp.task_id, "task_type": resp.task_type,
            "status": resp.status}


def _grpc_list_operations(tenant_id: str, resource_id: str = "",
                          operation_type: str = "") -> list[dict]:
    pb, stub = m066._grpc_stub_on_port(GRPC_PORT)
    resp = stub.ListOperations(pb.ListOperationsRequest(
        tenant_id=tenant_id, operation_type=operation_type,
        resource_id=resource_id), timeout=30)
    return [{"id": op.id, "idempotency_key": op.idempotency_key,
             "operation_type": op.operation_type, "resource_id": op.resource_id,
             "status": op.status} for op in resp.items]


def _grpc_get_operation_code(tenant_id: str, operation_id: str) -> str:
    import grpc
    pb, stub = m066._grpc_stub_on_port(GRPC_PORT)
    try:
        stub.GetOperation(pb.GetOperationRequest(
            tenant_id=tenant_id, operation_id=operation_id), timeout=30)
        return "OK"
    except grpc.RpcError as e:
        return e.code().name


def _restart_kb_with_env(extra: dict) -> bool:
    """停主实例 → 以 extra 环境变量重启（独占 8002/50053）→ 等就绪。"""
    if m066._main_kb_proc is not None:
        m066._kill_proc(m066._main_kb_proc)
        if m066._main_kb_proc in m066._procs:
            m066._procs.remove(m066._main_kb_proc)
        m066._main_kb_proc = None
        time.sleep(2)
    kb_env = m066._kb_service_env()
    kb_env.update(extra)
    kb_env["GRPC_PORT"] = "50053"
    m066._main_kb_proc = m066._start([sys.executable, "main.py"], kb_env,
                                     m066.KB_SVC, "kb-service")
    ok = (m066._wait_http("http://localhost:8002/health", 90)
          and m066._wait_port("localhost", GRPC_PORT, timeout=60))
    if ok:
        m066._wait_gateway_ready(60)
    return ok


def _tamper_put(upload_url: str, data: bytes, new_key: str) -> int:
    """把预签名 URL 的对象 key（path 中 bucket 之后部分）替换为 new_key。

    预签名签名覆盖原始 key → 篡改后签名失效，MinIO 必须拒绝（预定 key
    不可写他处，D1 预签名直传越权测试面）。
    """
    parsed = urllib.parse.urlsplit(upload_url)
    bucket = parsed.path.lstrip("/").split("/", 1)[0]
    tampered = parsed._replace(path=f"/{bucket}/{new_key}")
    url2 = urllib.parse.urlunsplit(tampered)
    log(f"[tamper PUT] signed key → replaced with {new_key} (signature invalid)")
    return m066.http_put_bytes(url2, data, "PUT tampered key")


# ══════════════════════════════════════════════════════════════════════════════
# 分步回归
# ══════════════════════════════════════════════════════════════════════════════
def _step1_create_kb_and_replay() -> str:
    log("════ STEP1: CreateKB + AC⑤ 同名重放 ════")
    kb_name = f"e2e067-kb-{E2E_TAG}"
    status, kb1 = m066.rest_request(
        "POST", "/knowledge-bases", body={
            "idempotency_key": str(uuid.uuid4()),
            "name": kb_name,
            "description": "KB-SPLIT-P4D 专项回归宿主 KB",
        }, expected_status=201, label="CreateKB#1",
        desc="创建宿主知识库")
    kb_id = kb1.get("id", "") if isinstance(kb1, dict) else ""
    check("STEP1 CreateKB → 201", status == 201 and bool(kb_id),
          f"status={status} kb_id={kb_id}")
    if not kb_id:
        return ""
    log(f"  kb_id={kb_id}")

    # AC⑤-CreateKB：proto 无 idempotency_key 字段（gateway 也不透传）→ kb 侧
    # 以确定性键 create_kb:{tenant}:{name} 兜底 → 同名第二次创建命中回放。
    status, kb2 = m066.rest_request(
        "POST", "/knowledge-bases", body={
            "idempotency_key": str(uuid.uuid4()),  # gateway 必填校验用，与首次不同
            "name": kb_name,
            "description": "KB-SPLIT-P4D 专项回归宿主 KB",
        }, expected_status=201, label="CreateKB#2(same name)",
        desc="AC⑤: 同名重复 CreateKB → 确定性幂等键命中回放")
    kb2_id = kb2.get("id", "") if isinstance(kb2, dict) else ""
    check("AC⑤ CreateKB 同名重放 → 201 且同一 kb_id（无重复副作用）",
          status == 201 and kb2_id == kb_id,
          f"status={status} kb2_id={kb2_id}", layer="真实依赖级")

    rows = m066._run_async(_fetch_operations_rows(TENANT_ID, "kb.create", kb_id))
    check("AC⑤ CreateKB 重放 → kb_operations 恰 1 条 kb.create",
          len(rows) == 1, f"rows={rows}", layer="真实依赖级")

    status, lst = m066.rest_request(
        "GET", "/knowledge-bases?limit=50", expected_status=200,
        label="ListKBs(对照)")
    items = lst.get("items", []) if isinstance(lst, dict) else []
    dup = [i for i in items if i.get("id") == kb_id]
    check("AC⑤ CreateKB 重放 → 无重复 KB 行（列表中该 id 恰 1 条）",
          status == 200 and len(dup) == 1,
          f"items={len(items)} dup={len(dup)}", layer="真实依赖级")
    return kb_id


def _step2_md_doc_baseline(kb_id: str) -> tuple[str, dict, list[str], int]:
    """md 文档直传 → complete → ready；返回 (doc_id, chunk_stats, chunk_ids, vec)。"""
    log("════ STEP2: md 文档直传 + 解析 ready（AC② 基线 + AC④-0 key 越权） ════")
    content = m066._doc_md_bytes()
    fname = f"e2e067-doc-{E2E_TAG}.md"
    status, up = m066.rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents", body={
            "idempotency_key": str(uuid.uuid4()),
            "file_name": fname, "file_type": "md",
            "file_size_bytes": len(content),
        }, expected_status=200, label="GetUploadURL(md)")
    doc_id = up.get("doc_id", "") if isinstance(up, dict) else ""
    upload_url = up.get("upload_url", "") if isinstance(up, dict) else ""
    storage_path = up.get("storage_path", "") if isinstance(up, dict) else ""
    check("STEP2 GetUploadURL(md) → 200 + storage_path={kb}/{doc}",
          status == 200 and bool(doc_id) and storage_path == f"{kb_id}/{doc_id}",
          f"status={status} doc_id={doc_id} storage_path={storage_path}")
    if not (doc_id and upload_url):
        return "", {}, [], -1

    put = m066.http_put_bytes(upload_url, content, "PUT presigned(md)")
    check("STEP2 预签名 PUT(md) → 200", put == 200, f"status={put}")

    # AC④-0：预签名 URL 的 key 越权 — 篡改 path 中的 key → 签名失效被拒。
    # MinIO 对签名不匹配可能回 403，也可能直接重置连接（传输层拒绝，status=-1），
    # 故接受「任何非 200」；并以「篡改 key 下无对象落地」作为未写入他处的硬证据。
    tam = _tamper_put(upload_url, content,
                      f"{kb_id}/{doc_id}/tampered-{E2E_TAG}.md")
    tam_keys = _list_object_keys(f"{kb_id}/{doc_id}/tampered-")
    check("AC④-0 预签名 key 越权（篡改 key 被拒且未写入他处）",
          tam != 200 and not tam_keys,
          f"status={tam} tampered_objects={tam_keys}", layer="真实依赖级")

    status, _ = m066.rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents/{doc_id}/complete", body={
            "idempotency_key": str(uuid.uuid4()),
            "file_name": fname, "file_type": "md",
            "checksum_sha256": "", "custom_metadata": "",
        }, expected_status=202, label="Complete(md)")
    check("STEP2 Complete(md) → 202", status == 202, f"status={status}")
    state = m066.wait_doc_ready(doc_id, timeout=300)
    check("STEP2 md 解析 ready", state.get("parse_status") == "ready",
          f"state={state}", layer="产品链路级")
    stats = m066._run_async(m066._fetch_chunk_stats(kb_id, doc_id))
    ids = m066._run_async(_fetch_chunk_ids(kb_id, doc_id))
    vec = _milvus_doc_vector_count(kb_id, doc_id)
    check("STEP2 基线: child chunks + Milvus 向量已就位",
          stats.get("child", 0) > 0 and vec > 0,
          f"chunks={stats} vectors={vec}", layer="真实依赖级")
    return doc_id, stats, ids, vec


def _step_object_url_guards(kb_id: str, doc_id: str) -> None:
    """AC④-0: GetObjectURL 越权三档拒绝（400 / 409 / 409）。"""
    log("════ AC④-0: GetObjectURL 越权三档拒绝 ════")
    status, _ = m066.rest_request(
        "GET", f"/knowledge-bases/{kb_id}/objects?object_id=no-slash-key",
        expected_status=400, label="GetObjectURL(no slash)")
    check("AC④-0 object_id 无 '/' → 400（INVALID_ARGUMENT）",
          status == 400, f"status={status}", layer="真实依赖级")
    status, _ = m066.rest_request(
        "GET", f"/knowledge-bases/{kb_id}/objects?object_id="
               f"{urllib.parse.quote('other-bucket/some/key.png')}",
        expected_status=409, label="GetObjectURL(foreign bucket)")
    check("AC④-0 object_id bucket ≠ kb 自有 bucket → 409（FAILED_PRECONDITION）",
          status == 409, f"status={status}", layer="真实依赖级")
    status, _ = m066.rest_request(
        "GET", f"/knowledge-bases/{kb_id}/objects?object_id="
               f"{urllib.parse.quote(m066.MINIO_BUCKET + '/other-kb/leak.png')}",
        expected_status=409, label="GetObjectURL(foreign prefix)")
    check("AC④-0 object_id key 前缀 ≠ {kb_id}/ → 409（FAILED_PRECONDITION）",
          status == 409, f"status={status}", layer="真实依赖级")


def _step3_update_replay(kb_id: str) -> str:
    log("════ STEP3: AC⑤ UpdateKB 同键重放 ════")
    k = str(uuid.uuid4())
    status, u1 = m066.rest_request(
        "PUT", f"/knowledge-bases/{kb_id}", body={
            "idempotency_key": k, "description": "P4D 专项回归描述 v1",
        }, expected_status=200, label="UpdateKB#1")
    check("STEP3 UpdateKB#1 → 200", status == 200, f"status={status}")
    status, u2 = m066.rest_request(
        "PUT", f"/knowledge-bases/{kb_id}", body={
            "idempotency_key": k, "description": "P4D 专项回归描述 v1",
        }, expected_status=200, label="UpdateKB#2(same key)",
        desc="AC⑤: 同 idempotency_key 重复 UpdateKB → 命中 kb_operations 回放")
    desc1 = u1.get("description") if isinstance(u1, dict) else None
    desc2 = u2.get("description") if isinstance(u2, dict) else None
    check("AC⑤ UpdateKB 同键重放 → 200 且结果一致",
          status == 200 and desc2 == desc1 == "P4D 专项回归描述 v1",
          f"status={status} desc1={desc1!r} desc2={desc2!r}", layer="真实依赖级")
    rows = m066._run_async(_fetch_operations_rows(TENANT_ID, "kb.update", kb_id))
    same_key = [r for r in rows if r["idempotency_key"] == k]
    check("AC⑤ UpdateKB 重放 → 该键操作行恰 1 条（未重复执行）",
          len(same_key) == 1,
          f"rows={len(rows)} same_key={len(same_key)}", layer="真实依赖级")
    return k


def _step4_session(kb_id: str) -> str:
    log("════ STEP4: SSE 会话（供 AC③ Session/Message 负向） ════")
    try:
        events = m066._sse_query(kb_id, "ANI 知识库平台支持哪三种检索模式？",
                                 retrieval_mode="hybrid", top_k=5,
                                 score_threshold=0.0)
        ans, it, ot, srcs = m066._sse_answer(events)
        sess_id = ""
        for e in events:
            if e["event"] == "done" and isinstance(e["data"], dict):
                sess_id = str(e["data"].get("session_id", ""))
        check("STEP4 SSE 查询成功（done 带 session_id + sources）",
              bool(sess_id) and len(srcs) > 0,
              f"session={sess_id} sources={len(srcs)} tokens={it}+{ot}",
              layer="产品链路级")
        if sess_id:
            msgs = m066._run_async(m066._fetch_session_messages(sess_id))
            roles = [mm.get("role") for mm in msgs]
            check("STEP4 会话 DB 双写（user + assistant 落库）",
                  "user" in roles and "assistant" in roles, f"roles={roles}",
                  layer="真实依赖级")
        return sess_id
    except Exception as e:  # noqa: BLE001
        check("STEP4 SSE 查询成功", False, repr(e), layer="产品链路级")
        return ""


def _step5_cross_tenant_negative(kb_id: str, doc_id: str, session_id: str,
                                 owner_op_id: str, owner_update_key: str) -> None:
    log("════ STEP5: AC③ 跨租户负向（X-Dev-Tenant-ID=另一租户） ════")

    def nx(label: str, method: str, path: str, body: dict | None = None,
           expect: int = 404) -> None:
        status, payload = m066.rest_request(
            method, path, body=body, expected_status=expect, headers=H_OTHER,
            label=label)
        check(f"AC③ {label}", status == expect,
              f"status={status} expect={expect}", layer="真实依赖级")

    # KB
    nx("另一租户 GetKB → 404", "GET", f"/knowledge-bases/{kb_id}")
    nx("另一租户 UpdateKB（持有者幂等键）→ 404（无跨租户幂等回放泄漏）",
       "PUT", f"/knowledge-bases/{kb_id}",
       body={"idempotency_key": owner_update_key,
             "description": "cross-tenant hijack"})
    nx("另一租户 DeleteKB → 404", "DELETE", f"/knowledge-bases/{kb_id}")
    status, lst = m066.rest_request(
        "GET", "/knowledge-bases?limit=50", expected_status=200,
        headers=H_OTHER, label="另一租户 ListKBs")
    items = lst.get("items", []) if isinstance(lst, dict) else []
    check("AC③ 另一租户 ListKBs → 宿主 KB 不可见（空集/不含 kb_id）",
          status == 200 and all(i.get("id") != kb_id for i in items),
          f"status={status} items={len(items)}", layer="真实依赖级")
    # Document
    nx("另一租户 GetUploadURL → 404", "POST",
       f"/knowledge-bases/{kb_id}/documents",
       body={"idempotency_key": str(uuid.uuid4()), "file_name": "x.md",
             "file_type": "md"})
    nx("另一租户 ListDocuments → 404", "GET",
       f"/knowledge-bases/{kb_id}/documents")
    nx("另一租户 GetDocument → 404", "GET",
       f"/knowledge-bases/{kb_id}/documents/{doc_id}")
    nx("另一租户 DeleteDocument → 404", "DELETE",
       f"/knowledge-bases/{kb_id}/documents/{doc_id}")
    # Chunk
    nx("另一租户 ListDocumentChunks → 404", "GET",
       f"/knowledge-bases/{kb_id}/documents/{doc_id}/chunks")
    # Permission
    nx("另一租户 GetKBPermissions → 404", "GET",
       f"/knowledge-bases/{kb_id}/permissions")
    # Session / Message
    nx("另一租户 ListSessions → 404", "GET",
       f"/knowledge-bases/{kb_id}/sessions")
    if session_id:
        nx("另一租户 GetSessionMessages → 404", "GET",
           f"/knowledge-bases/{kb_id}/sessions/{session_id}/messages")
        nx("另一租户 DeleteSession → 404", "DELETE",
           f"/knowledge-bases/{kb_id}/sessions/{session_id}")
    else:
        check_not_verified("AC③ 另一租户 GetSessionMessages/DeleteSession → 404",
                           "STEP4 SSE 未产出 session_id，会话负向不可触达",
                           layer="真实依赖级")
    # Reparse（gRPC 之外的 REST 面）
    nx("另一租户 ReparseDocument → 404", "POST",
       f"/knowledge-bases/{kb_id}/documents/{doc_id}/reparse",
       body={"idempotency_key": str(uuid.uuid4())})
    # Audit
    nx("另一租户 ListAuditLogs → 404", "GET",
       f"/knowledge-bases/{kb_id}/audit-logs")

    # owner 对照（404 是租户过滤，不是资源缺失）
    status, _ = m066.rest_request(
        "GET", f"/knowledge-bases/{kb_id}", expected_status=200,
        label="owner GetKB(对照)")
    check("AC③ owner 对照 GetKB → 200（404 为租户过滤而非资源缺失）",
          status == 200, f"status={status}", layer="真实依赖级")

    # Operation 面（gateway 无 REST 路由 → gRPC 直连）
    ops_other = _grpc_list_operations(OTHER_TENANT_ID, resource_id=kb_id)
    check("AC③ 另一租户 gRPC ListOperations → 空集",
          len(ops_other) == 0, f"items={len(ops_other)}", layer="真实依赖级")
    if owner_op_id:
        code = _grpc_get_operation_code(OTHER_TENANT_ID, owner_op_id)
        check("AC③ 另一租户 gRPC GetOperation(持有者操作) → NOT_FOUND",
              code == "NOT_FOUND", f"code={code}", layer="真实依赖级")
    else:
        check_not_verified("AC③ 另一租户 gRPC GetOperation → NOT_FOUND",
                           "无持有者操作行可对照", layer="真实依赖级")
    ops_owner = _grpc_list_operations(TENANT_ID, resource_id=kb_id)
    check("AC③ owner 对照 gRPC ListOperations → 非空",
          len(ops_owner) >= 1, f"items={len(ops_owner)}", layer="真实依赖级")

    # 负向后 owner 资源完好（跨租户 DELETE/UPDATE 均被 404 拦截）
    st_doc = m066._run_async(m066._fetch_doc_state(doc_id))
    check("AC③ 负向后 owner 资源完好（doc 仍 ready）",
          bool(st_doc) and st_doc.get("parse_status") == "ready",
          f"doc={st_doc}", layer="真实依赖级")


def _reparse_once(kb_id: str, doc_id: str, tag: str) -> dict:
    """seed failed → gRPC reparse（显式幂等键）→ 等重跑终态 → 事件核对。

    返回 {"k": idempotency_key, "ref": AsyncTaskRef dict, "ok": bool}。
    """
    seeded = m066._run_async(_seed_doc_failed(doc_id))
    check(f"{tag} seed: 文档置 failed（触发 reparse 门）", seeded,
          f"doc={doc_id}", layer="真实依赖级")
    if not seeded:
        return {"k": "", "ref": {}, "ok": False}
    k = str(uuid.uuid4())
    try:
        ref = _grpc_reparse(kb_id, doc_id, k)
    except Exception as e:  # noqa: BLE001
        check(f"{tag} gRPC ReparseDocument → AsyncTaskRef", False, repr(e),
              layer="真实依赖级")
        return {"k": k, "ref": {}, "ok": False}
    check(f"{tag} gRPC ReparseDocument → AsyncTaskRef（run_id={ref.get('task_id')}）",
          bool(ref.get("task_id")), f"ref={ref}", layer="真实依赖级")
    if not ref.get("task_id"):
        return {"k": k, "ref": ref, "ok": False}
    # 先等消费方真正重置（离开 failed 种子态），再等终态 —— 提交前的旧 failed 态
    # 不能被误读为本轮重跑的终态。
    left = _wait_doc_leaves(doc_id, "failed", timeout=90)
    st = m066.wait_doc_ready(doc_id, timeout=300)
    ok = left and st.get("parse_status") == "ready"
    check(f"{tag} 重解析完成 → ready（run_id={ref.get('task_id')}）", ok,
          f"left={left} state={st}", layer="产品链路级")
    ev = m066._run_async(m066._fetch_outbox_event(doc_id, "kb.reparse"))
    # asyncpg 对 JSONB 默认返回 str（不自动解码）→ 统一 json.loads 后断言
    ev_payload = ev.get("payload") if ev else None
    if isinstance(ev_payload, str):
        try:
            ev_payload = json.loads(ev_payload)
        except json.JSONDecodeError:
            ev_payload = {}
    check(f"{tag} kb.reparse 事件已投递且 payload.task_id==run_id",
          bool(ev) and ev.get("published") is True
          and isinstance(ev_payload, dict)
          and ev_payload.get("task_id") == ref.get("task_id"),
          f"event={ev}", layer="真实依赖级")
    return {"k": k, "ref": ref, "ok": ok}


def _step6a_garbage_doc_reparse(kb_id: str) -> None:
    """AC②-0: 自然失败文档（garbage pdf）→ failed → reparse 可重跑。"""
    log("════ STEP6a: AC②-0 自然失败文档 → reparse 可重跑 ════")
    gb = (b"%PDF-1.7\n% KB-SPLIT-P4D garbage: not a real pdf body. "
          b"This must fail parsing.\n")
    fname = f"e2e067-broken-{E2E_TAG}.pdf"
    status, up = m066.rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents", body={
            "idempotency_key": str(uuid.uuid4()), "file_name": fname,
            "file_type": "pdf", "file_size_bytes": len(gb),
        }, expected_status=200, label="GetUploadURL(broken pdf)")
    doc_id = up.get("doc_id", "") if isinstance(up, dict) else ""
    upload_url = up.get("upload_url", "") if isinstance(up, dict) else ""
    if not (doc_id and upload_url):
        check("AC②-0 garbage pdf 上传链路", False, f"status={status} up={up}",
              layer="真实依赖级")
        return
    m066.http_put_bytes(upload_url, gb, "PUT presigned(broken pdf)")
    m066.rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents/{doc_id}/complete", body={
            "idempotency_key": str(uuid.uuid4()), "file_name": fname,
            "file_type": "pdf", "checksum_sha256": "", "custom_metadata": "",
        }, expected_status=202, label="Complete(broken pdf)")
    st = m066.wait_doc_ready(doc_id, timeout=180)
    check("AC②-0 garbage pdf → 解析自然失败（failed）",
          st.get("parse_status") == "failed", f"state={st}", layer="产品链路级")
    if st.get("parse_status") != "failed":
        return
    k0 = str(uuid.uuid4())
    try:
        ref0 = _grpc_reparse(kb_id, doc_id, k0)
    except Exception as e:  # noqa: BLE001
        check("AC②-0 failed 文档 reparse → AsyncTaskRef", False, repr(e),
              layer="真实依赖级")
        return
    check("AC②-0 failed 文档 reparse → AsyncTaskRef（新 run_id）",
          bool(ref0.get("task_id")), f"ref={ref0}", layer="真实依赖级")
    left = _wait_doc_leaves(doc_id, "failed", timeout=90)
    st0 = m066.wait_doc_ready(doc_id, timeout=180)
    check("AC②-0 reparse 重跑完成（解析必然再失败，但幂等键未阻塞重跑）",
          left and st0.get("parse_status") == "failed",
          f"left={left} state={st0}", layer="产品链路级")
    ops = m066._run_async(_fetch_operations_rows(TENANT_ID, "kb.reparse", doc_id))
    row0 = [r for r in ops if r["idempotency_key"] == k0]
    check("AC②-0 kb_operations kb.reparse 行闭环（终态 completed/failed）",
          bool(row0) and row0[0]["status"] in ("completed", "failed"),
          f"rows={row0}", layer="真实依赖级")
    ev0 = m066._run_async(m066._fetch_outbox_event(doc_id, "kb.reparse"))
    check("AC②-0 kb.reparse 事件已投递（published=TRUE）",
          bool(ev0) and ev0.get("published") is True, f"event={ev0}",
          layer="真实依赖级")


def _step7_md_reparse(kb_id: str, doc_id: str, stats0: dict,
                      ids0: list[str], vec0: int) -> dict:
    """AC②-1/2: md 文档两轮重解析 — 新 run_id + 旧块旧向量清理（DB + Milvus）。

    返回 {"run1":…, "run2":…, "subset": 基线向量↔chunk 关系子集名（供 AC⑤ 复用）}。
    """
    log("════ STEP7: AC②-1/2 md 文档重解析幂等 + 旧块旧向量清理 ════")
    subset0 = _vec_subset(stats0, vec0)
    check("AC②-1 基线向量↔chunk 关系可归纳（供清理不变量）",
          subset0 is not None,
          f"vec={vec0} chunks={stats0} subset={subset0}", layer="真实依赖级")

    run1 = _reparse_once(kb_id, doc_id, "AC②-1")
    if not run1["ok"]:
        return {"subset": subset0}
    stats1 = m066._run_async(m066._fetch_chunk_stats(kb_id, doc_id))
    ids1 = m066._run_async(_fetch_chunk_ids(kb_id, doc_id))
    vec1 = _milvus_doc_vector_count(kb_id, doc_id)
    left1 = set(ids0) & set(ids1)
    check("AC②-1 旧 chunk 行显式清理（新旧 chunk_id 零交集，全量新行）",
          len(ids1) > 0 and not left1,
          f"old={len(ids0)} new={len(ids1)} overlap={len(left1)}",
          layer="真实依赖级")
    if subset0 is not None:
        expected1 = _VEC_SUBSETS[subset0](stats1)
        check("AC②-1 旧向量显式清理（Milvus 向量数 == 重跑后预期，未叠加旧值）",
              vec1 == expected1 and vec1 > 0,
              f"subset={subset0} vec_before={vec0} vec_after={vec1} "
              f"expected={expected1} chunks={stats1}", layer="真实依赖级")
    else:
        check_not_verified(
            "AC②-1 旧向量显式清理（Milvus 向量数 == 重跑后预期）",
            "基线向量↔chunk 关系不可归纳，该不变量不可用；chunk 侧清理已由 "
            "chunk_id 集合断言承载", layer="真实依赖级")

    run2 = _reparse_once(kb_id, doc_id, "AC②-2")
    if not run2["ok"]:
        return {"run1": run1, "subset": subset0}
    check("AC②-2 第二次重解析生成新 run_id（task_id ≠ 上一轮）",
          run2["ref"].get("task_id") not in ("", run1["ref"].get("task_id")),
          f"T1={run1['ref'].get('task_id')} T2={run2['ref'].get('task_id')}",
          layer="真实依赖级")
    ids2 = m066._run_async(_fetch_chunk_ids(kb_id, doc_id))
    left2 = set(ids1) & set(ids2)
    check("AC②-2 连续两轮重跑均全新 chunk（parse-{doc}-{run}-b{n} 不撞）",
          len(ids2) > 0 and not left2,
          f"run1={len(ids1)} run2={len(ids2)} overlap={len(left2)}",
          layer="真实依赖级")
    ops = m066._run_async(_fetch_operations_rows(TENANT_ID, "kb.reparse", doc_id))
    keys = {r["idempotency_key"] for r in ops}
    check("AC②-2 kb_operations kb.reparse 恰 2 条（K1/K2 各一）",
          len(ops) == 2 and keys == {run1["k"], run2["k"]},
          f"keys={sorted(keys)}", layer="真实依赖级")
    return {"run1": run1, "run2": run2, "subset": subset0}


def _step8_reparse_replay(kb_id: str, doc_id: str,
                          vec_subset: str | None = None) -> None:
    """AC⑤: Reparse 同键幂等回放（pending 态 / completed 态）+ 副作用核对。"""
    log("════ STEP8: AC⑤ Reparse 同键幂等回放 ════")
    seeded = m066._run_async(_seed_doc_failed(doc_id))
    check("AC⑤ seed: md 文档置 failed（reparse 回放用）", seeded,
          layer="真实依赖级")
    if not seeded:
        return
    k3 = str(uuid.uuid4())
    try:
        ref3 = _grpc_reparse(kb_id, doc_id, k3)
    except Exception as e:  # noqa: BLE001
        check("AC⑤ reparse 首次提交 → AsyncTaskRef", False, repr(e),
              layer="真实依赖级")
        return
    check("AC⑤ reparse 首次提交 → AsyncTaskRef（run_id 待生成）",
          bool(ref3.get("task_id")), f"ref={ref3}", layer="真实依赖级")
    if not ref3.get("task_id"):
        return
    # pending 态同键重放 ×2
    ref3b = _grpc_reparse(kb_id, doc_id, k3)
    ref3c = _grpc_reparse(kb_id, doc_id, k3)
    check("AC⑤ 同键重放 ×2（pending 态）→ 同一 task_id，不重复执行",
          ref3b.get("task_id") == ref3.get("task_id")
          and ref3c.get("task_id") == ref3.get("task_id"),
          f"T3={ref3.get('task_id')} b={ref3b.get('task_id')} "
          f"c={ref3c.get('task_id')}", layer="真实依赖级")
    # 等首次提交的 run 完成
    _wait_doc_leaves(doc_id, "failed", timeout=90)
    st = m066.wait_doc_ready(doc_id, timeout=300)
    check("AC⑤ 首次提交的 run 正常完成 → ready", st.get("parse_status") == "ready",
          f"state={st}", layer="产品链路级")
    # completed 态同键再回放：回放判定先于 doc-ready 门（否则 FAILED_PRECONDITION）
    ref3d = _grpc_reparse(kb_id, doc_id, k3)
    check("AC⑤ 终态后同键再回放 → 仍同一 task_id（回放先于 ready 门）",
          ref3d.get("task_id") == ref3.get("task_id"),
          f"completed_replay={ref3d}", layer="真实依赖级")
    # 副作用核对：操作行 / outbox 事件 / 向量 均无重复
    ops = m066._run_async(_fetch_operations_rows(TENANT_ID, "kb.reparse", doc_id))
    k3_rows = [r for r in ops if r["idempotency_key"] == k3]
    check("AC⑤ 副作用核对: k3 操作行恰 1 条（重放未新增）",
          len(k3_rows) == 1, f"k3_rows={len(k3_rows)} all={len(ops)}",
          layer="真实依赖级")
    evs = m066._run_async(_fetch_outbox_events(doc_id, "kb.reparse"))
    check("AC⑤ 副作用核对: kb.reparse 事件数 == 操作行数（无重复发布）",
          len(evs) == len(ops), f"events={len(evs)} ops={len(ops)}",
          layer="真实依赖级")
    stats = m066._run_async(m066._fetch_chunk_stats(kb_id, doc_id))
    vec = _milvus_doc_vector_count(kb_id, doc_id)
    if vec_subset is not None:
        expected = _VEC_SUBSETS[vec_subset](stats)
        check("AC⑤ 副作用核对: 向量数符合基线不变量（无重复插入）",
              vec == expected and vec > 0,
              f"subset={vec_subset} vec={vec} chunks={stats}",
              layer="真实依赖级")
    else:
        check_not_verified("AC⑤ 副作用核对: 向量数符合基线不变量",
                           "向量↔chunk 关系不可归纳，由操作行/事件计数承载",
                           layer="真实依赖级")


def _step9_image_prefix_delete(kb_id: str) -> None:
    """AC④-3: 图片产物按 {kb}/{doc}/images/ 前缀批量删除（含对象/chunk/向量）。"""
    log("════ STEP9: AC④-3 图片产物按 {kb}/{doc}/images/ 前缀批量删除 ════")
    pdf_content = m066._doc_pdf_with_image_bytes()
    fname = f"e2e067-img-{E2E_TAG}.pdf"
    status, up = m066.rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents", body={
            "idempotency_key": str(uuid.uuid4()), "file_name": fname,
            "file_type": "pdf", "file_size_bytes": len(pdf_content),
        }, expected_status=200, label="GetUploadURL(img pdf)")
    doc_id = up.get("doc_id", "") if isinstance(up, dict) else ""
    upload_url = up.get("upload_url", "") if isinstance(up, dict) else ""
    if not (doc_id and upload_url):
        check("AC④-3 img pdf 上传链路", False, f"status={status} up={up}",
              layer="真实依赖级")
        return
    put = m066.http_put_bytes(upload_url, pdf_content, "PUT presigned(img pdf)")
    check("AC④-3 预签名 PUT(img pdf) → 200", put == 200, f"status={put}")
    m066.rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents/{doc_id}/complete", body={
            "idempotency_key": str(uuid.uuid4()), "file_name": fname,
            "file_type": "pdf", "checksum_sha256": "", "custom_metadata": "",
        }, expected_status=202, label="Complete(img pdf)")
    state = m066.wait_doc_ready(doc_id, timeout=300)
    check("AC④-3 img pdf 解析 ready", state.get("parse_status") == "ready",
          f"state={state}", layer="产品链路级")
    img_obj = m066._find_image_in_doc(kb_id, doc_id)
    check("AC④-3 图片产物入库（{kb}/{doc}/images/ 前缀存在）", bool(img_obj),
          f"image_object={img_obj}", layer="真实依赖级")
    vec_before = _milvus_doc_vector_count(kb_id, doc_id)
    if img_obj:
        status, urlresp = m066.rest_request(
            "GET", f"/knowledge-bases/{kb_id}/objects"
                   f"?object_id={urllib.parse.quote(img_obj)}",
            expected_status=200, label="GetObjectURL(image, 删除前对照)")
        img_url = urlresp.get("url", "") if isinstance(urlresp, dict) else ""
        gs, glen = (m066.http_get_bytes(img_url, "GET image bytes(删除前对照)")
                    if img_url else (-1, 0))
        check("AC④-3 删除前对照: 图片可取回（200 非空）",
              gs == 200 and glen > 0, f"status={gs} bytes={glen}",
              layer="真实依赖级")
    status, _ = m066.rest_request(
        "DELETE", f"/knowledge-bases/{kb_id}/documents/{doc_id}",
        expected_status=204, label="DeleteDocument(img pdf)")
    check("AC④-3 DeleteDocument → 204", status == 204, f"status={status}")
    img_after = m066._find_image_in_doc(kb_id, doc_id)
    check("AC④-3 图片按 {kb}/{doc}/images/ 前缀批量删除（无残留）",
          img_after is None, f"residual={img_after}", layer="真实依赖级")
    post_keys = _list_object_keys(f"{kb_id}/{doc_id}")
    check("AC④-3 原始对象一并删除（{kb}/{doc} 前缀下无对象残留）",
          not post_keys, f"residual_keys={post_keys[:3]}", layer="真实依赖级")
    stats = m066._run_async(m066._fetch_chunk_stats(kb_id, doc_id))
    check("AC④-3 chunk 行清理（DB 无残留）", not stats, f"chunks={stats}",
          layer="真实依赖级")
    vec_after = _milvus_doc_vector_count(kb_id, doc_id)
    check("AC④-3 向量清理（Milvus doc 向量归零）",
          vec_after == 0, f"before={vec_before} after={vec_after}",
          layer="真实依赖级")


def _degrade_redis_probe_067(kb_id: str) -> None:
    """AC①-1: Redis 挂 → 会话 DB-only；/readyz 仍 ok（session_cache 不门控）。"""
    log("── AC①-1: Redis 挂 → 会话 DB-only + /readyz 语义 ──")
    _kill_main_kb()  # 停主实例并等端口释放，防降级探针打到旧进程
    kb_env = m066._kb_service_env()
    kb_env["REDIS_URL"] = "redis://127.0.0.1:1/0"  # 不可达 → cache=None
    kb_env["GRPC_PORT"] = "50053"
    p = m066._start([sys.executable, "main.py"], kb_env, m066.KB_SVC,
                    "kb-service-noredis")
    try:
        if not (m066._wait_http("http://localhost:8002/health", 90)
                and m066._wait_port("localhost", GRPC_PORT, timeout=60)):
            check("AC①-1 Redis 挂 → 降级实例仍可启动", False,
                  "8002/50053 未就绪", layer="真实依赖级")
            return
        check("AC①-1 Redis 挂 → 降级实例仍可启动（8002 + gRPC 50053 就绪）",
              True, "Redis 不可达但服务正常降级启动", layer="真实依赖级")

        # /readyz 门控语义（#063）= db + outbox_dispatcher + grpc 三项；Redis 属
        # session_cache 只上报不门控。注意 _session_cache 是**构造态**上报
        # （main.py：aioredis.from_url 惰性连接不 ping → client 对象存在即
        # True），真实降级由运行期 kb_degradation_total 计数承载（下方断言），
        # 故此处只断言 status=ok + 三门控项，不对 session_cache 作 False 断言。
        st, rz = _readyz()
        comp = rz.get("components", {}) if isinstance(rz, dict) else {}
        check("AC①-1 Redis 挂 → /readyz status=ok（session_cache 不参与门控）",
              st == 200 and rz.get("status") == "ok"
              and comp.get("outbox_dispatcher") is True
              and comp.get("grpc") is True and comp.get("db") is True,
              f"status={st} readyz={rz}", layer="正式进程级")
        hst = _healthz_status()
        check("AC①-1 Redis 挂 → /health 仍 200（进程不因依赖降级而不健康）",
              hst == 200, f"status={hst}", layer="正式进程级")
        deps = rz.get("dependencies", {}) if isinstance(rz, dict) else {}
        check("AC①-1 dependencies.nats 仍 healthy（report-only，不门控）",
              deps.get("nats", {}).get("healthy") is True,
              f"nats={deps.get('nats')}", layer="正式进程级")

        # 真实 gRPC Query（rag-engine + Milvus + PG 全真实依赖）
        q_sess = str(uuid.uuid4())
        try:
            ans, it, ot, srcs, raw_metrics = m066._grpc_query_on_port(
                kb_id, "ANI 知识库平台支持哪三种检索模式？", GRPC_PORT,
                session_id=q_sess)
            q_ok = True
            q_detail = f"answer_len={len(ans or '')} tokens={it}+{ot} sources={len(srcs)}"
        except Exception as e:  # noqa: BLE001
            ans, it, ot, srcs, raw_metrics = "", 0, 0, [], ""
            q_ok, q_detail = False, repr(e)
        check("AC①-1 Redis 挂 → Query 仍成功返回（会话不中断）", q_ok,
              q_detail, layer="产品链路级")
        msgs = m066._run_async(m066._fetch_session_messages(q_sess))
        roles = [mm.get("role") for mm in msgs]
        check("AC①-1 Redis 挂 → 会话仍落 DB（DB-only 持久化）",
              "user" in roles and "assistant" in roles, f"roles={roles}",
              layer="真实依赖级")
        redis_present = m066._redis_session_key_exists(q_sess)
        check("AC①-1 Redis 挂 → 会话缓存未写（DB-only 对照）",
              not redis_present, f"redis_key_exists={redis_present}",
              layer="真实依赖级")
        deg = (m066._extract_metric(raw_metrics, "kb_degradation_total")
               if raw_metrics else [])
        session_deg = [d for d in deg
                       if "session_cache" in d and "redis_unavailable" in d]
        check("AC①-1 Redis 挂 → "
              "kb_degradation_total{session_cache,redis_unavailable}>0",
              bool(session_deg), f"samples={session_deg[:2]}",
              layer="真实依赖级")
    finally:
        m066._kill_proc(p)
        if p in m066._procs:
            m066._procs.remove(p)
        _wait_ports_closed()
        ok = m066._restart_main_kb(wait_ready=True)
        check("AC①-1 收尾: 主实例恢复（真实 Redis 可达，gRPC 就绪）", ok,
              layer="真实依赖级")


def _degrade_nats_probe_067(kb_id: str) -> None:
    """AC①-2: NATS 挂 → outbox 积压 + /readyz degraded；恢复后 at-least-once 追平。"""
    log("── AC①-2: NATS 挂 → outbox 积压 → 恢复后追平 ──")
    _kill_main_kb()  # 停主实例并等端口释放，防降级探针打到旧进程
    kb_env = m066._kb_service_env()
    kb_env["NATS_URL"] = "nats://127.0.0.1:1"  # 不可达 → dispatcher 不启动
    kb_env["GRPC_PORT"] = "50053"
    p = m066._start([sys.executable, "main.py"], kb_env, m066.KB_SVC,
                    "kb-service-nonats")
    backlog_doc = ""
    try:
        if not (m066._wait_http("http://localhost:8002/health", 90)
                and m066._wait_port("localhost", GRPC_PORT, timeout=60)):
            check("AC①-2 NATS 挂 → 降级实例仍可启动", False,
                  "8002/50053 未就绪", layer="真实依赖级")
            return
        check("AC①-2 NATS 挂 → 降级实例仍可启动（8002 + gRPC 50053 就绪）",
              True, "NATS 不可达但服务正常降级启动", layer="真实依赖级")

        st, rz = _readyz()
        comp = rz.get("components", {}) if isinstance(rz, dict) else {}
        check("AC①-2 NATS 挂 → /readyz status=degraded"
              "（outbox_dispatcher 参与门控）",
              st == 200 and rz.get("status") == "degraded"
              and comp.get("outbox_dispatcher") is False
              and comp.get("db") is True and comp.get("grpc") is True,
              f"status={st} readyz={rz}", layer="正式进程级")
        hst = _healthz_status()
        check("AC①-2 NATS 挂 → /health 仍 200（进程不因依赖降级而不健康）",
              hst == 200, f"status={hst}", layer="正式进程级")

        backlog_doc = m066._grpc_complete_doc_on_port(kb_id, GRPC_PORT)
        check("AC①-2 NATS 挂 → complete 成功写入 kb 自持 outbox",
              bool(backlog_doc), f"doc_id={backlog_doc}", layer="真实依赖级")
        if not backlog_doc:
            return
        time.sleep(6)
        ev = m066._run_async(m066._fetch_outbox_event(backlog_doc, "kb.parse"))
        check("AC①-2 NATS 挂 → outbox 事件积压（published=FALSE）",
              bool(ev) and ev.get("published") is False, f"event={ev}",
              layer="真实依赖级")
    finally:
        m066._kill_proc(p)
        if p in m066._procs:
            m066._procs.remove(p)
        _wait_ports_closed()

    # 恢复主实例（真实 NATS 可达）→ 同一 kb 自持 outbox 的 dispatcher 追平
    ok = m066._restart_main_kb(wait_ready=True)
    check("AC①-2 NATS 恢复 → 主实例重启（gRPC 就绪）", ok, layer="真实依赖级")
    if ok and backlog_doc:
        caught = False
        deadline = time.time() + 60
        while time.time() < deadline:
            ev2 = m066._run_async(
                m066._fetch_outbox_event(backlog_doc, "kb.parse"))
            if ev2 and ev2.get("published") is True:
                caught = True
                break
            time.sleep(2)
        check("AC①-2 NATS 恢复 → 积压事件投递追平"
              "（published=TRUE，at-least-once）",
              caught,
              f"event={m066._run_async(m066._fetch_outbox_event(backlog_doc, 'kb.parse'))}",
              layer="产品链路级")
        st2 = m066.wait_doc_ready(backlog_doc, timeout=300)
        check("AC①-2 NATS 恢复 → 积压文档解析 ready（事件不丢）",
              st2.get("parse_status") == "ready", f"state={st2}",
              layer="产品链路级")


def _step10_degrade_probes(kb_id: str) -> None:
    log("════ STEP10: AC① Redis/NATS 降级路径 + /readyz 语义 ════")
    _degrade_redis_probe_067(kb_id)
    _degrade_nats_probe_067(kb_id)


def _step11_presign_ttl(kb_id: str, doc_md: str) -> None:
    """AC④-1/2: 预签名时效 — MINIO_PRESIGN_TTL_SECONDS=2 → 过期 PUT/GET 拒绝。"""
    log("════ STEP11: AC④-1/2 预签名时效（TTL=2s → 过期拒绝） ════")
    ok = _restart_kb_with_env({"MINIO_PRESIGN_TTL_SECONDS": "2"})
    if not ok:
        check("AC④-1 TTL 实例重启", False, "kb-service 未就绪", layer="真实依赖级")
        m066._restart_main_kb(wait_ready=True)
        return
    try:
        # ① 上传 URL 过期：签发 → 等 4s（>TTL）→ PUT 被拒且对象未落地
        content = m066._doc_md_bytes()
        status, up = m066.rest_request(
            "POST", f"/knowledge-bases/{kb_id}/documents", body={
                "idempotency_key": str(uuid.uuid4()),
                "file_name": f"e2e067-ttl-{E2E_TAG}.md", "file_type": "md",
                "file_size_bytes": len(content),
            }, expected_status=200, label="GetUploadURL(TTL=2s)")
        up_doc = up.get("doc_id", "") if isinstance(up, dict) else ""
        upload_url = up.get("upload_url", "") if isinstance(up, dict) else ""
        if not (up_doc and upload_url):
            check("AC④-1 签发短 TTL 上传 URL", False, f"status={status}",
                  layer="真实依赖级")
            return
        log("  waiting 4s for presign expiry (TTL=2s) ...")
        time.sleep(4)
        put = m066.http_put_bytes(upload_url, content, "PUT expired upload url")
        # MinIO 对过期签名可能回 403，也可能直接重置连接（status=-1，传输层
        # 拒绝）；以「非 200 + 预定 key（{kb}/{doc} 无文件名后缀）无对象落地」
        # 双证据判定时效生效
        landed = _list_object_keys(f"{kb_id}/{up_doc}")
        check("AC④-1 过期上传 URL → PUT 拒绝且对象未落地（时效生效）",
              put != 200 and not landed,
              f"status={put} landed={landed}", layer="真实依赖级")

        # ② 取用 URL 过期：md 正本 key = {kb}/{doc}（无文件名后缀，ADR §D1）
        object_id = f"{m066.MINIO_BUCKET}/{kb_id}/{doc_md}"
        status, urlresp = m066.rest_request(
            "GET", f"/knowledge-bases/{kb_id}/objects"
                   f"?object_id={urllib.parse.quote(object_id)}",
            expected_status=200, label="GetObjectURL(TTL=2s)")
        get_url = urlresp.get("url", "") if isinstance(urlresp, dict) else ""
        if not get_url:
            check("AC④-2 签发短 TTL 取用 URL", False, f"status={status}",
                  layer="真实依赖级")
            return
        time.sleep(4)
        gs, _ = m066.http_get_bytes(get_url, "GET expired object url")
        check("AC④-2 过期取用 URL → GET 拒绝（时效生效）", gs != 200,
              f"status={gs}", layer="真实依赖级")
    finally:
        ok2 = m066._restart_main_kb(wait_ready=True)
        check("AC④ 收尾: 主实例恢复正常 TTL", ok2, layer="真实依赖级")


# ══════════════════════════════════════════════════════════════════════════════
# 主回归流程
# ══════════════════════════════════════════════════════════════════════════════
def run_regression() -> int:
    log("════ PRE: 独立环境探针 ════")
    pg = m066._run_async(m066._probe_db_tables())
    check("PRE kb 专属 PG 可达且无 RLS",
          pg.get("tables", 0) >= 9 and pg.get("any_rls") is False,
          f"tables={pg.get('tables')} any_rls={pg.get('any_rls')}",
          layer="真实依赖级")
    emb_st, emb_model, emb_dim = m066._probe_embedding()
    check("PRE 真实 embedding 依赖可达（qwen3-embedding-0.6b, dim=1024）",
          emb_st == 200 and emb_dim == 1024,
          f"status={emb_st} model={emb_model} dim={emb_dim}",
          layer="真实依赖级")

    kb_id = _step1_create_kb_and_replay()
    if not kb_id:
        return 1
    doc_md, stats0, ids0, vec0 = _step2_md_doc_baseline(kb_id)
    if not doc_md:
        return 1
    _step_object_url_guards(kb_id, doc_md)
    update_key = _step3_update_replay(kb_id)
    owner_ops = m066._run_async(
        _fetch_operations_rows(TENANT_ID, "kb.create", kb_id))
    owner_op_id = owner_ops[0]["id"] if owner_ops else ""
    sess_id = _step4_session(kb_id)
    _step5_cross_tenant_negative(kb_id, doc_md, sess_id, owner_op_id, update_key)
    _step6a_garbage_doc_reparse(kb_id)
    ac2 = _step7_md_reparse(kb_id, doc_md, stats0, ids0, vec0)
    _step8_reparse_replay(kb_id, doc_md,
                          ac2.get("subset") if isinstance(ac2, dict) else None)
    _step9_image_prefix_delete(kb_id)
    _step10_degrade_probes(kb_id)
    _step11_presign_ttl(kb_id, doc_md)

    log("════ CLEANUP ════")
    try:
        purge = m066._run_async(m066._purge_rows(kb_id))
        check("cleanup: kb rows purged", not purge.get("failures"),
              f"failures={purge.get('failures') or '{}'} "
              f"outbox_retained={purge.get('outbox_rows')}")
    except Exception as e:  # noqa: BLE001
        check("cleanup: kb rows purged", False, repr(e))
    status, _ = m066.rest_request(
        "DELETE", f"/knowledge-bases/{kb_id}", expected_status=204,
        label="cleanup DeleteKB",
        desc="清理: 软删知识库（既有稳定接口）")
    check("cleanup DeleteKB → 204", status == 204, f"status={status}")
    return 0


def main() -> int:
    m066._open_log()
    log("════════════════════════════════════════════════════════════════")
    log(f"  KB-SPLIT-P4D（#067）专项回归 — tag={E2E_TAG}")
    log(f"  tenant={TENANT_ID}  other={OTHER_TENANT_ID}")
    log(f"  log file: {m066.E2E_LOG}")
    log("════════════════════════════════════════════════════════════════")

    if not m066.GATEWAY_EXE.exists():
        log(f"FATAL: gateway exe not found at {m066.GATEWAY_EXE} — "
            f"run go build first")
        return 1

    log("── Killing stale processes on ports 8080/8001/8002/50052/50053 ──")
    m066._kill_stale_processes()

    log("── Starting embedding SSH tunnel (127.0.0.1:%d -> %s) ──"
        % (m066.EMBED_TUNNEL_PORT, m066.EMBED_CLUSTER_IP))
    if not m066.start_embed_tunnel():
        log("FATAL: embedding tunnel not up — cannot verify real embedding")
        return 1

    try:
        log("── Starting rag-engine (:8001 / gRPC :50052) ──")
        m066.start_rag_engine()
        if not m066._wait_http("http://localhost:8001/health", 90):
            log("FATAL: rag-engine not ready")
            return 1

        log("── Starting kb-service (:8002 / gRPC :50053) ──")
        m066.start_kb_service()
        if not m066._wait_http("http://localhost:8002/health", 90):
            log("WARNING: kb-service /health not ready — continuing")
        if not m066._wait_port("localhost", GRPC_PORT, timeout=60):
            log("FATAL: kb-service gRPC :50053 not reachable")
            return 1
        log("  kb-service gRPC :50053 ready")

        log("── Starting ani-gateway (:8080) ──")
        m066.start_gateway()
        if not m066._wait_http(
                "http://localhost:8080/api/v1/svc/knowledge-bases?limit=1", 60):
            log("FATAL: gateway not ready")
            return 1
        if not m066._wait_gateway_ready(60):
            log("FATAL: gateway→kb-service gRPC channel still 503 after warmup")
            return 1
        log("  gateway→kb-service gRPC channel warm")

        log("════ PRE-FLIGHT: ensure tenant rows ════")
        try:
            m066._run_async(m066._ensure_tenant_rows())
        except Exception as e:  # noqa: BLE001
            log(f"  ensure tenant rows failed (may already exist): {e!r}")

        rc = run_regression()

        n_pass = sum(1 for v in m066.RESULTS.values() if v == "pass")
        n_fail = sum(1 for v in m066.RESULTS.values() if v == "fail")
        n_nv = sum(1 for v in m066.RESULTS.values() if v == "not_verified")
        log("══════════════════ RESULT SUMMARY ══════════════════")
        log(f"  PASS: {n_pass}   FAIL: {n_fail}   NOT_VERIFIED: {n_nv}   "
            f"total: {len(m066.RESULTS)}")
        log("  证据层次（AC⑥）:")
        for layer in ("源码级", "真实依赖级", "正式进程级", "产品链路级", "部署级"):
            n = sum(1 for v in m066.LAYERS.values() if v == layer)
            if n:
                log(f"    {layer}: {n} 条")
        if n_fail:
            log("  FAILED checks:")
            for k, v in m066.RESULTS.items():
                if v == "fail":
                    log(f"    ✗ {k}")
        if n_nv:
            log("  NOT_VERIFIED checks:")
            for k, v in m066.RESULTS.items():
                if v == "not_verified":
                    log(f"    ⊘ {k}")
        log(f"  full log: {m066.E2E_LOG}")
        log("════════════════════════════════════════════════════════")
        return rc if rc else (0 if n_fail == 0 else 2)
    finally:
        m066._kill_all()
        m066._stop_embed_tunnel()
        log("── All services stopped ──")


if __name__ == "__main__":
    sys.exit(main())
