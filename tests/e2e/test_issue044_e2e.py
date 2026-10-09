"""
E2E test for issue-044 (B1) — Gateway UpdateKB + GetDocument ONLY.

Scope per issue-044 acceptance criteria — the two NEW B1 endpoints:
  PUT  /knowledge-bases/{kb_id}                     (updateKnowledgeBase)
  GET  /knowledge-bases/{kb_id}/documents/{doc_id}  (getKnowledgeBaseDocument)

UpdateKB scenarios:
  T1a  rename (name+description)        -> 200, fields persisted
  T1b  empty name/description           -> 200, no change (noop)
  T1c  same idempotency_key (replay)    -> 200, identical JSON to first call
  T1d  name collides with another KB    -> 409
  T1e  unknown kb_id                     -> 404
  T1f  missing idempotency_key          -> 400

GetDocument scenarios:
  T3a  existing doc                     -> 200, full field set + id match
  T3b  unknown doc_id                   -> 404

Preconditions (existing stable interfaces, NOT under test):
  - tenant row + kb-docs bucket
  - POST /knowledge-bases (CreateKB) -> 201 (x2: kb1 + collision target kb2)
  - 3-step md upload (text / pipe tables / image links) as GetDocument payload

Cleanup: DELETE /knowledge-bases/{kb_id} -> 204 (+ 404 verify)

Services (local only, never uploaded to the server):
  - ani-gateway (Go, :8080)      repo/bin/ani-gateway.exe
  - kb-service  (Python, :8002 / gRPC :50053)
  rag-engine is NOT started: async parse is disabled for this run and the
  endpoints under test do not depend on parse/embedding.

Infrastructure (server-deployed NodePorts on 10.10.1.66):
  PostgreSQL :30945 / MinIO :30900 / NATS :31062 / Redis :30453 / Milvus :31930

For every request the interface purpose (作用), input (INPUT) and
output (OUTPUT, with expected status) are printed BOTH to terminal and
tests/e2e/e2e_issue044_result.log.

Usage:
  python tests/e2e/test_issue044_e2e.py
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import time
import uuid
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
from typing import Any

# ── paths ────────────────────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parents[4]  # repo/
KB_SVC = REPO / "services" / "kb-service"
GATEWAY_EXE = REPO / "bin" / "ani-gateway.exe"
E2E_LOG = KB_SVC / "tests" / "e2e" / "e2e_issue044_result.log"

# ── server component endpoints (NodePorts on 10.10.1.66) ─────────────────────
SRV = "10.10.1.66"
PG_URL = f"postgres://ani_app_user:ani_dev_password@{SRV}:30945/ani?sslmode=disable"
MILVUS_ADDR = f"{SRV}:31930"
MINIO_ENDPOINT = f"{SRV}:30900"
MINIO_AK = "ani-s05-minio"
MINIO_SK = "F36UCbnRR-bY9Upv8uuammuBwkHFlTYABiXCbtMCmlc"
NATS_URL = f"nats://{SRV}:31062"
REDIS_URL = f"redis://:ani_dev_password@{SRV}:30453/0"

VLLM_MODEL = "Qwen3-235B-A22B"
VLLM_API_BASE = "http://10.10.20.181:3011/v1"
VLLM_API_KEY = "sk-YOp8k71BXjxBTeZniPPvQlbGgciH0CB9WOWXkmuCzjfIZ5R8"
EMBEDDING_MODEL = "Qwen3-Embedding-0.6B"

TENANT_ID = "00000000-0000-0000-0000-000000000002"
E2E_TAG = f"e2e044-{int(time.time())}"

# REST base via gateway
REST_BASE = "http://localhost:8080/api/v1/svc"
CORE_BASE = "http://localhost:8080/api/v1"
REST_HDRS = {"Content-Type": "application/json", "X-Dev-Tenant-ID": TENANT_ID}

# ── logging (terminal + file) ────────────────────────────────────────────────
_log_fh: io.TextIOBase | None = None


def _open_log():
    global _log_fh
    E2E_LOG.parent.mkdir(parents=True, exist_ok=True)
    _log_fh = open(E2E_LOG, "w", encoding="utf-8")


def log(msg: str = "", **kw):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}" if msg else ""
    print(line, flush=True, **kw)
    if _log_fh:
        _log_fh.write(line + "\n")
        _log_fh.flush()


def log_json(label: str, data):
    log(f"── {label} ──")
    try:
        text = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    except Exception:
        text = str(data)
    print(text, flush=True)
    if _log_fh:
        _log_fh.write(text + "\n")
        _log_fh.flush()


# ── process management ───────────────────────────────────────────────────────
_procs: list[subprocess.Popen] = []


def _start(args: list[str], env: dict, cwd: Path, name: str) -> subprocess.Popen:
    full_env = os.environ.copy()
    full_env.update(env)
    log(f"Starting {name}: {' '.join(args[:3])}... (cwd={cwd})")
    # Service stdout/stderr goes to a per-service file (avoids PIPE blocking)
    log_file = open(E2E_LOG.parent / f"{name}_issue044.stdout.log", "w",
                    encoding="utf-8")
    p = subprocess.Popen(
        args,
        env=full_env,
        cwd=str(cwd),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    _procs.append(p)
    log(f"  {name} started: pid={p.pid}")
    for key in sorted(env):
        if env.get(key) == os.environ.get(key):
            continue  # inherited unchanged from parent — not interesting
        if any(s in key for s in ("PASSWORD", "SECRET", "API_KEY", "AK", "SK")):
            continue
        log(f"    {name} env {key}={env[key]}")
    return p


def _wait_http(url: str, timeout: float = 60.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=3)
            log(f"  ready at {url}")
            return True
        except urllib.error.HTTPError:
            log(f"  ready (HTTP error response = server up) at {url}")
            return True
        except Exception:
            time.sleep(1.0)
    log(f"  NOT ready at {url} (timeout {timeout}s)")
    return False


def _kill_all():
    for p in reversed(_procs):
        try:
            p.terminate()
            p.wait(timeout=5)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass


def _kill_stale_processes():
    """Kill any previously-started local service processes on our ports."""
    names = ["ani-gateway", "kb-service", "rag-engine", "uvicorn"]
    for n in names:
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", f"{n}.exe"],
                capture_output=True, timeout=10,
            )
        except Exception:
            pass
    # kill python processes listening on our ports via netstat
    for port in (8001, 8002, 50052, 50053, 8080):
        try:
            out = subprocess.run(
                ["netstat", "-ano", "-p", "tcp"],
                capture_output=True, text=True, timeout=10,
            ).stdout
            for line in out.splitlines():
                if f":{port}" in line and "LISTENING" in line:
                    pid = line.strip().split()[-1]
                    subprocess.run(["taskkill", "/F", "/PID", pid],
                                   capture_output=True, timeout=10)
        except Exception:
            pass
    time.sleep(1.0)


# ── service environments ─────────────────────────────────────────────────────
def _gateway_env():
    env = os.environ.copy()
    env.update({
        "ANI_AUTH_MODE": "dev",
        "GATEWAY_LISTEN_ADDR": ":8080",
        "DATABASE_URL": PG_URL,
        "REDIS_URL": REDIS_URL,
        "OBJECT_STORE_PROVIDER": "minio",
        "OBJECT_STORE_ENDPOINT": MINIO_ENDPOINT,
        "OBJECT_STORE_PUBLIC_ENDPOINT": MINIO_ENDPOINT,
        "OBJECT_STORE_ACCESS_KEY_ID": MINIO_AK,
        "OBJECT_STORE_SECRET_ACCESS_KEY": MINIO_SK,
        "OBJECT_STORE_SECURE": "false",
        "OBJECT_STORE_BUCKET_PREFIX": "ani-s13-",
        "OBJECT_STORE_REGION": "us-east-1",
        "VECTOR_STORE_PROVIDER": "milvus",
        "VECTOR_STORE_ENDPOINT": f"http://{MILVUS_ADDR}",
        "VECTOR_STORE_COLLECTION_PREFIX": "ani_s13_",
        "NATS_URL": NATS_URL,
        "STORAGE_PROVIDER": "",
        "K8S_CLUSTER_PROVIDER_MODE": "",
        "GPU_INVENTORY_PROVIDER": "",
        "NETWORK_PROVIDER": "",
        "REGISTRY_PROVIDER_MODE": "",
        "INSTANCE_OBSERVABILITY_PROVIDER": "",
        # B1: gateway -> kb-service gRPC connection (REQUIRED for KB endpoints)
        "KB_SERVICE_GRPC_ADDR": "localhost:50053",
        "KB_SERVICE_GRPC_CALL_TIMEOUT": "30s",
        # SSE streaming: vLLM + rag-engine retrieval endpoints
        "RAG_ENGINE_URL": "http://localhost:8001",
        "VLLM_API_BASE": VLLM_API_BASE,
        "VLLM_API_KEY": VLLM_API_KEY,
        "VLLM_MODEL": VLLM_MODEL,
    })
    return env


def _kb_service_env():
    env = os.environ.copy()
    env.update({
        "DATABASE_URL": PG_URL,
        "NATS_URL": NATS_URL,
        # Unique subject: even with the consumer disabled below, never let the
        # server-side rag-engine consumer pick up our e2e messages.
        "NATS_PARSE_SUBJECT": f"ani.tasks.kb.parse.e2e044.{E2E_TAG}",
        "RAG_ENGINE_GRPC_ADDR": "localhost:50052",
        "GRPC_PORT": "50053",
        "REDIS_URL": REDIS_URL,
        # Parse pipeline is OUT OF SCOPE for this run (new endpoints don't
        # depend on it; embedding endpoint is known-down anyway).
        "KB_PARSE_CONSUMER_ENABLED": "false",
        "ANI_DEV_TENANT_ID": TENANT_ID,
    })
    return env


def start_gateway():
    _start([str(GATEWAY_EXE)], _gateway_env(),
           REPO / "services" / "ani-gateway", "gateway")


def start_kb_service():
    _start([sys.executable, "main.py"], _kb_service_env(), KB_SVC, "kb-service")


# ── REST helpers (input/output dual-printing) ─────────────────────────────────
def rest_request(method: str, path: str, body: dict | None = None,
                 expected_status: int | None = None, label: str = "",
                 timeout: float = 60.0, base: str | None = None,
                 headers: dict | None = None, desc: str = ""):
    """Send a REST request through the gateway; log the interface purpose
    (desc), input and output to terminal and log file.
    Returns (status_code, parsed_body)."""
    url = (base or REST_BASE) + path
    hdrs = dict(headers or REST_HDRS)
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    if desc:
        log(f"[{label or method}] 接口作用: {desc}")
    req = urllib.request.Request(url, data=data, method=method.upper(), headers=hdrs)
    log(f"[{label or method + ' ' + path}] INPUT: {method} {url}")
    if body is not None:
        log_json(f"[{label}] REQUEST BODY", body)
    status = -1
    payload: Any = None
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        status = resp.status
        raw = resp.read().decode("utf-8", errors="replace")
        if raw:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = raw
    except urllib.error.HTTPError as e:
        status = e.code
        raw = e.read().decode("utf-8", errors="replace") if e.fp else ""
        if raw:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = raw
    except Exception as e:
        log(f"[{label}] REQUEST ERROR: {e!r}")
        return -1, {"error": repr(e)}
    log(f"[{label}] OUTPUT: HTTP {status}")
    log_json(f"[{label}] RESPONSE BODY", payload)
    if expected_status is not None and status != expected_status:
        log(f"[{label}] ✗ UNEXPECTED STATUS: expected={expected_status} actual={status}")
    return status, payload


def _put_bytes(url: str, content: bytes, label: str, desc: str = "") -> bool:
    """PUT raw bytes to a presigned URL, dual-printing the outcome."""
    if desc:
        log(f"[{label}] 接口作用: {desc}")
    log(f"[{label}] INPUT: PUT {url} ({len(content)} bytes)")
    req = urllib.request.Request(
        url, data=content, method="PUT",
        headers={"Content-Type": "application/octet-stream"})
    try:
        resp = urllib.request.urlopen(req, timeout=120)
        log(f"[{label}] OUTPUT: HTTP {resp.status} (uploaded {len(content)} bytes)")
        return True
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace") if e.fp else ""
        log(f"[{label}] OUTPUT: HTTP {e.code} ERROR {detail[:500]}")
        return False
    except Exception as e:
        log(f"[{label}] OUTPUT: REQUEST ERROR {e!r}")
        return False


# ── pre-flight helpers ───────────────────────────────────────────────────────
def _ensure_tenant() -> bool:
    """Ensure the E2E tenant row exists in PG (tenants.id FK source).

    storage_buckets.tenant_id has an FK to tenants; previous E2E runs relied on
    tenant ...0002 already existing, but the shared dev DB may not have it.
    Idempotent: INSERT ... ON CONFLICT DO NOTHING.
    """
    import asyncpg
    import asyncio

    async def _q():
        conn = await asyncpg.connect(PG_URL)
        try:
            exists = await conn.fetchval(
                "SELECT id::text FROM tenants WHERE id=$1",
                uuid.UUID(TENANT_ID))
            if exists:
                log(f"  tenant {TENANT_ID} already exists")
                return True
            # plan_id FK -> tenant_plans; use the seeded entry plan
            await conn.execute(
                "INSERT INTO tenants (id, name, display_name, plan_id) "
                "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
                uuid.UUID(TENANT_ID), "e2e044-dev-tenant",
                "E2E 044 Dev Tenant",
                uuid.UUID("00000000-0000-0000-0000-000000000001"))
            log(f"  tenant {TENANT_ID} inserted (plan=entry)")
            return True
        finally:
            await conn.close()

    try:
        return asyncio.run(_q())
    except Exception as e:
        log(f"  ERROR: ensure tenant failed: {e}")
        return False


def _ensure_kb_docs_bucket() -> bool:
    """Create the 'kb-docs' bucket via gateway REST API if it doesn't exist."""
    try:
        status, data = rest_request(
            "GET", "/buckets?limit=100", label="list-buckets",
            base=CORE_BASE, timeout=10)
        if status == 200 and isinstance(data, dict):
            for item in data.get("items", []):
                if item.get("name") == "kb-docs":
                    log(f"  kb-docs bucket already exists (id={item.get('id')})")
                    return True
    except Exception as e:
        log(f"  Warning: list buckets failed: {e}")
    status, _ = rest_request(
        "POST", "/buckets", body={
            "name": "kb-docs",
            "access_mode": "private",
            "idempotency_key": f"kb-docs-{TENANT_ID}-{E2E_TAG}",
        }, expected_status=None, label="create-kb-docs-bucket",
        desc="前置: 创建 kb-docs bucket（文档上传的目标存储桶）",
        base=CORE_BASE, timeout=10)
    if status in (200, 201, 409):
        log(f"  kb-docs bucket available (HTTP {status})")
        return True
    log(f"  ERROR: create kb-docs bucket failed (HTTP {status})")
    return False


def _create_test_md() -> bytes:
    """Markdown precondition doc for GetDocument: text sections + pipe
    tables + image links (rich content; parse is out of scope for this run)."""
    md = f"""# ANI 知识库平台架构设计（E2E {E2E_TAG}）

## 一、系统架构概览

ANI 知识库平台采用三层服务架构：网关层（ani-gateway）、服务编排层（kb-service）和
AI 推理层（rag-engine）。网关层负责请求路由、认证鉴权和对象存储代理；服务编排层负责
知识库 CRUD 和文档解析管线编排；AI 推理层负责文档解析、向量嵌入和摘要生成等无状态
RPC 执行。所有写操作都支持幂等性控制，通过 idempotency_key 保证在网络抖动和服务
重启场景下的数据一致性。

| 层级 | 组件 | 职责 | 通信协议 |
|------|------|------|----------|
| 网关层 | ani-gateway | 路由、认证、存储代理 | HTTP/REST |
| 编排层 | kb-service | CRUD、解析编排、查询编排 | gRPC |
| 推理层 | rag-engine | Parse、Embed、Generate | gRPC |

## 二、文档解析管线详解

文档解析管线负责将用户上传的文档转换为结构化的文本块，并生成向量嵌入用于后续的
语义检索。管线流程包括：获取下载URL、调用Parse RPC、图片上传、摘要生成、向量嵌入、
向量插入、PG写入、状态更新。

| 步骤 | 操作 | 执行者 | 输出 |
|------|------|--------|------|
| 1 | 获取下载URL | kb-service | download_url |
| 2 | Parse RPC | rag-engine | ParsedChunk 列表 |
| 3 | 图片上传 | kb-service | image_url |
| 4 | 摘要生成 | rag-engine | doc_summary |

![三层架构图](https://example.com/diagrams/three-layer-arch.png)

## 三、混合检索策略

知识库平台支持三种检索模式：关键词检索（pg_trgm）、向量检索（Milvus HNSW）和混合
检索（RRF 融合）。检索结果经过去重和重排序后，取 top-k 个 chunk 作为 LLM 的上下文。

![检索策略对比图](https://example.com/diagrams/retrieval-strategies.png)
"""
    return md.encode("utf-8")


# ── check helpers ─────────────────────────────────────────────────────────────
RESULTS: dict[str, str] = {}  # label -> "pass" | "fail"


def check(label: str, ok: bool, detail: str = ""):
    RESULTS[label] = "pass" if ok else "fail"
    mark = "✓" if ok else "✗"
    log(f"  {mark} {label}" + (f" — {detail}" if detail else ""))


# ── run_e2e ───────────────────────────────────────────────────────────────────
def run_e2e():
    # ══ Pre-flight: tenant + kb-docs bucket ══
    log("════ PRE-FLIGHT: ensure tenant + kb-docs bucket ════")
    if not _ensure_tenant():
        log("FATAL: cannot ensure tenant row — abort")
        return 1
    if not _ensure_kb_docs_bucket():
        log("FATAL: cannot ensure kb-docs bucket — abort")
        return 1

    # ══ Precondition 1: CreateKB (kb1) -> 201 ══
    log("════ PRECONDITION: POST /knowledge-bases (CreateKB kb1 -> 201) ════")
    kb1_name = f"e2e044-kb1-{E2E_TAG}"
    status, kb1 = rest_request(
        "POST", "/knowledge-bases", body={
            "idempotency_key": f"idem-create-kb1-{E2E_TAG}",
            "name": kb1_name,
            "description": "E2E issue-044 KB1 (UpdateKB/GetDocument baseline)",
            "embedding_model": EMBEDDING_MODEL,
            "chunk_size": 512,
            "top_k": 5,
            "score_threshold": 0.5,
            "retrieval_mode": "hybrid",
        }, expected_status=201, label="CreateKB kb1",
        desc="前置: 创建知识库 kb1（UpdateKB/GetDocument 的测试基线），"
             "请求 Milvus 建 collection，返回完整 KB 对象")
    kb1_id = kb1.get("id", "") if isinstance(kb1, dict) else ""
    check("precondition CreateKB kb1 -> 201", status == 201, f"status={status}")
    check("precondition CreateKB kb1 returns id", bool(kb1_id),
          f"kb_id={kb1_id}")
    if not kb1_id:
        return 1

    # ══ T1: UpdateKB — the NEW endpoint #1 (all scenarios) ══
    log("════ T1: PUT /knowledge-bases/{kb_id} (UpdateKB — B1 new endpoint) ════")

    # T1a: rename name+description -> 200 + persisted
    upd_idem = f"idem-update-kb1-{E2E_TAG}"
    upd_body = {
        "idempotency_key": upd_idem,
        "name": f"e2e044-kb1-renamed-{E2E_TAG}",
        "description": "updated description (T1a rename)",
    }
    status, upd_first = rest_request(
        "PUT", f"/knowledge-bases/{kb1_id}", body=upd_body,
        expected_status=200, label="T1a UpdateKB rename",
        desc="【新增接口】UpdateKB: 修改知识库名称和描述，"
             "空字段不更新，返回更新后的完整 KB 对象")
    check("T1a UpdateKB rename -> 200", status == 200, f"status={status}")
    if status == 200 and isinstance(upd_first, dict):
        check("T1a name persisted", upd_first.get("name") == upd_body["name"],
              f"actual={upd_first.get('name')}")
        check("T1a description persisted",
              upd_first.get("description") == upd_body["description"])

    # T1b: empty name/description = no change -> 200
    status, payload = rest_request(
        "PUT", f"/knowledge-bases/{kb1_id}", body={
            "idempotency_key": f"idem-update-kb1-noop-{E2E_TAG}",
            "name": "",
            "description": "",
        }, expected_status=200, label="T1b UpdateKB empty-noop",
        desc="【新增接口】UpdateKB 空字段用例: name/description 传空串"
             "应视为不更新（noop），返回 200 且字段保持原值")
    check("T1b UpdateKB empty fields -> 200 (noop)", status == 200,
          f"status={status}")
    if status == 200 and isinstance(payload, dict):
        check("T1b name unchanged", payload.get("name") == upd_body["name"],
              f"actual={payload.get('name')}")
        check("T1b description unchanged",
              payload.get("description") == upd_body["description"])

    # T1c: idempotent replay — same idempotency_key returns recorded result
    status, replay = rest_request(
        "PUT", f"/knowledge-bases/{kb1_id}", body=upd_body,
        expected_status=200, label="T1c UpdateKB idempotent replay",
        desc="【新增接口】UpdateKB 幂等重放用例: 用与 T1a 相同的 "
             "idempotency_key 重发，应返回首次更新录制的相同 JSON 结果")
    check("T1c UpdateKB replay -> 200", status == 200, f"status={status}")
    if status == 200 and isinstance(replay, dict) and isinstance(upd_first, dict):
        check("T1c replay JSON identical to first update", replay == upd_first,
              "identical" if replay == upd_first else "diff")

    # T1d: name collision with another KB -> 409
    kb2_name = f"e2e044-kb2-{E2E_TAG}"
    status, kb2 = rest_request(
        "POST", "/knowledge-bases", body={
            "idempotency_key": f"idem-create-kb2-{E2E_TAG}",
            "name": kb2_name,
            "description": "E2E issue-044 KB2 (name collision target)",
        }, expected_status=201, label="T1d create KB2 (collision target)",
        desc="前置: 创建知识库 kb2（与 kb1 同租户，制造名称冲突场景）")
    kb2_id = kb2.get("id", "") if isinstance(kb2, dict) else ""
    check("T1d precondition KB2 created (201)", status == 201)
    status, payload = rest_request(
        "PUT", f"/knowledge-bases/{kb1_id}", body={
            "idempotency_key": f"idem-update-kb1-collide-{E2E_TAG}",
            "name": kb2_name,  # collide with KB2
        }, expected_status=409, label="T1d UpdateKB name collision",
        desc="【新增接口】UpdateKB 名称冲突用例: 将 kb1 改名为 kb2 已占用的"
             "名称，应返回 409 ALREADY_EXISTS 拒绝修改")
    check("T1d UpdateKB name collision -> 409", status == 409,
          f"status={status}")

    # T1e: unknown kb_id -> 404
    status, payload = rest_request(
        "PUT", f"/knowledge-bases/{uuid.uuid4()}", body={
            "idempotency_key": f"idem-update-unknown-{E2E_TAG}",
            "name": f"e2e044-ghost-{E2E_TAG}",
        }, expected_status=404, label="T1e UpdateKB unknown kb_id",
        desc="【新增接口】UpdateKB 未知识库用例: 对不存在的 kb_id 发起"
             "更新，应返回 404 NOT_FOUND")
    check("T1e UpdateKB unknown kb_id -> 404", status == 404,
          f"status={status}")

    # T1f: missing idempotency_key -> 400
    status, payload = rest_request(
        "PUT", f"/knowledge-bases/{kb1_id}", body={
            "name": f"e2e044-noidem-{E2E_TAG}",
        }, expected_status=400, label="T1f UpdateKB missing idempotency_key",
        desc="【新增接口】UpdateKB 缺参用例: 请求体缺少 idempotency_key，"
             "应返回 400 BAD_REQUEST")
    check("T1f UpdateKB missing idempotency_key -> 400", status == 400,
          f"status={status}")

    # ══ Precondition 2: 3-step doc upload (md) ══
    log("════ PRECONDITION: 3-step document upload (md) ════")
    md_name = f"e2e044-arch-{E2E_TAG}.md"
    md_bytes = _create_test_md()

    # step 1: request upload URL
    status, payload = rest_request(
        "POST", f"/knowledge-bases/{kb1_id}/documents", body={
            "idempotency_key": f"idem-doc-md-{E2E_TAG}",
            "file_name": md_name,
            "file_type": "md",
            "file_size_bytes": len(md_bytes),
        }, expected_status=200, label="upload step1 get-upload-url")
    doc_id = ""
    if status == 200 and isinstance(payload, dict):
        doc_id = payload.get("doc_id", "")
        upload_url = payload.get("upload_url", "")
        storage_path = payload.get("storage_path", "")
        check("upload step1 -> 200 with doc_id/upload_url/storage_path",
              bool(doc_id and upload_url and storage_path),
              f"doc_id={doc_id} storage_path={storage_path}")
    else:
        check("upload step1 -> 200", False, f"status={status}")

    doc_uploaded = False
    if doc_id:
        # step 2: PUT file bytes to presigned URL
        ok = _put_bytes(upload_url, md_bytes, "upload step2 PUT bytes",
                        desc="前置: 三步上传第2步 — 将 md 文件字节直传 "
                             "MinIO 预签名 URL（内容含文字段落/表格/图片链接）")
        check("upload step2 PUT -> 2xx", ok)
        # step 3: complete upload (async parse disabled for this run)
        status, payload = rest_request(
            "POST",
            f"/knowledge-bases/{kb1_id}/documents/{doc_id}/complete",
            body={
                "idempotency_key": f"idem-doc-md-complete-{E2E_TAG}",
                "file_name": md_name,
                "file_type": "md",
                "checksum_sha256": "",
                "custom_metadata": "",
            },
            expected_status=202, label="upload step3 complete",
            desc="前置: 三步上传第3步 — kb 校验对象已入库并登记文档，"
                 "投递解析任务（本运行关闭消费端），返回 202 + task_id",
            timeout=60)
        check("upload step3 complete -> 202", status == 202, f"status={status}")
        doc_uploaded = status == 202

    # ══ T3: GetDocument — the NEW endpoint #2 ══
    log("════ T3: GET /knowledge-bases/{kb_id}/documents/{doc_id} "
        "(GetDocument — B1 new endpoint) ════")
    check("T3 precondition: md doc uploaded", doc_uploaded)
    if doc_id:
        status, payload = rest_request(
            "GET", f"/knowledge-bases/{kb1_id}/documents/{doc_id}",
            expected_status=200, label="T3a GetDocument",
            desc="【新增接口】GetDocument: 查询单个文档详情，返回 "
                 "tenant_id/kb_id/id/file_name/parse_status 等 12 个字段")
        check("T3a GetDocument -> 200", status == 200, f"status={status}")
        if status == 200 and isinstance(payload, dict):
            for field in ("tenant_id", "kb_id", "id", "file_name", "file_type",
                          "file_size_bytes", "parse_status", "chunk_count",
                          "error_message", "custom_metadata", "created_at"):
                check(f"T3a response has field '{field}'", field in payload,
                      f"missing: {field}" if field not in payload else "ok")
            check("T3a doc id matches", payload.get("id") == doc_id)
            check("T3a kb_id matches", payload.get("kb_id") == kb1_id)
            check("T3a tenant_id matches", payload.get("tenant_id") == TENANT_ID)
            check("T3a file_name matches", payload.get("file_name") == md_name)
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb1_id}/documents/{uuid.uuid4()}",
        expected_status=404, label="T3b GetDocument unknown doc_id",
        desc="【新增接口】GetDocument 未文档用例: 查询不存在的 doc_id，"
             "应返回 404 NOT_FOUND")
    check("T3b GetDocument unknown doc_id -> 404", status == 404,
          f"status={status}")

    # ══ Cleanup: DeleteKB ══
    log("════ CLEANUP: DELETE /knowledge-bases/{kb_id} ════")
    status, _ = rest_request(
        "DELETE", f"/knowledge-bases/{kb2_id}", expected_status=204,
        label="cleanup DeleteKB kb2",
        desc="清理: 删除知识库 kb2（软删，验证清理不残留）")
    check("cleanup DeleteKB kb2 -> 204", status == 204, f"status={status}")
    status, _ = rest_request(
        "DELETE", f"/knowledge-bases/{kb1_id}", expected_status=204,
        label="cleanup DeleteKB kb1 (with doc)",
        desc="清理: 删除知识库 kb1（含已上传文档）")
    check("cleanup DeleteKB kb1 -> 204", status == 204, f"status={status}")
    status, _ = rest_request(
        "GET", f"/knowledge-bases/{kb1_id}", expected_status=404,
        label="cleanup verify kb1 deleted",
        desc="清理验证: 再查 kb1 应 404，确认删除生效")
    check("cleanup verify kb1 deleted -> 404", status == 404, f"status={status}")

    return 0


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    _open_log()
    log("════════════════════════════════════════════════════════════════")
    log(f"  E2E issue-044 (B1) UpdateKB+GetDocument — tag={E2E_TAG}")
    log(f"  tenant={TENANT_ID}")
    log(f"  log file: {E2E_LOG}")
    log("════════════════════════════════════════════════════════════════")

    if not GATEWAY_EXE.exists():
        log(f"FATAL: gateway exe not found at {GATEWAY_EXE} — run go build first")
        return 1

    # 0. cleanup stale processes
    log("── Killing stale processes on ports 8080/8001/8002/50052/50053 ──")
    _kill_stale_processes()

    try:
        # 1. start gateway
        log("── Starting ani-gateway (:8080) ──")
        start_gateway()
        if not _wait_http("http://localhost:8080/api/v1/svc/knowledge-bases?limit=1", 60):
            log("FATAL: gateway not ready")
            return 1

        # dev-mode sanity: with X-Dev-Tenant-ID header the route responds
        # (404/500/503 all prove the KB router is mounted — 503 means the
        # gateway tried to proxy to kb-service which isn't started yet)
        status, payload = rest_request(
            "GET", "/knowledge-bases?limit=1", label="dev-mode sanity check",
            desc="启动自检: 验证网关 KB 路由已挂载（kb-service 未起时 503 "
                 "也证明路由存在）")
        check("dev sanity: KB route mounted on gateway",
              status in (200, 404, 500, 503) and status != -1, f"status={status}")
        if status == -1:
            return 1

        # 2. start kb-service (FastAPI :8002 / gRPC :50053, parse consumer OFF)
        log("── Starting kb-service (:8002 / gRPC :50053, parse_consumer=OFF) ──")
        start_kb_service()
        if not _wait_http("http://localhost:8002/health", 90):
            if not _wait_http("http://localhost:8002/", 15):
                log("WARNING: kb-service health check failed — continuing "
                    "(gRPC may still be up)")

        # wait for kb-service gRPC port to accept connections
        import socket
        ok = False
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                with socket.create_connection(("localhost", 50053), timeout=2):
                    ok = True
                    break
            except Exception:
                time.sleep(1)
        log(f"  kb-service gRPC :50053 {'ready' if ok else 'NOT reachable'}")
        if not ok:
            log("FATAL: kb-service gRPC not reachable on :50053")
            return 1

        # warm up the gateway→kb-service gRPC channel: the gateway dials
        # non-blockingly (grpc.NewClient); the sanity check above hit it
        # while kb-service was still down, putting the channel into
        # TRANSIENT_FAILURE with exponential backoff (1s→1.6s→2.56s...).
        # Poll ListKBs until the channel reconnects (no longer 503),
        # otherwise the first real RPC fails with UNAVAILABLE.
        deadline = time.time() + 30
        warmed = False
        while time.time() < deadline:
            status, _ = rest_request(
                "GET", "/knowledge-bases?limit=1", label="grpc-channel warmup",
                desc="通道预热: 轮询直到网关→kb-service gRPC 通道退出退避"
                     "（非 503 即就绪）", timeout=10)
            if status != 503:
                warmed = True
                break
            time.sleep(2)
        if not warmed:
            log("FATAL: gateway→kb-service gRPC channel still 503 after 30s")
            return 1
        log("  gateway→kb-service gRPC channel warm")

        # 3. run E2E
        rc = run_e2e()

        # 4. summary
        n_pass = sum(1 for v in RESULTS.values() if v == "pass")
        n_fail = sum(1 for v in RESULTS.values() if v == "fail")
        log("════════════════════════ RESULT SUMMARY ══════════════════════════")
        log(f"  PASS: {n_pass}   FAIL: {n_fail}")
        log(f"  total checks: {len(RESULTS)}")
        if n_fail:
            log("  FAILED checks:")
            for k, v in RESULTS.items():
                if v == "fail":
                    log(f"    ✗ {k}")
        log(f"  full log: {E2E_LOG}")
        log("════════════════════════════════════════════════════════════════")
        return rc if rc else (0 if n_fail == 0 else 2)
    finally:
        _kill_all()
        log("── All services stopped ──")


if __name__ == "__main__":
    sys.exit(main())
if __name__ == "__main__":
    sys.exit(main())
    sys.exit(main())
