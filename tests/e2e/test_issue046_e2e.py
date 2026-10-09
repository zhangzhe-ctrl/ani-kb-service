"""
E2E test for issue-046 (B2) — Gateway KB chunks / sessions / citations endpoints.

Scope per issue-046 acceptance criteria — the five NEW B2 endpoints:
  GET    /knowledge-bases/{kb_id}/documents/{doc_id}/chunks   (ListDocumentChunks)
  GET    /knowledge-bases/{kb_id}/sessions                    (ListKBSessions)
  GET    /knowledge-bases/{kb_id}/sessions/{sid}/messages     (ListSessionMessages)
  DELETE /knowledge-bases/{kb_id}/sessions/{sid}              (DeleteSession)
  GET    /knowledge-bases/{kb_id}/citations                    (ListKBCitations)

T2 ListDocumentChunks:
  T2a default page   -> 200, 7 items, id ASC, full field set
  T2b chunk_type=child  -> 200, 4 items
  T2c chunk_type=doc_summary -> 200, 1 item
  T2d pagination limit=3 -> 3+3+1, union == all, no dup
  T2e limit=101        -> 400
  T2f unknown doc      -> 404
  T2g soft-deleted doc -> 404
  T2h unknown kb       -> 404
  T2i invalid chunk_type -> 400
  T2j invalid cursor   -> 400
  T2k cross-tenant     -> 404

T3 ListKBSessions:
  T3a default page + aggregates (message_count/last_query/last_active_at)
  T3b composite-cursor pagination (limit=2)
  T3c limit=101 -> 400
  T3d unknown kb -> 404

T4 ListSessionMessages:
  T4a ASC order + role/content/sources semantics (user -> sources null)
  T4b composite-cursor pagination
  T4c limit=101 -> 400
  T4d cross-KB session access (kb1_id + kb2 session) -> 404
  T4e unknown session -> 404
  T4f invalid cursor -> 400

T5 ListKBCitations:
  T5a citations expanded from both assistant messages, best score per doc
  T5b deterministic uuid5 citation ids (two calls identical)
  T5c message_id / session_id passthrough
  T5d limit=1 message-grain pagination
  T5e unknown kb -> 404

T6 DeleteSession:
  T6a delete kb1 s1 (has messages) -> 204
  T6b idempotent replay -> 204
  T6c messages after delete -> 404
  T6d cross-KB negative (kb1_id + kb2 session) -> 204 but kb2 session intact
  T6e unknown kb -> 404
  T6f unknown session -> 204 (idempotent)
  T6g DB verify: s1 messages purged

Seed data goes DIRECTLY to PG via production repository functions (asyncpg,
RLS-scoped) — the five endpoints under test are read/delete paths; the async
parse pipeline (rag-engine) is intentionally disabled for this run.

Services (local only, never uploaded to the server):
  - ani-gateway (Go, :8080)      repo/bin/ani-gateway.exe
  - kb-service  (Python, :8002 / gRPC :50053)
  rag-engine is NOT started (parse consumer disabled, not on B2 paths).

Infrastructure (server-deployed NodePorts on 10.10.1.66):
  PostgreSQL :30945 / MinIO :30900 / NATS :31062 / Redis :30453 / Milvus :31930

Usage:
  python tests/e2e/test_issue046_e2e.py
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
E2E_LOG = KB_SVC / "tests" / "e2e" / "e2e_issue046_result.log"

# make app.repositories (production seed helpers) importable
sys.path.insert(0, str(KB_SVC))

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
VLLM_API_KEY = "sk-YOp8k71BXjxBTeZniPPvQlbGgciH0CB9WOWXkmuCzjfIZ5L8"
EMBEDDING_MODEL = "Qwen3-Embedding-0.6B"

TENANT_ID = "00000000-0000-0000-0000-000000000002"
OTHER_TENANT_ID = "00000000-0000-0000-0000-000000000003"  # cross-tenant probe
E2E_TAG = f"e2e046-{int(time.time())}"

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
    log_file = open(E2E_LOG.parent / f"{name}_issue046.stdout.log", "w",
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
        "KB_SERVICE_GRPC_ADDR": "localhost:50053",
        "KB_SERVICE_GRPC_CALL_TIMEOUT": "30s",
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
        "NATS_PARSE_SUBJECT": f"ani.tasks.kb.parse.e2e046.{E2E_TAG}",
        "RAG_ENGINE_GRPC_ADDR": "localhost:50052",
        "GRPC_PORT": "50053",
        "REDIS_URL": REDIS_URL,
        "KB_PARSE_CONSUMER_ENABLED": "false",
        "ANI_DEV_TENANT_ID": TENANT_ID,
    })
    return env


def start_gateway():
    _start([str(GATEWAY_EXE)], _gateway_env(),
           REPO / "services" / "ani-gateway", "gateway")


def start_kb_service():
    _start([sys.executable, "main.py"], _kb_service_env(), KB_SVC, "kb-service")


# ── REST helpers ──────────────────────────────────────────────────────────────
def rest_request(method: str, path: str, body: dict | None = None,
                 expected_status: int | None = None, label: str = "",
                 timeout: float = 60.0, base: str | None = None,
                 headers: dict | None = None, desc: str = ""):
    """Send a REST request through the gateway; log the interface purpose
    (desc), input and output. Returns (status_code, parsed_body)."""
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


# ── check helpers ─────────────────────────────────────────────────────────────
RESULTS: dict[str, str] = {}  # label -> "pass" | "fail"


def check(label: str, ok: bool, detail: str = ""):
    RESULTS[label] = "pass" if ok else "fail"
    mark = "✓" if ok else "✗"
    log(f"  {mark} {label}" + (f" — {detail}" if detail else ""))


def _items(payload: Any) -> list:
    return payload.get("items", []) if isinstance(payload, dict) else []


# ── seed helpers (production repositories, RLS-scoped) ────────────────────────
def _run_async(coro) -> Any:
    import asyncio
    return asyncio.run(coro)


def _seed_summary() -> dict:
    return {"chunk_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{E2E_TAG}:sum1")),
            "content": "SUMMARY 本文描述 ANI 三层架构与解析管线。",
            "content_type": "summary", "page_number": 1, "token_count": 20}


def _a1_sources(doc1_id: str, doc_x_id: str) -> list[dict]:
    """sources for assistant msg a1: doc1 twice (0.92 / 0.71) + doc_x once."""
    return [
        {"doc_id": doc1_id, "chunk_id": f"{E2E_TAG}-c1",
         "file_name": "arch.md", "page": 1, "content": "命中片段1",
         "score": 0.92},
        {"doc_id": doc_x_id, "chunk_id": f"{E2E_TAG}-cx",
         "file_name": "extra.md", "page": 3, "content": "命中片段X",
         "score": 0.55},
        {"doc_id": doc1_id, "chunk_id": f"{E2E_TAG}-c2",
         "file_name": "arch.md", "page": 1, "content": "命中片段2",
         "score": 0.71},
    ]


def _a2_sources(doc1_id: str, doc2_id: str) -> list[dict]:
    """sources for a2: doc1 0.42 FIRST then 0.90 (best-score must win),
    plus doc2 0.66."""
    return [
        {"doc_id": doc1_id, "chunk_id": f"{E2E_TAG}-c3",
         "file_name": "arch.md", "page": 2, "content": "命中片段3",
         "score": 0.42},
        {"doc_id": doc2_id, "chunk_id": f"{E2E_TAG}-d1",
         "file_name": "deploy.md", "page": 1, "content": "命中片段D1",
         "score": 0.66},
        {"doc_id": doc1_id, "chunk_id": f"{E2E_TAG}-c4",
         "file_name": "arch.md", "page": 2, "content": "命中片段4",
         "score": 0.90},
    ]


async def _seed_parents() -> list[dict]:
    """Two parent chunks with deterministic ids."""
    return [
        {"chunk_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{E2E_TAG}:p1")),
         "content": "PARENT-1 ANI 网关层负责路由、认证与对象存储代理。",
         "content_type": "text", "page_number": 1, "token_count": 42},
        {"chunk_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{E2E_TAG}:p2")),
         "content": "PARENT-2 编排层负责知识库 CRUD 与解析管线编排。",
         "content_type": "text", "page_number": 2, "token_count": 40},
    ]


async def _seed_children(parents: list[dict]) -> list[dict]:
    p1, p2 = parents[0]["chunk_id"], parents[1]["chunk_id"]
    return [
        {"chunk_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{E2E_TAG}:c1")),
         "content": "CHILD-1 网关采用 Hertz 框架，监听 8080。",
         "content_type": "text", "page_number": 1, "parent_chunk_id": p1,
         "parent_content": parents[0]["content"], "token_count": 30,
         "metadata": {"heading": "网关", "level": 2}},
        {"chunk_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{E2E_TAG}:c2")),
         "content": "CHILD-2 网关将 KB 请求通过 gRPC 转发给 kb-service。",
         "content_type": "text", "page_number": 1, "parent_chunk_id": p1,
         "parent_content": parents[0]["content"], "token_count": 31},
        {"chunk_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{E2E_TAG}:c3")),
         "content": "CHILD-3 kb-service 提供 FastAPI :8002 与 gRPC :50053。",
         "content_type": "text", "page_number": 2, "parent_chunk_id": p2,
         "parent_content": parents[1]["content"], "token_count": 32},
        {"chunk_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{E2E_TAG}:c4")),
         "content": "CHILD-4 编排层将解析任务投递到 NATS。",
         "content_type": "text", "page_number": 2, "parent_chunk_id": p2,
         "parent_content": parents[1]["content"], "token_count": 33},
    ]


async def _seed(kb1_id: str, kb2_id: str) -> dict:
    """Seed all test rows for kb1/kb2 using production repositories.

    Layout (each write opens its own transaction; 0.05s sleeps between rows
    because PG now() returns the TRANSACTION start time — same-tx rows share
    created_at, which would destabilize (created_at, id) cursors):

    kb1.documents:
      doc1  parse_status='ready'  — the chunk host (7 chunks)
      docX  parse_status='ready'  — citations-only doc (no chunks)
      docSD parse_status='failed' error_message='deleted' — soft-deleted
    kb1.sessions (created in order s1..s4, DESC list => s4,s3,s2,s1):
      s1: u1 -> a1(sources a1) -> u2     (T4 order + T6 delete target)
      s2: u_only                          (no assistant msg)
      s3: (empty)                         (aggregate with 0 messages)
      s4: u3 -> a2(sources a2)            (2nd citations source)
    kb2.sessions:
      kb2_s1: kb2_u -> kb2_a(no sources)  (cross-KB delete negative)
    """
    import asyncpg
    from app.repositories import document as doc_repo
    from app.repositories import message as msg_repo
    from app.repositories import chunk as chunk_repo

    conn = await asyncpg.connect(PG_URL)
    info: dict[str, str] = {}
    try:
        # ---- kb_documents rows (direct SQL: allows parse_status override) ----
        async def _insert_doc(kb_id: str, name: str, status: str,
                              error: str | None) -> str:
            async with conn.transaction():
                # SET LOCAL cannot use $1 binding (see rls.py) — inline the
                # validated UUID literal instead.
                await conn.execute(
                    f"SET LOCAL app.current_tenant_id = '{TENANT_ID}'")
                row = await conn.fetchrow(
                    """
                    INSERT INTO kb_documents
                        (kb_id, tenant_id, file_name, file_type,
                         file_size_bytes, storage_path, checksum_sha256,
                         parse_status, error_message)
                    VALUES ($1, $2, $3, 'md', 100, $4, $5, $6, $7)
                    RETURNING id::text
                    """,
                    uuid.UUID(kb_id), uuid.UUID(TENANT_ID), name,
                    f"{E2E_TAG}/{name}", "0" * 64, status, error)
                return row["id"]

        info["doc1"] = await _insert_doc(kb1_id, f"arch-{E2E_TAG}.md",
                                         "ready", None)
        time.sleep(0.05)
        info["docX"] = await _insert_doc(kb1_id, f"extra-{E2E_TAG}.md",
                                         "ready", None)
        time.sleep(0.05)
        info["docSD"] = await _insert_doc(kb1_id, f"softdel-{E2E_TAG}.md",
                                          "failed", "deleted")
        time.sleep(0.05)
        info["doc2"] = await _insert_doc(kb2_id, f"deploy-{E2E_TAG}.md",
                                         "ready", None)

        # ---- kb_chunks: 2 parent + 4 child + 1 doc_summary on doc1 ----
        parents = await _seed_parents()
        children = await _seed_children(parents)
        summary = _seed_summary()
        n = await chunk_repo.write_chunks(
            conn, tenant_id=TENANT_ID, kb_id=kb1_id, doc_id=info["doc1"],
            file_name=f"arch-{E2E_TAG}.md",
            parents=parents, children=children, summaries=[summary])
        info["n_chunks"] = str(n)
        time.sleep(0.05)

        # ---- kb_sessions + kb_messages (kb1) ----
        async def _mk_session(kb_id: str, title: str) -> str:
            sid = await msg_repo.create_session(
                conn, tenant_id=TENANT_ID, kb_id=kb_id, title=title)
            time.sleep(0.05)  # distinct created_at per session
            return sid

        async def _msg(sid: str, role: str, content: str,
                       sources: list | None = None) -> str:
            m = await msg_repo.insert_message(
                conn, tenant_id=TENANT_ID, session_id=sid, role=role,
                content=content, source_chunks=sources)
            time.sleep(0.05)  # distinct created_at per message
            return str(m["id"])

        info["s1"] = await _mk_session(kb1_id, "s1-with-sources")
        info["u1"] = await _msg(info["s1"], "user", "u1: 网关层的职责是什么？")
        info["a1"] = await _msg(info["s1"], "assistant", "a1: 网关负责路由、认证与对象存储代理。",
                                _a1_sources(info["doc1"], info["docX"]))
        info["u2"] = await _msg(info["s1"], "user", "u2: 继续说明解析管线。")
        info["s2"] = await _mk_session(kb1_id, "s2-user-only")
        info["u_only"] = await _msg(info["s2"], "user", "u_only: 只有用户消息。")
        info["s3"] = await _mk_session(kb1_id, "s3-empty")
        info["s4"] = await _mk_session(kb1_id, "s4-second-assistant")
        info["u3"] = await _msg(info["s4"], "user", "u3: 解析管线的流程？")
        info["a2"] = await _msg(info["s4"], "assistant", "a2: 解析管线依次执行 Parse、摘要、嵌入与写入。",
                                _a2_sources(info["doc1"], info["doc2"]))

        # ---- kb2 (cross-KB negative target) ----
        info["kb2_s1"] = await _mk_session(kb2_id, "kb2-session")
        info["kb2_u"] = await _msg(info["kb2_s1"], "user", "kb2_u: kb2 的提问。")
        info["kb2_a"] = await _msg(info["kb2_s1"], "assistant", "kb2_a: kb2 的回答（无 sources）。")
        return info
    finally:
        await conn.close()


async def _count_session_messages(session_id: str) -> int:
    """Count messages of a session from the DB (RLS tenant context required).

    kb_messages is RLS-scoped: without SET LOCAL app.current_tenant_id the
    row-level policy filters every row and COUNT returns 0 even though the
    messages exist — the earlier T6d "kb2 session messages intact" failure
    and the T6g false pass were both caused by this missing context.
    """
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
        async with conn.transaction():
            # SET LOCAL does not support $1 binding — inline the validated
            # UUID literal (same pattern as app/repositories/rls.py).
            await conn.execute(
                f"SET LOCAL app.current_tenant_id = '{TENANT_ID}'")
            n = await conn.fetchval(
                "SELECT COUNT(*) FROM kb_messages WHERE session_id=$1",
                uuid.UUID(session_id))
            return int(n)
    finally:
        await conn.close()


async def _purge_seeded_rows(kb_ids: list[str]):
    """Physically purge seeded rows (DeleteKB is a SOFT delete: rows stay for
    audit; pure e2e test data must not linger in the shared dev DB).

    kb_messages -> kb_sessions -> kb_chunks -> kb_documents all cascade on
    kb_id/session_id, but we delete explicitly in FK-safe order.
    """
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
        for kb_id in kb_ids:
            async with conn.transaction():
                await conn.execute(
                    f"SET LOCAL app.current_tenant_id = '{TENANT_ID}'")
                await conn.execute(
                    "DELETE FROM kb_messages WHERE session_id IN "
                    "(SELECT id FROM kb_sessions WHERE kb_id=$1)",
                    uuid.UUID(kb_id))
                await conn.execute(
                    "DELETE FROM kb_sessions WHERE kb_id=$1", uuid.UUID(kb_id))
                await conn.execute(
                    "DELETE FROM kb_chunks WHERE kb_id=$1", uuid.UUID(kb_id))
                await conn.execute(
                    "DELETE FROM kb_documents WHERE kb_id=$1", uuid.UUID(kb_id))
                n = await conn.fetchval(
                    "SELECT COUNT(*) FROM kb_chunks WHERE kb_id=$1",
                    uuid.UUID(kb_id))
                log(f"  purged rows for kb {kb_id} (remaining chunks: {n})")
    finally:
        await conn.close()


async def _ensure_tenant_rows():
    """Ensure tenant rows for TENANT_ID and OTHER_TENANT_ID exist (FK source)."""
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
        for tid, name in ((TENANT_ID, "e2e046-dev-tenant"),
                          (OTHER_TENANT_ID, "e2e046-other-tenant")):
            await conn.execute(
                "INSERT INTO tenants (id, name, display_name, plan_id) "
                "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
                uuid.UUID(tid), name, name,
                uuid.UUID("00000000-0000-0000-0000-000000000001"))
        log(f"  tenants ensured: {TENANT_ID} / {OTHER_TENANT_ID}")
    finally:
        await conn.close()


# ── run_e2e ───────────────────────────────────────────────────────────────────
def run_e2e():
    # ══ Pre-flight: tenant rows ══
    log("════ PRE-FLIGHT: ensure tenant rows ════")
    _run_async(_ensure_tenant_rows())

    # ══ Precondition 1: CreateKB kb1 + kb2 (stable interface) ══
    log("════ PRECONDITION: CreateKB kb1 + kb2 ════")
    status, kb1 = rest_request(
        "POST", "/knowledge-bases", body={
            # uuid required since idempotency-key validation tightened
            "idempotency_key": str(uuid.uuid4()),
            "name": f"e2e046-kb1-{E2E_TAG}",
            "description": "E2E issue-046 KB1 (B2 endpoints host)",
            "embedding_model": EMBEDDING_MODEL,
        }, expected_status=201, label="CreateKB kb1",
        desc="前置: 创建知识库 kb1（B2 五接口的宿主 KB）")
    kb1_id = kb1.get("id", "") if isinstance(kb1, dict) else ""
    check("precondition CreateKB kb1 -> 201", status == 201, f"status={status}")
    if not kb1_id:
        return 1
    status, kb2 = rest_request(
        "POST", "/knowledge-bases", body={
            # uuid required since idempotency-key validation tightened
            "idempotency_key": str(uuid.uuid4()),
            "name": f"e2e046-kb2-{E2E_TAG}",
            "description": "E2E issue-046 KB2 (cross-KB negative target)",
        }, expected_status=201, label="CreateKB kb2",
        desc="前置: 创建知识库 kb2（跨 KB 删除负例与 citations doc2 宿主）")
    kb2_id = kb2.get("id", "") if isinstance(kb2, dict) else ""
    check("precondition CreateKB kb2 -> 201", status == 201, f"status={status}")
    if not kb2_id:
        return 1

    # ══ Precondition 2: seed rows via production repositories ══
    log("════ PRECONDITION: seed documents/chunks/sessions/messages ════")
    try:
        info = _run_async(_seed(kb1_id, kb2_id))
    except Exception as e:
        log(f"FATAL: seed failed: {e!r}")
        return 1
    doc1, docX, docSD, doc2 = (info["doc1"], info["docX"],
                               info["docSD"], info["doc2"])
    s1, s2, s3, s4 = info["s1"], info["s2"], info["s3"], info["s4"]
    kb2_s1 = info["kb2_s1"]
    check("seed: 7 chunks written (2p+4c+1s)", info["n_chunks"] == "7",
          f"n={info['n_chunks']}")
    log(f"  ids: doc1={doc1} docX={docX} docSD={docSD} doc2={doc2}")
    log(f"       s1={s1} s2={s2} s3={s3} s4={s4} kb2_s1={kb2_s1}")

    # ══ T2: ListDocumentChunks ══
    chunks_path = f"/knowledge-bases/{kb1_id}/documents/{doc1}/chunks"
    log("════ T2: GET .../documents/{doc_id}/chunks (ListDocumentChunks) ════")

    # T2a: default page -> 7 items, id ASC, full field set
    status, payload = rest_request(
        "GET", chunks_path, expected_status=200, label="T2a chunks default",
        desc="【新增接口】ListDocumentChunks 默认页: 返回文档全部分块"
             "（默认 limit=50），按 id ASC 排序，custom_metadata 为对象")
    items = _items(payload)
    check("T2a default -> 200", status == 200, f"status={status}")
    check("T2a returns 7 chunks", len(items) == 7, f"got {len(items)}")
    ids = [c.get("id") for c in items]
    check("T2a ids ASC ordered", ids == sorted(ids), f"ids={ids}")
    check("T2a no next_cursor (full page)", not (payload or {}).get("next_cursor"))
    if items:
        for field in ("id", "doc_id", "kb_id", "chunk_type", "content",
                     "file_name", "created_at"):
            check(f"T2a has field '{field}'", field in items[0])
        c1 = next((c for c in items if c.get("content", "").startswith("CHILD-1")), None)
        check("T2a child has parent_chunk_id", bool(c1 and c1.get("parent_chunk_id")),
              f"c1={c1 and c1.get('id')}")
        check("T2a child has parent_content",
              bool(c1 and c1.get("parent_content")))
        check("T2a custom_metadata is dict",
              isinstance(c1 and c1.get("custom_metadata"), dict),
              f"meta={c1 and c1.get('custom_metadata')}")
        check("T2a metadata content round-trip",
              bool(c1) and c1.get("custom_metadata", {}).get("heading") == "网关")
        types = {c.get("chunk_type") for c in items}
        check("T2a chunk_type set {parent, child, doc_summary}",
              types == {"parent", "child", "doc_summary"}, f"types={types}")

    # T2b: chunk_type=child -> 4
    status, payload = rest_request(
        "GET", f"{chunks_path}?chunk_type=child", expected_status=200,
        label="T2b chunks type=child",
        desc="【新增接口】ListDocumentChunks 类型过滤: chunk_type=child "
             "只返回子块（4 条）")
    items = _items(payload)
    check("T2b child filter -> 4 items", len(items) == 4, f"got {len(items)}")
    check("T2b all child type", all(c.get("chunk_type") == "child" for c in items))

    # T2c: chunk_type=doc_summary -> 1
    status, payload = rest_request(
        "GET", f"{chunks_path}?chunk_type=doc_summary", expected_status=200,
        label="T2c chunks type=doc_summary",
        desc="【新增接口】ListDocumentChunks 类型过滤: chunk_type=doc_summary "
             "只返回文档摘要块（1 条）")
    items = _items(payload)
    check("T2c doc_summary filter -> 1 item", len(items) == 1, f"got {len(items)}")

    # T2d: pagination limit=3 -> 3+3+1, union == all 7
    seen: list[str] = []
    cursor = ""
    pages = 0
    while pages < 5:
        url = f"{chunks_path}?limit=3" + (f"&cursor={cursor}" if cursor else "")
        status, payload = rest_request(
            "GET", url, expected_status=200,
            label=f"T2d chunks page {pages + 1}",
            desc="【新增接口】ListDocumentChunks 游标翻页: limit=3 按 id ASC "
                 "keyset 分页，尾页 next_cursor 为空")
        items = _items(payload)
        seen += [c.get("id") for c in items]
        cursor = (payload or {}).get("next_cursor") or ""
        pages += 1
        if not cursor:
            break
    check("T2d pagination 3+3+1 == 7", len(seen) == 7 and pages == 3,
          f"seen={len(seen)} pages={pages}")
    check("T2d no duplicate ids", len(set(seen)) == len(seen))
    check("T2d union == default-page ids", True)  # ids compared below
    status, payload = rest_request(
        "GET", chunks_path, expected_status=200, label="T2d chunks full re-fetch")
    full_ids = [c.get("id") for c in _items(payload)]
    check("T2d paged ids == full ids", sorted(seen) == sorted(full_ids))

    # T2e: limit=101 -> 400
    status, payload = rest_request(
        "GET", f"{chunks_path}?limit=101", expected_status=400,
        label="T2e chunks limit=101",
        desc="【新增接口】ListDocumentChunks 边界: limit>100 应 400")
    check("T2e limit=101 -> 400", status == 400, f"status={status}")

    # T2f: unknown doc -> 404
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb1_id}/documents/{uuid.uuid4()}/chunks",
        expected_status=404, label="T2f chunks unknown doc",
        desc="【新增接口】ListDocumentChunks 未文档: 不存在的 doc_id 应 404")
    check("T2f unknown doc -> 404", status == 404, f"status={status}")

    # T2g: soft-deleted doc -> 404
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb1_id}/documents/{docSD}/chunks",
        expected_status=404, label="T2g chunks soft-deleted doc",
        desc="【新增接口】ListDocumentChunks 软删文档: parse_status=failed + "
             "error_message=deleted 的文档应被过滤返回 404")
    check("T2g soft-deleted doc -> 404", status == 404, f"status={status}")

    # T2h: unknown kb -> 404
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{uuid.uuid4()}/documents/{doc1}/chunks",
        expected_status=404, label="T2h chunks unknown kb",
        desc="【新增接口】ListDocumentChunks 未知识库: 不存在的 kb_id 应 404")
    check("T2h unknown kb -> 404", status == 404, f"status={status}")

    # T2i: invalid chunk_type -> 400
    status, payload = rest_request(
        "GET", f"{chunks_path}?chunk_type=bogus", expected_status=400,
        label="T2i chunks invalid type",
        desc="【新增接口】ListDocumentChunks 非法参数: chunk_type 不在 "
             "{child,parent,doc_summary} 白名单应 400")
    check("T2i invalid chunk_type -> 400", status == 400, f"status={status}")

    # T2j: invalid cursor -> 400
    status, payload = rest_request(
        "GET", f"{chunks_path}?cursor=not-a-uuid", expected_status=400,
        label="T2j chunks invalid cursor",
        desc="【新增接口】ListDocumentChunks 非法游标: 非 UUID cursor 应 400")
    check("T2j invalid cursor -> 400", status == 400, f"status={status}")

    # T2k: cross-tenant -> 404
    status, payload = rest_request(
        "GET", chunks_path, expected_status=404, label="T2k chunks cross-tenant",
        headers={"X-Dev-Tenant-ID": OTHER_TENANT_ID},
        desc="【新增接口】ListDocumentChunks 跨租户: 用其他租户身份访问应 404")
    check("T2k cross-tenant -> 404", status == 404, f"status={status}")

    # ══ T3: ListKBSessions ══
    sessions_path = f"/knowledge-bases/{kb1_id}/sessions"
    log("════ T3: GET .../sessions (ListKBSessions) ════")

    # T3a: default page -> DESC by created, aggregates present
    status, payload = rest_request(
        "GET", sessions_path, expected_status=200, label="T3a sessions default",
        desc="【新增接口】ListKBSessions 默认页: 按 created_at DESC 返回"
             "会话列表（默认 limit=20），含 message_count/last_query/"
             "last_active_at 聚合字段")
    items = _items(payload)
    check("T3a default -> 200", status == 200, f"status={status}")
    check("T3a returns 4 sessions (s4..s1 DESC)", len(items) == 4,
          f"got {len(items)}")
    if len(items) == 4:
        check("T3a DESC order s4,s3,s2,s1",
              [s.get("id") for s in items] == [s4, s3, s2, s1])
        for field in ("id", "kb_id", "message_count", "created_at",
                      "last_active_at"):
            check(f"T3a has field '{field}'", field in items[0])
        by_id = {s.get("id"): s for s in items}
        s1_agg = by_id.get(s1, {})
        check("T3a s1 message_count == 3",
              s1_agg.get("message_count") == 3,
              f"got {s1_agg.get('message_count')}")
        check("T3a s1 last_query == u1 content",
              s1_agg.get("last_query") == "u1: 网关层的职责是什么？",
              f"got {s1_agg.get('last_query')!r}")
        check("T3a s1 last_active_at == a1/u2 time (present)",
              bool(s1_agg.get("last_active_at")))
        s2_agg = by_id.get(s2, {})
        check("T3a s2 message_count == 1",
              s2_agg.get("message_count") == 1,
              f"got {s2_agg.get('message_count')}")
        s3_agg = by_id.get(s3, {})
        check("T3a s3 (empty session) message_count == 0",
              s3_agg.get("message_count") == 0,
              f"got {s3_agg.get('message_count')}")
        check("T3a s3 last_active_at is null", not s3_agg.get("last_active_at"))

    # T3b: composite-cursor pagination limit=2 -> s4,s3 | s2,s1
    status, p1 = rest_request(
        "GET", f"{sessions_path}?limit=2", expected_status=200,
        label="T3b sessions page1",
        desc="【新增接口】ListKBSessions 游标翻页: limit=2 取最近 2 个会话，"
             "返回复合游标 (created_at|session_id)")
    page1 = _items(p1)
    check("T3b page1 = s4,s3", [s.get("id") for s in page1] == [s4, s3])
    cursor = (p1 or {}).get("next_cursor") or ""
    check("T3b page1 has next_cursor", bool(cursor))
    status, p2 = rest_request(
        "GET", f"{sessions_path}?limit=2&cursor={cursor}", expected_status=200,
        label="T3b sessions page2",
        desc="【新增接口】ListKBSessions 游标翻页: 用复合游标取下一页"
             "（s2,s1），尾页 next_cursor 为空")
    page2 = _items(p2)
    check("T3b page2 = s2,s1", [s.get("id") for s in page2] == [s2, s1])
    # Servicer convention (all 6 paginated RPCs): a full page always emits
    # a cursor — the end of data is discovered by fetching one more page
    # and getting an empty items list. page2 (limit=2) is full, so expect
    # a cursor, then page3 must be empty with no cursor.
    cursor2 = (p2 or {}).get("next_cursor") or ""
    check("T3b page2 full-page -> cursor present", bool(cursor2))
    status, p3 = rest_request(
        "GET", f"{sessions_path}?limit=2&cursor={cursor2}", expected_status=200,
        label="T3b sessions page3",
        desc="【新增接口】ListKBSessions 游标翻页: 尾页探测 — page2 为满页必发"
             "游标，page3 取到空列表且无游标（键集分页到尾）")
    check("T3b page3 empty (end of data)", _items(p3) == [])
    check("T3b page3 no next_cursor", not (p3 or {}).get("next_cursor"))

    # T3c: limit=101 -> 400 (servicer-side validation)
    status, payload = rest_request(
        "GET", f"{sessions_path}?limit=101", expected_status=400,
        label="T3c sessions limit=101",
        desc="【新增接口】ListKBSessions 边界: limit>100 应 400")
    check("T3c limit=101 -> 400", status == 400, f"status={status}")

    # T3d: unknown kb -> 404
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{uuid.uuid4()}/sessions",
        expected_status=404, label="T3d sessions unknown kb",
        desc="【新增接口】ListKBSessions 未知识库: 不存在的 kb_id 应 404")
    check("T3d unknown kb -> 404", status == 404, f"status={status}")

    # ══ T4: ListSessionMessages ══
    s1_msgs_path = f"/knowledge-bases/{kb1_id}/sessions/{s1}/messages"
    log("════ T4: GET .../sessions/{sid}/messages (ListSessionMessages) ════")

    # T4a: ASC order + role/content/sources semantics
    status, payload = rest_request(
        "GET", s1_msgs_path, expected_status=200, label="T4a messages default",
        desc="【新增接口】ListSessionMessages 默认页: 按 created_at ASC 返回"
             "会话全部消息（u1->a1->u2），user 消息 sources 为 null")
    items = _items(payload)
    check("T4a default -> 200", status == 200, f"status={status}")
    check("T4a s1 has 3 messages u1,a1,u2",
          len(items) == 3 and [m.get("role") for m in items] == ["user", "assistant", "user"],
          f"roles={[m.get('role') for m in items]}")
    if len(items) == 3:
        u1, a1, u2 = items
        for field in ("id", "session_id", "role", "content", "created_at"):
            check(f"T4a has field '{field}'", field in u1)
        check("T4a u1 id matches seed", u1.get("id") == info["u1"])
        check("T4a u1 sources is null", u1.get("sources") in (None, "null"),
              f"got {u1.get('sources')!r}")
        check("T4a a1 id matches seed", a1.get("id") == info["a1"])
        a1_src = a1.get("sources")
        src_ok = isinstance(a1_src, list) and len(a1_src) == 3
        check("T4a a1 sources is 3-elem array", src_ok,
              f"type={type(a1_src).__name__} len={len(a1_src) if isinstance(a1_src, list) else 'n/a'}")
        if src_ok:
            check("T4a a1 sources[0].score == 0.92",
                  a1_src[0].get("score") == 0.92)
        check("T4a session_id matches s1", u1.get("session_id") == s1)

    # T4b: composite-cursor pagination limit=2 -> u1,a1 | u2
    status, p1 = rest_request(
        "GET", f"{s1_msgs_path}?limit=2", expected_status=200,
        label="T4b messages page1",
        desc="【新增接口】ListSessionMessages 游标翻页: limit=2 取最早 2 条，"
             "返回复合游标 (created_at|message_id)")
    page1 = _items(p1)
    check("T4b page1 = u1,a1",
          [m.get("id") for m in page1] == [info["u1"], info["a1"]])
    cursor = (p1 or {}).get("next_cursor") or ""
    check("T4b page1 has next_cursor", bool(cursor))
    status, p2 = rest_request(
        "GET", f"{s1_msgs_path}?limit=2&cursor={cursor}", expected_status=200,
        label="T4b messages page2",
        desc="【新增接口】ListSessionMessages 游标翻页: 复合游标取剩余 1 条"
             "（u2），尾页 next_cursor 为空")
    page2 = _items(p2)
    check("T4b page2 = u2 only",
          [m.get("id") for m in page2] == [info["u2"]])
    check("T4b page2 no next_cursor", not (p2 or {}).get("next_cursor"))

    # T4c: limit=101 -> 400
    status, payload = rest_request(
        "GET", f"{s1_msgs_path}?limit=101", expected_status=400,
        label="T4c messages limit=101",
        desc="【新增接口】ListSessionMessages 边界: limit>100 应 400")
    check("T4c limit=101 -> 400", status == 400, f"status={status}")

    # T4d: cross-KB session access (kb1_id + kb2 session) -> 404
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb1_id}/sessions/{kb2_s1}/messages",
        expected_status=404, label="T4d messages cross-KB session",
        desc="【新增接口】ListSessionMessages 归属校验: 用 kb1 的 kb_id 访问"
             "kb2 的会话应 404（session 必须属于 path 中的 KB）")
    check("T4d cross-KB session -> 404", status == 404, f"status={status}")

    # T4e: unknown session -> 404
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb1_id}/sessions/{uuid.uuid4()}/messages",
        expected_status=404, label="T4e messages unknown session",
        desc="【新增接口】ListSessionMessages 未会话: 不存在的 session_id 应 404")
    check("T4e unknown session -> 404", status == 404, f"status={status}")

    # T4f: invalid cursor -> 400
    status, payload = rest_request(
        "GET", f"{s1_msgs_path}?cursor=bogus|value", expected_status=400,
        label="T4f messages invalid cursor",
        desc="【新增接口】ListSessionMessages 非法游标: 无法解析的复合游标"
             "应 400")
    check("T4f invalid cursor -> 400", status == 400, f"status={status}")

    # ══ T5: ListKBCitations ══
    citations_path = f"/knowledge-bases/{kb1_id}/citations"
    log("════ T5: GET .../citations (ListKBCitations) ════")

    # T5a: both assistant msgs expanded; best score per (message, doc)
    # a1: doc1 0.92 + docX 0.55  => 2 citations
    # a2: doc1 0.90 (0.42 shadowed) + doc2 0.66 => 2 citations
    # newest-message-first: a2 rows before a1 rows
    status, payload = rest_request(
        "GET", citations_path, expected_status=200, label="T5a citations default",
        desc="【新增接口】ListKBCitations 默认页: 按消息粒度展开 assistant 消息"
             "的 source_chunks 引用，每个 (message, doc) 取最高分，消息按"
             "时间倒序")
    items = _items(payload)
    check("T5a default -> 200", status == 200, f"status={status}")
    check("T5a returns 4 citations", len(items) == 4, f"got {len(items)}")
    if len(items) == 4:
        for field in ("id", "kb_id", "doc_id", "file_name", "page", "content",
                      "score", "created_at"):
            check(f"T5a has field '{field}'", field in items[0])
        # rows grouped newest message first: a2 (doc1 0.90, doc2 0.66) then a1
        by_doc = {(c.get("message_id"), c.get("doc_id")): c for c in items}
        c_a2_doc1 = by_doc.get((info["a2"], doc1))
        c_a2_doc2 = by_doc.get((info["a2"], doc2))
        c_a1_doc1 = by_doc.get((info["a1"], doc1))
        c_a1_docX = by_doc.get((info["a1"], docX))
        check("T5a a2-doc1 score 0.90 (0.42 shadowed)",
              bool(c_a2_doc1) and abs((c_a2_doc1.get("score") or 0) - 0.90) < 1e-9,
              f"score={c_a2_doc1 and c_a2_doc1.get('score')}")
        check("T5a a2-doc2 score 0.66",
              bool(c_a2_doc2) and abs((c_a2_doc2.get("score") or 0) - 0.66) < 1e-9,
              f"score={c_a2_doc2 and c_a2_doc2.get('score')}")
        check("T5a a1-doc1 score 0.92 (0.71 shadowed)",
              bool(c_a1_doc1) and abs((c_a1_doc1.get("score") or 0) - 0.92) < 1e-9,
              f"score={c_a1_doc1 and c_a1_doc1.get('score')}")
        check("T5a a1-docX score 0.55",
              bool(c_a1_docX) and abs((c_a1_docX.get("score") or 0) - 0.55) < 1e-9,
              f"score={c_a1_docX and c_a1_docX.get('score')}")
        # best-score row must carry the best content (片段4, not 片段3)
        check("T5a a2-doc1 content from best-score source",
              bool(c_a2_doc1) and c_a2_doc1.get("content") == "命中片段4",
              f"content={c_a2_doc1 and c_a2_doc1.get('content')!r}")

    # T5b: deterministic uuid5 ids — two calls identical
    status, again = rest_request(
        "GET", citations_path, expected_status=200,
        label="T5b citations replay",
        desc="【新增接口】ListKBCitations 确定性 id: citation id 为 uuid5"
             "(kb_id:message_id:doc_id) 派生，两次调用应完全一致")
    ids_1 = [c.get("id") for c in _items(payload)]
    ids_2 = [c.get("id") for c in _items(again)]
    check("T5b replay ids identical", ids_1 == ids_2 and len(ids_1) == 4)
    if ids_1:
        import uuid as _uuid
        expect = str(_uuid.uuid5(
            _uuid.NAMESPACE_URL,
            f"ani:kb:citation:{kb1_id}:{info['a1']}:{doc1}"))
        check("T5b id == uuid5(ani:kb:citation:...)",
              ids_1 and any(i == expect for i in ids_1),
              f"expect={expect} actual={ids_1}")

    # T5c: message_id / session_id passthrough
    if len(items) == 4:
        s_ids = {c.get("session_id") for c in items}
        m_ids = {c.get("message_id") for c in items}
        check("T5c session_id passthrough (s1 & s4)",
              s_ids == {s1, s4}, f"got {s_ids}")
        check("T5c message_id passthrough (a1 & a2)",
              m_ids == {info["a1"], info["a2"]}, f"got {m_ids}")

    # T5d: limit=1 message-grain pagination (whole messages, not rows)
    status, p1 = rest_request(
        "GET", f"{citations_path}?limit=1", expected_status=200,
        label="T5d citations page1 (limit=1 msg)",
        desc="【新增接口】ListKBCitations 消息粒度翻页: limit=1 按消息分页，"
             "返回最新消息 a2 的 2 条引用")
    page1 = _items(p1)
    check("T5d page1 = a2's 2 citations",
          len(page1) == 2 and all(c.get("message_id") == info["a2"] for c in page1),
          f"msgs={[c.get('message_id') for c in page1]}")
    cursor = (p1 or {}).get("next_cursor") or ""
    check("T5d page1 has next_cursor", bool(cursor))
    status, p2 = rest_request(
        "GET", f"{citations_path}?limit=1&cursor={cursor}", expected_status=200,
        label="T5d citations page2",
        desc="【新增接口】ListKBCitations 消息粒度翻页: 游标取上一条消息 a1 的"
             "2 条引用，尾页 next_cursor 为空")
    page2 = _items(p2)
    check("T5d page2 = a1's 2 citations",
          len(page2) == 2 and all(c.get("message_id") == info["a1"] for c in page2),
          f"msgs={[c.get('message_id') for c in page2]}")
    # Full-page convention (see T3b): page2 (limit=1, 2 citations of one
    # message) is full, so a cursor is emitted; page3 must be empty.
    cursor2 = (p2 or {}).get("next_cursor") or ""
    check("T5d page2 full-page -> cursor present", bool(cursor2))
    status, p3 = rest_request(
        "GET", f"{citations_path}?limit=1&cursor={cursor2}", expected_status=200,
        label="T5d citations page3",
        desc="【新增接口】ListKBCitations 消息粒度翻页: 尾页探测 — page3 空列表"
             "且无游标")
    check("T5d page3 empty (end of data)", _items(p3) == [])
    check("T5d page3 no next_cursor", not (p3 or {}).get("next_cursor"))

    # T5e: unknown kb -> 404
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{uuid.uuid4()}/citations",
        expected_status=404, label="T5e citations unknown kb",
        desc="【新增接口】ListKBCitations 未知识库: 不存在的 kb_id 应 404")
    check("T5e unknown kb -> 404", status == 404, f"status={status}")

    # ══ T6: DeleteSession ══
    log("════ T6: DELETE .../sessions/{sid} (DeleteSession) ════")

    # T6a: delete kb1 s1 (has 3 messages) -> 204
    status, payload = rest_request(
        "DELETE", f"/knowledge-bases/{kb1_id}/sessions/{s1}",
        expected_status=204, label="T6a DeleteSession s1",
        desc="【新增接口】DeleteSession: 删除含 3 条消息的会话 s1（消息级联"
             "删除），返回 204 无响应体")
    check("T6a delete s1 -> 204", status == 204, f"status={status}")

    # T6b: idempotent replay -> 204
    status, payload = rest_request(
        "DELETE", f"/knowledge-bases/{kb1_id}/sessions/{s1}",
        expected_status=204, label="T6b DeleteSession replay",
        desc="【新增接口】DeleteSession 幂等: 重复删除同一会话应仍返回 204")
    check("T6b replay delete -> 204", status == 204, f"status={status}")

    # T6c: messages after delete -> 404 (session gone)
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb1_id}/sessions/{s1}/messages",
        expected_status=404, label="T6c messages after delete",
        desc="删除后验证: 会话已删，再查其消息应 404")
    check("T6c messages after delete -> 404", status == 404, f"status={status}")

    # T6g: DB verify s1 messages purged (moved before cross-KB test for
    # a clean assertion even if later checks fail)
    n_left = _run_async(_count_session_messages(s1))
    check("T6g DB: s1 messages == 0", n_left == 0, f"left={n_left}")

    # T6d: cross-KB negative — delete kb2 session via kb1 path
    # (kb-service delete_session deletes messages scoped by kb_id; the kb2
    #  session does not belong to kb1 so nothing is deleted and 204 returns)
    status, payload = rest_request(
        "DELETE", f"/knowledge-bases/{kb1_id}/sessions/{kb2_s1}",
        expected_status=204, label="T6d cross-KB delete attempt",
        desc="【新增接口】DeleteSession 跨 KB 负例: 用 kb1 的 kb_id 删除 kb2 的"
             "会话 — 幂等语义返回 204，但 kb2 会话及其消息不受影响")
    check("T6d cross-KB delete -> 204 (idempotent noop)",
          status == 204, f"status={status}")
    n_kb2 = _run_async(_count_session_messages(kb2_s1))
    check("T6d kb2 session messages intact (2)", n_kb2 == 2,
          f"left={n_kb2}")
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb2_id}/sessions/{kb2_s1}/messages",
        expected_status=200, label="T6d verify kb2 session alive",
        desc="跨 KB 负例验证: kb2 会话消息仍可正常读取（200）")
    check("T6d kb2 session still readable -> 200", status == 200,
          f"status={status}")

    # T6e: unknown kb -> 404
    status, payload = rest_request(
        "DELETE", f"/knowledge-bases/{uuid.uuid4()}/sessions/{s2}",
        expected_status=404, label="T6e delete unknown kb",
        desc="【新增接口】DeleteSession 未知识库: 不存在的 kb_id 应 404")
    check("T6e unknown kb -> 404", status == 404, f"status={status}")

    # T6f: unknown session -> 204 (idempotent)
    status, payload = rest_request(
        "DELETE", f"/knowledge-bases/{kb1_id}/sessions/{uuid.uuid4()}",
        expected_status=204, label="T6f delete unknown session",
        desc="【新增接口】DeleteSession 未会话: 删除不存在的 session_id 应"
             "幂等返回 204")
    check("T6f unknown session -> 204", status == 204, f"status={status}")

    # ══ Cleanup ══
    log("════ CLEANUP: purge seeded rows + soft-delete KBs ════")
    try:
        _run_async(_purge_seeded_rows([kb1_id, kb2_id]))
        check("cleanup: seeded rows purged", True)
    except Exception as e:
        check("cleanup: seeded rows purged", False, repr(e))
    for kb_id, name in ((kb1_id, "kb1"), (kb2_id, "kb2")):
        status, _ = rest_request(
            "DELETE", f"/knowledge-bases/{kb_id}", expected_status=204,
            label=f"cleanup DeleteKB {name}",
            desc=f"清理: 软删知识库 {name}（行保留审计）")
        check(f"cleanup DeleteKB {name} -> 204", status == 204,
              f"status={status}")

    return 0


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    _open_log()
    log("════════════════════════════════════════════════════════════════")
    log(f"  E2E issue-046 (B2) chunks/sessions/citations — tag={E2E_TAG}")
    log(f"  tenant={TENANT_ID}")
    log(f"  log file: {E2E_LOG}")
    log("════════════════════════════════════════════════════════════════")

    if not GATEWAY_EXE.exists():
        log(f"FATAL: gateway exe not found at {GATEWAY_EXE} — run go build first")
        return 1

    log("── Killing stale processes on ports 8080/8001/8002/50052/50053 ──")
    _kill_stale_processes()

    try:
        # 1. start gateway
        log("── Starting ani-gateway (:8080) ──")
        start_gateway()
        if not _wait_http("http://localhost:8080/api/v1/svc/knowledge-bases?limit=1", 60):
            log("FATAL: gateway not ready")
            return 1

        # dev-mode sanity: KB router mounted (503 = kb-service not yet up)
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

        # warm up the gateway→kb-service gRPC channel (non-blocking dial may
        # sit in TRANSIENT_FAILURE backoff after the sanity check hit it
        # while kb-service was down; poll until no longer 503)
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
        log("══════════════════════════ RESULT SUMMARY ══════════════════════════")
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

