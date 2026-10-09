"""Unified E2E test for the KB API completion batch (B1-B3, issue-040~048) —
ALL 8 NEW endpoints on one live business flow, with every interface purpose
(作用), INPUT and OUTPUT printed BOTH to terminal and the result log file.

The 8 NEW endpoints under test (SPEC §4.3):
  #5  PUT    /knowledge-bases/{kb_id}                        (updateKnowledgeBase)
  #10 GET    /knowledge-bases/{kb_id}/documents/{doc_id}     (getKnowledgeBaseDocument)
  #11 GET    /knowledge-bases/{kb_id}/documents/{doc_id}/chunks (listDocumentChunks)
  #12 POST   /knowledge-bases/{kb_id}/documents/{doc_id}/reparse (reparseDocument)
  #15 GET    /knowledge-bases/{kb_id}/citations               (listKBCitations)
  #16 GET    /knowledge-bases/{kb_id}/sessions               (listKBSessions)
  #17 GET    /knowledge-bases/{kb_id}/sessions/{sid}/messages (listSessionMessages)
  #18 DELETE /knowledge-bases/{kb_id}/sessions/{sid}          (deleteSession)

Business flow (real data end-to-end, no direct DB seeding for the happy
path — the doc is REALLY uploaded, parsed and embedded):
  1. CreateKB
  2. upload document (GetDocumentUploadURL -> PUT presigned MinIO URL
     -> CompleteDocumentUpload) -> wait parse_status=ready
  3. query KB -> creates a session + assistant message (citations source)
  4. Test #5  UpdateKB (rename + description)
  5. Test #10 GetDocument
  6. Test #11 ListDocumentChunks (default page + chunk_type filter)
  7. Test #12 ReparseDocument (doc must be non-ready: use a SECOND seeded
     failed doc for the happy path; also negative guards on the real doc)
  8. Test #16 ListKBSessions (aggregates from the query above)
  9. Test #17 ListSessionMessages (user + assistant messages)
  10. Test #15 ListKBCitations (expanded from assistant message sources)
  11. Test #18 DeleteSession (204 + messages 404 afterwards)
  12. Cleanup (DeleteKB soft delete + purge seeded rows)

Services (local only, NEVER uploaded to the server):
  - ani-gateway (Go, :8080)      repo/bin/ani-gateway.exe
  - kb-service  (Python, :8002 / gRPC :50053)
  - rag-engine  (Python, :8001 / gRPC :50052)  [needed: real parse+embed+query]

Infrastructure (server-deployed NodePorts on 10.10.1.66):
  PostgreSQL :30945 / MinIO :30900 / NATS :31062 / Redis :30453 / Milvus :31930

Usage:
  python tests/e2e/test_kb_new_apis_e2e.py
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

# ── paths ────────────────────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parents[4]  # repo/
KB_SVC = REPO / "services" / "kb-service"
RAG_ENGINE = REPO / "ai" / "rag-engine"
GATEWAY_EXE = REPO / "bin" / "ani-gateway.exe"
E2E_LOG = KB_SVC / "tests" / "e2e" / "e2e_kb_new_apis_result.log"

# make app.repositories (production helpers) importable
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

VLLM_MODEL = "Qwen3.8-27B-FP8"
VLLM_API_BASE = "http://10.10.1.2:6080/v1"
VLLM_API_KEY = "sk-YOp8k71BXjxBTeZniPPvQlbGgciH0CB9WOWXkmuCzjfIZ5R8"
# interim embedding endpoints (10.10.20.197:8006, coreshub) are dead/invalid;
# use the SiliconFlow OpenAI-compatible endpoint which serves BAAI/bge-m3
EMBEDDING_MODEL = "BAAI/bge-m3"
EMBEDDING_API_BASE = "https://api.siliconflow.cn/v1"
EMBEDDING_API_KEY = "sk-fdckgjrqrzfmjrscxkzuppxsfhijborgiwqjwbamdgcyitwc"

TENANT_ID = "00000000-0000-0000-0000-000000000002"
OTHER_TENANT_ID = "00000000-0000-0000-0000-000000000003"
E2E_TAG = f"kbnew-{int(time.time())}"

# unique NATS parse subjects so the server-deployed kb-service (v2) and
# rag-engine (legacy) consumers on the default subjects never race with our
# local ones on the shared NATS server
LOCAL_PARSE_SUBJECT = f"ani.tasks.kb.parse.kbnew.{E2E_TAG}"
LOCAL_PARSE_SUBJECT_V2 = f"ani.tasks.kb.parse.kbnew.v2.{E2E_TAG}"

REST_BASE = "http://localhost:8080/api/v1/svc"
REST_HDRS = {"Content-Type": "application/json", "X-Dev-Tenant-ID": TENANT_ID}
OTHER_HDRS = {"Content-Type": "application/json", "X-Dev-Tenant-ID": OTHER_TENANT_ID}

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
    log_file = open(E2E_LOG.parent / f"{name}_kbnew.stdout.log", "w",
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
        "KB_SERVICE_GRPC_CALL_TIMEOUT": "120s",
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
        # unique subjects: only our LOCAL services consume them; the
        # server-deployed kb-service/rag-engine on the default subjects
        # never races us (outbox publishes to V2 when the local parse
        # consumer is ON — the default V2 subject is shared with the
        # SERVER kb-service, so it MUST be overridden too)
        "NATS_PARSE_SUBJECT": LOCAL_PARSE_SUBJECT,
        "NATS_PARSE_SUBJECT_V2": LOCAL_PARSE_SUBJECT_V2,
        "RAG_ENGINE_GRPC_ADDR": "localhost:50052",
        "GRPC_PORT": "50053",
        "REDIS_URL": REDIS_URL,
        # LOCAL parse consumer ON: the complete-upload outbox event must be
        # consumed locally so the real doc actually gets parsed+embedded
        "KB_PARSE_CONSUMER_ENABLED": "true",
        "ANI_DEV_TENANT_ID": TENANT_ID,
    })
    return env


def _rag_engine_env():
    env = os.environ.copy()
    env.pop("ANI_GATEWAY_INTERNAL_URL", None)  # AliasChoices: use ANI_GATEWAY_URL
    env.update({
        "MILVUS_ADDR": MILVUS_ADDR,
        "DATABASE_URL": PG_URL,
        "NATS_URL": NATS_URL,
        "NATS_PARSE_SUBJECT": LOCAL_PARSE_SUBJECT,
        "EMBEDDING_MODEL": EMBEDDING_MODEL,
        "EMBEDDING_API_BASE": EMBEDDING_API_BASE,
        "EMBEDDING_API_KEY": EMBEDDING_API_KEY,
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ACCESS_KEY": MINIO_AK,
        "MINIO_SECRET_KEY": MINIO_SK,
        "MINIO_SECURE": "false",
        "MINIO_BUCKET": "ani-kb-docs",
        "VLLM_MODEL": VLLM_MODEL,
        "VLLM_API_BASE": VLLM_API_BASE,
        "VLLM_API_KEY": VLLM_API_KEY,
        "VLLM_CONTEXT_WINDOW": "32768",
        "REDIS_URL": REDIS_URL,
        "ANI_GATEWAY_URL": "http://localhost:8080",
        "ANI_DEV_TENANT_ID": TENANT_ID,
    })
    return env


def start_gateway():
    _start([str(GATEWAY_EXE)], _gateway_env(),
           REPO / "services" / "ani-gateway", "gateway")


def start_kb_service():
    _start([sys.executable, "main.py"], _kb_service_env(), KB_SVC, "kb-service")


def start_rag_engine():
    _start([sys.executable, "main.py"], _rag_engine_env(), RAG_ENGINE, "rag-engine")


# ── REST helpers ──────────────────────────────────────────────────────────────
def rest_request(method: str, path: str, body: dict | None = None,
                 expected_status: int | None = None, label: str = "",
                 timeout: float = 120.0, base: str | None = None,
                 headers: dict | None = None, desc: str = "",
                 print_body_limit: int = 40) -> tuple[int, Any]:
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
    log(f"[{label or method + ' ' + path}] INPUT: {method} {url}")
    log(f"[{label}] INPUT headers: {dict((k, v) for k, v in hdrs.items())}")
    if body is not None:
        log_json(f"[{label}] INPUT BODY", body)
    status = -1
    payload: Any = None
    try:
        req = urllib.request.Request(url, data=data, method=method.upper(),
                                     headers=hdrs)
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
    log(f"[{label}] OUTPUT: HTTP {status}"
        + (f" (期望 {expected_status})" if expected_status is not None else ""))
    # print long bodies compactly (items are lists of dicts); keep full for small ones
    if isinstance(payload, dict) and any(
            isinstance(v, list) and len(v) > print_body_limit for v in payload.values()):
        slim = {}
        for k, v in payload.items():
            if isinstance(v, list) and len(v) > print_body_limit:
                slim[k] = f"<{len(v)} items — showing first {print_body_limit}>"
                slim[f"__{k}_preview__"] = v[:print_body_limit]
            else:
                slim[k] = v
        log_json(f"[{label}] OUTPUT BODY (compact)", slim)
    else:
        log_json(f"[{label}] OUTPUT BODY", payload)
    if expected_status is not None and status != expected_status:
        log(f"[{label}] ✗ UNEXPECTED STATUS: expected={expected_status} actual={status}")
    return status, payload


# ── check helpers ─────────────────────────────────────────────────────────────
RESULTS: dict[str, str] = {}  # label -> "pass" | "fail"


def check(label: str, ok: bool, detail: str = ""):
    RESULTS[label] = "pass" if ok else "fail"
    mark = "✓" if ok else "✗"
    log(f"  {mark} {label}" + (f" — {detail}" if detail else ""))


def _msg_of(payload: Any) -> str:
    if isinstance(payload, dict):
        return str(payload.get("message", ""))
    return str(payload or "")


# ── raw HTTP helpers (presigned PUT, no JSON logging) ────────────────────────
def http_put_bytes(url: str, data: bytes, label: str) -> int:
    req = urllib.request.Request(url, data=data, method="PUT",
                                 headers={"Content-Type": "application/octet-stream"})
    log(f"[{label}] INPUT: PUT <presigned url> ({len(data)} bytes, url hidden)")
    try:
        resp = urllib.request.urlopen(req, timeout=60)
        log(f"[{label}] OUTPUT: HTTP {resp.status}")
        return resp.status
    except urllib.error.HTTPError as e:
        log(f"[{label}] OUTPUT: HTTP {e.code} — {e.read().decode('utf-8', errors='replace')[:200]}")
        return e.code
    except Exception as e:
        log(f"[{label}] REQUEST ERROR: {e!r}")
        return -1


# ── PG helpers (doc status polling + reparse-target seeding) ─────────────────
def _run_async(coro) -> Any:
    import asyncio
    return asyncio.run(coro)


async def _ensure_tenant_rows():
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
        for tid, name in ((TENANT_ID, "kbnew-dev-tenant"),
                          (OTHER_TENANT_ID, "kbnew-other-tenant")):
            await conn.execute(
                "INSERT INTO tenants (id, name, display_name, plan_id) "
                "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
                uuid.UUID(tid), name, name,
                uuid.UUID("00000000-0000-0000-0000-000000000001"))
        log(f"  tenants ensured: {TENANT_ID} / {OTHER_TENANT_ID}")
    finally:
        await conn.close()


async def _fetch_doc_state(doc_id: str) -> dict | None:
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
        async with conn.transaction():
            await conn.execute(
                f"SET LOCAL app.current_tenant_id = '{TENANT_ID}'")
            row = await conn.fetchrow(
                """
                SELECT parse_status, error_message, chunk_count
                  FROM kb_documents WHERE id = $1
                """,
                uuid.UUID(doc_id))
            return dict(row) if row else None
    finally:
        await conn.close()


async def _wait_doc_ready(doc_id: str, timeout: float = 240.0):
    import asyncio
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
        deadline = asyncio.get_event_loop().time() + timeout
        last = None
        while asyncio.get_event_loop().time() < deadline:
            async with conn.transaction():
                await conn.execute(
                    f"SET LOCAL app.current_tenant_id = '{TENANT_ID}'")
                row = await conn.fetchrow(
                    "SELECT parse_status, error_message, chunk_count "
                    "  FROM kb_documents WHERE id = $1",
                    uuid.UUID(doc_id))
            if row:
                last = dict(row)
                if row["parse_status"] in ("ready", "failed"):
                    return dict(row)
            await asyncio.sleep(3)
        return {**(last or {}), "parse_status": "timeout"}
    finally:
        await conn.close()


def wait_doc_ready(doc_id: str, timeout: float = 240.0) -> dict:
    return _run_async(_wait_doc_ready(doc_id, timeout))


async def _insert_doc(kb_id: str, name: str, status: str,
                      error: str | None) -> str:
    """Insert one kb_documents row directly (reparse target seeding: the
    normal upload pipeline never yields a 'failed' doc on demand)."""
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
        async with conn.transaction():
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
    finally:
        await conn.close()


async def _set_doc_status(doc_id: str, status: str, error: str | None):
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
        async with conn.transaction():
            await conn.execute(
                f"SET LOCAL app.current_tenant_id = '{TENANT_ID}'")
            await conn.execute(
                "UPDATE kb_documents SET parse_status=$2, error_message=$3 "
                "WHERE id=$1",
                uuid.UUID(doc_id), status, error)
    finally:
        await conn.close()


async def _fetch_task(task_id: str) -> dict | None:
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
        async with conn.transaction():
            await conn.execute(
                f"SET LOCAL app.current_tenant_id = '{TENANT_ID}'")
            row = await conn.fetchrow(
                """
                SELECT id::text, task_type, status, idempotency_key,
                       resource_id::text
                  FROM async_tasks WHERE id = $1
                """,
                uuid.UUID(task_id))
            return dict(row) if row else None
    finally:
        await conn.close()


async def _purge_seeded_rows(kb_id: str, idem_keys: list[str]):
    """Physically purge this run's rows (DeleteKB is a SOFT delete; pure e2e
    test data must not linger in the shared dev DB)."""
    import asyncpg
    conn = await asyncpg.connect(PG_URL)
    try:
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
                "SELECT COUNT(*) FROM kb_documents WHERE kb_id=$1",
                uuid.UUID(kb_id))
            log(f"  purged rows for kb {kb_id} (docs remaining: {n})")
        # neutralize outbox events (audit table: no DELETE permission) so the
        # server-deployed dispatcher never processes them
        async with conn.transaction():
            await conn.execute(
                f"SET LOCAL app.current_tenant_id = '{TENANT_ID}'")
            res = await conn.execute(
                """
                UPDATE outbox_events
                   SET published = TRUE, published_at = now()
                 WHERE tenant_id=$1
                   AND payload->>'kb_id' = ANY($2::text[])
                   AND published = FALSE
                """,
                uuid.UUID(TENANT_ID), [str(kb_id)])
            log(f"  neutralized outbox events: {res}")
        try:
            async with conn.transaction():
                await conn.execute(
                    f"SET LOCAL app.current_tenant_id = '{TENANT_ID}'")
                res = await conn.execute(
                    "DELETE FROM async_tasks WHERE tenant_id=$1 "
                    "AND idempotency_key = ANY($2::text[])",
                    uuid.UUID(TENANT_ID), idem_keys)
                log(f"  purged async_tasks: {res}")
        except Exception as e:
            log(f"  async_tasks DELETE denied (audit table, expected): {e!r}")
    finally:
        await conn.close()


class _OutboxGuardThread:
    """Pre-connected watchdog that shields a reparse/notify outbox event from
    the SERVER-deployed kb-service sharing the dev DB (it polls outbox_events
    every ~1s BYPASSRLS; if it snatches our event it publishes it to the
    SERVER NATS where a stale server consumer fails the doc against the
    server MinIO). Pre-connect + 10ms tight-poll shrinks the race window."""
    _doc_id: str
    _stop: threading.Event
    _ready: threading.Event
    _done: threading.Event
    _thread: threading.Thread | None
    result: str
    latency_ms: float | None
    error: str

    def __init__(self, doc_id: str, event_type: str = "kb.reparse"):
        self._doc_id = doc_id
        self._event_type = event_type
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._done = threading.Event()
        self._thread = None
        self.result = ""
        self.latency_ms = None
        self.error = ""

    def _run(self):
        import asyncio
        import asyncpg

        async def guard_loop():
            conn = await asyncpg.connect(PG_URL)
            try:
                await conn.execute(
                    f"SET app.current_tenant_id = '{TENANT_ID}'")
                self._ready.set()
                sent_at = time.monotonic()
                deadline = sent_at + 30.0
                while not self._stop.is_set() and time.monotonic() < deadline:
                    async with conn.transaction():
                        rid = await conn.fetchval(
                            """
                            SELECT id FROM outbox_events
                             WHERE aggregate_id = $1
                               AND event_type = $2
                               AND published = FALSE
                            """,
                            uuid.UUID(self._doc_id), self._event_type)
                        if rid is not None:
                            res = await conn.execute(
                                """
                                UPDATE outbox_events
                                   SET published = TRUE, published_at = now()
                                 WHERE id = $1 AND published = FALSE
                                """,
                                rid)
                            self.result = str(res)
                            self.latency_ms = (
                                time.monotonic() - sent_at) * 1000.0
                            return
                    await asyncio.sleep(0.01)
            finally:
                self._done.set()
                await conn.close()

        try:
            asyncio.run(guard_loop())
        except Exception as e:
            self.error = repr(e)
            self._ready.set()
            self._done.set()

    def start(self, connect_timeout: float = 5.0) -> bool:
        self._thread = threading.Thread(
            target=self._run, name=f"outbox-guard-{self._doc_id[:8]}",
            daemon=True)
        self._thread.start()
        if not self._ready.wait(connect_timeout):
            log("  outbox guard: PG connect did not become ready in time")
            self._stop.set()
            return False
        if self.error:
            log(f"  outbox guard: armed WITH error: {self.error}")
            return False
        return True

    def finish(self, timeout: float = 3.0) -> bool:
        if self._thread and self._thread.is_alive():
            self._done.wait(timeout)
        return self.stop(2.0)

    def stop(self, timeout: float = 5.0) -> bool:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout)
        if self._thread and self._thread.is_alive():
            log(f"  outbox guard thread did not exit within {timeout}s")
            return False
        return True


# ── test document content ────────────────────────────────────────────────────
def _doc_md_bytes() -> bytes:
    md = """# ANI 知识库平台测试文档

## 一、平台概述
ANI 知识库平台提供企业级 RAG 能力，支持文档上传、解析、向量化和检索增强生成。
用户可以创建多个知识库，每个知识库支持上传 Markdown、PDF、TXT 等格式的文档。
系统将文档解析为语义块（chunk），并通过向量检索和关键词检索融合的方式召回最相关的内容。

## 二、检索模式
平台支持三种检索模式：
1. 关键词检索（keyword）：基于 PostgreSQL pg_trgm 模糊匹配，适合精确匹配场景
2. 向量检索（vector）：基于 Milvus HNSW 索引的余弦相似度检索，适合语义查询
3. 混合检索（hybrid）：通过 RRF 融合算法合并两种检索结果，兼顾精确性与语义性

RRF 公式为 score = sum(1 / (k + rank_i))，其中 k 为常数，rank_i 为结果排名。

## 三、会话管理
每次 Query 调用会产生一个会话（session），会话内保留用户提问与助手回答。
多轮会话通过 session_id 关联，支持上下文连续对话。
会话消息中的 source_chunks 记录引用来源，可用于引用追溯（citations）。

## 四、异步任务
文档解析与重新解析均为异步任务（async_tasks），通过幂等键（idempotency_key）
保证重复提交安全。任务状态包括 pending、running、succeeded、failed。
"""
    return md.encode("utf-8")


# ── run_e2e ───────────────────────────────────────────────────────────────────
def run_e2e() -> int:
    idem_keys: list[str] = []

    # ════ Step 1: CreateKB (precondition, stable interface) ════
    log("════ STEP 1: 前置 — CreateKB ════")
    kb_idem = str(uuid.uuid4())
    idem_keys.append(kb_idem)
    status, kb = rest_request(
        "POST", "/knowledge-bases", body={
            "idempotency_key": kb_idem,
            "name": f"kbnew-kb-{E2E_TAG}",
            "description": "统一 e2e：8 个新增接口测试宿主 KB",
        }, expected_status=201, label="CreateKB",
        desc="前置: 创建知识库（既有稳定接口，作为本批次 8 个新接口的宿主）")
    kb_id = kb.get("id", "") if isinstance(kb, dict) else ""
    check("STEP1 CreateKB -> 201", status == 201 and bool(kb_id),
          f"status={status} kb_id={kb_id}")
    if not kb_id:
        return 1
    log(f"  kb_id={kb_id}")

    # ════ Step 2: upload + parse real document ════
    log("════ STEP 2: 前置 — 文档上传三步曲（真实解析+向量化） ════")
    up_idem = str(uuid.uuid4())
    idem_keys.append(up_idem)
    content = _doc_md_bytes()
    status, up = rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents", body={
            "idempotency_key": up_idem,
            "file_name": f"kbnew-doc-{E2E_TAG}.md",
            "file_type": "md",
            "file_size_bytes": len(content),
        }, expected_status=200, label="GetUploadURL",
        desc="前置: 获取文档上传预签名 URL（既有接口；返回 doc_id/upload_url/storage_path）")
    doc_id = up.get("doc_id", "") if isinstance(up, dict) else ""
    upload_url = up.get("upload_url", "") if isinstance(up, dict) else ""
    storage_path = up.get("storage_path", "") if isinstance(up, dict) else ""
    check("STEP2a GetDocumentUploadURL -> 200", status == 200 and bool(doc_id),
          f"status={status} doc_id={doc_id}")
    if not (doc_id and upload_url):
        return 1
    log(f"  doc_id={doc_id} storage_path={storage_path}")

    put_status = http_put_bytes(upload_url, content, "PUT object to MinIO")
    check("STEP2b presigned PUT -> 200", put_status == 200,
          f"status={put_status}")

    complete_idem = str(uuid.uuid4())
    idem_keys.append(complete_idem)
    status, task = rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents/{doc_id}/complete",
        body={
            "idempotency_key": complete_idem,
            "file_name": f"kbnew-doc-{E2E_TAG}.md",
            "file_type": "md",
            "checksum_sha256": "",
            "custom_metadata": "",
        },
        expected_status=202, label="CompleteDocumentUpload",
        desc="前置: 完成文档上传（D1 三步流第三步；HeadObject 校验 + 登记 + 触发异步解析 202 + AsyncTaskRef）")
    check("STEP2c CompleteDocumentUpload -> 202", status == 202,
          f"status={status} task={task}")
    log("  waiting for parse to finish (local rag-engine parse consumer)...")
    doc_state = wait_doc_ready(doc_id, timeout=300)
    parse_ok = doc_state.get("parse_status") == "ready"
    # retry once if the server-side dispatcher race failed the doc
    if not parse_ok:
        log(f"  parse NOT ready (state={doc_state}) — one retry with fresh key")
        _run_async(_set_doc_status(doc_id, "pending", None))
        retry_idem = str(uuid.uuid4())
        idem_keys.append(retry_idem)
        status, task = rest_request(
            "POST", f"/knowledge-bases/{kb_id}/documents/{doc_id}/complete",
            body={
                "idempotency_key": retry_idem,
                "file_name": f"kbnew-doc-{E2E_TAG}.md",
                "file_type": "md",
                "checksum_sha256": "",
                "custom_metadata": "",
            },
            expected_status=202, label="CompleteDocumentUpload-retry",
            desc="前置重试: 重新完成上传（服务端 outbox dispatcher 竞态兜底）")
        doc_state = wait_doc_ready(doc_id, timeout=300)
        parse_ok = doc_state.get("parse_status") == "ready"
    check("STEP2d parse -> ready", parse_ok,
          f"state={doc_state}")
    log(f"  parse_status={doc_state.get('parse_status')} "
        f"chunk_count={doc_state.get('chunk_count')}")

    # ════ Step 3: query KB (creates session + messages + citations) ════
    log("════ STEP 3: 前置 — Query KB（产生会话/消息/引用数据源） ════")
    q_idem = str(uuid.uuid4())
    idem_keys.append(q_idem)
    status, answer = rest_request(
        "POST", f"/knowledge-bases/{kb_id}/query", body={
            "idempotency_key": q_idem,
            "question": "ANI 知识库平台支持哪三种检索模式？RRF 公式是什么？",
            "top_k": 4,
        }, expected_status=200, label="Query",
        desc="前置: 知识库查询（既有接口；返回 answer/sources/session_id，"
             "产生本批次会话/消息/引用接口所需的测试数据）")
    session_id = answer.get("session_id", "") if isinstance(answer, dict) else ""
    sources = answer.get("sources", []) if isinstance(answer, dict) else []
    check("STEP3 Query -> 200 + session_id", status == 200 and bool(session_id),
          f"status={status} session_id={session_id} sources={len(sources)}")
    if not session_id:
        return 1
    log(f"  session_id={session_id} sources={len(sources)}")

    # ════════════════════════════════════════════════════════════════════
    # 8 NEW ENDPOINTS (SPEC §4.3)
    # ════════════════════════════════════════════════════════════════════

    # ── #5 PUT /knowledge-bases/{kb_id} — updateKnowledgeBase ──
    log("════ TEST #5: PUT /knowledge-bases/{kb_id} (updateKnowledgeBase) ════")
    upd_idem = str(uuid.uuid4())
    idem_keys.append(upd_idem)
    status, upd = rest_request(
        "PUT", f"/knowledge-bases/{kb_id}", body={
            "idempotency_key": upd_idem,
            "name": f"kbnew-kb-renamed-{E2E_TAG}",
            "description": "统一 e2e：更新后的描述（#5 UpdateKB 已验证）",
        }, expected_status=200, label="#5 UpdateKB",
        desc="新接口#5: 更新知识库名称与描述（幂等键 + 部分更新语义，空字段=不变）")
    upd_ok = (isinstance(upd, dict)
              and upd.get("name") == f"kbnew-kb-renamed-{E2E_TAG}"
              and "更新后的描述" in str(upd.get("description", ""))
              and upd.get("id") == kb_id)
    check("#5a UpdateKB -> 200 + fields persisted",
          status == 200 and upd_ok, f"status={status} body={upd}")
    # replay same key -> identical response
    status, upd2 = rest_request(
        "PUT", f"/knowledge-bases/{kb_id}", body={
            "idempotency_key": upd_idem,
            "name": f"kbnew-kb-renamed-{E2E_TAG}",
            "description": "统一 e2e：更新后的描述（#5 UpdateKB 已验证）",
        }, expected_status=200, label="#5 UpdateKB replay",
        desc="新接口#5 幂等回放: 同一幂等键重复提交 → 200 且返回与首次一致的结果")
    check("#5b UpdateKB idempotent replay -> 200 + same JSON",
          status == 200 and upd == upd2, f"status={status}")
    # negative: unknown kb
    status, payload = rest_request(
        "PUT", f"/knowledge-bases/{uuid.uuid4()}", body={
            "idempotency_key": str(uuid.uuid4()),
            "name": "ghost",
        }, expected_status=404, label="#5 UpdateKB 404",
        desc="新接口#5 负例: 更新不存在的知识库 → 404")
    check("#5c UpdateKB unknown kb -> 404", status == 404,
          f"status={status} msg={_msg_of(payload)!r}")

    # ── #10 GET /knowledge-bases/{kb_id}/documents/{doc_id} ──
    log("════ TEST #10: GET /knowledge-bases/{kb_id}/documents/{doc_id} "
        "(getKnowledgeBaseDocument) ════")
    status, doc = rest_request(
        "GET", f"/knowledge-bases/{kb_id}/documents/{doc_id}",
        expected_status=200, label="#10 GetDocument",
        desc="新接口#10: 查询文档详情（解析状态/chunk 数/存储路径等全字段）")
    doc_ok = (isinstance(doc, dict) and doc.get("id") == doc_id
              and doc.get("parse_status") == "ready"
              and int(doc.get("chunk_count", 0) or 0) > 0)
    check("#10a GetDocument -> 200 + id match + ready",
          status == 200 and doc_ok, f"status={status} body={doc}")
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb_id}/documents/{uuid.uuid4()}",
        expected_status=404, label="#10 GetDocument 404",
        desc="新接口#10 负例: 查询不存在的文档 → 404")
    check("#10b GetDocument unknown doc -> 404", status == 404,
          f"status={status} msg={_msg_of(payload)!r}")

    # ── #11 GET /knowledge-bases/{kb_id}/documents/{doc_id}/chunks ──
    log("════ TEST #11: GET .../documents/{doc_id}/chunks "
        "(listKnowledgeBaseDocumentChunks) ════")
    status, chunks = rest_request(
        "GET", f"/knowledge-bases/{kb_id}/documents/{doc_id}/chunks?limit=20",
        expected_status=200, label="#11 ListChunks",
        desc="新接口#11: 分页列出文档解析产生的语义块（chunk_type/页码/父块/内容）")
    items = chunks.get("items", []) if isinstance(chunks, dict) else []
    chunk_ok = (isinstance(chunks, dict) and len(items) > 0
                and all("content" in it for it in items[:3]))
    check("#11a ListChunks -> 200 + non-empty items",
          status == 200 and chunk_ok,
          f"status={status} items={len(items)} "
          f"total={chunks.get('total') if isinstance(chunks, dict) else ''}")
    if items:
        # find a child chunk_type if any, to exercise the filter param
        ctypes = {it.get("chunk_type") for it in items}
        probe_type = "child" if "child" in ctypes else next(iter(ctypes))
        status, filtered = rest_request(
            "GET", f"/knowledge-bases/{kb_id}/documents/{doc_id}/chunks"
                   f"?chunk_type={probe_type}&limit=20",
            expected_status=200, label="#11 ListChunks filtered",
            desc=f"新接口#11 过滤: chunk_type={probe_type}（返回值应全部为该类型）")
        fitems = filtered.get("items", []) if isinstance(filtered, dict) else []
        check("#11b ListChunks chunk_type filter homogeneous",
              status == 200 and len(fitems) > 0
              and all(it.get("chunk_type") == probe_type for it in fitems),
              f"status={status} filtered={len(fitems)} of {len(items)} "
              f"(types={sorted(str(t) for t in ctypes)})")
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb_id}/documents/{uuid.uuid4()}/chunks",
        expected_status=404, label="#11 ListChunks 404",
        desc="新接口#11 负例: 不存在的文档 → 404")
    check("#11c ListChunks unknown doc -> 404", status == 404,
          f"status={status}")

    # ── #12 POST .../documents/{doc_id}/reparse — reparseKnowledgeBaseDocument ──
    log("════ TEST #12: POST .../documents/{doc_id}/reparse "
        "(reparseKnowledgeBaseDocument) ════")
    # reparse only accepts non-'ready' docs; seed a failed doc for the happy
    # path (the normal pipeline never produces 'failed' on demand)
    doc_fail = _run_async(_insert_doc(kb_id, f"kbnew-failed-{E2E_TAG}.md",
                                      "failed", "parse error: seeded"))
    log(f"  seeded reparse target doc (failed): {doc_fail}")
    rp_idem = str(uuid.uuid4())
    idem_keys.append(rp_idem)
    guard = _OutboxGuardThread(doc_fail)
    guard.start()
    status, taskref = rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents/{doc_fail}/reparse",
        body={"idempotency_key": rp_idem},
        expected_status=202, label="#12 Reparse",
        desc="新接口#12: 对 failed 文档发起重新解析 → 202 + 异步任务引用"
             "（task_type=kb.reparse, status=pending），文档状态重置 pending")
    guard.finish()
    if guard.result:
        log(f"  outbox shield applied: {guard.result} "
            f"({(guard.latency_ms or 0):.0f}ms)")
    task_ok = (isinstance(taskref, dict)
               and taskref.get("task_type") == "kb.reparse"
               and taskref.get("status") == "pending"
               and taskref.get("task_id"))
    check("#12a Reparse failed doc -> 202 + AsyncTaskRef shape",
          status == 202 and task_ok, f"status={status} body={taskref}")
    task_id = taskref.get("task_id", "") if isinstance(taskref, dict) else ""
    if task_id:
        task_row = _run_async(_fetch_task(task_id))
        check("#12b async_tasks row: kb.reparse / pending / resource=doc",
              bool(task_row) and task_row.get("task_type") == "kb.reparse"
              and task_row.get("status") == "pending"
              and task_row.get("resource_id") == doc_fail,
              f"row={task_row}")
        fstate = _run_async(_fetch_doc_state(doc_fail))
        check("#12c doc reset: parse_status=pending, error cleared",
              bool(fstate) and fstate.get("parse_status") == "pending"
              and fstate.get("error_message") is None, f"state={fstate}")
    # replay same key -> same task_id
    status, taskref2 = rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents/{doc_fail}/reparse",
        body={"idempotency_key": rp_idem},
        expected_status=202, label="#12 Reparse replay",
        desc="新接口#12 幂等回放: 同一幂等键重复提交 → 202 且返回同一 task_id")
    check("#12d Reparse replay -> same task_id",
          status == 202 and isinstance(taskref2, dict)
          and taskref2.get("task_id") == task_id,
          f"status={status}")
    # negative: ready doc -> 409
    status, payload = rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents/{doc_id}/reparse",
        body={"idempotency_key": str(uuid.uuid4())},
        expected_status=409, label="#12 Reparse 409 ready",
        desc="新接口#12 负例: 对 ready 文档重解析 → 409（守卫仅拒绝 ready 状态）")
    check("#12e Reparse ready doc -> 409 CONFLICT",
          status == 409 and "ready" in _msg_of(payload),
          f"status={status} msg={_msg_of(payload)!r}")
    # negative: non-uuid idempotency key -> 400
    status, payload = rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents/{doc_fail}/reparse",
        body={"idempotency_key": "not-a-uuid"},
        expected_status=400, label="#12 Reparse 400 key",
        desc="新接口#12 负例: 非 uuid 幂等键 → 400")
    check("#12f Reparse non-uuid key -> 400",
          status == 400 and "must be a uuid" in _msg_of(payload),
          f"status={status} msg={_msg_of(payload)!r}")
    # negative: cross-tenant -> 404
    status, payload = rest_request(
        "POST", f"/knowledge-bases/{kb_id}/documents/{doc_fail}/reparse",
        body={"idempotency_key": str(uuid.uuid4())},
        expected_status=404, label="#12 Reparse cross-tenant",
        headers=OTHER_HDRS,
        desc="新接口#12 负例: 其他租户访问 → 404（RLS 租户隔离）")
    check("#12g Reparse cross-tenant -> 404", status == 404,
          f"status={status}")

    # ── #16 GET /knowledge-bases/{kb_id}/sessions ──
    log("════ TEST #16: GET /knowledge-bases/{kb_id}/sessions "
        "(listKnowledgeBaseSessions) ════")
    status, sessions = rest_request(
        "GET", f"/knowledge-bases/{kb_id}/sessions?limit=10",
        expected_status=200, label="#16 ListSessions",
        desc="新接口#16: 列出知识库查询会话（含消息数/最后提问/最后活跃时间聚合）")
    s_items = sessions.get("items", []) if isinstance(sessions, dict) else []
    s_ok = (isinstance(sessions, dict) and len(s_items) >= 1
            and any(it.get("id") == session_id for it in s_items))
    check("#16a ListSessions -> 200 + our session present",
          status == 200 and s_ok,
          f"status={status} items={len(s_items)}")
    s_row = next((it for it in s_items if it.get("id") == session_id), {})
    if s_row:
        check("#16b session aggregates (message_count/last_query)",
              int(s_row.get("message_count", 0) or 0) >= 2
              and bool(s_row.get("last_query")),
              f"row={ {k: s_row.get(k) for k in ('id', 'message_count', 'last_query', 'last_active_at')} }")
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{uuid.uuid4()}/sessions",
        expected_status=404, label="#16 ListSessions 404",
        desc="新接口#16 负例: 不存在的知识库 → 404")
    check("#16c ListSessions unknown kb -> 404", status == 404,
          f"status={status}")

    # ── #17 GET /knowledge-bases/{kb_id}/sessions/{sid}/messages ──
    log("════ TEST #17: GET .../sessions/{sid}/messages "
        "(listKnowledgeBaseSessionMessages) ════")
    status, messages = rest_request(
        "GET", f"/knowledge-bases/{kb_id}/sessions/{session_id}/messages?limit=10",
        expected_status=200, label="#17 ListMessages",
        desc="新接口#17: 列出会话内消息（user 与 assistant 交替，assistant 含来源引用）")
    m_items = messages.get("items", []) if isinstance(messages, dict) else []
    m_ok = (isinstance(messages, dict) and len(m_items) >= 2
            and m_items[0].get("role") == "user")
    check("#17a ListMessages -> 200 + user-first order",
          status == 200 and m_ok,
          f"status={status} items={len(m_items)} "
          f"roles={[it.get('role') for it in m_items]}")
    if m_items:
        assistant = next((it for it in m_items if it.get("role") == "assistant"), None)
        check("#17b assistant message carries sources",
              bool(assistant) and bool(assistant.get("sources")),
              f"sources={len(assistant.get('sources') or []) if assistant else 0}")
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb_id}/sessions/{uuid.uuid4()}/messages",
        expected_status=404, label="#17 ListMessages 404",
        desc="新接口#17 负例: 不存在的会话 → 404")
    check("#17c ListMessages unknown session -> 404", status == 404,
          f"status={status}")

    # ── #15 GET /knowledge-bases/{kb_id}/citations ──
    log("════ TEST #15: GET /knowledge-bases/{kb_id}/citations "
        "(listKnowledgeBaseCitations) ════")
    status, citations = rest_request(
        "GET", f"/knowledge-bases/{kb_id}/citations?limit=20",
        expected_status=200, label="#15 ListCitations",
        desc="新接口#15: 列出知识库引用（从会话 assistant 消息的 source_chunks 展开，"
             "含确定性引用 ID、文档、评分）")
    c_items = citations.get("items", []) if isinstance(citations, dict) else []
    c_ok = (isinstance(citations, dict) and len(c_items) >= 1
            and all(it.get("id") and it.get("doc_id") for it in c_items[:3]))
    check("#15a ListCitations -> 200 + deterministic ids + doc backfill",
          status == 200 and c_ok,
          f"status={status} items={len(c_items)}")
    if c_items:
        # deterministic uuid5: same call twice -> identical bodies
        status2, citations2 = rest_request(
            "GET", f"/knowledge-bases/{kb_id}/citations?limit=20",
            expected_status=200, label="#15 ListCitations repeat",
            desc="新接口#15 确定性: 两次调用引用 ID 一致（uuid5 确定性生成）")
        c_items2 = citations2.get("items", []) if isinstance(citations2, dict) else []
        check("#15b citation ids deterministic across calls",
              status2 == 200 and c_items == c_items2,
              f"same={c_items == c_items2}")
        check("#15c citations reference our real doc",
              any(it.get("doc_id") == doc_id for it in c_items),
              f"doc_ids={sorted({it.get('doc_id') for it in c_items})}")
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{uuid.uuid4()}/citations",
        expected_status=404, label="#15 ListCitations 404",
        desc="新接口#15 负例: 不存在的知识库 → 404")
    check("#15d ListCitations unknown kb -> 404", status == 404,
          f"status={status}")

    # ── #18 DELETE /knowledge-bases/{kb_id}/sessions/{sid} ──
    log("════ TEST #18: DELETE /knowledge-bases/{kb_id}/sessions/{sid} "
        "(deleteKnowledgeBaseSession) ════")
    status, payload = rest_request(
        "DELETE", f"/knowledge-bases/{kb_id}/sessions/{session_id}",
        expected_status=204, label="#18 DeleteSession",
        desc="新接口#18: 删除会话（DB 硬删 + Redis 缓存清理；幂等 204）")
    check("#18a DeleteSession -> 204", status == 204, f"status={status}")
    # idempotent replay -> 204 again
    status, payload = rest_request(
        "DELETE", f"/knowledge-bases/{kb_id}/sessions/{session_id}",
        expected_status=204, label="#18 DeleteSession replay",
        desc="新接口#18 幂等回放: 删除已删除的会话 → 仍 204")
    check("#18b DeleteSession idempotent replay -> 204", status == 204,
          f"status={status}")
    # messages after delete -> 404
    status, payload = rest_request(
        "GET", f"/knowledge-bases/{kb_id}/sessions/{session_id}/messages",
        expected_status=404, label="#18 verify messages gone",
        desc="新接口#18 验证: 会话删除后其消息不可再访问 → 404")
    check("#18c messages after delete -> 404", status == 404,
          f"status={status}")
    # unknown kb -> 404
    status, payload = rest_request(
        "DELETE", f"/knowledge-bases/{uuid.uuid4()}/sessions/{session_id}",
        expected_status=404, label="#18 DeleteSession 404",
        desc="新接口#18 负例: 不存在的知识库 → 404")
    check("#18d DeleteSession unknown kb -> 404", status == 404,
          f"status={status}")

    # ════ cleanup ════
    log("════ CLEANUP ════")
    try:
        _run_async(_purge_seeded_rows(kb_id, idem_keys))
        check("cleanup: rows purged", True)
    except Exception as e:
        check("cleanup: rows purged", False, repr(e))
    status, payload = rest_request(
        "DELETE", f"/knowledge-bases/{kb_id}",
        expected_status=204, label="cleanup DeleteKB",
        desc="清理: 软删知识库（行保留审计；既有稳定接口）")
    check("cleanup DeleteKB -> 204", status == 204, f"status={status}")
    return 0


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    _open_log()
    log("════════════════════════════════════════════════════════════════")
    log(f"  统一 E2E：KB API 补全批次 8 个新接口 — tag={E2E_TAG}")
    log(f"  tenant={TENANT_ID}")
    log(f"  log file: {E2E_LOG}")
    log("════════════════════════════════════════════════════════════════")

    if not GATEWAY_EXE.exists():
        log(f"FATAL: gateway exe not found at {GATEWAY_EXE} — run go build first")
        return 1

    log("── Killing stale processes on ports 8080/8001/8002/50052/50053 ──")
    _kill_stale_processes()

    try:
        # 1. start rag-engine first (kb-service parse consumer needs its gRPC)
        log("── Starting rag-engine (:8001 / gRPC :50052) ──")
        start_rag_engine()
        if not _wait_http("http://localhost:8001/health", 90):
            log("FATAL: rag-engine not ready")
            return 1

        # 2. start kb-service (FastAPI :8002 / gRPC :50053, parse consumer ON)
        log("── Starting kb-service (:8002 / gRPC :50053, parse_consumer=ON) ──")
        start_kb_service()
        if not _wait_http("http://localhost:8002/health", 90):
            log("WARNING: kb-service /health not ready — continuing")

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

        # 3. start gateway
        log("── Starting ani-gateway (:8080) ──")
        start_gateway()
        if not _wait_http(
                "http://localhost:8080/api/v1/svc/knowledge-bases?limit=1", 60):
            log("FATAL: gateway not ready")
            return 1

        # warm up gateway->kb-service gRPC channel (poll until non-503)
        deadline = time.time() + 30
        warmed = False
        while time.time() < deadline:
            status, _ = rest_request(
                "GET", "/knowledge-bases?limit=1", label="grpc-channel warmup",
                desc="通道预热: 轮询直到网关→kb-service gRPC 通道就绪（非 503）",
                timeout=10)
            if status != 503:
                warmed = True
                break
            time.sleep(2)
        if not warmed:
            log("FATAL: gateway→kb-service gRPC channel still 503 after 30s")
            return 1
        log("  gateway→kb-service gRPC channel warm")

        # pre-flight: tenant rows
        log("════ PRE-FLIGHT: ensure tenant rows ════")
        try:
            _run_async(_ensure_tenant_rows())
        except Exception as e:
            log(f"  ensure tenant rows failed (may already exist): {e!r}")

        # 4. run E2E
        rc = run_e2e()

        # 5. summary
        n_pass = sum(1 for v in RESULTS.values() if v == "pass")
        n_fail = sum(1 for v in RESULTS.values() if v == "fail")
        log("══════════════════════════ RESULT SUMMARY ══════════════════════════")
        log(f"  PASS: {n_pass}   FAIL: {n_fail}   total: {len(RESULTS)}")
        log("  新接口覆盖: #5 UpdateKB / #10 GetDocument / #11 ListChunks / "
            "#12 Reparse / #15 ListCitations / #16 ListSessions / "
            "#17 ListMessages / #18 DeleteSession")
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
