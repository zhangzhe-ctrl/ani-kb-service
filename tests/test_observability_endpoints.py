"""Observability run-contract tests (KB-SPLIT-P3C, issue-063).

Covers the issue's behavioural contracts that were previously only provable
by code review:

1. ``/readyz`` gating semantics — ``ok`` is gated by exactly db pool +
   outbox dispatcher + gRPC server; ``session_cache`` / consumers are
   reported but never gate; the reported dependency health (NATS / rag-engine
   / Milvus / MinIO) never gates (AC1).
2. ``/metrics`` serialises kb's private registry in Prometheus text format
   (AC7) and every metric family uses only bounded labels — identifiers
   (tenant_id / kb_id / document_id / run_id) are never labels (AC2).
3. The four-element log-correlation helper renders/omits correctly (AC3).

The FastAPI app is exercised through ``httpx.ASGITransport`` (no lifespan),
so the real DB pools / gRPC loop are never started and the process globals
are monkeypatched per test.
"""
import os
import sys

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

import httpx  # noqa: E402
import pytest  # noqa: E402

import main  # noqa: E402
from app.observability import metrics  # noqa: E402
from app.observability.correlation import corr  # noqa: E402
from app.repositories import outbox as outbox_repo  # noqa: E402

# Identifiers that must NEVER appear as metric labels (ADR §五-阶段3:
# 指标标签控制基数，无高基数标签).
_FORBIDDEN_LABELS = {"tenant_id", "kb_id", "document_id", "doc_id", "run_id"}

_ALL_METRIC_FAMILIES = [
    "kb_outbox_undispatched_events",
    "kb_consumer_inflight",
    "kb_consumer_retries_total",
    "kb_parse_run_duration_seconds",
    "kb_query_no_result_gate_total",
    "kb_degradation_total",
    "kb_presign_operations_total",
]


class _FakePool:
    """Minimal stand-in for an asyncpg pool — readyz only reads ``_closed``."""

    def __init__(self, *, closed=False):
        self._closed = closed


async def _get(path):
    """Issue a request straight to the ASGI app without running the lifespan."""
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        return await c.get(path)


async def _all_dependencies_down():
    return {
        "nats": {"healthy": False, "detail": "down"},
        "redis": {"healthy": False, "detail": "down"},
        "rag_engine": {"healthy": False, "detail": "down"},
        "milvus": {"healthy": False, "detail": "down"},
        "minio": {"healthy": False, "detail": "down"},
    }


def _set_core_gates(monkeypatch, *, db="up", dispatcher="up", grpc="up"):
    monkeypatch.setattr(main, "_db_pool", None if db == "down"
                        else _FakePool(closed=(db == "closed")))
    monkeypatch.setattr(main, "_outbox_pool", _FakePool())
    monkeypatch.setattr(main, "_outbox_dispatcher",
                        None if dispatcher == "down" else object())
    monkeypatch.setattr(main, "_grpc_server", None if grpc == "down" else object())
    # Non-gating components, deliberately absent.
    monkeypatch.setattr(main, "_session_cache", None)
    monkeypatch.setattr(main, "_parse_consumer", None)
    monkeypatch.setattr(main, "_rebuild_consumer", None)
    monkeypatch.setattr(main, "_probe_dependencies", _all_dependencies_down)


async def test_readyz_ok_when_core_gates_up_even_if_dependencies_down(monkeypatch):
    """AC1: db + outbox dispatcher + gRPC up ⇒ ok, regardless of NATS /
    rag-engine / Milvus / MinIO reachability (report-only, never gating)."""
    _set_core_gates(monkeypatch)

    resp = await _get("/readyz")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    components = body["components"]
    assert components["db"] is True
    assert components["outbox_dispatcher"] is True
    assert components["grpc"] is True
    # Non-gating components are reported (False) but did not degrade status.
    assert components["session_cache"] is False
    assert components["parse_consumer"] is False
    assert components["rebuild_consumer"] is False
    # Dependency health is surfaced for observability only.
    assert body["dependencies"]["nats"]["healthy"] is False
    assert body["dependencies"]["redis"]["healthy"] is False
    assert body["dependencies"]["milvus"]["healthy"] is False


@pytest.mark.parametrize(
    "broken",
    [
        {"db": "down"},
        {"db": "closed"},
        {"dispatcher": "down"},
        {"grpc": "down"},
    ],
)
async def test_readyz_degraded_when_any_core_gate_missing(monkeypatch, broken):
    """AC1: losing any one gating component flips status to degraded."""
    _set_core_gates(monkeypatch, **broken)

    resp = await _get("/readyz")

    assert resp.status_code == 200
    assert resp.json()["status"] == "degraded"


async def test_readyz_route_registered():
    """AC1: the readiness route is registered on the app."""
    paths = {getattr(r, "path", None) for r in main.app.routes}
    assert "/readyz" in paths


async def test_probe_tcp_never_raises_on_unparseable_addr():
    """AC1: a malformed dependency addr yields an unhealthy report, no raise."""
    result = await main._probe_tcp("no-port-here")
    assert result["healthy"] is False
    assert "unparseable" in result["detail"]


async def test_probe_tcp_unreachable_is_reported_not_raised():
    """AC1: an unreachable endpoint is reported unhealthy without raising."""
    result = await main._probe_tcp("256.256.256.256:1", timeout=0.2)
    assert result["healthy"] is False
    assert result["detail"]


async def test_probe_dependencies_reports_redis_and_never_gates(monkeypatch):
    """Redis is surfaced as a report-only dependency: an unreachable Redis
    (probed from settings.redis_url) yields an unhealthy report and must NOT
    flip /readyz to degraded — Redis down is in-design DB-only degradation."""
    real_probe = main._probe_dependencies
    monkeypatch.setattr(main.settings, "redis_url", "redis://127.0.0.1:1/0")
    _set_core_gates(monkeypatch)
    # Restore the real probe so the redis derivation under test actually runs.
    monkeypatch.setattr(main, "_probe_dependencies", real_probe)

    resp = await _get("/readyz")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["dependencies"]["redis"]["healthy"] is False


async def test_metrics_endpoint_serves_prometheus_text():
    """AC7: /metrics returns the six domain families in Prometheus text
    exposition format (scrapeable by #066)."""
    # Touch every family so labelled metrics emit at least one sample.
    metrics.set_outbox_undispatched(3)
    metrics.set_consumer_inflight("parse", 1)
    metrics.inc_consumer_retry("parse")
    metrics.observe_parse_run("success", 1.5)
    metrics.inc_no_result_gate("empty_retrieval")
    metrics.inc_degradation("nats", "disconnected")
    metrics.inc_presign("presign_put", True)

    resp = await _get("/metrics")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert "version=0.0.4" in resp.headers["content-type"]
    text = resp.text
    for family in _ALL_METRIC_FAMILIES:
        assert family in text
    # Gauge reflects the recorded backlog value.
    assert "kb_outbox_undispatched_events 3.0" in text


async def test_metrics_route_registered():
    """AC7: the scrape route is registered on the app."""
    paths = {getattr(r, "path", None) for r in main.app.routes}
    assert "/metrics" in paths


async def test_metric_labels_are_bounded_no_identifiers():
    """AC2: no metric family uses a high-cardinality identifier as a label."""
    # Populate every label dimension so all children exist in the registry.
    metrics.set_consumer_inflight("parse", 0)
    metrics.inc_consumer_retry("rebuild")
    metrics.observe_parse_run("failure", 0.1)
    metrics.inc_no_result_gate("low_score")
    metrics.inc_degradation("redis", "unavailable")
    metrics.inc_presign("complete", False)

    seen_labels: set[str] = set()
    for metric in metrics.REGISTRY.collect():
        for sample in metric.samples:
            seen_labels.update(sample.labels.keys())

    assert not (seen_labels & _FORBIDDEN_LABELS), (
        f"high-cardinality identifier leaked as a label: "
        f"{seen_labels & _FORBIDDEN_LABELS}"
    )


async def test_render_metrics_returns_prometheus_bytes():
    """AC7: the serialiser returns Prometheus text bytes."""
    out = metrics.render_metrics()
    assert isinstance(out, bytes)
    assert b"kb_outbox_undispatched_events" in out


async def test_count_undispatched_returns_int_backlog():
    """AC2: the outbox backlog read returns an int for the gauge."""

    class _Conn:
        async def fetchval(self, sql):
            assert "kb_outbox_events" in sql
            assert "published = FALSE" in sql
            return 7

    assert await outbox_repo.count_undispatched(_Conn()) == 7


async def test_count_undispatched_none_coalesces_to_zero():
    """AC2: a NULL count (no rows) is coerced to 0, never None."""

    class _Conn:
        async def fetchval(self, sql):
            return None

    assert await outbox_repo.count_undispatched(_Conn()) == 0


def test_corr_renders_all_four_elements():
    """AC3: request / kb / document / run_id all render as key=value."""
    suffix = corr(
        request_id="req-1", kb_id="kb-1", doc_id="doc-1", run_id="run-1"
    )
    assert suffix == "request_id=req-1 kb_id=kb-1 doc_id=doc-1 run_id=run-1"


def test_corr_omits_missing_elements():
    """AC3: absent elements are omitted (safe to append unconditionally)."""
    assert corr(kb_id="kb-1", run_id="run-1") == "kb_id=kb-1 run_id=run-1"
    assert corr() == ""
