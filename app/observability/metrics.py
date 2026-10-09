"""Domain metrics for kb-service (KB-SPLIT-P3C, issue-063; ADR §五-阶段3/4).

Six metric families covering the run-contract observability requirements:

  1. ``kb_outbox_undispatched_events``        — outbox backlog gauge
  2. ``kb_consumer_inflight``                 — consumer in-flight tasks gauge
     ``kb_consumer_retries_total``            — consumer poison/retry counter
  3. ``kb_parse_run_duration_seconds``        — parse run duration histogram
  4. ``kb_query_no_result_gate_total``        — three no-result gate hits
  5. ``kb_degradation_total``                 — designed-degradation events
  6. ``kb_presign_operations_total``          — presign sign / complete success

All labels are LOW-cardinality bounded enumerations. Identifiers (tenant_id /
kb_id / document_id / run_id) are deliberately NOT used as labels (ADR
§五-阶段3: 指标标签控制基数，无高基数标签).

The module exposes thin helper functions so call sites stay readable and the
metric names/labels live in one place. Helpers are no-ops if the metric is
somehow unavailable (defensive only — prometheus_client is a hard dependency
declared in requirements.txt).
"""
from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# A private registry keeps kb metrics isolated from the process-wide default
# registry (which python-grpc / httpx helpers may also populate), so /metrics
# emits exactly kb's domain metrics. main.py serialises this registry.
REGISTRY = CollectorRegistry()

# --- 1. Outbox backlog -------------------------------------------------------
# Undispatched events observed by the last dispatcher poll. A rising value with
# a healthy dispatcher means NATS is unreachable (designed degradation: events
# stay queued, at-least-once). No labels: the backlog is a single number.
OUTBOX_UNDISPATCHED = Gauge(
    "kb_outbox_undispatched_events",
    "Undispatched kb_outbox_events rows seen on the last dispatcher poll.",
    registry=REGISTRY,
)

# --- 2. Consumer in-flight / retries ----------------------------------------
# In-flight = NATS messages currently being handled by a consumer task. Retries
# = messages redelivered after an InProgress lease was not renewed / a crash.
CONSUMER_INFLIGHT = Gauge(
    "kb_consumer_inflight",
    "Consumer messages currently in flight (handled but not yet settled).",
    ["consumer"],
    registry=REGISTRY,
)
CONSUMER_RETRIES = Counter(
    "kb_consumer_retries_total",
    "Consumer message redeliveries observed (crash/lease-expiry path).",
    ["consumer"],
    registry=REGISTRY,
)

# --- 3. Parse run duration --------------------------------------------------
# Per parse-run wall-clock duration. ``outcome`` is a bounded enum
# (success/failure/skipped); run_id/document_id are NOT labels.
PARSE_RUN_DURATION = Histogram(
    "kb_parse_run_duration_seconds",
    "Wall-clock duration of a document parse run.",
    ["outcome"],
    # Buckets span the expected range (sub-second skip checks → minutes of
    # parsing for large documents).
    buckets=(0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600),
    registry=REGISTRY,
)

# --- 4. Three no-result gates -----------------------------------------------
# Hits on the query path's no-result gates. ``gate`` is a bounded enum:
#   empty_retrieval — retrieval returned nothing
#   low_score       — top score below threshold
#   empty_sources   — sources empty after de-duplication
NO_RESULT_GATE = Counter(
    "kb_query_no_result_gate_total",
    "Query-path no-result gate hits, by gate.",
    ["gate"],
    registry=REGISTRY,
)

# --- 5. Degradation events --------------------------------------------------
# Designed-degradation transitions (NATS down → outbox backlog; Redis down →
# session DB-only; JetStream publish fallback to core publish). ``component``
# is a bounded enum; ``reason`` is a bounded enum.
DEGRADATION = Counter(
    "kb_degradation_total",
    "Designed-degradation events, by component and reason.",
    ["component", "reason"],
    registry=REGISTRY,
)

# --- 6. Presign / complete upload success -----------------------------------
# Upload-path outcomes. ``operation``: presign_put | presign_get | complete.
# ``outcome``: success | failure | idempotent.
# P3-4: an idempotent replay (a prior complete returned for the same key) is
# its OWN outcome, not ``success`` — otherwise client retries of an already
# completed upload would inflate the true first-upload success rate.
PRESIGN_OPERATIONS = Counter(
    "kb_presign_operations_total",
    "Object-store presign / upload-complete operations, by operation and outcome.",
    ["operation", "outcome"],
    registry=REGISTRY,
)


# --- Helper API (call sites use these) --------------------------------------

def set_outbox_undispatched(count: int) -> None:
    """Record the outbox backlog observed on a dispatcher poll."""
    OUTBOX_UNDISPATCHED.set(count)


def set_consumer_inflight(consumer: str, count: int) -> None:
    """Record the in-flight message count for a consumer."""
    CONSUMER_INFLIGHT.labels(consumer=consumer).set(count)


def inc_consumer_retry(consumer: str) -> None:
    """Count a consumer message redelivery."""
    CONSUMER_RETRIES.labels(consumer=consumer).inc()


def observe_parse_run(outcome: str, duration_seconds: float) -> None:
    """Observe a parse-run duration with a bounded outcome label."""
    PARSE_RUN_DURATION.labels(outcome=outcome).observe(duration_seconds)


def inc_no_result_gate(gate: str) -> None:
    """Count a query-path no-result gate hit (bounded gate enum)."""
    NO_RESULT_GATE.labels(gate=gate).inc()


def inc_degradation(component: str, reason: str) -> None:
    """Count a designed-degradation event."""
    DEGRADATION.labels(component=component, reason=reason).inc()


def inc_presign(operation: str, ok: bool) -> None:
    """Count a presign/complete operation outcome."""
    PRESIGN_OPERATIONS.labels(
        operation=operation, outcome="success" if ok else "failure"
    ).inc()


def inc_presign_idempotent(operation: str) -> None:
    """Count an idempotent replay (P3-4): kept out of the success rate."""
    PRESIGN_OPERATIONS.labels(operation=operation, outcome="idempotent").inc()


def render_metrics() -> bytes:
    """Serialise the kb metric registry in Prometheus text format."""
    return generate_latest(REGISTRY)
