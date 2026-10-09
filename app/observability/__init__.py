"""Observability package: Prometheus domain metrics (KB-SPLIT-P3C, issue-063).

The registry and metric objects here are module-level singletons so every
call site (outbox dispatcher, consumers, orchestrators, session cache, gRPC
servicers) records into the same process-wide registry exposed by the
``/metrics`` endpoint in ``main.py``.

CARDINALITY RULE (ADR §五-阶段3/4): labels are LOW-cardinality only —
bounded enumerations (component names, event types, gate names, outcomes).
Identifiers such as tenant_id / kb_id / document_id / run_id MUST NEVER be
used as label values; they are unbounded and would blow up the registry.
"""
