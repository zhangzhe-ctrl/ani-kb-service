"""kb_outbox_events repository (SPEC §2.4, §6.1; KB-SPLIT-P1A issue-050).

Covers the outbox pattern: CompleteDocumentUpload writes an outbox event in
the same transaction as the business write, and a separate dispatcher polls
undispatched events to publish to NATS (US-010 wires the dispatcher).

KB-SPLIT-P1A (ADR 0001 §D4): the table is kb-service's OWN outbox table
(`kb_outbox_events`, migrations/009_kb_outbox.sql) — transactional outbox is
a per-service asset, so kb no longer reads or writes the shared-library
outbox_events. pending→published state machine and dispatcher polling
semantics are unchanged; only the data landing place moved.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

import asyncpg

from .rls import set_tenant_context


async def insert_event(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> int:
    """INSERT a kb_outbox_events row and return its id (tenant-filtered).

    Called inside the same transaction as the business write so the event is
    committed atomically with the business state change (SPEC §6.1).
    """
    payload_json = json.dumps(payload or {}, default=str)
    # NOTE: this must be called inside an existing transaction so the outbox
    # row commits only if the business write also commits. Callers should
    # NOT wrap in a new transaction here (they manage the tx externally).
    await set_tenant_context(conn, tenant_id)
    row = await conn.fetchrow(
        """
        INSERT INTO kb_outbox_events
            (aggregate_type, aggregate_id, event_type, tenant_id, payload)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        aggregate_type,
        uuid.UUID(aggregate_id),
        event_type,
        uuid.UUID(tenant_id),
        payload_json,
    )
    return int(row["id"])


async def list_undispatched(
    conn: asyncpg.Connection, *, limit: int = 100
) -> list[dict[str, Any]]:
    """List undispatched kb_outbox_events rows ordered by created_at.

    Used by the dispatcher worker. Deliberately cross-tenant (no tenant
    filter): the dispatcher polls the whole outbox across tenants, as the
    platform-bypass semantics required since 009 — with RLS gone (#054)
    this is simply an unfiltered query.
    """
    rows = await conn.fetch(
        """
        SELECT id, aggregate_type, aggregate_id, event_type, tenant_id,
               payload, published, published_at, created_at
          FROM kb_outbox_events
         WHERE published = FALSE
         ORDER BY created_at ASC
         LIMIT $1
        """,
        limit,
    )
    return [dict(r) for r in rows]


async def count_undispatched(conn: asyncpg.Connection) -> int:
    """Count undispatched kb_outbox_events rows (outbox backlog gauge).

    KB-SPLIT-P3C (issue-063): feeds ``kb_outbox_undispatched_events`` so a
    rising backlog with a healthy dispatcher is visible as NATS
    unreachability (designed degradation). Cross-tenant, unfiltered — the
    dispatcher owns this read (same semantics as list_undispatched).
    """
    row = await conn.fetchval(
        "SELECT COUNT(*) FROM kb_outbox_events WHERE published = FALSE"
    )
    return int(row or 0)


async def mark_dispatched(
    conn: asyncpg.Connection, *, event_id: int
) -> bool:
    """Mark a kb_outbox_events row as published.

    Deliberately cross-tenant (marks by primary key): the dispatcher owns
    this UPDATE and processes events of every tenant, which has been the
    correct behavior since 009's platform-bypass semantics.
    """
    result = await conn.execute(
        """
        UPDATE kb_outbox_events
           SET published = TRUE, published_at = now()
         WHERE id = $1
        """,
        event_id,
    )
    return result == "UPDATE 1"


async def mark_dispatched_batch(
    conn: asyncpg.Connection, *, event_ids: list[int]
) -> int:
    """Mark multiple kb_outbox_events rows as published in one UPDATE.

    Deliberately cross-tenant (no tenant filter, same semantics as
    mark_dispatched): the dispatcher marks by primary key across all
    tenants — the platform-bypass shape required since 009 — with RLS
    gone (#054) this is simply an unfiltered UPDATE by id.
    """
    if not event_ids:
        return 0
    result = await conn.execute(
        """
        UPDATE kb_outbox_events
           SET published = TRUE, published_at = now()
         WHERE id = ANY($1::int[])
        """,
        event_ids,
    )
    # asyncpg execute returns "UPDATE N"; parse the count.
    try:
        return int(result.split()[-1])
    except (IndexError, ValueError):
        return 0
