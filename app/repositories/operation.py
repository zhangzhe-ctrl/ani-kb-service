"""kb_operations repository (KB-SPLIT-P1B issue-051, ADR 0001 §D4).

Idempotency replay is kb-local state: CreateKB / UpdateKB /
CompleteDocumentUpload / ReparseDocument / RebuildKB / UpdateKBConfig /
UpdateKBPermissions record their idempotency_key results here so retries
return the same response (SPEC §6.1, §6.4). kb no longer reads or writes
the shared async_tasks table — that table belongs to the future task
domain; task progress is visible externally only through outbox events
plus the ListOperations / GetOperation query RPCs (KB-SPLIT-P1C issue-052).
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

import asyncpg

from .cursor import parse_composite_cursor
from .rls import set_tenant_context


async def find_by_idempotency_key(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    idempotency_key: str,
    operation_type: str | None = None,
) -> dict[str, Any] | None:
    """Look up an existing kb_operation by (tenant_id, idempotency_key).

    `operation_type` narrows the match to one operation kind (e.g.
    'kb.update'), so the same client uuid reused across different
    operations in a tenant never replays another operation's result.

    Returns the row (including `result`) if a prior call with the same key
    succeeded, enabling idempotent replay (SPEC §6.1, §6.4).
    """
    async with conn.transaction():
        await set_tenant_context(conn, tenant_id)
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, idempotency_key, operation_type,
                   resource_id, status, payload, result,
                   started_at, completed_at, created_at, updated_at
              FROM kb_operations
             WHERE tenant_id = $1 AND idempotency_key = $2
               AND ($3::text IS NULL OR operation_type = $3)
            """,
            uuid.UUID(tenant_id),
            idempotency_key,
            operation_type,
        )
    return dict(row) if row else None


async def create_operation(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    idempotency_key: str,
    operation_type: str,
    resource_type: str | None = None,
    resource_id: str | None = None,
    payload: dict[str, Any] | None = None,
    status: str = "pending",
) -> dict[str, Any]:
    """INSERT a new kb_operations row and return it (tenant-filtered).

    `resource_type` has no dedicated column (the table carries
    operation_type/status/result); it is folded into `payload` so the
    recorded intent is preserved. The composite
    UNIQUE(tenant_id, idempotency_key) makes a duplicate insert raise
    asyncpg.UniqueViolationError; callers should check
    find_by_idempotency_key first.
    """
    async with conn.transaction():
        return await create_operation_in_tx(
            conn,
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            operation_type=operation_type,
            resource_type=resource_type,
            resource_id=resource_id,
            payload=payload,
            status=status,
        )


async def create_operation_in_tx(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    idempotency_key: str,
    operation_type: str,
    resource_type: str | None = None,
    resource_id: str | None = None,
    payload: dict[str, Any] | None = None,
    status: str = "pending",
) -> dict[str, Any]:
    """INSERT a kb_operations row inside the caller's transaction (tenant-filtered).

    Does NOT open its own transaction. Used by the notify / reparse /
    rebuild / update paths so the idempotency record commits atomically
    with the business write. `resource_type` has no dedicated column and
    is folded into `payload` (resource_id is a real column). The composite
    UNIQUE(tenant_id, idempotency_key) makes a duplicate insert raise
    asyncpg.UniqueViolationError.
    """
    body = dict(payload or {})
    if resource_type:
        body.setdefault("resource_type", resource_type)
    payload_json = json.dumps(body, default=str)
    await set_tenant_context(conn, tenant_id)
    row = await conn.fetchrow(
        """
        INSERT INTO kb_operations
            (tenant_id, idempotency_key, operation_type, resource_id,
             status, payload)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, tenant_id, idempotency_key, operation_type,
                  resource_id, status, payload, result,
                  started_at, completed_at, created_at, updated_at
        """,
        uuid.UUID(tenant_id),
        idempotency_key,
        operation_type,
        uuid.UUID(resource_id) if resource_id else None,
        status,
        payload_json,
    )
    return dict(row)


async def complete_operation(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    operation_id: str,
    result: dict[str, Any] | None = None,
    status: str = "completed",
) -> bool:
    """Mark an operation completed with its result (tenant-filtered)."""
    async with conn.transaction():
        return await complete_operation_in_tx(
            conn,
            tenant_id=tenant_id,
            operation_id=operation_id,
            result=result,
            status=status,
        )


async def complete_operation_in_tx(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    operation_id: str,
    result: dict[str, Any] | None = None,
    status: str = "completed",
) -> bool:
    """Mark an operation completed inside the caller's transaction (tenant-filtered).

    Does NOT open its own transaction. Used by UpdateKB/CreateKB so the
    kb_operations write commits atomically with the other statements in
    the same transaction (single-round-trip idempotency record).

    Terminal-state guard: refuses to overwrite a row already in a terminal
    state ('completed'/'failed') — under at-least-once redelivery,
    interleaved consumers must not overwrite each other's terminal state;
    the first completion wins.
    """
    result_json = json.dumps(result, default=str) if result else None
    await set_tenant_context(conn, tenant_id)
    res = await conn.execute(
        """
        UPDATE kb_operations
           SET status = $2, result = $3, completed_at = now(),
               updated_at = now()
         WHERE id = $1 AND tenant_id = $4
           AND status NOT IN ('completed', 'failed')
        """,
        uuid.UUID(operation_id),
        status,
        result_json,
        uuid.UUID(tenant_id),
    )
    return res == "UPDATE 1"


async def acquire_run(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    operation_id: str,
) -> bool:
    """Set a non-terminal operation to running (rebuild consumer pickup).

    Conditional UPDATE matching any non-terminal state:
      - status='pending' — the normal first pickup;
      - status='running' — orphan takeover: the previous consumer crashed
        without acking, and JetStream's redelivery is the takeover signal
        (a live handler holds the delivery lease via InProgress
        heartbeats, so a running rebuild is never redelivered mid-run).

    Terminal rows ('completed'/'failed') never re-acquire — a duplicate
    delivery of an already-processed rebuild is skipped entirely.
    """
    async with conn.transaction():
        await set_tenant_context(conn, tenant_id)
        res = await conn.execute(
            """
            UPDATE kb_operations
               SET status = 'running', started_at = now(), updated_at = now()
             WHERE id = $1 AND tenant_id = $2
               AND status NOT IN ('completed', 'failed')
            """,
            uuid.UUID(operation_id),
            uuid.UUID(tenant_id),
        )
        return res == "UPDATE 1"


async def revive_operation_in_tx(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    operation_id: str,
) -> dict[str, Any] | None:
    """Reset a failed kb_operations row to pending (tenant-scoped, in-tx).

    Used by the CompleteDocumentUpload / ReparseDocument UNIQUE-race
    self-heal: a re-upload after a failed parse may reuse the same key, and
    the failed row is revived to pending so the new outbox event re-runs and
    the parse consumer closes the same row.

    Conditional on status='failed' (returns None otherwise) so a concurrent
    status change between the caller's lookup and this UPDATE is detected
    instead of silently overwritten.
    """
    await set_tenant_context(conn, tenant_id)
    row = await conn.fetchrow(
        """
        UPDATE kb_operations
           SET status = 'pending', result = NULL,
               completed_at = NULL, updated_at = now()
         WHERE id = $1 AND tenant_id = $2 AND status = 'failed'
        RETURNING id, tenant_id, idempotency_key, operation_type,
                  resource_id, status, payload, result,
                  started_at, completed_at, created_at, updated_at
        """,
        uuid.UUID(operation_id),
        uuid.UUID(tenant_id),
    )
    return dict(row) if row else None


async def get_operation(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    operation_id: str,
) -> dict[str, Any] | None:
    """SELECT a single kb_operations row by id.

    Tenant isolation is the explicit ``tenant_id =`` filter (same shape as
    list_operations) — the durable guarantee since #054 (the session RLS
    context is gone).
    """
    async with conn.transaction():
        await set_tenant_context(conn, tenant_id)
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, idempotency_key, operation_type,
                   resource_id, status, payload, result,
                   started_at, completed_at, created_at, updated_at
              FROM kb_operations
             WHERE id = $1 AND tenant_id = $2
            """,
            uuid.UUID(operation_id),
            uuid.UUID(tenant_id),
        )
    return dict(row) if row else None


async def list_operations(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    operation_type: str | None = None,
    resource_id: str | None = None,
    status: str | None = None,
    limit: int = 20,
    cursor: str | None = None,
) -> list[dict[str, Any]]:
    """List kb_operations with keyset pagination (P1C #052, tenant-filtered).

    Tenant isolation is the explicit ``tenant_id =`` filter — the durable
    guarantee since #054 (the session RLS context is gone). Optional
    filters: operation_type / resource_id (the (tenant_id, operation_type,
    resource_id) index triple) and status.

    ORDER BY created_at DESC, id DESC (newest first). Cursor format
    ``{created_at_iso}|{operation_id}`` — same composite keyset as audit
    logs / sessions / messages. The fetch is plain ``LIMIT limit`` and the
    servicer emits next_cursor when exactly ``limit`` rows came back (the
    same boundary convention as every other List RPC in this service).
    """
    cursor_ts: datetime | None = None
    cursor_id: uuid.UUID | None = None
    if cursor:
        cursor_ts, cursor_id = parse_composite_cursor(cursor)

    where = ["tenant_id = $1"]
    params: list[Any] = [uuid.UUID(tenant_id)]
    if operation_type:
        params.append(operation_type)
        where.append(f"operation_type = ${len(params)}")
    if resource_id:
        params.append(uuid.UUID(resource_id))
        where.append(f"resource_id = ${len(params)}")
    if status:
        params.append(status)
        where.append(f"status = ${len(params)}")
    if cursor:
        params.append(cursor_ts)
        params.append(cursor_id)
        where.append(f"(created_at, id) < (${len(params) - 1}, ${len(params)})")
    params.append(limit)

    async with conn.transaction():
        await set_tenant_context(conn, tenant_id)
        rows = await conn.fetch(
            f"""
            SELECT id, tenant_id, idempotency_key, operation_type,
                   resource_id, status, payload, result,
                   started_at, completed_at, created_at, updated_at
              FROM kb_operations
             WHERE {" AND ".join(where)}
             ORDER BY created_at DESC, id DESC
             LIMIT ${len(params)}
            """,
            *params,
        )
    return [dict(r) for r in rows]


async def delete_kb_create_replay_records(
    conn: asyncpg.Connection, *, tenant_id: str, kb_id: str
) -> None:
    """Delete kb.create idempotency replay rows for a soft-deleted KB.

    Called by DeleteKB (C-fix for the delete → same-name re-create bug):
    kb-service falls back to the deterministic key create_kb:{tenant}:{name}
    when the HTTP request carries no idempotency_key, so every same-name
    create in a tenant shares one replay row. If the old KB's row survives
    the delete, the next same-name create replays the deleted KB's stale
    snapshot (old id, never inserted). Deleting the row at delete time
    makes the re-create take the fresh INSERT path. Best-effort: the
    caller treats failures as a warning, not an abort.
    """
    async with conn.transaction():
        await set_tenant_context(conn, tenant_id)
        await conn.execute(
            """
            DELETE FROM kb_operations
             WHERE tenant_id = $1
               AND operation_type = 'kb.create'
               AND resource_id = $2
            """,
            uuid.UUID(tenant_id),
            uuid.UUID(kb_id),
        )
