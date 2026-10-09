"""Tenant-isolation filter helper for kb-service repositories (issue-054).

KB-SPLIT-P1E (#054, ADR 0001 §五-阶段1-5): row-level security with the
`app.current_tenant_id` GUC is replaced by explicit, parameterized
``tenant_id = $N`` filters in every repository WHERE clause. RLS policies
and the GUC plumbing are dropped (migration 012); tenant isolation is now
enforced by the SQL itself, reviewed by the cross-tenant negative tests
(tests/test_explicit_tenant_isolation.py).

This module keeps only the UUID validation that repositories call before
binding tenant_id into a statement. It no longer executes any SQL and no
longer holds a connection, so the historical per-statement
`set_tenant_context` call sites are retained as no-op validation to keep
the diff of #054 mechanical (each repository's WHERE clause carries the
real isolation now).
"""
from __future__ import annotations

import uuid

import asyncpg


def validate_tenant_id(tenant_id: str) -> uuid.UUID:
    """Validate a tenant_id string and return it as a UUID.

    Raises ValueError for non-UUID input. Every repository binds the
    tenant filter through this (directly or via `tenant_filter`), so a
    malformed tenant context can never reach the database.
    """
    return uuid.UUID(tenant_id)


async def set_tenant_context(conn: asyncpg.Connection, tenant_id: str) -> None:
    """Backward-compatible validation hook — no longer sets any GUC.

    KB-SPLIT-P1E (#054): PostgreSQL RLS on the kb tables was dropped; the
    `SET LOCAL app.current_tenant_id` statement this used to issue is gone.
    Callers keep invoking it for tenant_id validation only. The connection
    argument is accepted (and ignored) so existing call sites stay
    mechanical to review; new code should prefer `validate_tenant_id`.
    """
    validate_tenant_id(tenant_id)
