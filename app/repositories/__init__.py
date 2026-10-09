"""kb-service data access layer (SPEC §2.4, §8.1).

All repositories use asyncpg with explicit tenant isolation (#054,
KB-SPLIT-P1E): every WHERE clause carries a parameterized
``tenant_id = $N`` filter, and composite FKs ``(tenant_id, id)`` across
the kb table hierarchy guarantee child rows pin to the parent row of
the same tenant at the database level. PostgreSQL RLS and the
``app.current_tenant_id`` GUC were dropped (migration 012).
"""
