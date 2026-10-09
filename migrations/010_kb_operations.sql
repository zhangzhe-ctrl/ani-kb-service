-- KB-SPLIT-P1B (issue-051, ADR 0001 §D4): kb-owned idempotency operations table.
-- Idempotency replay is kb-internal semantics (CreateKB / UpdateKB /
-- CompleteDocumentUpload / ReparseDocument / RebuildKB / UpdateKBConfig /
-- UpdateKBPermissions record their idempotency_key results here so retries
-- return the same response). kb no longer writes the shared async_tasks table
-- (that table belongs to the future task domain); task progress is visible
-- externally only through outbox events.
-- Idempotent: CREATE ... IF NOT EXISTS (SPEC §3.4).
-- Rollback (no production data): DROP TABLE kb_operations;
CREATE TABLE IF NOT EXISTS kb_operations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    operation_type  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    resource_id     UUID,
    payload         JSONB NOT NULL DEFAULT '{}',
    result          JSONB,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Composite constraint: the same client key in different tenants never
    -- collides or replays each other's results.
    CONSTRAINT kb_operations_tenant_id_idempotency_key_key
        UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT kb_operations_status_check
        CHECK (status IN ('pending', 'running', 'completed', 'failed'))
);

-- DeleteKB cleans up kb.create replay rows by resource; #052
-- (ListOperations/GetOperation) filters by the same triple.
CREATE INDEX IF NOT EXISTS idx_kb_operations_resource
    ON kb_operations(tenant_id, operation_type, resource_id);

-- Grant app role access (convention: all business tables follow 20260501000100_init_schema.sql).
GRANT SELECT, INSERT, UPDATE, DELETE ON kb_operations TO ani_app;

-- Row Level Security (same effective shape as kb_outbox_events, per 009):
-- a tenant context reaches only its own rows; no cross-tenant scan exists
-- for kb_operations today (the dispatcher never touches this table).
-- KB-SPLIT-P1E (#054) replaces RLS with explicit tenant filtering later.
ALTER TABLE kb_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE kb_operations FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS kb_operations_platform_bypass ON kb_operations;
DROP POLICY IF EXISTS kb_operations_self ON kb_operations;
CREATE POLICY kb_operations_platform_bypass ON kb_operations
    AS PERMISSIVE FOR ALL
    USING (current_setting('app.current_tenant_id', true) IS NULL)
    WITH CHECK (current_setting('app.current_tenant_id', true) IS NULL);
CREATE POLICY kb_operations_self ON kb_operations
    AS PERMISSIVE FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid
    );
