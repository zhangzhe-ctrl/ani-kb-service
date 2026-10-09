-- KB-SPLIT-P1A (issue-050, ADR 0001 §D4): kb-owned outbox table.
-- Transactional outbox is a per-service asset: kb-service owns its own table
-- instead of writing the shared-library outbox_events. Structure follows the
-- current outbox_events design (deploy/migrations/20260501000100_init_schema.sql
-- §7 + RLS repair 20260829_002): pending→published state machine preserved,
-- the dispatcher polls unpublished rows ordered by created_at (at-least-once).
-- kb-service is the only writer; other services own their own outbox tables.
-- Idempotent: all statements use CREATE ... IF NOT EXISTS (SPEC §3.4).
-- Rollback (no production data): DROP TABLE kb_outbox_events;
CREATE TABLE IF NOT EXISTS kb_outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    UUID NOT NULL,
    event_type      TEXT NOT NULL,
    tenant_id       UUID NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    published       BOOLEAN NOT NULL DEFAULT FALSE,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The dispatcher polls this partial index, scanning only unpublished rows.
CREATE INDEX IF NOT EXISTS idx_kb_outbox_unpublished ON kb_outbox_events(created_at) WHERE NOT published;

-- Grant app role access (convention: all business tables follow 20260501000100_init_schema.sql).
GRANT SELECT, INSERT, UPDATE ON kb_outbox_events TO ani_app;

-- Row Level Security (current effective shape of the shared table, per
-- 20260829_002): the dispatcher scans cross-tenant WITHOUT tenant context
-- (platform bypass); a tenant context only reaches its own rows.
-- KB-SPLIT-P1E (#054) replaces RLS with explicit tenant filtering later.
ALTER TABLE kb_outbox_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE kb_outbox_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS kb_outbox_events_platform_bypass ON kb_outbox_events;
DROP POLICY IF EXISTS kb_outbox_events_self ON kb_outbox_events;
CREATE POLICY kb_outbox_events_platform_bypass ON kb_outbox_events
    AS PERMISSIVE FOR ALL
    USING (current_setting('app.current_tenant_id', true) IS NULL)
    WITH CHECK (current_setting('app.current_tenant_id', true) IS NULL);
CREATE POLICY kb_outbox_events_self ON kb_outbox_events
    AS PERMISSIVE FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid
    );
