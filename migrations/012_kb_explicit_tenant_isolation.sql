-- KB-SPLIT-P1E (issue-054, ADR 0001 §五-阶段1-5 / §四 #4):
-- Replace RLS (GUC app.current_tenant_id + RESTRICTIVE/PERMISSIVE policies)
-- with explicit parameterized tenant_id filtering in app/repositories/*.py,
-- and turn every internal parent->child relation into a composite FK
-- (tenant_id, id) so the database itself guarantees tenant consistency of
-- the hierarchy (an id-only FK is satisfiable cross-tenant because id is
-- globally unique; only the (tenant_id, id) pair pins the same tenant).
--
-- What this migration does:
--   1. DROP every RLS policy: tenant_isolation (RESTRICTIVE) on the five
--      011/002 tables, plus the PERMISSIVE *_self / *_platform_bypass
--      pairs on kb_permissions / kb_audit_log / kb_outbox_events /
--      kb_operations.
--   2. NO FORCE + DISABLE ROW LEVEL SECURITY on all 9 kb-owned tables.
--      With the policies gone the app role reaches every row; isolation
--      is enforced only by the repositories' WHERE tenant_id = $N.
--   3. DROP the stale cross-library FKs kb_permissions.tenant_id /
--      kb_audit_log.tenant_id -> tenants(id) (005/006 predate #053, which
--      already removed the equivalent on knowledge_bases in 011).
--   4. DROP the id-only FKs on child tables (auto-named <table>_<col>_fkey)
--      and re-add them as composite FKs referencing (tenant_id, id):
--        kb_documents / kb_sessions / kb_permissions / kb_audit_log
--          -> knowledge_bases(tenant_id, id)
--        kb_messages  -> kb_sessions(tenant_id, id)
--        kb_chunks    -> kb_documents(tenant_id, id)
--      Parent-side UNIQUE (tenant_id, id) indexes are created first (a
--      composite FK must reference a UNIQUE constraint or unique index).
--
-- kb_outbox_events keeps cross-tenant visibility for the dispatcher poll
-- (list_undispatched) — that was already the platform-bypass semantics;
-- no RLS means no special case anymore.
--
-- Idempotent: DROP ... IF EXISTS everywhere; ADD CONSTRAINT has no
-- IF NOT EXISTS form in PostgreSQL, so the adds are guarded by a
-- pg_constraint existence check in a DO block (SPEC §3.4).
-- Rollback (no production data): drop the composite FKs + unique
-- indexes, then re-create the policies from 002/005/006/009/010/011.

-- ===========================================================================
-- 1. DROP RLS policies (9 tables)
-- ===========================================================================
-- B-layer tables (011) + kb_chunks (002): single RESTRICTIVE policy.
DROP POLICY IF EXISTS tenant_isolation ON knowledge_bases;
DROP POLICY IF EXISTS tenant_isolation ON kb_documents;
DROP POLICY IF EXISTS tenant_isolation ON kb_sessions;
DROP POLICY IF EXISTS tenant_isolation ON kb_messages;
DROP POLICY IF EXISTS tenant_isolation ON kb_chunks;
-- Double-policy pairs (PERMISSIVE self + platform bypass): 005/006/009/010.
DROP POLICY IF EXISTS kbp_self ON kb_permissions;
DROP POLICY IF EXISTS kbp_platform_bypass ON kb_permissions;
DROP POLICY IF EXISTS kal_self ON kb_audit_log;
DROP POLICY IF EXISTS kal_platform_bypass ON kb_audit_log;
DROP POLICY IF EXISTS kb_outbox_events_self ON kb_outbox_events;
DROP POLICY IF EXISTS kb_outbox_events_platform_bypass ON kb_outbox_events;
DROP POLICY IF EXISTS kb_operations_self ON kb_operations;
DROP POLICY IF EXISTS kb_operations_platform_bypass ON kb_operations;

-- ===========================================================================
-- 2. NO FORCE + DISABLE ROW LEVEL SECURITY (9 tables)
-- ===========================================================================
ALTER TABLE knowledge_bases NO FORCE ROW LEVEL SECURITY;
ALTER TABLE knowledge_bases DISABLE ROW LEVEL SECURITY;
ALTER TABLE kb_documents NO FORCE ROW LEVEL SECURITY;
ALTER TABLE kb_documents DISABLE ROW LEVEL SECURITY;
ALTER TABLE kb_sessions NO FORCE ROW LEVEL SECURITY;
ALTER TABLE kb_sessions DISABLE ROW LEVEL SECURITY;
ALTER TABLE kb_messages NO FORCE ROW LEVEL SECURITY;
ALTER TABLE kb_messages DISABLE ROW LEVEL SECURITY;
ALTER TABLE kb_chunks NO FORCE ROW LEVEL SECURITY;
ALTER TABLE kb_chunks DISABLE ROW LEVEL SECURITY;
ALTER TABLE kb_permissions NO FORCE ROW LEVEL SECURITY;
ALTER TABLE kb_permissions DISABLE ROW LEVEL SECURITY;
ALTER TABLE kb_audit_log NO FORCE ROW LEVEL SECURITY;
ALTER TABLE kb_audit_log DISABLE ROW LEVEL SECURITY;
ALTER TABLE kb_outbox_events NO FORCE ROW LEVEL SECURITY;
ALTER TABLE kb_outbox_events DISABLE ROW LEVEL SECURITY;
ALTER TABLE kb_operations NO FORCE ROW LEVEL SECURITY;
ALTER TABLE kb_operations DISABLE ROW LEVEL SECURITY;

-- ===========================================================================
-- 3. DROP stale cross-library FKs (005/006 predate #053)
-- ===========================================================================
-- Constraint names follow the PostgreSQL auto-generation convention
-- (<table>_<column>_fkey); 005/006 used inline REFERENCES without a name.
ALTER TABLE kb_permissions DROP CONSTRAINT IF EXISTS kb_permissions_tenant_id_fkey;
ALTER TABLE kb_audit_log DROP CONSTRAINT IF EXISTS kb_audit_log_tenant_id_fkey;

-- ===========================================================================
-- 4. Composite FKs (tenant_id, id)
-- ===========================================================================
-- 4a. Parent-side UNIQUE indexes: a composite FK must reference a UNIQUE
-- constraint or unique index; (tenant_id, id) is a superset of the PK and
-- pins child rows to the parent row of the SAME tenant.
CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_bases_tenant_id_id
    ON knowledge_bases(tenant_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_kb_sessions_tenant_id_id
    ON kb_sessions(tenant_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_kb_documents_tenant_id_id
    ON kb_documents(tenant_id, id);

-- 4b. DROP the id-only FKs (auto-named <table>_<column>_fkey from the
-- inline REFERENCES in 005/006/011). An id-only FK admits cross-tenant
-- children; the composite replacement below does not.
ALTER TABLE kb_documents DROP CONSTRAINT IF EXISTS kb_documents_kb_id_fkey;
ALTER TABLE kb_sessions DROP CONSTRAINT IF EXISTS kb_sessions_kb_id_fkey;
ALTER TABLE kb_messages DROP CONSTRAINT IF EXISTS kb_messages_session_id_fkey;
ALTER TABLE kb_permissions DROP CONSTRAINT IF EXISTS kb_permissions_kb_id_fkey;
ALTER TABLE kb_audit_log DROP CONSTRAINT IF EXISTS kb_audit_log_kb_id_fkey;

-- 4c. Add the composite FKs (guarded: ADD CONSTRAINT has no IF NOT EXISTS).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'kb_documents_kb_tenant_fk') THEN
        ALTER TABLE kb_documents
            ADD CONSTRAINT kb_documents_kb_tenant_fk
            FOREIGN KEY (tenant_id, kb_id)
            REFERENCES knowledge_bases(tenant_id, id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'kb_sessions_kb_tenant_fk') THEN
        ALTER TABLE kb_sessions
            ADD CONSTRAINT kb_sessions_kb_tenant_fk
            FOREIGN KEY (tenant_id, kb_id)
            REFERENCES knowledge_bases(tenant_id, id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'kb_messages_session_tenant_fk') THEN
        ALTER TABLE kb_messages
            ADD CONSTRAINT kb_messages_session_tenant_fk
            FOREIGN KEY (tenant_id, session_id)
            REFERENCES kb_sessions(tenant_id, id) ON DELETE CASCADE;
    END IF;

    -- kb_chunks had NO FK to kb_documents at all (002: "no FK — hard-
    -- deleting would strand chunks"); the composite form is safe because
    -- ON DELETE CASCADE removes the chunks with their document.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'kb_chunks_doc_tenant_fk') THEN
        ALTER TABLE kb_chunks
            ADD CONSTRAINT kb_chunks_doc_tenant_fk
            FOREIGN KEY (tenant_id, doc_id)
            REFERENCES kb_documents(tenant_id, id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'kb_permissions_kb_tenant_fk') THEN
        ALTER TABLE kb_permissions
            ADD CONSTRAINT kb_permissions_kb_tenant_fk
            FOREIGN KEY (tenant_id, kb_id)
            REFERENCES knowledge_bases(tenant_id, id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'kb_audit_log_kb_tenant_fk') THEN
        ALTER TABLE kb_audit_log
            ADD CONSTRAINT kb_audit_log_kb_tenant_fk
            FOREIGN KEY (tenant_id, kb_id)
            REFERENCES knowledge_bases(tenant_id, id) ON DELETE CASCADE;
    END IF;
END $$;
