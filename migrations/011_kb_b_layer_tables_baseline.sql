-- KB-SPLIT-P1D (issue-053, ADR 0001 §一-1.2 B层 / §四 #2 #3 / §五-阶段1-2):
-- Bring the four B-layer tables (knowledge_bases / kb_documents /
-- kb_sessions / kb_messages) into the kb migration baseline as the single
-- DDL authority. Their DDL previously lived in the shared library
-- (deploy/migrations/20260501000100_init_schema.sql §6) while kb-side
-- migrations 003/004/007 only carried incremental ALTERs — split ownership.
--
-- This migration rebuilds the complete DDL verified against what
-- app/repositories/*.py actually reads/writes (columns, indexes,
-- constraints, RLS), folding in every later incremental ALTER:
--   - kb_name_unique_active (deploy 20260903000100): partial unique index
--     replacing UNIQUE(tenant_id, name) — soft-deleted rows free the name.
--   - kb_sessions_kb_id_index (deploy 20260903000300): kb_sessions(kb_id,
--     created_at DESC) for ListKBSessions / ListKBCitations keyset paging.
--   - kb_embedding_model_full_name (deploy 20260911000200): data fix,
--     'bge-m3' → 'BAAI/bge-m3'; here the DDL default stays the full name
--     (grpc_server.py / config.py default) — no rows exist on a fresh DB.
--     Note (review fix #2, ADR-conformance round): app-layer create_kb
--     always supplies embedding_model explicitly (empty value raises in
--     knowledge_base.py), so this DDL default is defense-in-depth for
--     hand-written SQL only, never relied on by the app.
--   - kb_default_inference_service (deploy 20260911000100): column addition.
-- Plus the kb-local increments: 003 retrieval_mode, 004 vector_store_id,
-- 007 ocr_enabled, and the operational columns custom_metadata / object_id
-- on kb_documents (from scripts/_fix_kb_schema.sql — repos read/write them).
--
-- Cross-library FK removal (ADR §四 #3): knowledge_bases.tenant_id no
-- longer REFERENCES tenants(id) ON DELETE CASCADE — tenants lives in the
-- shared Core library. Tenant existence is validated at the application
-- layer (CreateKB trusts the authenticated tenant context injected by the
-- Gateway, D2 boundary convergence); the CASCADE semantics move to kb's
-- own DeleteKB flow. No reference to any shared-library object exists in
-- this file.
--
-- RLS: kept as-is in this migration (the tenant_isolation RESTRICTIVE
-- policy from init_schema §11); KB-SPLIT-P1E (#054) removes it in favor of
-- explicit tenant filtering.
--
-- Idempotent: all statements use CREATE ... IF NOT EXISTS (SPEC §3.4).
-- Rollback (no production data): DROP TABLE kb_messages, kb_sessions,
-- kb_documents, knowledge_bases (in FK-dependency order).

-- ===========================================================================
-- knowledge_bases (aggregate root)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS knowledge_bases (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                UUID NOT NULL,  -- no cross-library FK (#053)
    name                     TEXT NOT NULL,
    description              TEXT,
    embedding_model          TEXT NOT NULL DEFAULT 'BAAI/bge-m3',
    chunk_size               INT NOT NULL DEFAULT 512,
    top_k                    INT NOT NULL DEFAULT 5,
    score_threshold          FLOAT NOT NULL DEFAULT 0.3,
    retrieval_mode           TEXT NOT NULL DEFAULT 'hybrid'
        CHECK (retrieval_mode IN ('vector', 'hybrid', 'keyword')),
    vector_store_id          TEXT,
    default_inference_service TEXT,
    ocr_enabled              BOOLEAN NOT NULL DEFAULT FALSE,
    status                   TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'rebuilding', 'deleted')),
    doc_count                INT NOT NULL DEFAULT 0,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kb_tenant_id ON knowledge_bases(tenant_id);

-- kb_name_unique_active (deploy 20260903000100): partial unique index —
-- soft-deleted rows (status='deleted') do not hold the name; matches the
-- status <> 'deleted' filter of every read path in knowledge_base.py.
CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_bases_tenant_name_active
    ON knowledge_bases(tenant_id, name)
    WHERE status <> 'deleted';

-- ===========================================================================
-- kb_documents (document state machine authority)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS kb_documents (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kb_id            UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    tenant_id        UUID NOT NULL,
    file_name        TEXT NOT NULL,
    file_type        TEXT NOT NULL,                  -- pdf | docx | xlsx | pptx | md | txt
    file_size_bytes  BIGINT NOT NULL,
    storage_path     TEXT NOT NULL,                  -- object-storage path
    checksum_sha256  TEXT NOT NULL,
    parse_status     TEXT NOT NULL DEFAULT 'pending'
        CHECK (parse_status IN ('pending', 'parsing', 'indexing', 'ready', 'failed')),
    chunk_count      INT NOT NULL DEFAULT 0,
    error_message    TEXT,
    custom_metadata  JSONB DEFAULT '{}'::jsonb,
    object_id        TEXT,
    parsed_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_kb_docs_kb_id ON kb_documents(kb_id);
CREATE INDEX IF NOT EXISTS idx_kb_docs_parse_status ON kb_documents(kb_id, parse_status);

-- ===========================================================================
-- kb_sessions (session metadata, kb-owned)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS kb_sessions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kb_id       UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    tenant_id   UUID NOT NULL,
    user_id     UUID,
    title       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- kb_sessions_kb_id_index (deploy 20260903000300): ListKBSessions /
-- ListKBCitations keyset paging (kb_id filter + created_at DESC ordering).
CREATE INDEX IF NOT EXISTS idx_kb_sessions_kb_id_created_at
    ON kb_sessions(kb_id, created_at DESC);

-- ===========================================================================
-- kb_messages (per-turn chat messages)
-- tenant_id denormalized on the row so isolation never depends on a
-- session JOIN (ANI-09 P0 fix).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS kb_messages (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id      UUID NOT NULL REFERENCES kb_sessions(id) ON DELETE CASCADE,
    tenant_id       UUID NOT NULL,                  -- denormalized on purpose
    role            TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content         TEXT NOT NULL,
    source_chunks   JSONB,                          -- [{doc_id, file_name, page, score}]
    input_tokens    INT,
    output_tokens   INT,
    duration_ms     INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_kb_messages_session ON kb_messages(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_kb_messages_tenant ON kb_messages(tenant_id, created_at);

-- ===========================================================================
-- Grants + RLS (original shape preserved; #054 replaces RLS later)
-- ===========================================================================
GRANT SELECT, INSERT, UPDATE, DELETE ON knowledge_bases TO ani_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON kb_documents TO ani_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON kb_sessions TO ani_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON kb_messages TO ani_app;

ALTER TABLE knowledge_bases ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_bases FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON knowledge_bases;
CREATE POLICY tenant_isolation ON knowledge_bases
    AS RESTRICTIVE
    USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid);

ALTER TABLE kb_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE kb_documents FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON kb_documents;
CREATE POLICY tenant_isolation ON kb_documents
    AS RESTRICTIVE
    USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid);

ALTER TABLE kb_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE kb_sessions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON kb_sessions;
CREATE POLICY tenant_isolation ON kb_sessions
    AS RESTRICTIVE
    USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid);

ALTER TABLE kb_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE kb_messages FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON kb_messages;
CREATE POLICY tenant_isolation ON kb_messages
    AS RESTRICTIVE
    USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid);
