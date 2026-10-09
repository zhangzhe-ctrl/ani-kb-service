-- KB-SPLIT-P1E (issue-054 review fix #1): kb_chunks tenant/doc index.
--
-- Migration 012 added the composite FK kb_chunks(tenant_id, doc_id)
-- -> kb_documents(tenant_id, id) ON DELETE CASCADE. The FK machinery looks
-- up referencing rows via WHERE tenant_id = $ AND doc_id = $, but the
-- pre-existing kb_chunks indexes from 002 all lead with kb_id
-- (idx_kb_chunks_kb_doc(kb_id, doc_id), parent/type/GIN) — none of them
-- has doc_id as the leading column, so the FK orphan check / CASCADE
-- lookup degrades to a sequential scan on kb_chunks (the largest table:
-- 1000 docs x 100k chunks => 1000 full scans per DeleteKB).
--
-- This index restores the lookup:
--   - FK orphan check on kb_documents DELETE (tenant_id, doc_id) probe
--   - repository list_parent_chunks (doc_id + tenant_id WHERE clause)
--
-- Idempotent: CREATE INDEX IF NOT EXISTS (SPEC §3.4).
-- Rollback (no production data): DROP INDEX idx_kb_chunks_tenant_doc;

CREATE INDEX IF NOT EXISTS idx_kb_chunks_tenant_doc
    ON kb_chunks(tenant_id, doc_id);
