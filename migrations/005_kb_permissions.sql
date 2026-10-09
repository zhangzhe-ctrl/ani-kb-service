-- B4 (#20/#19): KB-level ACL store. Defaults when no row exists:
-- public_read=false, allowed_user_ids=[] (plan §2.3).
--
-- KB-SPLIT-P1F (issue-055, ADR 0001 §五-阶段1-验收): 零重放修复。
-- 原 DDL 的两条内联 FK 在全新 kb 库上重放必失败：
--   tenant_id REFERENCES tenants(id)      —— tenants 是共享 Core 库的表，
--     kb 专属库永不存在（ADR §四 #3，011 已在 knowledge_bases 上先行去除）；
--   kb_id   REFERENCES knowledge_bases(id) —— 重放序上该表 011 才 CREATE。
-- 去除内联 FK 后由 012 统一补复合 FK kb_permissions_kb_tenant_fk
-- (tenant_id, kb_id) -> knowledge_bases(tenant_id, id)：比原 id-only FK
-- 更强（钉死同租户父行）；012 对旧约束名的 DROP IF EXISTS 变 no-op，
-- 共享库上重放同样安全。终态：复合 FK（租户一致）优于原 id-only FK。

CREATE TABLE IF NOT EXISTS kb_permissions (
  kb_id            UUID PRIMARY KEY,
  tenant_id        UUID NOT NULL,
  public_read      BOOLEAN NOT NULL DEFAULT FALSE,
  allowed_user_ids UUID[] NOT NULL DEFAULT '{}',
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE kb_permissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE kb_permissions FORCE ROW LEVEL SECURITY;

-- Standard double-policy RLS pattern (matches knowledge_bases /
-- kb_documents / kb_chunks / async_tasks): a single RESTRICTIVE policy
-- with no PERMISSIVE companion denies ALL access (PostgreSQL requires at
-- least one PERMISSIVE policy to pass). kbp_self scopes rows by tenant;
-- kbp_platform_bypass allows no-tenant-context platform paths.
-- (Removed by 012 in favor of explicit tenant filtering, #054.)
DROP POLICY IF EXISTS tenant_isolation ON kb_permissions;
DROP POLICY IF EXISTS kbp_self ON kb_permissions;
DROP POLICY IF EXISTS kbp_platform_bypass ON kb_permissions;
CREATE POLICY kbp_self ON kb_permissions
  AS PERMISSIVE
  FOR ALL
  TO public
  USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid);
CREATE POLICY kbp_platform_bypass ON kb_permissions
  AS PERMISSIVE
  FOR ALL
  TO public
  USING (current_setting('app.current_tenant_id', true) IS NULL)
  WITH CHECK (current_setting('app.current_tenant_id', true) IS NULL);

CREATE INDEX IF NOT EXISTS idx_kb_permissions_tenant ON kb_permissions(tenant_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON kb_permissions TO ani_app;
