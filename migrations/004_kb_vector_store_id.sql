-- 004_kb_vector_store_id.sql
-- 新增 knowledge_bases.vector_store_id 列：持久化 Core API 返回的向量库 ID。
-- 改造后 kb-service 作为唯一编排者，需记录 Core 侧 vector_store id 以便
-- 后续 insert/search/delete 向量操作 (Plan §3.1)。
-- 幂等：列已存在则跳过。仅用于已有库升级；新装库直接重放 011 基线。
--
-- KB-SPLIT-P1F (issue-055, ADR 0001 §五-阶段1-验收): 零重放修复。
-- 同 003：重放序上 knowledge_bases 由 011 CREATE，004 先于 011 执行，
-- 全新 kb 库上裸 ALTER TABLE 报错。DO $$ 守护（表不存在则跳过）；列已
-- 折叠进 011 的 CREATE TABLE（vector_store_id TEXT），终态不变。

DO $$
BEGIN
    IF to_regclass('public.knowledge_bases') IS NOT NULL THEN
        ALTER TABLE knowledge_bases
            ADD COLUMN IF NOT EXISTS vector_store_id TEXT;
    END IF;
END $$;
