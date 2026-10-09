-- 003_kb_retrieval_mode.sql
-- 新增 knowledge_bases.retrieval_mode 列：指定 KB 的检索方式。
--   枚举：vector | hybrid | keyword
--     vector   向量检索（Milvus cosine）
--     hybrid   混合检索（向量 + 全文 pg_trgm + RRF，默认）
--     keyword  全文检索（pg_trgm）
-- 幂等：列已存在则跳过。仅用于已有库升级；新装库直接重放 011 基线。
--
-- KB-SPLIT-P1F (issue-055, ADR 0001 §五-阶段1-验收): 零重放修复。
-- 重放器按文件名排序执行，003
-- 排在 011（knowledge_bases 的唯一 CREATE 处，KB 库专属基线）之前——
-- 全新 kb 库上执行本文件时表尚不存在，裸 ALTER TABLE 必然报错。
-- 修复：DO $$ 守护（to_regclass 检查表不存在则跳过）。列已折叠进 011
-- 的 CREATE TABLE（retrieval_mode TEXT NOT NULL DEFAULT 'hybrid' CHECK），
-- 守护式跳过后终态与共享库逐文件升级完全一致。

DO $$
BEGIN
    IF to_regclass('public.knowledge_bases') IS NOT NULL THEN
        ALTER TABLE knowledge_bases
            ADD COLUMN IF NOT EXISTS retrieval_mode TEXT NOT NULL DEFAULT 'hybrid'
                CHECK (retrieval_mode IN ('vector', 'hybrid', 'keyword'));
    END IF;
END $$;
