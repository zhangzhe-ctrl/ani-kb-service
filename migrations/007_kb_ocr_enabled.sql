-- B5 (#22): KBConfig.ocr_enabled persistence. Default false (v1.yaml default).
--
-- KB-SPLIT-P1F (issue-055, ADR 0001 §五-阶段1-验收): 零重放修复。
-- 同 003/004：重放序上 knowledge_bases 由 011 CREATE（列已折叠：
-- ocr_enabled BOOLEAN NOT NULL DEFAULT FALSE），007 先于 011 执行，
-- 全新 kb 库上裸 ALTER TABLE 报错。DO $$ 守护（表不存在则跳过），终态不变。

DO $$
BEGIN
    IF to_regclass('public.knowledge_bases') IS NOT NULL THEN
        ALTER TABLE knowledge_bases
            ADD COLUMN IF NOT EXISTS ocr_enabled BOOLEAN NOT NULL DEFAULT FALSE;
    END IF;
END $$;
