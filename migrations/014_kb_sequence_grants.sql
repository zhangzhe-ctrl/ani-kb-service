-- KB-SPLIT-P1F (issue-055): 序列授权补齐。
-- 009 kb_outbox_events.id 是 BIGSERIAL，产生 kb_outbox_events_id_seq，但
-- 009 只授权了表级 DML（SELECT/INSERT/UPDATE），未授权序列 USAGE——应用
-- 角色 INSERT（触发 nextval）会被 permission denied 拒绝。
-- 对齐共享库 deploy/migrations/20260828000200 的 ALL SEQUENCES 授权模式：
-- kb 库全部序列授权应用角色（当前仅 kb_outbox_events 一张 SERIAL 表，其余
-- 表 UUID 主键用 gen_random_uuid() 不占序列）。
-- 幂等：GRANT 可重复执行。
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ani_app;
