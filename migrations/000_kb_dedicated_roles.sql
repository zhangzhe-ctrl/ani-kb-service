-- KB-SPLIT-P1F (issue-055, ADR 0001 §五-阶段1-3): kb 专属库角色基线。
-- 沿用共享库 ani_app 角色模式（deploy/migrations/20260828000100），在 kb
-- 专属实例上重建独立的最小权限体系：
--   ani_app        NOLOGIN 组角色 —— 表级 DML 权限的授予目标
--   ani_app_user   LOGIN 应用角色 —— kb-service 运行时唯一身份（继承 ani_app）
--   ani_migrator   迁移/运维角色 —— 建库重放、扩展、批量维护的执行身份
-- 无 platform_bypass / BYPASSRLS 类超级角色：#054 起 RLS 已移除，隔离由
-- repositories 的显式 tenant_id 过滤保证，不存在需要绕行权限的账号。
-- 密码不写入 migration，由部署 Secret / test-infra init 脚本设置。
--
-- 排序约定：000_ 前缀保证本文件在第一个 GRANT TO ani_app（002）之前执行
--（重放器按文件名排序；角色必须先于 GRANT 存在，否则 002 在全新库上
-- 重放失败）。
-- 幂等：DO $$ 守护（pg_roles 检查）+ ALTER ROLE 可重复执行。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ani_app') THEN
        CREATE ROLE ani_app NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ani_app_user') THEN
        CREATE ROLE ani_app_user LOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ani_migrator') THEN
        CREATE ROLE ani_migrator NOLOGIN;
    END IF;
END
$$;

-- 固定安全属性：全部 NOSUPERUSER / NOCREATEDB / NOCREATEROLE / NOBYPASSRLS。
ALTER ROLE ani_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
ALTER ROLE ani_app_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS INHERIT;
ALTER ROLE ani_migrator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;

-- 角色继承：应用登录身份获得 ani_app 的表级 DML 权限。
GRANT ani_app TO ani_app_user;

-- 库级连接权限：用 current_database() 而非硬编码库名——本文件在哪个库
-- 重放（kb 专属库 / 测试临时库 / 共享库）就授权哪个库，重放保持幂等。
-- GRANT ... ON DATABASE 授予的 CREATE 仅指"在库内新建 schema"，不含
-- "在既有 schema（public）内建表"——PG15 起 PUBLIC 对 public schema 的
-- CREATE 默认已被收回，故 migrator（SET ROLE 身份，无密码 NOLOGIN）执行
-- 非限定名 CREATE TABLE 会被 permission denied 拒绝，必须显式补 SCHEMA
-- 级授权（USAGE + CREATE）。migrator 建出的对象其 owner 为 migrator，
-- 后续迁移内的 GRANT TO ani_app 由 migrator 自身执行，权限自洽。
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO ani_app_user', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO ani_migrator', current_database());
    EXECUTE format('GRANT ALL PRIVILEGES ON DATABASE %I TO ani_migrator', current_database());
    EXECUTE format('GRANT USAGE, CREATE ON SCHEMA public TO ani_migrator');
END
$$;
