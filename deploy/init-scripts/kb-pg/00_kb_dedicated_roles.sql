-- KB-SPLIT-P3B (issue-062): kb 自包含的 init-script 副本。
-- 内容（live SQL）与 migrations/000_kb_dedicated_roles.sql 完全一致，
-- 由 scripts/validate_kb_pg_init_consistency.py 门禁保证不漂移。
-- compose / 独立部署首次建库时执行本文件，确保应用连接前角色已存在。
-- 注释可自由不同，SQL 语句必须逐行一致（门禁按 live SQL 比对）。
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

ALTER ROLE ani_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
ALTER ROLE ani_app_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS INHERIT;
ALTER ROLE ani_migrator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;

GRANT ani_app TO ani_app_user;

DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO ani_app_user', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO ani_migrator', current_database());
    EXECUTE format('GRANT ALL PRIVILEGES ON DATABASE %I TO ani_migrator', current_database());
    EXECUTE format('GRANT USAGE, CREATE ON SCHEMA public TO ani_migrator');
END
$$;
