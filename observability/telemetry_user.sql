-- OpenTenBase 最小权限监控账户（pg_monitor 角色，不碰业务权限）
-- 在 OpenTenBase 实例（CN）以超级用户执行：
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'telemetry_user') THEN
    CREATE ROLE telemetry_user LOGIN PASSWORD 'CHANGE_ME';
  END IF;
END
$$;

ALTER ROLE telemetry_user SET search_path = pg_catalog, pg_monitor;
GRANT CONNECT ON DATABASE postgres TO telemetry_user;
GRANT pg_monitor TO telemetry_user;

-- 验证：以 telemetry_user 查询系统目录应成功
-- SELECT * FROM pg_stat_database LIMIT 1;
