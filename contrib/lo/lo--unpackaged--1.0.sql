-- Abort if this script is sourced in psql instead of being run via CREATE EXTENSION
-- 如果在 psql 内源执行此脚本，而不是通过 CREATE EXTENSION 运行，那么中断执行
\echo Use "CREATE EXTENSION lo FROM unpackaged" to load this file properly. \quit

-- Ensure the 'lo' extension exists in unpackaged form before trying to modify it
-- 确认 'lo' 扩展已经以未打包的形式存在，然后尝试进行修改
DO $$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'lo' AND extrelocatable = false) THEN
        RAISE EXCEPTION 'Extension "lo" is not in an unpackaged state. Use CREATE EXTENSION to create it instead.';
    END IF;
END
$$;

-- Add existing domain and functions to the 'lo' extension
-- 将已存在的域和函数添加到 'lo' 扩展
ALTER EXTENSION lo ADD domain lo;
ALTER EXTENSION lo ADD function lo_oid(lo);
ALTER EXTENSION lo ADD function lo_manage();
