/* contrib/citext/citext--1.2--1.3.sql */

-- 如果在 psql 中引入脚本，而不是通过 ALTER EXTENSION，请发出警告
\echo 使用 "ALTER EXTENSION citext UPDATE TO '1.3'" 命令来加载此文件。 \quit

-- 更新 pg_aggregate 表，将 aggcombinefn 字段设置为 'citext_smaller'，
-- 当 aggfnoid 为 'min(citext)'::pg_catalog.regprocedure 时进行更新
UPDATE pg_aggregate SET aggcombinefn = 'citext_smaller'
WHERE aggfnoid = 'min(citext)'::pg_catalog.regprocedure;
