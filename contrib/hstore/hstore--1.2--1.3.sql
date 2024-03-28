/* contrib/hstore/hstore--1.2--1.3.sql */
-- complain if script is sourced in psql, rather than via ALTER EXTENSION
/*如果脚本通过 psql 而不是通过 ALTER EXTENSION 命令加载，则输出提示信息并退出 */
\echo Use "ALTER EXTENSION hstore UPDATE TO '1.3'" to load this file. \quit

/*创建函数 hstore_to_jsonb，将 hstore 转换为 jsonb*/
CREATE FUNCTION hstore_to_jsonb(hstore)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'hstore_to_jsonb'
LANGUAGE C IMMUTABLE STRICT;

/* 创建从 hstore 到 jsonb 的转换*/
CREATE CAST (hstore AS jsonb)
  WITH FUNCTION hstore_to_jsonb(hstore);

/* 创建函数 hstore_to_jsonb_loose，宽松模式下将 hstore 转换为 jsonb */
CREATE FUNCTION hstore_to_jsonb_loose(hstore)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'hstore_to_jsonb_loose'
LANGUAGE C IMMUTABLE STRICT;
