/* contrib/postgres_fdw/postgres_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION postgres_fdw" to load this file. \quit

-- 创建postgres_fdw_handler函数，返回fdw_handler类型
CREATE FUNCTION postgres_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- 创建postgres_fdw_validator函数，接受text[]和oid作为参数，返回void类型
CREATE FUNCTION postgres_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- 创建名为postgres_fdw的FOREIGN DATA WRAPPER，指定处理程序为postgres_fdw_handler，验证器为postgres_fdw_validator
CREATE FOREIGN DATA WRAPPER postgres_fdw
  HANDLER postgres_fdw_handler
  VALIDATOR postgres_fdw_validator;
