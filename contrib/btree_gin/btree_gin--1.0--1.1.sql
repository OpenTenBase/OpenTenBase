/* contrib/btree_gin/btree_gin--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION btree_gin UPDATE TO '1.1'" to load this file. \quit

-- macaddr8 datatype support new in 10.0.
-- 创建用于提取值的函数 gin_extract_value_macaddr8
CREATE FUNCTION gin_extract_value_macaddr8(macaddr8, internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 用于提取值的 C 语言函数路径
LANGUAGE C STRICT IMMUTABLE;

-- 创建用于比较前缀的函数 gin_compare_prefix_macaddr8
CREATE FUNCTION gin_compare_prefix_macaddr8(macaddr8, macaddr8, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'  -- 用于比较前缀的 C 语言函数路径
LANGUAGE C STRICT IMMUTABLE;

-- 创建用于提取查询条件的函数 gin_extract_query_macaddr8
CREATE FUNCTION gin_extract_query_macaddr8(macaddr8, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 用于提取查询条件的 C 语言函数路径
LANGUAGE C STRICT IMMUTABLE;

-- 创建操作符类 macaddr8_ops
CREATE OPERATOR CLASS macaddr8_ops
DEFAULT FOR TYPE macaddr8 USING gin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       macaddr8_cmp(macaddr8, macaddr8),  -- 比较函数
    FUNCTION        2       gin_extract_value_macaddr8(macaddr8, internal),  -- 提取值函数
    FUNCTION        3       gin_extract_query_macaddr8(macaddr8, internal, int2, internal, internal),  -- 提取查询条件函数
    FUNCTION        4       gin_btree_consistent(internal, int2, anyelement, int4, internal, internal),  -- 一致性检查函数
    FUNCTION        5       gin_compare_prefix_macaddr8(macaddr8, macaddr8, int2, internal),  -- 比较前缀函数
STORAGE         macaddr8;  -- 存储类型