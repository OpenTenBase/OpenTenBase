/* contrib/btree_gin/btree_gin--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION btree_gin UPDATE TO '1.1'" to load this file. \quit

--
--
--
-- enum ops
--
--


-- 创建用于提取值的函数 gin_extract_value_anyenum
CREATE FUNCTION gin_extract_value_anyenum(anyenum, internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 用于提取值的 C 语言函数路径
LANGUAGE C STRICT IMMUTABLE;

-- 创建用于比较前缀的函数 gin_compare_prefix_anyenum
CREATE FUNCTION gin_compare_prefix_anyenum(anyenum, anyenum, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'  -- 用于比较前缀的 C 语言函数路径
LANGUAGE C STRICT IMMUTABLE;

-- 创建用于提取查询条件的函数 gin_extract_query_anyenum
CREATE FUNCTION gin_extract_query_anyenum(anyenum, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 用于提取查询条件的 C 语言函数路径
LANGUAGE C STRICT IMMUTABLE;

-- 创建枚举比较函数 gin_enum_cmp
CREATE FUNCTION gin_enum_cmp(anyenum, anyenum)
RETURNS int4
AS 'MODULE_PATHNAME'  -- 枚举比较函数的 C 语言函数路径
LANGUAGE C STRICT IMMUTABLE;

-- 创建操作符类 enum_ops
CREATE OPERATOR CLASS enum_ops
DEFAULT FOR TYPE anyenum USING gin
AS
    OPERATOR        1       <,  -- 小于操作符
    OPERATOR        2       <=, -- 小于等于操作符
    OPERATOR        3       =,  -- 等于操作符
    OPERATOR        4       >=, -- 大于等于操作符
    OPERATOR        5       >,  -- 大于操作符
    FUNCTION        1       gin_enum_cmp(anyenum, anyenum),  -- 枚举比较函数
    FUNCTION        2       gin_extract_value_anyenum(anyenum, internal),  -- 提取值函数
    FUNCTION        3       gin_extract_query_anyenum(anyenum, internal, int2, internal, internal),  -- 提取查询条件函数
    FUNCTION        4       gin_btree_consistent(internal, int2, anyelement, int4, internal, internal),  -- 一致性检查函数
    FUNCTION        5       gin_compare_prefix_anyenum(anyenum, anyenum, int2, internal),  -- 比较前缀函数
STORAGE         anyenum;  -- 存储类型