/*
 * contrib/btree_gin/btree_gin--1.1--1.2.sql
 *
 * 此脚本将 btree_gin 扩展从版本 1.1 升级到版本 1.2。
 * 它添加了对枚举类型的支持。
 */

-- 如果在 psql 中而不是通过 CREATE EXTENSION 加载此文件，则显示警告信息
\echo Use "ALTER EXTENSION btree_gin UPDATE TO '1.1'" to load this file. \quit

--
--
--
-- 枚举操作
--
--


-- 创建提取枚举值的函数
CREATE FUNCTION gin_extract_value_anyenum(anyenum, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

-- 创建比较枚举前缀的函数
CREATE FUNCTION gin_compare_prefix_anyenum(anyenum, anyenum, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

-- 创建提取枚举查询的函数
CREATE FUNCTION gin_extract_query_anyenum(anyenum, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

-- 创建比较枚举的函数
CREATE FUNCTION gin_enum_cmp(anyenum, anyenum)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

-- 创建枚举操作符类
CREATE OPERATOR CLASS enum_ops
DEFAULT FOR TYPE anyenum USING gin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       gin_enum_cmp(anyenum,anyenum),
    FUNCTION        2       gin_extract_value_anyenum(anyenum, internal),
    FUNCTION        3       gin_extract_query_anyenum(anyenum, internal, int2, internal, internal),
    FUNCTION        4       gin_btree_consistent(internal, int2, anyelement, int4, internal, internal),
    FUNCTION        5       gin_compare_prefix_anyenum(anyenum,anyenum,int2, internal),
STORAGE         anyenum;
*/

