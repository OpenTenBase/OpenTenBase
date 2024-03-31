/* contrib/btree_gist/btree_gist--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION btree_gist UPDATE TO '1.1'" to load this file. \quit

-- Index-only scan support new in 9.5.
-- 创建函数 gbt_oid_fetch，用于检索 oid 数据类型
CREATE FUNCTION gbt_oid_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_int2_fetch，用于检索 int2 数据类型
CREATE FUNCTION gbt_int2_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_int4_fetch，用于检索 int4 数据类型
CREATE FUNCTION gbt_int4_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_int8_fetch，用于检索 int8 数据类型
CREATE FUNCTION gbt_int8_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_float4_fetch，用于检索 float4 数据类型
CREATE FUNCTION gbt_float4_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_float8_fetch，用于检索 float8 数据类型
CREATE FUNCTION gbt_float8_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_ts_fetch，用于检索 timestamp 数据类型
CREATE FUNCTION gbt_ts_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_time_fetch，用于检索 time 数据类型
CREATE FUNCTION gbt_time_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_date_fetch，用于检索 date 数据类型
CREATE FUNCTION gbt_date_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_intv_fetch，用于检索 interval 数据类型
CREATE FUNCTION gbt_intv_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_cash_fetch，用于检索 money 数据类型
CREATE FUNCTION gbt_cash_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_macad_fetch，用于检索 macaddr 数据类型
CREATE FUNCTION gbt_macad_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 创建函数 gbt_var_fetch，用于检索 text、bpchar、bytea、numeric、bit、varbit 数据类型
CREATE FUNCTION gbt_var_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'  -- 替换为实际的模块路径名
LANGUAGE C IMMUTABLE STRICT;

-- 修改操作符族 gist_oid_ops，添加针对 oid 数据类型的检索函数 gbt_oid_fetch
ALTER OPERATOR FAMILY gist_oid_ops USING gist ADD
    FUNCTION    9 (oid, oid) gbt_oid_fetch (internal) ;

-- 修改操作符族 gist_int2_ops，添加针对 int2 数据类型的检索函数 gbt_int2_fetch
ALTER OPERATOR FAMILY gist_int2_ops USING gist ADD
    FUNCTION    9 (int2, int2) gbt_int2_fetch (internal) ;

-- 修改操作符族 gist_int4_ops，添加针对 int4 数据类型的检索函数 gbt_int4_fetch
ALTER OPERATOR FAMILY gist_int4_ops USING gist ADD
    FUNCTION    9 (int4, int4) gbt_int4_fetch (internal) ;

-- 修改操作符族 gist_int8_ops，添加针对 int8 数据类型的检索函数 gbt_int8_fetch
ALTER OPERATOR FAMILY gist_int8_ops USING gist ADD
    FUNCTION    9 (int8, int8) gbt_int8_fetch (internal) ;

-- 修改操作符族 gist_float4_ops，添加针对 float4 数据类型的检索函数 gbt_float4_fetch
ALTER OPERATOR FAMILY gist_float4_ops USING gist ADD
    FUNCTION    9 (float4, float4) gbt_float4_fetch (internal) ;

-- 修改操作符族 gist_float8_ops，添加针对 float8 数据类型的检索函数 gbt_float8_fetch
ALTER OPERATOR FAMILY gist_float8_ops USING gist ADD
    FUNCTION    9 (float8, float8) gbt_float8_fetch (internal) ;

-- 修改操作符族 gist_timestamp_ops，添加针对 timestamp 数据类型的检索函数 gbt_ts_fetch
ALTER OPERATOR FAMILY gist_timestamp_ops USING gist ADD
    FUNCTION    9 (timestamp, timestamp) gbt_ts_fetch (internal) ;

-- 修改操作符族 gist_timestamptz_ops，添加针对 timestamptz 数据类型的检索函数 gbt_ts_fetch
ALTER OPERATOR FAMILY gist_timestamptz_ops USING gist ADD
    FUNCTION    9 (timestamptz, timestamptz) gbt_ts_fetch (internal) ;

-- 修改操作符族 gist_time_ops，添加针对 time 数据类型的检索函数 gbt_time_fetch
ALTER OPERATOR FAMILY gist_time_ops USING gist ADD
    FUNCTION    9 (time, time) gbt_time_fetch (internal) ;

-- 修改操作符族 gist_date_ops，添加针对 date 数据类型的检索函数 gbt_date_fetch
ALTER OPERATOR FAMILY gist_date_ops USING gist ADD
    FUNCTION    9 (date, date) gbt_date_fetch (internal) ;

-- 修改操作符族 gist_interval_ops，添加针对 interval 数据类型的检索函数 gbt_intv_fetch
ALTER OPERATOR FAMILY gist_interval_ops USING gist ADD
    FUNCTION    9 (interval, interval) gbt_intv_fetch (internal) ;

-- 修改操作符族 gist_cash_ops，添加针对 money 数据类型的检索函数 gbt_cash_fetch
ALTER OPERATOR FAMILY gist_cash_ops USING gist ADD
    FUNCTION    9 (money, money) gbt_cash_fetch (internal) ;

-- 修改操作符族 gist_macaddr_ops，添加针对 macaddr 数据类型的检索函数 gbt_macad_fetch
ALTER OPERATOR FAMILY gist_macaddr_ops USING gist ADD
	FUNCTION	9 (macaddr, macaddr) gbt_macad_fetch (internal) ;

ALTER OPERATOR FAMILY gist_text_ops USING gist ADD
	FUNCTION	9 (text, text) gbt_var_fetch (internal) ;

ALTER OPERATOR FAMILY gist_bpchar_ops USING gist ADD
	FUNCTION	9 (bpchar, bpchar) gbt_var_fetch (internal) ;

ALTER OPERATOR FAMILY gist_bytea_ops USING gist ADD
	FUNCTION	9 (bytea, bytea) gbt_var_fetch (internal) ;

ALTER OPERATOR FAMILY gist_numeric_ops USING gist ADD
	FUNCTION	9 (numeric, numeric) gbt_var_fetch (internal) ;

ALTER OPERATOR FAMILY gist_bit_ops USING gist ADD
	FUNCTION	9 (bit, bit) gbt_var_fetch (internal) ;

ALTER OPERATOR FAMILY gist_vbit_ops USING gist ADD
	FUNCTION	9 (varbit, varbit) gbt_var_fetch (internal) ;
