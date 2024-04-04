/* contrib/btree_gist/btree_gist--1.0--1.1.sql */

-- 如果在 psql 中引入脚本，而不是通过 CREATE EXTENSION，请发出警告
\echo 使用 "ALTER EXTENSION btree_gist UPDATE TO '1.1'" 命令来加载此文件。\quit

-- 9.5 版本新增了支持索引扫描的功能。
-- 创建返回类型为 internal 的函数，用于索引扫描优化。
CREATE FUNCTION gbt_oid_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_int2_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_int4_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_int8_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_float4_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_float8_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_ts_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_time_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_date_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_intv_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_cash_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_macad_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_var_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- 将上述函数添加到对应的操作符族中
ALTER OPERATOR FAMILY gist_oid_ops USING gist ADD
    FUNCTION    9 (oid, oid) gbt_oid_fetch (internal);

ALTER OPERATOR FAMILY gist_int2_ops USING gist ADD
    FUNCTION    9 (int2, int2) gbt_int2_fetch (internal);

ALTER OPERATOR FAMILY gist_int4_ops USING gist ADD
    FUNCTION    9 (int4, int4) gbt_int4_fetch (internal);

ALTER OPERATOR FAMILY gist_int8_ops USING gist ADD
    FUNCTION    9 (int8, int8) gbt_int8_fetch (internal);

ALTER OPERATOR FAMILY gist_float4_ops USING gist ADD
    FUNCTION    9 (float4, float4) gbt_float4_fetch (internal);

ALTER OPERATOR FAMILY gist_float8_ops USING gist ADD
    FUNCTION    9 (float8, float8) gbt_float8_fetch (internal);

ALTER OPERATOR FAMILY gist_timestamp_ops USING gist ADD
    FUNCTION    9 (timestamp, timestamp) gbt_ts_fetch (internal);

ALTER OPERATOR FAMILY gist_timestamptz_ops USING gist ADD
    FUNCTION    9 (timestamptz, timestamptz) gbt_ts_fetch (internal);

ALTER OPERATOR FAMILY gist_time_ops USING gist ADD
    FUNCTION    9 (time, time) gbt_time_fetch (internal);

ALTER OPERATOR FAMILY gist_date_ops USING gist ADD
    FUNCTION    9 (date, date) gbt_date_fetch (internal);

ALTER OPERATOR FAMILY gist_interval_ops USING gist ADD
    FUNCTION    9 (interval, interval) gbt_intv_fetch (internal);

ALTER OPERATOR FAMILY gist_cash_ops USING gist ADD
    FUNCTION    9 (money, money) gbt_cash_fetch (internal);

ALTER OPERATOR FAMILY gist_macaddr_ops USING gist ADD
    FUNCTION    9 (macaddr, macaddr) gbt_macad_fetch (internal);

ALTER OPERATOR FAMILY gist_text_ops USING gist ADD
    FUNCTION    9 (text, text) gbt_var_fetch (internal);

ALTER OPERATOR FAMILY gist_bpchar_ops USING gist ADD
    FUNCTION    9 (bpchar, bpchar) gbt_var_fetch (internal);

ALTER OPERATOR FAMILY gist_bytea_ops USING gist ADD
    FUNCTION    9 (bytea, bytea) gbt_var_fetch (internal);

ALTER OPERATOR FAMILY gist_numeric_ops USING gist ADD
    FUNCTION    9 (numeric, numeric) gbt_var_fetch (internal);

ALTER OPERATOR FAMILY gist_bit_ops USING gist ADD
    FUNCTION    9 (bit, bit) gbt_var_fetch (internal);

ALTER OPERATOR FAMILY gist_vbit_ops USING gist ADD
    FUNCTION    9 (varbit, varbit) gbt_var_fetch (internal);
