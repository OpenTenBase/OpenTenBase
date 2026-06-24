-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.27'" to load this file. \quit

-- following is used to support window frame range in
-- begin:20240906
-- int8
CREATE FUNCTION pg_catalog.in_range(int8, int8, int8, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_int8_int8_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- int4
CREATE FUNCTION pg_catalog.in_range(int4, int4, int8, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_int4_int8_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pg_catalog.in_range(int4, int4, int4, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_int4_int4_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pg_catalog.in_range(int4, int4, int2, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_int4_int2_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pg_catalog.in_range(int2, int2, int8, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_int2_int8_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pg_catalog.in_range(int2, int2, int2, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_int2_int2_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pg_catalog.in_range(int2, int2, int4, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_int2_int4_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- pg_catalog.date
CREATE FUNCTION pg_catalog.in_range(pg_catalog.date, pg_catalog.date, interval, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_date_interval_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- pg_catalog.timestamp
CREATE FUNCTION pg_catalog.in_range(pg_catalog.timestamp, pg_catalog.timestamp, interval, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_timestamp_interval_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- pg_catalog.timestamptz
CREATE FUNCTION pg_catalog.in_range(pg_catalog.timestamptz, pg_catalog.timestamptz, interval, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_timestamptz_interval_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- interval
CREATE FUNCTION pg_catalog.in_range(interval, interval, interval, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_interval_interval_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- time
CREATE FUNCTION pg_catalog.in_range(time, time, interval, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_time_interval_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- timetz
CREATE FUNCTION pg_catalog.in_range(timetz, timetz, interval, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_timetz_interval_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- float8
CREATE FUNCTION pg_catalog.in_range(float8, float8, float8, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_float8_float8_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- float4
CREATE FUNCTION pg_catalog.in_range(float4, float4, float8, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_float4_float8_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- numeric
CREATE FUNCTION pg_catalog.in_range(numeric, numeric, numeric, bool, bool)
RETURNS BOOL AS
'MODULE_PATHNAME', 'in_range_numeric_numeric_ex'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

ALTER OPERATOR FAMILY datetime_ops USING btree
ADD FUNCTION 3 ( PG_CATALOG.DATE, INTERVAL) in_range(PG_CATALOG.DATE, PG_CATALOG.DATE, interval, bool, bool);

ALTER OPERATOR FAMILY datetime_ops USING btree
ADD FUNCTION 3 ( PG_CATALOG.TIMESTAMP, INTERVAL) in_range(PG_CATALOG.TIMESTAMP, PG_CATALOG.TIMESTAMP, interval, bool, bool);

ALTER OPERATOR FAMILY datetime_ops USING btree
ADD FUNCTION 3 ( PG_CATALOG.TIMESTAMPTZ, INTERVAL) in_range(PG_CATALOG.TIMESTAMPTZ, PG_CATALOG.TIMESTAMPTZ, interval, bool, bool);

ALTER OPERATOR FAMILY float_ops USING btree
ADD FUNCTION 3 (float8, float8) in_range(float8, float8, float8, bool, bool);

ALTER OPERATOR FAMILY float_ops USING btree
ADD FUNCTION 3 (float4, float8) in_range(float4, float4, float8, bool, bool);

ALTER OPERATOR FAMILY integer_ops USING btree
ADD FUNCTION 3 (int2, int8) in_range(int2, int2, int8, bool, bool);

ALTER OPERATOR FAMILY integer_ops USING btree
ADD FUNCTION 3 (int2, int4) in_range(int2, int2, int4, bool, bool);

ALTER OPERATOR FAMILY integer_ops USING btree
ADD FUNCTION 3 (int2, int2) in_range(int2, int2, int2, bool, bool);

ALTER OPERATOR FAMILY integer_ops USING btree
ADD FUNCTION 3 (int4, int8) in_range(int4, int4, int8, bool, bool);

ALTER OPERATOR FAMILY integer_ops USING btree
ADD FUNCTION 3 (int4, int4) in_range(int4, int4, int4, bool, bool);

ALTER OPERATOR FAMILY integer_ops USING btree
ADD FUNCTION 3 (int4, int2) in_range(int4, int4, int2, bool, bool);

ALTER OPERATOR FAMILY integer_ops USING btree
ADD FUNCTION 3 (int8, int8) in_range(int8, int8, int8, bool, bool);

ALTER OPERATOR FAMILY interval_ops USING btree
ADD FUNCTION 3 (interval, interval) in_range(interval, interval, interval, bool, bool);

ALTER OPERATOR FAMILY numeric_ops USING btree
ADD FUNCTION 3 (numeric, numeric) in_range(numeric, numeric, numeric, bool, bool);

ALTER OPERATOR FAMILY time_ops USING btree
ADD FUNCTION 3 (time, interval) in_range(time, time, interval, bool, bool);

ALTER OPERATOR FAMILY timetz_ops USING btree
ADD FUNCTION 3 (timetz, interval) in_range(timetz, timetz, interval, bool, bool);
