/* opentenbase_mysql_compat--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION opentenbase_mysql_compat" to load this file. \quit

COMMENT ON EXTENSION opentenbase_mysql_compat IS 'MySQL-compatible functions for OpenTenBase';

/* ---------------------------- string ---------------------------- */

-- MySQL CONCAT(): NULL argument makes the whole result NULL, and every
-- argument is implicitly converted to text.
CREATE FUNCTION concat(VARIADIC "any")
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_concat'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- MySQL CONCAT_WS(): NULL separator yields NULL, NULL arguments are skipped.
CREATE FUNCTION concat_ws(text, VARIADIC "any")
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_concat_ws'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION find_in_set(text, text)
RETURNS int
AS 'MODULE_PATHNAME', 'mysql_find_in_set'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION elt(int, VARIADIC text)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_elt'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION field(text, VARIADIC text)
RETURNS int
AS 'MODULE_PATHNAME', 'mysql_field'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- INSERT is an SQL reserved word, so the MySQL spelling must be quoted
-- when called: SELECT "insert"('quadratic', 2, 3, 'What');
-- mysql_insert() is provided as an alias that needs no quoting.
CREATE FUNCTION "insert"(text, int, int, text)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_insert'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION mysql_insert(text, int, int, text)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_insert'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION substring_index(text, text, int)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_substring_index'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION locate(text, text)
RETURNS int
AS 'MODULE_PATHNAME', 'mysql_locate2'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION locate(text, text, int)
RETURNS int
AS 'MODULE_PATHNAME', 'mysql_locate3'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION instr(text, text)
RETURNS int
AS 'MODULE_PATHNAME', 'mysql_instr'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION reverse(text)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_reverse'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION space(int)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_space'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION unhex(text)
RETURNS bytea
AS 'MODULE_PATHNAME', 'mysql_unhex'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

/* --------------------------- datetime --------------------------- */

CREATE FUNCTION date_format(timestamp, text)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_date_format'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION date_format(date, text)
RETURNS text
AS 'SELECT date_format($1::timestamp, $2)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION str_to_date(text, text)
RETURNS timestamp
AS 'MODULE_PATHNAME', 'mysql_str_to_date'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION datediff(timestamp, timestamp)
RETURNS int
AS 'MODULE_PATHNAME', 'mysql_datediff'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION datediff(date, date)
RETURNS int
AS 'SELECT datediff($1::timestamp, $2::timestamp)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION timestampdiff(text, timestamp, timestamp)
RETURNS bigint
AS 'MODULE_PATHNAME', 'mysql_timestampdiff'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION timestampadd(text, bigint, timestamp)
RETURNS timestamp
AS 'MODULE_PATHNAME', 'mysql_timestampadd'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION last_day(timestamp)
RETURNS date
AS 'MODULE_PATHNAME', 'mysql_last_day'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION last_day(date)
RETURNS date
AS 'SELECT last_day($1::timestamp)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION quarter(timestamp)
RETURNS int
AS 'SELECT ((extract(month from $1)::int + 2) / 3)::int'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION quarter(date)
RETURNS int
AS 'SELECT quarter($1::timestamp)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dayofweek(timestamp)
RETURNS int
AS 'SELECT (extract(dow from $1)::int + 1)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION weekday(timestamp)
RETURNS int
AS 'SELECT ((extract(dow from $1)::int + 6) % 7)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dayofmonth(timestamp)
RETURNS int
AS 'SELECT extract(day from $1)::int'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dayofyear(timestamp)
RETURNS int
AS 'SELECT extract(doy from $1)::int'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION weekofyear(timestamp)
RETURNS int
AS 'SELECT extract(week from $1)::int'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

/* ----------------------------- math ----------------------------- */

CREATE FUNCTION format(numeric, int)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_format'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION format(double precision, int)
RETURNS text
AS 'SELECT format($1::numeric, $2)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION format(int, int)
RETURNS text
AS 'SELECT format($1::numeric, $2)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- TRUNCATE is an SQL reserved word; quote it when calling, or use the
-- trunc() wrapper below with MySQL-compatible argument typing.
CREATE FUNCTION "truncate"(numeric, int)
RETURNS numeric
AS 'SELECT trunc($1, $2)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION "truncate"(double precision, int)
RETURNS double precision
AS 'SELECT trunc($1::numeric, $2)::double precision'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION "truncate"(int, int)
RETURNS numeric
AS 'SELECT trunc($1::numeric, $2)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION mysql_truncate(numeric, int)
RETURNS numeric
AS 'SELECT trunc($1, $2)'
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bin(bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_bin'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION oct(bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_oct'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

/* -------------------------- flow control ------------------------- */

CREATE FUNCTION isnull("any")
RETURNS int
AS 'MODULE_PATHNAME', 'mysql_isnull'
LANGUAGE C VOLATILE;

CREATE FUNCTION if(boolean, anyelement, anyelement)
RETURNS anyelement
AS 'SELECT CASE WHEN $1 THEN $2 ELSE $3 END'
LANGUAGE SQL;

CREATE FUNCTION ifnull(anyelement, anyelement)
RETURNS anyelement
AS 'SELECT coalesce($1, $2)'
LANGUAGE SQL;

/* --------------------------- aggregates -------------------------- */

CREATE FUNCTION mysql_group_concat_trans(text, anyelement)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_group_concat_trans'
LANGUAGE C;

CREATE FUNCTION mysql_group_concat_sep_trans(text, anyelement, text)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_group_concat_sep_trans'
LANGUAGE C;

-- GROUP_CONCAT(expr) joins non-NULL values with commas; use the
-- aggregate ORDER BY syntax for ordering: GROUP_CONCAT(x ORDER BY x).
CREATE AGGREGATE group_concat(anyelement) (
    SFUNC = mysql_group_concat_trans,
    STYPE = text
);

-- GROUP_CONCAT(expr, sep) is the two-argument spelling of
-- GROUP_CONCAT(expr SEPARATOR sep).
CREATE AGGREGATE group_concat(anyelement, text) (
    SFUNC = mysql_group_concat_sep_trans,
    STYPE = text
);
