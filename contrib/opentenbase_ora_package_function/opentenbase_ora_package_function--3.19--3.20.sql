-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.20'" to load this file. \quit

-- create operator of interval and integer
/*CREATE FUNCTION opentenbase_ora.interval_int4_lt(a interval, b int4) RETURNS bool
AS $$SELECT extract(epoch from (a)/60/60/24) < b; $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.int4_interval_lt(a int4, b interval) RETURNS bool
AS $$SELECT a < extract(epoch from (b)/60/60/24); $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.interval_int4_gt(a interval, b int4) RETURNS bool
AS $$SELECT extract(epoch from (a)/60/60/24) > b; $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.int4_interval_gt(a int4, b interval) RETURNS bool
AS $$SELECT a > extract(epoch from (b)/60/60/24); $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.interval_int4_eq(a interval, b int4) RETURNS bool
AS $$SELECT extract(epoch from (a)/60/60/24) = b; $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.int4_interval_eq(a int4, b interval) RETURNS bool
AS $$SELECT a = extract(epoch from (b)/60/60/24); $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.interval_int4_ne(a interval, b int4) RETURNS bool
AS $$SELECT extract(epoch from (a)/60/60/24) <> b; $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.int4_interval_ne(a int4, b interval) RETURNS bool
AS $$SELECT a <> extract(epoch from (b)/60/60/24); $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.interval_int4_le(a interval, b int4) RETURNS bool
AS $$SELECT extract(epoch from (a)/60/60/24) <= b; $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.int4_interval_le(a int4, b interval) RETURNS bool
AS $$SELECT a <= extract(epoch from (b)/60/60/24); $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.interval_int4_ge(a interval, b int4) RETURNS bool
AS $$SELECT extract(epoch from (a)/60/60/24) >= b; $$
LANGUAGE SQL;
CREATE FUNCTION opentenbase_ora.int4_interval_ge(a int4, b interval) RETURNS bool
AS $$SELECT a >= extract(epoch from (b)/60/60/24); $$
LANGUAGE SQL;

CREATE OPERATOR < (
	leftarg = interval,
	rightarg = int4,
	commutator = >,
	negator = >=,
	function = opentenbase_ora.interval_int4_lt);
CREATE OPERATOR < (
	leftarg = int4,
	rightarg = interval,
	commutator = >,
	negator = >=,
	function = opentenbase_ora.int4_interval_lt);
CREATE OPERATOR > (
	leftarg = interval,
	rightarg = int4,
	commutator = <,
	negator = <=,
	function = opentenbase_ora.interval_int4_gt);
CREATE OPERATOR > (
	leftarg = int4,
	rightarg = interval,
	commutator = <,
	negator = <=,
	function = opentenbase_ora.int4_interval_gt);
CREATE OPERATOR = (
	leftarg = interval,
	rightarg = int4,
	commutator = =,
	negator = <>,
	function = opentenbase_ora.interval_int4_eq,
	HASHES, MERGES);
CREATE OPERATOR = (
	leftarg = int4,
	rightarg = interval,
	commutator = =,
	negator = <>,
	function = opentenbase_ora.int4_interval_eq,
	HASHES, MERGES);
CREATE OPERATOR <> (
	leftarg = interval,
	rightarg = int4,
	commutator = <>,
	negator = =,
	function = opentenbase_ora.interval_int4_ne);
CREATE OPERATOR <> (
	leftarg = int4,
	rightarg = interval,
	commutator = <>,
	negator = =,
	function = opentenbase_ora.int4_interval_ne);
CREATE OPERATOR <= (
	leftarg = interval,
	rightarg = int4,
	commutator = >=,
	negator = >,
	function = opentenbase_ora.interval_int4_le);
CREATE OPERATOR <= (
	leftarg = int4,
	rightarg = interval,
	commutator = >=,
	negator = >,
	function = opentenbase_ora.int4_interval_le);
CREATE OPERATOR >= (
	leftarg = interval,
	rightarg = int4,
	commutator = <=,
	negator = <,
	function = opentenbase_ora.interval_int4_ge);
CREATE OPERATOR >= (
	leftarg = int4,
	rightarg = interval,
	commutator = <=,
	negator = <,
	function = opentenbase_ora.int4_interval_ge);

alter operator = (opentenbase_ora.date, opentenbase_ora.date) set (RESTRICT = 'eqsel', JOIN = 'eqjoinsel');
alter operator <> (opentenbase_ora.date, opentenbase_ora.date) set (RESTRICT = 'neqsel', JOIN = 'neqjoinsel');
alter operator < (opentenbase_ora.date, opentenbase_ora.date) set (RESTRICT = 'scalarltsel', JOIN = 'scalarltjoinsel');
alter operator <= (opentenbase_ora.date, opentenbase_ora.date) set (RESTRICT = 'scalarltsel', JOIN = 'scalarltjoinsel');
alter operator > (opentenbase_ora.date, opentenbase_ora.date) set (RESTRICT = 'scalargtsel', JOIN = 'scalargtjoinsel');
alter operator >= (opentenbase_ora.date, opentenbase_ora.date) set (RESTRICT = 'scalargtsel', JOIN = 'scalargtjoinsel');*/
