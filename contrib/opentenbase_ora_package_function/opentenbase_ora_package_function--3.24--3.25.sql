-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.25'" to load this file. \quit

CREATE OR REPLACE FUNCTION pg_catalog.floor(pls_integer) RETURNS pls_integer AS
$$
SELECT pg_catalog.floor($1::numeric)::int;
$$
LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE;

ALTER FUNCTION SYS_GUID() PARALLEL RESTRICTED;
--upgrade package dbms_random
drop package dbms_random cascade;
create package dbms_random as
$p$
  PROCEDURE initialize(int);
  FUNCTION normal() RETURNS numeric;
  FUNCTION random() RETURNS integer;
  PROCEDURE seed(integer);
  PROCEDURE seed(text);
  FUNCTION string(opt text, len int) RETURNS text;
  PROCEDURE terminate();
  FUNCTION value(low numeric, high numeric) RETURNS numeric;
  FUNCTION value() RETURNS numeric;
$p$;

create package body dbms_random as
$p$
  PROCEDURE initialize(int)
  AS 'MODULE_PATHNAME','dbms_random_initialize'
  LANGUAGE C;
  FUNCTION normal()
  RETURNS NUMERIC
  AS 'MODULE_PATHNAME','dbms_random_normal'
  LANGUAGE C VOLATILE;
  FUNCTION random()
  RETURNS integer
  AS 'MODULE_PATHNAME','dbms_random_random'
  LANGUAGE C VOLATILE;
  PROCEDURE seed(integer)
  AS 'MODULE_PATHNAME','dbms_random_seed_int'
  LANGUAGE C;
  PROCEDURE seed(text)
  AS 'MODULE_PATHNAME','dbms_random_seed_varchar'
  LANGUAGE C;
  FUNCTION string(opt text, len int)
  RETURNS text
  AS 'MODULE_PATHNAME','dbms_random_string'
  LANGUAGE C VOLATILE;
  PROCEDURE terminate()
  AS 'MODULE_PATHNAME','dbms_random_terminate'
  LANGUAGE C;
  FUNCTION value(low numeric, high numeric)
  RETURNS numeric
  AS 'MODULE_PATHNAME','dbms_random_value_range'
  LANGUAGE C STRICT VOLATILE;
  FUNCTION value()
  RETURNS numeric
  AS 'MODULE_PATHNAME','dbms_random_value'
  LANGUAGE C VOLATILE;
$p$;
GRANT USAGE ON SCHEMA DBMS_RANDOM TO PUBLIC;
