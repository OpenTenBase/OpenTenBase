-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.26'" to load this file. \quit

/*
 * VALUE takes as its argument a correlation variable (table alias) associated
 * with a row of an object table and returns object instances stored in the
 * object table.
 */
create or replace function opentenbase_ora.value(a anyelement) return anyelement
AS 'MODULE_PATHNAME','orcl_value'
LANGUAGE C IMMUTABLE;

drop package dbms_utility;
create package dbms_utility as
$p$
	FUNCTION format_call_stack(text) RETURNS text;
	FUNCTION format_call_stack() RETURNS text;
	FUNCTION get_hash_value(VARCHAR2, NUMBER, NUMBER) RETURNS NUMBER;
	FUNCTION get_time() RETURNS NUMBER;
$p$;

create package body dbms_utility as
$p$
	FUNCTION dbms_utility.format_call_stack(text)
	RETURNS text
	AS 'MODULE_PATHNAME','dbms_utility_format_call_stack1'
	LANGUAGE C STRICT VOLATILE;

	FUNCTION dbms_utility.format_call_stack()
	RETURNS text
	AS 'MODULE_PATHNAME','dbms_utility_format_call_stack0'
	LANGUAGE C VOLATILE;

	FUNCTION dbms_utility.get_hash_value(name VARCHAR2, base NUMBER, hash_size NUMBER)
	RETURNS NUMBER
	AS 'MODULE_PATHNAME','get_hash_value_ora'
	LANGUAGE C IMMUTABLE;

	FUNCTION dbms_utility.get_time()
	RETURNS NUMBER
	AS 'MODULE_PATHNAME','dbms_utility_get_time'
	LANGUAGE C VOLATILE;
$p$;

GRANT USAGE ON SCHEMA DBMS_UTILITY TO PUBLIC;
