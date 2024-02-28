\echo Use "CREATE EXTENSION pg_testgen" to load this file. \quit

CREATE FUNCTION record(integer)
    RETURNS integer
    AS 'MODULE_PATHNAME', 'record'
    LANGUAGE C STRICT;

CREATE FUNCTION report()
    RETURNS SETOF varchar 
    AS 'MODULE_PATHNAME', 'report'
    LANGUAGE C STRICT;
    