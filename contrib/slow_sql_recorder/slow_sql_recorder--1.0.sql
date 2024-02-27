\echo Use "CREATE EXTENSION pg_testgen" to load this file. \quit

CREATE FUNCTION record()
    RETURNS integer
    AS 'MODULE_PATHNAME', 'record'
    LANGUAGE C STRICT;

CREATE FUNCTION report()
    RETURNS integer
    AS 'MODULE_PATHNAME', 'report'
    LANGUAGE C STRICT;
    