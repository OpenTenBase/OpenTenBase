\echo Use "CREATE EXTENSION slow_sql_recorder" to load this file. \quit

CREATE FUNCTION record(integer)
    RETURNS integer
    AS 'MODULE_PATHNAME', 'record'
    LANGUAGE C STRICT;

CREATE FUNCTION report()
    RETURNS integer
    AS 'MODULE_PATHNAME', 'report'
    LANGUAGE C STRICT;
    