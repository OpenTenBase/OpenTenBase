/* contrib/pg_unlock/pg_unlock--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION pg_unlock UPDATE TO '1.1'" to load this file. \quit

-- Register functions.
CREATE FUNCTION pg_cancel_backend_msg(
	IN	pid	int4,
	IN	cancel_msg text,
	OUT success bool
)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

GRANT ALL ON FUNCTION pg_cancel_backend_msg(pid	int4, cancel_msg text) TO PUBLIC;

