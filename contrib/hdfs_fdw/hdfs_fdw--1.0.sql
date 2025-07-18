-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hdfs_fdw" to load this file. \quit

CREATE FUNCTION hdfs_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION hdfs_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER hdfs_fdw
  HANDLER hdfs_fdw_handler
  VALIDATOR hdfs_fdw_validator;
