/* src/test/modules/test_csnlog_compress/test_csnlog_compress--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_csnlog_compress" to load this file. \quit

CREATE FUNCTION test_compress_csnlog_file()
RETURNS pg_catalog.bool STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_rename_csnlog_file()
RETURNS pg_catalog.bool STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
