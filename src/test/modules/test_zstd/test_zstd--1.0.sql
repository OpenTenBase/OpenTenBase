/* src/test/modules/test_zstd/test_zstd--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_zstd" to load this file. \quit

CREATE FUNCTION test_compress_and_decompress()
RETURNS pg_catalog.bool STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

