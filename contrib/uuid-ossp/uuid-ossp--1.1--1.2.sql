/* contrib/uuid-ossp/uuid-ossp--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION uuid-ossp UPDATE TO '1.2'" to load this file. \quit

CREATE OR REPLACE FUNCTION pg_catalog.sys_guid()
    RETURNS text
AS 'MODULE_PATHNAME', 'sys_guid'
    VOLATILE STRICT LANGUAGE C PARALLEL SAFE;
