/*
 * contrib/opentenbase_memory/opentenbase_memory_tools--1.0.sql
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "create EXTENSION opentenbase_memory_tools" to load this file. \quit

--
-- pg_node_memory_detail()
--
CREATE FUNCTION pg_node_memory_detail(
    OUT nodename text,
    OUT pid int,
    OUT memorytype text,
    OUT memorykbytes int)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_node_memory_detail'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- pg_session_memory_detail()
--
CREATE FUNCTION pg_session_memory_detail(
    OUT contextname text,
    OUT contextlevel int,
    OUT parent text,
    OUT totalsize int,
    OUT freesize int)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_session_memory_detail'
LANGUAGE C STRICT PARALLEL SAFE;