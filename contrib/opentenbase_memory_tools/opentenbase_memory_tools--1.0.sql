/*
 * contrib/opentenbase_memory/opentenbase_memory_tools--1.0.sql
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
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