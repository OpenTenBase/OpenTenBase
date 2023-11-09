/*
 * contrib/pg_unlock/pg_unlock--unpackaged--1.0.sql
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_unlock" to load this file. \quit

ALTER EXTENSION pg_unlock ADD function pg_unlock_execute();
ALTER EXTENSION pg_unlock ADD function pg_unlock_check_deadlock();
ALTER EXTENSION pg_unlock ADD function pg_unlock_killbypid(nodename text, pid int4);
ALTER EXTENSION pg_unlock ADD function pg_unlock_check_dependency();
ALTER EXTENSION pg_findgxid ADD function pg_findgxid(txnid int8);