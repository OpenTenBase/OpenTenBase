/*
 * contrib/pg_unlock/pg_unlock--unpackaged--1.0.sql
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_unlock" to load this file. \quit

ALTER EXTENSION pg_unlock ADD function pg_unlock_execute();
ALTER EXTENSION pg_unlock ADD function pg_unlock_check_deadlock();
ALTER EXTENSION pg_unlock ADD function pg_unlock_killbypid(nodename text, pid int4);
ALTER EXTENSION pg_unlock ADD function pg_unlock_check_dependency();
ALTER EXTENSION pg_findgxid ADD function pg_findgxid(txnid int8);