/* contrib/pg_unlock/pg_unlock--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_unlock" to load this file. \quit

ALTER EXTENSION pg_unlock ADD function pg_unlock_execute();
ALTER EXTENSION pg_unlock ADD function pg_unlock_check_deadlock();
ALTER EXTENSION pg_unlock ADD function pg_unlock_killbypid(nodename text, pid int4);
ALTER EXTENSION pg_unlock ADD function pg_unlock_check_dependency();
ALTER EXTENSION pg_unlock ADD function pg_findgxid(txnid int8);
ALTER EXTENSION pg_unlock ADD function pg_unlock_wait_status();
ALTER EXTENSION pg_unlock ADD function pg_unlock_check_deadlock_bak();
ALTER EXTENSION pg_unlock ADD function pg_unlock_check_dependency_bak();
ALTER EXTENSION pg_unlock ADD function pg_unlock_execute_bak();
