/* contrib/opentenbase_pooler_stat/opentenbase_pooler_stat--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION opentenbase_pooler_stat FROM unpackaged" to load this file. \quit

ALTER EXTENSION opentenbase_pooler_stat ADD function opentenbase_get_pooler_cmd_statistics();
ALTER EXTENSION opentenbase_pooler_stat ADD function opentenbase_reset_pooler_cmd_statistics();
ALTER EXTENSION opentenbase_pooler_stat ADD function opentenbase_get_pooler_conn_statistics();