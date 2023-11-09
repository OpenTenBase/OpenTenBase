/*
 * contrib/pg_stat_cluster_activity/pg_stat_cluster_activity--unpackaged--1.0.sql
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_stat_cluster_activity" to load this file. \quit

ALTER EXTENSION pg_stat_cluster_activity ADD function pg_stat_cluster_get_activity();
ALTER EXTENSION pg_stat_statements ADD view pg_stat_cluster_activity;
ALTER EXTENSION pg_stat_statements ADD view pg_stat_cluster_activity_cn;
