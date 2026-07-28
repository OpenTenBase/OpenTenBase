-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set ON_ERROR_STOP on
\pset pager off
\timing on
SET search_path TO otb_bench, public;

\echo '=== distribution-key point read ==='
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, NODES)
SELECT account_id, region_id, status, balance
FROM account
WHERE account_id = 42;

\echo '=== cross-shard aggregate ==='
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, NODES)
SELECT account_id, sum(amount), count(*)
FROM customer_order
WHERE account_id BETWEEN 1 AND 1000
GROUP BY account_id;

\echo '=== colocated join ==='
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, NODES)
SELECT a.account_id, count(*), sum(o.amount)
FROM account AS a
JOIN customer_order AS o USING (account_id)
WHERE a.account_id BETWEEN 1 AND 1000
GROUP BY a.account_id;

\echo '=== replicated dimension join ==='
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, NODES)
SELECT r.region_name, count(*), sum(o.amount)
FROM customer_order AS o
JOIN region AS r USING (region_id)
WHERE o.account_id BETWEEN 1 AND 1000
GROUP BY r.region_name;

\echo '=== redistributed join ==='
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, NODES)
SELECT count(*)
FROM customer_order AS o
JOIN order_audit AS x USING (order_id)
WHERE o.account_id BETWEEN 1 AND 1000;
