-- 本文件中的 section 会由 benchmark_runner.sh 按名字提取并执行。
-- 以 pgbench_ 开头的 section 是给 pgbench 使用的 workload 脚本片段。

-- @section pgbench_w1_insert
\set user_id random(1, 10000)
\set amount_cent random(100, 500000)
\set status random(0, 4)
INSERT INTO perf_order (user_id, amount, status, created_at)
VALUES (:user_id, (:amount_cent / 100.0), :status, clock_timestamp());
-- @end

-- @section pgbench_w2_dist_key_lookup
\set user_id random(1, 10000)
SELECT order_id, amount, status, created_at
FROM perf_order
WHERE user_id = :user_id
ORDER BY created_at DESC
LIMIT 20;
-- @end

-- @section pgbench_w3_non_dist_filter
\set status random(0, 4)
SELECT count(*) AS order_count
FROM perf_order
WHERE status = :status;
-- @end

-- @section pgbench_w4_dist_key_aggregate
\set user_id_low random(1, 9900)
SELECT user_id, sum(amount) AS total_amount, count(*) AS order_count
FROM perf_order
WHERE user_id BETWEEN :user_id_low AND (:user_id_low + 99)
GROUP BY user_id
ORDER BY user_id;
-- @end

-- @section pgbench_w5_non_dist_aggregate
SELECT status, count(*) AS order_count, sum(amount) AS total_amount
FROM perf_order
GROUP BY status
ORDER BY status;
-- @end

-- @section pgbench_w6_colocated_join
\set user_id_low random(1, 9900)
SELECT u.user_id, u.user_name, sum(o.amount) AS total_amount, count(*) AS order_count
FROM perf_user u
JOIN perf_order o
  ON u.user_id = o.user_id
WHERE u.user_id BETWEEN :user_id_low AND (:user_id_low + 99)
GROUP BY u.user_id, u.user_name
ORDER BY u.user_id;
-- @end

-- @section pgbench_w7_replication_join
\set user_id_low random(1, 9900)
SELECT u.user_id, c.city_name, count(*) AS user_count
FROM perf_user u
JOIN perf_city c
  ON u.city_id = c.city_id
WHERE u.user_id BETWEEN :user_id_low AND (:user_id_low + 99)
GROUP BY u.user_id, c.city_name
ORDER BY u.user_id;
-- @end

-- @section pgbench_w8_gtm_short_tx
\set user_id random(1, 10000)
\set event_type random(0, 7)
\set status random(0, 4)
BEGIN;
INSERT INTO perf_event (user_id, event_type, payload, created_at)
VALUES (:user_id, :event_type, 'gtm_short_tx', clock_timestamp());
SELECT count(*) AS recent_orders
FROM perf_order
WHERE user_id = :user_id
  AND status = :status;
COMMIT;
-- @end

-- @section explain
\echo '=== W1 单表写入 ==='
EXPLAIN VERBOSE
INSERT INTO perf_order (user_id, amount, status, created_at)
VALUES (100, 99.99, 1, now());

\echo '=== W2 分布键点查 ==='
EXPLAIN (ANALYZE, VERBOSE, COSTS TRUE)
SELECT order_id, amount, status, created_at
FROM perf_order
WHERE user_id = 100
ORDER BY created_at DESC
LIMIT 20;

\echo '=== W3 非分布键过滤 ==='
EXPLAIN (ANALYZE, VERBOSE, COSTS TRUE)
SELECT count(*)
FROM perf_order
WHERE status = 2;

\echo '=== W4 分布键聚合 ==='
EXPLAIN (ANALYZE, VERBOSE, COSTS TRUE)
SELECT user_id, sum(amount) AS total_amount, count(*) AS order_count
FROM perf_order
WHERE user_id BETWEEN 100 AND 199
GROUP BY user_id
ORDER BY user_id;

\echo '=== W5 非分布键聚合 ==='
EXPLAIN (ANALYZE, VERBOSE, COSTS TRUE)
SELECT status, count(*) AS order_count, sum(amount) AS total_amount
FROM perf_order
GROUP BY status
ORDER BY status;

\echo '=== W6 同分布 Join ==='
EXPLAIN (ANALYZE, VERBOSE, COSTS TRUE)
SELECT u.user_id, u.user_name, sum(o.amount) AS total_amount, count(*) AS order_count
FROM perf_user u
JOIN perf_order o
  ON u.user_id = o.user_id
WHERE u.user_id BETWEEN 100 AND 199
GROUP BY u.user_id, u.user_name
ORDER BY u.user_id;

\echo '=== W7 复制表 Join ==='
EXPLAIN (ANALYZE, VERBOSE, COSTS TRUE)
SELECT u.user_id, c.city_name, count(*) AS user_count
FROM perf_user u
JOIN perf_city c
  ON u.city_id = c.city_id
WHERE u.user_id BETWEEN 100 AND 199
GROUP BY u.user_id, c.city_name
ORDER BY u.user_id;

\echo '=== W8 GTM 短事务组件 ==='
EXPLAIN VERBOSE
INSERT INTO perf_event (user_id, event_type, payload, created_at)
VALUES (100, 1, 'gtm_short_tx_explain', now());

EXPLAIN (ANALYZE, VERBOSE, COSTS TRUE)
SELECT count(*)
FROM perf_order
WHERE user_id = 100
  AND status = 1;
-- @end

-- @section distribution
SELECT node_name, node_type, node_host, node_port
FROM pgxc_node
ORDER BY node_name;

DROP TABLE IF EXISTS benchmark_distribution_snapshot;
CREATE TEMP TABLE benchmark_distribution_snapshot (
    node_name   text,
    table_name  text,
    row_count   bigint
);

DO $$
DECLARE
    node_rec record;
    row_rec  record;
    tbl_name text;
    stmt     text;
BEGIN
    FOR node_rec IN
        SELECT node_name
        FROM pgxc_node
        WHERE node_type = 'D'
        ORDER BY node_name
    LOOP
        FOREACH tbl_name IN ARRAY ARRAY['perf_user', 'perf_order', 'perf_city', 'perf_event']
        LOOP
            stmt := format(
                'EXECUTE DIRECT ON (%s) ''SELECT count(*) AS row_count FROM %I''',
                node_rec.node_name,
                tbl_name
            );

            FOR row_rec IN EXECUTE stmt
            LOOP
                INSERT INTO benchmark_distribution_snapshot (node_name, table_name, row_count)
                VALUES (node_rec.node_name, tbl_name, row_rec.row_count);
            END LOOP;
        END LOOP;
    END LOOP;
END;
$$;

SELECT node_name, table_name, row_count
FROM benchmark_distribution_snapshot
ORDER BY table_name, node_name;

SELECT
    table_name,
    max(row_count) AS max_rows,
    min(row_count) AS min_rows,
    round(avg(row_count)::numeric, 2) AS avg_rows,
    round(max(row_count)::numeric / NULLIF(avg(row_count), 0), 4) AS skew_ratio,
    round((max(row_count) - min(row_count))::numeric / NULLIF(avg(row_count), 0), 4) AS max_deviation_ratio
FROM benchmark_distribution_snapshot
GROUP BY table_name
ORDER BY table_name;

SELECT relname, reltuples::bigint AS planner_estimate
FROM pg_class
WHERE relname IN ('perf_user', 'perf_order', 'perf_city', 'perf_event')
ORDER BY relname;
-- @end

-- @section cleanup
DROP TABLE IF EXISTS perf_event;
DROP TABLE IF EXISTS perf_order;
DROP TABLE IF EXISTS perf_user;
DROP TABLE IF EXISTS perf_city;

DROP SEQUENCE IF EXISTS perf_event_id_seq;
DROP SEQUENCE IF EXISTS perf_order_id_seq;
-- @end
