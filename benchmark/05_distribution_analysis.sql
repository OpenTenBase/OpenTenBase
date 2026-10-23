-- ============================================================
-- OpenTenBase 数据分布分析脚本
-- 用于评估各 DN 的数据倾斜和查询计划特征
-- ============================================================

\timing on

\echo '🔍 OpenTenBase 分布式特征分析'
\echo ''

-- ============================================================
-- 一、数据分布倾斜分析
-- ============================================================
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '📌 1. 数据分布倾斜分析'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'

-- 各 DN 行数统计（需在 DN 上直接执行或通过 exec_on_dn）
\echo 'Hash 分布 — 数据分布均匀性检查'
SELECT
    node_name,
    COUNT(*) AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM bench_hash_orders
GROUP BY node_name
ORDER BY node_name;

\echo 'Shard 分布 — 数据分布均匀性检查'
SELECT
    node_name,
    COUNT(*) AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM bench_shard_transactions
GROUP BY node_name
ORDER BY node_name;

\echo 'Modulo 分布 — 数据分布均匀性检查'
SELECT
    node_name,
    COUNT(*) AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM bench_modulo_products
GROUP BY node_name
ORDER BY node_name;

\echo 'Replication 分布 — 全 DN 完整副本验证（每个 DN 行数应一致）'
-- 注意：CN 对 Replication 表通常仅查单个 DN，如需验证每个 DN 副本完整性，
-- 请通过 EXECUTE DIRECT ON (dn_1) ... 分别连接各 DN 执行 COUNT(*)。
SELECT
    node_name,
    COUNT(*) AS row_count
FROM bench_rep_customers
GROUP BY node_name
ORDER BY node_name;

-- ============================================================
-- 二、查询计划特征分析（CN 分发 vs DN 本地执行）
-- ============================================================
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '📌 2. 查询计划特征分析'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'

\echo 'Plan A: 分布键查询 — Remote SQL 下推到单个 DN'
EXPLAIN (VERBOSE, COSTS)
SELECT * FROM bench_hash_orders WHERE order_id = 50000;

\echo 'Plan B: 非分布键查询 — 需广播到所有 DN'
EXPLAIN (VERBOSE, COSTS)
SELECT * FROM bench_hash_orders WHERE customer_id = 100;

\echo 'Plan C: Replication 表查询 — CN 本地扫描'
EXPLAIN (VERBOSE, COSTS)
SELECT * FROM bench_rep_customers WHERE tier = 'vip';

\echo 'Plan D: Hash × Replication Join — 可 DN 本地执行'
EXPLAIN (VERBOSE, COSTS)
SELECT o.order_id, c.name
FROM bench_hash_orders o
JOIN bench_rep_customers c ON o.customer_id = c.customer_id
WHERE o.order_id = 50000;

\echo 'Plan E: Hash × Hash Join — 需重分布'
EXPLAIN (VERBOSE, COSTS)
SELECT o.order_id, p.name
FROM bench_hash_orders o
JOIN bench_modulo_products p ON o.product_id = p.product_id
WHERE o.customer_id = 100;

\echo 'Plan F: 全表聚合 — DN 本地聚合 + CN 合并'
EXPLAIN (VERBOSE, COSTS)
SELECT status, COUNT(*), AVG(price) FROM bench_hash_orders GROUP BY status;

\echo 'Plan G: ORDER BY + LIMIT — CN 排序合并'
EXPLAIN (VERBOSE, COSTS)
SELECT * FROM bench_hash_orders ORDER BY price DESC LIMIT 10;

-- ============================================================
-- 三、GTM 事务压力测试
-- ============================================================
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '📌 3. GTM 事务压力特征分析'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'

\echo 'GTM 活动统计 (pgxc_gtm_snap_stats)'
SELECT * FROM pgxc_gtm_snap_stats;

\echo '节点连接统计 (pgxc_node_stat)'
SELECT node_name, node_type, node_status FROM pgxc_node;

\echo ''
\echo '✅ 分布式特征分析完成。'
\echo '将以上 EXPLAIN ANALYZE 结果整理到性能报告中，分析瓶颈来源。'
