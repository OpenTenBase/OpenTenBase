-- ============================================================
-- OpenTenBase 基准性能测试查询脚本
-- 覆盖: 简单查询、聚合查询、Join 查询
-- 每类查询包含 EXPLAIN ANALYZE + 实际执行
-- ============================================================

\timing on

-- ============================================================
-- 一、简单查询基准 (Simple Select)
-- ============================================================
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '📌 一、简单查询基准 (Simple Select)'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'

-- 1.1 Hash 分布表 — 主键查询（命中单个 DN）
\echo 'Q1: Hash 主键查询 (单 DN 定位)'
EXPLAIN ANALYZE SELECT * FROM bench_hash_orders WHERE order_id = 50000;

-- 1.2 Hash 分布表 — 非分布键查询（需广播到所有 DN）
\echo 'Q2: Hash 非分布键查询 (全 DN 广播)'
EXPLAIN ANALYZE SELECT * FROM bench_hash_orders WHERE customer_id = 100;

-- 1.3 Replication 分布表 — 查询（CN 本地执行，无远程交互）
\echo 'Q3: Replication 查询 (CN 本地)'
EXPLAIN ANALYZE SELECT * FROM bench_rep_customers WHERE customer_id = 1000;

-- 1.4 Shard 分布表 — 分布键查询（命中特定 shard）
\echo 'Q4: Shard 分布键查询 (定向 DN)'
EXPLAIN ANALYZE SELECT * FROM bench_shard_transactions WHERE account_id = 50000;

-- 1.5 Modulo 分布表 — 均匀查询
\echo 'Q5: Modulo 查询 (均匀分布)'
EXPLAIN ANALYZE SELECT * FROM bench_modulo_products WHERE product_id = 500;

-- ============================================================
-- 二、聚合查询基准 (Aggregation)
-- ============================================================
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '📌 二、聚合查询基准 (Aggregation)'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'

-- 2.1 Hash 分布 — COUNT 全表聚合（两阶段：DN 本地聚合 → CN 合并）
\echo 'Q6: Hash COUNT 全表聚合 (DN→CN 两阶段)'
EXPLAIN ANALYZE SELECT COUNT(*) FROM bench_hash_orders;

-- 2.2 Hash 分布 — GROUP BY 聚合（按分布键 vs 非分布键）
\echo 'Q7: Hash GROUP BY 分布键 (DN 本地 GROUP BY → CN 合并)'
EXPLAIN ANALYZE SELECT order_id % 1000 AS bucket, COUNT(*), AVG(price)
FROM bench_hash_orders GROUP BY order_id % 1000 ORDER BY bucket LIMIT 20;

\echo 'Q8: Hash GROUP BY 非分布键 (需重分布 → DN → CN)'
EXPLAIN ANALYZE SELECT status, COUNT(*), SUM(price)
FROM bench_hash_orders GROUP BY status;

-- 2.3 Shard 分布 — 时间范围聚合
\echo 'Q9: Shard 时间范围聚合'
EXPLAIN ANALYZE
SELECT DATE(created_at) AS day, txn_type, COUNT(*), SUM(amount)
FROM bench_shard_transactions
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY DATE(created_at), txn_type
ORDER BY day, txn_type;

-- 2.4 Hash 分布 — DISTINCT 基数聚合
\echo 'Q10: Hash DISTINCT 聚合'
EXPLAIN ANALYZE SELECT COUNT(DISTINCT customer_id) FROM bench_hash_orders;

-- 2.5 Replication — 本地聚合
\echo 'Q11: Replication 本地聚合 (CN 单节点执行)'
EXPLAIN ANALYZE SELECT tier, COUNT(*) FROM bench_rep_customers GROUP BY tier;

-- ============================================================
-- 三、Join 查询基准 (Join)
-- ============================================================
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '📌 三、Join 查询基准 (Join)'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'

-- 3.1 Hash × Replication Join (Hash 表 + 复制表)
-- Replication 表在每个 DN 都有副本，CN 可将 join 下推到 DN 本地执行
\echo 'Q12: Hash × Replication Join (可 DN 本地执行)'
EXPLAIN ANALYZE
SELECT o.order_id, o.price, c.name, c.tier
FROM bench_hash_orders o
JOIN bench_rep_customers c ON o.customer_id = c.customer_id
WHERE o.status = 'shipped' LIMIT 100;

-- 3.2 Hash × Hash Join (两表均为 Hash 分布，分布键不同)
-- 需要对其中一个表做重分布 (redistribute) → 网络开销大
\echo 'Q13: Hash × Hash Join 不同分布键 (需重分布)'
EXPLAIN ANALYZE
SELECT o.order_id, o.price, p.name AS product_name, p.category
FROM bench_hash_orders o
JOIN bench_modulo_products p ON o.product_id = p.product_id
WHERE o.price > 100 LIMIT 100;

-- 3.3 Hash × Shard Join (分布方式不同)
-- 需要评估 CN 的 join 策略选择
\echo 'Q14: Hash × Shard Join (分布策略不同)'
EXPLAIN ANALYZE
SELECT o.order_id, o.customer_id, t.txn_type, t.amount
FROM bench_hash_orders o
JOIN bench_shard_transactions t ON o.customer_id = t.account_id % 10000
WHERE o.order_date >= DATE('2025-06-01')
LIMIT 100;

-- 3.4 三表 Join (Hash + Replication + Modulo)
\echo 'Q15: 三表 Join (复杂分布式 join)'
EXPLAIN ANALYZE
SELECT o.order_id, c.name, c.tier, p.category, p.price AS product_price
FROM bench_hash_orders o
JOIN bench_rep_customers c ON o.customer_id = c.customer_id
JOIN bench_modulo_products p ON o.product_id = p.product_id
WHERE o.status = 'delivered' AND c.tier = 'vip'
LIMIT 100;

-- 3.5 自连接 (Self-Join on Hash table — 分布键相同)
\echo 'Q16: Hash 自连接 (相同分布键可 DN 本地执行)'
EXPLAIN ANALYZE
SELECT o1.order_id AS id1, o2.order_id AS id2, o1.price + o2.price AS total
FROM bench_hash_orders o1
JOIN bench_hash_orders o2 ON o1.customer_id = o2.customer_id AND o1.order_id <> o2.order_id
WHERE o1.customer_id = 100
LIMIT 50;

-- ============================================================
-- 四、数据分布方式对比 (Distribution Comparison)
-- ============================================================
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '📌 四、数据分布方式对比 (Distribution Comparison)'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'

-- 查看各表的 DN 数据分布情况
\echo 'Q17: Hash 分布 — 各 DN 行数'
SELECT node_name, COUNT(*) FROM bench_hash_orders GROUP BY node_name;

\echo 'Q18: Modulo 分布 — 各 DN 行数'
SELECT node_name, COUNT(*) FROM bench_modulo_products GROUP BY node_name;

\echo 'Q19: Shard 分布 — 各 DN 行数'
SELECT node_name, COUNT(*) FROM bench_shard_transactions GROUP BY node_name;

-- ============================================================
-- 五、分布式事务 (2PC) 基准
-- ============================================================
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '📌 五、分布式事务 (2PC) 基准'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'

-- 5.1 跨 DN 事务：Hash × Shard 双表写入 (2PC)
-- 涉及不同 DN 的事务需要 GTM 协调两阶段提交
\echo 'Q20: 跨 DN 2PC 事务 (Hash + Shard 双表写入)'
BEGIN;
INSERT INTO bench_hash_orders (order_id, customer_id, product_id, quantity, price, order_date, status, region)
VALUES (99990001, 9999, 999, 1, 100.00, NOW()::DATE, 'pending', 'Shanghai');
INSERT INTO bench_shard_transactions (txn_id, account_id, amount, txn_type, created_at)
VALUES (99990001, 9999, -100.00, 'withdrawal', NOW());
COMMIT;

-- 查看事务提交统计
\echo 'Q20-follow: 事务统计 (pgxc_stat_activity)'
SELECT node_name, xact_commit, xact_rollback FROM pgxc_stat_activity WHERE datname = current_database();

-- 5.2 跨 DN 事务回滚：验证 2PC 原子性
\echo 'Q21: 跨 DN 2PC 回滚 (原子性验证)'
BEGIN;
INSERT INTO bench_hash_orders (order_id, customer_id, product_id, quantity, price, order_date, status, region)
VALUES (99990002, 9999, 999, 2, 200.00, NOW()::DATE, 'pending', 'Shanghai');
INSERT INTO bench_shard_transactions (txn_id, account_id, amount, txn_type, created_at)
VALUES (99990002, 9999, -200.00, 'withdrawal', NOW());
ROLLBACK;

-- 验证回滚成功（以上两条记录不应存在）
\echo 'Q21-verify: 回滚验证'
SELECT COUNT(*) AS should_be_zero FROM bench_hash_orders WHERE order_id >= 99990000;
SELECT COUNT(*) AS should_be_zero FROM bench_shard_transactions WHERE txn_id >= 99990000;

-- 5.3 跨节点 Join + 数据倾斜场景
-- 使用 WHERE 条件使大部分数据落在单个 DN (模拟倾斜)
\echo 'Q22: Hash × Hash Join + 数据倾斜 (模拟热点)'
EXPLAIN ANALYZE
SELECT o.order_id, o.price, p.name, p.category
FROM bench_hash_orders o
JOIN bench_modulo_products p ON o.product_id = p.product_id
WHERE o.customer_id BETWEEN 1 AND 50  -- 倾斜：只查少量客户，大部分在同一 DN
LIMIT 100;

-- 5.4 长事务 + GTM 压力测试
\echo 'Q23: 长事务 (多语句 2PC，GTM 压力)'
BEGIN;
UPDATE bench_hash_orders SET status = 'processing' WHERE order_id BETWEEN 1 AND 100;
UPDATE bench_hash_orders SET status = 'shipped' WHERE order_id BETWEEN 101 AND 200;
INSERT INTO bench_hash_logs (service, level, message, host)
VALUES ('order-service', 'INFO', 'Batch update: 200 orders processed', 'host-1');
COMMIT;

\echo '✅ Benchmark queries complete (including 2PC and skew tests). See 04_pgbench_scripts.sh for concurrency tests.'
