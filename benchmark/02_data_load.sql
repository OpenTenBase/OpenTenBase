-- ============================================================
-- OpenTenBase 数据加载脚本
-- 使用 generate_series + 随机函数批量插入
-- 避免单行 INSERT，模拟真实批量写入场景
-- ============================================================

-- 配置参数（根据测试规模调整）
-- 小规模测试: 10K 行 / 中规模: 100K 行 / 大规模: 1M 行
-- 默认使用中规模

\echo '🚀 开始数据加载（中规模: ~100K 行）...'

-- ============================================================
-- 1. Hash 分布订单表 (bench_hash_orders)
-- ============================================================
INSERT INTO bench_hash_orders (order_id, customer_id, product_id, quantity, price, order_date, status, region, notes)
SELECT
    g,
    (random() * 9999 + 1)::INT,
    (random() * 999 + 1)::INT,
    (random() * 10 + 1)::INT,
    (random() * 999.99)::DECIMAL(10,2),
    DATE('2025-01-01') + (random() * 365)::INT,
    CASE (random() * 4)::INT
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'shipped'
        WHEN 2 THEN 'delivered'
        ELSE 'cancelled'
    END,
    CASE (random() * 5)::INT
        WHEN 0 THEN 'Beijing'
        WHEN 1 THEN 'Shanghai'
        WHEN 2 THEN 'Guangzhou'
        WHEN 3 THEN 'Shenzhen'
        ELSE 'Chengdu'
    END,
    NULL
FROM generate_series(1, 100000) AS g;

\echo '✅ bench_hash_orders: 100,000 rows loaded.'

-- ============================================================
-- 2. Replication 分布客户表 (bench_rep_customers)
-- ============================================================
INSERT INTO bench_rep_customers (customer_id, name, email, city, tier)
SELECT
    g,
    'customer_' || g,
    'cust' || g || '@example.com',
    CASE (random() * 5)::INT
        WHEN 0 THEN 'Beijing'
        WHEN 1 THEN 'Shanghai'
        WHEN 2 THEN 'Guangzhou'
        WHEN 3 THEN 'Shenzhen'
        ELSE 'Chengdu'
    END,
    CASE (random() * 3)::INT
        WHEN 0 THEN 'normal'
        WHEN 1 THEN 'premium'
        ELSE 'vip'
    END
FROM generate_series(1, 10000) AS g;

\echo '✅ bench_rep_customers: 10,000 rows loaded.'

-- ============================================================
-- 3. Modulo 分布产品表 (bench_modulo_products)
-- ============================================================
INSERT INTO bench_modulo_products (product_id, name, category, price, stock, supplier_id)
SELECT
    g,
    'product_' || g,
    CASE (random() * 5)::INT
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Food'
        WHEN 3 THEN 'Books'
        ELSE 'Sports'
    END,
    (random() * 499.99 + 10)::DECIMAL(10,2),
    (random() * 1000)::INT,
    (random() * 99 + 1)::INT
FROM generate_series(1, 1000) AS g;

\echo '✅ bench_modulo_products: 1,000 rows loaded.'

-- ============================================================
-- 4. Shard 分布交易表 (bench_shard_transactions)
-- ============================================================
INSERT INTO bench_shard_transactions (txn_id, account_id, amount, txn_type, created_at, balance_after)
SELECT
    g,
    (random() * 999999 + 1)::BIGINT,
    (random() * 99999.99 - 50000)::DECIMAL(12,2),
    CASE (random() * 3)::INT
        WHEN 0 THEN 'deposit'
        WHEN 1 THEN 'withdrawal'
        ELSE 'transfer'
    END,
    NOW() - (random() * 86400 * 30)::INT * INTERVAL '1 second',
    (random() * 100000)::DECIMAL(12,2)
FROM generate_series(1, 100000) AS g;

\echo '✅ bench_shard_transactions: 100,000 rows loaded.'

-- ============================================================
-- 5. Hash 分布日志表 (bench_hash_logs) — 高吞吐场景
-- ============================================================
INSERT INTO bench_hash_logs (service, level, message, host, trace_id)
SELECT
    CASE (random() * 5)::INT
        WHEN 0 THEN 'order-service'
        WHEN 1 THEN 'payment-service'
        WHEN 2 THEN 'auth-service'
        WHEN 3 THEN 'inventory-service'
        ELSE 'gateway'
    END,
    CASE (random() * 4)::INT
        WHEN 0 THEN 'INFO'
        WHEN 1 THEN 'WARN'
        WHEN 2 THEN 'ERROR'
        ELSE 'DEBUG'
    END,
    'Log message #' || g || ': ' || CASE (random() * 3)::INT
        WHEN 0 THEN 'Request processed successfully'
        WHEN 1 THEN 'Timeout detected on downstream'
        ELSE 'Authentication failed'
    END,
    'host-' || (random() * 10 + 1)::INT,
    'trace-' || g
FROM generate_series(1, 500000) AS g;

\echo '✅ bench_hash_logs: 500,000 rows loaded.'

-- ============================================================
-- 统计确认
-- ============================================================
\echo '📊 加载完成，统计各表行数:'
SELECT 'bench_hash_orders' AS tbl, COUNT(*) AS cnt FROM bench_hash_orders
UNION ALL
SELECT 'bench_rep_customers', COUNT(*) FROM bench_rep_customers
UNION ALL
SELECT 'bench_modulo_products', COUNT(*) FROM bench_modulo_products
UNION ALL
SELECT 'bench_shard_transactions', COUNT(*) FROM bench_shard_transactions
UNION ALL
SELECT 'bench_hash_logs', COUNT(*) FROM bench_hash_logs;

\echo '✅ Data loading complete. Next: run 03_benchmark_queries.sql for benchmark queries.'
