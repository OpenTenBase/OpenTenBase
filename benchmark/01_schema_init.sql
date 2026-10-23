-- ============================================================
-- OpenTenBase 分布式数据库基准性能测试 SQL 脚本
-- Issue #202: 基准性能测试方案设计与 AI 辅助分析
-- ============================================================
-- 本脚本覆盖五大测试维度：
--   1. 单表写入 (Insert Benchmark)
--   2. 简单查询 (Simple Select Benchmark)
--   3. 聚合查询 (Aggregation Benchmark)
--   4. Join 查询 (Join Benchmark)
--   5. 并发连接 (Concurrency Stress)
--
-- 数据分布方式测试：
--   - Hash 分布 (DISTRIBUTE BY HASH)
--   - Replication 分布 (DISTRIBUTE BY REPLICATION)
--   - Modulo 分布 (DISTRIBUTE BY MODULO)
--   - Shard 分布 (DISTRIBUTE BY SHARD)
--
-- 使用方式：
--   psql -h <CN_IP> -p <CN_PORT> -U opentenbase -d postgres -f 01_schema_init.sql
-- ============================================================

-- ============================================================
-- 一、创建测试表（覆盖多种分布方式）
-- ============================================================

-- 1.1 Hash 分布表 — 模拟大多数业务场景的默认分布
CREATE TABLE IF NOT EXISTS bench_hash_orders (
    order_id      BIGINT        NOT NULL,
    customer_id   INT           NOT NULL,
    product_id    INT           NOT NULL,
    quantity      INT           NOT NULL DEFAULT 1,
    price         DECIMAL(10,2) NOT NULL,
    order_date    DATE          NOT NULL,
    status        VARCHAR(20)   NOT NULL DEFAULT 'pending',
    region        VARCHAR(50),
    notes         TEXT
) DISTRIBUTE BY HASH(order_id) TO GROUP default_group;

-- 1.2 Replication 分布表 — 模拟小维度表（全节点冗余）
CREATE TABLE IF NOT EXISTS bench_rep_customers (
    customer_id   INT           NOT NULL,
    name          VARCHAR(100)  NOT NULL,
    email         VARCHAR(200),
    city          VARCHAR(50),
    tier          VARCHAR(10)   NOT NULL DEFAULT 'normal',
    registered_at TIMESTAMP     NOT NULL DEFAULT NOW()
) DISTRIBUTE BY REPLICATION TO GROUP default_group;

-- 1.3 Modulo 分布表 — 模拟均匀轮询分布场景
CREATE TABLE IF NOT EXISTS bench_modulo_products (
    product_id    INT           NOT NULL,
    name          VARCHAR(100)  NOT NULL,
    category      VARCHAR(50)   NOT NULL,
    price         DECIMAL(10,2) NOT NULL,
    stock         INT           NOT NULL DEFAULT 0,
    supplier_id   INT
) DISTRIBUTE BY MODULO(product_id) TO GROUP default_group;

-- 1.4 Shard 分布表 — 模拟分片大表场景
CREATE TABLE IF NOT EXISTS bench_shard_transactions (
    txn_id        BIGINT        NOT NULL,
    account_id    BIGINT        NOT NULL,
    amount        DECIMAL(12,2) NOT NULL,
    txn_type      VARCHAR(20)   NOT NULL,
    created_at    TIMESTAMP     NOT NULL DEFAULT NOW(),
    balance_after DECIMAL(12,2)
) DISTRIBUTE BY SHARD(account_id) TO GROUP default_group;

-- 1.5 Hash 分布日志表 — 模拟高吞吐写入场景
CREATE TABLE IF NOT EXISTS bench_hash_logs (
    log_id        BIGSERIAL,
    service       VARCHAR(50)   NOT NULL,
    level         VARCHAR(10)   NOT NULL,
    message       TEXT          NOT NULL,
    timestamp     TIMESTAMP     NOT NULL DEFAULT NOW(),
    host          VARCHAR(50),
    trace_id      VARCHAR(64)
) DISTRIBUTE BY HASH(log_id) TO GROUP default_group;

-- ============================================================
-- 二、创建索引（评估索引对 CN 分发 + DN 执行的影响）
-- ============================================================

-- Hash orders: 常用查询字段索引
CREATE INDEX IF NOT EXISTS idx_hash_orders_customer ON bench_hash_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_hash_orders_date ON bench_hash_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_hash_orders_status ON bench_hash_orders(status);

-- Shard transactions: 账户维度查询
CREATE INDEX IF NOT EXISTS idx_shard_txn_account ON bench_shard_transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_shard_txn_type ON bench_shard_transactions(txn_type);

-- Hash logs: 日志查询
CREATE INDEX IF NOT EXISTS idx_hash_logs_service ON bench_hash_logs(service);
CREATE INDEX IF NOT EXISTS idx_hash_logs_level ON bench_hash_logs(level);
CREATE INDEX IF NOT EXISTS idx_hash_logs_ts ON bench_hash_logs(timestamp);

-- ============================================================
-- 三、辅助配置
-- ============================================================

-- 启用查询计时
\timing on

-- 显示查询计划输出开关（手动使用 EXPLAIN ANALYZE 时打开）
-- SET opentenbase_enable_query_plan_log = true;

-- 提示
\echo '✅ Schema initialized. Next: run 02_data_load.sql to populate data.'
