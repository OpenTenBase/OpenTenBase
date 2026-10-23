#!/bin/bash
# ============================================================
# OpenTenBase pgbench 并发压力测试脚本
# Issue #202: 基准性能测试方案设计与 AI 辅助分析
# ============================================================
#
# 前置条件:
#   1. OpenTenBase 集群已启动 (CN/DN/GTM 运行中)
#   2. 测试表已通过 01_schema_init.sql + 02_data_load.sql 初始化
#   3. pgbench 可用 (OpenTenBase 包含 pgbench 工具)
#
# 兼容性说明:
#   - pgbench 版本: PostgreSQL 12+ 或 OpenTenBase 自带的 pgbench
#   - 操作系统: Linux (Ubuntu 18.04+ / CentOS 7+ / Debian 10+)
#   - macOS: 支持，但 \if 语法需 pgbench 11+ 版本
#   - 自定义脚本中的 \set 和 \if 语法在 pgbench 11+ 可用
#   - 确认 pgbench 版本: pgbench --version
#
# 版本兼容矩阵:
#   ┌──────────────────┬──────────┬──────────────────┐
#   │ pgbench 版本      │ 兼容性    │ 备注              │
#   ├──────────────────┼──────────┼──────────────────┤
#   │ OpenTenBase 内置  │ ✅ 推荐   │ 最稳定             │
#   │ PostgreSQL 16     │ ✅       │ 完全兼容           │
#   │ PostgreSQL 15     │ ✅       │ 完全兼容           │
#   │ PostgreSQL 14     │ ✅       │ 完全兼容           │
#   │ PostgreSQL 13     │ ✅       │ 完全兼容           │
#   │ PostgreSQL 12     │ ✅       │ 完全兼容           │
#   │ PostgreSQL 11     │ ⚠️       │ \if 语法可用       │
#   │ ≤ PG 10          │ ❌       │ 不支持 \if 语法    │
#   └──────────────────┴──────────┴──────────────────┘
#
# 预期输出示例 (Hash INSERT, 4 连接, 单机最小拓扑):
#   transaction type: benchmark/bench_hash_insert.sql
#   number of clients: 4
#   duration: 60 s
#   tps = 380.123456 (without initial connection time)
#   latency average = 10.523 ms
#   若偏差 > 50%，请检查磁盘 I/O 和网络延迟
#
# 使用方式:
#   chmod +x 04_pgbench_scripts.sh
#   ./04_pgbench_scripts.sh <CN_HOST> <CN_PORT> <DB_USER>
#
# ============================================================

set -euo pipefail

# 前置检查: pgbench 是否可用
if ! command -v pgbench &>/dev/null; then
    echo "❌ 错误: pgbench 未找到。请确认 pgbench 已安装并在 PATH 中。"
    echo "   OpenTenBase 自带 pgbench，路径通常为: ./pgbench"
    echo "   或手动安装: sudo apt install postgresql-contrib (Ubuntu/Debian)"
    exit 1
fi

# 版本检查: pgbench 11+ 支持自定义脚本 \if 语法
PGBENCH_VERSION=$(pgbench --version 2>&1 | head -1)
echo "✅ pgbench 版本: ${PGBENCH_VERSION}"

CN_HOST="${1:-127.0.0.1}"
CN_PORT="${2:-11000}"
DB_USER="${3:-opentenbase}"
DB_NAME="postgres"
RESULTS_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"

mkdir -p "${RESULTS_DIR}"

echo "🚀 OpenTenBase pgbench 并发压力测试"
echo "   CN Host: ${CN_HOST}"
echo "   CN Port: ${CN_PORT}"
echo "   DB User: ${DB_USER}"
echo "   Results: ${RESULTS_DIR}/"
echo ""

PGCMD="psql -h ${CN_HOST} -p ${CN_PORT} -U ${DB_USER} -d ${DB_NAME}"
PGBENCH="pgbench -h ${CN_HOST} -p ${CN_PORT} -U ${DB_USER} -d ${DB_NAME}"

# 错误日志函数 — 集中输出到 RESULTS_DIR/errors.log
log_error() {
    local msg="$1"
    local ts
    ts=$(date '+%Y-%m-%d %H:%M:%S')
    echo "❌ [${ts}] ${msg}" | tee -a "${RESULTS_DIR}/errors.log"
}

# ============================================================
# 一、单表写入基准 (pgbench custom script)
# ============================================================

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📌 1. 单表写入基准 (Hash 分布 INSERT)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 写入测试脚本 — Hash 分布订单表
cat > "${RESULTS_DIR}/bench_write_hash.sql" << 'SCRIPT'
\set seq random(1, 10000000)
\set cust random(1, 10000)
\set prod random(1, 1000)
\set qty random(1, 10)
\set pric random(1, 99999) / 100
INSERT INTO bench_hash_orders (order_id, customer_id, product_id, quantity, price, order_date, status, region)
VALUES (:seq, :cust, :prod, :qty, :pric, DATE('2025-01-01') + (random() * 365)::INT, 'pending', 'Beijing');
SCRIPT

# 写入测试脚本 — Shard 分布交易表
cat > "${RESULTS_DIR}/bench_write_shard.sql" << 'SCRIPT'
\set txn random(1, 10000000)
\set acct random(1, 999999)
\set amt random(-50000, 50000) / 100
\set typ random(1, 3)
INSERT INTO bench_shard_transactions (txn_id, account_id, amount, txn_type, created_at)
VALUES (:txn, :acct, :amt,
    CASE :typ WHEN 1 THEN 'deposit' WHEN 2 THEN 'withdrawal' ELSE 'transfer' END,
    NOW());
SCRIPT

# 写入测试脚本 — 日志表 (高吞吐)
cat > "${RESULTS_DIR}/bench_write_logs.sql" << 'SCRIPT'
\set svc random(1, 5)
\set lvl random(1, 4)
INSERT INTO bench_hash_logs (service, level, message, host)
VALUES (
    CASE :svc WHEN 1 THEN 'order-service' WHEN 2 THEN 'payment-service' WHEN 3 THEN 'auth-service' WHEN 4 THEN 'inventory-service' ELSE 'gateway' END,
    CASE :lvl WHEN 1 THEN 'INFO' WHEN 2 THEN 'WARN' WHEN 3 THEN 'ERROR' ELSE 'DEBUG' END,
    'Log entry at ' || NOW(),
    'host-' || random(1, 10)
);
SCRIPT

# 运行写入基准 — 逐表、逐并发级别
for TABLE_SCRIPT in bench_write_hash bench_write_shard bench_write_logs; do
    for CONN in 1 4 8 16 32; do
        echo ""
        echo "▶ ${TABLE_SCRIPT} | 连接数: ${CONN} | 60秒测试"
        if ! ${PGBENCH} -c ${CONN} -j ${CONN} -T 60 -f "${RESULTS_DIR}/${TABLE_SCRIPT}.sql" \
            > "${RESULTS_DIR}/${TABLE_SCRIPT}_c${CONN}.log" 2>&1; then
            log_error "pgbench 写入失败: ${TABLE_SCRIPT} c${CONN} — 详见 ${TABLE_SCRIPT}_c${CONN}.log (跳过此项，继续后续测试)"
            continue
        fi
        # 输出摘要
        grep -E "tps|latency|scaling" "${RESULTS_DIR}/${TABLE_SCRIPT}_c${CONN}.log" || echo "(结果待收集)"
    done
done

# ============================================================
# 二、只读查询基准 (pgbench custom script)
# ============================================================

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📌 2. 只读查询基准 (SELECT)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 简单查询 — 主键命中
cat > "${RESULTS_DIR}/bench_read_pk.sql" << 'SCRIPT'
\set oid random(1, 100000)
SELECT * FROM bench_hash_orders WHERE order_id = :oid;
SCRIPT

# 简单查询 — 非分布键 (全 DN 广播)
cat > "${RESULTS_DIR}/bench_read_non_dist.sql" << 'SCRIPT'
\set cid random(1, 10000)
SELECT * FROM bench_hash_orders WHERE customer_id = :cid LIMIT 10;
SCRIPT

# 聚合查询
cat > "${RESULTS_DIR}/bench_read_agg.sql" << 'SCRIPT'
SELECT status, COUNT(*), AVG(price) FROM bench_hash_orders GROUP BY status;
SCRIPT

# Join 查询 (Hash × Replication)
cat > "${RESULTS_DIR}/bench_read_join.sql" << 'SCRIPT'
\set cid random(1, 10000)
SELECT o.order_id, o.price, c.name, c.tier
FROM bench_hash_orders o
JOIN bench_rep_customers c ON o.customer_id = c.customer_id
WHERE o.customer_id = :cid LIMIT 5;
SCRIPT

# 混合查询 (70% 读 + 30% 写)
cat > "${RESULTS_DIR}/bench_mixed.sql" << 'SCRIPT'
\set op random(1, 10)
\set oid random(1, 100000)
\set cid random(1, 10000)
\set prod random(1, 1000)

-- 70% 读: op <= 7
\if :op <= 7
  SELECT * FROM bench_hash_orders WHERE order_id = :oid;
\elif :op <= 9
  -- 20% 写
  INSERT INTO bench_hash_orders (order_id, customer_id, product_id, quantity, price, order_date, status)
  VALUES (:oid + 10000000, :cid, :prod, 1, 99.99, NOW()::DATE, 'pending');
\else
  -- 10% 更新
  UPDATE bench_hash_orders SET status = 'shipped' WHERE order_id = :oid;
\endif
SCRIPT

# 运行只读基准
for READ_SCRIPT in bench_read_pk bench_read_non_dist bench_read_agg bench_read_join bench_mixed; do
    for CONN in 1 4 8 16 32; do
        echo ""
        echo "▶ ${READ_SCRIPT} | 连接数: ${CONN} | 60秒测试"
        if [[ "${READ_SCRIPT}" == "bench_mixed" ]]; then
            if ! ${PGBENCH} -c ${CONN} -j ${CONN} -T 60 -f "${RESULTS_DIR}/${READ_SCRIPT}.sql" \
                > "${RESULTS_DIR}/${READ_SCRIPT}_c${CONN}.log" 2>&1; then
                log_error "pgbench 混合失败: ${READ_SCRIPT} c${CONN} — 详见 ${READ_SCRIPT}_c${CONN}.log (跳过此项，继续后续测试)"
                continue
            fi
        else
            if ! ${PGBENCH} -c ${CONN} -j ${CONN} -T 60 -f "${RESULTS_DIR}/${READ_SCRIPT}.sql" -S \
                > "${RESULTS_DIR}/${READ_SCRIPT}_c${CONN}.log" 2>&1; then
                log_error "pgbench 只读失败: ${READ_SCRIPT} c${CONN} — 详见 ${READ_SCRIPT}_c${CONN}.log (跳过此项，继续后续测试)"
                continue
            fi
        fi
        grep -E "tps|latency|scaling" "${RESULTS_DIR}/${READ_SCRIPT}_c${CONN}.log" || echo "(结果待收集)"
    done
done

# ============================================================
# 三、默认 pgbench TPC-B 基准 (参考基线)
# ============================================================

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📌 3. 默认 pgbench TPC-B 基准 (基线参考)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 初始化 pgbench 默认表
echo "▶ 初始化 pgbench 默认表..."
${PGCMD} -c "DROP TABLE IF EXISTS pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers;"
if ! ${PGBENCH} -i -s 10 > "${RESULTS_DIR}/pgbench_init.log" 2>&1; then
    log_error "pgbench 初始化失败 — 详见 ${RESULTS_DIR}/pgbench_init.log (跳过 TPC-B 基线测试，继续后续)"
    PGBENCH_TPCB_SKIP=1
fi

if [[ "${PGBENCH_TPCB_SKIP:-0}" != "1" ]]; then
    for CONN in 1 4 8 16 32; do
        echo ""
        echo "▶ pgbench TPC-B | 连接数: ${CONN} | 60秒测试"
        if ! ${PGBENCH} -c ${CONN} -j ${CONN} -T 60 \
            > "${RESULTS_DIR}/pgbench_default_c${CONN}.log" 2>&1; then
            log_error "pgbench TPC-B 失败: c${CONN} — 详见 ${RESULTS_DIR}/pgbench_default_c${CONN}.log (跳过此项，继续后续测试)"
            continue
        fi
        grep -E "tps|latency|scaling" "${RESULTS_DIR}/pgbench_default_c${CONN}.log" || echo "(结果待收集)"
    done
fi

# ============================================================
# 四、收集结果汇总
# ============================================================

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📌 4. 测试完成，结果汇总"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo "所有结果已保存到: ${RESULTS_DIR}/"
echo ""
echo "请将此目录下的 .log 文件整理到 benchmark/README.md 的结果表格中。"
echo ""

# 生成结果摘要模板
cat > "${RESULTS_DIR}/results_summary_template.md" << 'TEMPLATE'
# OpenTenBase 基准性能测试结果摘要

## 测试环境

| 项目 | 配置 |
|------|------|
| 操作系统 | (填写) |
| CPU | (填写) |
| 内存 | (填写) |
| 磁盘 | (填写) |
| OpenTenBase 版本 | (填写) |
| 集群拓扑 | GTM: x, CN: x, DN: x |
| 数据总量 | Hash Orders: 100K, Replication Customers: 10K, Shard Txn: 100K, Logs: 500K |

## 写入基准 (TPS)

| 场景 | 1 连接 | 4 连接 | 8 连接 | 16 连接 | 32 连接 |
|------|--------|--------|--------|---------|---------|
| Hash INSERT (bench_hash_orders) | | | | | |
| Shard INSERT (bench_shard_transactions) | | | | | |
| Logs INSERT (bench_hash_logs) | | | | | |

## 只读基准 (QPS)

| 场景 | 1 连接 | 4 连接 | 8 连接 | 16 连接 | 32 连接 |
|------|--------|--------|--------|---------|---------|
| 主键查询 (Hash PK) | | | | | |
| 非分布键查询 (全 DN 广播) | | | | | |
| 聚合查询 (GROUP BY) | | | | | |
| Join 查询 (Hash × Replication) | | | | | |
| 混合 (70R/20W/10U) | | | | | |

## pgbench TPC-B 基线

| 连接数 | TPS | 平均延迟 |
|--------|-----|----------|
| 1 | | |
| 4 | | |
| 8 | | |
| 16 | | |
| 32 | | |

## 瓶颈分析

(在此填写对 CN/DN/网络/磁盘瓶颈的分析)

TEMPLATE

echo "✅ 已生成结果模板: ${RESULTS_DIR}/results_summary_template.md"
