# [Discussion] OpenTenBase 基准性能测试方案与结果

> 建议发帖位置：https://github.com/OpenTenBase/OpenTenBase/discussions
> 分类：Show and tell / Ideas
> 标签：benchmark, performance, testing

---

## 概述

我在 WSL2 Ubuntu 22.04 上部署了 OpenTenBase v5.0 集中式实例（1 DN，scale=100），使用 pgbench 和自定义 SQL 完成了一轮基准性能测试。以下是测试方案、结果和几点观察。

## 测试环境

- CPU: 24 vCPU (Intel i7-13700), 31GB RAM, NVMe SSD
- OpenTenBase v5.0 (commit: ac54d240f), centralized mode, 1 DN @ 127.0.0.1:11000
- pgbench scale=100 (10M rows in pgbench_accounts)

## 核心结果

### pgbench SELECT-only（纯读）

| 并发 | TPS | 平均延迟 |
|------|-----|---------|
| 1    | 3,554 | 0.28 ms |
| 10   | 34,790 | 0.29 ms |
| 50   | 26,865 | 1.86 ms |
| 100  | 21,018 | 4.76 ms |

### pgbench TPC-B-like（混合读写）

| 并发 | TPS | 平均延迟 |
|------|-----|---------|
| 1    | 269 | 3.72 ms |
| 10   | 2,329 | 4.30 ms |
| 50   | 3,881 | 12.89 ms |

### 自定义 SQL（单次执行）

- GROUP BY 聚合 10M 行（100 组）：~3.0s
- JOIN + 聚合 10M 行：~6.1s

## 几点观察

1. **纯读扩展性良好**：1→10 并发接近线性（9.8×），50+ 并发时锁竞争开始显现
2. **写入瓶颈在 WAL**：TPC-B 类负载 TPS 远低于纯读，WAL fsync 和行锁是主要瓶颈
3. **集中式 = 单机 PG**：本次测试缺少 CN/GTM，无法体现 OpenTenBase 的分布式特征（CN 分发、DN 并行、GTM 事务协调）

## 待讨论

1. **社区是否有分布式模式的公开性能数据？** 如果有 TPC-C/TPC-H 级别的测试结果，可以建立性能基准线
2. **GTM 对 TPS 的影响有多大？** 从集中式→分布式，GTM 的事务 ID 分配会增加多少延迟？
3. **CN 的查询分发延迟在什么数量级？** CN 解析 SQL + 路由到 DN 的额外开销是多少毫秒？
4. **多 DN 的聚合加速比是多少？** 4 个 DN 并行扫描是否接近 4× 加速？

## 完整报告

详见仓库 `doc/BENCHMARK_REPORT_ZH.md`（分支 `benchmark-analysis`），包含测试脚本、完整结果、瓶颈分析和改进建议。

欢迎反馈、质疑或补充数据！🙏
