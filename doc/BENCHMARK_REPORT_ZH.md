# OpenTenBase 基准性能测试报告

> 日期：2026-07-12 | 分支：benchmark-analysis | 实例模式：集中式（单 DN）

## 一、测试环境

### 硬件配置

| 项目 | 规格 |
|------|------|
| CPU | Intel Core i7-13700（24 vCPU），WSL2 虚拟化 |
| 内存 | 31 GB DDR5 |
| 磁盘 | NVMe SSD，251 GB，228 GB 可用 |
| 操作系统 | Ubuntu 22.04.5 LTS（WSL2） |
| 文件系统 | ext4（WSL2 虚拟磁盘） |

### OpenTenBase 拓扑

| 角色 | 数量 | IP:Port | 模式 |
|------|------|---------|------|
| DN（主） | 1 | 127.0.0.1:11000 | 集中式（无 GTM、无 CN） |

> **说明**：本次测试使用集中式实例（`type=centralized`），仅含 1 个 DN 节点。集中式模式下无 CN 层和 GTM 层，所有查询直接在 DN 上执行。这相当于 OpenTenBase 的"单机模式"基准线。分布式模式的性能特征将在后续报告中补充。

### 软件版本

- OpenTenBase v5.0（commit: ac54d240f）
- PostgreSQL 10.0 兼容
- 预编译包（GCC 4.8.5, CentOS 7）
- pgbench 10.0 @ OpenTenBase_v5.0

---

## 二、测试方案

### 测试工具：pgbench + 自定义 SQL

| 工具 | 用途 | 版本 |
|------|------|------|
| `pgbench` | TPC-B 类负载（SELECT / UPDATE / INSERT 混合） | OpenTenBase 内置 |
| 自定义 SQL | 聚合查询、JOIN 查询、写入吞吐 | — |

### 测试数据集

```sql
-- pgbench 初始化（scale=100）
pgbench -i -s 100  →  10,000,000 行 pgbench_accounts
                        100 行 pgbench_branches
                        1,000 行 pgbench_tellers
                        0 行 pgbench_history（随测试增长）
```

### 测试场景

| # | 场景 | 工具 | 并发 | 时长 | 说明 |
|---|------|------|------|------|------|
| 1 | **点查询（SELECT by PK）** | pgbench -S | 1/10/50/100 | 15s | 纯读，命中主键索引 |
| 2 | **混合读写（TPC-B）** | pgbench 默认 | 1/10/50 | 15s | 每事务含 UPDATE+SELECT+INSERT |
| 3 | **聚合查询（GROUP BY + SUM/AVG）** | 自定义 SQL | 1 | — | 10M 行跨 100 组聚合 |
| 4 | **JOIN 查询** | 自定义 SQL | 1 | — | 3 表 JOIN |
| 5 | **复杂 JOIN + 聚合** | 自定义 SQL | 1 | — | JOIN + GROUP BY + SUM/COUNT |

---

## 三、测试结果

### 3.1 点查询吞吐（pgbench -S）

10M 行 pgbench_accounts 表，主键索引命中。

| 并发连接 | 事务数（15s） | TPS | 平均延迟 | 相对 1-client 扩展比 |
|----------|-------------|-----|---------|-------------------|
| **1** | 53,304 | **3,554** | 0.281 ms | 1.00× |
| **10** | 521,673 | **34,790** | 0.288 ms | 9.79× |
| **50** | 403,151 | **26,865** | 1.862 ms | 7.56× |
| **100** | 315,495 | **21,018** | 4.759 ms | 5.91× |

**分析**：

- 10 并发时接近线性扩展（9.79×），延迟几乎不变（0.288ms），说明 CPU 核心充足
- 50 并发时 TPS 开始下降，延迟升至 1.862ms，瓶颈从 CPU 转向**锁竞争**（pgbench 各连接竞争同一组 branch 行）
- 100 并发时 TPS 进一步下降至 21K，延迟升至 4.76ms

**瓶颈判断**：
1. **CPU**：10 并发以下无瓶颈，24 核充足
2. **锁竞争**：50+ 并发时 pgbench_accounts 的 `abalance` 更新竞争显著
3. **磁盘 I/O**：SSD 下 WAL 写入不是瓶颈（点查询不涉及写）

### 3.2 混合读写吞吐（pgbench 默认 TPC-B）

每事务包含：1×UPDATE pgbench_accounts + 1×SELECT pgbench_tellers + 1×UPDATE pgbench_branches + 1×INSERT pgbench_history。

| 并发连接 | 事务数（15s） | TPS | 平均延迟 |
|----------|-------------|-----|---------|
| **1** | 4,034 | **269** | 3.719 ms |
| **10** | 34,928 | **2,329** | 4.295 ms |
| **50** | 58,410 | **3,881** | 12.886 ms |

**分析**：

- 混合读写 TPS 远低于纯读（269 vs 3,554），每事务约 13× 慢于纯读——符合预期（TPC-B 每事务 3-4 条 SQL）
- 10→50 并发扩展比仅 1.67×，延迟从 4.3ms 升至 12.9ms
- 主要瓶颈：**WAL 写入**（每次 UPDATE/INSERT 需 fsync）+ **行锁等待**（branch 行是热点）

**瓶颈判断**：
1. **WAL I/O**：写密集型负载下，WAL 同步写入是最大瓶颈
2. **行锁**：`pgbench_branches.bbalance` 更新是全局热点（只有 100 行）
3. **磁盘**：SSD 延迟低，但 fsync 频率高时成为瓶颈

### 3.3 聚合查询（自定义 SQL）

10M 行 `pgbench_accounts`，按 `bid`（100 组）GROUP BY + SUM/AVG/COUNT/MIN/MAX。

| 查询类型 | 数据量 | 耗时 | 说明 |
|---------|--------|------|------|
| GROUP BY + 5 聚合函数 | 10M 行 → 100 组 | **~3.0s** | 全表扫描 |
| 3 表 JOIN（LIMIT 100） | 100 行 | **~0.013s** | 索引查找 |
| JOIN + GROUP BY | 10M 行 → 100 组 | **~6.1s** | 全表扫描 + JOIN |

**分析**：

- GROUP BY 全表扫描 10M 行耗时 3.0s，约 **3.3M 行/秒** 的扫描速率
- JOIN+聚合 6.1s（约 1.6M 行/秒），JOIN 增加了 Hash Join 开销
- 点 JOIN（100 行）几乎瞬时（13ms），索引效率高

**瓶颈判断**：
1. **全表扫描**（Seq Scan）：3-6s 的延迟对 OLAP 场景可接受
2. **shared_buffers**：如果数据能缓存到内存（10M 行 × ~300B ≈ 3GB），重复查询会更快
3. **单节点限制**：分布式模式下 CN 可将聚合拆分为各 DN 局部聚合 + CN 全局聚合，显著加速

### 3.4 集中式 vs 分布式性能预测

| 维度 | 集中式（本次实测） | 分布式（预测） |
|------|-----------------|--------------|
| **点查询 TPS** | ~35K（10c） | CN 路由开销 +5-10% 延迟，但 CN 可水平扩展 |
| **聚合查询** | ~3s（10M 行单节点） | CN 拆分查询 → N 个 DN 并行 → 理论上可 N× 加速 |
| **写入 TPS** | ~2.3K（10c） | GTM 全局事务 ID 分配增加 ~1-5ms；跨 DN 的事务需 2PC |
| **并发扩展** | 50c 以上锁竞争明显 | CN 层可分担连接，DN 间无锁竞争 |

---

## 四、瓶颈分析总结

```
                         写入瓶颈                    读瓶颈
                           │                          │
                    ┌──────┴──────┐            ┌──────┴──────┐
                    │   WAL fsync  │            │  缓存命中率  │
                    │  (每事务必等)  │            │  (shared_buf) │
                    └──────┬──────┘            └──────┬──────┘
                           │                          │
                    ┌──────┴──────┐            ┌──────┴──────┐
                    │  行锁竞争    │            │  CPU 并行度  │
                    │  (热点行)    │            │  (Seq Scan)  │
                    └──────┬──────┘            └──────┬──────┘
                           │                          │
                    ┌──────┴──────────────────────────┴──────┐
                    │          单节点限制（集中式模式）         │
                    │  无 CN 分发 → 无并行扫描                 │
                    │  无多 DN → 数据不跨节点 → 无网络开销      │
                    └─────────────────────────────────────────┘
```

### 已知局限

1. **集中式模式无 CN/GTM**：无法测量 CN 查询分发、GTM 事务协调的开销
2. **单客户端单连接**：自定义 SQL 未做并发压测
3. **数据未分片**：所有数据在单个 DN 上，无法测试跨节点 JOIN 和数据重分布
4. **WSL2 虚拟化**：磁盘 I/O 性能可能低于裸 Linux（WSL2 的 9p/ext4 虚拟磁盘）

---

## 五、改进建议

### 对 OpenTenBase 部署

| 建议 | 优先级 | 说明 |
|------|--------|------|
| 使用分布式模式进行性能测试 | 高 | 需要至少 2 台物理机或 3+ K8s Pod 来搭建 GTM+CN+DN 完整拓扑 |
| 调整 `shared_buffers` | 中 | 当前使用默认值，建议设为物理内存的 25%（~8GB） |
| 调整 `wal_sync_method` | 中 | `open_datasync` 或 `fdatasync` 在 SSD 上可能优于默认 `fsync` |
| 配置 PgBouncer 连接池 | 中 | 100+ 并发时连接池可减少后端进程创建开销 |

### 对后续基准测试

| 建议 | 说明 |
|------|------|
| 分布式 4-DN 拓扑测试 | 对比单 DN vs 4 DN 的聚合查询加速比 |
| 数据倾斜场景 | 模拟真实业务中某些 branch 的 account 远超平均水平 |
| CN 瓶颈测试 | 在分布式模式下单独压测 CN 的查询解析和分发能力 |
| 长时间稳定性测试 | 8h+ TPC-B 持续压测，观察 WAL 堆积、autovacuum 行为 |

---

## 六、测试脚本

所有脚本位于仓库 `bench/` 目录，可直接复现本次测试。

### 脚本目录结构

```
bench/
├── run_benchmarks.sh              # 一键运行脚本
├── sql/
│   ├── setup.sql                  # 建表（4 张表 + 14 个索引）
│   ├── data_load.sql              # 数据加载（10 万账户 + 100 万交易 + 1 万商品）
│   ├── bench_point_select.sql     # 点查询（主键索引命中）
│   ├── bench_single_insert.sql    # 单行 INSERT
│   ├── bench_batch_insert.sql     # 批量 INSERT
│   ├── bench_aggregation.sql      # GROUP BY + SUM/AVG/COUNT/MIN/MAX
│   ├── bench_aggregation_txn.sql  # 事务内聚合
│   ├── bench_join.sql             # 3 表 JOIN
│   ├── bench_mixed.sql            # 混合负载（40% 读 + 20% 点查 + 15% 写 + 10% 聚合 + 10% 更新 + 5% JOIN）
└── results/                       # 测试结果输出目录
```

### 快速复现

```bash
# 1. 建表
psql -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -f bench/sql/setup.sql

# 2. 加载数据
psql -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -f bench/sql/data_load.sql

# 3. 运行全部基准测试
cd bench && ./run_benchmarks.sh all

# 或单独运行某个测试
./run_benchmarks.sh single bench_point_select
```

### 本次测试实际使用命令

由于本次在集中式单 DN 实例上测试，以下为实际执行的命令和参数：

```bash
# pgbench 初始化（基于内置 TPC-B 表）
pgbench -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -i -s 100

# 点查询测试（4 级并发）
pgbench -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -S -c 1  -T 15
pgbench -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -S -c 10 -T 15
pgbench -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -S -c 50 -T 15
pgbench -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -S -c 100 -T 15

# 混合读写测试（3 级并发）
pgbench -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -c 1  -T 15
pgbench -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -c 10 -T 15
pgbench -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -c 50 -T 15

# 自定义聚合 / JOIN（单次执行，time 计时）
psql -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -c "
SELECT bid, COUNT(*), SUM(abalance), AVG(abalance)::bigint,
       MIN(abalance), MAX(abalance)
FROM pgbench_accounts GROUP BY bid ORDER BY bid;
"

psql -h 127.0.0.1 -p 11000 -U opentenbase -d bench_test -c "
SELECT b.bid, b.bbalance, COUNT(a.aid), SUM(a.abalance)
FROM pgbench_branches b
LEFT JOIN pgbench_accounts a ON b.bid = a.bid
GROUP BY b.bid, b.bbalance ORDER BY b.bid;
"
```

---

## 七、GitHub Discussion 讨论要点

1. **分布式性能基准需求**：社区是否有公开的 OpenTenBase 分布式性能数据（如 TPC-C/TPC-H）？
2. **GTM 开销量化**：在分布式模式下，GTM 的全局事务 ID 分配对 TPS 的影响是多少毫秒？
3. **CN 路由开销**：CN 的查询解析和 DN 分发延迟在什么数量级？
4. **数据分片策略对性能的影响**：HASH 分片 vs 复制表 vs 随机分片，各自对 JOIN 和聚合的影响？

---

> 本报告由 Claude Code（DeepSeek-V4-Pro）协助设计测试方案、生成脚本、执行测试和分析结果。所有测试数据均为 WSL2 环境实测值。AI 使用详情见 [AI 使用策略报告](AI_USAGE_REPORT_BENCHMARK_ZH.md)。
