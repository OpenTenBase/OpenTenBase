# OpenTenBase 基础性能测试

这个目录提供一套 5,000,000 行的 OpenTenBase 基准测试，覆盖单表写入、简单查询、聚合查询、JOIN 查询和并发连接。

实测报告见 [`REPORT.md`](REPORT.md)，汇总数据见 [`results`](results)，正式测量前遇到的节点连接端口、表分布语法和 pgbench 标准表问题见 [`Pitfalls.md`](Pitfalls.md)。

## 目录用途

```text
benchmark/
├── sql/          建表、加载数据、更新统计信息、检查分布和执行计划
├── workloads/    交给 pgbench 反复执行的测试脚本
├── results/      环境、汇总和原始测试输出
├── REPORT.md     测试方法、结果和简明分析
├── Pitfalls.md   测试问题与排障记录
└── AI_USAGE.md   AI 使用策略报告
```

`sql/` 中的文件通过 `psql -f` 执行；`workloads/` 中的文件通过 `pgbench -f` 执行。文中“测试脚本”对应 pgbench 的 workload（测试脚本）；“事务”表示一次完整的脚本执行。CN 是接收请求并安排执行的节点，DN 是实际保存和处理数据的节点，GTM 负责分布式事务协调。

## 测试环境变量

```bash
export PG_HOME=.../opentenbase/install/opentenbase_bin_v5.0
export PATH="$PG_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$PG_HOME/lib"
export PGHOST=127.0.0.1
export PGPORT=11003
export PGUSER=opentenbase
export PGDATABASE=database
```

## 初始化顺序

下面两步会删除并重建 `bench_*` 测试表，只能对专用的 `database`数据库执行：

```bash
psql -f benchmark/sql/00_create_schema.sql
psql -f benchmark/sql/01_load_data.sql
```

先创建辅助索引、更新统计信息并检查分布和执行计划，随后执行：

```bash
psql -f benchmark/sql/02_create_indexes.sql
psql -f benchmark/sql/03_analyze.sql
psql -f benchmark/sql/04_verify_distribution.sql
psql -f benchmark/sql/05_explain.sql
```

初始数据量为：

| 表 | 行数 |
| --- | ---: |
| `bench_users` | 500,000 |
| `bench_orders` | 3,000,000 |
| `bench_categories` | 20 |
| `bench_payments` | 1,499,980 |

总计 5,000,000 行。

## pgbench 命令含义

```bash
pgbench -n  -f benchmark/workloads/point_select.sql -c 4 -j 4 -T 20 -P 5
```

| 参数 | 含义 |
| --- | --- |
| `-n` | 不尝试维护 pgbench 自带的标准表 |
| `-f` | 反复执行指定测试脚本 |
| `-c 4` | 模拟 4 个数据库客户端 |
| `-j 4` | 使用 4 个 pgbench 工作线程 |
| `-T 20` | 持续运行 20 秒 |
| `-P 5` | 每 5 秒显示一次进度 |

本测试中一个测试脚本循环算一个事务。点查询脚本每个循环只有一条`SELECT`，所以它的 TPS 也近似表示每秒查询数。

正式单客户端测试命令如下；只读测试脚本各运行 30 秒，写入测试脚本运行 100 个事务：

```bash
pgbench -n -f benchmark/workloads/point_select.sql  -c 1 -j 1 -T 30 database
pgbench -n -f benchmark/workloads/scatter_select.sql -c 1 -j 1 -T 30 database
pgbench -n -f benchmark/workloads/aggregate.sql     -c 1 -j 1 -T 30 database
pgbench -n -f benchmark/workloads/join.sql          -c 1 -j 1 -T 30 database
pgbench -n -f benchmark/workloads/insert.sql        -c 1 -j 1 -t 100 database
```

并发趋势测试使用 `point_select.sql`，将 `-c` 设置为 1、4、8、16、32；报告中的并发矩阵是每个客户端数单独运行 10 秒的补充采样。

`join_non_colocated.sql` 是额外的非共址 JOIN 测试脚本：它连接按 `user_id` 分布的`bench_orders` 和按 `payment_id` 分布的 `bench_payments`，不属于上面的五类正式汇总，可用以下命令单独运行：

```bash
pgbench -n -f benchmark/workloads/join_non_colocated.sql -c 1 -j 1 -T 30 database
```

## 注意事项

- 所有 SQL 都应连接 CN。
- 正式结果使用 `excluding connections establishing` 的 TPS，即不把建立数据库连接的时间算进去。
- 写入测试临时插入 100 行，测后删除这些行并重新 `ANALYZE`。
- 不同测试脚本每次完成的工作不同，不能只按 TPS 高低判断 SQL 好坏。
