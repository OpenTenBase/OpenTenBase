# OpenTenBase 基准测试过程

这个目录用于承接 Issue #202 的基础性能测试交付物。目标不是提供一组零散 SQL，而是提供一套可以复用、可以解释、可以扩展的最小基准测试骨架。

## 为什么补成这 7 个文件

只放一个大 SQL 文件不够，原因有三点：

1. 建表、造数、压测脚本、执行控制、结果模板属于不同职责，混在一起后很难复跑，也很难定位问题。
2. 我发现OpenTenBase 的关键观察点不只是 TPS，还包括 CN 分发、DN 分布、GTM 短事务压力和数据倾斜，因此需要专门的 `workload.sql` 和结果模板。
3. 为了控制复杂度，文件数没有太多；用 7 个文件可以把职责拆清楚，又不会演变成难维护的大目录。

## 文件作用

| 文件 | 作用 |
| --- | --- |
| `schema.sql` | 创建测试表、序列和索引，定义 `SHARD` / `REPLICATION` 分布方式。 |
| `load_data.sql` | 生成固定规模的测试数据，并执行 `ANALYZE`，保证结果可复现。 |
| `workload.sql` | 统一存放 pgbench workload 片段、`EXPLAIN`、分布分析、清理和扩展占位 section。 |
| `benchmark_runner.sh` | 统一执行 `setup/load/warmup/run/analyze/cleanup/all`，避免手工一条条敲命令。 |
| `results_template.md` | 当前已填写完成的结果报告，记录环境、指标、计划、倾斜、瓶颈和扩展状态。 |
| `ai_report.md` | 独立的 AI 使用策略与人工验证报告。 |
| `discussion_post.md` | 可直接发布到 GitHub Discussions 的性能测试报告草稿。 |
| `README.md` | 说明设计原则、运行顺序、依赖条件和输出目录。 |

## 兼容性约束

- 当前默认使用：
  - `DISTRIBUTE BY SHARD(...) TO GROUP default_group`
  - `DISTRIBUTE BY REPLICATION TO GROUP default_group`
- 当前方案不假设环境已经启用 `--enable-alltype-distri`，因此没有把 `HASH` / `MODULO` 放进建表脚本里。
- `default_group` 是运行时对象，不在仓库中创建。执行 `schema.sql` 前，需要先在 benchmark 数据库中准备好它。

## 对象设计

- `perf_user`：按 `user_id` 分布的用户表，用于同分布 Join。
- `perf_order`：按 `user_id` 分布的订单表，是主要读写压测对象。
- `perf_city`：复制维表，用于验证复制表 Join。
- `perf_event`：按 `user_id` 分布的事件表，用于高吞吐写入和 GTM 短事务测试。

## 运行前提

执行前需要保证：

1. 所有正式连接都走 CN。
2. `psql` 和 `pgbench` 已经在 `PATH` 中。
3. 目标数据库可连接，并且已存在 `default_group`。
4. 环境变量至少设置以下内容：

```bash
export CN_HOST=172.17.0.2
export CN_PORT=11003
export DB_USER=opentenbase
export DB_NAME=benchmark
```

如果需要口令认证，再额外设置：

```bash
export PGPASSWORD='your-password'
```

## 推荐执行顺序

```bash
cd benchmark

bash benchmark_runner.sh setup \
  --host "${CN_HOST}" \
  --port "${CN_PORT}" \
  --user "${DB_USER}" \
  --database "${DB_NAME}"

bash benchmark_runner.sh load \
  --host "${CN_HOST}" \
  --port "${CN_PORT}" \
  --user "${DB_USER}" \
  --database "${DB_NAME}"

bash benchmark_runner.sh warmup \
  --host "${CN_HOST}" \
  --port "${CN_PORT}" \
  --user "${DB_USER}" \
  --database "${DB_NAME}"

bash benchmark_runner.sh run \
  --host "${CN_HOST}" \
  --port "${CN_PORT}" \
  --user "${DB_USER}" \
  --database "${DB_NAME}" \
  --clients "1,4,8" \
  --duration 30

bash benchmark_runner.sh analyze \
  --host "${CN_HOST}" \
  --port "${CN_PORT}" \
  --user "${DB_USER}" \
  --database "${DB_NAME}"
```

如果需要执行更完整的矩阵，也可以一次性执行：

```bash
bash benchmark_runner.sh all \
  --host "${CN_HOST}" \
  --port "${CN_PORT}" \
  --user "${DB_USER}" \
  --database "${DB_NAME}" \
  --clients "1,4,8,16,32,64" \
  --duration 60
```

## 运行器输出

每次运行默认会创建一个独立结果目录：

```text
benchmark_results_YYYYMMDD_HHMMSS/
├── run_context.txt
├── run_summary.tsv
└── raw/
```

- `run_context.txt`：记录当前运行使用的参数。
- `run_summary.tsv`：提取 pgbench 输出中的关键指标。
- `raw/`：保存原始 pgbench 日志、事务日志、`EXPLAIN` 和分布分析输出。

## 当前已完成结果

- 当前仓库内已填写的正式样本结果目录为 `benchmark_results_formal_pctl_20260721_092139`。
- 该样本使用 `1,4,8 clients × 30s` 的正式矩阵，主要用于证明可复现、可解释且无错误。
- 如果需要补更完整的结果，可继续执行 `1,4,8,16,32,64 clients × 60s` 的完整矩阵(已执行过确认无误)。
