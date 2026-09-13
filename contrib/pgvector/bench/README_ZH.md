# pgvector IVFFlat 基准测试

[English Documentation](README.md)

本目录包含一套用于 OpenTenBase PG19 `pgvector` IVFFlat 访问方法的可复现基准测试。

当前 benchmark 覆盖 L2 距离、Inner Product 距离和 Cosine 距离，并记录平均延迟、TPS、事务状态和 Recall@K。

## 目录结构

```text
bench/
├── Makefile
├── README.md
├── README_ZH.md
├── config/
│   ├── smoke.conf
│   ├── quick.conf
│   ├── large.conf
│   └── disk.conf
├── scripts/
│   ├── build_tools.sh
│   ├── capture_environment.sh
│   ├── compare_results.sh
│   ├── prepare_dataset.sh
│   ├── run_benchmark.sh
│   └── run_filtered_benchmark.sh
├── sql/
│   ├── initialize.sql
│   ├── exact_search.sql
│   ├── exact_search_filtered.sql
│   ├── create_index.sql
│   ├── measure_recall.sql
│   ├── measure_filtered_recall.sql
│   ├── workload_l2.sql
│   ├── workload_ip.sql
│   └── workload_cosine.sql
│   ├── workload_filtered_l2.sql
│   ├── workload_filtered_ip.sql
│   └── workload_filtered_cosine.sql
└── src/
    ├── generate_vectors.c
    └── summarize_latency.c
```

当前 benchmark 使用 `generate_vectors`。`summarize_latency.c` 为后续延迟汇总功能预留。

## 环境要求

需要以下组件：

- 安装并运行带有 `vector` 扩展的 OpenTenBase PG19。
- 一个可以连接的 PostgreSQL 兼容 benchmark 数据库。
- Bash、`make`、C 编译器、`psql` 和 `pgbench`。

脚本不会创建或删除数据库。只有在执行 `prepare_dataset.sh --recreate` 时，脚本才会重建 `vector_bench` schema。

所有数据库命令都应连接测试环境中的 OpenTenBase Coordinator 或 PostgreSQL 客户端入口，不要直接连接只读 Datanode。

## 编译工具

请在 benchmark 目录中执行以下命令：

```bash
make generate_vectors
```

生成的程序位于：

```text
build/generate_vectors
```

也可以使用构建脚本：

```bash
./scripts/build_tools.sh
```

## 配置文件

配置文件使用可信的 Bash 变量赋值格式。

| 变量 | 说明 |
| --- | --- |
| `ROWS` | item 向量数量 |
| `VECTOR_TYPE` | 向量列类型：`vector` 或 `halfvec`，默认为 `vector` |
| `DIMENSIONS` | 向量维度 |
| `QUERY_COUNT` | 查询向量数量 |
| `RECALL_QUERY_COUNT` | 用于 Recall@K 的查询数量 |
| `CATEGORY_COUNT` | 确定性过滤分类的数量 |
| `TOP_K` | 每次返回的最近邻数量 |
| `LISTS_VALUES` | IVFFlat `lists` 参数矩阵 |
| `PROBES_VALUES` | IVFFlat `probes` 参数矩阵 |
| `CLIENT_VALUES` | pgbench 客户端数量矩阵 |
| `WARMUP_SECONDS` | 每组配置的预热时间 |
| `DURATION_SECONDS` | 每组配置的正式测量时间 |
| `REPEATS` | 正式测量重复次数 |
| `WORK_MEM` | 单次查询可用的工作内存 |
| `MAINTENANCE_WORK_MEM` | 创建索引时可用的内存 |

当前配置 profile：

- `config/smoke.conf`：10,000 条数据、32 维向量，用于短时探索性测试。
- `config/quick.conf`：100,000 条数据、128 维向量，用于日常回归测试。
- `config/large.conf`：为大规模测试 profile 预留。
- `config/disk.conf`：8,000,000 条数据、128 维向量，用于 controlled-disk 测试。

现有 profile 默认使用 `vector`。可以用环境变量复用同一 profile 测试
`halfvec`，并使用独立结果目录：

```bash
VECTOR_TYPE=halfvec ./scripts/prepare_dataset.sh \
  --config config/quick.conf \
  --output results/quick-halfvec/dataset \
  --recreate
```

为保证 baseline/candidate 使用同一个物理索引，可在只包含一个 lists 值的
配置上使用 `--reuse-index`。该模式会校验现有索引的操作符类和 lists，不会重建索引。

## 准备数据集

准备脚本只会删除并重建 `vector_bench` schema。由于已有 benchmark 数据会被替换，必须显式指定 `--recreate`。

先检查所选配置文件中的连接参数：

```bash
tail -n 5 config/smoke.conf
```

默认本地环境使用：

```text
DB_HOST=127.0.0.1
DB_PORT=6543
DB_USER=linnaid
DB_NAME=pgvector_bench
```

准备 smoke 数据集：

```bash
./scripts/prepare_dataset.sh \
  --config config/smoke.conf \
  --recreate
```

脚本会创建以下表：

- `vector_bench.items`：待检索的 item 向量和确定性的 `category_id` 值。
- `vector_bench.queries`：查询向量。
- `vector_bench.truth`：精确搜索 ground truth。

脚本还会将数据集元信息写入对应的结果目录。

`category_id` 按照 `(id - 1) % CATEGORY_COUNT` 自动生成。默认使用 1,000 个分类时，一个分类约匹配 0.1% 的数据，十个分类约匹配 1% 的数据。

## 生成精确真值

在运行 Recall@K 或 benchmark 参数矩阵前，先生成精确 Top-K 结果：

```bash
psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v recall_query_count=100 \
  -v top_k=10 \
  -f sql/exact_search.sql
```

对于 `quick.conf`，使用配置中的查询数量：

```bash
psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v recall_query_count=1000 \
  -v top_k=10 \
  -f sql/exact_search.sql
```

每种距离类型都应生成 `QUERY_COUNT * TOP_K` 条真值记录。

准备带分类字段的数据集后，生成过滤场景的精确真值：

```bash
psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v recall_query_count=100 \
  -v top_k=10 \
  -v category_count=1000 \
  -f sql/exact_search_filtered.sql
```

该脚本会为 `percent_1` 和 `percent_0_1` 两种过滤条件生成 L2、Inner Product 和 Cosine 的精确结果。

## 过滤 workload

过滤 workload 文件是 pgbench 脚本，必须使用 `pgbench` 执行；`measure_filtered_recall.sql` 必须使用 `psql` 执行。

过滤 workload 支持以下变量：

| 变量 | 说明 |
| --- | --- |
| `query_count` | 可用查询向量数量 |
| `top_k` | 每次返回的最近邻数量 |
| `probes` | 初始 IVFFlat probes 数量 |
| `filter_limit` | `category_id` 的排他上界 |
| `iterative_scan` | `off` 或 `relaxed_order` |
| `max_probes` | iterative scan 允许的最大 probes |

当 `CATEGORY_COUNT=1000` 时，使用以下过滤参数：

| profile | `filter_limit` | 近似选择率 |
| --- | ---: | ---: |
| `percent_1` | `10` | 1% |
| `percent_0_1` | `1` | 0.1% |

运行过滤 workload 前，先创建匹配的 IVFFlat 索引。例如创建 L2 索引：

```bash
psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v metric=l2 \
  -v opclass=vector_l2_ops \
  -v lists=100 \
  -f sql/create_index.sql
```

执行一次过滤 L2 workload：

```bash
pgbench \
  -n \
  -M simple \
  -c 1 \
  -j 1 \
  -t 1 \
  -D query_count=100 \
  -D top_k=10 \
  -D probes=1 \
  -D filter_limit=10 \
  -D iterative_scan=off \
  -D max_probes=100 \
  -f sql/workload_filtered_l2.sql \
  pgvector_bench
```

测量相同索引和过滤条件下的 Recall@10：

```bash
psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v metric=l2 \
  -v filter_name=percent_1 \
  -v filter_limit=10 \
  -v probes=1 \
  -v iterative_scan=off \
  -v max_probes=100 \
  -v recall_query_count=100 \
  -v top_k=10 \
  -f sql/measure_filtered_recall.sql
```

将 `iterative_scan` 改为 `relaxed_order` 后重复测试，可以比较 iterative list 扩展的影响。`max_probes` 应不小于初始 `probes` 值。

## 运行 benchmark

运行 L2 smoke 参数矩阵：

```bash
./scripts/run_benchmark.sh \
  --config config/smoke.conf \
  --metrics l2 \
  --output results/smoke/run_l2
```

运行 IP 和 Cosine：

```bash
./scripts/run_benchmark.sh \
  --config config/smoke.conf \
  --metrics ip,cosine \
  --output results/smoke/run_ip_cosine
```

运行全部三种距离：

```bash
./scripts/run_benchmark.sh \
  --config config/smoke.conf \
  --output results/smoke/run_all
```

运行更大规模的 quick profile：

```bash
./scripts/prepare_dataset.sh \
  --config config/quick.conf \
  --recreate

psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v recall_query_count=1000 \
  -v top_k=10 \
  -f sql/exact_search.sql

./scripts/run_benchmark.sh \
  --config config/quick.conf \
  --output results/quick/run_all
```

benchmark runner 会自动为每种距离创建匹配的 IVFFlat 操作符类，跳过 `probes > lists` 的组合，执行预热、pgbench、Recall@K 测量并写出 CSV。

## 输出文件

每次运行都会创建带时间戳的 CSV 和辅助日志：

```text
results/<profile>/<run>/
├── benchmark_YYYYmmdd_HHMMSS.csv
└── logs/
    ├── create_index_<metric>_lists_<lists>.log
    ├── pgbench_<metric>_clients_<clients>_probes_<probes>_warmup_<repeat>.log
    ├── pgbench_<metric>_clients_<clients>_probes_<probes>_measured_<repeat>.log
    ├── recall_<metric>_probes_<probes>.out
    └── recall_<metric>_probes_<probes>.err
```

benchmark CSV 字段包括：

```text
profile,metric,lists,probes,clients,jobs,repeat,
latency_avg_ms,tps,transactions,failed_transactions,
query_count,top_k,returned_items,matched_items,expected_items,
recall_at_k,min_query_recall,max_query_recall
```

检查每条数据记录是否包含 19 个字段：

```bash
awk -F, 'NR > 1 && NF != 19 {
    print "invalid:", FILENAME, "line:", NR, "fields:", NF
}' results/smoke/run_l2/*.csv
```

CSV 第一行是表头，不应被当作数据行错误。

## 记录测试环境

在 benchmark 结果旁记录测试环境：

```bash
./scripts/capture_environment.sh \
  --config config/smoke.conf \
  --output results/smoke/run_all
```

脚本会记录 benchmark 参数、Git revision、客户端工具路径、主机信息、编译器版本、数据库版本、扩展版本和选定的 PostgreSQL 参数。

脚本会创建：

```text
results/smoke/run_all/environment.txt
results/smoke/run_all/database_environment.txt
```

## 对比结果

比较使用相同距离类型和参数矩阵的两次运行：

```bash
BASELINE=$(find results/smoke/baseline \
  -type f -name '*.csv' | sort | tail -n 1)

CANDIDATE=$(find results/smoke/candidate \
  -type f -name '*.csv' | sort | tail -n 1)

./scripts/compare_results.sh \
  --baseline "$BASELINE" \
  --candidate "$CANDIDATE" \
  --output results/comparison/l2_baseline_candidate.csv
```

记录会按照 `metric`、`lists`、`probes`、`clients`、`jobs` 和 `repeat` 对齐。对比 CSV 会报告延迟百分比变化、TPS 百分比变化、Recall 绝对变化、事务失败数和结果数量。

不要直接比较 L2 与 IP 或 Cosine；它们使用不同的操作符类，最近邻排序结果也不同。

## 手动 workload 测试

workload SQL 文件是 pgbench 脚本，必须使用 `pgbench` 执行，不能使用 `psql -f` 直接执行。

请在 benchmark 目录中执行命令，或者为 workload SQL 使用绝对路径：

```bash
/opt/opentenbase-pg19/bin/pgbench \
  -h "$PGHOST" \
  -p "$PGPORT" \
  -U "$PGUSER" \
  -n \
  -M simple \
  -c 1 \
  -j 1 \
  -t 1 \
  -D query_count=100 \
  -D top_k=10 \
  -D probes=1 \
  -f sql/workload_l2.sql \
  "$PGDATABASE"
```

`build/src/bin/pgbench` 是目录；源码构建的可执行文件是 `build/src/bin/pgbench/pgbench`。

## 常见问题

### 数据库连接失败

直接测试配置中的数据库连接：

```bash
psql -X \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -c 'SELECT 1;'
```

### 重建 schema 时等待锁

处于 `idle in transaction` 状态的会话可能会在 `vector_bench.items` 上持有 `AccessShareLock`。重新执行 `--recreate` 前，应提交或回滚旧会话；必要时只终止已经确认的过期客户端 backend。

```bash
psql -X -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c \
  "SELECT pid, usename, state, wait_event_type, wait_event
   FROM pg_stat_activity
   WHERE datname = current_database();"
```

### CSV 解析失败

检查 pgbench 测量日志和 Recall 输出：

```bash
cat results/<profile>/<run>/logs/pgbench_*.log
cat results/<profile>/<run>/logs/recall_*.out
cat results/<profile>/<run>/logs/recall_*.err
```

测试失败后建议使用新的输出目录，避免不完整文件与后续结果混在一起。

## 可复现性规则

baseline 和 optimized 对比时，以下内容必须保持一致：

- 数据集和查询向量。
- 随机种子和向量维度。
- `TOP_K`、`lists` 和 `probes` 参数矩阵。
- 数据库配置和 `PGOPTIONS`。
- 预热时间、正式测量时间和重复次数。
- 客户端数量、worker 数量和查询模式。

不要把单次 smoke 运行当作性能结论。应使用多轮测量，并同时保留正向结果和负向结果。

大规模原始数据、临时日志和机器相关的绝对路径应保留在本机，除非它们对正式复现实验确实必要。
