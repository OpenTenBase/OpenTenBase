# Benchmark 结果

[English Documentation](README.md)

本目录用于保存 pgvector IVFFlat benchmark 生成的本地测试结果。当前 benchmark 针对 OpenTenBase PG19 分支中的 pgvector `0.8.6`。

## 结果目录结构

每次 benchmark 运行都应使用独立的结果目录：

```text
results/
├── smoke/
│   ├── dataset_metadata.txt
│   └── run_l2/
│       ├── benchmark_YYYYmmdd_HHMMSS.csv
│       └── logs/
├── quick/
│   └── run_all/
│       ├── benchmark_YYYYmmdd_HHMMSS.csv
│       └── logs/
├── disk/
│   └── run_all/
│       ├── benchmark_YYYYmmdd_HHMMSS.csv
│       └── logs/
└── comparison/
    └── l2_baseline_candidate.csv
```

建议使用具有明确含义的目录名称，例如：

```text
results/smoke/baseline/
results/smoke/candidate/
results/quick/pr283-parity/
results/quick/optimized/
results/comparison/
```

测试失败后不要继续复用原结果目录。失败运行可能留下不完整的 CSV 或日志，导致后续分析结果不明确。

## 生成的文件

benchmark 脚本可能生成以下文件：

- `benchmark_*.csv`：汇总后的延迟、吞吐量、事务状态和 Recall@K 结果。
- `dataset_metadata.txt`：数据集大小、维度、随机种子、数据库和版本元信息。
- `environment.txt`：主机、编译器、Git、benchmark 配置和工具元信息。
- `database_environment.txt`：数据库版本、扩展版本和选定的 PostgreSQL 参数。
- `logs/create_index_*.log`：IVFFlat 索引创建输出。
- `logs/pgbench_*.log`：预热和正式 pgbench 输出。
- `logs/recall_*.out`：Recall@K SQL 原始输出。
- `logs/recall_*.err`：Recall@K 错误输出。
- 过滤场景的 ground truth 保存在 `vector_bench.filtered_truth` 中，并随数据集重新生成。
- 过滤 workload 和 Recall 输出应与对应 CSV 结果使用相同的 `metric`、过滤 profile 和 probes 参数。
- `filtered_benchmark_*.csv`：过滤 workload 的延迟和 Recall@K 结果。

## CSV 格式

benchmark CSV 文件使用以下 19 个字段：

```text
profile,metric,lists,probes,clients,jobs,repeat,
latency_avg_ms,tps,transactions,failed_transactions,
query_count,top_k,returned_items,matched_items,expected_items,
recall_at_k,min_query_recall,max_query_recall
```

在比较结果文件前，先检查每条数据记录的字段数量：

```bash
awk -F, 'NR > 1 && NF != 19 {
    print "invalid:", FILENAME, "line:", NR, "fields:", NF
}' results/<profile>/<run>/*.csv
```

这里使用 `NR > 1` 排除了第一行 CSV 表头。

## 运行一个测试 profile

运行 profile 前，先准备数据集并生成精确搜索 ground truth：

```bash
./scripts/prepare_dataset.sh \
  --config config/smoke.conf \
  --output results/smoke \
  --recreate

psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v recall_query_count=100 \
  -v top_k=10 \
  -f sql/exact_search.sql

./scripts/run_benchmark.sh \
  --config config/smoke.conf \
  --output results/smoke/run_all
```

在同一个结果目录中记录测试环境：

```bash
./scripts/capture_environment.sh \
  --config config/smoke.conf \
  --output results/smoke/run_all
```

## 对比测试结果

只有在以下条件一致时，才应比较两次运行：

- 数据集一致。
- 查询集一致。
- 距离类型一致。
- 参数矩阵一致。
- 数据库配置一致。
- 测量设置一致。

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

对比脚本按照 `metric`、`lists`、`probes`、`clients`、`jobs` 和 `repeat` 对齐记录，并报告延迟百分比变化、TPS 百分比变化、Recall 绝对变化、事务失败数和结果数量。

不要直接比较 L2、Inner Product 和 Cosine 记录。它们使用不同的操作符类和距离排序规则。

## Git 提交规则

仓库会忽略生成的结果文件：

```text
results/*
```

默认情况下，results 目录中只保留本 README 和 `.gitignore`。除非明确需要一个小型、经过说明的示例，否则不要提交以下文件：

- 大型向量数据文件。
- pgbench 原始日志。
- Recall@K 原始输出。
- 与主机相关的环境文件。
- 失败运行产生的临时输出。
- 与机器相关的绝对路径。

正式实验结果应保存在仓库之外，或随项目报告一起归档。如果 PR 必须包含结果，优先提交小型、脱敏后的汇总表或有说明的结果附件，不要直接提交完整的原始输出。

## 可复现性元数据

每组正式实验结果都应在被忽略的原始结果文件之外，或项目报告旁边保留以下元数据：

- OpenTenBase commit 和分支。
- pgvector 版本和扩展 commit。
- 操作系统、CPU、内存和存储信息。
- 编译器和构建配置。
- 数据集行数、维度、聚类数量、过滤分类数量和随机种子。
- 查询数量、`TOP_K`、`lists` 和 `probes` 参数。
- 客户端数量、worker 数量、预热时间、测量时间和重复次数。
- `work_mem`、JIT 和扫描路径等 PostgreSQL 参数。
- `maintenance_work_mem` 等索引构建参数。
- 测试使用 warm cache、cold cache 还是受控缓存条件。

可以使用 `capture_environment.sh` 自动收集其中的大部分信息。
