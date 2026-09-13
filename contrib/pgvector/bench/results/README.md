# Benchmark Results

[中文文档](README_ZH.md)

This directory stores local outputs produced by the pgvector IVFFlat
benchmark. The benchmark currently targets the OpenTenBase PG19 branch with
pgvector `0.8.6`.

## Result Layout

Each benchmark run should use a separate directory:

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

Use descriptive directory names such as:

```text
results/smoke/baseline/
results/smoke/candidate/
results/quick/pr283-parity/
results/quick/optimized/
results/comparison/
```

Do not reuse a result directory after a failed run. A failed run can leave
partial CSV files or logs that make later analysis ambiguous.

## Generated Files

The benchmark scripts can generate the following files:

- `benchmark_*.csv`: summarized latency, throughput, transaction, and Recall@K results.
- `dataset_metadata.txt`: dataset size, dimensions, seeds, database, and version metadata.
- `environment.txt`: host, compiler, Git, benchmark configuration, and tool metadata.
- `database_environment.txt`: database version, extension versions, and selected PostgreSQL settings.
- `logs/create_index_*.log`: IVFFlat index creation output.
- `logs/pgbench_*.log`: warmup and measured pgbench output.
- `logs/recall_*.out`: raw Recall@K SQL output.
- `logs/recall_*.err`: Recall@K error output.
- Filtered ground truth is stored in `vector_bench.filtered_truth` and is regenerated with the dataset.
- Filtered workload and Recall outputs use the same `metric`, filter profile, and probe settings as the corresponding CSV result.
- `filtered_benchmark_*.csv`: filtered workload latency and Recall@K results.

## CSV Schema

Benchmark CSV files use the following 19 columns:

```text
profile,metric,lists,probes,clients,jobs,repeat,
latency_avg_ms,tps,transactions,failed_transactions,
query_count,top_k,returned_items,matched_items,expected_items,
recall_at_k,min_query_recall,max_query_recall
```

Validate the field count before comparing result files:

```bash
awk -F, 'NR > 1 && NF != 19 {
    print "invalid:", FILENAME, "line:", NR, "fields:", NF
}' results/<profile>/<run>/*.csv
```

The header is excluded from the validation with `NR > 1`.

## Running a Profile

Prepare a dataset and generate exact ground truth before running a profile:

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

Capture the environment in the same result directory:

```bash
./scripts/capture_environment.sh \
  --config config/smoke.conf \
  --output results/smoke/run_all
```

## Comparing Runs

Compare only runs with the same dataset, query set, metric, parameter matrix,
database configuration, and measurement settings:

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

The comparison script matches rows by `metric`, `lists`, `probes`, `clients`,
`jobs`, and `repeat`. It reports latency percentage change, TPS percentage
change, Recall absolute change, transaction failures, and result counts.

Do not compare L2, inner product, and cosine rows with each other. They use
different operator classes and distance orderings.

## Git Policy

Generated result files are ignored by the repository:

```text
results/*
```

Only this README and `.gitignore` are retained under the results directory by
default. Do not commit the following files unless a small, documented sample
is explicitly required:

- Large vector data files.
- Raw pgbench logs.
- Raw Recall@K output.
- Host-specific environment files.
- Temporary failed-run output.
- Machine-specific absolute paths.

Keep formal experiment outputs outside the repository or archive them with the
project report. If a result is included in a pull request, prefer a small
sanitized summary table or a documented result artifact rather than committing
the complete raw output.

## Reproducibility Metadata

Every formal result set should retain the following metadata outside the
ignored raw-result files or beside the project report:

- OpenTenBase commit and branch.
- pgvector version and extension commit.
- Operating system, CPU, memory, and storage information.
- Compiler and build configuration.
- Dataset rows, dimensions, clusters, filter categories, and random seeds.
- Query count, `TOP_K`, `lists`, and `probes` values.
- Client count, worker count, warmup, duration, and repetition count.
- PostgreSQL settings such as `work_mem`, JIT, and scan-path settings.
- Index-build settings such as `maintenance_work_mem`.
- Whether the test used warm cache, cold cache, or controlled cache conditions.

Use `capture_environment.sh` to collect most of this information automatically.
