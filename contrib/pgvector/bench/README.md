# pgvector IVFFlat Benchmark

[中文文档](README_ZH.md)

This directory contains a reproducible benchmark for the OpenTenBase PG19
`pgvector` IVFFlat access method.

The benchmark covers L2 distance, inner product, and cosine distance. It
records average latency, TPS, transaction status, and Recall@K.

## Directory Layout

```text
bench/
├── Makefile
├── README.md
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

The current benchmark uses `generate_vectors`. `summarize_latency.c` is
reserved for a future latency-summary implementation.

## Requirements

The following components are required:

- OpenTenBase PG19 with the `vector` extension installed.
- A reachable PostgreSQL-compatible benchmark database.
- Bash, `make`, a C compiler, `psql`, and `pgbench`.

The scripts do not create or drop the database. They only recreate the
`vector_bench` schema when `prepare_dataset.sh --recreate` is used.

All database commands should connect to the OpenTenBase Coordinator or the
PostgreSQL client entry point used by the test environment. Do not connect
directly to a read-only Datanode for the benchmark.

## Build Tools

Run the following commands from the benchmark directory:

```bash
make generate_vectors
```

The generated binary is:

```text
build/generate_vectors
```

The build script can also be used:

```bash
./scripts/build_tools.sh
```

## Configuration

Configuration files use trusted Bash-style variable assignments.

| Variable | Description |
| --- | --- |
| `ROWS` | Number of item vectors |
| `VECTOR_TYPE` | Vector column type: `vector` or `halfvec` (default: `vector`) |
| `DIMENSIONS` | Vector dimension |
| `QUERY_COUNT` | Number of query vectors |
| `RECALL_QUERY_COUNT` | Number of queries used for Recall@K |
| `CATEGORY_COUNT` | Number of deterministic filter categories |
| `TOP_K` | Number of nearest neighbors returned |
| `LISTS_VALUES` | IVFFlat `lists` parameter matrix |
| `PROBES_VALUES` | IVFFlat `probes` parameter matrix |
| `CLIENT_VALUES` | pgbench client-count matrix |
| `WARMUP_SECONDS` | Warmup duration for each configuration |
| `DURATION_SECONDS` | Measurement duration for each configuration |
| `REPEATS` | Number of measured repetitions |
| `WORK_MEM` | Per-query working memory |
| `MAINTENANCE_WORK_MEM` | Memory available during index creation |

Available profiles:

- `config/smoke.conf`: 10,000 rows, 32 dimensions, short exploratory test.
- `config/quick.conf`: 100,000 rows, 128 dimensions, daily regression test.
- `config/large.conf`: reserved for the large-scale profile.
- `config/disk.conf`: 8,000,000 rows, 128 dimensions, controlled-disk test.

Existing profiles default to `vector`. Reuse a profile for `halfvec` with an
independent output directory:

```bash
VECTOR_TYPE=halfvec ./scripts/prepare_dataset.sh \
  --config config/quick.conf \
  --output results/quick-halfvec/dataset \
  --recreate
```

For a baseline/candidate comparison against the same physical index, use
`--reuse-index` with exactly one metric and one `LISTS_VALUES` entry. The
runner validates the existing operator class and lists value before measuring.

## Prepare Dataset

The preparation script drops and recreates only the `vector_bench` schema.
Use `--recreate` explicitly because existing benchmark data will be replaced.

First verify the connection settings in the selected configuration file:

```bash
tail -n 5 config/smoke.conf
```

The default local setup uses:

```text
DB_HOST=127.0.0.1
DB_PORT=6543
DB_USER=linnaid
DB_NAME=pgvector_bench
```

Prepare the smoke dataset:

```bash
./scripts/prepare_dataset.sh \
  --config config/smoke.conf \
  --recreate
```

The script creates the following tables:

- `vector_bench.items`: searchable item vectors and deterministic `category_id` values.
- `vector_bench.queries`: query vectors.
- `vector_bench.truth`: exact-search ground truth.

It also writes dataset metadata to the selected result directory.

`category_id` is generated as `(id - 1) % CATEGORY_COUNT`. With the default
1,000 categories, one category selects approximately 0.1% of rows and ten
categories select approximately 1% of rows.

## Generate Ground Truth

Generate exact Top-K results before running Recall@K or the benchmark matrix:

```bash
psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v recall_query_count=100 \
  -v top_k=10 \
  -f sql/exact_search.sql
```

For `quick.conf`, use the configured query count:

```bash
psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v recall_query_count=1000 \
  -v top_k=10 \
  -f sql/exact_search.sql
```

The output should contain `QUERY_COUNT * TOP_K` rows for each distance metric.

Generate filtered ground truth after preparing a dataset with categories:

```bash
psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v recall_query_count=100 \
  -v top_k=10 \
  -v category_count=1000 \
  -f sql/exact_search_filtered.sql
```

The filtered script generates exact results for `percent_1` and
`percent_0_1` across L2, inner product, and cosine distance.

## Filtered Workloads

The filtered workload files are pgbench scripts. They must be executed with
`pgbench`, while `measure_filtered_recall.sql` must be executed with `psql`.

The workloads support these variables:

| Variable | Description |
| --- | --- |
| `query_count` | Number of available query vectors |
| `top_k` | Number of neighbors returned |
| `probes` | Initial IVFFlat probe count |
| `filter_limit` | Exclusive upper bound for `category_id` |
| `iterative_scan` | `off` or `relaxed_order` |
| `max_probes` | Maximum probes for iterative scans |

With `CATEGORY_COUNT=1000`, use:

| Profile | `filter_limit` | Approximate selectivity |
| --- | ---: | ---: |
| `percent_1` | `10` | 1% |
| `percent_0_1` | `1` | 0.1% |

Create the matching IVFFlat index before running a filtered workload. For
example, the L2 index is created with:

```bash
psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -v metric=l2 \
  -v opclass=vector_l2_ops \
  -v lists=100 \
  -f sql/create_index.sql
```

Run one filtered L2 workload transaction:

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

Measure filtered Recall@10 for the same index and filter:

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

Repeat the measurement with `iterative_scan=relaxed_order` to compare the
effect of iterative list expansion. Keep `max_probes` at least as large as
the initial `probes` value.

## Run Benchmark

Run an L2 smoke matrix:

```bash
./scripts/run_benchmark.sh \
  --config config/smoke.conf \
  --metrics l2 \
  --output results/smoke/run_l2
```

Run IP and cosine:

```bash
./scripts/run_benchmark.sh \
  --config config/smoke.conf \
  --metrics ip,cosine \
  --output results/smoke/run_ip_cosine
```

Run all three metrics:

```bash
./scripts/run_benchmark.sh \
  --config config/smoke.conf \
  --output results/smoke/run_all
```

Run the larger quick profile:

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

The benchmark runner automatically creates the matching IVFFlat operator
class for each metric, skips `probes > lists`, runs warmup, executes pgbench,
measures Recall@K, and writes CSV output.

## Output Files

Each run creates a timestamped CSV and supporting logs:

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

The benchmark CSV contains the following columns:

```text
profile,metric,lists,probes,clients,jobs,repeat,
latency_avg_ms,tps,transactions,failed_transactions,
query_count,top_k,returned_items,matched_items,expected_items,
recall_at_k,min_query_recall,max_query_recall
```

Check that every data row contains 19 fields:

```bash
awk -F, 'NR > 1 && NF != 19 {
    print "invalid:", FILENAME, "line:", NR, "fields:", NF
}' results/smoke/run_l2/*.csv
```

The first CSV line is the header and should not be treated as a data-row
error.

## Capture Environment

Capture the environment beside a benchmark result:

```bash
./scripts/capture_environment.sh \
  --config config/smoke.conf \
  --output results/smoke/run_all
```

The script records benchmark parameters, Git revision, client tool paths, host
information, compiler version, database version, extension versions, and
selected PostgreSQL settings.

It creates:

```text
results/smoke/run_all/environment.txt
results/smoke/run_all/database_environment.txt
```

## Compare Results

Compare two runs with the same metric and parameter matrix:

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

Rows are matched by `metric`, `lists`, `probes`, `clients`, `jobs`, and
`repeat`. The comparison CSV reports latency percentage change, TPS percentage
change, Recall absolute change, transaction failures, and result counts.

Do not compare L2 directly with IP or cosine. They use different operator
classes and produce different nearest-neighbor orderings.

## Manual Workload Test

The workload SQL files are pgbench scripts and must be run with `pgbench`, not
with `psql -f`.

Run the command from the benchmark directory, or use an absolute path for the
workload SQL file:

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

The path `build/src/bin/pgbench` is a directory. The source-built executable
is `build/src/bin/pgbench/pgbench`.

## Troubleshooting

### Database Connection Fails

Test the configured connection directly:

```bash
psql -X \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -c 'SELECT 1;'
```

### Schema Recreation Waits for a Lock

An `idle in transaction` session can retain an `AccessShareLock` on
`vector_bench.items`. Commit or roll back the old session, or terminate only a
confirmed stale client backend before retrying `--recreate`.

```bash
psql -X -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c \
  "SELECT pid, usename, state, wait_event_type, wait_event
   FROM pg_stat_activity
   WHERE datname = current_database();"
```

### CSV Parsing Fails

Inspect the measured pgbench log and Recall output:

```bash
cat results/<profile>/<run>/logs/pgbench_*.log
cat results/<profile>/<run>/logs/recall_*.out
cat results/<profile>/<run>/logs/recall_*.err
```

Use a new output directory after a failed run so partial files are not mixed
with a later result set.

## Reproducibility Rules

For baseline and optimized comparisons, keep the following values identical:

- Dataset and query vectors.
- Random seeds and vector dimensions.
- `TOP_K`, `lists`, and `probes` matrix.
- Database configuration and `PGOPTIONS`.
- Warmup duration, measurement duration, and repetition count.
- Client count, worker count, and query mode.

Do not treat a single smoke run as a performance conclusion. Use repeated
measurements and retain both positive and negative results.

Large raw datasets, temporary logs, and machine-specific absolute paths
should remain local unless they are necessary for a documented reproduction.
