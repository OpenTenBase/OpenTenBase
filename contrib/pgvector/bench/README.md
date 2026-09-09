# IVFFlat reproducible benchmark

This directory measures Recall@10, P50/P95 latency, QPS, result stability, relation size, and PostgreSQL I/O for L2, inner product, and cosine IVFFlat indexes. It is intended for a quiet Linux host and has no Python dependency.

## Requirements

- PostgreSQL 18 or 19 built with pgvector 0.8.6
- `psql`, `pgbench`, `gawk`, a C11 compiler, and `pg_prewarm`
- Enough free disk space for the selected profile
- `PG_CONFIG` set to the `pg_config` matching the measured server when it is not the first one in `PATH`
- For the disk profile, `PGDATA` and `PG_CTL` must identify the benchmark cluster so the tool can stop it and evict only the benchmark relation files with `posix_fadvise`

The `--prepare` option drops and recreates only the `vector_bench` schema in the selected database. Use a dedicated benchmark cluster.

## Profiles

| Profile | Rows | Dimensions | Lists | Probes |
| --- | --: | --: | --- | --- |
| `quick` | 100,000 | 128 | 100 | 1, 10, 20 |
| `memory` | 1,000,000 | 768 | 100, 500, 1000 | configured matrix around sqrt(lists) plus exact |
| `disk` | 8,000,000 | 384 | 1000 | 16, 32, 64 |

Vectors are generated as deterministic clustered data with seed `20260825`. The same data distribution includes varying vector norms so IP and cosine are not treated as equivalent workloads.

IVFFlat sampling and k-means use PostgreSQL's internal global PRNG, which SQL `setseed()` does not control. For a strict A/B comparison, build both modules with `PG_CPPFLAGS=-DIVFFLAT_BENCH_SEED=42`. Apply `pgvector-0.8.6-deterministic-build.patch` to an unmodified upstream baseline with `git apply --unidiff-zero` first. This switch affects only index construction; it adds no scan-path code.

## Run

Run baseline and optimized builds against separate dedicated clusters. Build both modules with the deterministic benchmark switch described above so equal data and parameters produce comparable indexes.

```bash
export PGURI=postgresql://localhost:6543/postgres
./run.sh --profile quick --variant baseline --output /tmp/ivf-base --prepare

export PGURI=postgresql://localhost:6544/postgres
./run.sh --profile quick --variant optimized --output /tmp/ivf-opt --prepare

./compare.sh /tmp/ivf-base/results.csv /tmp/ivf-opt/results.csv /tmp/ivf-report.md
```

For the standard memory profile, each point has a 15-second warmup and a 60-second measurement, repeated three times for one client and twelve clients. Override these values for smoke tests without editing scripts:

```bash
WARMUP_SECONDS=2 DURATION_SECONDS=5 REPEATS=1 \
  ./run.sh --profile quick --variant optimized --output /tmp/ivf-smoke --prepare
```

For the disk profile:

```bash
export PGDATA=/path/to/data
export PG_CTL=/path/to/pg_ctl
./run.sh --profile disk --variant optimized --output /path/to/results --prepare
```

The disk profile performs its warmup first, then stops PostgreSQL, syncs dirty pages, evicts every segment of the item table and active IVFFlat index with `posix_fadvise(POSIX_FADV_DONTNEED)`, restarts PostgreSQL, and immediately starts the measured sample.

`machine.txt`, `pg_stat_io.csv`, per-transaction pgbench logs, summaries, and `results.csv` are retained in the output directory. The report must state the actual table/index sizes from `results.csv`; cache residency is not inferred only from configured row counts.

## Hotspot profile

Stop the prepared cluster, then profile one metric in PostgreSQL single-user mode. The script first rebuilds the matching operator-class index outside the profiled process, using 100 lists by default, so index construction does not pollute the scan profile. Set `PROFILE_LISTS` to match another matrix point; `PROFILE_QUERIES` controls the query count and defaults to 1000. This avoids profiling the pgbench client instead of the backend.

```bash
./profile.sh /path/to/postgres /path/to/data postgres l2 /tmp/ivf-profile-l2
```

On systems where gprofng sampling is unavailable, rebuild pgvector with `PG_CFLAGS=-DIVFFLAT_BENCH` and use the emitted `GetScanLists`, `GetScanItems`, `BuildCandidateHeap`, and `tuplesort_performsort` phase timings together with `EXPLAIN (ANALYZE, BUFFERS)`.

## PG18 baseline

The optimization PR targets `REL_19_STABLE`. Build unmodified upstream pgvector 0.8.6 with `USE_PGXS=1 PG_CONFIG=/path/to/pg18/pg_config` to collect the PG18 baseline with these same scripts. No PG18 optimization patch is part of this delivery.
