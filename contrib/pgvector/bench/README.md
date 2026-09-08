# Benchmark for the index build diagnostics

`run_bench.sh` reproduces the numbers quoted in
[`doc/vector-index-tuning.md`](../doc/vector-index-tuning.md). It walks through
the same story a user would:

1. collect the environment
2. build an index with the default `maintenance_work_mem` and read
   `vector_index_build_report()`
3. ask `vector_index_memory_estimate()` / `vector_index_memory_advise()` for a
   recommended setting and rebuild with it
4. verify: estimated vs measured memory, whether the graph spilled to disk,
   recall@k, and the `vector_index_diagnose()` output
5. write `report.md` next to this file

## Prerequisites

* a PostgreSQL 18 server with the `vector` extension available
* `psql` on `PATH`

Point the script at your server with:

```sh
PGHOST_DIR=/tmp PGPORT=5432 PGUSER=postgres PGDATABASE=postgres ./run_bench.sh
```

and tune the workload with `ROWS`, `DIMS`, `M`, `EF_CONSTRUCTION`, `EF_SEARCH`,
`K`, `SAMPLES`, `REPEATS` and `START_MEM`.

## Caveats that will bite you

* **`vector_index_build_report()` is backend-local.** `CREATE INDEX` and the
  query that reads the report must be sent in the *same* psql session. The
  script does this with a heredoc; splitting them across two `psql -c` calls
  returns an empty report.

* **The pre-build NOTICE is emitted on stdout.** This build warns *before*
  creating the index when the graph is unlikely to fit, so psql output mixes
  notices with data. Every data row is therefore tagged `BENCHROW|` and pulled
  back out with `grep`.

* **Parallelism.** `CREATE INDEX` takes its workers from
  `max_parallel_maintenance_workers`, **not** from
  `max_parallel_workers_per_gather`. The script sets both to 0 so the run is
  really serial. This matters for the numbers: in a parallel build the reported
  `memory_total` is the shared budget minus bookkeeping, and a mid-build flush
  performed by a worker is not reflected in the leader's `flush_ms`.

* **`-march=native`.** With some compilers (observed with gcc 9.4 on a recent
  AMD CPU) pgvector's default `-march=native` produces code that segfaults while
  building HNSW indexes above roughly 20k rows. Rebuild the extension with
  `make OPTFLAGS=""` if that happens. This script does not change any Makefile.

## Files

| File | Purpose |
| --- | --- |
| `run_bench.sh` | the benchmark itself |
| `report.md` | output of the last run |
| `README.md` | this file |
