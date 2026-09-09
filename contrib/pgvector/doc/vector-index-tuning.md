# Vector index tuning guide

This document explains how to read the build diagnostics added on top of
upstream pgvector, how the memory estimate is derived, and which parameters to
change when a build is slow, memory hungry, or when queries return poor recall.

The numbers quoted here come from a real run of [`bench/run_bench.sh`](../bench/run_bench.sh);
see [Environment](#8-environment-of-the-reference-run) for the machine and the
workload.

---

## 1. The four phases of an HNSW build

A build reports its progress through `pg_stat_progress_create_index` using these
sub-phases:

| # | Phase | What happens |
| --- | --- | --- |
| 1 | `initializing` | the build state is set up |
| 2 | `loading tuples` | the heap is scanned and every vector is inserted into the graph |
| 3 | `flushing graph` | the in-memory graph is written out to index pages |
| 4 | `finalizing` | the new pages are WAL logged and the build is cleaned up |

**`flushing graph` is the one to watch.** It normally appears once, at the end
of the build. If it appears *early* — that is, `loading tuples` is interrupted
by `flushing graph` and then resumes — the graph outgrew `maintenance_work_mem`
and was materialised to disk ahead of time. Every vector inserted after that
point goes through the on-disk path, which is why the build gets much slower.
In that situation PostgreSQL also logs:

```
NOTICE:  hnsw graph no longer fits into maintenance_work_mem after N tuples
```

To avoid guessing, ask for an estimate *before* building:

```sql
SELECT * FROM vector_index_memory_advise('my_table');
```

and, since this build also warns up front, you will see the recommendation
before `CREATE INDEX` does any work at all:

```
NOTICE:  hnsw index build is estimated to need 128.6 MB for 100000 rows of 128 dimensions (m = 16)
DETAIL:  The current maintenance_work_mem setting is 65536 kB, which is below the estimate.
HINT:    Set maintenance_work_mem to at least 256MB before creating the index ...
```

## 2. Reading `vector_index_build_report()`

After a build, in the same session:

```sql
SELECT * FROM vector_index_build_report();
```

| Column | Meaning |
| --- | --- |
| `init_ms` | time spent setting the build state up |
| `load_ms` | heap scan plus graph construction — normally the dominant phase |
| `flush_ms` | total time spent writing the graph out (see [the note below](#21-a-note-on-flush_ms)) |
| `finalize_ms` | WAL logging and cleanup |
| `total_ms` | wall time of the whole build |
| `memory_peak` | highest graph memory observed |
| `memory_total` | the budget derived from `maintenance_work_mem` |
| `tuples_done` | number of vectors inserted |
| `flushed_to_disk` | true when the graph was written out *before* the build finished |
| `valid` | false when this session has not built an index yet |

A typical healthy build at 100k rows / 128 dimensions reports roughly
`load_ms` 5490, `flush_ms` 235, `finalize_ms` 100, `memory_peak` 118 MB,
`flushed_to_disk = false`. The same data with a too small
`maintenance_work_mem` reports `load_ms` 14029 and `flushed_to_disk = true` —
about **2.4x slower** for the same index.

### 2.1 A note on `flush_ms`

`flush_ms` accumulates *every* call that writes the graph out, including one
that happens in the middle of the build when memory runs out. Two consequences:

* in a build that spilled, part of the write cost sits inside `load_ms` as
  well, because the vectors inserted afterwards go through the on-disk path;
* in a **parallel** build the mid-build flush may be performed by a worker, and
  that worker's time is not reflected in the leader's `flush_ms`.

If you want the numbers to be exactly comparable, run the build serially
(`SET max_parallel_maintenance_workers = 0;`), which is what the benchmark
does.

## 3. How the memory estimate is derived

The model mirrors what the build actually allocates for every element:

1. **the vector itself** — `HnswAlloc(allocator, valueSize)`
2. **an `HnswElementData`** — allocated by `HnswInitElement()`
3. **an array of per-level neighbour array pointers** — one entry per level
4. **one `HnswNeighborArray` per level** — level 0 holds `2 * m` slots, higher
   levels hold `m`

### 3.1 Allocation overhead

Each allocation goes through `MemoryContextAlloc()`, which prepends a
`MemoryChunk` header and rounds the result up to `MAXIMUM_ALIGNOF`. The estimate
mirrors that instead of using the raw request size:

```c
#define HNSW_ALLOC_SIZE(size) MAXALIGN(add_size((Size) (size), sizeof(MemoryChunk)))
```

### 3.2 Expected number of levels

`HnswInitElement()` draws a level as

```c
int level = (int) (-log(RandomDouble()) * ml);   /* ml = 1 / log(m) */
```

Since `-log(U)` is exponentially distributed, `P(level >= k) = m^-k`, so the
expected number of levels **above** the first is

```
E[extra levels] = 1 / (m - 1)
```

For the default `m = 16` that is about 0.067, i.e. the overwhelming majority of
elements sit at level 0.

### 3.3 Safety margin, and why two different accuracies are quoted

The graph lives in a generation memory context that hands memory out in blocks,
so the real total is a little above the sum of the individual allocations. The
estimate therefore applies a **one-sided 10% margin**: it can only ever be
inflated, never reduced.

That produces two numbers that are easy to confuse, so they are always reported
separately:

| Quantity | What it is | How to get it |
| --- | --- | --- |
| **raw model** | the model in 3.1–3.2 with no margin | published estimate / 1.1 |
| **published estimate** | what `vector_index_memory_estimate()` reports | raw model x 1.1 |

Measured against real builds ([section 8](#8-environment-of-the-reference-run),
reproduced by `bench/run_bench.sh`):

| rows | dims | m | published estimate (B) | raw model (B) | measured peak (B) | raw dev | with margin |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 10000 | 128 | 16 | 13486000 | 12259999 | 12582784 | +2.63% | -6.7% |
| 100000 | 128 | 16 | 134860000 | 122599999 | 123731840 | +0.92% | -8.3% |
| 100000 | 768 | 16 | 416460000 | 378599999 | 379584384 | +0.26% | -8.9% |
| 500000 | 128 | 32 | 955350000 | 868499999 | 869269376 | +0.09% | -9.0% |

* **raw dev** (0.09% – 2.63%, under 1% for all but the smallest configuration)
  is the accuracy of the **model itself** — this is the figure quoted in the
  source comment.
* **with margin** (-6.7% … -9.0%) compares the measured peak against what the
  tools actually report. It is **negative on purpose**: the published estimate
  sits above the measured peak, not below it. The margin is the reason the two
  accuracies differ, and it is one-sided so that following the advice does not
  spill.

A reader who only sees "-8.3%" might read it as "the estimate is 8% wrong"; it
is not — it is the intended headroom. The model error is the "raw dev" column.

### 3.4 Units

`estimated_bytes` is authoritative and is reported in bytes. The convenience
string `estimated_pretty` uses **binary** units and is labelled accordingly
(MiB/GiB), which is not the same convention as PostgreSQL's `pg_size_pretty()`
(decimal kB/MB/GB). Benchmark tables quote bytes.

### 3.5 Scope and limits

* the model covers **HNSW only**. IVFFlat build memory is not predicted
  (see [4.2](#42-ivfflat)).
* **`vector` and `halfvec` are both modelled**, through `bytes_per_dim`: 4 for
  `vector` (one `float4` per dimension) and 2 for `halfvec`.
  `vector_index_memory_advise()` reads the column type and picks the right
  value itself, so no argument is needed there.

  **Warning:** the default is 4. Calling `vector_index_memory_estimate()` on a
  `halfvec` workload without passing `bytes_per_dim => 2` therefore
  **overestimates**, and because the vector dominates the total at high
  dimensions the result can be close to **double** the real figure — for 100k
  rows of 768 dimensions, 416,460,000 bytes instead of 247,500,000. Use
  `vector_index_memory_advise()` (type-aware), or pass the argument:

  ```sql
  SELECT * FROM vector_index_memory_estimate(100000, 768, 16, 2);
  ```

  Values other than 2 and 4 are rejected.
* `sparsevec` is **not** covered: its size depends on how many components are
  non-zero, which is a property of the data rather than of the dimensions.
* the number of rows comes from `pg_class.reltuples`, so `ANALYZE` first.
* `ef_construction` mainly affects build *time*, not the size of the graph, so
  it is not part of the estimate.

## 4. Parameter recommendations

### 4.1 HNSW

| Parameter | Default | When to raise | Notes |
| --- | --- | --- | --- |
| `m` | 16 | larger datasets or higher recall targets (16 – 32) | raises build time and memory roughly linearly |
| `ef_construction` | 64 | 64 – 200 for better graph quality | build-time only; must be `>= 2 * m` |
| `hnsw.ef_search` | 40 | 100 – 400 when recall matters more than latency | session GUC, no rebuild needed |

`hnsw.ef_search` below 10 is a red flag: in the reference run setting it to 1
dropped recall@10 to **0.1**, which `vector_index_recall_check()` reports as
`low` and `vector_index_diagnose()` reports as a `warning`.

### 4.2 IVFFlat

| Parameter | Recommendation |
| --- | --- |
| `lists` | `rows / 1000` up to 1M rows, `sqrt(rows)` above |
| `ivfflat.probes` | start at `lists / 10` |

`probes = 1` with more than a handful of lists usually means poor recall;
`vector_index_diagnose()` warns about it. `probes >= lists` means every list is
visited, at which point an exact scan is simpler and just as fast.

**Two honest caveats about IVFFlat here.**

1. *Build memory is not predicted.* An IVFFlat build can fail outright during
   k-means with `ERROR: memory required is N MB, maintenance_work_mem is M MB`.
   We hit this while building a 100k-row index with `lists = 1000`
   (222 MB required). The tools here do **not** predict it, because the memory
   model in [section 3](#3-how-the-memory-estimate-is-derived) covers HNSW only.
   Treat IVFFlat memory prediction as future work, and raise
   `maintenance_work_mem` when a build with many lists fails this way.

2. *The probes warning was not reproduced as a recall regression.* On the
   synthetic uniform dataset used by `bench/run_bench.sh`, `recall_check`
   measured recall@10 = `1.0000` at `probes = 1` and also at `probes = 10`.
   The diagnose path correctly warns on `probes = 1`, but the measured recall
   did not drop, so the benchmark demonstrates the diagnose behaviour rather
   than a probes-driven regression. A probes-sensitive case needs a realistic
   clustered dataset, since production embeddings are clustered. We could not
   reproduce one here, so no number is claimed for it.

### 4.3 Dimensions

Above roughly 1000 dimensions both distance computation and memory grow
painfully. Consider reducing the dimensionality, or switching to `halfvec`,
which stores 2 bytes per dimension instead of 4.

The tools follow the type: `vector_index_memory_advise()` detects a `halfvec`
column and reports the smaller estimate automatically, and
`vector_index_diagnose()` works on a `halfvec` index as well. Only the
standalone `vector_index_memory_estimate()` needs to be told, via
`bytes_per_dim => 2` (see [3.5](#35-scope-and-limits)).

## 5. Troubleshooting by symptom

| Symptom | First thing to check | Likely fix |
| --- | --- | --- |
| **build is slow** | `vector_index_build_report()` — is `flushed_to_disk` true? | set `maintenance_work_mem` to the value suggested by `vector_index_memory_advise()` and rebuild |
| **build eats memory / risks spilling** | `vector_index_memory_advise('table')` | raise `maintenance_work_mem`, or lower `m`, or use `halfvec` |
| **recall is low** | `vector_index_diagnose('index', true)` | raise `hnsw.ef_search` (or `ivfflat.probes`); if that is not enough, rebuild with a larger `m` or a corrected `lists` |
| **queries are slow** | is `hnsw.ef_search` (or `ivfflat.probes`) very high? | lower it and re-measure recall; that is the recall/latency trade-off knob |
| **the index is ignored / never used** | `vector_index_diagnose('index')` | an invalid index is reported with severity **error** plus a `REINDEX` suggestion, and recall is reported as not measured rather than as a number |
| **nothing looks wrong** | `vector_index_diagnose('index', true)` | it also reports unknown statistics (never `ANALYZE`d) and tables too small to benefit from an index |

Severity is `ok`, `info`, `warning` or `error`. Only an invalid index is
`error`: it is the one case where nothing else in the report is meaningful
until the index is rebuilt. It is what a failed `CREATE INDEX CONCURRENTLY`
leaves behind.

## 6. Measuring recall, and why the function beats doing it by hand

Upstream's `## Monitoring` section shows how to compare an index scan against an
exact one by hand. `vector_index_recall_check()` performs the same comparison,
and adds three things a hand-written query does not:

1. **It produces a real exact baseline.** A manual comparison usually flips
   `enable_seqscan` off and assumes the other side is exact. For KNN queries it
   is not — see pitfall 1 below.
2. **It refuses to report a number it cannot stand behind.** If the index cannot
   be used for the query shape, it raises an error rather than returning the
   misleading `1.0` a manual comparison would silently produce — pitfall 2.
3. **It returns advice, not just a number.** Besides recall@k it reports an
   assessment (`good` / `acceptable` / `low`) and a concrete next step, so the
   result can be acted on without knowing the thresholds by heart. And
   `vector_index_diagnose(index, true)` folds the same measurement into a full
   report, as in [section 4 of the benchmark report](../bench/report.md).

Both pitfalls below have the same failure mode: a naive comparison reports a
perfect score.

1. **`enable_seqscan = off` does not give you an exact baseline.** For a KNN
   query (`ORDER BY v <-> q LIMIT k`) the planner prefers the index anyway,
   because an index scan that stops after `k` rows is far cheaper than sorting
   the whole table. Turning *sequential* scans off therefore changes nothing,
   and both scans use the index — recall comes out as a misleading `1.0`. The
   exact baseline is instead produced by disabling **index** scans
   (`enable_indexscan`, `enable_indexonlyscan`, `enable_bitmapscan`), which
   forces the sequential scan plus top-N sort that is the real ground truth.

   Upstream documents the same idea for manual comparisons under
   `## Monitoring`.

2. **A check that cannot use the index must fail, not report a perfect score.**
   If the planner cannot use the index for the query shape — wrong operator
   class, unsupported distance operator, an index that is not valid — the index
   scan silently degenerates and recall would again be `1.0`. The check
   therefore verifies the plan first and raises an error instead of returning a
   number:

   ```
   ERROR:  the planner cannot use index my_index for this query shape
   ```

## 7. Verifying a change

```sql
-- 1. does the build still fit, and is it still fast?
SELECT * FROM vector_index_build_report();

-- 2. is the result quality still good?
SELECT * FROM vector_index_recall_check('my_index', 10, 20);

-- 3. one call that answers both, plus parameter and memory findings
SELECT * FROM vector_index_diagnose('my_index', true);
```

`vector_index_recall_check()` scans the table twice, so treat it as an on-demand
check rather than a routine one.

## 8. Environment of the reference run

Reproduced with [`bench/run_bench.sh`](../bench/run_bench.sh); the full output is
in [`bench/report.md`](../bench/report.md).

| Item | Value |
| --- | --- |
| PostgreSQL | 18.6 on x86_64-pc-linux-gnu |
| OS / kernel | Linux 6.6.87.2-microsoft-standard-WSL2 x86_64 |
| CPU | AMD Ryzen 9 8945HX with Radeon Graphics (32 cores) |
| Memory | 15 GB |
| Compiler | gcc (Ubuntu 9.4.0-1ubuntu1~20.04.2) 9.4.0 |
| Build flags | `make OPTFLAGS=""` — see the note in `bench/README.md` about `-march=native` |
| Dataset | 100,000 rows x 128 dimensions |
| HNSW parameters | `m = 16`, `ef_construction = 64`, `hnsw.ef_search = 40` |
| Repeats | 3 per configuration |
| Statistics | median of 3 runs |
| Parallelism | serial: `max_parallel_maintenance_workers = 0`, `max_parallel_workers_per_gather = 0` |

### Results

| Configuration | `maintenance_work_mem` | total ms | load ms | flush ms | memory peak | spilled to disk |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 64MB | 14155.6 | 14029.2 | 129.9 | 68157312 B (65 MiB) | yes |
| as recommended | 256MB | 5821.7 | 5490.4 | 234.5 | 123731840 B (118 MiB) | no |

* following the recommendation made the build **about 2.4x faster** and removed
  the mid-build spill;
* the published estimate was 134,860,000 bytes (128.6 MiB) against a measured
  peak of 123,731,840 bytes (118 MiB) — the measured value is 8.3% below the
  published estimate, which is the intended one-sided margin, not model error;
  the model error itself is 0.92%, see the sweep in
  [section 3.3](#33-safety-margin-and-why-two-different-accuracies-are-quoted).
  The sweep covers 10k–500k rows, 128 and 768 dimensions, m = 16 and 32
  (model error 0.09% – 2.63%).
* recall@10 over 20 sample queries was `1.0000` (`good`) after rebuilding with
  the recommended setting.

## 9. Anomaly behaviour

Every case below is asserted by a TAP test; the table shows what a user
actually sees. `bench/run_bench.sh` regenerates this table in its report.

| Case | Statement | Reported |
| --- | --- | --- |
| rows out of range | `vector_index_memory_estimate(0, 128, 16)` | `ERROR: rows must be positive` |
| dims out of range | `vector_index_memory_estimate(1000, 99999, 16)` | `ERROR: dims must be between 1 and 2000` |
| m out of range | `vector_index_memory_estimate(1000, 128, 1)` | `ERROR: m must be between 2 and 100` |
| table never analyzed | `vector_index_memory_advise('t')` | `ERROR: row count unknown`, plus a hint to run `ANALYZE` |
| no vector column | `vector_index_memory_advise('t')` | `ERROR: table does not have a vector column` |
| not an hnsw/ivfflat index | `vector_index_diagnose('btree_idx')` | `ERROR: index ... uses the btree access method, expected hnsw or ivfflat` |
| k out of range | `vector_index_recall_check('idx', 0, 20)` | `ERROR: k must be between 1 and 1000` |
| samples out of range | `vector_index_recall_check('idx', 10, 0)` | `ERROR: samples must be between 1 and 1000` |
| index unusable for the query | `vector_index_recall_check('idx', ...)` | `ERROR: the planner cannot use index ... for this query shape`, see [6](#6-two-pitfalls-when-measuring-recall)

Note the empty-table case is *not* an error: building on an empty table is
valid and `vector_index_build_report()` reports `tuples_done = 0`。

## 10. See also

* [`bench/run_bench.sh`](../bench/run_bench.sh) and
  [`bench/README.md`](../bench/README.md) — how to reproduce the numbers
* upstream `README.md` — `## Monitoring`, `## Indexing Progress`,
  `## Filtering`, `## Performance`
