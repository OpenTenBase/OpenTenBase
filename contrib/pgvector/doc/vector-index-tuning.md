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

### 3.3 Safety margin

The graph lives in a generation memory context that hands memory out in blocks,
so the real total is a little above the sum of the individual allocations. The
estimate therefore applies a **one-sided 10% margin**: it can only ever be
inflated, never reduced, so a user who follows the advice should not spill.

Measured against real builds the estimate lands close to the observed peak and
errs on the high side (see [section 8](#8-environment-of-the-reference-run):
estimate 128.6 MB vs measured 118 MB).

### 3.4 Scope and limits

* only the **`vector`** type is modelled, i.e. `sizeof(float4)` per dimension.
  `halfvec` (2 bytes per dimension) and `sparsevec` need different factors and
  are **not** covered.
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

`probes = 1` with more than a handful of lists almost always means poor recall;
`vector_index_diagnose()` warns about it. `probes >= lists` means every list is
visited, at which point an exact scan is simpler and just as fast.

### 4.3 Dimensions

Above roughly 1000 dimensions both distance computation and memory grow
painfully. Consider reducing the dimensionality, or the `halfvec` type to halve
the vector memory.

## 5. Troubleshooting by symptom

| Symptom | First thing to check | Likely fix |
| --- | --- | --- |
| **build is slow** | `vector_index_build_report()` — is `flushed_to_disk` true? | set `maintenance_work_mem` to the value suggested by `vector_index_memory_advise()` and rebuild |
| **build eats memory / risks spilling** | `vector_index_memory_advise('table')` | raise `maintenance_work_mem`, or lower `m`, or use `halfvec` |
| **recall is low** | `vector_index_diagnose('index', true)` | raise `hnsw.ef_search` (or `ivfflat.probes`); if that is not enough, rebuild with a larger `m` or a corrected `lists` |
| **queries are slow** | is `hnsw.ef_search` (or `ivfflat.probes`) very high? | lower it and re-measure recall; that is the recall/latency trade-off knob |
| **nothing looks wrong** | `vector_index_diagnose('index', true)` | it also reports unknown statistics (never `ANALYZE`d) and tables too small to benefit from an index |

## 6. Two pitfalls when measuring recall

`vector_index_recall_check()` compares an index scan against an exact
(brute-force) scan. Both halves have a trap:

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
| baseline | 64MB | 14155.6 | 14029.2 | 129.9 | 65 MiB | yes |
| as recommended | 256MB | 5821.7 | 5490.4 | 234.5 | 118 MiB | no |

* following the recommendation made the build **about 2.4x faster** and removed
  the mid-build spill;
* the estimate was 134,860,000 bytes (128.6 MB) against a measured peak of
  123,731,840 bytes — the measured value is 8.3% below the estimate, i.e. the
  estimate is conservative, as intended;
* recall@10 over 20 sample queries was `1.0000` (`good`) after rebuilding with
  the recommended setting.

## 9. See also

* [`bench/run_bench.sh`](../bench/run_bench.sh) and
  [`bench/README.md`](../bench/README.md) — how to reproduce the numbers
* upstream `README.md` — `## Monitoring`, `## Indexing Progress`,
  `## Filtering`, `## Performance`
