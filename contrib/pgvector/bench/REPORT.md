# pgvector IVFFlat scan optimization report

Date: 2026-08-25

## Outcome

The PG19 quick A/B gate passed for L2, inner product, and cosine. Across all 18 measured configurations, Recall@10 was unchanged and every Top-K result hash matched. Median P95 decreased by 38.31% to 72.98%, while median QPS increased by 45.98% to 311.50%. No measured configuration regressed.

The optimization targets `REL_19_STABLE`. PG18 is measured as an unmodified pgvector 0.8.6 baseline only; cross-major results are not used to calculate the optimization gain.

## Sources and builds

| Component | Revision / build |
| --- | --- |
| PG19 baseline and optimized | `origin/REL_19_STABLE` at `d446ca2c459c5541c257fbff05ec5a0bdbeb6a0c` |
| PG18 baseline | `origin/REL_18_STABLE` at `4c66f172a09296b08d53526f802ddd2b461bd7e8` |
| pgvector baseline | upstream v0.8.6 at `8ee86c96f0fd72390f890aa8a336fda6d3ab4c6c` |
| Compiler flags | `-O2 -march=native -ftree-vectorize -fassociative-math -fno-signed-zeros -fno-trapping-math` |
| PG19 baseline A/B module | SHA-256 `2d96a3503b6d3bc566641e451adabcb1692b3896200f9d16de913d9dd2e65eeb` |
| PG19 optimized A/B module | SHA-256 `5011a3fcbc2cd10a195858225b0c152ac8aafa6fd096dcfd2f948d09d9927690` |
| PG18 baseline module | SHA-256 `432089bcd9c3c8baa7f50ebecf1d441753487cddb1680af7aee3cdf517af1a7c` |
| Final PG19 production module | SHA-256 `825475eb55031c14bc0f8256599f3c7a1fe27d401712ce8617cf6d8ef335e242` |

IVFFlat training uses PostgreSQL's internal global PRNG, which SQL `setseed()` does not control. Both sides of the A/B were therefore compiled with `PG_CPPFLAGS=-DIVFFLAT_BENCH_SEED=42`. This switch resets the PRNG only when building an IVFFlat index and adds no scan-path code. The upstream baseline adaptation is in `pgvector-0.8.6-deterministic-build.patch`. The final production module was rebuilt without this switch and used for all final regression tests.

## Machine

| Item | Value |
| --- | --- |
| Environment | WSL2 Linux, kernel `6.18.33.2-microsoft-standard-WSL2` |
| CPU | AMD Ryzen 9 9900X, 12 cores / 24 threads, 32 MiB L3 |
| Memory | 23 GiB RAM, 6 GiB swap; about 19 GiB available during capture |
| Storage | WSL virtual ext4, 1 TiB, 896 GiB available, reported `ROTA=1` |
| Compiler | GCC 13.3.0 |
| PG19 | PostgreSQL 19beta3, `shared_buffers=128MB`, `work_mem=4MB` server default |
| PG18 | PostgreSQL 18.6, same PostgreSQL memory settings |
| Benchmark query | `work_mem=64MB`, JIT off, sequential scans disabled |

This is a development-machine result, not a bare-metal release number. Host activity and WSL storage virtualization remain sources of variance.

## Workload

The measured quick workload used 100,000 rows at 128 dimensions, 1,000 query vectors, 50 exact-recall queries, K=10, and clustered deterministic vectors. The item table was 58,720,256 bytes (56 MiB); an IVFFlat index was about 54,976,512 bytes (52.4 MiB). The relation pair fits in host RAM and the run used a two-second warmup before each five-second sample. Each point was repeated three times; tables below report medians.

The primary A/B used `lists=100`, probes 1/10/20, and 1/12 clients. A separate exploratory sweep used lists 50/100/200 and probes 1/5/10/20 with one repeat and a two-second sample. The sweep is for parameter trends only.

## PG19 A/B

The balanced `lists=100, probes=10` points are:

| Metric | Clients | Recall | P50 ms base / opt | P95 ms base / opt | QPS base / opt | P95 delta | QPS delta |
| --- | --: | --: | --: | --: | --: | --: | --: |
| L2 | 1 | 0.982 | 3.424 / 0.850 | 4.293 / 1.199 | 286.3 / 1123.5 | -72.07% | +292.39% |
| L2 | 12 | 0.982 | 4.146 / 1.178 | 5.056 / 1.499 | 2888.7 / 10189.7 | -70.35% | +252.74% |
| IP | 1 | 0.954 | 3.013 / 0.959 | 3.710 / 1.319 | 326.2 / 1003.1 | -64.45% | +207.46% |
| IP | 12 | 0.954 | 3.552 / 1.303 | 4.342 / 1.664 | 3351.3 / 9147.3 | -61.68% | +172.95% |
| Cosine | 1 | 0.946 | 3.028 / 0.934 | 3.699 / 1.287 | 326.5 / 1031.3 | -65.21% | +215.87% |
| Cosine | 12 | 0.946 | 3.547 / 1.294 | 4.344 / 1.672 | 3351.7 / 9198.2 | -61.51% | +174.44% |

Gate summary over all probes and client counts:

- Result stability: PASS, 18/18 configurations matched and no hash differed.
- Input integrity: PASS, all repetitions had stable hashes/result counts and all A/B points returned the requested 500 rows.
- Recall stability: PASS, worst change was 0.000 percentage points.
- Regression guard: PASS, worst P95 change was -38.31% and worst QPS change was +45.98%.
- Improvement: PASS, best P95 change was -72.98% and best QPS change was +311.50%.

Raw repetitions are in `results/pg19-baseline.csv` and `results/pg19-optimized.csv`.

## Lists and probes

Recall@10 from the optimized exploratory sweep:

| Metric | Lists | probes=1 | probes=5 | probes=10 | probes=20 |
| ------ | ----: | -------: | -------: | --------: | --------: |
| L2     |    50 |    0.152 |    0.352 |     0.834 |     1.000 |
| L2     |   100 |    0.144 |    0.782 |     0.982 |     1.000 |
| L2     |   200 |    0.580 |    0.844 |     0.934 |     0.984 |
| IP     |    50 |    0.378 |    0.686 |     0.876 |     0.982 |
| IP     |   100 |    0.770 |    0.936 |     0.954 |     0.978 |
| IP     |   200 |    0.528 |    0.982 |     1.000 |     1.000 |
| Cosine |    50 |    0.380 |    0.676 |     0.864 |     0.962 |
| Cosine |   100 |    0.776 |    0.930 |     0.946 |     0.974 |
| Cosine |   200 |    0.572 |    0.984 |     1.000 |     1.000 |

For this distribution, the lowest measured probes setting at or above 0.95 Recall@10 was:

| Metric | Lists | Probes | Recall | P95 ms, 1 client | QPS, 1 client |
| ------ | ----: | -----: | -----: | ---------------: | ------------: |
| L2     |    50 |     20 |  1.000 |            6.454 |         184.4 |
| L2     |   100 |     10 |  0.982 |            1.241 |        1101.1 |
| L2     |   200 |     20 |  0.984 |            0.986 |        1446.0 |
| IP     |    50 |     20 |  0.982 |            4.496 |         283.1 |
| IP     |   100 |     10 |  0.954 |            1.308 |        1003.0 |
| IP     |   200 |      5 |  0.982 |            0.585 |        2412.8 |
| Cosine |    50 |     20 |  0.962 |            4.358 |         288.2 |
| Cosine |   100 |     20 |  0.974 |            2.178 |         577.7 |
| Cosine |   200 |      5 |  0.984 |            0.587 |        2391.9 |

`lists=200, probes=1` returned only 485 of the requested 500 IP/Cosine results. Recall uses the fixed denominator `query_count * K`, so missing rows are counted as misses. All `lists=100` A/B points returned 500 rows. Full sweep data is in `results/pg19-optimized-list-sweep.csv`.

The practical starting point for this data is 100 lists and about 10 probes. Higher lists can improve both recall and latency for IP/Cosine, but the L2 result shows that lists alone is not monotonic; probes must be tuned against measured recall for the actual distribution.

## Scan hotspots

`gprofng` sampled 1,000 L2 queries with lists=100 and probes=32 in PostgreSQL single-user mode. Index construction ran outside the profiled process.

| Hotspot | Baseline | Optimized |
| --- | --: | --: |
| Total sampled CPU | 1.061 s | 0.410 s |
| `GetScanItems` inclusive | 55.66% | 100.00% |
| `tuplesort_performsort` / `qsort_ssup` inclusive | 40.57% | absent |
| `tuplesort_puttupleslot` inclusive | 18.87% | absent |
| Buffer read path inclusive | 30.19% | 75.61% |
| Candidate heap sift inclusive | absent | 4.88% |
| Direct L2 distance exclusive | hidden behind fmgr samples | 2.44% |

Baseline time is dominated by materializing every candidate as a tuple and fully sorting it. The optimized path stores compact `{distance,TID}` records, heapifies them in linear time, and pops only as many records as LIMIT needs. After removing tuplesort and fmgr overhead, buffer reads become the dominant remaining cost. Percentages mix inclusive and exclusive samples and should not be summed.

## PG18 baseline

The independent PG18 v0.8.6 baseline at lists=100, probes=10 was:

| Metric | Recall | 1 client P50 / P95 / QPS | 12 clients P50 / P95 / QPS |
| ------ | -----: | -----------------------: | -------------------------: |
| L2     |  0.982 |    3.755 / 4.782 / 261.6 |     4.365 / 5.322 / 2735.1 |
| IP     |  0.954 |    3.130 / 3.895 / 314.1 |     3.672 / 4.550 / 3243.8 |
| Cosine |  0.946 |    3.117 / 3.916 / 314.4 |     3.658 / 4.551 / 3250.6 |

Raw PG18 repetitions are in `results/pg18-baseline.csv`.

## Implementation

The scan optimization consists of:

- direct vector L2-squared and negative-inner-product support calls after a one-time dimension check (Cosine uses the normalized inner-product path);
- a contiguous max-heap for closest-list selection instead of pairingheap nodes;
- compact candidate storage and linear min-heap construction;
- incremental candidate output so LIMIT K does not require a full sort;
- deterministic distance/TID tie ordering;
- a candidate budget of 25% of `work_mem`, with automatic tuplesort fallback;
- complete rescan and iterative-batch state reset;
- optional phase timing and deterministic-build controls for reproducible profiling.

L2, IP, and normalized Cosine all bypass fmgr in the built-in `vector` path; other IVFFlat types retain the generic support-function call while benefiting from the list and candidate heap changes.

## Correctness and regression tests

- SQL regression: 14/14 passed.
- TAP: 49 files, 1,276 tests passed in 215 seconds.
- New TAP coverage includes L2/IP/Cosine, exact results when all lists are probed, the in-memory candidate heap, 64 kB `work_mem` fallback, nested-loop rescans, and dimension mismatch errors.
- Benchmark helpers compile as C11 with `-Wall -Wextra -Werror`; all shell scripts pass `bash -n`.
- Both GNU Make and Meson build the production `vector.so` target without warnings.

## Memory and disk classification

The reported quick matrix is a warm-memory validation: the 108.4 MiB table plus index pair fits in 23 GiB host RAM, follows exact-truth generation, and uses a per-point warmup. It does not claim that every page remained in PostgreSQL's 128 MiB shared buffer pool.

The reproducibility tool also defines, but this report did not execute:

- `memory`: 1,000,000 rows at 768 dimensions, lists 100/500/1000, explicit `pg_prewarm`, 15-second warmup, 60-second samples, three repeats;
- `disk`: 8,000,000 rows at 384 dimensions, lists=1000, controlled PostgreSQL stop, `sync`, and `posix_fadvise(POSIX_FADV_DONTNEED)` over every 1 GiB relation segment before each sample.

Those standard matrices are intentionally left as required release-host follow-up. No 1M/8M result is inferred from the 100k WSL run.

## Reproduction

Build both A/B modules with deterministic index construction:

```bash
git -C /path/to/pgvector-v0.8.6 apply --unidiff-zero \
  /path/to/OpenTenBase/contrib/pgvector/bench/pgvector-0.8.6-deterministic-build.patch
make -C /path/to/pgvector-v0.8.6 clean
make -C /path/to/pgvector-v0.8.6 \
  USE_PGXS=1 PG_CONFIG=/path/to/pg_config \
  PG_CPPFLAGS=-DIVFFLAT_BENCH_SEED=42

make -C /path/to/OpenTenBase-build/contrib/pgvector clean
make -C /path/to/OpenTenBase-build/contrib/pgvector \
  PG_CPPFLAGS=-DIVFFLAT_BENCH_SEED=42
```

Run baseline and optimized clusters with the same environment:

```bash
export PGURI=postgresql://localhost:6543/postgres
contrib/pgvector/bench/run.sh --profile quick --variant baseline \
  --output /results/pg19-baseline --prepare

export PGURI=postgresql://localhost:6544/postgres
contrib/pgvector/bench/run.sh --profile quick --variant optimized \
  --output /results/pg19-optimized --prepare

contrib/pgvector/bench/compare.sh \
  /results/pg19-baseline/results.csv \
  /results/pg19-optimized/results.csv \
  /results/report.md
```

`compare.sh` exits nonzero for malformed input, unstable repetitions, missing configurations, differing Top-K hashes/result counts, Recall@10 drop beyond 0.1 percentage points, regression beyond 3%, or failure to reach a 10% P95/QPS improvement.
