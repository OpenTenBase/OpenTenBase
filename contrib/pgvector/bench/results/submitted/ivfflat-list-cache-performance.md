# IVFFlat list-directory cache performance report

[中文报告](ivfflat-list-cache-performance_ZH.md)

Date: 2026-09-11

## Summary

This experiment compares the type-aware distance-dispatch baseline at commit
with an IVFFlat list-directory cache candidate. The candidate
caches immutable list centers and entry start pages in the backend-local
relation cache (`rd_amcache`). It does not cache query results or heap TIDs.

Across the four measured configurations, median average latency decreased by
0.95% to 2.97% and median TPS increased by 0.97% to 3.06%. Recall@10, returned
item counts, matched item counts, and failed transaction counts were unchanged
in all 12 baseline/candidate pairs.

## Compared builds

| Variant | Source | vector.so SHA-256 |
| --- | --- | --- |
| Baseline | `0b16fb5bd0` | `c4b17e53576e25958ab152f9d7550c41f534ce1eaf077cd8fcc5c2f2953de67d` |
| Candidate | uncommitted list-directory cache candidate | `2591f41c656b3f1c4924a3a8fffaefc05e85f6fe3e0d12a1b1500e1449733ef8` |

Both modules were built with the same PostgreSQL installation and `COPT=-O2`.
The benchmark reused the same physical IVFFlat index across the two variants.

## Workload

| Parameter | Value |
| --- | --- |
| PostgreSQL | 19beta3 |
| pgvector | 0.8.6 |
| Data type | `vector` |
| Rows | 100,000 |
| Dimensions | 128 |
| Query vectors | 1,000 |
| Recall queries | 1,000 |
| K | 10 |
| Lists | 1,000 |
| Probes | 1, 10 |
| Clients | 1, 12 |
| Jobs | 1 |
| Warmup | 5 seconds per repetition |
| Measurement | 15 seconds per repetition |
| Repetitions | 3 |
| JIT | off |
| Sequential scan | disabled |
| work_mem | 64 MB |
| Dataset seed | 20260911 |
| Query seed | 20260912 |

## Median results

Each row reports the median of three repetitions.

| Probes | Clients | Recall@10 | Baseline latency ms | Candidate latency ms | Latency change | Baseline TPS | Candidate TPS | TPS change |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 0.0862 | 0.525 | 0.518 | -1.33% | 1906.16 | 1932.30 | +1.37% |
| 1 | 12 | 0.0862 | 0.797 | 0.782 | -1.88% | 15056.24 | 15336.64 | +1.86% |
| 10 | 1 | 0.4588 | 2.124 | 2.061 | -2.97% | 470.72 | 485.11 | +3.06% |
| 10 | 12 | 0.4588 | 2.830 | 2.803 | -0.95% | 4240.54 | 4281.78 | +0.97% |

## Correctness gate

All 12 repetition-level comparisons passed:

- comparison status: 12/12 matched;
- Recall@10 change: 0.000000 in every row;
- failed transactions: 0 on both variants;
- returned items: 10,000 on both variants;
- matched items: identical on both variants.

The implementation also passed the existing IVFFlat vector, halfvec, and bit
SQL regression tests. Manual same-session checks returned 100 rows before and
after cache reuse, after an insert, and after `REINDEX`.

## Why the gain is modest

The cache removes repeated buffer lookup, pin, shared-lock, page-item traversal,
and unlock operations for IVFFlat list-directory pages. For 1,000 lists at 128
dimensions, the directory is roughly 65 to 70 index pages and the backend-local
cache is about 0.5 MiB.

The optimization does not remove list-center distance calculations, candidate
page reads, distance calculations for candidates, tuple materialization, or
tuplesort. Those operations continue to dominate when probes increases. This
explains why the measured whole-query improvement is in the 1% to 3% range.

## Variance and limitations

Individual 15-second repetitions ranged from a 6.02% latency regression to an
8.64% improvement. These extremes did not persist across all three repetitions;
the median result is therefore used instead of the best run.

Current limitations:

- only L2 was measured;
- samples ran for 15 seconds rather than the final 60-second gate;
- Recall@10 was low at probes 1 and 10, so this is primarily a scan-overhead
  experiment rather than a high-recall production configuration;
- no phase-level `GetScanLists` timing was captured;
- no cold-cache, 1M-row, halfvec, or lists sweep was run;
- the candidate did not yet have a commit hash during measurement.

## Conclusion

The list-directory cache passes the correctness gate and shows a consistent
positive median result in every measured configuration. The most stable and
largest median benefit was at probes 10 with one client: latency decreased by
2.97% and TPS increased by 3.06%.

Before making a final performance claim, rerun the representative configurations
for 60 seconds with three repetitions and add probes 20. A result around 1% to
3% should be reported as a targeted reduction in IVFFlat list-selection
overhead, not as a broad end-to-end acceleration.
