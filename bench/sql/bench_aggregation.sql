-- =============================================================================
-- bench_aggregation.sql
-- Aggregation (SUM/AVG/COUNT with GROUP BY) benchmark for pgbench.
-- Runs a multi-column GROUP BY on bench_accounts with summarization.
-- Compatible with pgbench on PostgreSQL 10+.
-- =============================================================================

-- Variant 1 — uncomment the block you want to benchmark, or use separate files.
-- The runner script can create copies or use --variable to switch.

-- Default: Tier-by-region aggregation with SUM/AVG/COUNT (realistic BI query)
SELECT tier, region,
       count(*)        AS account_count,
       sum(balance)    AS total_balance,
       avg(balance)    AS avg_balance,
       min(balance)    AS min_balance,
       max(balance)    AS max_balance
FROM bench_accounts
GROUP BY tier, region
ORDER BY tier, region;
