-- =============================================================================
-- bench_aggregation_txn.sql
-- Alternative aggregation query: transaction summary by status and date range.
-- =============================================================================
SELECT status,
       count(*)      AS txn_count,
       sum(amount)   AS total_amount,
       avg(amount)   AS avg_amount,
       min(amount)   AS min_amount,
       max(amount)   AS max_amount
FROM bench_transactions
WHERE ts >= now() - interval '60 days'
GROUP BY status
ORDER BY status;
