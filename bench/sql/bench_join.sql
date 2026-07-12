-- =============================================================================
-- bench_join.sql
-- JOIN query benchmark for pgbench.
-- Joins bench_transactions with bench_accounts to retrieve enriched transaction
-- history for a randomly selected "from" account.
-- =============================================================================
\set aid random(1, 100000)

SELECT t.id          AS txn_id,
       t.amount,
       t.txn_type,
       t.status,
       t.ts,
       a_src.name     AS from_account_name,
       a_src.tier     AS from_tier,
       a_dst.name     AS to_account_name,
       a_dst.tier     AS to_tier
FROM bench_transactions t
JOIN bench_accounts a_src ON t.from_account = a_src.id
JOIN bench_accounts a_dst ON t.to_account   = a_dst.id
WHERE t.from_account = :aid
ORDER BY t.ts DESC
LIMIT 50;
