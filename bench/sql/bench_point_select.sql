-- =============================================================================
-- bench_point_select.sql
-- Point-lookup (PK select) benchmark for pgbench.
-- Fetches a single row from bench_accounts by primary key id.
-- Randomizes the target id across the full range 1..100000.
-- =============================================================================
\set aid random(1, 100000)

SELECT id, name, balance, tier, region, created_at
FROM bench_accounts
WHERE id = :aid;
