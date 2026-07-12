-- =============================================================================
-- bench_mixed.sql
-- Concurrent mixed-workload benchmark for pgbench.
--
-- Each invocation runs exactly one SQL statement chosen by the pgbench
-- random() function embedded in a helper query.  This works on pgbench 10+
-- because all statements in the script are executed, but only ONE of them
-- produces a meaningful result — the others become cheap no-ops that SELECT
-- a constant from the already-chosen branch.
--
-- Strategy:
--   1. \set picks a random number 1..100
--   2. A "dispatch" SELECT evaluates which branch to take and stores the
--      decision in a pgbench variable via \gset.
--   3. Each branch query is guarded by a WHERE clause that references the
--      decision variable, so only one query does real work.
--
-- Weight distribution (matches realistic OLTP mix):
--   Point lookup (read account)     : 40%
--   Point lookup (read item)        : 20%
--   Single insert (transaction)     : 15%
--   Small aggregation               : 10%
--   Update account balance          : 10%
--   Two-table join                  :  5%
-- =============================================================================

-- Step 1: Pick a random branch (1..100)
\set branch random(1, 100)

-- Step 2: Evaluate which query to run; store boolean flags via \gset
-- We use a trick: each branch runs a tiny SELECT that stores its flag.
-- The flag is 1 if this is the active branch, 0 otherwise.

SELECT CASE WHEN :branch <= 40 THEN 1 ELSE 0 END AS do_read \gset
SELECT CASE WHEN :branch > 40 AND :branch <= 60 THEN 1 ELSE 0 END AS do_item \gset
SELECT CASE WHEN :branch > 60 AND :branch <= 75 THEN 1 ELSE 0 END AS do_insert \gset
SELECT CASE WHEN :branch > 75 AND :branch <= 85 THEN 1 ELSE 0 END AS do_agg \gset
SELECT CASE WHEN :branch > 85 AND :branch <= 95 THEN 1 ELSE 0 END AS do_update \gset
SELECT CASE WHEN :branch > 95 THEN 1 ELSE 0 END AS do_join \gset

-- =========================================================================
-- Query 1 (40%): Point lookup — read one account by PK
-- =========================================================================
\set aid random(1, 100000)
SELECT id, name, balance, tier, region
FROM bench_accounts
WHERE id = :aid AND :do_read = 1;

-- =========================================================================
-- Query 2 (20%): Point lookup — read one item by PK
-- =========================================================================
\set iid random(1, 10000)
SELECT id, name, category, price, stock
FROM bench_items
WHERE id = :iid AND :do_item = 1;

-- =========================================================================
-- Query 3 (15%): Single-row INSERT into bench_transactions
-- =========================================================================
\set from_acc random(1, 100000)
\set to_acc   random(1, 100000)
\set amt_int  random(1, 999999)
\set st       random(1, 100)
\set tp       random(1, 100)

-- We use a dummy INSERT ... SELECT pattern: insert zero rows when do_insert=0
-- by using a WHERE false clause.  When do_insert=1, insert normally.
INSERT INTO bench_transactions (from_account, to_account, amount, status, txn_type, ts, description)
SELECT :from_acc, :to_acc,
       (:amt_int / 100.0)::double precision,
       CASE WHEN :st <= 94 THEN 0 WHEN :st <= 97 THEN 1 ELSE 2 END,
       CASE WHEN :tp <= 55 THEN 'transfer'
            WHEN :tp <= 75 THEN 'deposit'
            WHEN :tp <= 90 THEN 'withdrawal'
            ELSE 'refund'
       END,
       clock_timestamp(),
       CASE WHEN random() < 0.2 THEN 'desc_' || left(md5(random()::text), 6) ELSE '' END
WHERE :do_insert = 1;

-- =========================================================================
-- Query 4 (10%): Aggregation — transaction count/sum by status (30-day window)
-- =========================================================================
SELECT status, count(*), sum(amount), avg(amount)
FROM bench_transactions
WHERE ts >= now() - interval '30 days' AND :do_agg = 1
GROUP BY status
ORDER BY status;

-- =========================================================================
-- Query 5 (10%): UPDATE — adjust an account balance (e.g., apply interest)
-- =========================================================================
\set upd_aid random(1, 100000)
\set delta  random(-500, 500)
UPDATE bench_accounts
SET balance = balance + (:delta / 100.0)::double precision
WHERE id = :upd_aid AND :do_update = 1;

-- =========================================================================
-- Query 6 (5%): Two-table JOIN — recent transactions for a random account
-- =========================================================================
\set jid random(1, 100000)
SELECT t.id, t.amount, t.txn_type, t.ts,
       a_dst.name AS to_acct_name
FROM bench_transactions t
JOIN bench_accounts a_dst ON t.to_account = a_dst.id
WHERE t.from_account = :jid AND :do_join = 1
ORDER BY t.ts DESC
LIMIT 10;
