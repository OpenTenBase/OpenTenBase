-- =============================================================================
-- OpenTenBase / PostgreSQL Centralized Benchmark Suite
-- data_load.sql: Populate benchmark tables with realistic data
-- =============================================================================
-- Target volumes:
--   bench_accounts      : 100,000 rows
--   bench_transactions  : 1,000,000 rows
--   bench_items         : 10,000 rows
--
-- All data uses pseudo-random distributions to avoid unrealistic all-sequential
-- patterns.  We use md5() on a seed for deterministic repeatability.
-- =============================================================================

\set ON_ERROR_STOP on
\set ACCOUNT_COUNT  100000
\set TXN_COUNT      1000000
\set ITEM_COUNT     10000

\timing on

-- ---------------------------------------------------------------------------
-- 1. Populate bench_accounts (100K rows)
-- ---------------------------------------------------------------------------
\echo '==> Loading bench_accounts (100,000 rows) ...'

-- We use generate_series for the id but randomize all attribute columns.
INSERT INTO bench_accounts (id, name, balance, tier, region, created_at)
SELECT
    gs,
    -- Realistic name: "user_" + hex hash to get varied lengths / patterns
    'user_' || md5(gs::text || 'saltA'),
    -- Balance: normal-like distribution around 5000 with stddev 3000,
    -- clamped to be non-negative, plus a long tail of high-value accounts (Pareto).
    CASE
        WHEN random() < 0.01 THEN (random() * 900000 + 100000)::numeric(12,2)::double precision   -- 1% VIPs with large balances
        WHEN random() < 0.20 THEN (random() * 50000  + 5000)::numeric(12,2)::double precision     -- 20% medium
        ELSE GREATEST(0.0, (5000 + (random() - 0.5) * 6000)::numeric(12,2)::double precision)     -- 79% around mean
    END,
    -- tier: Pareto-like — most are tier 1, fewer tier 2, very few tier 3
    CASE
        WHEN random() < 0.02 THEN 3
        WHEN random() < 0.15 THEN 2
        ELSE 1
    END,
    -- region: 1..100 with slight skew (some regions larger)
    (1 + (random() * random() * 100))::int,
    -- created_at: spread over the last 2 years with a burst in recent 6 months
    now() - (random() * interval '730 days')
        - CASE WHEN random() < 0.4 THEN 0 ELSE random() * interval '180 days' END
FROM generate_series(1, :ACCOUNT_COUNT) AS gs;

\echo '    bench_accounts loaded.'

-- ---------------------------------------------------------------------------
-- 2. Populate bench_items (10K rows)
-- ---------------------------------------------------------------------------
\echo '==> Loading bench_items (10,000 rows) ...'

-- Categories 1..20 with realistic product names generated via md5 prefix.
INSERT INTO bench_items (id, name, category, price, stock, weight, created_at)
SELECT
    gs,
    -- Slightly more readable: "item_" + first 8 chars of a hash
    'item_' || left(md5(gs::text || 'saltI'), 8),
    -- category: 1..20, some categories have more items (power-law)
    (1 + (random() * random() * 20))::int,
    -- price: log-normal-like distribution, most items $0.99..$500, tail up to $10K
    CASE
        WHEN random() < 0.02 THEN (random() * 9000 + 1000)::numeric(10,2)::double precision    -- 2% luxury items
        WHEN random() < 0.30 THEN (random() * 490  + 10)::numeric(10,2)::double precision      -- 30% mid-range
        ELSE (random() * 49 + 0.99)::numeric(10,2)::double precision                            -- 68% cheap
    END,
    -- stock: most items well-stocked, some out-of-stock (realistic)
    CASE
        WHEN random() < 0.05 THEN 0                         -- 5% out of stock
        WHEN random() < 0.10 THEN (random() * 10)::int      -- 10% low stock
        ELSE (random() * 500 + 10)::int                     -- 85% normal stock
    END,
    -- weight: most items light (<5kg), some heavy
    CASE
        WHEN random() < 0.05 THEN (random() * 50 + 10)::numeric(6,2)::double precision
        ELSE (random() * 5 + 0.1)::numeric(6,2)::double precision
    END,
    -- created_at: uniform over last 3 years
    now() - (random() * interval '1095 days')
FROM generate_series(1, :ITEM_COUNT) AS gs;

\echo '    bench_items loaded.'

-- ---------------------------------------------------------------------------
-- 3. Populate bench_transactions (1M rows)
-- ---------------------------------------------------------------------------
\echo '==> Loading bench_transactions (1,000,000 rows) ...'
\echo '    (This may take 1-3 minutes depending on hardware)'

-- Realistic transaction patterns:
--   70% of txns involve "hot" accounts (top 20% of accounts)
--   amount distribution is log-normal-like (many small, few large)
--   status is mostly "completed" (95%) with some pending/failed
--   txn_type weights: 55% transfer, 20% deposit, 15% withdrawal, 10% refund
--   timestamps cluster around business hours for recent data, spread for older

INSERT INTO bench_transactions (from_account, to_account, amount, status, txn_type, ts, description)
SELECT
    -- from_account: Pareto — most transactions originate from a small set of accounts
    CASE
        WHEN random() < 0.70 THEN (1 + (random() * random() * :ACCOUNT_COUNT * 0.20))::int              -- hot accounts (top 20%)
        ELSE (1 + (random() * :ACCOUNT_COUNT))::int                                                      -- remaining accounts
    END,
    -- to_account: different from from_account (no self-transfers)
    CASE
        WHEN random() < 0.70 THEN (1 + (random() * random() * :ACCOUNT_COUNT * 0.20))::int
        ELSE (1 + (random() * :ACCOUNT_COUNT))::int
    END,
    -- amount: log-normal-like — many micro-transactions, few large ones
    CASE
        WHEN random() < 0.60 THEN (random() * 100 + 0.01)::numeric(10,2)::double precision              -- 60% under $100
        WHEN random() < 0.30 THEN (random() * 900 + 100)::numeric(10,2)::double precision               -- 30% $100-$1000
        WHEN random() < 0.08 THEN (random() * 9000 + 1000)::numeric(10,2)::double precision             -- 8% $1K-$10K
        ELSE (random() * 90000 + 10000)::numeric(12,2)::double precision                                -- 2% >$10K
    END,
    -- status: mostly completed
    CASE
        WHEN random() < 0.94 THEN 0   -- completed
        WHEN random() < 0.50 THEN 1   -- pending
        ELSE 2                         -- failed
    END,
    -- txn_type with realistic weights
    CASE
        WHEN random() < 0.55 THEN 'transfer'
        WHEN random() < 0.75 THEN 'deposit'
        WHEN random() < 0.90 THEN 'withdrawal'
        ELSE 'refund'
    END,
    -- timestamps: last 365 days, with more density in recent 3 months and business-hour bias
    now()
        - (random() * random() * interval '365 days')       -- skew toward recent
        - CASE
            -- Add business-hour bias: shift to daytime hours (8am-6pm UTC-ish)
            WHEN random() < 0.70 THEN interval '0 hours'
            ELSE (random() * interval '14 hours')
          END
        - CASE
            -- Weekend reduction: fewer txns on Saturday/Sunday
            WHEN extract(dow from now() - random() * interval '365 days') IN (0, 6)
                 AND random() < 0.60
            THEN interval '2 days'
            ELSE interval '0 hours'
          END,
    -- description: empty for most, short text for some
    CASE
        WHEN random() < 0.20 THEN 'txn_ref_' || left(md5(random()::text), 6)
        ELSE ''
    END
FROM generate_series(1, :TXN_COUNT) AS gs;

\echo '    bench_transactions loaded.'

-- ---------------------------------------------------------------------------
-- 4. Fix self-referencing transactions (where from = to)
-- ---------------------------------------------------------------------------
\echo '==> Fixing self-referencing transactions ...'
UPDATE bench_transactions
SET to_account = CASE WHEN to_account >= :ACCOUNT_COUNT THEN 1 ELSE to_account + 1 END
WHERE from_account = to_account;

-- ---------------------------------------------------------------------------
-- 5. Update table statistics
-- ---------------------------------------------------------------------------
\echo '==> Running ANALYZE to update statistics ...'
ANALYZE bench_accounts;
ANALYZE bench_transactions;
ANALYZE bench_items;

-- ---------------------------------------------------------------------------
-- 6. Print summary
-- ---------------------------------------------------------------------------
\echo ''
\echo '========================================'
\echo ' Data Load Summary'
\echo '========================================'
SELECT 'bench_accounts'     AS table_name, count(*) AS row_count FROM bench_accounts
UNION ALL
SELECT 'bench_transactions', count(*) FROM bench_transactions
UNION ALL
SELECT 'bench_items',       count(*) FROM bench_items
ORDER BY table_name;

\echo ''
\echo '==> Data load complete.'
\timing off
