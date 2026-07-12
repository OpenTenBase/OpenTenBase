-- =============================================================================
-- OpenTenBase / PostgreSQL Centralized Benchmark Suite
-- setup.sql: Schema definition for benchmark tables
-- =============================================================================

-- Accounts table: simulates a bank/user account ledger
-- Each row carries a name, balance, and timestamps for realistic OLTP patterns.
DROP TABLE IF EXISTS bench_accounts CASCADE;
CREATE TABLE bench_accounts (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    balance         DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    tier            INTEGER NOT NULL DEFAULT 1,           -- 1=basic, 2=premium, 3=vip
    region          INTEGER NOT NULL DEFAULT 1,           -- 1..100 region codes
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for common access patterns
CREATE INDEX idx_accounts_name    ON bench_accounts (name);
CREATE INDEX idx_accounts_tier    ON bench_accounts (tier);
CREATE INDEX idx_accounts_region  ON bench_accounts (region);
CREATE INDEX idx_accounts_created ON bench_accounts (created_at);

-- Transactions table: a journal of money movements between accounts
-- Realistic pattern: most transactions reference a small set of "hot" accounts;
-- the balance distribution follows a Pareto (80/20) shape.
DROP TABLE IF EXISTS bench_transactions CASCADE;
CREATE TABLE bench_transactions (
    id              BIGSERIAL PRIMARY KEY,
    from_account    INTEGER NOT NULL,
    to_account      INTEGER NOT NULL,
    amount          DOUBLE PRECISION NOT NULL,
    status          INTEGER NOT NULL DEFAULT 0,           -- 0=completed, 1=pending, 2=failed
    txn_type        TEXT NOT NULL DEFAULT 'transfer',     -- transfer, deposit, withdrawal, refund
    ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    description     TEXT DEFAULT ''
);

-- Composite indexes for the most frequent query shapes
CREATE INDEX idx_txn_from        ON bench_transactions (from_account);
CREATE INDEX idx_txn_to          ON bench_transactions (to_account);
CREATE INDEX idx_txn_ts          ON bench_transactions (ts);
CREATE INDEX idx_txn_status      ON bench_transactions (status);
CREATE INDEX idx_txn_from_ts     ON bench_transactions (from_account, ts);
CREATE INDEX idx_txn_to_ts       ON bench_transactions (to_account, ts);
CREATE INDEX idx_txn_amount      ON bench_transactions (amount);

-- Items table: a product / inventory catalog
DROP TABLE IF EXISTS bench_items CASCADE;
CREATE TABLE bench_items (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    category        INTEGER NOT NULL,                     -- 1..20 categories
    price           DOUBLE PRECISION NOT NULL,
    stock           INTEGER NOT NULL DEFAULT 0,
    weight          DOUBLE PRECISION DEFAULT 1.0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_items_category ON bench_items (category);
CREATE INDEX idx_items_price    ON bench_items (price);
CREATE INDEX idx_items_name     ON bench_items (name);

-- Summary log table for benchmark metadata (optional, useful for self-documenting runs)
DROP TABLE IF EXISTS bench_run_log CASCADE;
CREATE TABLE bench_run_log (
    run_id          SERIAL PRIMARY KEY,
    scenario        TEXT NOT NULL,
    variant         TEXT NOT NULL,
    clients         INTEGER,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    tps             DOUBLE PRECISION,
    avg_latency_ms  DOUBLE PRECISION,
    notes           TEXT
);

\echo '==> Schema setup complete.'
\echo '    bench_accounts       : PK + 4 secondary indexes'
\echo '    bench_transactions   : PK + 7 secondary indexes'
\echo '    bench_items          : PK + 3 secondary indexes'
\echo '    bench_run_log        : metadata table'
