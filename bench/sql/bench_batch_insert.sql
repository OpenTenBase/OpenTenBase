-- =============================================================================
-- bench_batch_insert.sql
-- Batch INSERT benchmark for pgbench.
-- Inserts 100 rows per transaction using generate_series.
-- Each batch has a unique base ID derived from a pgbench variable so batches
-- do not collide across concurrent clients.
-- =============================================================================
\set batch_seed random(1, 100000000)
\set base_id     random(1000000, 990000000)
\set cat_start   random(1, 15)
\set price_base  random(100, 10000)

INSERT INTO bench_items (id, name, category, price, stock, weight, created_at)
SELECT
    :base_id + gs,
    'batch_' || :batch_seed || '_' || gs,
    :cat_start + (gs % 5),                              -- cycle through 5 categories
    (:price_base + (random() * 500)::int)::double precision,
    (random() * 1000)::int,
    (random() * 10 + 0.1)::numeric(6,2)::double precision,
    now()
FROM generate_series(1, 100) AS gs;
