-- Inner Product distance workload for pgbench.

-- Example:
--
-- pgbench \
--     -n \
--     -M simple \
--     -c 1 \
--     -j 1 \
--     -T 10 \
--     -D query_count=100 \
--     -D top_k=10 \
--     -D probes=2 \
--     -f sql/workload_ip.sql \
--     pgvector_bench

-- Select a random query vector for the current pgbench transaction.

\set query_id random(1, :query_count)


BEGIN;

SET LOCAL ivfflat.probes = :probes;

-- Prefer the IVFFlat index over a sequential scan.

SET LOCAL enable_seqscan = off;

SET LOCAL enable_indexscan = on;

-- Disable bitmap scans because they are not the target access path here.
SET LOCAL enable_bitmapscan = off;

-- Execute one inner-product nearest-neighbor query for the selected query vector.
-- The <#> operator returns the negative inner product.
-- Ascending order therefore returns vectors with the largest inner product first.
-- The ORDER BY expression must match the IVFFlat operator class.

SELECT
    neighbor.item_id,
    neighbor.distance
FROM vector_bench.queries AS query_vector
CROSS JOIN LATERAL
(
    SELECT
        item.id AS item_id,
        item.embedding <#> query_vector.embedding AS distance
    FROM vector_bench.items AS item
    ORDER BY item.embedding <#> query_vector.embedding
    LIMIT :top_k
) AS neighbor
WHERE query_vector.id = :query_id;


COMMIT;
