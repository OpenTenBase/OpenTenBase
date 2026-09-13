-- Filtered inner-product workload for pgbench.

-- Select a random query vector for the current transaction.
\set query_id random(1, :query_count)

-- Keep all scan settings active for the nearest-neighbor query.
BEGIN;

SET LOCAL ivfflat.probes = :probes;
SET LOCAL ivfflat.iterative_scan = :iterative_scan;
SET LOCAL ivfflat.max_probes = :max_probes;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_indexscan = on;
SET LOCAL enable_bitmapscan = off;

-- The <#> operator returns the negative inner product.
-- Execute a filtered inner-product nearest-neighbor query.
SELECT
    item.id AS item_id,
    item.embedding <#> query_vector.embedding AS distance
FROM vector_bench.items AS item
CROSS JOIN vector_bench.queries AS query_vector
WHERE query_vector.id = :query_id
    AND item.category_id < :filter_limit
ORDER BY item.embedding <#> query_vector.embedding
LIMIT :top_k;

COMMIT;
