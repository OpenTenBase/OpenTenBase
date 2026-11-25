-- ============================================================
-- Test: Distributed Vector Search FQS Pushdown
-- Description: Verify that ORDER BY distance LIMIT K queries
--              are pushed down to DNs via FQS and CN correctly
--              merge-sorts results with SimpleSort + Limit.
-- ============================================================

-- Setup: Create extension and test table
CREATE EXTENSION IF NOT EXISTS vector;

-- Create distributed table with vector column
CREATE TABLE vec_items (
    id SERIAL PRIMARY KEY,
    embedding vector(3),
    metadata TEXT
) DISTRIBUTE BY HASH(id);

-- Insert test data
INSERT INTO vec_items (embedding, metadata) VALUES
    ('[1,0,0]', 'item1'),
    ('[0,1,0]', 'item2'),
    ('[0,0,1]', 'item3'),
    ('[1,1,0]', 'item4'),
    ('[1,0,1]', 'item5'),
    ('[0,1,1]', 'item6'),
    ('[1,1,1]', 'item7'),
    ('[0.5,0.5,0.5]', 'item8');

-- ============================================================
-- Test 1: L2 distance operator (<->) - most common vector search
-- ============================================================
-- Should use FQS pushdown with SimpleSort + Limit
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <-> '[1,0,0]'
LIMIT 3;

-- Test 1b: Verify FQS pushdown via EXPLAIN
EXPLAIN (COSTS OFF)
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <-> '[1,0,0]'
LIMIT 3;

-- ============================================================
-- Test 2: Cosine distance operator (<=)
-- ============================================================
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <=> '[1,0,0]'
LIMIT 3;

-- Test 2b: Verify FQS pushdown via EXPLAIN
EXPLAIN (COSTS OFF)
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <=> '[1,0,0]'
LIMIT 3;

-- ============================================================
-- Test 3: Inner product operator (<#>)
-- ============================================================
-- Note: <#> returns negative inner product, so ORDER BY <#> ASC
-- gives highest inner product (most similar)
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <#> '[1,0,0]'
LIMIT 3;

-- Test 3b: Verify FQS pushdown via EXPLAIN
EXPLAIN (COSTS OFF)
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <#> '[1,0,0]'
LIMIT 3;

-- ============================================================
-- Test 4: L1 distance operator (<+>)
-- ============================================================
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <+> '[1,0,0]'
LIMIT 3;

-- Test 4b: Verify FQS pushdown via EXPLAIN
EXPLAIN (COSTS OFF)
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <+> '[1,0,0]'
LIMIT 3;

-- ============================================================
-- Test 5: Function form - l2_distance()
-- ============================================================
SELECT id, embedding, metadata
FROM vec_items
ORDER BY l2_distance(embedding, '[1,0,0]')
LIMIT 3;

-- Test 5b: Verify FQS pushdown via EXPLAIN
EXPLAIN (COSTS OFF)
SELECT id, embedding, metadata
FROM vec_items
ORDER BY l2_distance(embedding, '[1,0,0]')
LIMIT 3;

-- ============================================================
-- Test 6: Function form - cosine_distance()
-- ============================================================
SELECT id, embedding, metadata
FROM vec_items
ORDER BY cosine_distance(embedding, '[1,0,0]')
LIMIT 3;

-- ============================================================
-- Test 7: Function form - l1_distance()
-- ============================================================
SELECT id, embedding, metadata
FROM vec_items
ORDER BY l1_distance(embedding, '[1,0,0]')
LIMIT 3;

-- ============================================================
-- Test 8: Function form - vector_negative_inner_product()
-- ============================================================
SELECT id, embedding, metadata
FROM vec_items
ORDER BY vector_negative_inner_product(embedding, '[1,0,0]')
LIMIT 3;

-- ============================================================
-- Test 9: ORDER BY distance without LIMIT - should NOT trigger
--         vector search FQS (needs LIMIT for TopK optimization)
-- ============================================================
-- This should fall back to single-node processing
EXPLAIN (COSTS OFF)
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <-> '[1,0,0]';

-- ============================================================
-- Test 10: LIMIT without ORDER BY distance - should NOT trigger
--          vector search FQS
-- ============================================================
EXPLAIN (COSTS OFF)
SELECT id, embedding, metadata
FROM vec_items
LIMIT 3;

-- ============================================================
-- Test 11: ORDER BY non-distance column with LIMIT - should NOT
--          trigger vector search FQS
-- ============================================================
EXPLAIN (COSTS OFF)
SELECT id, embedding, metadata
FROM vec_items
ORDER BY id
LIMIT 3;

-- ============================================================
-- Test 12: Project distance value in SELECT list
-- ============================================================
SELECT id, embedding <-> '[1,0,0]' AS distance, metadata
FROM vec_items
ORDER BY embedding <-> '[1,0,0]'
LIMIT 3;

-- ============================================================
-- Test 13: OFFSET + LIMIT - currently NOT supported for FQS
-- pushdown, should fall back to single-node processing
-- ============================================================
EXPLAIN (COSTS OFF)
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <-> '[1,0,0]'
LIMIT 3 OFFSET 2;

-- ============================================================
-- Test 14: Multiple sort keys (distance + regular column)
-- ============================================================
-- This should still trigger vector search FQS since the first
-- sort key is a distance operator
SELECT id, embedding, metadata
FROM vec_items
ORDER BY embedding <-> '[1,0,0]', id
LIMIT 3;

-- Cleanup
DROP TABLE vec_items;