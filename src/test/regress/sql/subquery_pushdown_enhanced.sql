-- ============================================================
-- A-level Subquery Pushdown Enhancement Test Cases
-- Tests for: P1 const_subquery + multi-DN merge,
--            P2 replicated subquery, P3 group subquery multi-DN
-- ============================================================

-- Setup: Create test tables
-- Distributed table (hash on id)
CREATE TABLE subq_dist (id INT PRIMARY KEY, name VARCHAR(50), value INT);
-- Replicated table
CREATE TABLE subq_repl (id INT PRIMARY KEY, code VARCHAR(20), description TEXT);

-- Insert test data
INSERT INTO subq_dist SELECT generate_series(1, 100), 'name_' || generate_series(1, 100), (random() * 100)::INT;
INSERT INTO subq_repl SELECT generate_series(1, 50), 'code_' || generate_series(1, 50), 'desc_' || generate_series(1, 50);

-- ============================================================
-- P1: const_subquery SubLink enhancement
-- A const subquery (no external references) should be compatible
-- with any main query exec_nodes
-- ============================================================

-- Test 1: SubLink with const subquery (no table reference)
-- This should be FQS-shippable since the subquery is constant
SELECT * FROM subq_dist WHERE value > (SELECT 50);

-- Test 2: SubLink with const subquery (aggregation on replicated table)
-- Replicated table scan produces same result on all DNs -> const_subquery
SELECT * FROM subq_dist WHERE value > (SELECT AVG(value) FROM subq_repl);

-- Test 3: EXISTS with const subquery
SELECT * FROM subq_dist WHERE EXISTS (SELECT 1 FROM subq_repl WHERE id < 10);

-- Test 4: IN with const subquery
SELECT * FROM subq_dist WHERE id IN (SELECT id FROM subq_repl WHERE id < 20);

-- ============================================================
-- P2: Replicated subquery in RTE_SUBQUERY
-- A replicated subquery should be shippable to any DN
-- ============================================================

-- Test 5: Subquery in FROM clause referencing replicated table
SELECT d.id, d.name, r.description
FROM subq_dist d,
     (SELECT id, description FROM subq_repl WHERE id < 30) r
WHERE d.id = r.id;

-- Test 6: Subquery in FROM clause with replicated table aggregation
SELECT d.id, d.name, sub.avg_val
FROM subq_dist d,
     (SELECT AVG(value) as avg_val FROM subq_repl) sub
WHERE d.value > sub.avg_val;

-- Test 7: CTE with replicated table
WITH repl_cte AS (SELECT id, code FROM subq_repl WHERE id < 20)
SELECT d.* FROM subq_dist d JOIN repl_cte r ON d.id = r.id;

-- ============================================================
-- P3: Group subquery multi-DN merge
-- GROUP BY with subquery source should support multi-DN merge
-- ============================================================

-- Test 8: GROUP BY with distributed table subquery (single DN)
SELECT sub.value_cat, COUNT(*)
FROM (SELECT id, CASE WHEN value < 30 THEN 'low' WHEN value < 70 THEN 'mid' ELSE 'high' END as value_cat
      FROM subq_dist WHERE id < 50) sub
GROUP BY sub.value_cat;

-- Test 9: GROUP BY with subquery joining distributed and replicated
SELECT sub.name_prefix, SUM(sub.cnt)
FROM (SELECT LEFT(name, 4) as name_prefix, COUNT(*) as cnt
      FROM subq_dist d JOIN subq_repl r ON d.id = r.id
      GROUP BY LEFT(name, 4)) sub
GROUP BY sub.name_prefix;

-- ============================================================
-- Combined tests: Multiple enhancements working together
-- ============================================================

-- Test 10: SubLink const + replicated FROM subquery
SELECT d.id, d.name
FROM subq_dist d,
     (SELECT AVG(value) as avg_val FROM subq_repl) sub
WHERE d.value > sub.avg_val
  AND d.id IN (SELECT id FROM subq_repl WHERE id < 25);

-- Test 11: Nested subqueries with replicated inner
SELECT * FROM subq_dist
WHERE value > (SELECT MIN(value) FROM subq_repl)
  AND id IN (SELECT id FROM subq_dist WHERE value > (SELECT 40));

-- Test 12: CTE + SubLink with const subquery
WITH dist_summary AS (
    SELECT id, name, value FROM subq_dist WHERE value > (SELECT 50)
)
SELECT * FROM dist_summary WHERE id < 80;

-- ============================================================
-- EXPLAIN tests: Verify FQS pushdown
-- ============================================================

-- Test 13: EXPLAIN - const subquery should show FQS pushdown
EXPLAIN (COSTS OFF) SELECT * FROM subq_dist WHERE value > (SELECT 50);

-- Test 14: EXPLAIN - replicated subquery in FROM should show FQS pushdown
EXPLAIN (COSTS OFF) SELECT d.id FROM subq_dist d, (SELECT id FROM subq_repl WHERE id < 10) r WHERE d.id = r.id;

-- Cleanup
DROP TABLE IF EXISTS subq_dist;
DROP TABLE IF EXISTS subq_repl;