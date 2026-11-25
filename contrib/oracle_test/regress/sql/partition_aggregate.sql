--
-- 分区聚合
-- 在分区表上测试分区聚合
--
-- 注意:为了确保计划的稳定性，最好让这个测试中的任何一个分区表的分区都具有不同的行数
--

-- 启用分区聚合，默认情况下禁用该聚合。
SET enable_partitionwise_aggregate TO true;
-- 启用分区连接，默认情况下禁用。
SET enable_partitionwise_join TO true;
-- 禁用并行计划。
SET max_parallel_workers_per_gather TO 0;
-- 禁用增量排序，这可能会由于模糊因素而影响所选计划。
SET enable_incremental_sort TO off;

--
-- 测试 LIST 分区表
--
CREATE TABLE pagg_tab (a int, b int, c text, d int) PARTITION BY LIST(c);
CREATE TABLE pagg_tab_p1 PARTITION OF pagg_tab FOR VALUES IN ('0000', '0001', '0002', '0003', '0004');
CREATE TABLE pagg_tab_p2 PARTITION OF pagg_tab FOR VALUES IN ('0005', '0006', '0007', '0008');
CREATE TABLE pagg_tab_p3 PARTITION OF pagg_tab FOR VALUES IN ('0009', '0010', '0011');
INSERT INTO pagg_tab SELECT i % 20, i % 30, to_char(i % 12, 'FM0000'), i % 30 FROM generate_series(0, 2999) i;
ANALYZE pagg_tab;

-- GROUP BY子句匹配时；对每个分区执行完全聚合
EXPLAIN (COSTS OFF)
SELECT c, sum(a), avg(b), count(*), min(a), max(b) FROM pagg_tab GROUP BY c HAVING avg(d) < 15 ORDER BY 1, 2, 3;
SELECT c, sum(a), avg(b), count(*), min(a), max(b) FROM pagg_tab GROUP BY c HAVING avg(d) < 15 ORDER BY 1, 2, 3;

-- 当GROUP BY子句不匹配时；对每个分区执行部分聚合
EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(b), count(*), min(a), max(b) FROM pagg_tab GROUP BY a HAVING avg(d) < 15 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(b), count(*), min(a), max(b) FROM pagg_tab GROUP BY a HAVING avg(d) < 15 ORDER BY 1, 2, 3;

-- 检查GROUP BY中的多列
EXPLAIN (COSTS OFF)
SELECT a, c, count(*) FROM pagg_tab GROUP BY a, c;
-- 检查GROUP BY中的多列，GROUP BY中顺序颠倒
EXPLAIN (COSTS OFF)
SELECT a, c, count(*) FROM pagg_tab GROUP BY c, a;
-- 检查GROUP BY中的多列，目标列表中的顺序颠倒
EXPLAIN (COSTS OFF)
SELECT c, a, count(*) FROM pagg_tab GROUP BY a, c;

-- 测试分组的输入关系何时为空
EXPLAIN (COSTS OFF)
SELECT c, sum(a) FROM pagg_tab WHERE 1 = 2 GROUP BY c;
SELECT c, sum(a) FROM pagg_tab WHERE 1 = 2 GROUP BY c;
EXPLAIN (COSTS OFF)
SELECT c, sum(a) FROM pagg_tab WHERE c = 'x' GROUP BY c;
SELECT c, sum(a) FROM pagg_tab WHERE c = 'x' GROUP BY c;

-- 通过禁用哈希聚合来测试GroupAggregate路径
SET enable_hashagg TO false;

-- 当GROUP BY子句匹配时，将对每个分区执行完全聚合。
EXPLAIN (COSTS OFF)
SELECT c, sum(a), avg(b), count(*) FROM pagg_tab GROUP BY 1 HAVING avg(d) < 15 ORDER BY 1, 2, 3;
SELECT c, sum(a), avg(b), count(*) FROM pagg_tab GROUP BY 1 HAVING avg(d) < 15 ORDER BY 1, 2, 3;

-- 当GROUP BY子句不匹配时；对每个分区执行部分聚合。当GROUP BY子句不匹配时；对每个分区执行部分聚合。
EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(b), count(*) FROM pagg_tab GROUP BY 1 HAVING avg(d) < 15 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(b), count(*) FROM pagg_tab GROUP BY 1 HAVING avg(d) < 15 ORDER BY 1, 2, 3;

-- 在没有任何聚合的情况下测试分区分组
EXPLAIN (COSTS OFF)
SELECT c FROM pagg_tab GROUP BY c ORDER BY 1;
SELECT c FROM pagg_tab GROUP BY c ORDER BY 1;
EXPLAIN (COSTS OFF)
SELECT a FROM pagg_tab WHERE a < 3 GROUP BY a ORDER BY 1;
SELECT a FROM pagg_tab WHERE a < 3 GROUP BY a ORDER BY 1;

RESET enable_hashagg;

-- ROLLUP，分区聚合不适用
EXPLAIN (COSTS OFF)
SELECT c, sum(a) FROM pagg_tab GROUP BY rollup(c) ORDER BY 1, 2;

--集合中的有序集合
--完全聚合；因为属于同一组的所有行都来了
--来自同一分区，在聚合中具有ORDER BY不会做出任何改变
EXPLAIN (COSTS OFF)
SELECT c, sum(b order by a) FROM pagg_tab GROUP BY c ORDER BY 1, 2;
--由于GROUP BY子句与PARTITION KEY不匹配；我们需要做
--部分聚集。然而，ORDERED SET不是部分安全的，因此不生成分区聚合计划。
EXPLAIN (COSTS OFF)
SELECT a, sum(b order by a) FROM pagg_tab GROUP BY a ORDER BY 1, 2;


-- JOIN query

CREATE TABLE pagg_tab1(x int, y int) PARTITION BY RANGE(x);
CREATE TABLE pagg_tab1_p1 PARTITION OF pagg_tab1 FOR VALUES FROM (0) TO (10);
CREATE TABLE pagg_tab1_p2 PARTITION OF pagg_tab1 FOR VALUES FROM (10) TO (20);
CREATE TABLE pagg_tab1_p3 PARTITION OF pagg_tab1 FOR VALUES FROM (20) TO (30);

CREATE TABLE pagg_tab2(x int, y int) PARTITION BY RANGE(y);
CREATE TABLE pagg_tab2_p1 PARTITION OF pagg_tab2 FOR VALUES FROM (0) TO (10);
CREATE TABLE pagg_tab2_p2 PARTITION OF pagg_tab2 FOR VALUES FROM (10) TO (20);
CREATE TABLE pagg_tab2_p3 PARTITION OF pagg_tab2 FOR VALUES FROM (20) TO (30);

INSERT INTO pagg_tab1 SELECT i % 30, i % 20 FROM generate_series(0, 299, 2) i;
INSERT INTO pagg_tab2 SELECT i % 20, i % 30 FROM generate_series(0, 299, 3) i;

ANALYZE pagg_tab1;
ANALYZE pagg_tab2;

-- GROUP BY子句匹配时；对每个分区执行完全聚合。
-- 后面不再添加注释 , 因为时间问题 , 如果有时间会更新注释
EXPLAIN (COSTS OFF)
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT t1.x, sum(t1.y), count(t1) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;
SELECT t1.x, sum(t1.y), count(t1) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT t2.y, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t2.y ORDER BY 1, 2, 3;

SET enable_hashagg TO false;
EXPLAIN (COSTS OFF)
SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;
SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;
RESET enable_hashagg;

EXPLAIN (COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;
SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;

EXPLAIN (COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;
SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;

EXPLAIN (COSTS OFF)
SELECT a.x, sum(b.x) FROM pagg_tab1 a FULL OUTER JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x ORDER BY 1 NULLS LAST;
SELECT a.x, sum(b.x) FROM pagg_tab1 a FULL OUTER JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x ORDER BY 1 NULLS LAST;

EXPLAIN (COSTS OFF)
SELECT a.x, b.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x < 20) a LEFT JOIN (SELECT * FROM pagg_tab2 WHERE y > 10) b ON a.x = b.y WHERE a.x > 5 or b.y < 20  GROUP BY a.x, b.y ORDER BY 1, 2;
SELECT a.x, b.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x < 20) a LEFT JOIN (SELECT * FROM pagg_tab2 WHERE y > 10) b ON a.x = b.y WHERE a.x > 5 or b.y < 20  GROUP BY a.x, b.y ORDER BY 1, 2;

EXPLAIN (COSTS OFF)
SELECT a.x, b.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x < 20) a FULL JOIN (SELECT * FROM pagg_tab2 WHERE y > 10) b ON a.x = b.y WHERE a.x > 5 or b.y < 20  GROUP BY a.x, b.y ORDER BY 1, 2;
SELECT a.x, b.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x < 20) a FULL JOIN (SELECT * FROM pagg_tab2 WHERE y > 10) b ON a.x = b.y WHERE a.x > 5 or b.y < 20 GROUP BY a.x, b.y ORDER BY 1, 2;

EXPLAIN (COSTS OFF)
SELECT a.x, a.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x = 1 AND x = 2) a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x, a.y ORDER BY 1, 2;
SELECT a.x, a.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x = 1 AND x = 2) a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x, a.y ORDER BY 1, 2;



CREATE TABLE pagg_tab_m (a int, b int, c int) PARTITION BY RANGE(a, ((a+b)/2));
CREATE TABLE pagg_tab_m_p1 PARTITION OF pagg_tab_m FOR VALUES FROM (0, 0) TO (12, 12);
CREATE TABLE pagg_tab_m_p2 PARTITION OF pagg_tab_m FOR VALUES FROM (12, 12) TO (22, 22);
CREATE TABLE pagg_tab_m_p3 PARTITION OF pagg_tab_m FOR VALUES FROM (22, 22) TO (30, 30);
INSERT INTO pagg_tab_m SELECT i % 30, i % 40, i % 50 FROM generate_series(0, 2999) i;
ANALYZE pagg_tab_m;

EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY a HAVING avg(c) < 22 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY a HAVING avg(c) < 22 ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY a, (a+b)/2 HAVING sum(b) < 50 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY a, (a+b)/2 HAVING sum(b) < 50 ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT a, c, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY (a+b)/2, 2, 1 HAVING sum(b) = 50 AND avg(c) > 25 ORDER BY 1, 2, 3;
SELECT a, c, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY (a+b)/2, 2, 1 HAVING sum(b) = 50 AND avg(c) > 25 ORDER BY 1, 2, 3;



CREATE TABLE pagg_tab_ml (a int, b int, c text) PARTITION BY RANGE(a);
CREATE TABLE pagg_tab_ml_p1 PARTITION OF pagg_tab_ml FOR VALUES FROM (0) TO (12);
CREATE TABLE pagg_tab_ml_p2 PARTITION OF pagg_tab_ml FOR VALUES FROM (12) TO (20) PARTITION BY LIST (c);
CREATE TABLE pagg_tab_ml_p2_s1 PARTITION OF pagg_tab_ml_p2 FOR VALUES IN ('0000', '0001', '0002');
CREATE TABLE pagg_tab_ml_p2_s2 PARTITION OF pagg_tab_ml_p2 FOR VALUES IN ('0003');

-- This level of partitioning has different column positions than the parent
CREATE TABLE pagg_tab_ml_p3(b int, c text, a int) PARTITION BY RANGE (b);
CREATE TABLE pagg_tab_ml_p3_s1(c text, a int, b int);
CREATE TABLE pagg_tab_ml_p3_s2 PARTITION OF pagg_tab_ml_p3 FOR VALUES FROM (7) TO (10);

ALTER TABLE pagg_tab_ml_p3 ATTACH PARTITION pagg_tab_ml_p3_s1 FOR VALUES FROM (0) TO (7);
ALTER TABLE pagg_tab_ml ATTACH PARTITION pagg_tab_ml_p3 FOR VALUES FROM (20) TO (30);

INSERT INTO pagg_tab_ml SELECT i % 30, i % 10, to_char(i % 4, 'FM0000') FROM generate_series(0, 29999) i;
ANALYZE pagg_tab_ml;

SET max_parallel_workers_per_gather TO 2;
SET parallel_setup_cost = 0;

EXPLAIN (COSTS OFF)
SELECT a, sum(b), array_agg(distinct c), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;
SELECT a, sum(b), array_agg(distinct c), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT a, sum(b), array_agg(distinct c), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3;

RESET parallel_setup_cost;

EXPLAIN (COSTS OFF)
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT b, sum(a), count(*) FROM pagg_tab_ml GROUP BY b ORDER BY 1, 2, 3;
SELECT b, sum(a), count(*) FROM pagg_tab_ml GROUP BY b HAVING avg(a) < 15 ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a, b, c HAVING avg(b) > 7 ORDER BY 1, 2, 3;
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a, b, c HAVING avg(b) > 7 ORDER BY 1, 2, 3;


SET min_parallel_table_scan_size TO '8kB';
SET parallel_setup_cost TO 0;

EXPLAIN (COSTS OFF)
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT b, sum(a), count(*) FROM pagg_tab_ml GROUP BY b ORDER BY 1, 2, 3;
SELECT b, sum(a), count(*) FROM pagg_tab_ml GROUP BY b HAVING avg(a) < 15 ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a, b, c HAVING avg(b) > 7 ORDER BY 1, 2, 3;
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a, b, c HAVING avg(b) > 7 ORDER BY 1, 2, 3;



SET parallel_setup_cost TO 10;

CREATE TABLE pagg_tab_para(x int, y int) PARTITION BY RANGE(x);
CREATE TABLE pagg_tab_para_p1 PARTITION OF pagg_tab_para FOR VALUES FROM (0) TO (12);
CREATE TABLE pagg_tab_para_p2 PARTITION OF pagg_tab_para FOR VALUES FROM (12) TO (22);
CREATE TABLE pagg_tab_para_p3 PARTITION OF pagg_tab_para FOR VALUES FROM (22) TO (30);

INSERT INTO pagg_tab_para SELECT i % 30, i % 20 FROM generate_series(0, 29999) i;

ANALYZE pagg_tab_para;

EXPLAIN (COSTS OFF)
SELECT x, sum(y), avg(y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;
SELECT x, sum(y), avg(y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;

EXPLAIN (COSTS OFF)
SELECT y, sum(x), avg(x), count(*) FROM pagg_tab_para GROUP BY y HAVING avg(x) < 12 ORDER BY 1, 2, 3;
SELECT y, sum(x), avg(x), count(*) FROM pagg_tab_para GROUP BY y HAVING avg(x) < 12 ORDER BY 1, 2, 3;

ALTER TABLE pagg_tab_para_p1 SET (parallel_workers = 0);
ALTER TABLE pagg_tab_para_p3 SET (parallel_workers = 0);
ANALYZE pagg_tab_para;

EXPLAIN (COSTS OFF)
SELECT x, sum(y), avg(y), sum(x+y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;
SELECT x, sum(y), avg(y), sum(x+y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;

ALTER TABLE pagg_tab_para_p2 SET (parallel_workers = 0);
ANALYZE pagg_tab_para;

EXPLAIN (COSTS OFF)
SELECT x, sum(y), avg(y), sum(x+y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;
SELECT x, sum(y), avg(y), sum(x+y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;

RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;

EXPLAIN (COSTS OFF)
SELECT x, sum(y), avg(y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;
SELECT x, sum(y), avg(y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;
