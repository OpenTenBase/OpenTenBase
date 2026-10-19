-- 小规模正确性测试：构造确定性图，人工可推演答案
\set ON_ERROR_STOP on

DROP VIEW IF EXISTS big_edge_check_edges;
DROP TABLE IF EXISTS t_edge;
CREATE UNLOGGED TABLE t_edge (
  c1   int,
  c2   int,
  prop jsonb,
  PRIMARY KEY (c1, c2)
);

-- 有向图，节点 1..12
-- 1 -> 2, 3
-- 2 -> 4
-- 3 -> 4, 5
-- 4 -> 6
-- 5 -> 6, 7
-- 6 -> 8
-- 7 -> 8
-- 8 -> 2   (环: 2->4->6->8->2)
-- 8 -> 9
-- 9 -> 10
-- 10 -> 11
-- 11 -> 12
INSERT INTO t_edge(c1, c2, prop) VALUES
 (1,2,'{"weight":0.9,"rel":"friend"}'), (1,3,'{"weight":0.5,"rel":"coworker"}'),
 (2,4,'{"weight":0.8,"rel":"friend"}'),
 (3,4,'{"weight":0.7,"rel":"friend"}'), (3,5,'{"weight":0.3,"rel":"weak"}'),
 (4,6,'{"weight":0.6,"rel":"friend"}'),
 (5,6,'{"weight":0.2,"rel":"weak"}'),   (5,7,'{"weight":0.95,"rel":"friend"}'),
 (6,8,'{"weight":0.4,"rel":"weak"}'),
 (7,8,'{"weight":0.5,"rel":"coworker"}'),
 (8,2,'{"weight":0.1,"rel":"weak"}'),   (8,9,'{"weight":0.85,"rel":"friend"}'),
 (9,10,'{"weight":0.6,"rel":"friend"}'),
 (10,11,'{"weight":0.6,"rel":"friend"}'),
 (11,12,'{"weight":0.6,"rel":"friend"}');

VACUUM ANALYZE t_edge;

DROP SCHEMA IF EXISTS gtest CASCADE;
CREATE SCHEMA gtest;

-- 安装实例（有向）。install() 返回 DDL 文本数组，这里丢弃只看是否成功
-- install() 返回 DDL 文本数组，输出重定向掉，只关心是否成功
\o /dev/null
SELECT graph_search.install('{
  "table":"public.t_edge","c1":"c1","c2":"c2","prop":"prop",
  "target_schema":"gtest","prefix":"t","directed":true
}'::jsonb);

-- 安装实例（无向，用于对比）
SELECT graph_search.install('{
  "table":"public.t_edge","c1":"c1","c2":"c2","prop":"prop",
  "target_schema":"gtest","prefix":"u","directed":false
}'::jsonb);
\o

\echo '=== 安装清单 ==='
SELECT * FROM graph_search.list_installed();
