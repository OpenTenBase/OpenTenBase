-- 正确性验证：结果必须与人工推演答案一致
\set ON_ERROR_STOP on
\timing on

\echo '=== 1) 3度搜索 root=1，验证深度分层与路径 ==='
SELECT o_depth, count(*) AS edges, min(array_length(o_path,1)) AS min_len, max(array_length(o_path,1)) AS max_len
FROM gtest.t_search(1, 3, 1000, 0) GROUP BY o_depth ORDER BY o_depth;

\echo '--- 期望: depth1=2条(1->2,1->3), depth2=3条(2->4,3->4,3->5), depth3=4条(4->6,5->6,5->7, 加上7->8?) ---'

\echo ''
\echo '=== 2) 全部 3 度路径明细 ==='
SELECT o_depth, o_path, o_point1, o_point2, o_link_prop->>'rel' AS rel
FROM gtest.t_search(1, 3, 1000, 0) ORDER BY o_depth, o_path;

\echo ''
\echo '=== 3) 防环验证：图中有 2->4->6->8->2 环，路径不得重复点 ==='
SELECT count(*) AS paths_with_dup_node
FROM gtest.t_search(1, 4, 1000, 0) s
WHERE array_length(o_path,1) <> (SELECT count(DISTINCT x) FROM unnest(o_path) x);
-- 期望 0

\echo ''
\echo '=== 4) 最短路径 1 -> 11，期望 [1,3,4,6,8,9,10,11] 或等价最短 ==='
SELECT o_path, o_depth, o_link_props FROM gtest.t_shortest_path(1, 11, 10);

\echo ''
\echo '=== 5) 最短路径 1 -> 2 (直达, depth=1) ==='
SELECT o_path, o_depth FROM gtest.t_shortest_path(1, 2, 10);

\echo ''
\echo '=== 6) 不可达：期望 0 行 ==='
SELECT count(*) AS unreachable_rows FROM gtest.t_shortest_path(12, 1, 10);

\echo ''
\echo '=== 7) p1 = p2 期望 depth=0 且 path=[1] ==='
SELECT o_path, o_depth FROM gtest.t_shortest_path(1, 1, 10);

\echo ''
\echo '=== 8) 权重过滤 i_weight=0.6，1度只剩 weight>=0.6 的边 ==='
SELECT o_path, o_point2, (o_link_prop->>'weight')::float8 AS w FROM gtest.t_search(1, 1, 100, 0.6) ORDER BY w DESC;

\echo ''
\echo '=== 9) 无向实例：6 -> 1 应该有路(6<-4<-2<-1) ==='
SELECT o_path, o_depth FROM gtest.u_shortest_path(6, 1, 10);

\echo ''
\echo '=== 10) 有向实例 6 -> 1 不可达 ==='
SELECT count(*) AS directed_6_to_1 FROM gtest.t_shortest_path(6, 1, 10);

\echo ''
\echo '=== 11) 分层统计 ==='
SELECT * FROM gtest.t_stats(1, 3, 1000, 0);

\echo ''
\echo '=== 12) 子图 JSON ==='
SELECT jsonb_pretty(gtest.t_subgraph(1, 2, 1000, 0));

\echo ''
\echo '=== 13) 游标版 ==='
BEGIN;
SELECT * FROM gtest.t_cursor(1, 'cur_test', 2, 1000, 0);
FETCH 5 FROM cur_test;
FETCH 3 FROM cur_test;
CLOSE cur_test;
COMMIT;

\echo ''
\echo '=== 14) 边界：i_depth 超过 cap 应报错 ==='
\set ON_ERROR_STOP off
SELECT * FROM gtest.t_search(1, 999, 100, 0);
\set ON_ERROR_STOP on

\echo ''
\echo '=== 15) 边界：i_depth=0 应报错 ==='
\set ON_ERROR_STOP off
SELECT * FROM gtest.t_search(1, 0, 100, 0);
\set ON_ERROR_STOP on
