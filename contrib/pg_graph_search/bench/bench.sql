-- 性能压测：对比生成函数 vs 文章原始 CTE 写法
\set ON_ERROR_STOP on
\timing on

\echo ''
\echo '############ A. 3度搜索，每层限 100 条 ############'

\echo '--- A1 生成函数 gbench.b_search(1, 3, 100, 0) ---'
SELECT count(*) AS rows FROM gbench.b_search(1, 3, 100, 0);

\echo '--- A2 文章原始写法(直接查视图, 不走 UDF) ---'
WITH RECURSIVE sg(src,dst,prop,w,depth,path) AS (
        SELECT t.src,t.dst,t.prop,t.w,t.depth,t.path
          FROM (SELECT e.src,e.dst,e.prop,e.w,1 AS depth, ARRAY[e.src] AS path
                  FROM gbench.b_edge e WHERE e.src=1 AND e.w>=0
                 ORDER BY e.w DESC LIMIT 100) t
    UNION ALL
        SELECT t.src,t.dst,t.prop,t.w,t.depth,t.path
          FROM (SELECT e.src,e.dst,e.prop,e.w,sg.depth+1 AS depth, sg.path||e.src AS path
                  FROM gbench.b_edge e JOIN sg ON e.src=sg.dst
                 WHERE sg.depth < 3 AND e.src<>ALL(sg.path) AND e.w>=0
                 ORDER BY e.w DESC LIMIT 100) t
) SELECT count(*) FROM sg;

\echo ''
\echo '############ B. 2度搜索，每层限 100 条 (更贴近文章场景) ############'
SELECT count(*) FROM gbench.b_search(1, 2, 100, 0);

\echo ''
\echo '############ C. 最短路径 ############'
\echo '--- C0 先取真实可达的目标点 (2跳内) ---'
CREATE TEMP TABLE tgt AS
SELECT e.c2 AS n
  FROM big_edge e
  JOIN (SELECT c2 AS n FROM big_edge WHERE c1 = 1) AS l1 ON l1.n = e.c1
 WHERE e.c2 NOT IN (SELECT c2 FROM big_edge WHERE c1 = 1)
 LIMIT 1;
SELECT * FROM tgt;

\echo '--- C1 最短路径 1 -> (2跳可达点), 期望 depth=2 ---'
SELECT o_depth, array_length(o_path,1) AS hops, o_path
  FROM gbench.b_shortest_path(1, (SELECT n FROM tgt), 5);

\echo '--- C2 深度不足(1跳)不可达, 期望 0 行 ---'
SELECT count(*) FROM gbench.b_shortest_path(1, (SELECT n FROM tgt), 1);

\echo '--- C3 不存在的节点, 期望 0 行 ---'
SELECT count(*) FROM gbench.b_shortest_path(1, 99999999, 5);

\echo ''
\echo '############ D. 权重过滤 (每层限 50, 权重大于 0.9) ############'
SELECT * FROM gbench.b_stats(1, 3, 50, 0.9);

\echo ''
\echo '############ E. 子图 JSON 耗时与体积 ############'
SELECT pg_column_size(gbench.b_subgraph(1, 3, 50, 0.9)) AS json_bytes;

\echo ''
\echo '############ F. 无向视图开销对比 ############'
\echo '--- F1 有向搜索 ---'
SELECT count(*) FROM gbench.b_search(1, 2, 50, 0);
\echo '--- F2 无向搜索 (视图 UNION ALL 双向) ---'
SELECT count(*) FROM gbench_u.bu_search(1, 2, 50, 0);

\echo ''
\echo '############ G. 并发压测 (pgbench, 16 并发) ############'
