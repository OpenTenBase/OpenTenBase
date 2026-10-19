-- pg_regress 用例：pg_graph_search
--
-- 设计原则（为了同一份期望输出能同时用于 OpenTenBase 与原生 PostgreSQL）：
--   1. 断言全部放在 DO 块里，失败即 RAISE（额外输出 = 差异 = 测试失败）；
--      DO 块成功时不打印任何版本相关信息。
--   2. 只 SELECT 由本扩展自己算出来的稳定结果（整数 / 本扩展生成的路径数组）。
--   3. 不打印 list_installed()（含 installed_at 时间戳）、不打印 pg 版本信息。
--   4. control 文件里的 sql_mode 是 OpenTenBase 专有参数，原生 PG 会打 WARNING；
--      把 client_min_messages 调到 error 抹平两平台差异。

SET client_min_messages = error;

CREATE EXTENSION pg_graph_search;

\echo '--- T1: 扩展安装后对象齐全 ---'
DO $$
DECLARE v_missing text[];
BEGIN
  SELECT array_agg(x) INTO v_missing
    FROM unnest(ARRAY['plan', 'install', 'uninstall', 'list_installed', 'advise',
                      'registry_distribution', 'set_registry_replication']) x
   WHERE NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                      WHERE n.nspname = 'graph_search' AND p.proname = x);
  IF v_missing IS NOT NULL THEN
    RAISE EXCEPTION '缺少函数: %', v_missing;
  END IF;
  IF to_regclass('graph_search.registry') IS NULL THEN
    RAISE EXCEPTION '缺少注册表 graph_search.registry';
  END IF;
END $$;
\echo 'T1 OK'

-- 确定性小图（12 个点），人工可推演
CREATE TABLE t_edge (
  c1   int NOT NULL,
  c2   int NOT NULL,
  prop jsonb,
  PRIMARY KEY (c1, c2)
);
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

CREATE SCHEMA gtest;

\echo '--- T2: plan() 只读且返回 11 条 DDL ---'
SELECT array_length(graph_search.plan(
  '{"table":"public.t_edge","prop":"prop","target_schema":"gtest","prefix":"t"}'::jsonb), 1)
  AS plan_statements;
\echo 'T2 OK'

\echo '--- T3: 安装有向实例 + 无向实例 ---'
SELECT graph_search.install(
  '{"table":"public.t_edge","c1":"c1","c2":"c2","prop":"prop",
    "target_schema":"gtest","prefix":"t","directed":true}'::jsonb) IS NOT NULL AS directed_installed;
SELECT graph_search.install(
  '{"table":"public.t_edge","c1":"c1","c2":"c2","prop":"prop",
    "target_schema":"gtest","prefix":"u","directed":false}'::jsonb) IS NOT NULL AS undirected_installed;
\echo 'T3 OK'

\echo '--- T4: 3 度搜索的深度分层精确为 2/3/4 ---'
SELECT o_depth, count(*) AS edges FROM gtest.t_search(1, 3, 1000, 0)
 GROUP BY o_depth ORDER BY o_depth;
\echo 'T4 OK'

\echo '--- T5: 防环（图中有 2->4->6->8->2 环），重复点路径数为 0 ---'
SELECT count(*) AS paths_with_dup_node
  FROM gtest.t_search(1, 4, 1000, 0) s
 WHERE array_length(o_path, 1) <> (SELECT count(DISTINCT x) FROM unnest(o_path) x);
\echo 'T5 OK'

\echo '--- T6: 最短路径含起终点 + 逐跳边属性与跳数对齐 ---'
SELECT o_path, o_depth, array_length(o_link_props, 1) AS link_props
  FROM gtest.t_shortest_path(1, 11, 10);
\echo 'T6 OK'

\echo '--- T7: p1 = p2 -> depth 0 -> path {p1}；不可达 -> 0 行 ---'
SELECT o_path, o_depth FROM gtest.t_shortest_path(1, 1, 10);
SELECT count(*) AS unreachable FROM gtest.t_shortest_path(12, 1, 10);
\echo 'T7 OK'

\echo '--- T8: 权重过滤（>=0.6 时 1 度只剩 weight 0.9 那条边） ---'
SELECT o_path, (o_link_prop->>'weight')::float8 AS w
  FROM gtest.t_search(1, 1, 100, 0.6) ORDER BY o_path;
\echo 'T8 OK'

\echo '--- T9: 有向 6->1 不可达，无向 6->1 可达 ---'
SELECT count(*) AS directed_6_to_1 FROM gtest.t_shortest_path(6, 1, 10);
SELECT o_depth FROM gtest.u_shortest_path(6, 1, 10);
\echo 'T9 OK'

\echo '--- T10: 分层统计与子图 JSON ---'
SELECT * FROM gtest.t_stats(1, 3, 1000, 0) ORDER BY o_depth;
\echo 'T10 OK'

\echo '--- T10b: 子图 JSON 结构 ---'
SELECT (gtest.t_subgraph(1, 2, 1000, 0) ->> 'node_count')::int AS node_count,
       (gtest.t_subgraph(1, 2, 1000, 0) ->> 'edge_count')::int AS edge_count,
       jsonb_array_length(gtest.t_subgraph(1, 2, 1000, 0) -> 'nodes') AS nodes_len,
       jsonb_array_length(gtest.t_subgraph(1, 2, 1000, 0) -> 'edges') AS edges_len;
\echo 'T10b OK'

\echo '--- T11: 游标分页与 search 行数一致（9 条路径） ---'
DO $$
DECLARE c refcursor; r record; n1 bigint; n2 bigint := 0;
BEGIN
  SELECT count(*) INTO n1 FROM gtest.t_search(1, 3, 1000, 0);
  SELECT gtest.t_cursor(1, 'c_regress', 3, 1000, 0) INTO c;
  LOOP
    FETCH c INTO r;
    EXIT WHEN NOT FOUND;
    n2 := n2 + 1;
  END LOOP;
  CLOSE c;
  IF n1 <> n2 THEN
    RAISE EXCEPTION '游标 % 行 vs search % 行', n2, n1;
  END IF;
  IF n1 <> 9 THEN
    RAISE EXCEPTION '期望 9 条路径，实际 %', n1;
  END IF;
END $$;
\echo 'T11 OK'

\echo '--- T12: 参数校验（深度 0 / 超 cap / limit 0 都要抛错） ---'
DO $$
DECLARE ok boolean;
BEGIN
  ok := false; BEGIN PERFORM * FROM gtest.t_search(1, 0, 100, 0); EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION 'i_depth=0 未报错'; END IF;
  ok := false; BEGIN PERFORM * FROM gtest.t_search(1, 999, 100, 0); EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION 'i_depth 超 cap 未报错'; END IF;
  ok := false; BEGIN PERFORM * FROM gtest.t_search(1, 3, 0, 0); EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION 'i_limit=0 未报错'; END IF;
END $$;
\echo 'T12 OK'

\echo '--- T13: spec 省略 prop 也能安装（prop 为 NULL、w 恒 0） ---'
SELECT graph_search.install(
  '{"table":"public.t_edge","target_schema":"gtest","prefix":"nop"}'::jsonb) IS NOT NULL AS installed_without_prop;
SELECT count(*) AS rows_without_prop FROM gtest.nop_search(1, 2, 10, 0);
\echo 'T13 OK'

\echo '--- T14: 幂等重装 + uninstall 清理干净 ---'
DO $$
DECLARE n1 bigint; n2 bigint;
BEGIN
  n1 := (SELECT count(*) FROM pg_proc p JOIN pg_namespace ns ON ns.oid = p.pronamespace
          WHERE ns.nspname = 'gtest');
  PERFORM graph_search.install(
    '{"table":"public.t_edge","c1":"c1","c2":"c2","prop":"prop",
      "target_schema":"gtest","prefix":"t","directed":true}'::jsonb);
  n2 := (SELECT count(*) FROM pg_proc p JOIN pg_namespace ns ON ns.oid = p.pronamespace
          WHERE ns.nspname = 'gtest');
  IF n1 <> n2 THEN
    RAISE EXCEPTION '重装后函数数变了: % -> %', n1, n2;
  END IF;
END $$;
DO $$
DECLARE left_cnt bigint; reg_cnt bigint;
BEGIN
  PERFORM graph_search.uninstall('gtest', 'nop', false);
  SELECT count(*) INTO reg_cnt FROM graph_search.registry
   WHERE target_schema = 'gtest' AND prefix = 'nop';
  IF reg_cnt <> 0 THEN RAISE EXCEPTION 'registry 残留记录'; END IF;
  PERFORM graph_search.uninstall('gtest', 't', false);
  SELECT count(*) INTO left_cnt
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'gtest' AND c.relname LIKE 't\_%';
  IF left_cnt <> 0 THEN RAISE EXCEPTION '残留对象 % 个', left_cnt; END IF;
END $$;
\echo 'T14 OK'

\echo '--- T15: 注册表分布策略（OpenTenBase 应为 REPLICATION；原生 PG 为 single-node） ---'
DO $$
DECLARE v text := graph_search.registry_distribution();
BEGIN
  IF v NOT IN ('single-node', 'REPLICATION') THEN
    RAISE EXCEPTION '注册表分布策略应为 single-node 或 REPLICATION，实际 %', v;
  END IF;
END $$;
\echo 'T15 OK'

\echo '==== pg_graph_search pg_regress 全部通过 ===='
