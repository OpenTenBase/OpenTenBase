-- 回归测试：断言式。任一断言不符立即报错退出。
\set ON_ERROR_STOP on

-- R4 需要逐跳校验边是否存在，建个直查视图
DROP VIEW IF EXISTS big_edge_check_edges;
CREATE VIEW big_edge_check_edges AS SELECT c1, c2 FROM public.t_edge;

\echo '--- R1: install 后对象齐全 ---'
DO $$
DECLARE v_missing text[];
BEGIN
  -- 视图查 regclass，函数查 regprocedure（函数不是 regclass）
  SELECT array_agg(x) INTO v_missing
    FROM unnest(ARRAY['t_edge','t_search','t_cursor','t_shortest_path','t_subgraph','t_stats']) x
   WHERE to_regclass(format('gtest.%s', x)) IS NULL
      AND NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                       WHERE n.nspname='gtest' AND p.proname = x);
  IF v_missing IS NOT NULL THEN
    RAISE EXCEPTION '缺少对象: %', v_missing;
  END IF;
  -- 函数签名校验（oidvectortypes 只给类型，不带参数名）
  PERFORM 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
   WHERE n.nspname='gtest' AND p.proname='t_search'
     AND pg_get_function_arguments(p.oid) = 'i_root integer, i_depth integer DEFAULT 3, '
         || 'i_limit bigint DEFAULT 100, i_weight double precision DEFAULT 0';
  IF NOT FOUND THEN
    RAISE EXCEPTION 't_search 签名不符合预期';
  END IF;
END $$;
\echo 'R1 OK'

\echo '--- R2: 3度搜索深度分层精确 = 2/3/4 ---'
DO $$
DECLARE v int[];
BEGIN
  SELECT array_agg(c ORDER BY d) INTO v
    FROM (SELECT o_depth d, count(*) c FROM gtest.t_search(1,3,1000,0) GROUP BY o_depth) x;
  IF v <> ARRAY[2,3,4] THEN
    RAISE EXCEPTION '深度分层不符: % (期望 2,3,4)', v;
  END IF;
END $$;
\echo 'R2 OK'

\echo '--- R3: 路径无重复点（防环） ---'
DO $$
DECLARE v bigint;
BEGIN
  SELECT count(*) INTO v FROM gtest.t_search(1,6,1000,0) s
   WHERE array_length(o_path,1) <> (SELECT count(DISTINCT x) FROM unnest(o_path) x);
  IF v <> 0 THEN RAISE EXCEPTION '存在 % 条含重复点的路径', v; END IF;
END $$;
\echo 'R3 OK'

\echo '--- R4: 最短路径包含起终点，且逐跳相邻 ---'
DO $$
DECLARE p int[]; i int;
BEGIN
  SELECT o_path INTO p FROM gtest.t_shortest_path(1, 11, 10);
  IF p IS NULL THEN RAISE EXCEPTION '最短路径未返回'; END IF;
  IF p[1] <> 1 OR p[array_length(p,1)] <> 11 THEN
    RAISE EXCEPTION '路径端点不对: %', p;
  END IF;
  FOR i IN 1..array_length(p,1)-1 LOOP
    IF NOT EXISTS (SELECT 1 FROM big_edge_check_edges e WHERE e.c1=p[i] AND e.c2=p[i+1]) THEN
      RAISE EXCEPTION '路径第 % 跳 %->% 不存在边', i, p[i], p[i+1];
    END IF;
  END LOOP;
END $$;
\echo 'R4 OK'

\echo '--- R5: 最短路径深度是最小值 ---'
DO $$
DECLARE v_d int; v_min int;
BEGIN
  SELECT o_depth INTO v_d FROM gtest.t_shortest_path(1, 11, 10);
  -- 用独立的逐层 BFS 算一遍最小跳数（每层一个查询，visited 用数组传递）
  SELECT min(d) INTO v_min FROM (
    WITH RECURSIVE lv(n,d,seen) AS (
      SELECT 1::int, 0, ARRAY[1::int]
      UNION ALL
      SELECT e.c2, lv.d+1, lv.seen || e.c2
        FROM lv JOIN t_edge e ON e.c1 = lv.n
       WHERE lv.d < 20 AND NOT e.c2 = ANY(lv.seen)
    ) SELECT n, d FROM lv
  ) q WHERE n = 11;
  IF v_d <> v_min THEN
    RAISE EXCEPTION '最短路径深度 % 非最优 %', v_d, v_min;
  END IF;
END $$;
\echo 'R5 OK'

\echo '--- R6: 不可达返回 0 行 ---'
DO $$
DECLARE v bigint;
BEGIN
  SELECT count(*) INTO v FROM gtest.t_shortest_path(12, 1, 10);
  IF v <> 0 THEN RAISE EXCEPTION '不可达却返回 % 行', v; END IF;
END $$;
\echo 'R6 OK'

\echo '--- R7: p1=p2 返回 depth=0 ---'
DO $$
DECLARE v_d int; v_p int[];
BEGIN
  SELECT o_depth, o_path INTO v_d, v_p FROM gtest.t_shortest_path(7, 7, 10);
  IF v_d <> 0 OR v_p <> ARRAY[7] THEN
    RAISE EXCEPTION 'p1=p2 处理错误: depth=% path=%', v_d, v_p;
  END IF;
END $$;
\echo 'R7 OK'

\echo '--- R8: 权重过滤生效 ---'
DO $$
DECLARE v int;
BEGIN
  SELECT count(*) INTO v FROM gtest.t_search(1,1,100,0.95) s
   WHERE (o_link_prop->>'weight')::float8 < 0.95;
  IF v <> 0 THEN RAISE EXCEPTION '权重过滤漏了 % 条', v; END IF;
END $$;
\echo 'R8 OK'

\echo '--- R9: 无向实例可达、有向实例不可达 ---'
DO $$
DECLARE v_u int; v_d int;
BEGIN
  SELECT o_depth INTO v_u FROM gtest.u_shortest_path(6, 1, 10);
  SELECT count(*) INTO v_d FROM gtest.t_shortest_path(6, 1, 10);
  IF v_u IS NULL THEN RAISE EXCEPTION '无向图 6->1 应可达'; END IF;
  IF v_d <> 0 THEN RAISE EXCEPTION '有向图 6->1 不应可达'; END IF;
END $$;
\echo 'R9 OK'

\echo '--- R10: 游标分页结果与 search 逐行一致 ---'
-- 注意（OpenTenBase）：gtest.t_search() 是 CN 侧 "pull-up" 函数，
--   * 不能出现在 DML 里：INSERT INTO ... SELECT f() → "Using pull-up functions
--     in DML statements is not supported"
--   * 不能出现在子计划里：... WHERE EXISTS (SELECT ... f()) → "Subplan contains a
--     function runs on CN which is not supported"
--   * 与本地临时表做 EXCEPT/INTERSECT 不报错但结果错（实测 9 行会变成 3 行）
--   * array_agg() 聚合 int[] 列会报 "cannot accumulate arrays of different
--     dimensionality"（array_agg 一律按 int[] 的数组处理，维度判断失真）
-- 所以这里不做集合运算、也不聚合数组，改为把每条路径转成文本、两侧各自排序成
-- text[] 之后直接比较数组。
DO $$
DECLARE
  c  refcursor;
  r  record;
  n1 int; n2 int;
  v_search  text[];
  v_cursor  text[] := '{}';
  v_cursor_sorted text[];
BEGIN
  SELECT count(*) INTO n1 FROM gtest.t_search(1,3,1000,0);

  SELECT gtest.t_cursor(1,'c_r10',3,1000,0) INTO c;
  LOOP
    FETCH c INTO r;
    EXIT WHEN NOT FOUND;
    n2 := n2 + 1;
    v_cursor := v_cursor || r.o_path::text;
  END LOOP;
  CLOSE c;

  IF n1 <> n2 THEN
    RAISE EXCEPTION '游标 % 行 vs search % 行', n2, n1;
  END IF;
  IF n1 <> 9 THEN
    RAISE EXCEPTION '期望 9 条路径，实际 %', n1;
  END IF;

  SELECT array_agg(o_path::text ORDER BY o_path::text)
    INTO v_search FROM gtest.t_search(1,3,1000,0);
  SELECT array_agg(p ORDER BY p)
    INTO v_cursor_sorted FROM unnest(v_cursor) AS p;
  IF v_cursor_sorted IS DISTINCT FROM v_search THEN
    RAISE EXCEPTION '游标与 search 的路径集合不一致: % vs %',
      v_cursor_sorted, v_search;
  END IF;
END $$;
\echo 'R10 OK'

\echo '--- R11: 参数校验抛错 ---'
DO $$
DECLARE ok boolean := false;
BEGIN
  BEGIN PERFORM * FROM gtest.t_search(1, 0, 100, 0); EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION 'i_depth=0 未报错'; END IF;
  ok := false;
  BEGIN PERFORM * FROM gtest.t_search(1, 999, 100, 0); EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION 'i_depth 超 cap 未报错'; END IF;
  ok := false;
  BEGIN PERFORM * FROM gtest.t_search(1, 3, 0, 0); EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION 'i_limit=0 未报错'; END IF;
END $$;
\echo 'R11 OK'

\echo '--- R12: _norm 拒绝非法输入 ---'
DO $$
DECLARE ok boolean;
BEGIN
  ok := false; BEGIN PERFORM graph_search._norm('{"table":"public.no_such_tbl"}'::jsonb);
    EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION '不存在的表未被拒绝'; END IF;
  ok := false; BEGIN PERFORM graph_search._norm('{"table":"public.t_edge","c1":"nope"}'::jsonb);
    EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION '不存在的列未被拒绝'; END IF;
  ok := false; BEGIN PERFORM graph_search._norm('{"table":"public.t_edge","weight_expr":"1; DROP TABLE x"}'::jsonb);
    EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION '含分号的 weight_expr 未被拒绝'; END IF;
  ok := false; BEGIN PERFORM graph_search._norm('{}'::jsonb);
    EXCEPTION WHEN OTHERS THEN ok := true; END;
  IF NOT ok THEN RAISE EXCEPTION '缺 table 未被拒绝'; END IF;
END $$;
\echo 'R12 OK'

\echo '--- R13: 幂等重装（覆盖安装不报错、不残留） ---'
DO $$
DECLARE n1 bigint; n2 bigint;
BEGIN
  n1 := (SELECT count(*) FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace
         WHERE ns.nspname='gtest');
  PERFORM graph_search.install('{"table":"public.t_edge","c1":"c1","c2":"c2","prop":"prop",
                               "target_schema":"gtest","prefix":"t","directed":true}'::jsonb);
  n2 := (SELECT count(*) FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace
         WHERE ns.nspname='gtest');
  IF n1 <> n2 THEN
    RAISE EXCEPTION '重装后函数数变了: % -> %', n1, n2;
  END IF;
END $$;
\echo 'R13 OK'

\echo '--- R14: uninstall 清理干净 ---'
DO $$
DECLARE left_cnt bigint; reg_cnt bigint;
BEGIN
  PERFORM graph_search.uninstall('gtest','t',false);
  SELECT count(*) INTO left_cnt
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='gtest' AND c.relname LIKE 't\_%';
  SELECT count(*) INTO reg_cnt FROM graph_search.registry
   WHERE target_schema='gtest' AND prefix='t';
  IF left_cnt <> 0 THEN RAISE EXCEPTION '残留对象 % 个', left_cnt; END IF;
  IF reg_cnt <> 0 THEN RAISE EXCEPTION 'registry 残留记录'; END IF;
  -- 装回来供后续使用
  PERFORM graph_search.install('{"table":"public.t_edge","c1":"c1","c2":"c2","prop":"prop",
                               "target_schema":"gtest","prefix":"t","directed":true}'::jsonb);
END $$;
\echo 'R14 OK'

\echo '--- R15: 最短路径沿途边属性与跳数一一对应 ---'
DO $$
DECLARE v jsonb[]; n int;
BEGIN
  SELECT o_link_props INTO v FROM gtest.t_shortest_path(1, 11, 10);
  IF v IS NULL OR array_length(v,1) IS NULL THEN
    RAISE EXCEPTION 'o_link_props 为空';
  END IF;
  SELECT array_length(o_path,1) INTO n FROM gtest.t_shortest_path(1, 11, 10);
  IF array_length(v,1) <> n - 1 THEN
    RAISE EXCEPTION 'o_link_props 有 % 条，路径 % 跳，应为 %', array_length(v,1), n, n - 1;
  END IF;
  IF EXISTS (SELECT 1 FROM unnest(v) x WHERE x IS NULL) THEN
    RAISE EXCEPTION 'o_link_props 含 NULL：有跳没有匹配到边';
  END IF;
END $$;
\echo 'R15 OK'

\echo '--- R16: spec 省略 prop 也能安装（prop/w 退化为 NULL/0） ---'
DO $$
DECLARE n int;
BEGIN
  PERFORM graph_search.install('{"table":"public.t_edge","target_schema":"gtest","prefix":"nop"}'::jsonb);
  SELECT count(*) INTO n FROM gtest.nop_search(1, 2, 10, 0);
  IF n < 1 THEN RAISE EXCEPTION '省略 prop 的实例搜索返回 0 行';
  END IF;
  PERFORM graph_search.uninstall('gtest', 'nop', false);
END $$;
\echo 'R16 OK'

\echo '--- R17: 注册表分布策略可观测（OpenTenBase 期望 REPLICATION，单机 single-node） ---'
DO $$
DECLARE v text := graph_search.registry_distribution();
BEGIN
  IF v NOT IN ('single-node', 'REPLICATION') THEN
    RAISE EXCEPTION '注册表分布策略应为 single-node 或 REPLICATION，实际 %；'
                      '在 OpenTenBase 上请执行 SELECT graph_search.set_registry_replication();', v;
  END IF;
END $$;
\echo 'R17 OK'

\echo '--- R18: set_registry_replication() 在单机上明确报错，在集群上幂等 ---'
DO $$
DECLARE v_cur text := graph_search.registry_distribution();
        v_out text;
        v_ok  boolean := false;
BEGIN
  IF v_cur = 'single-node' THEN
    BEGIN
      PERFORM graph_search.set_registry_replication();
    EXCEPTION WHEN OTHERS THEN
      v_ok := true;
    END;
    IF NOT v_ok THEN
      RAISE EXCEPTION '单机实例上 set_registry_replication() 应报错';
    END IF;
  ELSE
    v_out := graph_search.set_registry_replication();
    IF v_out IS NULL OR v_out = '' THEN
      RAISE EXCEPTION '集群上 set_registry_replication() 应返回说明文本';
    END IF;
    IF graph_search.registry_distribution() <> 'REPLICATION' THEN
      RAISE EXCEPTION 'set_registry_replication() 执行后仍不是 REPLICATION';
    END IF;
  END IF;
END $$;
\echo 'R18 OK'

\echo ''
\echo '==== 全部 18 项回归通过 ===='
