/*
 * 5. 最短路径：真正的 BFS 分层循环，命中即返回。
 *
 *    为什么不用递归 CTE + "where c2 = target limit 1"：
 *      - 那种写法依赖 RecursiveUnion 恰好以"层"为单位流式吐行 + LIMIT 1 早停。
 *        一旦执行计划里插进 Sort / Materialize 节点，早停就失效，
 *        退化成"把整个可达子图全展开再过滤"，内存和耗时都不可控。
 *      - 本实现显式维护 frontier(当前层) / visited(已访问)，每层只扫当前层，
 *        节点访问数是 O(可达节点数)，与路径条数无关；命中目标当层 return。
 *
 *    顺带修正原文 UDF3/UDF4 的一个输出 bug：原文递归项 path || g.c1 后
 *    最终 select path as o_path，返回的路径里【不含终点】。
 *    例：原文示例 {31208,319951,3199647} 而 o_point2=31957536，终点根本没在路径上。
 *    本实现返回含起终点的完整路径。
 */
CREATE OR REPLACE FUNCTION graph_search._ddl_fn_shortest(p_n jsonb)
RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_nt   text := p_n->>'node_type';
  v_fn   text := format('%I.%I', p_n->>'target_schema', p_n->>'prefix' || '_shortest_path');
  v_view text := format('%s.%s', quote_ident(p_n->>'target_schema'), quote_ident(p_n->>'prefix' || '_edge'));
BEGIN
  RETURN format($pgq$
CREATE OR REPLACE FUNCTION %1$s(
    i_p1     %2$s,
    i_p2     %2$s,
    i_depth  int    DEFAULT 3,
    i_limit  bigint DEFAULT 2000000000,
    i_weight float8 DEFAULT 0
) RETURNS TABLE(o_path %2$s[], o_depth int, o_link_props jsonb[])
LANGUAGE plpgsql STABLE AS $pgg$
DECLARE
  v_frontier %2$s[] := ARRAY[i_p1];
  v_visited  %2$s[] := ARRAY[i_p1];
  v_parents  jsonb   := '{}'::jsonb;
  v_new      %2$s[] := '{}';
  v_newmap   jsonb   := '{}'::jsonb;
  v_path     %2$s[] := '{}';
  v_cur      %2$s;
  v_d        int;
BEGIN
  IF i_depth < 1 THEN
    RAISE EXCEPTION 'i_depth 必须 >= 1，当前 %%', i_depth;
  END IF;
  IF i_depth > %3$s THEN
    RAISE EXCEPTION 'i_depth %% 超过本实例安装时的 max_depth_cap=%%', i_depth, %3$s;
  END IF;
  IF i_limit < 1 THEN
    RAISE EXCEPTION 'i_limit 必须 >= 1，当前 %%', i_limit;
  END IF;

  IF i_p1 = i_p2 THEN
    o_path := ARRAY[i_p1]; o_depth := 0; o_link_props := '{}'::jsonb[];
    RETURN NEXT; RETURN;
  END IF;

  FOR v_d IN 1..i_depth LOOP
    EXIT WHEN coalesce(array_length(v_frontier,1),0) = 0;

    -- 展开当前层：同一目标点只保留 w 最大的一条入边，保证结果确定性
    SELECT coalesce(jsonb_object_agg(t.dst::text, t.src::text), '{}'::jsonb),
           coalesce(array_agg(DISTINCT t.dst ORDER BY t.dst), '{}')
      INTO v_newmap, v_new
      FROM (SELECT DISTINCT ON (e.dst) e.dst, e.src, e.w
              FROM %4$s e
             WHERE e.src = ANY (v_frontier)
               AND e.dst <> ALL (v_visited)
               AND e.w >= i_weight
             ORDER BY e.dst, e.w DESC) t;

    EXIT WHEN coalesce(array_length(v_new,1),0) = 0;

    -- 先把本层的父子关系并入 parents，回溯时才查得到最后一跳
    v_parents := v_parents || v_newmap;

    -- 命中目标：回溯完整路径后立即返回
    IF i_p2 = ANY (v_new) THEN
      v_path := ARRAY[i_p2];
      v_cur  := i_p2;
      WHILE v_cur <> i_p1 LOOP
        v_cur := (v_parents ->> v_cur::text)::%2$s;
        EXIT WHEN v_cur IS NULL;
        v_path := v_path || v_cur;
      END LOOP;
      -- v_path 目前是 [终点 ... 起点]，翻转
      SELECT coalesce(array_agg(x ORDER BY t.ord DESC), '{}')
        INTO v_path
        FROM unnest(v_path) WITH ORDINALITY AS t(x, ord);

      o_path := v_path;
      o_depth := v_d;
      -- 路径相邻两点配对：src = v_path[1..n-1], dst = v_path[2..n]。
      -- 每条边取权重最大的一条，与 BFS 的 DISTINCT ON (dst) 口径一致。
      SELECT coalesce(array_agg(x.prop ORDER BY t.ord), '{}'::jsonb[])
        INTO o_link_props
        FROM unnest(v_path[1:array_length(v_path,1)-1],
                    v_path[2:array_length(v_path,1)]) WITH ORDINALITY AS t(src, dst, ord)
        CROSS JOIN LATERAL (
          SELECT e.prop
            FROM %4$s e
           WHERE e.src = t.src AND e.dst = t.dst
           ORDER BY e.w DESC
           LIMIT 1
        ) x;
      RETURN NEXT;
      RETURN;
    END IF;

    v_visited := v_visited || v_new;
    v_frontier := v_new;
  END LOOP;

  -- 深度内不可达：返回零行
  RETURN;
END $pgg$;$pgq$,
    v_fn, v_nt, p_n->>'max_depth_cap', v_view);
END
$pga$;


/* ---- 5.1 子图 JSON：绘图数据（nodes + edges） ---- */
CREATE OR REPLACE FUNCTION graph_search._ddl_fn_subgraph(p_n jsonb)
RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_nt  text := p_n->>'node_type';
  v_fn  text := format('%I.%I', p_n->>'target_schema', p_n->>'prefix' || '_subgraph');
BEGIN
  RETURN format($pgq$
CREATE OR REPLACE FUNCTION %1$s(
    i_root   %2$s,
    i_depth  int    DEFAULT 3,
    i_limit  bigint DEFAULT 100,
    i_weight float8 DEFAULT 0
) RETURNS jsonb
LANGUAGE plpgsql STABLE AS $pgg$
DECLARE v_sql text; v_res jsonb;
BEGIN
  IF i_depth < 1 THEN
    RAISE EXCEPTION 'i_depth 必须 >= 1，当前 %%', i_depth;
  END IF;
  IF i_depth > %3$s THEN
    RAISE EXCEPTION 'i_depth %% 超过本实例安装时的 max_depth_cap=%%', i_depth, %3$s;
  END IF;
  IF i_limit < 1 THEN
    RAISE EXCEPTION 'i_limit 必须 >= 1，当前 %%', i_limit;
  END IF;
  v_sql := $pgs$
WITH RECURSIVE sg(src, dst, prop, w, depth, path) AS (
        SELECT t.src, t.dst, t.prop, t.w, t.depth, t.path
          FROM (
                SELECT e.src, e.dst, e.prop, e.w, 1 AS depth, ARRAY[e.src] AS path
                  FROM %4$s e
                 WHERE e.src = $1
                   AND e.w >= $4
                 ORDER BY e.w DESC
                 LIMIT $3
               ) t
    UNION ALL
        SELECT t.src, t.dst, t.prop, t.w, t.depth, t.path
          FROM (
                SELECT e.src, e.dst, e.prop, e.w, sg.depth + 1 AS depth, sg.path || e.src AS path
                  FROM %4$s e
                  JOIN sg ON e.src = sg.dst
                 WHERE sg.depth < $2
                   AND e.src <> ALL(sg.path)   -- 新起点不能已在路径上
                   AND e.dst <> ALL(sg.path)   -- 新终点也不能已在路径上
                   AND e.w >= $4
                 ORDER BY e.w DESC
                 LIMIT $3
               ) t
), nodes AS (
        SELECT u.n AS id, min(u.d) AS depth
          FROM (
                SELECT unnest(sg.path) AS n, sg.depth - 1 AS d FROM sg
                UNION ALL
                SELECT sg.dst, sg.depth FROM sg
               ) u
         GROUP BY u.n
)
SELECT jsonb_build_object(
           'root', $1,
           'depth', $2,
           'node_count', (SELECT count(*) FROM nodes),
           'edge_count', (SELECT count(*) FROM sg),
           'nodes', coalesce((SELECT jsonb_agg(jsonb_build_object('id', id, 'depth', depth) ORDER BY id)
                                FROM nodes), '[]'::jsonb),
           'edges', coalesce((SELECT jsonb_agg(jsonb_build_object(
                                     'source', sg.src, 'target', sg.dst,
                                     'depth', sg.depth, 'prop', sg.prop) ORDER BY sg.depth, sg.src, sg.dst)
                                FROM sg), '[]'::jsonb)
         )$pgs$;
  EXECUTE v_sql INTO v_res USING i_root, i_depth, i_limit, i_weight;
  RETURN v_res;
END $pgg$;$pgq$,
    v_fn, v_nt, p_n->>'max_depth_cap',
    format('%s.%s', quote_ident(p_n->>'target_schema'), quote_ident(p_n->>'prefix' || '_edge')));
END
$pga$;


/* ---- 5.2 分层统计：每层边数 / 新增点数 / 平均路径长度 ---- */
CREATE OR REPLACE FUNCTION graph_search._ddl_fn_stats(p_n jsonb)
RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_nt  text := p_n->>'node_type';
  v_fn  text := format('%I.%I', p_n->>'target_schema', p_n->>'prefix' || '_stats');
BEGIN
  RETURN format($pgq$
CREATE OR REPLACE FUNCTION %1$s(
    i_root   %2$s,
    i_depth  int    DEFAULT 3,
    i_limit  bigint DEFAULT 100,
    i_weight float8 DEFAULT 0
) RETURNS TABLE(o_depth int, o_edges bigint, o_new_nodes bigint, o_avg_path_len numeric)
LANGUAGE plpgsql STABLE AS $pgg$
DECLARE v_sql text;
BEGIN
  IF i_depth < 1 THEN
    RAISE EXCEPTION 'i_depth 必须 >= 1，当前 %%', i_depth;
  END IF;
  IF i_depth > %3$s THEN
    RAISE EXCEPTION 'i_depth %% 超过本实例安装时的 max_depth_cap=%%', i_depth, %3$s;
  END IF;
  IF i_limit < 1 THEN
    RAISE EXCEPTION 'i_limit 必须 >= 1，当前 %%', i_limit;
  END IF;
  v_sql := $pgs$%4$s$pgs$;
  RETURN QUERY EXECUTE v_sql USING i_root, i_depth, i_limit, i_weight;
END $pgg$;$pgq$,
    v_fn, v_nt, p_n->>'max_depth_cap',
    format($pgs$
WITH RECURSIVE sg(src, dst, prop, w, depth, path) AS (
        SELECT t.src, t.dst, t.prop, t.w, t.depth, t.path
          FROM (
                SELECT e.src, e.dst, e.prop, e.w, 1 AS depth, ARRAY[e.src] AS path
                  FROM %1$s e
                 WHERE e.src = $1
                   AND e.w >= $4
                 ORDER BY e.w DESC
                 LIMIT $3
               ) t
    UNION ALL
        SELECT t.src, t.dst, t.prop, t.w, t.depth, t.path
          FROM (
                SELECT e.src, e.dst, e.prop, e.w, sg.depth + 1 AS depth, sg.path || e.src AS path
                  FROM %1$s e
                  JOIN sg ON e.src = sg.dst
                 WHERE sg.depth < $2
                   AND e.src <> ALL(sg.path)   -- 新起点不能已在路径上
                   AND e.dst <> ALL(sg.path)   -- 新终点也不能已在路径上
                   AND e.w >= $4
                 ORDER BY e.w DESC
                 LIMIT $3
               ) t
)
SELECT sg.depth,
       count(*)::bigint,
       count(DISTINCT sg.dst)::bigint,
       round(avg(array_length(sg.path,1)), 2)
  FROM sg
 GROUP BY sg.depth
 ORDER BY sg.depth$pgs$,
      format('%s.%s', quote_ident(p_n->>'target_schema'), quote_ident(p_n->>'prefix' || '_edge'))));
END
$pga$;
