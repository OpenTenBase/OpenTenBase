/*
 * 4. DDL 构造器 —— 全部只产出文本，不执行。plan() 直接把它们吐给用户预览，
 *    install() 才逐条 EXECUTE。这样"元函数"和"执行"彻底解耦，可审计。
 *
 *  dollar-quoting 分层约定（避免嵌套撞名）：
 *    $pga$  本扩展自身函数的体
 *    $pgq$   format() 模板串（内含生成的代码）
 *    $pgg$   被生成的 plpgsql 函数体
 *    $pgs$   被生成的函数体内部的动态 SQL 串
 */

/* ---- 4.1 统一边视图：有向直映射，无向 UNION ALL 双向展开 ---- */
CREATE OR REPLACE FUNCTION graph_search._ddl_edge_view(p_n jsonb)
RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_tbl   text := p_n->>'table';
  v_c1    text := format('%I', p_n->>'c1');
  v_c2    text := format('%I', p_n->>'c2');
  -- prop 可省略：format('%I', NULL) 会直接报错，必须先判空再格式化；
  -- 这里直接产出完整表达式（含 e. 前缀），缺省时不能拼成 e.NULL::jsonb
  v_prop  text := CASE WHEN p_n->>'prop' IS NULL THEN 'NULL::jsonb'
                       ELSE format('e.%I', p_n->>'prop') END;
  v_w     text := p_n->>'weight_expr';
  v_view  text := format('%I.%I', p_n->>'target_schema', p_n->>'prefix' || '_edge');
  v_one   text;
BEGIN
  v_one := format(
    'SELECT e.%1$s AS src, e.%2$s AS dst, %3$s AS prop, (%4$s) AS w FROM %5$s e',
    v_c1, v_c2, v_prop, v_w, v_tbl);

  IF (p_n->>'directed')::boolean THEN
    RETURN 'CREATE OR REPLACE VIEW ' || v_view || ' AS ' || v_one || ';';
  END IF;

  RETURN 'CREATE OR REPLACE VIEW ' || v_view || ' AS ' || v_one || ' UNION ALL ' ||
         format('SELECT e.%1$s AS src, e.%2$s AS dst, %3$s AS prop, (%4$s) AS w FROM %5$s e',
                v_c2, v_c1, v_prop, v_w, v_tbl) || ';';
END
$pga$;


/* ---- 4.2 N 度搜索的递归 CTE 主体（多个函数复用，抽出来保证口径一致） ----
 *    $1=root $2=depth $3=limit $4=min_weight
 */
CREATE OR REPLACE FUNCTION graph_search._cte_nsearch(
  p_n jsonb,
  p_deep_limit boolean DEFAULT true
) RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_view text := format('%s.%s', quote_ident(p_n->>'target_schema'), quote_ident(p_n->>'prefix' || '_edge'));
BEGIN
  RETURN format($pgs$
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
                SELECT e.src, e.dst, e.prop, e.w,
                       sg.depth + 1 AS depth,
                       sg.path || e.src AS path
                  FROM %1$s e
                  JOIN sg ON e.src = sg.dst
                 WHERE %2$s
                   AND e.src <> ALL(sg.path)   -- 新起点不能已在路径上
                   AND e.dst <> ALL(sg.path)   -- 新终点也不能已在路径上
                   AND e.w >= $4
                 ORDER BY e.w DESC
                 LIMIT $3
               ) t
)
SELECT sg.path || sg.dst AS o_path,
       sg.src            AS o_point1,
       sg.dst            AS o_point2,
       sg.prop           AS o_link_prop,
       sg.depth          AS o_depth
  FROM sg$pgs$,
    v_view,
    CASE WHEN p_deep_limit THEN 'sg.depth < $2' ELSE 'true' END);
END
$pga$;


/* ---- 4.3 <prefix>_search(root, depth, limit, weight) : N 度搜索，返回记录集 ---- */
CREATE OR REPLACE FUNCTION graph_search._ddl_fn_search(p_n jsonb)
RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_nt  text := p_n->>'node_type';
  v_fn  text := format('%I.%I', p_n->>'target_schema', p_n->>'prefix' || '_search');
BEGIN
  RETURN format($pgq$
CREATE OR REPLACE FUNCTION %1$s(
    i_root   %2$s,
    i_depth  int    DEFAULT 3,
    i_limit  bigint DEFAULT 100,
    i_weight float8 DEFAULT 0
) RETURNS TABLE(o_path %2$s[], o_point1 %2$s, o_point2 %2$s, o_link_prop jsonb, o_depth int)
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
    v_fn, v_nt, p_n->>'max_depth_cap', graph_search._cte_nsearch(p_n, true));
END
$pga$;


/* ---- 4.4 <prefix>_cursor(root, curname, depth, limit, weight) : 游标版 ---- */
CREATE OR REPLACE FUNCTION graph_search._ddl_fn_cursor(p_n jsonb)
RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_nt text := p_n->>'node_type';
  v_fn text := format('%I.%I', p_n->>'target_schema', p_n->>'prefix' || '_cursor');
BEGIN
  RETURN format($pgq$
CREATE OR REPLACE FUNCTION %1$s(
    i_root   %2$s,
    i_res    name,
    i_depth  int    DEFAULT 3,
    i_limit  bigint DEFAULT 100,
    i_weight float8 DEFAULT 0
) RETURNS refcursor
LANGUAGE plpgsql STABLE AS $pgg$
DECLARE v_sql text; v_cur refcursor := i_res;
BEGIN
  IF i_depth < 1 THEN
    RAISE EXCEPTION 'i_depth 必须 >= 1，当前 %%', i_depth;
  END IF;
  IF i_depth > %3$s THEN
    RAISE EXCEPTION 'i_depth %% 超过本实例安装时的 max_depth_cap=%%', i_depth, %3$s;
  END IF;
  v_sql := $pgs$%4$s$pgs$;
  OPEN v_cur FOR EXECUTE v_sql USING i_root, i_depth, i_limit, i_weight;
  RETURN v_cur;
END $pgg$;$pgq$,
    v_fn, v_nt, p_n->>'max_depth_cap', graph_search._cte_nsearch(p_n, true));
END
$pga$;
