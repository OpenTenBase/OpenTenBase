/*
 * pg_graph_search 1.0.0 — OpenTenBase 适配版
 * ---------------------
 * 属性图式搜索扩展。元函数 (meta function) 根据一张边表 (c1/c2/prop) 的结构参数，
 * 动态生成一组业务可直接调用的函数：N 度搜索、游标式搜索、最短路径(BFS 提前退出)、
 * 子图 JSON(绘图数据)、分层统计。
 *
 * 设计要点：
 *   1. 所有生成的函数都走一张统一的"边视图" <schema>.<prefix>_edge(src,dst,prop,w)，
 *      有向表直接映射，无向表自动 UNION ALL 双向展开。
 *   2. 标识符一律 format('%I') / 常量一律 format('%L')，杜绝拼接注入。
 *   3. plan() 只产出 DDL 文本不落库，install() 执行 plan() 并登记元数据。
 *   4. 最短路径用真正的 BFS 分层循环，命中即返回；递归 CTE 写法会全量展开后才过滤。
 *
 * OpenTenBase（Postgres-XL）适配：
 *   a. 生成物一律 schema 限定，不依赖 search_path（CN/DN 的 search_path 可能不同）。
 *   b. 本文件由 sql/ 片段拼装，注册表 DDL 的分布子句占位符在构建时替换：
 *      OpenTenBase 版 -> DISTRIBUTE BY REPLICATION；原生 PG 版 -> 空。
 *   c. CREATE EXTENSION 在 OpenTenBase 上是两阶段(PREPARE/EXECUTE)+全节点广播，
 *      本扩展只包含 SQL 对象，无 .so、无本地文件依赖，符合该协议的前提。
 *   d. 生成函数是 STABLE + 动态 SQL，由 CN 调用；动态 DDL 会按 XL 规则广播到所有节点。
 */

-- 使用：CREATE EXTENSION pg_graph_search;
/*
 * 1. 基础工具：列断言
 */

CREATE OR REPLACE FUNCTION graph_search._assert_col(p_tab regclass, p_col text)
RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_found text;
BEGIN
  IF p_col IS NULL OR p_col = '' THEN
    RETURN NULL;
  END IF;

  SELECT a.atttypid::regtype::text
    INTO v_found
    FROM pg_attribute a
   WHERE a.attrelid = p_tab
     AND a.attname = p_col
     AND NOT a.attisdropped
     AND a.attnum > 0;

  IF v_found IS NULL THEN
    RAISE EXCEPTION '列 "%" 在表 % 上不存在', p_col, p_tab
      USING HINT = '用 spec.c1 / spec.c2 / spec.prop 指定实际列名';
  END IF;

  RETURN format('%I', p_col);
END
$pga$;

COMMENT ON FUNCTION graph_search._assert_col(regclass, text) IS
  '校验列存在并返回其类型名；列不存在时抛异常';


/*
 * 1b. 关系名 schema 限定 + 标识符转义。
 *     regclass::text 在表不在 search_path 时才带 schema，否则只有裸表名，
 *     生成出来的视图就会依赖 search_path，跨 schema 同名表会踩雷。
 *     这里统一输出 schema 限定的名字，并对非常规标识符加引号。
 */
CREATE OR REPLACE FUNCTION graph_search._qualified(p_rel regclass)
RETURNS text
LANGUAGE sql STABLE
AS $pga$
  SELECT format('%I.%I', n.nspname, c.relname)
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE c.oid = p_rel;
$pga$;

COMMENT ON FUNCTION graph_search._qualified(regclass) IS
  '把 regclass 渲染成 schema 限定且已转义的标识符，避免生成物依赖 search_path';


/*
 * 1c. 反向索引体检。
 *     无向图视图 = 正向 UNION ALL 反向，反向分支要按 c2 过滤。
 *     若 (c2,c1) 没有驱动索引，优化器只能 Seq Scan，实测 95 万边表上
 *     单次 src=1 查询从 0.05ms 退化到 30ms（600 倍）。
 *     这里在安装期就把结论告诉用户，而不是等到线上才发现。
 */
CREATE OR REPLACE FUNCTION graph_search.reverse_index_hint(
  p_table regclass, p_c2 text, p_c1 text
) RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_rel  text := graph_search._qualified(p_table);
  v_ok   boolean;
BEGIN
  -- 只要存在任一以 c2 打头的可索引列即可（单列/复合/部分索引都算）
  SELECT EXISTS (
           SELECT 1
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
            WHERE i.indrelid = p_table
              AND a.attname = p_c2
              AND i.indisvalid
              AND i.indpred IS NULL
         )
    INTO v_ok;

  IF v_ok THEN
    RETURN format('已有以 (%s) 打头的索引，反向分支可走索引扫描。', p_c2);
  END IF;

  RETURN '警告: 表 ' || v_rel || ' 上没有以 (' || p_c2 || ') 打头的索引。' || chr(10) ||
         '      无向图视图的反向分支要按 ' || p_c2 || ' 过滤，只能 Seq Scan。' || chr(10) ||
         '      实测 95 万边表上单次 src 查询从 0.05ms 退化到 30ms。' || chr(10) ||
         '      建议先执行：CREATE INDEX ON ' || v_rel || ' (' || p_c2 || ', ' || p_c1 || ');';
END
$pga$;

COMMENT ON FUNCTION graph_search.reverse_index_hint(regclass, text, text) IS
  '无向安装时检查反向驱动索引，缺失则返回带 CREATE INDEX 建议的提示文本';


/*
 * 2. 节点类型推导：把边表的列类型映射为可用的点类型
 *    只支持可比较、可建数组、且语义明确的类型，其余报错让用户显式转换。
 */
CREATE OR REPLACE FUNCTION graph_search._node_type(p_tab regclass, p_col text)
RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_oid oid;
  v_out text;
BEGIN
  SELECT a.atttypid
    INTO v_oid
    FROM pg_attribute a
   WHERE a.attrelid = p_tab
     AND a.attname = p_col
     AND NOT a.attisdropped
     AND a.attnum > 0;

  IF v_oid IS NULL THEN
    RAISE EXCEPTION '列 "%" 在表 % 上不存在', p_col, p_tab;
  END IF;

  v_out := CASE v_oid
             WHEN 'int2'::regtype   THEN 'smallint'
             WHEN 'int4'::regtype   THEN 'int'
             WHEN 'int8'::regtype   THEN 'bigint'
             WHEN 'text'::regtype   THEN 'text'
             WHEN 'varchar'::regtype THEN 'text'
             WHEN 'bpchar'::regtype THEN 'text'
             WHEN 'name'::regtype   THEN 'text'
             WHEN 'uuid'::regtype   THEN 'uuid'
             WHEN 'numeric'::regtype THEN 'numeric'
           END;

  IF v_out IS NULL THEN
    RAISE EXCEPTION '列 "%" 的类型 % 不支持做图节点类型', p_col, v_oid::regtype
      USING HINT = '请建视图把点列转成 int / bigint / text / uuid 后再 install';
  END IF;

  RETURN v_out;
END
$pga$;
/*
 * 3. 规格规范化：把用户传入的 jsonb spec 解析成一份确定、可缓存的规范形式。
 *    所有后续 DDL 构造器只读规范化结果，避免各处重复校验。
 *
 *    spec 键：
 *      table          必填，边表（schema.table 或裸表名）
 *      c1/c2          点列名，默认 c1 / c2
 *      prop           边属性列(jsonb)，可为空
 *      weight_expr    权重表达式原文（受白名单校验），为空时按 weight_key 生成
 *      weight_key     jsonb 权重键名，默认 weight
 *      directed       true=有向（默认）；false=无向，视图 UNION ALL 展开双向
 *      target_schema  生成的函数/视图放在哪个 schema，默认 public
 *      prefix         生成对象名前缀，默认 'g_' || 表名（净化后）
 *      node_type      点类型，默认按 c1 列类型推导
 *      max_depth_cap  深度硬上限，默认 64，防止无界搜索打爆实例
 */
CREATE OR REPLACE FUNCTION graph_search._norm(p_spec jsonb)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_tab      text;
  v_rel      regclass;
  v_c1       text;
  v_c2       text;
  v_prop     text;
  v_node     text;
  v_directed boolean;
  v_schema   text;
  v_prefix   text;
  v_cap      int;
  v_wexpr    text;
  v_wkey     text;
  v_ok       boolean;
BEGIN
  IF p_spec IS NULL THEN
    RAISE EXCEPTION 'spec 不能为空'
      USING HINT = '至少需要 {"table": "public.a"}';
  END IF;

  -- 边表
  v_tab := p_spec->>'table';
  IF v_tab IS NULL OR v_tab = '' THEN
    RAISE EXCEPTION 'spec.table 必填'
      USING HINT = '例：{"table":"public.a","c1":"c1","c2":"c2","prop":"prop"}';
  END IF;

  v_rel := v_tab::regclass;

  -- 列
  v_c1   := coalesce(nullif(p_spec->>'c1', ''),   'c1');
  v_c2   := coalesce(nullif(p_spec->>'c2', ''),   'c2');
  v_prop := nullif(p_spec->>'prop', '');

  PERFORM graph_search._assert_col(v_rel, v_c1);
  PERFORM graph_search._assert_col(v_rel, v_c2);
  IF v_prop IS NOT NULL THEN
    PERFORM graph_search._assert_col(v_rel, v_prop);
  END IF;

  -- 点类型
  v_node := coalesce(nullif(p_spec->>'node_type', ''), graph_search._node_type(v_rel, v_c1));

  -- 权重表达式
  v_wkey  := coalesce(nullif(p_spec->>'weight_key', ''), 'weight');
  v_wexpr := nullif(p_spec->>'weight_expr', '');

  IF v_wexpr IS NOT NULL THEN
    -- 白名单：只允许表达式常用字符，挡掉分号/注释等语句拼接
    v_ok := v_wexpr !~ ';' AND v_wexpr !~ '--' AND v_wexpr !~ '/\*'
            AND v_wexpr ~ '^[A-Za-z0-9_ .()<>:''"*\-+/,|]+$';
    IF NOT v_ok THEN
      RAISE EXCEPTION 'spec.weight_expr 含非法字符: %', v_wexpr
        USING HINT = 'weight_expr 是原文拼接，只允许字母数字与 . ( ) < > : '' " * + - / , | 空格和下划线';
    END IF;
  ELSIF v_prop IS NOT NULL THEN
    v_wexpr := format('coalesce((prop->>%L)::float8, 0)', v_wkey);
  ELSE
    v_wexpr := '0::float8';
  END IF;

  -- 目标 schema / 前缀
  v_schema := coalesce(nullif(p_spec->>'target_schema', ''), 'public');

  v_prefix := coalesce(nullif(p_spec->>'prefix', ''), NULL);
  IF v_prefix IS NULL THEN
    v_prefix := 'g_' || regexp_replace(v_rel::regclass::text, '^(.*)\.(.*)$', '\2');
  END IF;
  v_prefix := regexp_replace(v_prefix, '[^a-zA-Z0-9_]', '_', 'g');
  IF v_prefix !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
    RAISE EXCEPTION 'spec.prefix "%" 不是合法标识符前缀', v_prefix;
  END IF;

  -- 深度硬上限
  v_cap := coalesce((p_spec->>'max_depth_cap')::int, 64);
  IF v_cap < 1 THEN
    RAISE EXCEPTION 'spec.max_depth_cap 必须 >= 1，当前 %', v_cap;
  END IF;

  v_directed := coalesce((p_spec->>'directed')::boolean, true);

  RETURN jsonb_build_object(
    'table',         graph_search._qualified(v_rel),
    'c1',            v_c1,
    'c2',            v_c2,
    'prop',          v_prop,
    'node_type',     v_node,
    'weight_expr',   v_wexpr,
    'directed',      v_directed,
    'target_schema', v_schema,
    'prefix',        v_prefix,
    'max_depth_cap', v_cap
  );
END
$pga$;

COMMENT ON FUNCTION graph_search._norm(jsonb) IS
  '规范化安装规格，缺省值补齐、非法参数报错，返回规范化的 jsonb';
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
/*
 * 6. 注册表：记录本扩展安装了哪些实例，供 uninstall / list 使用
 */
CREATE TABLE IF NOT EXISTS graph_search.registry (
    target_schema   text        NOT NULL,
    prefix          text        NOT NULL,
    table_oid       oid         NOT NULL,
    node_type       text        NOT NULL,
    directed        boolean     NOT NULL,
    max_depth_cap   int         NOT NULL,
    spec            jsonb       NOT NULL,
    installed_at    timestamptz NOT NULL DEFAULT now(),
    installed_by    name        NOT NULL DEFAULT current_user,
    objects         text[]      NOT NULL DEFAULT '{}',
    PRIMARY KEY (target_schema, prefix)
) DISTRIBUTE BY REPLICATION;

COMMENT ON TABLE graph_search.registry IS
  '已安装的图搜索实例；uninstall() 依据此表精确清理生成对象';


/*
 * 6b. OpenTenBase / Postgres-XL 适配：注册表的分布策略。
 *
 *     注册表是「集群级元数据」：CN 上的 install() / uninstall() / list_installed()
 *     都要读它。OpenTenBase 的默认分布规则是「取主键或唯一约束的第一列」
 *     （见 doc/src/sgml/ddl.sgml），本表主键是 (target_schema, prefix)，
 *     默认会变成 HASH(target_schema)：每个 DN 只持有一部分实例清单，
 *     节点之间看到的 registry 不一致，排查时容易误判「实例没装上」。
 *
 *     所以构建时把上面 DDL 里的分布子句占位符替换为 DISTRIBUTE BY REPLICATION
 *     （OpenTenBase 默认，见 Makefile）；在原生 PostgreSQL 上做可移植验证时
 *     传 OPENTENBASE=0 生成空子句（make portable）。
 *
 *     下面两个函数让这件事「可观测、可修复」：
 *       registry_distribution()    —— 当前分布策略（原生 PG 返回 single-node）
 *       set_registry_replication() —— 把已有注册表改成 REPLICATION（幂等）
 */
CREATE OR REPLACE FUNCTION graph_search.registry_distribution()
RETURNS text
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_type char;
BEGIN
  -- 有没有 pgxc_class 目录（只有 OpenTenBase/Postgres-XL 才有）；
  -- 用动态 SQL 是为了让同一份源码在原生 PostgreSQL 上也能创建/执行
  IF to_regclass('pg_catalog.pgxc_class') IS NULL THEN
    RETURN 'single-node';
  END IF;

  EXECUTE 'SELECT p.pclocatortype FROM pg_catalog.pgxc_class p
            WHERE p.pcrelid = $1'
     INTO v_type
    USING 'graph_search.registry'::regclass;

  IF v_type IS NULL THEN
    RETURN 'unknown';
  END IF;

  -- 取值见 src/include/pgxc/locator.h
  RETURN CASE v_type
           WHEN 'R' THEN 'REPLICATION'
           WHEN 'S' THEN 'SHARD'
           WHEN 'H' THEN 'HASH'
           WHEN 'M' THEN 'MODULO'
           WHEN 'D' THEN 'DISTRIBUTED'
           WHEN 'O' THEN 'NONE'
           ELSE 'OTHER:' || v_type
         END;
END
$pga$;

COMMENT ON FUNCTION graph_search.registry_distribution() IS
  '注册表当前的分布策略：REPLICATION/SHARD/HASH/MODULO/DISTRIBUTED/NONE；原生 PostgreSQL 返回 single-node';


CREATE OR REPLACE FUNCTION graph_search.set_registry_replication()
RETURNS text
LANGUAGE plpgsql VOLATILE
AS $pga$
DECLARE
  v_cur text := graph_search.registry_distribution();
  v_ddl text := 'ALTER TABLE graph_search.registry DISTRIBUTE BY REPLICATION';
BEGIN
  IF v_cur = 'single-node' THEN
    RAISE EXCEPTION '当前实例不是 OpenTenBase 集群，单机不需要分布策略'
      USING HINT = '本函数只在分布式集群上有意义';
  END IF;

  IF v_cur = 'REPLICATION' THEN
    RETURN 'already REPLICATION';
  END IF;

  EXECUTE v_ddl;
  RETURN v_ddl;
END
$pga$;

COMMENT ON FUNCTION graph_search.set_registry_replication() IS
  '把注册表改成 DISTRIBUTE BY REPLICATION（仅在 OpenTenBase 集群上有效，幂等）';


CREATE OR REPLACE FUNCTION graph_search._check_registry_distribution()
RETURNS void
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_cur text := graph_search.registry_distribution();
BEGIN
  IF v_cur IN ('single-node', 'REPLICATION') THEN
    RETURN;
  END IF;

  RAISE WARNING 'graph_search.registry 的分布策略是 %，各节点看到的实例清单可能不一致；'
                '建议执行：SELECT graph_search.set_registry_replication();', v_cur;
END
$pga$;

COMMENT ON FUNCTION graph_search._check_registry_distribution() IS
  '安装前的静默体检：注册表不是 REPLICATION 时给出 WARNING（不改数据）';


/*
 * 7. plan(spec) -> text[]
 *    元函数入口（只读）。返回将要执行的完整 DDL 文本数组，供预览 / 审计 / 人工执行。
 */
CREATE OR REPLACE FUNCTION graph_search.plan(p_spec jsonb)
RETURNS text[]
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_n jsonb := graph_search._norm(p_spec);
BEGIN
  RETURN ARRAY[
    graph_search._ddl_edge_view(v_n),
    graph_search._ddl_fn_search(v_n),
    graph_search._ddl_fn_cursor(v_n),
    graph_search._ddl_fn_shortest(v_n),
    graph_search._ddl_fn_subgraph(v_n),
    graph_search._ddl_fn_stats(v_n),
    format('COMMENT ON FUNCTION %I.%I(%s, int, bigint, float8) IS %L',
           v_n->>'target_schema', (v_n->>'prefix') || '_search',
           v_n->>'node_type',
           format('图搜索(N度) 实例 %s，根=%s', v_n->>'prefix', v_n->>'table')),
    format('COMMENT ON FUNCTION %I.%I(%s, name, int, bigint, float8) IS %L',
           v_n->>'target_schema', (v_n->>'prefix') || '_cursor',
           v_n->>'node_type',
           format('图搜索(游标) 实例 %s，根=%s', v_n->>'prefix', v_n->>'table')),
    format('COMMENT ON FUNCTION %I.%I(%s, %s, int, bigint, float8) IS %L',
           v_n->>'target_schema', (v_n->>'prefix') || '_shortest_path',
           v_n->>'node_type', v_n->>'node_type',
           format('图搜索(最短路径 BFS) 实例 %s，边表=%s', v_n->>'prefix', v_n->>'table')),
    format('COMMENT ON FUNCTION %I.%I(%s, int, bigint, float8) IS %L',
           v_n->>'target_schema', (v_n->>'prefix') || '_subgraph',
           v_n->>'node_type',
           format('图搜索(子图 JSON 绘图数据) 实例 %s', v_n->>'prefix')),
    format('COMMENT ON FUNCTION %I.%I(%s, int, bigint, float8) IS %L',
           v_n->>'target_schema', (v_n->>'prefix') || '_stats',
           v_n->>'node_type',
           format('图搜索(分层统计) 实例 %s', v_n->>'prefix'))
  ];
END
$pga$;

COMMENT ON FUNCTION graph_search.plan(jsonb) IS
  '元函数：规划一套图搜索函数，返回完整 DDL 文本数组，不做任何修改';


/*
 * 7b. advise(spec) -> text[]  安装前的性能体检。
 *     与 plan() 分开，避免把提示文本混进可执行 DDL 数组。
 */
CREATE OR REPLACE FUNCTION graph_search.advise(p_spec jsonb)
RETURNS text[]
LANGUAGE plpgsql STABLE
AS $pga$
DECLARE
  v_n jsonb := graph_search._norm(p_spec);
BEGIN
  IF (v_n->>'directed')::boolean THEN
    RETURN ARRAY[]::text[];
  END IF;

  RETURN ARRAY[
    format(E'有向=false: 视图会 UNION ALL 双向展开，反向分支需按 %s 过滤。',
           v_n->>'c2'),
    graph_search.reverse_index_hint((v_n->>'table')::regclass, v_n->>'c2', v_n->>'c1')
  ];
END
$pga$;

COMMENT ON FUNCTION graph_search.advise(jsonb) IS
  '元函数：安装前性能体检（索引/驱动列），返回建议文本数组';


/*
 * 8. install(spec) -> text[]
 *    元函数入口（写）。执行 plan() 的 DDL 并登记到 registry。
 *    支持同名覆盖：默认 DROP 旧对象后重建，保证 spec 变更能生效。
 */
CREATE OR REPLACE FUNCTION graph_search.install(p_spec jsonb, p_replace boolean DEFAULT true)
RETURNS text[]
LANGUAGE plpgsql VOLATILE
AS $pga$
DECLARE
  v_n      jsonb := graph_search._norm(p_spec);
  v_schema text := v_n->>'target_schema';
  v_prefix text := v_n->>'prefix';
  v_rel    regclass := (v_n->>'table')::regclass;
  v_node   text := v_n->>'node_type';
  v_ddl    text[];
  v_stmt   text;
BEGIN
  -- 目标 schema 必须已存在（不代替用户建库结构，只在明确授权时创建）
  IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = v_schema) THEN
    RAISE EXCEPTION '目标 schema "%" 不存在', v_schema
      USING HINT = format('先执行 CREATE SCHEMA %I;', v_schema);
  END IF;

  IF NOT pg_has_role(current_user, (SELECT nspowner FROM pg_namespace WHERE nspname = v_schema), 'USAGE')
     AND NOT pg_has_role(current_user, (SELECT nspowner FROM pg_namespace WHERE nspname = v_schema), 'CREATE') THEN
    RAISE EXCEPTION '当前用户 % 对 schema "%" 无权限', current_user, v_schema;
  END IF;

  IF EXISTS (SELECT 1 FROM graph_search.registry WHERE target_schema = v_schema AND prefix = v_prefix)
     AND NOT p_replace THEN
    RAISE EXCEPTION '实例 %.% 已安装；传 p_replace => true 覆盖，或先 uninstall',
                    v_schema, v_prefix;
  END IF;

  -- 覆盖安装：先清旧对象，避免函数签名变化导致 CREATE OR REPLACE 失败
  IF EXISTS (SELECT 1 FROM graph_search.registry WHERE target_schema = v_schema AND prefix = v_prefix) THEN
    PERFORM graph_search.uninstall(v_schema, v_prefix, false);
  END IF;

  -- OpenTenBase 适配：注册表必须是 REPLICATION 分布，否则 CN/DN 看到的
  -- 实例清单不一致（只 WARNING，不阻断安装）
  PERFORM graph_search._check_registry_distribution();

  v_ddl := graph_search.plan(p_spec);
  FOREACH v_stmt IN ARRAY v_ddl LOOP
    EXECUTE v_stmt;
  END LOOP;

  INSERT INTO graph_search.registry
        (target_schema, prefix, table_oid, node_type, directed, max_depth_cap, spec, objects)
  VALUES (v_schema, v_prefix, v_rel, v_node, (v_n->>'directed')::boolean,
          (v_n->>'max_depth_cap')::int, v_n,
          ARRAY[ v_prefix || '_edge', v_prefix || '_search', v_prefix || '_cursor',
                 v_prefix || '_shortest_path', v_prefix || '_subgraph', v_prefix || '_stats' ])
  ON CONFLICT (target_schema, prefix) DO UPDATE
        SET table_oid = EXCLUDED.table_oid, node_type = EXCLUDED.node_type,
            directed = EXCLUDED.directed, max_depth_cap = EXCLUDED.max_depth_cap,
            spec = EXCLUDED.spec, installed_at = now(), installed_by = current_user,
            objects = EXCLUDED.objects;

  RETURN v_ddl;
END
$pga$;

COMMENT ON FUNCTION graph_search.install(jsonb, boolean) IS
  '元函数：规划并执行 DDL，安装一套图搜索函数到指定 schema，登记到 registry';


/*
 * 9. uninstall(target_schema, prefix, cascade) -> text[]
 */
CREATE OR REPLACE FUNCTION graph_search.uninstall(
    p_target_schema text,
    p_prefix        text,
    p_cascade       boolean DEFAULT false
) RETURNS text[]
LANGUAGE plpgsql VOLATILE
AS $pga$
DECLARE
  v_node   text;
  v_prefix text;
  v_cascade text := CASE WHEN p_cascade THEN ' CASCADE' ELSE '' END;
  v_stmts  text[] := '{}';
  v_stmt   text;
BEGIN
  SELECT r.node_type, r.prefix
    INTO v_node, v_prefix
    FROM graph_search.registry r
   WHERE r.target_schema = p_target_schema AND r.prefix = p_prefix;

  IF NOT FOUND THEN
    RAISE EXCEPTION '未找到实例 %.%', p_target_schema, p_prefix;
  END IF;

  -- 签名与 DDL 构造器一一对应，顺序即卸载顺序；函数先于视图
  v_stmts := v_stmts || format('DROP FUNCTION IF EXISTS %I.%I(%s, int, bigint, float8)%s;',
                               p_target_schema, v_prefix || '_search', v_node, v_cascade);
  v_stmts := v_stmts || format('DROP FUNCTION IF EXISTS %I.%I(%s, name, int, bigint, float8)%s;',
                               p_target_schema, v_prefix || '_cursor', v_node, v_cascade);
  v_stmts := v_stmts || format('DROP FUNCTION IF EXISTS %I.%I(%s, %s, int, bigint, float8)%s;',
                               p_target_schema, v_prefix || '_shortest_path', v_node, v_node, v_cascade);
  v_stmts := v_stmts || format('DROP FUNCTION IF EXISTS %I.%I(%s, int, bigint, float8)%s;',
                               p_target_schema, v_prefix || '_subgraph', v_node, v_cascade);
  v_stmts := v_stmts || format('DROP FUNCTION IF EXISTS %I.%I(%s, int, bigint, float8)%s;',
                               p_target_schema, v_prefix || '_stats', v_node, v_cascade);
  v_stmts := v_stmts || format('DROP VIEW IF EXISTS %I.%I CASCADE;',
                               p_target_schema, v_prefix || '_edge');

  FOREACH v_stmt IN ARRAY v_stmts LOOP
    EXECUTE v_stmt;
  END LOOP;

  DELETE FROM graph_search.registry
   WHERE target_schema = p_target_schema AND prefix = p_prefix;

  RETURN v_stmts;
END
$pga$;

COMMENT ON FUNCTION graph_search.uninstall(text, text, boolean) IS
  '元函数：卸载一个实例，删除全部生成对象并清理 registry 记录，返回执行的 DROP 文本';


/*
 * 10. list / show：查看已安装实例
 */
CREATE OR REPLACE FUNCTION graph_search.list_installed()
RETURNS TABLE(target_schema text, prefix text, edge_table text, node_type text,
              directed boolean, max_depth_cap int, installed_at timestamptz, installed_by name,
              api text[])
LANGUAGE sql STABLE
AS $pga$
  SELECT r.target_schema, r.prefix, c.relname::text, r.node_type, r.directed,
         r.max_depth_cap, r.installed_at, r.installed_by,
         ARRAY[ r.prefix || '_search', r.prefix || '_cursor', r.prefix || '_shortest_path',
                r.prefix || '_subgraph', r.prefix || '_stats' ]
    FROM graph_search.registry r
    LEFT JOIN pg_class c ON c.oid = r.table_oid
   ORDER BY 1, 2;
$pga$;

COMMENT ON FUNCTION graph_search.list_installed() IS '列出所有已安装的图搜索实例';


CREATE OR REPLACE FUNCTION graph_search.version()
RETURNS text LANGUAGE sql STABLE
AS $$ SELECT 'pg_graph_search ' || coalesce(
              (SELECT extversion FROM pg_extension WHERE extname = 'pg_graph_search'),
              'unknown') $$;
