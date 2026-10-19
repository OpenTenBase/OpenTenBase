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
