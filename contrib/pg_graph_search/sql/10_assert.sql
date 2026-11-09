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
