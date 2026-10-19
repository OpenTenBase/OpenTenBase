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
) @DISTRIBUTE_CLAUSE@;

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
