-- OpenTenBase 节点级验证：确认扩展与生成对象在 DN 上同样存在
--
-- 前置：
--   * 已执行 01_setup_distributed.sql；
--   * 超级用户（EXECUTE DIRECT 限超级用户，且一次只能指定一个节点）；
--   * 把 dn0001/dn0002 换成你集群里的实际节点名（opentenbase_ctl status 可查）。
--
-- 说明：EXECUTE DIRECT 只允许查询，禁止 DDL/DML/事务语句
--       （见 doc/src/sgml/ref/execute_direct.sgml）。

\set ON_ERROR_STOP on

\echo '=== 1) 扩展在两个 DN 上都存在（CREATE EXTENSION 是 PREPARE/EXECUTE 两阶段 + 全节点广播） ==='
EXECUTE DIRECT ON (dn0001)
  'SELECT extname, extversion FROM pg_extension WHERE extname = ''pg_graph_search''';
EXECUTE DIRECT ON (dn0002)
  'SELECT extname, extversion FROM pg_extension WHERE extname = ''pg_graph_search''';

\echo '=== 2) 注册表：REPLICATION 表在 DN 上应能看到与 CN 相同的行 ==='
EXECUTE DIRECT ON (dn0001) 'SELECT count(*) AS registry_rows_on_dn FROM graph_search.registry';
-- CN 侧对照
SELECT count(*) AS registry_rows_on_cn FROM graph_search.registry;

\echo '=== 3) 生成的函数在 DN 上存在（install() 的动态 DDL 会广播） ==='
EXECUTE DIRECT ON (dn0001)
  'SELECT proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = ''gtest'' AND p.proname IN
          (''rep_search'',''rep_cursor'',''rep_shortest_path'',''rep_subgraph'',''rep_stats'')
    ORDER BY 1';

\echo '=== 4) 生成函数在 DN 上的签名一致（对照 CN） ==='
EXECUTE DIRECT ON (dn0001)
  'SELECT p.proname, pg_get_function_arguments(p.oid) AS args
     FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = ''gtest'' AND p.proname = ''rep_search''';
SELECT p.proname, pg_get_function_arguments(p.oid) AS args
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = 'gtest' AND p.proname = 'rep_search';

\echo '=== 5) 边视图在 DN 上存在 ==='
EXECUTE DIRECT ON (dn0001)
  'SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ''gtest'' AND c.relname = ''rep_edge''';

\echo '=== 6) 卸载后 DN 上不应残留（在 CN 上 uninstall 后重跑本段） ==='
-- SELECT graph_search.uninstall('gtest','rep');      -- 在 CN 上执行
-- EXECUTE DIRECT ON (dn0001)
--   'SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
--     WHERE n.nspname = ''gtest'' AND p.proname LIKE ''rep%''';   -- 期望 0
