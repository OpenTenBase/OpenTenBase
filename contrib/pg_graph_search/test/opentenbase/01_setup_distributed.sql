-- OpenTenBase 分布式用例：两种边表分布 × 有向/无向
--
-- 前置：
--   * 在 CN 上执行（PGHOST/PGPORT 指向 CN，默认端口 11000）；
--   * 已 CREATE EXTENSION pg_graph_search;
--   * 需要能创建 schema 的权限。
--
-- 与 test/01_setup.sql 的区别：这里的边表**显式声明分布策略**，
-- 因为 OpenTenBase 不写 DISTRIBUTE BY 时的默认规则是"取主键/唯一约束首列"，
-- 会隐式变成 HASH(c1)，不利于做对照实验。
--
-- 用法：psql -h <CN> -p 11000 -U opentenbase -d postgres -f test/opentenbase/01_setup_distributed.sql

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- A. 复制表（每个 DN 持有全量图）：中小图的首选，扫描可本地化
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS public.t_edge_rep CASCADE;
CREATE TABLE public.t_edge_rep (
  c1   int   NOT NULL,
  c2   int   NOT NULL,
  prop jsonb,
  PRIMARY KEY (c1, c2)
) DISTRIBUTE BY REPLICATION;

-- 无向图必须建反向索引（单机实测 600 倍差距；分布式下反向分支要按 c2 过滤）
CREATE INDEX ON public.t_edge_rep (c2, c1);

INSERT INTO public.t_edge_rep(c1, c2, prop) VALUES
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
VACUUM ANALYZE public.t_edge_rep;

-- ---------------------------------------------------------------------------
-- B. 哈希分布表（大图存储友好，代价是 CN 侧递归要跨节点取数）
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS public.t_edge_hash CASCADE;
CREATE TABLE public.t_edge_hash (
  c1   int   NOT NULL,
  c2   int   NOT NULL,
  prop jsonb,
  PRIMARY KEY (c1, c2)
) DISTRIBUTE BY shard (c1);
CREATE INDEX ON public.t_edge_hash (c2, c1);

INSERT INTO public.t_edge_hash SELECT c1, c2, prop FROM public.t_edge_rep;
VACUUM ANALYZE public.t_edge_hash;

-- ---------------------------------------------------------------------------
-- C. 安装四个实例：{REPLICATION, HASH} × {有向, 无向}
-- ---------------------------------------------------------------------------
DROP SCHEMA IF EXISTS gtest CASCADE;
CREATE SCHEMA gtest;

SELECT graph_search.install(
  '{"table":"public.t_edge_rep","c1":"c1","c2":"c2","prop":"prop",
    "target_schema":"gtest","prefix":"rep","directed":true}'::jsonb) IS NOT NULL AS rep_directed;
SELECT graph_search.install(
  '{"table":"public.t_edge_rep","c1":"c1","c2":"c2","prop":"prop",
    "target_schema":"gtest","prefix":"repu","directed":false}'::jsonb) IS NOT NULL AS rep_undirected;
SELECT graph_search.install(
  '{"table":"public.t_edge_hash","c1":"c1","c2":"c2","prop":"prop",
    "target_schema":"gtest","prefix":"hash","directed":true}'::jsonb) IS NOT NULL AS hash_directed;
SELECT graph_search.install(
  '{"table":"public.t_edge_hash","c1":"c1","c2":"c2","prop":"prop",
    "target_schema":"gtest","prefix":"hashu","directed":false}'::jsonb) IS NOT NULL AS hash_undirected;

\echo '=== 安装清单（CN 侧） ==='
SELECT target_schema, prefix, edge_table, node_type, directed, max_depth_cap
  FROM graph_search.list_installed() ORDER BY prefix;

\echo '=== 注册表分布策略（期望 REPLICATION） ==='
SELECT graph_search.registry_distribution() AS registry_distribution;

\echo '=== CN 侧分布快照（pgxc_class） ==='
SELECT c.relname,
       CASE p.pclocatortype
         WHEN 'R' THEN 'REPLICATION'
         WHEN 'S' THEN 'SHARD'
         WHEN 'H' THEN 'HASH'
         WHEN 'M' THEN 'MODULO'
         WHEN 'D' THEN 'DISTRIBUTED'
         WHEN 'O' THEN 'NONE'
         ELSE p.pclocatortype::text
       END AS distribution
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pgxc_class p ON p.pcrelid = c.oid
 WHERE n.nspname IN ('public', 'graph_search', 'gtest')
   AND c.relname IN ('t_edge_rep', 't_edge_hash', 'registry',
                     'rep_edge', 'repu_edge', 'hash_edge', 'hashu_edge')
 ORDER BY 1;
