-- 大规模压测数据：与文章同构的 bucket 分群模型
-- 参数化：\set NODES 节点总数  \set FANOUT 每点出边  \set BUCKETS 分群数
\set ON_ERROR_STOP on
\pset tuples_only on
\pset format unaligned

DROP TABLE IF EXISTS big_edge;
CREATE UNLOGGED TABLE big_edge (
  c1   int,
  c2   int,
  prop jsonb,
  PRIMARY KEY (c1, c2)
);

-- 关 autovacuum，灌完再统一 analyze
ALTER TABLE big_edge SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);

\echo '>>> 生成数据中...'
-- 分批并行灌数据：每个 worker 处理一段节点区间，bucket 分群保证图连通
INSERT INTO big_edge (c1, c2, prop)
SELECT id, ((width_bucket(id,1,:nodes,:buckets)-1)*:bucket_size
            + ceil(random()*(:bucket_size-1))::int),
       jsonb_build_object('weight', round((random()*1000)::numeric/1000, 3))
  FROM generate_series(:lo,:hi) AS s(id),
       LATERAL generate_series(1,:fanout) g
ON CONFLICT (c1,c2) DO NOTHING;

\echo '>>> vacuum analyze...'
VACUUM ANALYZE big_edge;

DROP SCHEMA IF EXISTS gbench CASCADE;
CREATE SCHEMA gbench;

SELECT graph_search.install(jsonb_build_object(
  'table','public.big_edge','c1','c1','c2','c2','prop','prop',
  'target_schema','gbench','prefix','b','directed',true,'max_depth_cap',10));

DROP SCHEMA IF EXISTS gbench_u CASCADE;
CREATE SCHEMA gbench_u;
SELECT graph_search.install(jsonb_build_object(
  'table','public.big_edge','c1','c1','c2','c2','prop','prop',
  'target_schema','gbench_u','prefix','bu','directed',false,'max_depth_cap',10));

\pset tuples_only off
\pset format aligned
SELECT count(*) AS edges, min(c1) AS min_src, max(c1) AS max_src FROM big_edge;
