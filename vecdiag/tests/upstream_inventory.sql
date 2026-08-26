-- T0.5 · 上游已有能力清单（用于报告的"边界声明"一节）
--
-- 关键前提：**必须先 LOAD 'vector'**。pgvector 的 GUC 在 _PG_init() 里注册，
-- 共享库惰性加载；不 LOAD 就查 pg_settings 会得到空结果，那是假阴性，
-- 不能当成"上游没有 GUC"的证据。

\pset pager off
\pset border 2

load 'vector';

\echo '=== 1. pgvector 注册的全部 GUC ==='
select name, setting, unit, short_desc,
       case when name like '%iterative%' or name like '%probes%' or name like '%ef_search%'
                 or name like '%scan%' then '查询/扫描侧' else '待判定' end as 作用面
from pg_settings
where name like 'ivfflat.%' or name like 'hnsw.%'
order by name;

\echo '=== 2. 构建侧 GUC 数量（预期为 0，这是本项目的立项依据）==='
select count(*) as build_side_guc_count
from pg_settings
where (name like 'ivfflat.%' or name like 'hnsw.%')
  and name not like '%iterative%' and name not like '%probes%'
  and name not like '%ef_search%' and name not like '%scan%';

\echo '=== 3. pg_stat_progress_create_index 的列（PostgreSQL 自带，非本项目实现）==='
select a.attnum, a.attname, format_type(a.atttypid, a.atttypmod) as type
from pg_attribute a
where a.attrelid = 'pg_stat_progress_create_index'::regclass
  and a.attnum > 0 and not a.attisdropped
order by a.attnum;

\echo '=== 4. 索引访问方法与它们支持的索引选项（构建参数只能走 WITH，不是 GUC）==='
select amname, amtype from pg_am where amname in ('ivfflat', 'hnsw') order by amname;

\echo '=== 5. vector 扩展的版本与安装位置 ==='
select extname, extversion from pg_extension where extname = 'vector';
