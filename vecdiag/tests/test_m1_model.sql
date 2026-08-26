-- vecdiag · M1 回归测试（纯 SQL，psql 可直接跑）
--
-- 用法：psql -p 5518 -d postgres -X -f tests/test_m1_model.sql
-- 通过标准：所有 ok 列为 t。任何 f 都要当作回归，不许"看起来差不多"就放过。
--
-- 这些期望值不是拍的，全部来自 2026-08-26 在 PG18.6 + pgvector 0.8.6 上
-- 捕获的真实报错原文（results/m1-r2-20260826/stderr/）。

\set ON_ERROR_STOP on
\pset pager off

\echo '== 1. ABI 常数存在且有来源标注 =='
select count(*) = 4 as ok_abi_rows,
       bool_and(source in ('measured','source-code')) as ok_abi_source
from vecdiag.abi_const;

\echo '== 2. 整数除法语义：C1 的 kB 阈值必须按 floor 比较 =='
-- 2000 行/128 维/lists=1000 时 C1 = 520024 B，floor(520024/1024) = 507。
-- mwm=506 应命中 C1；mwm=507 时 C1 放行、C2 越界。
-- 若 sum(bigint) 的 numeric 结果没转回 bigint，这里会整体偏 1 kB。
select (select first_hit from vecdiag.ivfflat_predict(2000,128,1000,143,false,506)) = 'C1' as ok_506,
       (select first_hit from vecdiag.ivfflat_predict(2000,128,1000,143,false,507)) = 'C2' as ok_507;

\echo '== 3. maxTuples 口径：上限是 relpages*291，不是表行数 =='
-- 143 页 → 41613，而不是 2000。若用行数算，C2 会小一个数量级。
select vecdiag.ivfflat_num_samples(1000, 143, false) = 41613 as ok_numsamples,
       (select num_samples from vecdiag.ivfflat_predict(2000,128,1000,143,false,1024)) = 41613 as ok_c2_uses_cap,
       (select sampled     from vecdiag.ivfflat_predict(2000,128,1000,143,false,1024)) = 2000  as ok_c3_uses_sampled;

\echo '== 4. 三检查点的报错数字与真实 stderr 逐字相同 =='
select (select predicted_mb from vecdiag.ivfflat_predict(2000,128,1000,143,false,506))   = 1  as ok_c1_1mb,
       (select predicted_mb from vecdiag.ivfflat_predict(2000,128,1000,143,false,1024))  = 22 as ok_c2_22mb,
       (select predicted_mb from vecdiag.ivfflat_predict(2000,128,1000,143,false,28016)) = 34 as ok_c3_34mb,
       (select first_hit    from vecdiag.ivfflat_predict(2000,128,1000,143,false,38489)) = 'none' as ok_build_ok;

\echo '== 5. 空表走 RandomCenters，C3 检查点不存在 =='
-- ivfkmeans.c:561-565：samples->length == 0 时不调 ElkanKmeans，因此没有 C3。
select (select c3_applicable from vecdiag.ivfflat_predict(0,128,100,0,false,121)) = false as ok_c3_na,
       (select first_hit     from vecdiag.ivfflat_predict(0,128,100,0,false,121)) = 'none' as ok_empty_no_error;

\echo '== 6. unlogged 表的正常建索引不走 numSamples=1 =='
-- heap==NULL 对应 ambuildempty，不是"表是 unlogged"。500 行/256 维/lists=200
-- 实测命中 C2 报 11 MB，即 numSamples=10000 的结果。
select (select predicted_mb from vecdiag.ivfflat_predict(500,256,200,72,false,10756)) = 11 as ok_unlogged_normal,
       vecdiag.ivfflat_num_samples(200, 72, true) = 1 as ok_empty_build_is_1;

\echo '== 7. 分项 breakdown 覆盖 11 项且每项都有源码出处 =='
select count(*) = 11 as ok_rows,
       bool_and(source_ref ~ '^(ivfbuild|ivfkmeans)\.c:[0-9]+$') as ok_refs,
       count(*) filter (where checkpoint = 'C3') = 9 as ok_nine_items
from vecdiag.ivfflat_memory_breakdown(2000,128,1000,143);

\echo '== 8. 内存参数解析：裸数字按 kB，不是 MB =='
select vecdiag.parse_mem_kb('1024')  = 1024    as ok_bare_is_kb,
       vecdiag.parse_mem_kb('256MB') = 262144  as ok_mb,
       vecdiag.parse_mem_kb('1GB')   = 1048576 as ok_gb,
       vecdiag.parse_mem_kb('7 zB')  is null   as ok_unknown_is_null;

\echo '== 9. 所有函数都固定了 search_path（防劫持）=='
select count(*) = 0 as ok_all_pinned
from pg_proc p join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'vecdiag'
  and not exists (select 1 from unnest(coalesce(p.proconfig, '{}')) c
                  where c like 'search_path=%');
