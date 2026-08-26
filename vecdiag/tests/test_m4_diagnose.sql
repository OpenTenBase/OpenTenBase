-- M4 回归测试：零参数体检的契约
-- 用法：psql -p 5518 -d postgres -X -f tests/test_m4_diagnose.sql
-- 通过标准：ok_* 全为 t。
\pset pager off

\echo '== 1. 入口函数必须是零参数（拿到库就能跑）=='
select p.proname, p.pronargs = 0 as ok_zero_arg
from pg_proc p join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'vecdiag' and p.proname = 'diagnose';

\echo '== 2. 每条输出的四要素都不能为空 =='
select count(*) as findings,
       count(*) filter (where coalesce(problem,'') = '' or coalesce(cause,'') = ''
                          or coalesce(fix,'') = '' or coalesce(verify,'') = '') = 0
         as ok_four_elements,
       count(*) filter (where severity not in ('error','warn','info')) = 0 as ok_severity
from vecdiag.diagnose();

\echo '== 3. 静态推断不得冒充执行计划事实（措辞检查）=='
select count(*) filter (where problem ilike '%EXPLAIN%' or cause ilike '%执行计划%') = 0
         as ok_no_explain_claim
from vecdiag.diagnose();

\echo '== 4. 修复建议必须给出可执行的动作（含 SET / ANALYZE / 脚本名之一）=='
select count(*) as findings,
       count(*) filter (where fix ~ '(SET|ANALYZE|tools/)') = count(*) as ok_actionable
from vecdiag.diagnose();

\echo '== 5. 本机 ABI 常数已实测（否则体检会给出 warn）=='
select count(*) filter (where source = 'measured') > 0 as ok_measured_present,
       count(*) filter (where source = 'measured') as measured_rows
from vecdiag.abi_const;

\echo '== 6. 阶段权重的可用性分层：消费视图里不含 pooled，也不含超限组 =='
select (select count(*) from vecdiag.stage_weight_usable where size_class = 'pooled') = 0
         as ok_no_pooled,
       (select count(*) from vecdiag.stage_weight_usable
         where dispersion > vecdiag.stage_weight_dispersion_limit()) = 0 as ok_within_limit,
       (select count(*) from vecdiag.stage_weight_audit where usability <> '可用') as retained_evidence;

\echo '== 7. 可用权重每组仍然求和为 1（按组排除，不留残缺组）=='
select dataset, size_class, count(*) as phases,
       abs(sum(weight) - 1) <= 0.0005 as ok_sum
from vecdiag.stage_weight_usable group by dataset, size_class order by size_class;

\echo '== 8. 选权重的入口在选不出来时必须明确拒绝，而不是随便给一组 =='
select applicable = false as ok_refuses, note ~ 'tools/' as ok_tells_what_to_run
from vecdiag.recommend_stage_weights('hnsw', 100000, 'sift1m');

\echo '== 9. 所有 vecdiag 函数都固定了 search_path =='
select count(*) = 0 as ok_all_pinned
from pg_proc p join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'vecdiag'
  and not exists (select 1 from unnest(coalesce(p.proconfig,'{}')) c where c like 'search_path=%');
