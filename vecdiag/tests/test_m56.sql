-- M5（参数建议表）与 M6（实时监控）回归断言
--
-- 全部是"不需要正在建索引"就能验证的部分。真正需要并发构建才能暴露的问题
-- （plpgsql 变量遮蔽、构建期拿不到 pg_class 行）在 tests/t/001_vecdiag_regression.pl
-- 与 tools/watch_build.sh 的实跑里覆盖——空跑一次监控函数是查不出来的，
-- 这一点本身就是踩过的坑，写在这里提醒不要只依赖本文件。
\pset pager off
\set ON_ERROR_STOP off

select '=== M5 · 参数建议表 ===' as section;

-- 1 三类来源都必须有，且不允许出现第四类
select 'M5-1 溯源只有三类且每类非空' as assertion,
       (select count(distinct source_kind) from vecdiag.param_advice_provenance) = 3
       and not exists (select 1 from vecdiag.param_advice_provenance
                        where source_kind not in ('source-code','upstream-doc','measured')
                           or coalesce(source_ref,'') = '') as ok;

-- 2 源码常量必须与 pgvector v0.8.6 对得上（改版本时这条会红，提醒去复核）
select 'M5-2 m 默认16/范围[2,100]，ef_construction 默认64/范围[4,1000]' as assertion,
       (select default_value = 16 and min_value = 2 and max_value = 100
          from vecdiag.param_limit where am='hnsw' and knob='m')
       and (select default_value = 64 and min_value = 4 and max_value = 1000
              from vecdiag.param_limit where am='hnsw' and knob='ef_construction') as ok;

-- 3 帕累托前沿的定义必须自洽：前沿上的点不能被任何点支配
select 'M5-3 前沿自洽（on_frontier 的点不被支配）' as assertion,
       not exists (select 1 from vecdiag.param_pareto
                    where on_frontier and dominated_by is not null) as ok;

-- 4 建议函数在超出标定范围时必须 applicable=false，而不是硬给一组
select 'M5-4 召回目标 0.9999 超出标定 → applicable=false' as assertion,
       (select bool_and(not applicable) from vecdiag.hnsw_param_advice(0.9999, 100000, 128)) as ok;

-- 5 建议函数在范围内必须给出前沿上的配置
select 'M5-5 召回目标 0.97 → 给出前沿上构建最快的配置' as assertion,
       exists (select 1 from vecdiag.hnsw_param_advice(0.97, 100000, 128) a
                join vecdiag.param_pareto p
                  on p.am='hnsw' and p.m = a.m and p.ef_construction = a.ef_construction
               where a.applicable and p.on_frontier) as ok;

-- 6 IVFFlat 建议必须把 lists 与 probes 的耦合写进 note（这是上游文档的硬约束）
select 'M5-6 IVFFlat 建议里写明 probes 要同步调大' as assertion,
       (select note like '%probes%' from vecdiag.ivfflat_param_advice(1000000, 128, 65536)) as ok;

select '=== M6 · 实时监控 ===' as section;

-- 7 空闲时必须返回 0 行且不报错（不能因为没有构建就抛异常）
select 'M6-7 空闲时返回 0 行' as assertion,
       (select count(*) = 0 from vecdiag.build_monitor()) as ok;

-- 8 访问方法推断表：三条判据必须与实测计数一致
select 'M6-8 访问方法推断（k-means→ivfflat，loading+tuples_total=0→hnsw）' as assertion,
       vecdiag.infer_am('building index: performing k-means', 0) = 'ivfflat'
       and vecdiag.infer_am('building index: loading tuples', 12345) = 'ivfflat'
       and vecdiag.infer_am('building index: loading tuples', 0) = 'hnsw'
       and vecdiag.infer_am('initializing', 0) is null as ok;

-- 9 状态表 upsert 幂等：同一 (pid, index_relid) 反复记不会长出多行，
--    且阶段名不变时 phase_started_at 不许被推进（ETA 的阶段内进度靠它）
do $$
declare t0 timestamptz; t1 timestamptz; n int;
begin
    delete from vecdiag.monitor_state where pid = -1;
    perform vecdiag.monitor_touch(-1, 0::oid, 'building index: loading tuples');
    select phase_started_at into t0 from vecdiag.monitor_state where pid = -1;
    perform pg_sleep(0.05);
    perform vecdiag.monitor_touch(-1, 0::oid, 'building index: loading tuples');
    select phase_started_at, count(*) over () into t1, n
      from vecdiag.monitor_state where pid = -1;
    raise notice 'M6-9 upsert 幂等且阶段未变时不推进起点: %', (t0 = t1 and n = 1);
    -- 阶段变了才推进
    perform vecdiag.monitor_touch(-1, 0::oid, 'building index: assigning tuples');
    select phase_started_at into t1 from vecdiag.monitor_state where pid = -1;
    raise notice 'M6-10 阶段变化时推进起点: %', (t1 > t0);
    delete from vecdiag.monitor_state where pid = -1;
end $$;

-- 11 降级减速倍数必须有标定，否则实时 ETA 不该声称"已修正"
select 'M6-11 降级减速倍数已标定' as assertion,
       vecdiag.hnsw_spill_penalty_factor(128, 16) is not null as ok;

-- 12 T3.5 的修正 ETA 必须优于朴素 ETA（这是"接入 M2 有用"的断言化）
select 'M6-12 修正 ETA 的全程偏差低于朴素 ETA' as assertion,
       coalesce((select avg(corrected_err_pct) < avg(naive_err_pct)
                   from vecdiag.hnsw_eta_corrected('t35-20260827/hnsw_spill', 100000, 128, 16, 61440)),
                true) as ok,
       '库里没有 t35 采样序列时本条自动为 true，需先跑 tools/hnsw_eta_spill.sh' as caveat;
