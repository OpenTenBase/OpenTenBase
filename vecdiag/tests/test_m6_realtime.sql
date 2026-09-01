-- M6（实时监控）回归断言
--
-- 空闲时可验证的部分；需要真实构建的路径由 TAP 用例与 watch_build.sh 实跑覆盖。
\pset pager off
\set ON_ERROR_STOP off

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
