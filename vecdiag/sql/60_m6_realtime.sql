-- vecdiag 70 · 实时构建监控（把 M1/M2/M3 合到一个"正在建索引时能看的东西"里）
--
-- 为什么需要它：M3 的离线曲线要先采样、再落库、再算，适合出报告；但真正在等索引的人
-- 需要的是**现在**就能看到"到哪了、还要多久、会不会出事"。上游 
-- pg_stat_progress_create_index 只给"现在在哪个阶段"，没有跨阶段百分比、没有剩余时间、
-- 也不会告诉你"按当前内存这次构建会降级落盘"。
--
-- 实现上的关键约束：进度视图**不提供阶段开始时刻**，只提供当前阶段名。
-- 要算"这个阶段跑了多久"，必须自己记住阶段是什么时候变的。所以这里用一张
-- unlogged 状态表，在每次调用监控函数时顺手维护——不需要后台进程、不需要改内核。
-- 代价：必须有人周期性调用（tools/watch_build.sh 就是干这个的）；没人调用时
-- 阶段起点会退化成"第一次被观测到的时刻"，输出里用 observed_from 列标出来。

\set ON_ERROR_STOP on

create unlogged table if not exists vecdiag.monitor_state (
    pid              int  not null,
    index_relid      oid  not null,
    phase            text,
    build_started_at timestamptz not null default clock_timestamp(),
    phase_started_at timestamptz not null default clock_timestamp(),
    max_pct          numeric     not null default 0,
    samples          int         not null default 0,
    primary key (pid, index_relid)
);

comment on table vecdiag.monitor_state is
  'unlogged：进程重启后清空，本来就只在构建期间有意义。'
  '记的是"阶段何时变的"——这是 pg_stat_progress_create_index 没有而算 ETA 必须有的信息。';

-- 取某张表向量列的维数：M2 的预测需要维数，而进度视图只给 relid
create or replace function vecdiag.vector_dims_of(p_relid oid)
returns int
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  select coalesce(max(atttypmod), 0)
  from pg_attribute a
  join pg_type t on t.oid = a.atttypid
  where a.attrelid = p_relid and a.attnum > 0 and not a.attisdropped
    and t.typname = 'vector';
$$;

comment on function vecdiag.vector_dims_of(oid) is
  'vector 的维数存在 atttypmod 里（不像 varchar 还要减 4）。多个向量列时取最大的那个。';

-- ---------------------------------------------------------------------------
-- 记一次观测：阶段变了就更新阶段起点
--
-- 抽成独立函数有两个原因：
--   1. 在 build_monitor() 里内联写 `on conflict (pid, index_relid)` 会报
--      "column reference pid is ambiguous"——因为 build_monitor 的 RETURNS TABLE
--      声明了同名输出变量 pid，而 on conflict 的目标列**不能加表别名限定**。
--      实测这个错误只在真有构建在跑时才触发（空闲时循环体不执行），
--      所以空跑一次 build_monitor() 是查不出来的，必须有并发构建才暴露。
--   2. 抽出来之后可以直接单测这条 upsert，不需要真的建一个索引。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.monitor_touch(
    p_pid int, p_index_relid oid, p_phase text)
returns void
language sql volatile
set search_path = pg_catalog, pg_temp
as $$
  insert into vecdiag.monitor_state as ms (pid, index_relid, phase, samples)
  values (p_pid, p_index_relid, p_phase, 1)
  on conflict on constraint monitor_state_pkey do update
    set phase            = p_phase,
        phase_started_at  = case when ms.phase is distinct from p_phase
                                 then clock_timestamp() else ms.phase_started_at end,
        samples           = ms.samples + 1;
$$;

comment on function vecdiag.monitor_touch(int, oid, text) is
  '幂等地记一次观测。阶段名变化时才推进 phase_started_at —— ETA 靠这个时刻算阶段内进度。';


-- ---------------------------------------------------------------------------
-- 访问方法只能从阶段名和计数列**推断**，不能从 pg_class 取
--
-- 这是实测撞出来的一条硬限制：非 CONCURRENTLY 的 CREATE INDEX 期间，
-- 新索引的 pg_class 行属于**尚未提交的事务**，别的会话看不见。
-- 所以在另一个会话里做实时监控时：
--     pg_stat_progress_create_index.relid       → 能在 pg_class 里查到（表是已存在的）
--     pg_stat_progress_create_index.index_relid → 查不到，join pg_class 直接把行过滤掉
-- 第一版监控就是 join pg_class 拿索引名和 relam，结果整个函数永远返回 0 行，
-- 而空闲时又看不出问题（循环体不执行）。
--
-- 推断依据（实测计数，results/centos7-20260826/m3r-sift1m-20260826 与 t35-20260827）：
--     phase = performing k-means / assigning tuples   → 只有 IVFFlat 有这两个阶段
--     phase = loading tuples 且 tuples_total > 0       → IVFFlat（254/254 个采样点都有）
--     phase = loading tuples 且 tuples_total = 0       → HNSW（2458 个采样点全为 0）
--     phase = initializing                             → 判不出来，返回 null
-- 判不出来时**不猜**：am 返回 null，百分比也不给。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.infer_am(p_phase text, p_tuples_total bigint)
returns text
language sql immutable
set search_path = pg_catalog, pg_temp
as $$
  select case
           when p_phase in ('building index: performing k-means',
                            'building index: assigning tuples') then 'ivfflat'
           when p_phase = 'building index: loading tuples'
                and coalesce(p_tuples_total, 0) > 0 then 'ivfflat'
           when p_phase = 'building index: loading tuples'
                and coalesce(p_tuples_total, 0) = 0 then 'hnsw'
           else null
         end;
$$;

comment on function vecdiag.infer_am(text, bigint) is
  '从进度视图推断访问方法。构建期间拿不到索引的 pg_class 行，所以只能这么推。'
  'initializing 阶段判不出来，返回 null——此时不给百分比，不猜。';

-- ---------------------------------------------------------------------------
-- 实时监控主入口
--
-- 一行一个正在构建的索引。列的分层要看清楚：
--   phase / intra_*        —— 观测（上游视图给的）
--   pct / eta_s            —— 推算（用实测权重加权；权重或访问方法不可用时留空）
--   risk / risk_detail     —— 预警（复用 M2 的事前模型）
-- ---------------------------------------------------------------------------
-- p_mwm_kb 的存在理由（必须写清，否则预警会误报）：
--   降级预警要拿**正在构建的那个后端**的 maintenance_work_mem 去算，但 PostgreSQL
--   不提供跨后端读 GUC 的能力（pg_stat_activity 里没有）。默认值 null 时用的是
--   **监控会话自己**的 maintenance_work_mem，这在两个会话设置不同时会误报。
--   实测踩过：构建方设 80MB、监控方 256MB，预警显示"正常"，而实际降级了。
--   所以监控别人的构建时，请把对方的值显式传进来。
create or replace function vecdiag.build_monitor(p_mwm_kb bigint default null)
returns table (
    pid           int,
    index_name    text,
    table_name    text,
    am            text,
    am_source     text,
    elapsed_source text,
    phase         text,
    intra_pct     numeric,
    intra_source  text,
    pct           numeric,
    elapsed_s     numeric,
    eta_s         numeric,
    eta_basis     text,
    weight_basis  text,
    risk          text,
    risk_detail   text
)
language plpgsql volatile
set search_path = pg_catalog, pg_temp
as $fn$
declare
    -- 变量名不能叫 r：下面查询里的 CTE 也叫 r，plpgsql 会报
    -- "column reference r.pid is ambiguous"。同名遮蔽是 plpgsql 的经典坑。
    srec record;
begin
    for srec in
        select p.pid, p.index_relid, p.phase
        from pg_stat_progress_create_index p
    loop
        perform vecdiag.monitor_touch(srec.pid, srec.index_relid, srec.phase);
    end loop;

    -- 结束的构建从状态表清掉，否则同 pid 的下一次构建会继承上一次的起点
    delete from vecdiag.monitor_state d
    where not exists (select 1 from pg_stat_progress_create_index p
                      where p.pid = d.pid and p.index_relid = d.index_relid);

    return query
    with cur as (
        select p.pid as bpid, p.index_relid, p.relid, p.phase as ph,
               p.blocks_total, p.blocks_done, p.tuples_total, p.tuples_done,
               -- 构建起点优先取 pg_stat_activity.query_start，也就是 CREATE INDEX 语句
               -- 真正开始的时刻。只有拿不到（权限不足或后端已退出）时才退回
               -- "第一次被本函数观测到的时刻"——那会把已经跑过的时间算漏，必须标出来。
               coalesce(sa.query_start, ms.build_started_at) as build_started_at,
               case when sa.query_start is not null then 'pg_stat_activity.query_start'
                    else 'first-observation（已用时间会偏小）' end as el_src,
               ms.phase_started_at, ms.max_pct,
               -- 索引名在构建期间查不到（未提交事务），所以 left join 且给出占位
               coalesce(ic.relname::text, '(building oid=' || p.index_relid || ')') as idx_name,
               tc.relname::text as tbl_name,
               coalesce(a.amname::text,
                        vecdiag.infer_am(p.phase, p.tuples_total)) as am_name,
               case when a.amname is not null then 'pg_class'
                    when vecdiag.infer_am(p.phase, p.tuples_total) is not null then 'phase+counters'
                    else 'unknown' end as am_src,
               greatest(tc.reltuples, 0)::bigint as est_rows,
               vecdiag.vector_dims_of(p.relid) as dims
        from pg_stat_progress_create_index p
        join vecdiag.monitor_state ms on ms.pid = p.pid and ms.index_relid = p.index_relid
        join pg_class tc on tc.oid = p.relid
        left join pg_class ic on ic.oid = p.index_relid
        left join pg_am    a  on a.oid  = ic.relam
        left join pg_stat_activity sa on sa.pid = p.pid
    ),
    wsel as (
        select c.*, w.applicable, w.size_class, w.dataset, w.note as wnote
        from cur c
        left join lateral vecdiag.recommend_stage_weights(c.am_name, c.est_rows, 'sift1m') w
               on c.am_name is not null
    ),
    wt as (
        select v.*, sw.weight, sw.prior_w
        from wsel v
        left join lateral (
            select s.weight, s.phase,
                   coalesce(sum(s.weight) over (order by po.ord
                            rows between unbounded preceding and 1 preceding), 0) as prior_w
            from vecdiag.stage_weight s
            join vecdiag.phase_order po on po.am = s.am and po.phase = s.phase
            where s.am = v.am_name and s.size_class = coalesce(v.size_class, 'x')
              and s.dataset = coalesce(v.dataset, 'x')
        ) sw on sw.phase = v.ph
    ),
    calc as (
        select w.*,
               vecdiag.intra_phase_pct(w.ph, w.blocks_total, w.blocks_done,
                                       w.tuples_total, w.tuples_done) as intra_raw,
               extract(epoch from clock_timestamp() - w.build_started_at)::numeric as el_s,
               extract(epoch from clock_timestamp() - w.phase_started_at)::numeric as ph_s
        from wt w
    ),
    est as (
        select c.*,
               case
                 when c.intra_raw is not null then c.intra_raw
                 -- 无计数阶段：用已完成阶段的实际耗时反推本阶段应有多长（与 M3 同一口径）
                 when c.prior_w > 0 and c.weight > 0 and c.el_s > c.ph_s
                   then least(1, c.ph_s / (c.weight * ((c.el_s - c.ph_s) / c.prior_w)))
               end as intra_eff,
               case when c.intra_raw is not null then 'view-counter'
                    when c.prior_w > 0 and c.weight > 0 and c.el_s > c.ph_s then 'phase-rate'
                    else 'unknown' end as intra_src
        from calc c
    ),
    -- HNSW 只有一个真正干活的阶段（hnsw.h:76 只定义了 LOAD），
    -- 这种访问方法**不存在跨阶段加权问题**，百分比直接等于阶段内计数。
    -- 早先一律要求"可用权重"，导致 HNSW 永远拿不到百分比——那是把 IVFFlat 的
    -- 多阶段约束错套到单阶段上。
    sp1 as (
        select e.*,
               (select count(*) from vecdiag.phase_order po
                 where po.am = e.am_name and po.phase <> 'initializing') = 1 as single_phase
        from est e
    ),
    pctc as (
        select s1.*,
               case when s1.intra_eff is null then null
                    when s1.single_phase then least(100, greatest(0, s1.intra_eff * 100))
                    when s1.weight is null then null
                    else least(100, greatest(0, (s1.prior_w + s1.weight * s1.intra_eff) * 100))
               end as pct_now
        from sp1 s1
    ),
    -- 单调化：实时场景没法回看整条序列，所以把历史最大值存在状态表里
    mono as (
        select p.*, greatest(coalesce(p.pct_now, 0), p.max_pct) as pct_mono from pctc p
    ),
    rk as (
        select m.*,
               sp.predicted_spill_tuples as spill_at,
               case when m.am_name = 'hnsw' and m.dims > 0 and sp.will_spill
                      then '⚠ 预测会降级落盘'
                    when m.am_name = 'hnsw' and m.dims > 0 then '正常'
                    when m.am_name = 'ivfflat' then '正常（IVFFlat 的内存检查在构建开始前已通过）'
                    else '未评估' end as risk_txt,
               case when m.am_name = 'hnsw' and m.dims > 0 and sp.will_spill
                      then format('按 %s 行×%s 维 m=16 预测图内存 %s MB 超过 maintenance_work_mem；'
                                  '预计第 %s 行开始转磁盘。建议 SET maintenance_work_mem = ''%sMB'' 后重建。',
                                  m.est_rows, m.dims, sp.estimated_graph_mb,
                                  sp.predicted_spill_tuples, sp.recommended_mwm_mb)
                    when m.am_name = 'hnsw' and m.dims > 0
                      then format('图内存预测 %s MB，未超 maintenance_work_mem', sp.estimated_graph_mb)
                    else '—' end as risk_dtl
        from mono m
        left join lateral vecdiag.hnsw_predict_spill(m.est_rows, m.dims, 16, p_mwm_kb) sp
               on m.am_name = 'hnsw' and m.dims > 0
    ),
    -- HNSW 的实时 ETA 要按降级修正，否则会一路偏低：朴素外推按元组线性推，
    -- 而降级后每元组耗时是降级前的 k 倍（k 由 hnsw_spill_penalty 标定）。
    -- 这与离线的 vecdiag.hnsw_eta_corrected() 是同一个式子。
    etac as (
        -- 不要在这里再 select 一次 k.spill_at：rk 里已经有这一列，重复投影会让
        -- 外层的 k.spill_at 变成 "column reference is ambiguous"。
        select k.*,
               vecdiag.hnsw_spill_penalty_factor(k.dims, 16) as pen,
               case when k.am_name = 'hnsw' and k.spill_at is not null
                         and coalesce(k.tuples_done, 0) > 0
                         and vecdiag.hnsw_spill_penalty_factor(k.dims, 16) is not null
                    then (k.el_s * 1000.0)
                         / nullif(least(k.tuples_done, k.spill_at)
                                  + greatest(k.tuples_done - k.spill_at, 0)
                                    * vecdiag.hnsw_spill_penalty_factor(k.dims, 16), 0)
               end as r_before
        from rk k
    ),
    upd as (
        update vecdiag.monitor_state ms
           set max_pct = k.pct_mono
          from etac k
         where ms.pid = k.bpid and ms.index_relid = k.index_relid
        returning ms.pid
    )
    -- upd 是数据修改型 CTE，PostgreSQL 保证它一定被执行一次，不需要在外层引用它。
    select k.bpid, k.idx_name, k.tbl_name, k.am_name, k.am_src, k.el_src, k.ph,
           round(k.intra_eff * 100, 2), k.intra_src,
           round(k.pct_mono, 2), round(k.el_s, 1),
           -- 有降级修正就用修正值，没有就退回朴素外推，并在 eta_basis 里说清用了哪个
           case
             when k.r_before is not null and k.est_rows > 0
               then round((greatest(k.spill_at - k.tuples_done, 0) * k.r_before
                           + (k.est_rows - greatest(k.tuples_done, k.spill_at))
                             * k.r_before * k.pen) / 1000.0, 1)
             when k.pct_mono > 0 and k.pct_mono < 100
               then round(k.el_s * (100 - k.pct_mono) / k.pct_mono, 1)
           end,
           case
             when k.r_before is not null and k.est_rows > 0
               then format('已按降级修正（减速倍数 %s，预测第 %s 行降级）', k.pen, k.spill_at)
             when k.pct_mono > 0 and k.pct_mono < 100
               then '朴素线性外推（未修正降级；HNSW 降级后会明显偏低）'
             else '—'
           end,
           case when k.am_name is null then '访问方法判不出来（initializing 阶段），不给百分比'
                when k.single_phase
                  then '单阶段访问方法：百分比即阶段内视图计数，不需要跨阶段权重'
                when k.weight is null
                  then coalesce('权重不可用：' || k.wnote, '权重不可用：该访问方法尚未标定')
                else format('%s 档 / %s 数据集实测权重', k.size_class, k.dataset) end,
           k.risk_txt, k.risk_dtl
    from etac k;
end;
$fn$;

comment on function vecdiag.build_monitor(bigint) is
  '零参数实时监控。pct/eta_s 为空说明访问方法判不出来或该访问方法没有可用实测权重——'
  '此时只报阶段，不编百分比。am_source 说明访问方法是查表得到的还是推断出来的。';
