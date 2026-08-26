-- vecdiag 40 · 构建阶段耗时分解与加权进度（M3）
--
-- 上游已有什么（**不得宣称原创**）：
--   * pg_stat_progress_create_index 是 PostgreSQL 自带视图；
--   * pgvector 自 0.2.3 起就在构建期上报子阶段（phase 列）。
-- 本模块做上游没有的三件事：
--   1. 阶段耗时分解——每个阶段实际占了多少时间（上游只告诉你"现在在哪个阶段"）；
--   2. 跨阶段加权百分比——上游的 blocks_done/tuples_done 只在阶段内部有意义，
--      阶段之间没有可比的进度口径；
--   3. 剩余时间预测。
--
-- 硬性要求：百分比必须**单调非递减**且终值为 100。阶段权重必须来自实测，
-- 并且要记样本数与离散度——权重是拍的还是测的，评审一定会问。

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- 阶段权重表（由 tools/load_stage_weights.sh 从实测采样序列写入）
--
-- size_class：'pooled' 是把各规模汇总的权重（极差大，只能当量级参考）；
-- 'S'/'M'/'L' 是分档权重。dataset：'synthetic' 或 'sift1m'——
-- 构建耗时与数据分布有关，所以真实数据与合成数据的权重**并列保存，不互相覆盖**。
-- ---------------------------------------------------------------------------
create table if not exists vecdiag.stage_weight (
    am          text    not null,
    phase       text    not null,
    size_class  text    not null default 'pooled',
    dataset     text    not null default 'synthetic',
    weight      numeric not null check (weight >= 0),
    n_samples   int     not null,
    dispersion  numeric,                       -- 各次重复之间该阶段占比的极差
    source      text    not null default 'measured',
    run_id      text,
    measured_at timestamptz not null default now(),
    primary key (am, phase, size_class, dataset)
);

-- 早期版本的主键只有 (am, phase)，升级时补列并换主键
do $$
begin
    if not exists (select 1 from pg_attribute
                   where attrelid = 'vecdiag.stage_weight'::regclass
                     and attname = 'size_class' and not attisdropped) then
        alter table vecdiag.stage_weight add column size_class text not null default 'pooled';
        alter table vecdiag.stage_weight add column dataset text not null default 'synthetic';
        alter table vecdiag.stage_weight drop constraint stage_weight_pkey;
        alter table vecdiag.stage_weight
          add primary key (am, phase, size_class, dataset);
    end if;
end;
$$;

comment on table vecdiag.stage_weight is
  '阶段权重 = 该阶段耗时占总构建耗时的比例，来自实测。dispersion 是重复之间的极差，'
  '必须一起报告：离散大说明该阶段耗时不稳定，用它做 ETA 要标注不确定性。';

-- 采样序列落库（\copy 自 tools/progress_sampler.sh 的 CSV）
create table if not exists vecdiag.progress_sample (
    run_id       text   not null,
    elapsed_ms   bigint not null,
    pid          int,
    phase        text,
    blocks_total bigint,
    blocks_done  bigint,
    tuples_total bigint,
    tuples_done  bigint,
    relid        oid,
    index_relid  oid
);

create index if not exists progress_sample_run_idx
  on vecdiag.progress_sample (run_id, elapsed_ms);

comment on table vecdiag.progress_sample is
  '进度视图的原始抽样序列。所有 M3 结论都必须能从这张表重算，不接受口述。';

-- ---------------------------------------------------------------------------
-- 阶段内进度：只有 LOAD 阶段能从视图拿到真实计数，其余阶段是时间插值
--
-- 这个区别必须暴露在输出里（intra_source 列），否则会把插值说成观测。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.intra_phase_pct(
    p_phase        text,
    p_blocks_total bigint,
    p_blocks_done  bigint,
    p_tuples_total bigint,
    p_tuples_done  bigint
) returns numeric
language sql immutable
set search_path = pg_catalog, pg_temp
as $$
  select case
           when coalesce(p_tuples_total, 0) > 0
             then least(p_tuples_done::numeric / p_tuples_total, 1)
           when coalesce(p_blocks_total, 0) > 0
             then least(p_blocks_done::numeric / p_blocks_total, 1)
           else null                       -- 拿不到计数 → 交给调用方做时间插值
         end;
$$;

-- ---------------------------------------------------------------------------
-- 进度曲线：把一次构建的采样序列换算成跨阶段加权百分比 + ETA
--
-- 单调性用窗口函数 max(...) over (rows unbounded preceding) 强制，
-- 因此"单调"是结构保证的，不靠事后修数据。
-- ---------------------------------------------------------------------------
-- 旧的两参数版本必须先删：参数个数不同会形成重载，调用时报 "is not unique"
drop function if exists vecdiag.progress_curve(text, text);

create or replace function vecdiag.progress_curve(
    p_run_id     text,
    p_am         text,
    p_size_class text default 'pooled',
    p_dataset    text default 'synthetic')
returns table (
    elapsed_ms   bigint,
    phase        text,
    intra_pct    numeric,
    intra_source text,
    raw_pct      numeric,
    mono_pct     numeric,
    eta_ms       bigint
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with s as (
    select ps.elapsed_ms, ps.phase,
           vecdiag.intra_phase_pct(ps.phase, ps.blocks_total, ps.blocks_done,
                                   ps.tuples_total, ps.tuples_done) as intra
    from vecdiag.progress_sample ps
    where ps.run_id = p_run_id
    order by ps.elapsed_ms
  ),
  w as (
    select phase, weight,
           sum(weight) over (order by weight desc, phase
                             rows between unbounded preceding and 1 preceding) as prior_w
    from vecdiag.stage_weight
    where am = p_am and size_class = p_size_class and dataset = p_dataset
  ),
  j as (
    select s.elapsed_ms, s.phase, s.intra,
           coalesce(w.weight, 0) as wt, coalesce(w.prior_w, 0) as prior_w
    from s left join w on w.phase = s.phase
  ),
  r as (
    select elapsed_ms, phase, intra,
           case when intra is null then 'time-interpolated' else 'view-counter' end as src,
           least(100, greatest(0,
             (prior_w + wt * coalesce(intra, 0.5)) * 100)) as raw
    from j
  ),
  m as (
    select r.*, max(raw) over (order by elapsed_ms
                               rows between unbounded preceding and current row) as mono
    from r
  )
  select elapsed_ms, phase, round(intra, 4), src, round(raw, 2), round(mono, 2),
         -- 线性外推：按当前单调进度推总时长，减去已用
         case when mono > 0 and mono < 100
              then (elapsed_ms * (100 - mono) / mono)::bigint
              else 0 end
  from m;
$$;

comment on function vecdiag.progress_curve(text, text, text, text) is
  'intra_source=time-interpolated 的行说明该阶段的阶段内进度是插值，不是视图计数——'
  '报告里必须写清哪些阶段属于这一类。';

