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


-- ---------------------------------------------------------------------------
-- 可用性判定：证据全留，但**默认只让人用可用的那几组**
--
-- 为什么要这一层：阶段权重的极差（各次重复之间该阶段占比的极差）直接决定权重能不能用。
-- 实测发现 pooled 组极差 0.72、10 万行档 0.51，这种权重拿去算进度是自欺欺人；
-- 但把它们删掉，"为什么必须按规模分档"这个结论就失去了支撑。
-- 所以：不可用的组**保留在表里并显式标注原因**，消费方走 stage_weight_usable 视图。
--
-- 阈值 0.25 的来源：可用档的实测极差是 0.08 与 0.16，不可用档是 0.51 与 0.72，
-- 中间有明显空隙，取 0.25 落在空隙里。这是**依据实测分布定的阈值，不是拍的**，
-- 换机器或换数据集后应重新检查这个空隙是否还存在。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.stage_weight_dispersion_limit()
returns numeric language sql immutable
set search_path = pg_catalog, pg_temp
as $$ select 0.25::numeric $$;

comment on function vecdiag.stage_weight_dispersion_limit() is
  '权重可用性的极差上限。实测可用档 0.08/0.16、不可用档 0.51/0.72，阈值取中间空隙 0.25。';

-- 可用性必须**按组**判定，不能按行：一组权重里只要有一个阶段极差超限，
-- 整组就不能用。按行过滤会留下"缺了几个阶段、求和不为 1"的残缺权重集，
-- 那比整组排除更危险——实测 S 档就是这种情况（assigning 0.18 达标，
-- 但 loading 0.51、k-means 0.33 超限）。
create or replace view vecdiag.stage_weight_usable as
select w.am, w.phase, w.size_class, w.dataset, w.weight, w.n_samples, w.dispersion,
       w.source, w.run_id, w.measured_at
from vecdiag.stage_weight w
join (
  select am, size_class, dataset
  from vecdiag.stage_weight
  where size_class <> 'pooled'
  group by am, size_class, dataset
  having max(coalesce(dispersion, 1)) <= vecdiag.stage_weight_dispersion_limit()
) ok on ok.am = w.am and ok.size_class = w.size_class and ok.dataset = w.dataset;

comment on view vecdiag.stage_weight_usable is
  '**消费方默认用这个视图。** 只含极差达标且按规模分档的权重；pooled 一律排除。'
  '被排除的组仍在 vecdiag.stage_weight 里，附极差可查，用于说明"为什么必须分档"。';

create or replace view vecdiag.stage_weight_audit as
select am, phase, size_class, dataset, weight, n_samples, dispersion,
       case
         when size_class = 'pooled' then '不可用：pooled 把多个规模档混在一起求平均，无物理意义'
         when max(coalesce(dispersion, 1)) over (partition by am, size_class, dataset)
                > vecdiag.stage_weight_dispersion_limit()
           then format('不可用：本组最大极差 %s 超过上限 %s（该组内某阶段耗时占比不稳定，'
                       '通常是构建太快、被检查点或 autovacuum 整体拖慢）',
                       max(coalesce(dispersion, 1)) over (partition by am, size_class, dataset),
                       vecdiag.stage_weight_dispersion_limit())
         else '可用'
       end as usability,
       run_id, measured_at
from vecdiag.stage_weight;

comment on view vecdiag.stage_weight_audit is
  '给审查者看的全量视图：每组权重都带"可用/不可用 + 原因"。证据不删，结论不混。';

-- ---------------------------------------------------------------------------
-- 自动选权重：给定访问方法、行数、数据集，返回该用哪一组
--
-- 这个函数是给**人和 AI 都能直接用**准备的：不需要先读文档才知道 S/M/L 怎么分、
-- 哪组能用。选不出来时返回 applicable=false 并说明原因，**不退化成随便给一组**。
--
-- 规模档的行数边界来自实际标定点：S=10 万、M=30 万、L=100 万。
-- 落在两档之间取较近的一档；超出 L 档上界按 L 处理但把 note 标成外插。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.recommend_stage_weights(
    p_am      text,
    p_rows    bigint,
    p_dataset text default 'sift1m'
) returns table (
    applicable  boolean,
    size_class  text,
    dataset     text,
    phases      int,
    max_dispersion numeric,
    note        text
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with pick as (
    select case
             when p_rows <= 200000  then 'S'
             when p_rows <= 650000  then 'M'
             else 'L'
           end as cls
  ),
  cand as (
    select w.size_class, w.dataset, count(*)::int as phases, max(w.dispersion) as disp
    from vecdiag.stage_weight_usable w, pick
    where w.am = p_am and w.dataset = p_dataset and w.size_class = pick.cls
    group by w.size_class, w.dataset
  ),
  -- 首选档不可用时，退到同数据集下任何可用档，并在 note 里说清换了档
  fallback as (
    select w.size_class, w.dataset, count(*)::int as phases, max(w.dispersion) as disp
    from vecdiag.stage_weight_usable w
    where w.am = p_am and w.dataset = p_dataset
    group by w.size_class, w.dataset
    order by max(w.dispersion)
    limit 1
  )
  select true, c.size_class, c.dataset, c.phases, c.disp,
         case when p_rows > 1300000
              then format('按行数 %s 命中 %s 档；极差 %s 达标。**但已超出标定上界 100 万行，'
                          '属外插**，ETA 只能当量级参考', p_rows, c.size_class, c.disp)
              else format('按行数 %s 命中 %s 档；极差 %s 达标', p_rows, c.size_class, c.disp)
         end
  from cand c
  union all
  select true, f.size_class, f.dataset, f.phases, f.disp,
         format('行数 %s 对应的档位没有达标权重，退用极差最小的 %s 档；'
                'ETA 只能当量级参考，结论里要注明换档', p_rows, f.size_class)
  from fallback f where not exists (select 1 from cand)
  union all
  select false, null, p_dataset, 0, null,
         format('数据集 %s 下 %s 没有任何达标权重。请先跑 tools/measure_build_time.sh '
                '与 tools/load_stage_weights.sh 标定，不要拿别的数据集的权重顶替', p_dataset, p_am)
  where not exists (select 1 from cand) and not exists (select 1 from fallback);
$$;

comment on function vecdiag.recommend_stage_weights(text, bigint, text) is
  '给人和 AI 用的入口：不用先读文档就知道该取哪组权重。选不出来时明确返回 applicable=false，'
  '并给出该跑哪个脚本去标定，而不是随便返回一组凑数。';
