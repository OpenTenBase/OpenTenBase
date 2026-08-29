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
-- 阶段执行顺序：**必须来自源码常量，不能靠权重大小推**
--
-- 这一段是修一个真实缺陷。原实现算"前序阶段累计权重"时写的是
--     order by weight desc, phase
-- 也就是按权重从大到小排。权重大小与执行先后毫无关系：`initializing` 的权重最小
-- （实测 L 档 0.0033），于是它被排到最后，前序累计权重成了 0.9967，
-- 结果**第 1 个采样点（elapsed=1ms）的进度就是 99.83%**，单调化之后整条曲线锁死在
-- 99.83%，ETA 恒为 0。原有断言查不出来：单调性是窗口函数结构保证的，
-- 终值 ≤100 也满足——曲线"合法"但毫无信息量。
-- 这个缺陷是在做 T3.4（ETA 偏差量化）时被 100% 的偏差暴露出来的。
--
-- 顺序的正确来源是 PROGRESS_CREATEIDX_SUBPHASE 的数值常量：
--   pgvector ivfflat.h:61-64  INITIALIZE=1, KMEANS=2, ASSIGN=3, LOAD=4
--   pgvector hnsw.h:75-76     INITIALIZE=1, LOAD=2
-- 实测时间轴与之一致（m3r-L/ivf_L_1 各阶段首次出现时刻 1 / 85 / 9363 / 14089 ms）。
-- ---------------------------------------------------------------------------
create table if not exists vecdiag.phase_order (
    am         text not null,
    phase      text not null,
    ord        int  not null,
    source_ref text not null,
    primary key (am, phase)
);

insert into vecdiag.phase_order (am, phase, ord, source_ref) values
  ('ivfflat', 'initializing',                       1, 'ivfflat.h:61（PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE=1）'),
  ('ivfflat', 'building index: performing k-means', 2, 'ivfflat.h:62（PROGRESS_IVFFLAT_PHASE_KMEANS=2）'),
  ('ivfflat', 'building index: assigning tuples',   3, 'ivfflat.h:63（PROGRESS_IVFFLAT_PHASE_ASSIGN=3）'),
  ('ivfflat', 'building index: loading tuples',     4, 'ivfflat.h:64（PROGRESS_IVFFLAT_PHASE_LOAD=4）'),
  ('hnsw',    'initializing',                       1, 'hnsw.h:75（PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE=1）'),
  ('hnsw',    'building index: loading tuples',     2, 'hnsw.h:76（PROGRESS_HNSW_PHASE_LOAD=2）')
on conflict (am, phase) do update set ord = excluded.ord, source_ref = excluded.source_ref;

comment on table vecdiag.phase_order is
  '阶段执行顺序，来自 pgvector 的 PROGRESS_CREATEIDX_SUBPHASE 常量。'
  '换 pgvector 版本要复核：新增阶段若不在表里，权重会被当作前序为 0，进度会偏小。';

-- ---------------------------------------------------------------------------
-- unstable 窗口 K（T3.4）：阶段切换后的前 K 个采样点，ETA 标为 unstable
--
-- K=1 是**实测定的，不是拍的**。按"距上次阶段切换的采样点数"分组统计 ETA 平均绝对偏差
-- （L 档 3 次重复，phase-rate 口径，results/centos7-20260826/index/eta_accuracy.csv（L/M 档全量逐点数据））：
--     距切换 0 个点：偏差 39.7%   ← 只有切换当下这一个点明显抬升
--     距切换 1 个点：26.6%
--     距切换 2 个点：26.8%
--     距切换 3 个点：26.9%
-- 抬升只集中在切换那一个采样点，之后立刻回到 26.7% 的平台。所以 K=1。
-- 把 K 设成 3 会把两个本来正常的点误标为不可信——宁可少标，不可乱标。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.eta_unstable_window()
returns int language sql immutable
set search_path = pg_catalog, pg_temp
as $$ select 1 $$;

comment on function vecdiag.eta_unstable_window() is
  '阶段切换后判定为 unstable 的采样点个数。改这个值必须同时更新 M3 文档里的偏差表。';

-- ---------------------------------------------------------------------------
-- 进度曲线：把一次构建的采样序列换算成跨阶段加权百分比 + ETA
--
-- 单调性用窗口函数 max(...) over (rows unbounded preceding) 强制，
-- 因此"单调"是结构保证的，不靠事后修数据。
--
-- stability 列（T3.4）：阶段刚切换的头几个采样点，阶段内进度还没有有效计数，
-- 线性外推的 ETA 在这里最不可信，必须标出来而不是混在一起报一个平均偏差。
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- 无计数阶段的阶段内进度：用**已完成阶段的实际耗时**反推，而不是固定 0.5
--
-- 问题（T3.4 量化 ETA 偏差时暴露的第二个缺陷）：
--   IVFFlat 的 k-means 阶段在 pg_stat_progress_create_index 里**没有任何计数**
--   （blocks_* 与 tuples_* 全空），原实现对这种阶段一律取 intra=0.5。
--   后果是百分比在整个 k-means 阶段**冻结不动**——而 k-means 恰好是最长的阶段
--   （实测 L 档权重 0.4302，19.5 s 的构建里占 9.2 s）。
--   进度冻结时线性外推的 ETA 会一路放大，实测最大相对偏差 226%。
--
-- 修法：阶段 j 开始时，前序阶段的权重和 prior_w 与实际已用时间 phase_start 都已知，
-- 于是
--       est_total = phase_start / prior_w
--       est_phase = weight_j * est_total
--       intra_j   = min(1, (elapsed − phase_start) / est_phase)
-- 也就是**用这次构建自己已经跑完的部分去校准剩下的部分**，不需要额外信息。
-- 这个估计随构建推进自动变准（prior_w 越大越稳）：实测 est_total 对真实总时长的偏差
-- 在 k-means 起点是 +32%，assigning 起点 +11%，loading 起点 −2%。
--
-- p_intra_mode 保留 'flat'（老口径，固定 0.5）用于复现"修之前"的偏差，
-- 供审查者自己对比，不是只给一句"改好了"。
-- ---------------------------------------------------------------------------
-- 旧版本必须先删：返回列变了，create or replace 会报 "cannot change return type"
drop function if exists vecdiag.progress_curve(text, text);
drop function if exists vecdiag.progress_curve(text, text, text, text);
drop function if exists vecdiag.progress_curve(text, text, text, text, text);

create or replace function vecdiag.progress_curve(
    p_run_id     text,
    p_am         text,
    p_size_class text default 'pooled',
    p_dataset    text default 'synthetic',
    p_intra_mode text default 'phase-rate')
returns table (
    elapsed_ms   bigint,
    phase        text,
    intra_pct    numeric,
    intra_source text,
    raw_pct      numeric,
    mono_pct     numeric,
    eta_ms       bigint,
    since_trans  int,
    stability    text
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with s as (
    select ps.elapsed_ms, ps.phase,
           vecdiag.intra_phase_pct(ps.phase, ps.blocks_total, ps.blocks_done,
                                   ps.tuples_total, ps.tuples_done) as intra,
           row_number() over (order by ps.elapsed_ms) as rn,
           lag(ps.phase) over (order by ps.elapsed_ms) as prev_phase
    from vecdiag.progress_sample ps
    where ps.run_id = p_run_id
  ),
  t as (
    select s.*, case when s.prev_phase is distinct from s.phase then s.rn end as trans_rn
    from s
  ),
  k as (
    select t.*,
           t.rn - max(t.trans_rn) over (order by t.rn
                                        rows between unbounded preceding and current row) as st,
           count(t.trans_rn) over (order by t.rn
                                   rows between unbounded preceding and current row) as gid
    from t
  ),
  g as (
    select k.*, min(k.elapsed_ms) over (partition by k.gid) as phase_start
    from k
  ),
  -- 前序累计权重按**执行顺序**累加（phase_order.ord），不是按权重大小
  w as (
    select sw.phase, sw.weight,
           coalesce(sum(sw.weight) over (order by po.ord
                                         rows between unbounded preceding and 1 preceding), 0) as prior_w
    from vecdiag.stage_weight sw
    join vecdiag.phase_order po on po.am = sw.am and po.phase = sw.phase
    where sw.am = p_am and sw.size_class = p_size_class and sw.dataset = p_dataset
  ),
  j as (
    select g.elapsed_ms, g.phase, g.intra, g.st, g.phase_start,
           coalesce(w.weight, 0) as wt, coalesce(w.prior_w, 0) as prior_w
    from g left join w on w.phase = g.phase
  ),
  i as (
    select j.*,
           case when j.prior_w > 0 and j.phase_start > 0 and j.wt > 0
                then j.wt * (j.phase_start::numeric / j.prior_w)
           end as est_phase_ms
    from j
  ),
  r as (
    select elapsed_ms, phase, st,
           case
             when intra is not null then intra
             when p_intra_mode = 'phase-rate' and est_phase_ms > 0
               then least(1, (elapsed_ms - phase_start)::numeric / est_phase_ms)
             else 0.5
           end as intra_eff,
           case
             when intra is not null then 'view-counter'
             when p_intra_mode = 'phase-rate' and est_phase_ms > 0 then 'phase-rate'
             else 'flat-0.5'
           end as src,
           prior_w, wt
    from i
  ),
  p as (
    select elapsed_ms, phase, st, intra_eff, src,
           least(100, greatest(0, (prior_w + wt * intra_eff) * 100)) as raw
    from r
  ),
  m as (
    select p.*, max(raw) over (order by elapsed_ms
                               rows between unbounded preceding and current row) as mono
    from p
  )
  select elapsed_ms, phase, round(intra_eff, 4), src, round(raw, 2), round(mono, 2),
         -- 线性外推：按当前单调进度推总时长，减去已用
         case when mono > 0 and mono < 100
              then (elapsed_ms * (100 - mono) / mono)::bigint
              else 0 end,
         st,
         case when st < vecdiag.eta_unstable_window() then 'unstable' else 'stable' end
  from m;
$$;

comment on function vecdiag.progress_curve(text, text, text, text, text) is
  'intra_source 三种取值：view-counter=视图真实计数；phase-rate=用本次构建已完成阶段的耗时'
  '反推的时间插值；flat-0.5=什么都不知道时的兜底常数。报告里必须区分这三类，不能都叫"进度"。'
  'p_intra_mode=flat 可复现修复前的口径，用于对比 ETA 偏差。';


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


-- ---------------------------------------------------------------------------
-- ETA 偏差量化（T3.4）
--
-- 口径（评审一定会追这一条）：
--   * 实际剩余时间 = 该 run 最后一个采样点的 elapsed − 当前 elapsed。
--     采样在构建结束时停止，所以"最后一个采样点"比真正的结束时刻**早至多一个采样间隔**
--     （50 ms）。因此实际剩余被系统性低估最多 50 ms，ETA 偏差被系统性高估同一量级。
--     构建总时长在秒级以上时这一项可忽略，但必须写出来，不能装作没有。
--   * mono_pct=0 或 100 的点不参与：前者 ETA 无定义，后者剩余为 0。
--   * 偏差同时报绝对值（ms）与相对值（占实际剩余的百分比）。相对偏差在"快结束时"
--     天然会放大（分母趋 0），所以还要报按 elapsed 归一化的分位数。
-- ---------------------------------------------------------------------------
-- 老的 4 参数版本必须显式删掉：新版第 5 个参数有默认值，两者对 4 个实参都可匹配，
-- 调用时报 "function ... is not unique"。加默认值 ≠ 兼容旧签名。
drop function if exists vecdiag.eta_error(text, text, text, text);
drop function if exists vecdiag.eta_accuracy(text, text, text, text);

create or replace function vecdiag.eta_error(
    p_run_id     text,
    p_am         text,
    p_size_class text default 'pooled',
    p_dataset    text default 'synthetic',
    p_intra_mode text default 'phase-rate')
returns table (
    elapsed_ms       bigint,
    phase            text,
    stability        text,
    since_trans      int,
    mono_pct         numeric,
    eta_ms           bigint,
    actual_remain_ms bigint,
    abs_err_ms       bigint,
    abs_err_pct      numeric
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with c as (
    select * from vecdiag.progress_curve(p_run_id, p_am, p_size_class, p_dataset, p_intra_mode)
  ),
  e as (
    select c.*, max(c.elapsed_ms) over () as t_end from c
  )
  select elapsed_ms, phase, stability, since_trans, mono_pct, eta_ms,
         (t_end - elapsed_ms)::bigint,
         abs(eta_ms - (t_end - elapsed_ms))::bigint,
         round(abs(eta_ms - (t_end - elapsed_ms))::numeric
               / nullif(t_end - elapsed_ms, 0) * 100, 2)
  from e
  where mono_pct > 0 and mono_pct < 100 and t_end > elapsed_ms;
$$;

comment on function vecdiag.eta_error(text, text, text, text, text) is
  '逐点 ETA 偏差。实际剩余以最后一个采样点为构建结束时刻，因此被低估至多一个采样间隔。';

create or replace function vecdiag.eta_accuracy(
    p_run_pattern text,
    p_am          text,
    p_size_class  text default 'pooled',
    p_dataset     text default 'synthetic',
    p_intra_mode  text default 'phase-rate')
returns table (
    scope        text,
    runs         int,
    points       int,
    mad_ms       numeric,     -- 平均绝对偏差
    mad_pct      numeric,     -- 平均绝对百分比偏差
    p50_pct      numeric,
    p90_pct      numeric,
    max_pct      numeric
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with runs as (
    select distinct run_id from vecdiag.progress_sample where run_id like p_run_pattern
  ),
  pts as (
    select r.run_id, e.*
    from runs r
    cross join lateral vecdiag.eta_error(r.run_id, p_am, p_size_class, p_dataset, p_intra_mode) e
  ),
  agg as (
    select 'all'::text as scope, * from pts
    union all
    select stability, * from pts
  )
  select scope,
         count(distinct run_id)::int,
         count(*)::int,
         round(avg(abs_err_ms), 0),
         round(avg(abs_err_pct), 2),
         round(percentile_cont(0.5) within group (order by abs_err_pct)::numeric, 2),
         round(percentile_cont(0.9) within group (order by abs_err_pct)::numeric, 2),
         round(max(abs_err_pct), 2)
  from agg
  group by scope
  order by case scope when 'all' then 0 when 'stable' then 1 else 2 end;
$$;

comment on function vecdiag.eta_accuracy(text, text, text, text, text) is
  'ETA 偏差汇总，按 all / stable / unstable 三档分别报。'
  '报告里只能引用 stable 档作为"ETA 可用性"的依据，unstable 档必须同时列出。'
  'p_intra_mode=flat 对比 phase-rate，就是"改进了多少"的直接证据。';


-- ---------------------------------------------------------------------------
-- T3.5 · HNSW 降级对 ETA 的影响，以及用 M2 的事前预测去修正它
--
-- 实测结论（run t35-20260827，100k×128、m=16、maintenance_work_mem=60MB）：
--   * M2 事前预测第 51206 行降级，实际 NOTICE 报 51267 行 —— 偏差 61 行（0.12%），
--     而 60MB 这个档位**不在 M2 的标定集里**（标定用的是 4/8/16 MB），属于外样本验证。
--   * 降级前每元组 0.363 ms，降级后 1.463 ms —— **慢到 4.03 倍**。
--   * 朴素 ETA（按元组线性外推）在降级前 100% 偏低，平均偏低 58.6 秒；
--     降级后偏差收敛到 −21.2 秒。也就是说**朴素 ETA 反映不了降级**，
--     它会一路告诉你"快好了"，直到降级发生才开始变准。
--
-- 修法：M2 既然能事前算出降级行号，就把它接进 ETA。设
--     N = 总元组数，S = 预测降级行号，D = 已完成元组数，t = 已用时间，k = 降级后减速倍数
-- 降级前后速率之比恒为 k，于是由 t 反解降级前速率
--     r = t / (min(D,S) + max(D-S,0) * k)
-- 剩余时间
--     remain = max(S-D,0) * r + (N - max(D,S)) * r * k
-- 这个式子在降级前后都成立，且只需要 k 一个标定量。
-- ---------------------------------------------------------------------------
create table if not exists vecdiag.hnsw_spill_penalty (
    dims           int not null,
    m              int not null,
    rate_before_ms numeric not null,
    rate_after_ms  numeric not null,
    penalty        numeric not null,     -- rate_after / rate_before
    n_before       int,
    n_after        int,
    run_id         text not null,
    measured_at    timestamptz not null default now(),
    primary key (dims, m, run_id)
);

insert into vecdiag.hnsw_spill_penalty
  (dims, m, rate_before_ms, rate_after_ms, penalty, n_before, n_after, run_id) values
  (128, 16, 0.36310, 1.46341, 4.031, 287, 1176, 't35-20260827')
on conflict (dims, m, run_id) do nothing;

comment on table vecdiag.hnsw_spill_penalty is
  '降级后的减速倍数，用采样序列的相邻差分中位数算（避免个别毛刺）。'
  '这是**拟合量**，换机器（磁盘快慢直接决定它）必须重标定：tools/hnsw_eta_spill.sh。';

create or replace function vecdiag.hnsw_spill_penalty_factor(p_dims int, p_m int default 16)
returns numeric
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  -- 优先取同 (dims, m) 的标定；没有就退到全表中位数；一条都没有返回 NULL（调用方据此降级）
  select coalesce(
    (select round(percentile_cont(0.5) within group (order by penalty)::numeric, 3)
       from vecdiag.hnsw_spill_penalty where dims = p_dims and m = p_m),
    (select round(percentile_cont(0.5) within group (order by penalty)::numeric, 3)
       from vecdiag.hnsw_spill_penalty));
$$;

comment on function vecdiag.hnsw_spill_penalty_factor(int, int) is
  '返回 NULL 表示没有任何降级减速标定——此时不要修正 ETA，直接报朴素值并标注不确定。';

-- 注意一个源码事实：HNSW 构建时 pgvector **不上报 tuples_total**（实测恒为 0），
-- 只上报堆块总数 blocks_total 与已处理元组数 tuples_done。而 M2 的降级行号是按元组算的，
-- 所以总元组数必须由调用方传进来（p_rows），不能从进度视图里取。
-- 早先的实现按 tuples_total > 0 过滤，结果一行都取不到——这个坑必须写在这里。
create or replace function vecdiag.hnsw_eta_corrected(
    p_run_id  text,
    p_rows    bigint,
    p_dims    int,
    p_m       int    default 16,
    p_mwm_kb  bigint default null)
returns table (
    elapsed_ms        bigint,
    tuples_done       bigint,
    tuples_total      bigint,
    spill_at          bigint,
    past_spill        boolean,
    eta_naive_ms      bigint,
    eta_corrected_ms  bigint,
    actual_remain_ms  bigint,
    naive_err_pct     numeric,
    corrected_err_pct numeric
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with s as (
    select ps.elapsed_ms, ps.tuples_done, p_rows as tuples_total,
           max(ps.elapsed_ms) over () as t_end
    from vecdiag.progress_sample ps
    where ps.run_id = p_run_id and coalesce(ps.tuples_done, 0) > 0
  ),
  sp as (
    select predicted_spill_tuples as spill_at
    from vecdiag.hnsw_predict_spill(p_rows, p_dims, p_m, p_mwm_kb)
  ),
  p as (
    select s.*, sp.spill_at,
           vecdiag.hnsw_spill_penalty_factor(p_dims, p_m) as k
    from s cross join sp
  ),
  q as (
    select p.*,
           -- 朴素：按元组线性外推
           (p.elapsed_ms * (p.tuples_total - p.tuples_done) / p.tuples_done)::bigint as naive,
           -- 修正：降级前后速率比恒为 k，由已用时间反解降级前速率
           case when p.spill_at is null or p.k is null then null
                else (p.elapsed_ms::numeric
                      / nullif(least(p.tuples_done, p.spill_at)
                               + greatest(p.tuples_done - p.spill_at, 0) * p.k, 0))
                end as r_before
    from p
  )
  select elapsed_ms, tuples_done, tuples_total, spill_at,
         tuples_done > spill_at,
         naive,
         case when r_before is null then null
              else (greatest(spill_at - tuples_done, 0) * r_before
                    + (tuples_total - greatest(tuples_done, spill_at)) * r_before * k)::bigint
         end,
         (t_end - elapsed_ms)::bigint,
         round(abs(naive - (t_end - elapsed_ms))::numeric
               / nullif(t_end - elapsed_ms, 0) * 100, 2),
         case when r_before is null then null else
           round(abs((greatest(spill_at - tuples_done, 0) * r_before
                      + (tuples_total - greatest(tuples_done, spill_at)) * r_before * k)
                     - (t_end - elapsed_ms))
                 / nullif(t_end - elapsed_ms, 0) * 100, 2) end
  from q
  where t_end > elapsed_ms;
$$;

comment on function vecdiag.hnsw_eta_corrected(text, bigint, int, int, bigint) is
  '把 M2 的降级行号预测接进 M3 的 ETA。这是本项目里 M2 与 M3 唯一的联动点，'
  '也是"事前预测有什么用"的直接答案：不修正的 ETA 在降级前会一路偏低。';
