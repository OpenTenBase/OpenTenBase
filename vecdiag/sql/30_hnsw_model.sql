-- vecdiag 30 · HNSW 图内存与落盘降级预警（M2）
--
-- 上游现状（这是本模块存在的理由）：
--   * 图放不下 maintenance_work_mem 时只打一个 **NOTICE**（不是 ERROR），
--     然后 FlushPages() 转磁盘继续建 —— 构建不会失败，只会变得很慢；
--     `hnswbuild.c:530-549`
--   * 内存数字只在 `#ifdef HNSW_MEMORY` 下 elog(INFO)（`hnswbuild.c:307`），生产拿不到
--   * 没有任何构建侧 GUC（实测 7 个 GUC 全是查询/扫描侧，见 tests/upstream_inventory.sql）
--   → 因此"**事前**预测图内存、预测在第几行降级、给出内存下限建议"是上游空白。
--     注意表述边界：不能说"上游没有内存管理"，它有 memoryTotal 与 FlushPages，
--     缺的是**事前预测**。
--
-- 标定原理：降级判定是 `memoryUsed + margin >= memoryTotal`（串行构建 margin = 0，
-- `hnswbuild.c:507`），而 `memoryTotal = maintenance_work_mem * 1024`（`hnswbuild.c:724`）。
-- 所以降级那一刻 memoryUsed ≈ maintenance_work_mem，于是
--     per_element = maintenance_work_mem_bytes / N      （N 来自 NOTICE 的 tuples 数）
-- 这个式子的可检验推论是 per_element 与 maintenance_work_mem 无关。
-- 实测（tools/hnsw_spill_probe.sh，results/m2-20260826/）：
--     dims=128,m=16：4 MB → 1226.4 B/元素；8 MB → 1226.8 B/元素（相对离散 0.03%）
--     dims=384,m=16：8 MB → 2252.0 B/元素；16 MB → 2250.8 B/元素（相对离散 0.05%）
-- 推论成立，因此可以用一次标定去预测其他内存档位的降级点。

\set ON_ERROR_STOP on

create table if not exists vecdiag.hnsw_calib (
    id            serial primary key,
    dims          int    not null,
    m             int    not null,
    ef_construction int  not null,
    mwm_kb        bigint not null,
    spill_tuples  bigint,
    per_element   numeric,
    run_id        text,
    measured_at   timestamptz not null default now()
);

comment on table vecdiag.hnsw_calib is
  '降级点标定样本。per_element = mwm_kb*1024/spill_tuples。换机器或换 pgvector 版本必须重标定。';

-- ---------------------------------------------------------------------------
-- per_element 解析式
--
--   per_element(dims, m) = base + slot_coef * m + 4 * dims
--
-- 三项的性质不同，报告里必须分开说：
--   4 * dims      **结构上可识别**：向量本体每维一个 float。实测 dims 128→384
--                 每维增量 4.005 字节，与 sizeof(float) 相符。
--   slot_coef * m **拟合项**：邻居列表。机制清楚（HnswInitNeighbors 为 0..level 每层
--                 分配一个数组，第 0 层 2m 个槽、其余每层 m 个槽，level 是几何分布，
--                 `hnswutils.c:218-227`），但每槽字节数与期望层数没有做结构推导，
--                 系数由标定拟合得到。
--   base          拟合项：HnswElementData 本体与指针数组的常数部分。
--
-- 拟合来源：dims=128 的 m ∈ {8,16,32} 三点做线性回归，再用 dims=384 校验常数项。
-- 残差见 docs/M2-hnsw-spill-model.md。
-- ---------------------------------------------------------------------------
create table if not exists vecdiag.hnsw_coef (
    key        text primary key,
    value      numeric not null,
    kind       text    not null check (kind in ('structural', 'fitted')),
    source_ref text    not null,
    note       text
);

insert into vecdiag.hnsw_coef (key, value, kind, source_ref, note) values
  ('bytes_per_dim', 4, 'structural', 'pgvector v0.8.6 vector 的 x[] 为 float',
   '实测 dims 128→384 每维增量 4.005 字节，与 sizeof(float) 相符'),
  ('slot_coef', 31.89, 'fitted', 'hnswutils.c:218-227（邻居数组分配点）',
   'dims=128 上 m∈{8,16,32} 三点线性拟合；机制清楚但每槽字节数未做结构推导'),
  ('base_bytes', 206.4, 'fitted', 'hnswutils.c:245-267（HnswInitElement）',
   'HnswElementData 与指针数组的常数部分，拟合值')
on conflict (key) do nothing;

create or replace function vecdiag.hnsw_per_element(p_dims int, p_m int default 16)
returns numeric
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  select (select value from vecdiag.hnsw_coef where key = 'base_bytes')
       + (select value from vecdiag.hnsw_coef where key = 'slot_coef') * p_m
       + (select value from vecdiag.hnsw_coef where key = 'bytes_per_dim') * p_dims;
$$;

comment on function vecdiag.hnsw_per_element(int, int) is
  '每元素图内存（字节）。ef_construction 不进入该式——实测 ef 64→200 降级点完全相同（6838 行）。';

-- ---------------------------------------------------------------------------
-- 降级预警：事前告诉用户会不会降级、在第几行降级、内存下限该给多少
-- ---------------------------------------------------------------------------
create or replace function vecdiag.hnsw_predict_spill(
    p_rows     bigint,
    p_dims     int,
    p_m        int    default 16,
    p_mwm_kb   bigint default null,
    p_margin   numeric default 1.15      -- 建议内存的安全系数
) returns table (
    will_spill              boolean,
    predicted_spill_tuples  bigint,
    estimated_graph_mb      numeric,
    recommended_mwm_mb      int,
    per_element_bytes       numeric,
    mwm_kb                  bigint,
    confidence              text,
    evidence_source         text
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with p as (
    select vecdiag.hnsw_per_element(p_dims, p_m)                as pe,
           coalesce(p_mwm_kb, vecdiag.current_mwm_kb())          as mwm
  ),
  q as (
    select p.*,
           pe * greatest(p_rows, 0)                              as graph_bytes,
           floor(mwm * 1024 / pe)::bigint                        as spill_at
    from p
  )
  select graph_bytes >= mwm * 1024,
         case when graph_bytes >= mwm * 1024 then spill_at else null end,
         round(graph_bytes / 1048576.0, 1),
         ceil(graph_bytes * p_margin / 1048576.0)::int,
         pe, mwm,
         -- 标定覆盖 dims∈[128,384]、m∈[8,32]；越界必须标 extrapolated
         case when p_dims between 128 and 384 and p_m between 8 and 32
              then 'calibrated' else 'extrapolated' end,
         'results/m2-20260826/hnsw_spill.csv（8 组实测，A/C 组自洽性 0.03%/0.05%）'
  from q;
$$;

comment on function vecdiag.hnsw_predict_spill(bigint, int, int, bigint, numeric) is
  '事前预警。confidence=extrapolated 时说明 (dims,m) 超出标定范围，结论只能当量级参考。';

-- ---------------------------------------------------------------------------
-- 降级点为什么只能给区间：memoryUsed 是"内存上下文已分配块总量"，不是请求量之和
--
-- 串行构建走 HnswMemoryContextAlloc（hnswbuild.c:646-654）：
--     memoryUsed = MemoryContextMemAllocated(graphCtx, false)
-- 而 AllocSet 的块是从 8 kB 起**倍增**（上限 8 MB）的，所以 memoryUsed 呈阶梯上升。
-- 后果：maintenance_work_mem 涨一点但不够容纳下一个块时，降级点几乎不动。
--
-- 实测证据（dims=128, m=16, results/m2v-20260826/）：
--     mwm 4096 kB → 第 3420 行降级
--     mwm 4608 kB → 第 3422 行降级   ← 内存多给 12.5%，降级点只挪了 2 行
--     mwm 8192 kB → 第 6838 行降级
-- 4096 与 4608 落在同一个块级台阶里，所以"按内存线性外推"在 4608 那档偏了 12.2%。
-- 这是 E2 那组唯一的大误差来源，不是模型系数错。
--
-- 台阶的下界：块序列 8K,16K,…,2^i·8K 的累计和为 8K·(2^(i+1)−1)。
-- 取不超过 maintenance_work_mem 的最大累计和作为**有效容量下界**。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.allocset_capacity_floor(p_bytes bigint)
returns bigint
language sql immutable strict
set search_path = pg_catalog, pg_temp
as $$
  -- 块从 8 kB 起倍增到 8 MB 封顶；封顶后按 8 MB 一块继续加。
  -- 倍增段累计和 = 8K·(2^(i+1)−1)，i=0..10（2^10·8K = 8 MB）。
  with s as (
    select 8192::bigint * (power(2, i + 1)::bigint - 1) as cum
    from generate_series(0, 10) as g(i)
  ),
  doubling as (
    select coalesce(max(cum), 0) as cum_le, (select max(cum) from s) as cum_max
    from s where cum <= p_bytes
  )
  select case
           when cum_le < cum_max then cum_le            -- 还在倍增段
           else cum_max + ((p_bytes - cum_max) / (8 * 1024 * 1024)) * (8 * 1024 * 1024)
         end
  from doubling;
$$;

comment on function vecdiag.allocset_capacity_floor(bigint) is
  'AllocSet 块倍增（8kB→8MB 封顶，之后每块 8MB）造成的有效容量台阶下界。';

create or replace function vecdiag.hnsw_spill_range(
    p_rows   bigint,
    p_dims   int,
    p_m      int    default 16,
    p_mwm_kb bigint default null
) returns table (
    spill_low   bigint,
    spill_high  bigint,
    range_pct   numeric,
    note        text
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with p as (
    select vecdiag.hnsw_per_element(p_dims, p_m)              as pe,
           coalesce(p_mwm_kb, vecdiag.current_mwm_kb()) * 1024 as bytes
  ),
  q as (
    -- 区间两端再各放 0.5%：per_element 本身是拟合值，标定样本间的离散约 0.3%。
    select floor(vecdiag.allocset_capacity_floor(bytes) / pe * 0.995)::bigint as lo,
           ceil(bytes / pe * 1.005)::bigint                                   as hi
    from p
  )
  select lo, hi,
         round((hi - lo) / nullif(hi, 0)::numeric * 100, 1),
         '下端来自 AllocSet 块台阶，上端为朴素线性外推，两端各放 0.5% 覆盖 per_element 的拟合离散。'
         '实测降级点应落在区间内；区间随 maintenance_work_mem 变大而收窄。'
  from q;
$$;

comment on function vecdiag.hnsw_spill_range(bigint, int, int, bigint) is
  '降级点区间。maintenance_work_mem 只有几 MB 时区间较宽，这是块粒度决定的，不是模型系数不准。';




