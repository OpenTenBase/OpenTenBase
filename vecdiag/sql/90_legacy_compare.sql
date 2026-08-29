-- vecdiag 20 · 旧模型（pgvector 0.8.0 口径）对照实现
--
-- 只用于"新旧对照"这一件事：证明把旧公式搬到新基线上会错多少、错在哪。
-- **不要**把它当预测工具用。
--
-- 0.8.0 的原始代码（in-tree pgvector 0.8.0，src/ivfkmeans.c:277-299）：
--   samplesSize        = VECTOR_ARRAY_SIZE(samples->maxlen, itemsize)   ← 容量，不是实采条数
--   centersSize        = VECTOR_ARRAY_SIZE(centers->maxlen, itemsize)
--   newCentersSize     = VECTOR_ARRAY_SIZE(numCenters, itemsize)
--   aggSize            = sizeof(float) * (int64) numCenters * dimensions
--   centerCountsSize   = sizeof(int) * numCenters
--   closestCentersSize = sizeof(int) * numSamples          ← numSamples = samples->length
--   lowerBoundSize     = sizeof(float) * numSamples * numCenters
--   upperBoundSize     = sizeof(float) * numSamples
--   sSize              = sizeof(float) * numCenters
--   halfcdistSize      = sizeof(float) * numCenters * numCenters
--   newcdistSize       = sizeof(float) * numCenters
--   totalSize = 以上 11 项之和                              ← **只有一个检查点**
--   触发条件  totalSize > (Size) maintenance_work_mem * 1024L   ← 按字节比，不是按 kB
--
-- 与 0.8.6 的三处结构差异：
--   1. numSamples = max(lists*50, 10000)，**没有 relpages*MaxHeapTuplesPerPage 上限**
--      （0.8.0 的 ivfbuild.c:414 就写着 "TODO Ensure within maintenance_work_mem"，
--        C1/C2 两个检查点正是后来对这个 TODO 的实现）；
--   2. 只有一个检查点，无法表达"报错数字取决于哪个检查点先越界"；
--   3. 阈值按字节比较，0.8.6 改成 floor(bytes/1024) > mwm_kb，两者最多差 1023 字节。

\set ON_ERROR_STOP on

create or replace function vecdiag.ivfflat_legacy080_num_samples(p_lists int)
returns bigint
language sql immutable
set search_path = pg_catalog, pg_temp
as $$
  select greatest(p_lists::bigint * 50, 10000::bigint);
$$;

comment on function vecdiag.ivfflat_legacy080_num_samples(int) is
  '0.8.0 口径：无 maxTuples 上限。小表上因此显著高估（对照实验用，不作预测）。';

create or replace function vecdiag.ivfflat_predict_legacy080(
    p_rows   bigint,
    p_dims   int,
    p_lists  int,
    p_mwm_kb bigint default null
) returns table (
    legacy_bytes   bigint,
    legacy_mb      int,
    legacy_fires   boolean,
    legacy_samples bigint,
    sampled        bigint
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with p as (
    select vecdiag.vector_itemsize(p_dims)                as isize,
           p_lists::bigint                                as l,
           p_dims::bigint                                 as d,
           vecdiag.ivfflat_legacy080_num_samples(p_lists)  as ns
  ),
  q as (
    select p.*, least(p.ns, greatest(p_rows, 0)) as sampled from p
  ),
  t as (
    select q.*,
           vecdiag.vector_array_size(ns, isize)                 -- samplesSize（用容量）
         + vecdiag.vector_array_size(l, isize)                  -- centersSize
         + vecdiag.vector_array_size(l, isize)                  -- newCentersSize
         + 4 * l * d                                            -- aggSize
         + 4 * l                                                -- centerCountsSize
         + 4 * sampled                                          -- closestCentersSize
         + 4 * sampled * l                                      -- lowerBoundSize
         + 4 * sampled                                          -- upperBoundSize
         + 4 * l                                                -- sSize
         + 4 * l * l                                            -- halfcdistSize
         + 4 * l as total                                       -- newcdistSize
    from q
  )
  select total,
         (total / 1048576 + 1)::int,
         -- 0.8.0 按字节比较：totalSize > mwm_kb * 1024
         total > coalesce(p_mwm_kb, vecdiag.current_mwm_kb()) * 1024,
         ns, sampled
  from t;
$$;

comment on function vecdiag.ivfflat_predict_legacy080(bigint, int, int, bigint) is
  '旧模型对照。它只有一个总量、没有 first_hit，因此在低内存/大 lists 场景下无法复现报错文本。';

