-- vecdiag 10 · IVFFlat 构建内存模型（对齐 pgvector v0.8.6）
--
-- 核心事实（与旧版本 0.8.0 的三处差异，缺一条模型就对不上报错原文）：
--   1) memoryUsed 是累积的，samples/centers 由 ivfbuild.c 累加，ElkanKmeans 再加 9 项；
--   2) IvfflatCheckMemoryUsage() 一次构建被调用 **三次**，累积值递增，
--      **第一个越界的检查点决定报错数字**：
--        C1  ivfbuild.c:394   仅 centers
--        C2  ivfbuild.c:459   + samples
--        C3  ivfkmeans.c:290  + kmeans 9 项
--   3) numSamples 的上限是 relpages * MaxHeapTuplesPerPage（ivfbuild.c:446），
--      **不是表行数**；而 C3 里用的是实际采到的条数 samples->length。
--
-- 报错文本（src/ivfutils.c:121-129）：
--   触发条件  totalSize / 1024 > maintenance_work_mem(kB)      ← 整数除法
--   报错数字  totalSize / (1024*1024) + 1                      ← +1 向上取整

\set ON_ERROR_STOP on

do $$
begin
    if not exists (select 1 from pg_type t join pg_namespace n on n.oid = t.typnamespace
                   where t.typname = 'checkpoint_kind' and n.nspname = 'vecdiag') then
        create type vecdiag.checkpoint_kind as enum ('C1', 'C2', 'C3', 'none');
    end if;
end;
$$;

-- 参数改过名（p_empty_build → p_empty_build），CREATE OR REPLACE 不允许改入参名，先删
drop function if exists vecdiag.ivfflat_num_samples(int, bigint, boolean);
drop function if exists vecdiag.ivfflat_memory_breakdown(bigint, int, int, bigint, boolean, bigint);
drop function if exists vecdiag.ivfflat_predict(bigint, int, int, bigint, boolean, bigint, bigint);
drop function if exists vecdiag.ivfflat_predict_table(regclass, int, name, bigint);
drop function if exists vecdiag.ivfflat_mwm_plan(bigint, int, int, bigint, boolean);

-- ---------------------------------------------------------------------------
-- numSamples：ivfbuild.c:442-459
--   buildstate->heap 为 NULL → 1
--   否则 numSamples = Max(Min(Max(lists*50, 10000), relpages*T), 1)
--
-- ⚠️ 关于 heap == NULL 的语义（2026-08-26 实测纠正）：
--   源码注释写的是 "Skip samples for unlogged table"，但 heap == NULL 实际对应的是
--   **ambuildempty（初始化 fork）** 那条路径，**不是**"对 unlogged 表执行 CREATE INDEX"。
--   实测：unlogged 表上正常建索引，numSamples 仍按上面的公式算（500 行/256 维/lists=200
--   实测命中 C2 报 11 MB，正是 numSamples=10000 的结果，而不是 numSamples=1）。
--   所以参数名叫 p_empty_build，不要理解成"表是不是 unlogged"。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.ivfflat_num_samples(
    p_lists       int,
    p_relpages    bigint,
    p_empty_build boolean default false
) returns bigint
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  select case
           when p_empty_build then 1::bigint
           else greatest(
                  least(
                    greatest(p_lists::bigint * 50, 10000::bigint),
                    coalesce(p_relpages, 0) * vecdiag.abi('max_heap_tuples_per_page')
                  ), 1::bigint)
         end;
$$;

comment on function vecdiag.ivfflat_num_samples(int, bigint, boolean) is
  '上限用 relpages*MaxHeapTuplesPerPage（ivfbuild.c:446），不是表行数。p_empty_build 对应 heap==NULL 的 ambuildempty 路径，与表是否 unlogged 无关。';

-- ---------------------------------------------------------------------------
-- relpages 估算（回退路径）
--
-- 优先用 pg_class.relpages。只有行数、没有真实页数时才用这个函数，
-- 并且必须在输出里把 pages_estimated 标成 true。
-- 简化前提：表形如 (int, vector(D))；其他表结构会有偏差，偏差要写进局限。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.estimate_relpages(p_rows bigint, p_dims int)
returns bigint
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with c as (
    select current_setting('block_size')::bigint                as blcksz,
           24::bigint                                           as page_header,
           4::bigint                                            as item_id,
           -- t_hoff = MAXALIGN(SizeofHeapTupleHeader=23) = 24
           -- 之后是 int(4) → 4 字节对齐补到 28 → vector 变长 520 → 合计 MAXALIGN
           vecdiag.maxalign(24 + 4 + 4 + vecdiag.vector_itemsize(p_dims)) as tuple_len
  )
  select greatest(
           ceil(p_rows::numeric
                / greatest(((blcksz - page_header) / (tuple_len + item_id)), 1))::bigint,
           case when p_rows > 0 then 1 else 0 end)
  from c;
$$;

comment on function vecdiag.estimate_relpages(bigint, int) is
  '仅在拿不到 pg_class.relpages 时使用；调用方必须把 pages_estimated 标为 true。'
  '⚠️ dims 大到让 vector 超过 TOAST 阈值（约 2000 字节，即 dims ≳ 498）时，值会被压缩或移到 '
  'TOAST 表，主元组变小、relpages 远小于本函数的估算，numSamples 上限随之算错。'
  '实测 100 行 × 960 维那组，用估算值会把预测从 17 MB 抬到 58 MB。高维场景必须先 ANALYZE 用真实 relpages。';

-- 高维时提醒调用方：estimate_relpages 不可信
create or replace function vecdiag.toast_risk(p_dims int)
returns boolean
language sql immutable strict
set search_path = pg_catalog, pg_temp
as $$
  -- TOAST_TUPLE_THRESHOLD = MAXALIGN(BLCKSZ/4) 之下才留在主元组，
  -- 超过就可能被压缩或外置，堆内元组大小与 itemsize 脱钩。
  select vecdiag.vector_itemsize(p_dims) > (current_setting('block_size')::bigint / 4);
$$;

comment on function vecdiag.toast_risk(int) is
  'true 表示该维度下 vector 可能被 TOAST，estimate_relpages 的结果不可用于预测，必须取真实 relpages。';

-- ---------------------------------------------------------------------------
-- 分项 breakdown
--
-- 每一行都标注它计入哪个检查点，以及对应的源码位置。评审会逐项核对，
-- 所以 source_ref 不许留空、不许写"见源码"这种话。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.ivfflat_memory_breakdown(
    p_rows     bigint,
    p_dims     int,
    p_lists    int,
    p_relpages bigint  default null,
    p_empty_build boolean default false,
    p_itemsize bigint  default null
) returns table (
    component  text,
    bytes      bigint,
    checkpoint vecdiag.checkpoint_kind,
    source_ref text,
    note       text
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with p as (
    select coalesce(p_itemsize, vecdiag.vector_itemsize(p_dims))          as isize,
           coalesce(p_relpages, vecdiag.estimate_relpages(p_rows, p_dims)) as pages,
           p_lists::bigint                                                as l,
           p_dims::bigint                                                 as d
  ),
  s as (
    select p.*,
           vecdiag.ivfflat_num_samples(p_lists, p.pages, p_empty_build)      as numsamples
    from p
  ),
  q as (
    select s.*,
           least(s.numsamples, greatest(p_rows, 0))                       as sampled
    from s
  )
  select 'centers (VectorArrayInit)',
         vecdiag.vector_array_size(l, isize), 'C1'::vecdiag.checkpoint_kind,
         'ivfbuild.c:394', 'memoryUsed 起点：VECTOR_ARRAY_SIZE(lists, itemsize)' from q
  union all
  select 'samples (VectorArrayInit)',
         vecdiag.vector_array_size(numsamples, isize), 'C2',
         'ivfbuild.c:459', 'numSamples 用上限值，不是实采条数' from q
  union all
  select 'newCenters', vecdiag.vector_array_size(l, isize), 'C3',
         'ivfkmeans.c:266', 'VECTOR_ARRAY_SIZE(numCenters, centers->itemsize)' from q
  union all
  select 'agg', 4 * l * d, 'C3', 'ivfkmeans.c:267', 'sizeof(float)*numCenters*dimensions' from q
  union all
  select 'centerCounts', 4 * l, 'C3', 'ivfkmeans.c:268', 'sizeof(int)*numCenters' from q
  union all
  select 'closestCenters', 4 * sampled, 'C3', 'ivfkmeans.c:269',
         'sizeof(int)*numSamples，此处 numSamples = samples->length（实采条数）' from q
  union all
  select 'lowerBound', 4 * sampled * l, 'C3', 'ivfkmeans.c:270',
         '主导项：sizeof(float)*numSamples*numCenters' from q
  union all
  select 'upperBound', 4 * sampled, 'C3', 'ivfkmeans.c:271', 'sizeof(float)*numSamples' from q
  union all
  select 's', 4 * l, 'C3', 'ivfkmeans.c:272', 'sizeof(float)*numCenters' from q
  union all
  select 'halfcdist', 4 * l * l, 'C3', 'ivfkmeans.c:273', 'sizeof(float)*numCenters*numCenters' from q
  union all
  select 'newcdist', 4 * l, 'C3', 'ivfkmeans.c:274', 'sizeof(float)*numCenters' from q;
$$;

comment on function vecdiag.ivfflat_memory_breakdown(bigint, int, int, bigint, boolean, bigint) is
  '11 行分项：centers(C1) + samples(C2) + kmeans 9 项(C3)。每行标源码行号，无魔数。';

-- ---------------------------------------------------------------------------
-- 汇总预测：给出三个检查点的累积值、哪个先越界、以及报错文本里会出现的数字
-- ---------------------------------------------------------------------------
create or replace function vecdiag.ivfflat_predict(
    p_rows     bigint,
    p_dims     int,
    p_lists    int,
    p_relpages bigint  default null,
    p_empty_build boolean default false,
    p_mwm_kb   bigint  default null,
    p_itemsize bigint  default null
) returns table (
    first_hit       vecdiag.checkpoint_kind,
    predicted_mb    int,
    mwm_kb          bigint,
    c1_bytes        bigint,
    c2_bytes        bigint,
    c3_bytes        bigint,
    num_samples     bigint,
    sampled         bigint,
    relpages_used   bigint,
    pages_estimated boolean,
    c3_applicable   boolean
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with b as (
    select checkpoint, sum(bytes)::bigint as bytes
    from vecdiag.ivfflat_memory_breakdown(p_rows, p_dims, p_lists, p_relpages,
                                          p_empty_build, p_itemsize)
    group by checkpoint
  ),
  c as (
    select coalesce((select bytes from b where checkpoint = 'C1'), 0)::bigint as c1,
           coalesce((select bytes from b where checkpoint = 'C2'), 0)::bigint as c2_delta,
           coalesce((select bytes from b where checkpoint = 'C3'), 0)::bigint as c3_delta
  ),
  a as (
    select c1, (c1 + c2_delta)::bigint as c2, (c1 + c2_delta + c3_delta)::bigint as c3 from c
  ),
  m as (
    select a.*,
           coalesce(p_mwm_kb, vecdiag.current_mwm_kb())                    as mwm,
           coalesce(p_relpages, vecdiag.estimate_relpages(p_rows, p_dims)) as pages,
           p_relpages is null                                             as est
    from a
  ),
  n as (
    select m.*,
           vecdiag.ivfflat_num_samples(p_lists, m.pages, p_empty_build)   as ns
    from m
  ),
  q as (
    -- samples->length == 0 时 IvfflatKmeans 走 RandomCenters，**根本不调 ElkanKmeans**，
    -- 因此 C3 这个检查点不存在（ivfkmeans.c:561-565）。空表就是这种情况。
    select n.*,
           least(n.ns, greatest(p_rows, 0))                               as sampled_len
    from n
  ),
  h as (
    -- 触发条件必须是**整数除法**：floor(bytes/1024) > mwm_kb（ivfutils.c:124）。
    -- 注意 sum(bigint) 返回 numeric，若不显式转回 bigint，除法会变成精确小数，
    -- 判定就成了"向上取整"，边界处会整体偏 1 kB —— 这个坑已被验证矩阵抓到过一次。
    select q.*,
           case when q.c1 / 1024 > q.mwm then 'C1'
                when q.c2 / 1024 > q.mwm then 'C2'
                when q.sampled_len > 0 and q.c3 / 1024 > q.mwm then 'C3'
                else 'none' end::vecdiag.checkpoint_kind as fh
    from q
  )
  select fh,
         case fh
           when 'C1' then (c1 / 1048576 + 1)::int      -- ivfutils.c:128 的 +1 向上取整
           when 'C2' then (c2 / 1048576 + 1)::int
           when 'C3' then (c3 / 1048576 + 1)::int
           else null
         end,
         mwm, c1, c2, c3, ns, sampled_len, pages, est, sampled_len > 0
  from h;
$$;

comment on function vecdiag.ivfflat_predict(bigint, int, int, bigint, boolean, bigint, bigint) is
  '必须看 first_hit：低 maintenance_work_mem 或大 lists 时先越界的往往是 C1/C2，只报单一总量的实现会系统性对不上报错原文。';

-- ---------------------------------------------------------------------------
-- 面向真实表的入口：行数/页数/维度全部从系统目录读，不靠人填
--
-- 维度取自 pg_attribute.atttypmod。实测确认 pgvector 的 vector 类型
-- 直接把维数存在 atttypmod 里（vector(128) → atttypmod = 128），
-- 不像 varchar 那样有 +4 偏移。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.ivfflat_predict_table(
    p_table        regclass,
    p_lists        int,
    p_column       name   default null,
    p_mwm_kb       bigint default null,
    p_rows_exact   bigint default null
) returns table (
    first_hit       vecdiag.checkpoint_kind,
    predicted_mb    int,
    mwm_kb          bigint,
    c1_bytes        bigint,
    c2_bytes        bigint,
    c3_bytes        bigint,
    num_samples     bigint,
    sampled         bigint,
    relpages_used   bigint,
    pages_estimated boolean,
    c3_applicable   boolean
)
language plpgsql stable
set search_path = pg_catalog, pg_temp
as $$
declare
    v_rows     bigint;
    v_pages    bigint;
    v_dims     int;
    v_col      name;
begin
    select c.reltuples::bigint, c.relpages::bigint
      into v_rows, v_pages
      from pg_class c where c.oid = p_table;

    if v_pages is null then
        raise exception 'vecdiag: 找不到关系 %', p_table;
    end if;

    -- reltuples 是 ANALYZE 的**估计值**。主导项 lowerBound = 4*sampled*lists 直接依赖它，
    -- 估计误差会原样放大成预测误差（实测见过 1 MB 级偏差）。验证矩阵这类知道真实行数的
    -- 场景请显式传 p_rows_exact，不要让统计误差混进"模型误差"。
    if p_rows_exact is not null then
        v_rows := p_rows_exact;
    end if;

    -- 定位向量列：显式指定优先，否则取第一个 vector 列
    select a.attname, a.atttypmod
      into v_col, v_dims
      from pg_attribute a
      join pg_type t on t.oid = a.atttypid
     where a.attrelid = p_table and a.attnum > 0 and not a.attisdropped
       and t.typname = 'vector'
       and (p_column is null or a.attname = p_column)
     order by a.attnum
     limit 1;

    if v_dims is null then
        raise exception 'vecdiag: 表 % 上找不到 vector 列（p_column=%）', p_table, p_column;
    end if;
    if v_dims <= 0 then
        raise exception 'vecdiag: 列 %.% 未声明维度，无法预测（vector 而非 vector(N)）', p_table, v_col;
    end if;
    if v_rows < 0 then
        raise exception 'vecdiag: % 的 reltuples 为 %，请先 ANALYZE', p_table, v_rows;
    end if;

    return query
      -- 正常的 CREATE INDEX 一定有 heap，因此 p_empty_build 恒为 false。
      -- 表是不是 unlogged 与此无关（见 ivfflat_num_samples 的注释）。
      select * from vecdiag.ivfflat_predict(v_rows, v_dims, p_lists, v_pages,
                                            false, p_mwm_kb, null);
end;
$$;

comment on function vecdiag.ivfflat_predict_table(regclass, int, name, bigint, bigint) is
  'relpages 依赖统计信息，用之前先 ANALYZE。reltuples 是估计值，知道真实行数时请传 p_rows_exact。';

-- ---------------------------------------------------------------------------
-- 验证矩阵设计辅助：算出让指定检查点先触发的 maintenance_work_mem
--
-- 为什么需要它：要验证 kmeans 那 9 项，mwm 必须落在 C2 与 C3 之间。
-- 低于 C2 会被 C2 截住（9 项一次都算不到），高于 C3 就不报错。
-- 这个窗口实测只有约 12 MB 宽（2000 行/128 维/lists=1000），
-- 靠感觉设内存参数拿到的全是 C2 的证据。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.ivfflat_mwm_plan(
    p_rows     bigint,
    p_dims     int,
    p_lists    int,
    p_relpages bigint  default null,
    p_empty_build boolean default false
) returns table (
    target        vecdiag.checkpoint_kind,
    mwm_kb        bigint,
    expect_mb     int,
    window_kb     bigint,
    comment_text  text
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with p as (
    select c1_bytes / 1024 as k1, c2_bytes / 1024 as k2, c3_bytes / 1024 as k3,
           c1_bytes, c2_bytes, c3_bytes
    from vecdiag.ivfflat_predict(p_rows, p_dims, p_lists, p_relpages, p_empty_build, null, null)
  )
  select 'C1'::vecdiag.checkpoint_kind, greatest(k1 - 1, 64),
         (c1_bytes / 1048576 + 1)::int, null::bigint,
         '低于 floor(C1/1024) 即在第一个检查点越界，报错只反映 centers' from p
  union all
  select 'C2', greatest(k1, 64), (c2_bytes / 1048576 + 1)::int, k2 - k1,
         '落在 [C1, C2) 之间：C1 放行、C2 越界' from p
  union all
  select 'C3', greatest((k2 + k3) / 2, 64), (c3_bytes / 1048576 + 1)::int, k3 - k2,
         '落在 [C2, C3) 之间才真正验证 kmeans 9 项；window_kb 就是这个窗口的宽度' from p
  union all
  select 'none', k3 + 4096, null::int, null::bigint,
         '高于 floor(C3/1024) 时构建应当成功，用于 K15 保守方向的正例' from p;
$$;

comment on function vecdiag.ivfflat_mwm_plan(bigint, int, int, bigint, boolean) is
  '生成验证矩阵用的 maintenance_work_mem 取值。设计 T1.5 的饱和组时必须先调它，不要凭感觉设内存。';





