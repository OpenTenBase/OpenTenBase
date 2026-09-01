-- vecdiag 60 · 构建参数建议表（T2.7）
--
-- 这个模块回答的问题是 M1/M2 不回答的那一个：**"那我到底该用什么参数"**。
--
-- 硬性纪律（门禁 K3/K7）：表里每一个数字都必须能说出它是哪来的，三类之一——
--   source-code   pgvector 源码里的常量/结构事实（默认值、上下界、谁影响谁）
--   upstream-doc  pgvector README 的口径（只给方向，不给量）
--   measured      本机实测（给量，并附 run_id 与产物路径）
-- 三类不能混。上游文档说"调大 ef_construction 召回更好"，那是 upstream-doc；
-- "在本机 100k×128 上调大 ef_construction 的性价比不如调大 m"是 measured，
-- 而且必须带上标定条件，不能当成普适结论。

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- 1) 源码事实：默认值与上下界
-- ---------------------------------------------------------------------------
create table if not exists vecdiag.param_limit (
    am            text not null,
    knob          text not null,
    default_value int,
    min_value     int,
    max_value     int,
    source_ref    text not null,
    primary key (am, knob)
);

insert into vecdiag.param_limit (am, knob, default_value, min_value, max_value, source_ref) values
  ('hnsw',    'm',               16,  2, 100,   'pgvector v0.8.6 src/hnsw.h:54-56'),
  ('hnsw',    'ef_construction', 64,  4, 1000,  'pgvector v0.8.6 src/hnsw.h:57-59'),
  ('hnsw',    'dims',            null, 1, 2000, 'pgvector v0.8.6 src/hnsw.h:33（HNSW_MAX_DIM）'),
  ('ivfflat', 'lists',           100, 1, 32768, 'pgvector v0.8.6 src/ivfflat.h:55-57'),
  ('ivfflat', 'dims',            null, 1, 2000, 'pgvector v0.8.6 src/ivfflat.h:37（IVFFLAT_MAX_DIM）')
on conflict (am, knob) do update
  set default_value = excluded.default_value, min_value = excluded.min_value,
      max_value = excluded.max_value, source_ref = excluded.source_ref;

comment on table vecdiag.param_limit is
  '参数默认值与上下界，全部来自 pgvector 源码常量。越界的建议一律不给。';

-- ---------------------------------------------------------------------------
-- 2) 上游文档口径：只记方向，不记量
-- ---------------------------------------------------------------------------
create table if not exists vecdiag.param_guidance (
    id          serial primary key,
    am          text not null,
    knob        text not null,
    direction   text not null,             -- 调大 / 调小
    effect      text not null,
    source_kind text not null check (source_kind in ('source-code', 'upstream-doc', 'measured')),
    source_ref  text not null
);

delete from vecdiag.param_guidance;
insert into vecdiag.param_guidance (am, knob, direction, effect, source_kind, source_ref) values
  ('hnsw', 'ef_construction', '调大', '召回更好，代价是构建/插入更慢（只给方向，未给量）',
   'upstream-doc', 'pgvector v0.8.6 README.md:268'),
  ('hnsw', 'ef_construction', '调大', '不改变索引体积：ef_construction 只影响建图时候选集大小，不进入每元素邻居槽数',
   'source-code', 'hnswutils.c:218-227（邻居数组按 m 分配，与 ef_construction 无关）'),
  ('hnsw', 'm', '调大', '每元素邻居槽数变多（第 0 层 2m，其余每层 m），图内存与索引体积同时上升',
   'source-code', 'hnswutils.c:218-227'),
  ('ivfflat', 'lists', '调大', '查询更快，代价是召回下降（上游明确写了"at the expense of recall"）',
   'upstream-doc', 'pgvector v0.8.6 README.md:736-739'),
  ('ivfflat', 'lists', '取值', '起点建议：≤100 万行取 rows/1000，超过 100 万行取 sqrt(rows)',
   'upstream-doc', 'pgvector v0.8.6 README.md:341'),
  ('ivfflat', 'probes', '取值', '起点建议 sqrt(lists)——**lists 调大时 probes 必须同步调大**，否则召回掉',
   'upstream-doc', 'pgvector v0.8.6 README.md:342');

comment on table vecdiag.param_guidance is
  '上游给的方向性口径。凡是"更好/更慢"这种没有量的说法都归在这里，'
  '本项目的贡献是把它们在本机变成有量的表（见 param_measure）。';

-- ---------------------------------------------------------------------------
-- 3) 本机实测：由 tools/load_param_facts.sh 从 param_sweep.csv 灌入
-- ---------------------------------------------------------------------------
create table if not exists vecdiag.param_measure (
    am               text   not null,
    m                int,
    ef_construction  int,
    lists            int,
    rows             bigint not null,
    dims             int    not null,
    dataset          text   not null,
    mwm              text   not null,
    n_repeats        int    not null,
    build_min_ms     bigint,
    build_median_ms  bigint,
    build_max_ms     bigint,
    index_mb         numeric,
    spilled          boolean,
    topk             int,
    query_knob       text,                 -- 召回测量时的查询侧参数（ef_search / probes）
    recall_at_k      numeric,
    query_ms_mean    numeric,
    run_id           text   not null,
    measured_at      timestamptz not null default now()
);

-- m / ef_construction / lists 三者互斥地为 NULL（HNSW 没有 lists，IVFFlat 没有 m），
-- 而主键列不允许 NULL，所以唯一性用表达式唯一索引来保证，不用主键。
create unique index if not exists param_measure_key on vecdiag.param_measure
  (am, coalesce(m, -1), coalesce(ef_construction, -1), coalesce(lists, -1),
   rows, dims, dataset, run_id);

comment on table vecdiag.param_measure is
  '构建参数扫描的实测结果。recall_at_k 的 ground truth 是**库内顺序扫描重算的 exact top-K**——'
  '公开数据集自带的 groundtruth 是针对全量 100 万底库的，用在子集上不成立。'
  '召回列属方向一的指标口径，这里只作为构建参数取舍的质量轴，不作为方向一交付。';

-- ---------------------------------------------------------------------------
-- 4) 帕累托前沿：哪些配置是"花了时间没换来召回"的
--
-- 定义：配置 A 被 B 支配 = B 构建更快**且**召回不低于 A。被支配的配置不该出现在建议里。
-- 这一层是本模块最有用的输出：它把"调大 ef_construction 提召回"这句上游口径，
-- 在本机标定上修正成"同样的时间预算下调大 m 更划算"。
-- ---------------------------------------------------------------------------
create or replace view vecdiag.param_pareto as
select a.am, a.rows, a.dims, a.dataset, a.run_id,
       a.m, a.ef_construction, a.lists,
       a.build_median_ms, a.index_mb, a.recall_at_k, a.query_ms_mean,
       a.topk, a.query_knob, a.n_repeats,
       not exists (
         select 1 from vecdiag.param_measure b
         where b.am = a.am and b.rows = a.rows and b.dims = a.dims
           and b.dataset = a.dataset and b.run_id = a.run_id
           and (b.m, b.ef_construction, b.lists) is distinct from (a.m, a.ef_construction, a.lists)
           and b.build_median_ms <= a.build_median_ms
           and b.recall_at_k     >= a.recall_at_k
           and (b.build_median_ms < a.build_median_ms or b.recall_at_k > a.recall_at_k)
       ) as on_frontier,
       -- format() 不会返回 NULL，所以这里不能用 coalesce 去区分两种访问方法，
       -- 否则 IVFFlat 行会打印成 "m=/ef="。用 case 显式分流。
       (select string_agg(
                 case when b.am = 'hnsw'
                      then format('m=%s/ef=%s（%s ms, recall %s）', b.m, b.ef_construction,
                                  b.build_median_ms, b.recall_at_k)
                      else format('lists=%s（%s ms, recall %s）', b.lists,
                                  b.build_median_ms, b.recall_at_k) end,
                 '; ' order by b.build_median_ms)
          from vecdiag.param_measure b
         where b.am = a.am and b.rows = a.rows and b.dims = a.dims
           and b.dataset = a.dataset and b.run_id = a.run_id
           and (b.m, b.ef_construction, b.lists) is distinct from (a.m, a.ef_construction, a.lists)
           and b.build_median_ms <= a.build_median_ms
           and b.recall_at_k     >= a.recall_at_k
           and (b.build_median_ms < a.build_median_ms or b.recall_at_k > a.recall_at_k)
       ) as dominated_by
from vecdiag.param_measure a;

comment on view vecdiag.param_pareto is
  'on_frontier=false 的配置"更慢且召回不更高"，属于白花时间，dominated_by 列写明是谁支配了它。'
  '注意 IVFFlat 与 HNSW 不在同一前沿上比较：两者的召回是在各自的查询侧参数下测的。';

-- ---------------------------------------------------------------------------
-- 5) 建议入口：HNSW
--
-- 给定召回目标，返回**前沿上满足目标且构建最快**的配置。
-- 拿不到实测覆盖时不猜：明确返回 applicable=false，并说明该跑哪个脚本。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.hnsw_param_advice(
    p_target_recall numeric,
    p_rows          bigint default null,
    p_dims          int    default 128,
    p_mwm_mb        int    default null
) returns table (
    applicable       boolean,
    m                int,
    ef_construction  int,
    expected_recall  numeric,
    build_median_ms  bigint,
    index_mb         numeric,
    graph_mb_pred    numeric,
    will_spill       boolean,
    calib_rows       bigint,
    note             text
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with cand as (
    select p.* from vecdiag.param_pareto p
    where p.am = 'hnsw' and p.on_frontier
      and p.recall_at_k >= p_target_recall
      and (p_dims is null or p.dims = p_dims)
    order by p.build_median_ms
    limit 1
  ),
  best as (   -- 目标高于已标定的最好召回时，给出能给的最好那个并说明差距
    select p.* from vecdiag.param_pareto p
    where p.am = 'hnsw' and p.on_frontier and (p_dims is null or p.dims = p_dims)
    order by p.recall_at_k desc, p.build_median_ms
    limit 1
  ),
  s as (
    select c.*, sp.will_spill, sp.estimated_graph_mb
    from cand c
    left join lateral vecdiag.hnsw_predict_spill(
        coalesce(p_rows, c.rows), c.dims, c.m,
        case when p_mwm_mb is null then null else p_mwm_mb::bigint * 1024 end) sp on true
  )
  select true, s.m, s.ef_construction, s.recall_at_k, s.build_median_ms, s.index_mb,
         s.estimated_graph_mb, s.will_spill, s.rows,
         format('目标召回 %s，取前沿上构建最快的配置 m=%s/ef_construction=%s。'
                '构建耗时 %s ms 是在 %s 行×%s 维 %s 上标定的%s；行数不同要按数据量重新标定，'
                '不要线性外推。%s',
                p_target_recall, s.m, s.ef_construction, s.build_median_ms, s.rows, s.dims, s.dataset,
                case when p_rows is not null and p_rows <> s.rows
                     then format('（你问的是 %s 行，与标定点不同）', p_rows) else '' end,
                case when s.will_spill
                     then format('⚠ 按 %s 行预测图内存 %s MB 放不下 maintenance_work_mem，会降级落盘，'
                                 '构建耗时会明显高于上面的标定值——先按 M2 的建议提高内存',
                                 coalesce(p_rows, s.rows), s.estimated_graph_mb)
                     else '按当前 maintenance_work_mem 预测不降级' end)
  from s
  union all
  select false, b.m, b.ef_construction, b.recall_at_k, b.build_median_ms, b.index_mb,
         null, null, b.rows,
         format('已标定范围内达不到召回 %s：前沿上最好的是 m=%s/ef_construction=%s，召回 %s。'
                '要么放低目标，要么先扩标定网格——跑 '
                'M_LIST="16 32 64" EF_LIST="64 200 400" bash tools/param_sweep.sh，'
                '不要拿外插的参数当建议。',
                p_target_recall, b.m, b.ef_construction, b.recall_at_k)
  from best b where not exists (select 1 from cand)
  union all
  select false, null, null, null, null, null, null, null, null,
         '没有任何 HNSW 实测标定。先跑 bash tools/param_sweep.sh，再 bash tools/load_param_facts.sh 灌入。'
  where not exists (select 1 from vecdiag.param_measure where am = 'hnsw');
$$;

comment on function vecdiag.hnsw_param_advice(numeric, bigint, int, int) is
  '按召回目标给 m/ef_construction。目标超出标定范围时返回 applicable=false 并说明要跑什么，'
  '不外插、不猜。耗时数字只在标定的行数/维度/数据集上成立。';

-- ---------------------------------------------------------------------------
-- 6) 建议入口：IVFFlat
--
-- lists 用上游经验式起步，再用 M1 检查这个 lists 在当前内存下**建得起来吗**——
-- 这一步是上游没有的：README 给 lists 的取值建议，但不检查
-- maintenance_work_mem 够不够，实际上 lists 大到一定程度 CREATE INDEX 直接报错。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.ivfflat_param_advice(
    p_rows   bigint,
    p_dims   int,
    p_mwm_kb bigint default null
) returns table (
    lists_suggested   int,
    rule_used         text,
    feasible          boolean,
    first_hit         text,
    need_mwm_mb       int,
    probes_suggested  int,
    note              text
)
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  with r as (
    select case when p_rows <= 1000000
                then greatest(1, (p_rows / 1000)::int)
                else greatest(1, floor(sqrt(p_rows))::int) end as lists,
           case when p_rows <= 1000000 then 'rows/1000（README.md:341，≤100 万行）'
                else 'sqrt(rows)（README.md:341，>100 万行）' end as rule
  ),
  chk as (
    -- ivfflat_predict 的第 4 个位置参数是 p_relpages，不是内存；这里必须用命名实参，
    -- 否则会把内存值当页数传进去，模型静默给出错的结论。
    select r.*, p.first_hit, p.predicted_mb
    from r
    left join lateral vecdiag.ivfflat_predict(
                p_rows  := p_rows,
                p_dims  := p_dims,
                p_lists := r.lists,
                p_mwm_kb := coalesce(p_mwm_kb, vecdiag.current_mwm_kb())) p on true
  )
  select c.lists, c.rule,
         c.first_hit = 'none'::vecdiag.checkpoint_kind,
         c.first_hit::text,
         c.predicted_mb,
         greatest(1, floor(sqrt(c.lists))::int),
         format('lists=%s 来自上游经验式；probes 起点 sqrt(lists)=%s（README.md:342）。'
                '%s 本机实测：lists 从 100 加到 1000（10 倍）而 probes 固定 10 时，'
                'recall@10 从 0.9860 掉到 0.8583、单查询从 6.90 ms 降到 0.76 ms、'
                '构建从 1147 ms 涨到 14311 ms（100k×128 SIFT 子集，run t27-20260827）——'
                '**lists 调大必须同步调大 probes，否则只是把召回换成了速度**。',
                c.lists, greatest(1, floor(sqrt(c.lists))::int),
                case when c.first_hit = 'none'::vecdiag.checkpoint_kind
                     then '当前内存下建得起来。'
                     else format('⚠ 当前 maintenance_work_mem 建不起来：最先超限的检查点是 %s，'
                                 '报错会写"%s MB"，把 maintenance_work_mem 提到该值以上即可。'
                                 '要么提内存，要么减小 lists。',
                                 c.first_hit, c.predicted_mb) end)
  from chk c;
$$;

comment on function vecdiag.ivfflat_param_advice(bigint, int, bigint) is
  '把上游的 lists 经验式与 M1 的可行性检查串起来：上游告诉你取多少，M1 告诉你建不建得起来。';

-- ---------------------------------------------------------------------------
-- 7) 溯源视图：把建议里用到的每个数字连同来源类型一起列出来
--    评审直接看这一张表就能判断"哪些是实测、哪些是引用、哪些是源码事实"
-- ---------------------------------------------------------------------------
create or replace view vecdiag.param_advice_provenance as
select 'source-code'::text as source_kind,
       format('%s.%s 默认 %s，范围 [%s, %s]', am, knob,
              coalesce(default_value::text, '—'), min_value, max_value) as fact,
       source_ref
from vecdiag.param_limit
union all
select source_kind, format('%s.%s %s → %s', am, knob, direction, effect), source_ref
from vecdiag.param_guidance
union all
select 'measured', format('%s %s：构建中位 %s ms，索引 %s MB，recall@%s %s，单查询 %s ms',
              am,
              case when am = 'hnsw' then format('m=%s/ef_construction=%s', m, ef_construction)
                   else format('lists=%s', lists) end,
              build_median_ms, index_mb, topk, recall_at_k, query_ms_mean),
       format('%s 行×%s 维 %s，mwm=%s，%s，重复 %s 次，run %s',
              rows, dims, dataset, mwm, query_knob, n_repeats, run_id)
from vecdiag.param_measure;

comment on view vecdiag.param_advice_provenance is
  '参数建议表的溯源清单：source_kind 三值 source-code / upstream-doc / measured，一行一个事实。';
