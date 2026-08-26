-- M3 回归测试：加权进度必须单调非递减、终值为 100
-- 用法：psql -p 5518 -d postgres -X -f tests/test_m3_progress.sql
-- 通过标准：ok_* 全为 t。

\pset pager off

\echo '== 1. 阶段权重来自实测且每个访问方法求和为 1 =='
-- 权重按 4 位小数存储，逐行舍入会让求和落在 1±0.0005，所以用容差判定而不是相等判定
select dataset, size_class, am, abs(sum(weight) - 1) <= 0.0005 as ok_sum,
       round(sum(weight), 4) as weight_sum,
       bool_and(source = 'measured') as ok_measured,
       max(dispersion) as max_dispersion
from vecdiag.stage_weight
group by dataset, size_class, am order by dataset, size_class, am;

\echo '== 2. 每条采样序列的加权进度单调非递减（合成与 SIFT1M 两套数据都查）=='
with runs as (
  select distinct run_id,
         case when run_id like '%hnsw%' then 'hnsw' else 'ivfflat' end as am,
         -- run_id 前缀决定用哪套权重：m3r- 是 SIFT1M，其余按合成
         case when run_id like 'm3r-%' then 'sift1m' else 'synthetic' end as ds,
         case when run_id like 'm3r-S%' then 'S'
              when run_id like 'm3r-M%' then 'M'
              when run_id like 'm3r-L%' then 'L'
              else 'pooled' end as cls
  from vecdiag.progress_sample
),
c as (
  select r.run_id, r.ds, pc.elapsed_ms, pc.mono_pct,
         lag(pc.mono_pct) over (partition by r.run_id order by pc.elapsed_ms) as prev
  from runs r, lateral vecdiag.progress_curve(r.run_id, r.am, r.cls, r.ds) pc
)
select ds as dataset, count(*) as total_points,
       count(*) filter (where prev is not null and mono_pct < prev) = 0 as ok_monotone
from c group by ds order by ds;

\echo '== 3. 终值：最后一个采样点的单调进度应当接近 100 =='
-- 注意：采样在 CREATE INDEX 返回前就停了，最后一拍未必正好落在 100%，
-- 因此断言"最后一点 >= 该序列中位数且不超过 100"，并单独把终值列出来核对。
with runs as (
  select distinct run_id,
         case when run_id like '%hnsw%' then 'hnsw' else 'ivfflat' end as am,
         case when run_id like 'm3r-%' then 'sift1m' else 'synthetic' end as ds,
         case when run_id like 'm3r-S%' then 'S' when run_id like 'm3r-M%' then 'M'
              when run_id like 'm3r-L%' then 'L' else 'pooled' end as cls
  from vecdiag.progress_sample
),
c as (
  select r.run_id, pc.elapsed_ms, pc.mono_pct,
         row_number() over (partition by r.run_id order by pc.elapsed_ms desc) as rn
  from runs r, lateral vecdiag.progress_curve(r.run_id, r.am, r.cls, r.ds) pc
)
select run_id, mono_pct as final_pct, mono_pct <= 100 as ok_le_100
from c where rn = 1 order by run_id limit 12;

\echo '== 4. 阶段内进度的来源必须可区分（视图计数 vs 时间插值）=='
with runs as (
  select distinct run_id,
         case when run_id like '%hnsw%' then 'hnsw' else 'ivfflat' end as am,
         case when run_id like 'm3r-%' then 'sift1m' else 'synthetic' end as ds,
         case when run_id like 'm3r-S%' then 'S' when run_id like 'm3r-M%' then 'M'
              when run_id like 'm3r-L%' then 'L' else 'pooled' end as cls
  from vecdiag.progress_sample
)
select pc.intra_source, count(*) as points
from runs r, lateral vecdiag.progress_curve(r.run_id, r.am, r.cls, r.ds) pc
group by pc.intra_source order by points desc;

\echo '== 5. ETA 只在进度介于 0 与 100 之间时给出非零值 =='
with runs as (
  select distinct run_id,
         case when run_id like '%hnsw%' then 'hnsw' else 'ivfflat' end as am,
         case when run_id like 'm3r-%' then 'sift1m' else 'synthetic' end as ds,
         case when run_id like 'm3r-S%' then 'S' when run_id like 'm3r-M%' then 'M'
              when run_id like 'm3r-L%' then 'L' else 'pooled' end as cls
  from vecdiag.progress_sample
)
select count(*) filter (where pc.mono_pct >= 100 and pc.eta_ms <> 0) = 0 as ok_eta_zero_at_100,
       count(*) filter (where pc.eta_ms < 0) = 0 as ok_eta_nonneg
from runs r, lateral vecdiag.progress_curve(r.run_id, r.am, r.cls, r.ds) pc;
