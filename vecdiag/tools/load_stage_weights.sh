#!/usr/bin/env bash
# M3 · 从采样序列算阶段权重并写入 vecdiag.stage_weight
#
# 权重定义：某阶段的采样点数占该次构建总采样点数的比例。
# 用采样点数而不是时间戳差值，是因为采样间隔固定，点数比例就是时间比例，
# 且不会被单次抖动放大。量化误差 = 一个采样间隔 / 总构建耗时，会一并记录。
#
# 用法（以 postgres 身份）：bash tools/load_stage_weights.sh <采样目录> <run_id>
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
SAMPLES=${1:?用法: load_stage_weights.sh <采样目录> <run_id>}
RUN_ID=${2:?缺 run_id}
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q"

# 1) 把每个采样 CSV 灌进 vecdiag.progress_sample（run_id 用文件名，便于逐次复算）
$PSQL -c "delete from vecdiag.progress_sample where run_id like '${RUN_ID}%';" >/dev/null
for f in "$SAMPLES"/*.csv; do
  base=$(basename "$f" .csv)
  # 跳过注释行与表头；空 phase 的行不要
  awk -F, 'NR>2 && NF>=9 && $3 != "" {print}' "$f" > /tmp/_ps.csv
  [ -s /tmp/_ps.csv ] || continue
  $PSQL -c "create temp table _stage_in (elapsed_ms bigint, pid int, phase text,
              blocks_total bigint, blocks_done bigint, tuples_total bigint,
              tuples_done bigint, relid oid, index_relid oid);
            -- 用服务端 COPY：\copy 是 psql 元命令，不能出现在 -c 的字符串里。
            -- 本实例的 postgres 是超级用户且文件就在同一台机器上，服务端 COPY 可用。
            copy _stage_in from '/tmp/_ps.csv' with (format csv);
            insert into vecdiag.progress_sample
              select '${RUN_ID}/${base}', elapsed_ms, pid, phase, blocks_total,
                     blocks_done, tuples_total, tuples_done, relid, index_relid
              from _stage_in;" >/dev/null
done
echo "已载入采样点：$($PSQL -Atc "select count(*) from vecdiag.progress_sample where run_id like '${RUN_ID}%';")"

# 2) 算权重：同一访问方法的多次重复取均值，并记极差作为离散度
$PSQL <<SQL
\set ON_ERROR_STOP on
with per_run as (
  select run_id,
         case when run_id like '%hnsw%' then 'hnsw' else 'ivfflat' end as am,
         phase,
         count(*)::numeric / sum(count(*)) over (partition by run_id) as share
  from vecdiag.progress_sample
  where run_id like '${RUN_ID}%'
  group by run_id, phase
),
agg as (
  select am, phase, avg(share) as w, count(*) as n,
         max(share) - min(share) as disp
  from per_run group by am, phase
),
-- 各次重复覆盖的阶段可能不同（短构建可能整段没采到 loading tuples），
-- 逐阶段取均值不保证求和为 1，必须归一化，否则加权百分比的终值不是 100。
norm as (
  select am, phase, w / sum(w) over (partition by am) as w, n, disp from agg
)
insert into vecdiag.stage_weight (am, phase, weight, n_samples, dispersion, source, run_id)
select am, phase, round(w, 4), n, round(disp, 4), 'measured', '${RUN_ID}'
from norm
on conflict (am, phase) do update
  set weight = excluded.weight, n_samples = excluded.n_samples,
      dispersion = excluded.dispersion, run_id = excluded.run_id,
      measured_at = now();

\echo '== 阶段权重（实测）=='
select am, phase, weight, n_samples as 重复次数, dispersion as 极差
from vecdiag.stage_weight order by am, weight desc;

\echo '== 每个访问方法的权重之和（应为 1）=='
select am, sum(weight) as total from vecdiag.stage_weight group by am;

\echo '== 逐次重复的阶段占比（用于核对上面的极差，可独立复算）=='
select run_id, phase, round(count(*)::numeric / sum(count(*)) over (partition by run_id), 4) as share
from vecdiag.progress_sample
where run_id like '${RUN_ID}%'
group by run_id, phase
order by run_id, share desc;
SQL
