#!/usr/bin/env bash
# T2.7 · 把 param_sweep.sh 的原始 CSV 灌进参数建议表
#
# 只接受**原始**逐次 CSV（param_sweep.csv），不接受已经统计过的 stats CSV：
# min/median/max 必须在库内从原始数据算，报告里引用的中位数才能被重算复核。
#
# 用法（以 postgres 身份）：
#   bash tools/load_param_facts.sh /data/artifacts/t27-20260827/param_sweep.csv [dataset] [topk]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
CSV=${1:?用法: load_param_facts.sh <param_sweep.csv> [dataset] [topk]}
DATASET=${2:-sift1m-subset}
TOPK=${3:-10}
RUN_ID=$(basename "$(dirname "$CSV")")
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=1"

case "$TOPK" in ''|*[!0-9]*) echo "[FAIL] topk 必须是整数" >&2; exit 2;; esac
[ -r "$CSV" ] || { echo "[FAIL] 读不到 $CSV" >&2; exit 2; }

# COPY 走服务端读文件（超级用户、同机），所以路径必须是服务器上的绝对路径
$PSQL <<SQL
create temp table stg (
  am text, m int, ef_construction int, lists int, rows bigint, dims int,
  mwm text, iter int, build_ms bigint, index_mb numeric, spilled text,
  recall_at_k numeric, query_ms_mean numeric
);
copy stg from '$CSV' with (format csv, header true, null '');

-- 同一 run 重灌时先清掉，避免"两次导入各三次重复"混成六次
delete from vecdiag.param_measure where run_id = '$RUN_ID';

insert into vecdiag.param_measure
  (am, m, ef_construction, lists, rows, dims, dataset, mwm, n_repeats,
   build_min_ms, build_median_ms, build_max_ms, index_mb, spilled,
   topk, query_knob, recall_at_k, query_ms_mean, run_id)
select am, m, ef_construction, lists, rows, dims, '$DATASET', mwm, count(*)::int,
       min(build_ms), 
       -- 中位数用 percentile_disc：取实际观测到的某一次，不造一个没跑过的数
       percentile_disc(0.5) within group (order by build_ms)::bigint,
       max(build_ms),
       max(index_mb),
       bool_or(spilled = 'yes'),
       $TOPK,
       case when am = 'hnsw' then 'hnsw.ef_search=40' else 'ivfflat.probes=10' end,
       round(avg(recall_at_k), 4),
       round(avg(query_ms_mean), 2),
       '$RUN_ID'
from stg
group by am, m, ef_construction, lists, rows, dims, mwm;

select am,
       case when am = 'hnsw' then format('m=%s/ef=%s', m, ef_construction)
            else format('lists=%s', lists) end as config,
       n_repeats, build_median_ms, index_mb, recall_at_k, query_ms_mean
from vecdiag.param_measure where run_id = '$RUN_ID'
order by am, build_median_ms;
SQL

echo
echo ">>> 帕累托前沿（on_frontier=false 表示更慢且召回不更高，属于白花时间）"
$PSQL -c "select am,
                 case when am = 'hnsw' then format('m=%s/ef=%s', m, ef_construction)
                      else format('lists=%s', lists) end as config,
                 build_median_ms, recall_at_k, on_frontier, dominated_by
            from vecdiag.param_pareto
           where run_id = '$RUN_ID'
           order by am, build_median_ms;"
