#!/usr/bin/env bash
# M2 · 标定结果装载：hnsw_calib_sweep.sh 的 CSV → vecdiag.hnsw_calib
#
# 用法（以 postgres 身份）：bash tools/load_hnsw_calib.sh <calib_sweep.csv>
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
CSV=${1:?用法: load_hnsw_calib.sh <calib_sweep.csv>}
[ -r "$CSV" ] || { echo "[FAIL] 读不到 $CSV" >&2; exit 2; }
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=1"

# CSV 第 7 列 run_id 自带引号（防逗号），COPY 时按带引号的 CSV 解析即可
$PSQL <<SQL
begin;
create temp table stg (dims int, m int, ef_construction int, mwm_kb bigint,
                        spill_tuples bigint, per_element numeric, run_id text);
copy stg from '$CSV' with (format csv, header true);
insert into vecdiag.hnsw_calib (dims, m, ef_construction, mwm_kb, spill_tuples, per_element, run_id)
  select dims, m, ef_construction, mwm_kb, spill_tuples, per_element, run_id from stg;
commit;
select run_id, count(*) as rows, count(distinct (dims, m)) as combos,
       round(min(per_element),1) as min_pe, round(max(per_element),1) as max_pe
  from vecdiag.hnsw_calib group by run_id order by max(measured_at);
SQL
