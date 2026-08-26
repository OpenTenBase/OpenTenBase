#!/usr/bin/env bash
# M2 · 外样本验证 + 建议有效性（E 组）
#
# 三件事：
#   1) 用标定好的模型去预测**未参与标定**的配置的降级点，再实建索引比对；
#   2) 验证"按建议内存重建后 NOTICE 消失"——这是"建议管用"的唯一直接证据；
#   3) 反向验证：把内存设到建议值以下，NOTICE 应当重新出现（否则上一条没有说服力）。
#
# 用法（以 postgres 身份）：bash tools/hnsw_validate.sh [run_id]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
RUN_ID=${1:-m2v-$(date +%Y%m%d-%H%M%S)}
OUT=${OUTDIR:-/data/artifacts/$RUN_ID}
mkdir -p "$OUT/stderr"
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=0"
CSV="$OUT/hnsw_validate.csv"
echo "case_id,kind,rows,dims,m,mwm_kb,pred_spill,actual_spill,rel_err_pct,confidence,verdict" > "$CSV"

mk() {
  local tbl=$1 rows=$2 dims=$3
  $PSQL -Atc "select count(*) from pg_class where relname='$tbl';" | grep -q '^1$' && return 0
  $PSQL -c "create table $tbl (id int, v vector($dims));" >/dev/null 2>&1
  $PSQL -c "insert into $tbl select i, (select array_agg(random())::vector($dims)
              from generate_series(1,$dims)) from generate_series(1,$rows) i;" >/dev/null 2>&1
  $PSQL -c "analyze $tbl;" >/dev/null 2>&1
}

run_case() {
  local case_id=$1 kind=$2 tbl=$3 rows=$4 dims=$5 m=$6 mwm_kb=$7 expect=$8
  local errf="$OUT/stderr/${case_id}.err"

  read -r will pred conf < <($PSQL -F' ' -Atc \
    "select will_spill, coalesce(predicted_spill_tuples::text,'none'), confidence
       from vecdiag.hnsw_predict_spill($rows, $dims, $m, $mwm_kb);")

  $PSQL -c "set client_min_messages = notice;
            set maintenance_work_mem = '${mwm_kb}kB';
            drop index if exists ${tbl}_h;
            create index ${tbl}_h on $tbl using hnsw (v vector_l2_ops) with (m = $m);" \
    > "$errf.out" 2> "$errf"
  local act
  act=$(grep -oE 'after [0-9]+ tuples' "$errf" | grep -oE '[0-9]+' | head -1)
  $PSQL -c "drop index if exists ${tbl}_h;" >/dev/null 2>&1
  [ -z "$act" ] && act=none

  local err_pct=""
  if [ "$pred" != "none" ] && [ "$act" != "none" ]; then
    err_pct=$(awk -v p="$pred" -v a="$act" 'BEGIN{printf "%.2f", (p-a)/a*100}')
  fi

  # expect: spill | nospill —— 判定只看"预警方向对不对"，误差单独报，不混在一起
  local verdict=FAIL
  if [ "$expect" = "spill" ] && [ "$act" != "none" ] && [ "$will" = "t" ]; then verdict=PASS; fi
  if [ "$expect" = "nospill" ] && [ "$act" = "none" ] && [ "$will" = "f" ]; then verdict=PASS; fi

  echo "$case_id,$kind,$rows,$dims,$m,$mwm_kb,$pred,$act,$err_pct,$conf,$verdict" >> "$CSV"
  printf '  [%-4s] %-8s %-22s 预测降级=%-8s 实测=%-8s 误差=%-8s %s\n' \
    "$verdict" "$case_id" "$kind" "$pred" "$act" "${err_pct:-—}" "$conf"
}

echo ">>> E 组前置：建两张未参与标定的表"
mk hnsw_v1 12000 256      # dims=256 从未标定过
mk hnsw_v2 6000  128

echo ">>> 一、外样本预测（dims=256 与 m=24 都不在标定点上）"
run_case V1 out-of-sample hnsw_v1 12000 256 16 6144  spill
run_case V2 out-of-sample hnsw_v1 12000 256 24 10240 spill
run_case V3 out-of-sample hnsw_v2 6000  128 16 4096  spill

echo ">>> 二、建议有效性：按 recommended_mwm_mb 重建，NOTICE 应当消失"
REC=$($PSQL -Atc "select recommended_mwm_mb from vecdiag.hnsw_predict_spill(6000, 128, 16, 4096);")
echo "     模型给 6000 行 × 128 维 × m=16 的内存下限建议：${REC} MB"
run_case E1 advice-works hnsw_v2 6000 128 16 $((REC * 1024)) nospill

echo ">>> 三、反向验证：低于建议值应当重新降级（否则上一条不构成证据）"
run_case E2 advice-tight hnsw_v2 6000 128 16 $(( (REC * 1024) / 2 )) spill

echo
echo ">>> 结果 -> $CSV"
column -t -s, "$CSV"
awk -F, 'NR>1 && $9 != "" {s+=($9<0?-$9:$9); n++} END{ if(n) printf ">>> 降级点预测平均绝对相对误差 %.2f%%（%d 组有可比值）\n", s/n, n }' "$CSV"
( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
$PSQL -c "drop table if exists hnsw_v1; drop table if exists hnsw_v2;" >/dev/null 2>&1
