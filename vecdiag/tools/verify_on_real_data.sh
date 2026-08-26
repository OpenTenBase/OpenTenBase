#!/usr/bin/env bash
# 在**公开数据集**（SIFT1M，1,000,000 × 128）上抽查 M1：预测值必须与报错原文逐字相等。
#
# 与 20 组合成矩阵的区别：这里的表不是脚本生成的随机向量，而是 TEXMEX 的原始基向量，
# 行数、relpages、数据分布都不是我们能挑的。**如果模型只是在拟合自己造的数据，这一步会露馅。**
#
# 用法（以 postgres 身份）：bash tools/verify_on_real_data.sh [run_id]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
TBL=${TBL:-sift_base}
ROWS_EXACT=${ROWS_EXACT:-1000000}
RUN_ID=${1:-real-$(date +%Y%m%d-%H%M%S)}
OUT=${OUTDIR:-/data/artifacts/$RUN_ID}
mkdir -p "$OUT/stderr"
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=0"
CSV="$OUT/real_data_m1.csv"
echo "case_id,lists,target,mwm_kb,first_hit,predicted_mb,actual_mb,verdict" > "$CSV"

$PSQL -c "analyze $TBL;" >/dev/null 2>&1
$PSQL -c "select relpages, reltuples::bigint,
                 vecdiag.ivfflat_num_samples(1000, relpages, false) as numsamples_lists1000
            from pg_class where relname='$TBL';"

one() {
  local case_id=$1 lists=$2 target=$3
  # 先问模型该用多大内存、会报多少
  read -r mwm expect < <($PSQL -F' ' -Atc \
    "select mwm_kb, coalesce(expect_mb::text,'none')
       from vecdiag.ivfflat_mwm_plan($ROWS_EXACT, 128, $lists,
              (select relpages::bigint from pg_class where relname='$TBL'), false)
      where target = '$target';")
  read -r fh pred < <($PSQL -F' ' -Atc \
    "select first_hit, coalesce(predicted_mb::text,'none')
       from vecdiag.ivfflat_predict_table('$TBL'::regclass, $lists, null, $mwm, $ROWS_EXACT);")

  local errf="$OUT/stderr/${case_id}.err"
  $PSQL -c "set maintenance_work_mem='${mwm}kB';
            drop index if exists ${TBL}_ivf_probe;
            create index ${TBL}_ivf_probe on $TBL using ivfflat (v vector_l2_ops) with (lists=$lists);" \
    > "$errf.out" 2> "$errf"
  local act
  act=$(grep -oE 'memory required is [0-9]+ MB' "$errf" | grep -oE '[0-9]+' | head -1)
  [ -z "$act" ] && act=none
  $PSQL -c "drop index if exists ${TBL}_ivf_probe;" >/dev/null 2>&1

  local v=FAIL
  [ "$pred" = "$act" ] && v=PASS
  echo "$case_id,$lists,$target,$mwm,$fh,$pred,$act,$v" >> "$CSV"
  printf '  [%-4s] %-8s lists=%-6s target=%-4s first_hit=%-4s 预测=%-6s 实际=%-6s mwm=%s kB\n' \
    "$v" "$case_id" "$lists" "$target" "$fh" "$pred" "$act" "$mwm"
}

echo ">>> 在 SIFT1M 上逐个检查点抽查（预测全部在建索引之前算完）"
one R1 32768 C1
one R2  8192 C2
one R3   100 C3
one R4  1000 C3

echo
column -t -s, "$CSV"
awk -F, 'NR>1 {n++; if($8=="PASS") p++} END {printf ">>> 真实数据抽查：%d/%d 逐字命中\n", p, n}' "$CSV"
( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
