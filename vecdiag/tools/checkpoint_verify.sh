#!/usr/bin/env bash
# T1.3 预演 · 三检查点端到端验证（C1 / C2 / C3 各打一次，并逐字比对报错数字）
# 目的：证明 M1 设计"变化四/变化五"的三段式模型能精确复现报错文本。
# 常数来源：MAXALIGN(itemsize)=520（03_abi_probe.sh 实测），H=sizeof(VectorArrayData)=24（源码），
#           MaxHeapTuplesPerPage=291（htup_details.h:629-631，8kB 块）。
# 用法（以 postgres 身份）：bash 04_checkpoint_verify.sh [run_id]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
ROWS=${ROWS:-2000}
DIMS=${DIMS:-128}
LISTS=${LISTS:-1000}
A=520          # MAXALIGN(itemsize) for vector(128)，实测
H=24           # sizeof(VectorArrayData)，源码 ivfflat.h:120-126
T=291          # MaxHeapTuplesPerPage @ 8kB
RUN_ID=${1:-ckpt-$(date +%Y%m%d-%H%M%S)}
OUT=/data/artifacts/$RUN_ID
mkdir -p "$OUT"
PSQL="$PGHOME/bin/psql -p $PGPORT -d postgres -X -q -v ON_ERROR_STOP=0"

echo ">>> 建表 $ROWS 行 × ${DIMS}维，lists=$LISTS"
$PSQL -c "drop table if exists ckpt_t;" >/dev/null 2>&1
$PSQL -c "create table ckpt_t (id int, v vector($DIMS));" >/dev/null
$PSQL -c "insert into ckpt_t select i, (select array_agg(random())::vector($DIMS)
            from generate_series(1,$DIMS)) from generate_series(1,$ROWS) i;" >/dev/null
$PSQL -c "analyze ckpt_t;" >/dev/null
RELPAGES=$($PSQL -Atc "select relpages from pg_class where relname='ckpt_t';")
echo "relpages = $RELPAGES"

MAXTUPLES=$(( RELPAGES * T ))
NUMSAMPLES=$(( LISTS * 50 )); [ $NUMSAMPLES -lt 10000 ] && NUMSAMPLES=10000
[ $NUMSAMPLES -gt $MAXTUPLES ] && NUMSAMPLES=$MAXTUPLES
[ $NUMSAMPLES -lt 1 ] && NUMSAMPLES=1
SAMPLED=$(( NUMSAMPLES < ROWS ? NUMSAMPLES : ROWS ))

C1=$(( H + LISTS * A ))
C2=$(( C1 + H + NUMSAMPLES * A ))
K9=$(( (H + LISTS*A) + 4*LISTS*DIMS + 4*LISTS + 4*SAMPLED + 4*SAMPLED*LISTS + 4*SAMPLED + 4*LISTS + 4*LISTS*LISTS + 4*LISTS ))
C3=$(( C2 + K9 ))
mb() { echo $(( $1 / 1048576 + 1 )); }
kb() { echo $(( $1 / 1024 )); }

{
echo "maxTuples   = relpages*T = $RELPAGES*$T = $MAXTUPLES"
echo "numSamples  = $NUMSAMPLES   （C2 用这个上限值）"
echo "sampled     = $SAMPLED      （C3 用实采条数）"
echo "C1 = $C1 B = $(kb $C1) kB → 报错应为 $(mb $C1) MB"
echo "C2 = $C2 B = $(kb $C2) kB → 报错应为 $(mb $C2) MB"
echo "C3 = $C3 B = $(kb $C3) kB → 报错应为 $(mb $C3) MB"
} | tee "$OUT/prediction.txt"

run_case() {
  local label=$1 mwm=$2 expect=$3
  local err
  err=$($PSQL -c "set maintenance_work_mem='${mwm}';
                  create index ckpt_i on ckpt_t using ivfflat (v vector_l2_ops) with (lists=$LISTS);" 2>&1)
  echo "$err" > "$OUT/case_${label}.err"
  $PSQL -c "drop index if exists ckpt_i;" >/dev/null 2>&1
  local got
  got=$(echo "$err" | grep -oE 'memory required is [0-9]+ MB' | grep -oE '[0-9]+' | head -1)
  if [ -z "$got" ]; then got="无报错(构建成功)"; fi
  if [ "$got" = "$expect" ]; then
    printf '  [PASS] %-28s mwm=%-8s 预测=%-22s 实际=%s\n' "$label" "$mwm" "$expect" "$got"
  else
    printf '  [FAIL] %-28s mwm=%-8s 预测=%-22s 实际=%s\n' "$label" "$mwm" "$expect" "$got"
  fi
}

echo
echo ">>> 逐个检查点验证（预测值来自上面的公式，未做任何回填）"
run_case "C1先触发"      "$(( $(kb $C1) - 100 ))kB" "$(mb $C1)"
run_case "C2先触发"      "1MB"                      "$(mb $C2)"
run_case "C3先触发(验9项)" "$(( ($(kb $C2) + $(kb $C3)) / 2 ))kB" "$(mb $C3)"
run_case "全部通过"      "$(( $(kb $C3) + 4096 ))kB" "无报错(构建成功)"

echo
echo "原始 stderr 与预测已存档 -> $OUT/"
( cd "$OUT" && sha256sum ./* > SHA256SUMS 2>/dev/null; echo "SHA256SUMS 已生成" )
$PSQL -c "drop table if exists ckpt_t;" >/dev/null 2>&1
