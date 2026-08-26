#!/usr/bin/env bash
# M3 · 阶段耗时测量（同时满足门禁 K5：每配置重复 ≥3 次，报 min/median/max）
#
# 做三件事：
#   1) 每个配置重复 N 次真实建索引，记录总耗时 → min/median/max；
#   2) 每次构建都并行采样 pg_stat_progress_create_index → 阶段耗时分解；
#   3) 测采样开销：同一配置在"有采样/无采样"下各跑 N 次对比（K5 要求把开销写进报告）。
#
# warm-up：每个配置正式计时前先跑一次并丢弃，避免首次的缓存与文件分配影响。
#
# 用法（以 postgres 身份）：bash tools/measure_build_time.sh [run_id] [重复次数]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
RUN_ID=${1:-m3-$(date +%Y%m%d-%H%M%S)}
REPEATS=${2:-3}
OUT=${OUTDIR:-/data/artifacts/$RUN_ID}
SAMPLER=$(dirname "$0")/progress_sampler.sh
mkdir -p "$OUT/samples"
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=0"
RAW="$OUT/build_times.csv"
echo "config,am,rows,dims,param,sampled,iter,elapsed_ms" > "$RAW"

mk() {
  local tbl=$1 rows=$2 dims=$3
  $PSQL -Atc "select count(*) from pg_class where relname='$tbl';" | grep -q '^1$' && return 0
  $PSQL -c "create table $tbl (id int, v vector($dims));" >/dev/null 2>&1
  $PSQL -c "insert into $tbl select i, (select array_agg(random())::vector($dims)
              from generate_series(1,$dims)) from generate_series(1,$rows) i;" >/dev/null 2>&1
  $PSQL -c "analyze $tbl;" >/dev/null 2>&1
}

build_once() {
  local tbl=$1 am=$2 param=$3 csv=$4
  local sp=""
  [ -n "$csv" ] && sp=$(bash "$SAMPLER" start "$csv" 200)
  local t0 t1
  t0=$(date +%s%3N)
  $PSQL -c "set maintenance_work_mem='256MB';
            drop index if exists ${tbl}_ix;
            create index ${tbl}_ix on $tbl using $am (v vector_l2_ops) with ($param);" >/dev/null 2>&1
  t1=$(date +%s%3N)
  [ -n "$sp" ] && bash "$SAMPLER" stop "$sp"
  $PSQL -c "drop index if exists ${tbl}_ix;" >/dev/null 2>&1
  echo $((t1 - t0))
}

# 一个配置：先 warm-up 一次丢弃，再重复 REPEATS 次
run_config() {
  local cfg=$1 tbl=$2 am=$3 rows=$4 dims=$5 param=$6 sampled=$7
  echo ">>> $cfg（$am, ${rows}行×${dims}维, $param, 采样=$sampled）"
  build_once "$tbl" "$am" "$param" "" >/dev/null      # warm-up，结果丢弃
  for i in $(seq 1 "$REPEATS"); do
    local csv=""
    [ "$sampled" = "yes" ] && csv="$OUT/samples/${cfg}_${i}.csv"
    local ms
    ms=$(build_once "$tbl" "$am" "$param" "$csv")
    echo "$cfg,$am,$rows,$dims,$param,$sampled,$i,$ms" >> "$RAW"
    printf '    第 %s 次：%s ms\n' "$i" "$ms"
  done
}

echo "重复次数 = $REPEATS（另有 1 次 warm-up 不计入）"

mk m3_s 3000  128
mk m3_m 12000 128
mk m3_l 20000 256

# IVFFlat 三档规模，带采样
run_config ivf_S m3_s ivfflat 3000  128 "lists=100" yes
run_config ivf_M m3_m ivfflat 12000 128 "lists=200" yes
run_config ivf_L m3_l ivfflat 20000 256 "lists=200" yes
# HNSW 一档（含降级场景另见 M2）
run_config hnsw_M m3_m hnsw 12000 128 "m=16, ef_construction=64" yes
# 采样开销对照：同配置不采样
run_config ivf_M_nosample m3_m ivfflat 12000 128 "lists=200" no

echo
echo ">>> 统计（min / median / max，单位 ms）"
awk -F, 'NR>1 {a[$1]=a[$1]" "$8} END {for (c in a) print c, a[c]}' "$RAW" |
while read -r cfg rest; do
  echo "$rest" | tr ' ' '\n' | grep -E '^[0-9]+$' | sort -n > /tmp/_m3v
  n=$(wc -l < /tmp/_m3v)
  mn=$(head -1 /tmp/_m3v); mx=$(tail -1 /tmp/_m3v)
  md=$(awk -v n="$n" 'NR==int((n+1)/2){print}' /tmp/_m3v)
  printf '  %-16s n=%s  min=%s  median=%s  max=%s\n' "$cfg" "$n" "$mn" "$md" "$mx"
  echo "$cfg,$n,$mn,$md,$mx" >> "$OUT/build_time_stats.csv"
done
sed -i '1i config,n,min_ms,median_ms,max_ms' "$OUT/build_time_stats.csv" 2>/dev/null || true

echo
echo ">>> 采样开销（ivf_M 有采样 vs 无采样的中位数之差）"
awk -F, 'NR>1 && $1=="ivf_M" {s[NR]=$8} END{}' "$RAW" >/dev/null
med() { grep "^$1," "$OUT/build_time_stats.csv" | cut -d, -f4; }
a=$(med ivf_M); b=$(med ivf_M_nosample)
if [ -n "$a" ] && [ -n "$b" ] && [ "$b" -gt 0 ]; then
  awk -v a="$a" -v b="$b" 'BEGIN{printf "  有采样 %s ms / 无采样 %s ms → 开销 %+.2f%%\n", a, b, (a-b)/b*100}'
fi

( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
echo ">>> 原始数据 -> $RAW；采样序列 -> $OUT/samples/"
$PSQL -c "drop table if exists m3_s; drop table if exists m3_m; drop table if exists m3_l;" >/dev/null 2>&1

