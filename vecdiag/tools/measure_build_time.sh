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

# DATASET=synthetic（默认）→ 随机向量；DATASET=sift → 从公开数据集 sift_base 取子集。
# 构建耗时与数据分布有关，所以两种数据的权重要分别测、并列报告，不互相覆盖。
DATASET=${DATASET:-synthetic}
SRC_TABLE=${SRC_TABLE:-sift_base}

mk() {
  local tbl=$1 rows=$2 dims=$3
  $PSQL -Atc "select count(*) from pg_class where relname='$tbl';" | grep -q '^1$' && return 0
  if [ "$DATASET" = "sift" ]; then
    [ "$dims" = "128" ] || { echo "[FAIL] SIFT1M 只有 128 维，配置要求 $dims 维" >&2; return 1; }
    $PSQL -c "create table $tbl as
                select id, v from $SRC_TABLE order by id limit $rows;" >/dev/null 2>&1 || return 1
  else
    $PSQL -c "create table $tbl (id int, v vector($dims));" >/dev/null 2>&1 || return 1
    $PSQL -c "insert into $tbl select i, (select array_agg(random())::vector($dims)
                from generate_series(1,$dims)) from generate_series(1,$rows) i;" >/dev/null 2>&1 || return 1
  fi
  $PSQL -c "analyze $tbl;" >/dev/null 2>&1
}

build_once() {
  local tbl=$1 am=$2 param=$3 csv=$4
  local sp=""
  [ -n "$csv" ] && sp=$(bash "$SAMPLER" start "$csv" "${INTERVAL_MS:-50}")
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
    # param 里可能有逗号，写 CSV 前换成分号，否则列会串位
    echo "$cfg,$am,$rows,$dims,${param//,/;},$sampled,$i,$ms" >> "$RAW"
    printf '    第 %s 次：%s ms\n' "$i" "$ms"
  done
}

echo "重复次数 = $REPEATS（另有 1 次 warm-up 不计入）"

if [ "$DATASET" = "sift" ]; then
  # 公开数据集只有 128 维，用行数分档：S 10 万 / M 30 万 / L 100 万
  mk m3_s 100000  128 || exit 1
  mk m3_m 300000  128 || exit 1
  mk m3_l 1000000 128 || exit 1
else
  mk m3_s 20000 128
  mk m3_m 60000 128
  mk m3_l 60000 256
fi

# IVFFlat 三档规模，带采样
if [ "$DATASET" = "sift" ]; then
  run_config ivf_S m3_s ivfflat 100000  128 "lists=100" yes
  run_config ivf_M m3_m ivfflat 300000  128 "lists=150" yes
  run_config ivf_L m3_l ivfflat 1000000 128 "lists=200" yes
else
  run_config ivf_S m3_s ivfflat 20000 128 "lists=100" yes
  run_config ivf_M m3_m ivfflat 60000 128 "lists=300" yes
  run_config ivf_L m3_l ivfflat 60000 256 "lists=300" yes
fi
# HNSW 一档（含降级场景另见 M2）
run_config hnsw_M m3_s hnsw 60000 128 "m=16, ef_construction=64" yes
# 采样开销对照：**交替**跑，抵消时间漂移
echo ">>> 采样开销对照（ivf_M，采样/不采样交替各 4 次）"
build_once m3_m ivfflat "lists=300" "" >/dev/null      # warm-up
for i in 1 2 3 4; do
  a=$(build_once m3_m ivfflat "lists=300" "$OUT/samples/ab_on_${i}.csv")
  b=$(build_once m3_m ivfflat "lists=300" "")
  echo "ab_on,ivfflat,60000,128,lists=300,yes,$i,$a" >> "$RAW"
  echo "ab_off,ivfflat,60000,128,lists=300,no,$i,$b" >> "$RAW"
  printf '    第 %s 轮：有采样 %s ms / 无采样 %s ms\n' "$i" "$a" "$b"
done

echo
echo ">>> 统计（min / median / max，单位 ms）"
awk -F, 'NR>1 {a[$1]=a[$1]" "$NF} END {for (c in a) print c, a[c]}' "$RAW" |
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
echo ">>> 采样开销（交替组 ab_on vs ab_off 的中位数之差）"
awk -F, 'NR>1 && $1=="ivf_M" {s[NR]=$8} END{}' "$RAW" >/dev/null
med() { grep "^$1," "$OUT/build_time_stats.csv" | cut -d, -f4; }
a=$(med ab_on); b=$(med ab_off)
if [ -n "$a" ] && [ -n "$b" ] && [ "$b" -gt 0 ]; then
  awk -v a="$a" -v b="$b" 'BEGIN{printf "  有采样 %s ms / 无采样 %s ms → 开销 %+.2f%%\n", a, b, (a-b)/b*100}'
fi

( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
echo ">>> 原始数据 -> $RAW；采样序列 -> $OUT/samples/"
$PSQL -c "drop table if exists m3_s; drop table if exists m3_m; drop table if exists m3_l;" >/dev/null 2>&1

