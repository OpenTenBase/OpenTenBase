#!/usr/bin/env bash
# M2 · HNSW 图内存标定与降级点验证
#
# 思路：不需要重编一份 -DHNSW_MEMORY 的 .so。降级 NOTICE 本身就是观测量——
#   hnswbuild.c:530  if (memoryUsed + margin >= memoryTotal)  → 打 NOTICE 并 FlushPages
#   hnswbuild.c:724  memoryTotal = maintenance_work_mem * 1024（串行构建 margin = 0）
# 所以在降级那一刻 memoryUsed ≈ maintenance_work_mem，于是
#   per_element ≈ maintenance_work_mem_bytes / N            （N = NOTICE 里的 tuples 数）
#
# 关键可检验命题：per_element 与 maintenance_work_mem 无关。
# 因此同一 (dims, m) 在不同内存下算出的 per_element 应当一致——这是标定的自洽性检查，
# 也是后面"用一次标定去预测另一档内存的降级点"这个外样本预测的前提。
#
# 用法（以 postgres 身份）：bash tools/hnsw_spill_probe.sh [run_id]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
RUN_ID=${1:-hnsw-$(date +%Y%m%d-%H%M%S)}
OUT=${OUTDIR:-/data/artifacts/$RUN_ID}
mkdir -p "$OUT/stderr"
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=0"
CSV="$OUT/hnsw_spill.csv"

echo "case_id,rows,dims,m,ef_construction,mwm_kb,spill_tuples,per_element_bytes,spilled" > "$CSV"

mk_table() {
  local tbl=$1 rows=$2 dims=$3
  $PSQL -Atc "select count(*) from pg_class where relname='$tbl';" | grep -q '^1$' && return 0
  $PSQL -c "create table $tbl (id int, v vector($dims));" >/dev/null 2>&1 || return 1
  $PSQL -c "insert into $tbl select i, (select array_agg(random())::vector($dims)
              from generate_series(1,$dims)) from generate_series(1,$rows) i;" >/dev/null 2>&1 || return 1
  $PSQL -c "analyze $tbl;" >/dev/null 2>&1
}

probe() {
  local case_id=$1 tbl=$2 rows=$3 dims=$4 m=$5 ef=$6 mwm_kb=$7
  local errf="$OUT/stderr/${case_id}.err"
  $PSQL -c "set client_min_messages = notice;
            set maintenance_work_mem = '${mwm_kb}kB';
            drop index if exists ${tbl}_h;
            create index ${tbl}_h on $tbl using hnsw (v vector_l2_ops)
              with (m = $m, ef_construction = $ef);" > "$errf.out" 2> "$errf"
  local n
  n=$(grep -oE 'after [0-9]+ tuples' "$errf" | grep -oE '[0-9]+' | head -1)
  $PSQL -c "drop index if exists ${tbl}_h;" >/dev/null 2>&1
  if [ -n "$n" ] && [ "$n" -gt 0 ]; then
    local per
    per=$(awk -v k="$mwm_kb" -v n="$n" 'BEGIN{printf "%.1f", k*1024/n}')
    echo "$case_id,$rows,$dims,$m,$ef,$mwm_kb,$n,$per,yes" >> "$CSV"
    printf '  [SPILL] %-10s dims=%-4s m=%-3s mwm=%-7s → 第 %-7s 行降级，per_element ≈ %s B\n' \
      "$case_id" "$dims" "$m" "${mwm_kb}kB" "$n" "$per"
  else
    echo "$case_id,$rows,$dims,$m,$ef,$mwm_kb,,,no" >> "$CSV"
    printf '  [ NO   ] %-10s dims=%-4s m=%-3s mwm=%-7s → 未降级（图放得下）\n' \
      "$case_id" "$dims" "$m" "${mwm_kb}kB"
  fi
}

echo ">>> A 组：固定 (dims=128, m=16)，只改内存 —— 检验 per_element 是否与内存无关"
mk_table hnsw_a 10000 128 || { echo "建表失败" >&2; exit 1; }
probe A1 hnsw_a 10000 128 16 64 4096
probe A2 hnsw_a 10000 128 16 64 8192
probe A3 hnsw_a 10000 128 16 64 16384

echo ">>> B 组：固定内存与维度，只改 m —— 看邻居列表占比"
probe B1 hnsw_a 10000 128 8  64 8192
probe B2 hnsw_a 10000 128 32 64 8192

echo ">>> C 组：换维度 —— 看向量本体占比"
mk_table hnsw_c 8000 384 || { echo "建表失败" >&2; exit 1; }
probe C1 hnsw_c 8000 384 16 64 8192
probe C2 hnsw_c 8000 384 16 64 16384

echo ">>> D 组：ef_construction 变化（预期不影响图内存，只影响构建耗时）"
probe D1 hnsw_a 10000 128 16 200 8192

echo
echo ">>> 自洽性检查：A 组三档内存算出的 per_element 应当一致"
awk -F, 'NR>1 && $1 ~ /^A/ && $8 != "" {s+=$8; n++; if($8>mx||n==1)mx=$8; if($8<mn||n==1)mn=$8}
         END{ if(n>0) printf "  A 组 per_element: 均值 %.1f B，极差 %.1f B，相对离散 %.2f%%（%d 个样本）\n",
                     s/n, mx-mn, (mx-mn)/(s/n)*100, n }' "$CSV"

echo ">>> 结果 -> $CSV"
column -t -s, "$CSV"
( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
$PSQL -c "drop table if exists hnsw_a; drop table if exists hnsw_c;" >/dev/null 2>&1
