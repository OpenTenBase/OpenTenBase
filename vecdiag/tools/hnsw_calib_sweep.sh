#!/usr/bin/env bash
# M2 · 加密标定扫描：为"在新机器上重拟合 slot_coef / base_bytes"采集数据
#
# 与 tools/hnsw_spill_probe.sh（8 点、够用即可）不同，本脚本跑一个**密网格**：
#   dims ∈ {128, 256, 384} × m ∈ {4, 8, 12, 16, 20, 32}，ef_construction 固定 64
#   （已证 ef 不进入图内存，m2-20260826 D 组：ef 64 与 200 降级点相同）
# 另在两个锚点各重复一次，量化 per_element 的测量重复性。
#
# **m=24 刻意不在网格里**——它是回归验证 hnsw_validate.sh V2 用例的取值，
# 必须保持"拟合外"身份，重拟合后拿它做外样本才有意义。
#
# 每个组合选 mwm 与行数使降级发生在构建中段（约 40%-70%）：
#   dims=128 rows=20000（图 16.9-34.8MB）、256 rows=12000、384 rows=10000，mwm 统一 16384 kB
#
# 用法（以 postgres 身份）：bash tools/hnsw_calib_sweep.sh [run_id]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
RUN=${1:-m2fit-$(date +%Y%m%d-%H%M%S)}
OUT=${OUTDIR:-/data/artifacts/$RUN}
MWM=16384
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=0"
mkdir -p "$OUT/raw"

CSV="$OUT/calib_sweep.csv"
echo "dims,m,ef_construction,mwm_kb,spill_tuples,per_element,run_id" > "$CSV"

rows_for() { case "$1" in 128) echo 20000;; 256) echo 12000;; 384) echo 10000;; esac; }

probe() {  # dims m tag
  local dims=$1 m=$2 tag=$3 rows log spilled pe
  rows=$(rows_for "$dims")
  log="$OUT/raw/d${dims}_m${m}_${tag}"
  $PSQL -c "drop table if exists cs_t;
            create table cs_t as
              select i as id, (select array_agg(random())::vector($dims)
                                 from generate_series(1,$dims)) as v
                from generate_series(1,$rows) i;" >/dev/null 2>&1
  $PSQL -c "analyze cs_t;
            set maintenance_work_mem='${MWM}kB';
            create index cs_ix on cs_t using hnsw (v vector_l2_ops) with (m=$m, ef_construction=64);" \
        >"$log.out" 2>"$log.err"
  spilled=$(sed -n 's/.*after \([0-9]*\) tuples.*/\1/p' "$log.err" | head -1)
  if [ -n "$spilled" ] && [ "$spilled" -gt 0 ]; then
    pe=$(awk -v w="$MWM" -v s="$spilled" 'BEGIN{printf "%.1f", w*1024/s}')
    echo "$dims,$m,64,$MWM,$spilled,$pe,'$RUN'" >> "$CSV"
    printf '  dims=%-3s m=%-2s %s：第 %s 行降级，per_element=%s B\n' "$dims" "$m" "$tag" "$spilled" "$pe"
  else
    echo "  dims=$dims m=$m $tag：[WARN] 未降级（图放得下）——检查 mwm/rows 配比" >&2
  fi
  $PSQL -c "drop table if exists cs_t;" >/dev/null 2>&1
}

echo ">>> 密网格标定（18 组 + 2 个重复性锚点，mwm=${MWM}kB 统一）"
for dims in 128 256 384; do
  for m in 4 8 12 16 20 32; do
    probe "$dims" "$m" main
  done
done
probe 128 16 repeat
probe 384 16 repeat

echo
echo ">>> 重复性（同配置两次的 per_element 差）"
awk -F, 'NR>1 && $7!="" {k=$1"_m"$2; v[k]=v[k]" "$6; n[k]++}
         END {for (k in v) if (n[k]>1) print "  "k": "v[k]}' "$CSV"

( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
echo ">>> 标定数据 -> $CSV"
echo ">>> 下一步：bash tools/load_hnsw_calib.sh $CSV && psql -c \"select * from vecdiag.hnsw_refit(false)\""
