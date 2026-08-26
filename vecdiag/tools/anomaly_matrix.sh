#!/usr/bin/env bash
# T4.3 + T4.4 · 异常场景矩阵 与 保守方向专项测试
#
# 两个目的合在一个脚本里，因为它们看的是同一件事的两面：
#   T4.4（异常场景矩阵）：每种异常配置下，工具的判定对不对；
#   T4.3（保守方向专项）：**有没有"模型说能建、实际建不起来"的用例**。
#       这是最危险的一类错误——工具说没事，用户照着做然后失败。
#       判定口径：
#         correct      模型说会失败 → 实际失败；模型说能建 → 实际建成
#         DANGEROUS    模型说能建   → 实际失败      ← 一个都不许有，出现即脚本非零退出
#         conservative 模型说会失败 → 实际建成      ← 可以接受，但要记下来
#
# 用法（以 postgres 身份）：bash tools/anomaly_matrix.sh [run_id]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
RUN=${1:-t44-$(date +%Y%m%d-%H%M%S)}
OUT=${OUTDIR:-/data/artifacts/$RUN}
mkdir -p "$OUT/raw"
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=0"
Q="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -At"
CSV="$OUT/anomaly_matrix.csv"
echo "case_id,scenario,am,rows,dims,param,mwm_kb,model_says,model_detail,actual,actual_detail,verdict" > "$CSV"
DANGEROUS=0

num() { case "$1" in ''|*[!0-9]*) echo "[FAIL] 期望整数，得到 '$1'" >&2; exit 2;; esac; }

# 造表：kind = normal（公开数据子集）| skewed（全同向量）| empty | tiny | unlogged
mk() {
  local tbl=$1 rows=$2 dims=$3 kind=$4
  $PSQL -c "drop table if exists $tbl;" >/dev/null 2>&1
  case "$kind" in
    normal)
      $PSQL -c "create table $tbl as select id, v from sift_base order by id limit $rows;" >/dev/null 2>&1 ;;
    unlogged)
      $PSQL -c "create unlogged table $tbl as select id, v from sift_base order by id limit $rows;" >/dev/null 2>&1 ;;
    skewed)
      # 全部向量完全相同：k-means 会退化（所有中心重合），用来看构建是否仍然成功
      $PSQL -c "create table $tbl as
                  select i as id, (select array_agg(1.0)::vector($dims)
                                     from generate_series(1,$dims)) as v
                    from generate_series(1,$rows) i;" >/dev/null 2>&1 ;;
    empty)
      $PSQL -c "create table $tbl (id int, v vector($dims));" >/dev/null 2>&1 ;;
    tiny)
      $PSQL -c "create table $tbl as select id, v from sift_base order by id limit $rows;" >/dev/null 2>&1 ;;
  esac
  $PSQL -c "analyze $tbl;" >/dev/null 2>&1
}

# 跑一个用例：先取模型判定，再真建一次，比对
one() {
  local id=$1 scenario=$2 am=$3 rows=$4 dims=$5 param=$6 mwm_kb=$7 kind=${8:-normal} extra=${9:-}
  num "$rows"; num "$dims"; num "$mwm_kb"
  # 表名必须小写：不加引号的标识符会被 PostgreSQL 折叠成小写，
  # 而后面是拿 relname 去 pg_class 里比字符串的。用 am_A1 建表、再查 relname='am_A1_ix'
  # 永远查不到，于是每个用例都被判成 failed —— 第一版就栽在这里。
  local tbl=am_$(printf '%s' "$id" | tr 'A-Z' 'a-z')
  mk "$tbl" "$rows" "$dims" "$kind"
  local real_rows; real_rows=$($Q -c "select count(*) from $tbl;")

  # ---- 模型判定 ----
  local says detail
  if [ "$am" = "ivfflat" ]; then
    local lists=${param#lists=}
    read -r fh mb < <($Q -F' ' -c "select first_hit::text, predicted_mb
                                     from vecdiag.ivfflat_predict(p_rows := $real_rows,
                                          p_dims := $dims, p_lists := $lists,
                                          p_mwm_kb := $mwm_kb);")
    if [ "$fh" = "none" ]; then says=buildable; else says=will_fail; fi
    detail="first_hit=$fh predicted_mb=$mb"
  else
    local m=16
    read -r sp gmb rec < <($Q -F' ' -c "select will_spill::text, estimated_graph_mb, recommended_mwm_mb
                                          from vecdiag.hnsw_predict_spill($real_rows, $dims, $m, $mwm_kb);")
    # HNSW 的降级不是失败：上游只打 NOTICE 然后转磁盘继续。所以模型判定分两档。
    if [ "$sp" = "true" ]; then says=will_spill; else says=no_spill; fi
    detail="graph_mb=$gmb recommended_mwm_mb=$rec"
  fi

  # ---- 实际构建 ----
  local log="$OUT/raw/$id"
  $PSQL -c "set maintenance_work_mem='${mwm_kb}kB'; $extra
            create index ${tbl}_ix on $tbl using $am (v vector_l2_ops) with ($param);" \
        >"$log.out" 2>"$log.err"
  local built; built=$($Q -c "select count(*) from pg_class where relname='${tbl}_ix';")
  local actual actual_detail
  if [ "$built" = "1" ]; then
    actual=built
    if grep -qi 'no longer fits' "$log.err"; then
      actual=built_spilled
      actual_detail=$(sed -n 's/.*after \([0-9]*\) tuples.*/spill_at=\1/p' "$log.err" | head -1)
    else
      actual_detail=$(grep -iE '^(WARNING|NOTICE)' "$log.err" | head -1 | tr ',' ';')
    fi
  else
    actual=failed
    actual_detail=$(grep -iE '^ERROR' "$log.err" | head -1 | tr ',' ';')
  fi
  [ -z "$actual_detail" ] && actual_detail='-'

  # ---- 判定 ----
  local verdict
  case "$says/$actual" in
    buildable/built)        verdict=correct ;;
    buildable/failed)       verdict=DANGEROUS ;;
    will_fail/failed)       verdict=correct ;;
    will_fail/built)        verdict=conservative ;;
    no_spill/built)         verdict=correct ;;
    no_spill/built_spilled) verdict=DANGEROUS ;;
    will_spill/built_spilled) verdict=correct ;;
    will_spill/built)       verdict=conservative ;;
    *)                      verdict="unclassified($says/$actual)" ;;
  esac
  [ "$verdict" = DANGEROUS ] && DANGEROUS=$((DANGEROUS + 1))

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$id" "$scenario" "$am" "$real_rows" "$dims" "${param//,/;}" "$mwm_kb" \
    "$says" "$detail" "$actual" "$actual_detail" "$verdict" >> "$CSV"
  printf '  %-6s %-22s %-12s → 实际 %-14s %s\n' "$id" "$scenario" "$says" "$actual" "$verdict"

  $PSQL -c "drop table if exists $tbl;" >/dev/null 2>&1
}

echo ">>> T4.4 异常场景矩阵"
one A1 低内存-IVFFlat          ivfflat 100000 128 "lists=100"   1024   normal
one A2 低内存-刚好够           ivfflat 100000 128 "lists=100"   65536  normal
one A3 极端lists-上界          ivfflat 10000  128 "lists=32768" 262144 normal
one A4 极端lists-远超行数      ivfflat 1000   128 "lists=5000"  262144 normal
one A5 空表                    ivfflat 0      128 "lists=100"   262144 empty
one A6 小表-10行               ivfflat 10     128 "lists=100"   262144 tiny
one A7 偏斜-全同向量           ivfflat 20000  128 "lists=100"   262144 skewed
one A8 unlogged表              ivfflat 20000  128 "lists=100"   262144 unlogged
one A9 HNSW-低内存降级         hnsw    50000  128 "m=16"        20480  normal
one A10 HNSW-内存充足          hnsw    50000  128 "m=16"        262144 normal
one A11 HNSW-并行构建          hnsw    50000  128 "m=16"        20480  normal "set max_parallel_maintenance_workers=2;"
one A12 HNSW-禁并行            hnsw    50000  128 "m=16"        20480  normal "set max_parallel_maintenance_workers=0;"

echo
echo ">>> T4.3 保守方向专项：把内存卡在真正的临界值上下各 1 kB"
# 用 ivfflat_min_mwm_kb 给出的**所需内存**（三个检查点的最大值），不是报错里的 MB。
# 第一版就是拿报错里的 predicted_mb 去设内存，结果"临界+0MB"也失败了——
# 因为抬高内存只让第一个检查点放行，接着撞上更大的 C3。这条正是 T4.3 要抓的东西。
#   need   kB → 必须建成，失败即 DANGEROUS（模型偏乐观）
#   need-1 kB → 必须失败，建成则记 conservative
for cfg in "100000 128 200" "100000 128 500" "50000 128 1000"; do
  set -- $cfg
  rows=$1; dims=$2; lists=$3
  need=$($Q -c "select min_mwm_kb from vecdiag.ivfflat_min_mwm_kb($rows, $dims, $lists);")
  num "$need"
  one "B${lists}hi" "所需内存+0kB(lists=$lists)" ivfflat "$rows" "$dims" "lists=$lists" "$need"           normal
  one "B${lists}lo" "所需内存-1kB(lists=$lists)" ivfflat "$rows" "$dims" "lists=$lists" $((need - 1))     normal
done

echo
echo ">>> 判定汇总"
awk -F, 'NR>1 {c[$NF]++} END {for (k in c) printf "  %-14s %s\n", k, c[k]}' "$CSV"
( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
echo ">>> 矩阵 -> $CSV；每个用例的原始 stdout/stderr -> $OUT/raw/"

if [ "$DANGEROUS" -gt 0 ]; then
  echo "[FAIL] 出现 $DANGEROUS 个 DANGEROUS 用例（模型说能建/不降级，实际失败/降级）——必须修模型或改判定口径" >&2
  exit 1
fi
echo "[OK] 没有 DANGEROUS 用例"
