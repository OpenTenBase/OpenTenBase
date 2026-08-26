#!/usr/bin/env bash
# vecdiag · M1 验证 harness（四态状态机）
#
# 状态定义（RQ-108，禁止互相改写）：
#   real            真实 CREATE INDEX 并捕获到报错原文，预测与报错逐字比对   → 可作 A 级证据
#   real-threshold  通过压低 maintenance_work_mem 触发同源检查点            → A 级证据，但结论必须附边界声明
#   auto            无法连库，只有模型输出                                  → 不得称为观测值
#   blocked         环境限制未能执行                                        → 如实记录，**不可被判为 pass**
#
# 代码级保证：verdict() 是唯一产出判定的地方，auto/blocked 分支根本不产生 PASS；
# 且只要出现 blocked，脚本最终以非零退出，报告脚本无法把它当成功。
#
# 用法：
#   bash validate_memory_model.sh [矩阵文件] [run_id]
#   PGHOME=/data/pg18/install PGPORT=5518 bash validate_memory_model.sh
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
MATRIX=${1:-$(dirname "$0")/../tests/matrix_m1.tsv}
RUN_ID=${2:-m1-$(date +%Y%m%d-%H%M%S)}
OUTDIR=${OUTDIR:-/data/artifacts/$RUN_ID}
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=0"

mkdir -p "$OUTDIR/stderr"
RESULT="$OUTDIR/results.tsv"
SUMMARY="$OUTDIR/summary.txt"

n_pass=0; n_fail=0; n_blocked=0; n_auto=0

# 环境可用性：决定是 real* 还是 auto
if $PSQL -Atc 'select 1' >/dev/null 2>&1; then
  DB_UP=1
else
  DB_UP=0
  echo "[WARN] 连不上数据库（$PGHOME/bin/psql -p $PGPORT），本轮所有用例只能是 auto" >&2
fi

printf 'case_id\tclass\trows\tdims\tlists\ttarget\tmode\tfirst_hit\tpredicted_mb\tactual_mb\tmwm_kb\tverdict\tstderr_file\n' > "$RESULT"

# ---------------------------------------------------------------------------
# 唯一的判定出口。auto/blocked 不产生 PASS，这是代码级保证而不是自觉。
# ---------------------------------------------------------------------------
verdict() {
  local mode=$1 predicted=$2 actual=$3
  case "$mode" in
    real|real-threshold)
      if [ -z "$predicted" ] || [ -z "$actual" ]; then
        echo "FAIL"
      elif [ "$predicted" = "$actual" ]; then
        echo "PASS"
      else
        echo "FAIL"
      fi
      ;;
    auto)    echo "AUTO-ONLY" ;;   # 只有模型输出，不是观测值
    blocked) echo "BLOCKED"  ;;    # 未能执行，必须如实记录
    *)       echo "BLOCKED"  ;;    # 未知模式一律按 blocked 处理，不许乐观兜底
  esac
}

# 参数白名单校验：拒绝把任何非数字塞进 SQL（T4.6 安全加固）
is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }

# 建表并 ANALYZE。成功返回 0，失败返回 1（调用方据此进入 blocked）
prepare_table() {
  local rows=$1 dims=$2 unlogged=$3 tbl=$4
  local kw=""
  [ "$unlogged" = "1" ] && kw="unlogged"
  $PSQL -c "drop table if exists $tbl;" >/dev/null 2>&1
  $PSQL -c "create $kw table $tbl (id int, v vector($dims));" >/dev/null 2>&1 || return 1
  if [ "$rows" -gt 0 ]; then
    $PSQL -c "insert into $tbl select i, (select array_agg(random())::vector($dims)
                from generate_series(1,$dims)) from generate_series(1,$rows) i;" >/dev/null 2>&1 || return 1
  fi
  $PSQL -c "analyze $tbl;" >/dev/null 2>&1 || return 1
  return 0
}

# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------
while IFS=$'\t' read -r -u 3 case_id class rows dims lists target unlogged; do
  [ -z "${case_id:-}" ] && continue
  case "$case_id" in \#*) continue ;; esac

  for v in "$rows" "$dims" "$lists"; do
    if ! is_uint "$v"; then
      echo "[SKIP] $case_id 参数非法（$v），拒绝执行" >&2
      printf '%s\t%s\t%s\t%s\t%s\t%s\tblocked\t\t\t\t\tBLOCKED\t\n' \
        "$case_id" "$class" "$rows" "$dims" "$lists" "$target" >> "$RESULT"
      n_blocked=$((n_blocked + 1)); continue 2
    fi
  done
  [ "${unlogged:-0}" = "1" ] || unlogged=0

  TBL="vecdiag_case_${case_id//[^A-Za-z0-9_]/_}"
  ERRF="$OUTDIR/stderr/${case_id}.err"

  if [ "$DB_UP" = "0" ]; then
    printf '%s\t%s\t%s\t%s\t%s\t%s\tauto\t\t\t\t\t%s\t\n' \
      "$case_id" "$class" "$rows" "$dims" "$lists" "$target" "$(verdict auto '' '')" >> "$RESULT"
    n_auto=$((n_auto + 1)); continue
  fi

  if ! prepare_table "$rows" "$dims" "$unlogged" "$TBL"; then
    echo "[BLOCKED] $case_id 建表或 ANALYZE 失败" >&2
    printf '%s\t%s\t%s\t%s\t%s\t%s\tblocked\t\t\t\t\tBLOCKED\t\n' \
      "$case_id" "$class" "$rows" "$dims" "$lists" "$target" >> "$RESULT"
    n_blocked=$((n_blocked + 1)); continue
  fi

  # 先问模型：目标检查点该用多大的 maintenance_work_mem，预测报错多少 MB
  # 行数用矩阵里的真值，不用 reltuples（后者是 ANALYZE 估计值，误差会混进模型误差）。
  # p_empty_build 恒为 false：正常 CREATE INDEX 一定有 heap，与表是否 unlogged 无关。
  read -r mwm_kb expect_mb < <($PSQL -Atc \
    "select mwm_kb||' '||coalesce(expect_mb::text,'none')
       from vecdiag.ivfflat_mwm_plan(
              $rows, $dims, $lists,
              (select relpages::bigint from pg_class where oid='$TBL'::regclass),
              false)
      where target = '$target';" 2>/dev/null)

  if ! is_uint "${mwm_kb:-}"; then
    echo "[BLOCKED] $case_id 模型未能给出 mwm 取值（target=$target）" >&2
    printf '%s\t%s\t%s\t%s\t%s\t%s\tblocked\t\t\t\t\tBLOCKED\t\n' \
      "$case_id" "$class" "$rows" "$dims" "$lists" "$target" >> "$RESULT"
    n_blocked=$((n_blocked + 1))
    $PSQL -c "drop table if exists $TBL;" >/dev/null 2>&1
    continue
  fi

  # 模型完整预测（用同一个 mwm 值），拿 first_hit 与 predicted_mb
  read -r first_hit predicted_mb < <($PSQL -Atc \
    "select first_hit||' '||coalesce(predicted_mb::text,'none')
       from vecdiag.ivfflat_predict_table('$TBL'::regclass, $lists, null, $mwm_kb, $rows);" 2>/dev/null)

  # 真正建索引，捕获 stderr 原文（一字不改归档）
  $PSQL -c "set maintenance_work_mem='${mwm_kb}kB';
            create index ${TBL}_ivf on $TBL using ivfflat (v vector_l2_ops) with (lists=$lists);" \
    > "$ERRF.out" 2> "$ERRF"
  actual_mb=$(grep -oE 'memory required is [0-9]+ MB' "$ERRF" | grep -oE '[0-9]+' | head -1)

  # 模式判定：目标是 none（构建应成功）时属于 real；否则是压内存触发的 real-threshold
  [ -z "$actual_mb" ] && actual_mb=none
  if [ "$target" = "none" ] || [ "${predicted_mb:-}" = "none" ]; then
    # 预期不报错：包括 target=none，以及空表这种 C3 不适用（走 RandomCenters）的情况
    mode=real
  else
    mode=real-threshold
  fi

  v=$(verdict "$mode" "${predicted_mb:-}" "${actual_mb:-}")
  case "$v" in
    PASS)      n_pass=$((n_pass + 1)) ;;
    BLOCKED)   n_blocked=$((n_blocked + 1)) ;;
    AUTO-ONLY) n_auto=$((n_auto + 1)) ;;
    *)         n_fail=$((n_fail + 1)) ;;
  esac

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$case_id" "$class" "$rows" "$dims" "$lists" "$target" "$mode" \
    "${first_hit:-}" "${predicted_mb:-}" "${actual_mb:-}" "$mwm_kb" "$v" \
    "stderr/${case_id}.err" >> "$RESULT"
  printf '  [%-9s] %-14s target=%-4s first_hit=%-4s 预测=%-5s 实际=%-5s mwm=%s kB\n' \
    "$v" "$case_id" "$target" "${first_hit:-?}" "${predicted_mb:-?}" "${actual_mb:-?}" "$mwm_kb"

  $PSQL -c "drop index if exists ${TBL}_ivf; drop table if exists $TBL;" >/dev/null 2>&1
done 3< "$MATRIX"

# ---------------------------------------------------------------------------
# 汇总与归档
# ---------------------------------------------------------------------------
{
  echo "run_id      = $RUN_ID"
  echo "matrix      = $MATRIX"
  echo "PGHOME      = $PGHOME  PGPORT = $PGPORT"
  echo "采集时间     = $(date -Is)"
  echo
  echo "PASS        = $n_pass"
  echo "FAIL        = $n_fail"
  echo "BLOCKED     = $n_blocked   （未能执行，不得计为通过）"
  echo "AUTO-ONLY   = $n_auto      （仅模型输出，不是观测值）"
  echo
  echo "边界声明：mode=real-threshold 的用例是通过压低 maintenance_work_mem 触发同源检查点得到的，"
  echo "         只证明后端检查点与预测一致，不证明巨型索引构建成功。"
} | tee "$SUMMARY"

if command -v sha256sum >/dev/null 2>&1; then
  ( cd "$OUTDIR" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
  echo "SHA256SUMS 已生成 -> $OUTDIR/SHA256SUMS"
fi

echo "结果 -> $RESULT"

# blocked 存在即非零退出：报告脚本无法把它当成功
if [ "$n_blocked" -gt 0 ] || [ "$n_fail" -gt 0 ]; then
  exit 1
fi
exit 0




