#!/usr/bin/env bash
# T1.6 · 新旧模型对照：把 20 组矩阵的实测报错值、新模型预测、旧模型（0.8.0 口径）预测放在一起
#
# 输出 CSV，供 tools/plot_compare.py 重建对照图。图必须能从这份 CSV 重画出来，
# 否则图就成了不可复核的装饰。
#
# 用法：bash tools/compare_models.sh <harness 的 results.tsv> [输出目录]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
RESULTS=${1:?用法: compare_models.sh <results.tsv> [outdir]}
OUTDIR=${2:-$(dirname "$RESULTS")}
CSV="$OUTDIR/model_compare.csv"
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -At"

echo "case_id,class,rows,dims,lists,mwm_kb,first_hit,actual_mb,new_mb,legacy_mb,legacy_over_actual,legacy_samples,new_samples" > "$CSV"

while IFS=$'\t' read -r case_id class rows dims lists target mode first_hit predicted_mb actual_mb mwm_kb relpages v errf; do
  [ "$case_id" = "case_id" ] && continue
  [ -z "${case_id:-}" ] && continue

  # relpages 直接取 harness 当时记录的真实值。用 estimate_relpages 回填会把估算误差
  # 混进模型误差——高维（vector 超过 TOAST 阈值）时能差出几倍，X2 那组就踩过这个坑。
  pages=${relpages:-}
  if ! [[ "$pages" =~ ^[0-9]+$ ]]; then
    echo "[SKIP] $case_id 缺真实 relpages，跳过（不用估算值凑）" >&2
    continue
  fi

  new=$($PSQL -c "select coalesce(predicted_mb::text,'none')
                    from vecdiag.ivfflat_predict($rows,$dims,$lists,$pages,false,$mwm_kb);")
  new_ns=$($PSQL -c "select num_samples
                       from vecdiag.ivfflat_predict($rows,$dims,$lists,$pages,false,$mwm_kb);")
  read -r leg_mb leg_fires leg_ns < <($PSQL -F' ' -c \
    "select legacy_mb, legacy_fires, legacy_samples
       from vecdiag.ivfflat_predict_legacy080($rows,$dims,$lists,$mwm_kb);")

  # 旧模型相对实测报错值的倍数。实测无报错时留空，不硬凑一个比值。
  if [ "$actual_mb" != "none" ] && [ -n "$actual_mb" ]; then
    ratio=$(awk -v a="$leg_mb" -v b="$actual_mb" 'BEGIN{printf "%.2f", a/b}')
  else
    ratio=""
  fi

  echo "$case_id,$class,$rows,$dims,$lists,$mwm_kb,$first_hit,$actual_mb,$new,$leg_mb,$ratio,$leg_ns,$new_ns" >> "$CSV"
done < "$RESULTS"

echo "对照 CSV -> $CSV"
column -t -s, "$CSV"
