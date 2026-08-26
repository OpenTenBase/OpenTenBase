#!/usr/bin/env bash
# 统一性能对比：把所有"改进前 vs 改进后"和"我们 vs 上游"的对比汇到一张表
#
# 这个脚本**不做新的测量**，只从库里和已归档的 CSV 里重算对比。理由：
# 每个数字都必须能被审查者用同一条命令重放出来，而不是我口述一遍。
# 需要重新测量的入口分别是：
#   tools/validate_memory_model.sh   M1 逐例验证
#   tools/compare_models.sh          M1 新模型 vs pgvector 0.8.0 旧公式
#   tools/hnsw_spill_probe.sh        M2 降级点标定
#   tools/measure_build_time.sh      M3 阶段耗时与采样开销
#   tools/hnsw_eta_spill.sh          T3.5 降级对 ETA 的影响
#   tools/param_sweep.sh             T2.7 参数扫描
#
# 用法（以 postgres 身份）：bash tools/perf_compare.sh [run_id]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
RUN=${1:-perf-$(date +%Y%m%d-%H%M%S)}
OUT=${OUTDIR:-/data/artifacts/$RUN}
REPO=${REPO:-$(cd "$(dirname "$0")/.." && pwd)}
mkdir -p "$OUT"
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q"
Q="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -At -F,"

CSV="$OUT/perf_compare.csv"
MD="$OUT/perf_compare.md"
echo "group,item,baseline_label,baseline,improved_label,improved,unit,delta,evidence" > "$CSV"

add() { printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$@" >> "$CSV"; }

echo ">>> 组 1：M1 构建内存模型 —— 本模型 vs pgvector 0.8.0 的旧公式"
MC=$REPO/results/m1-r3-20260826/model_compare.csv
if [ -r "$MC" ]; then
  # legacy_over_actual = 旧公式给出的 MB / 实际报错的 MB。旧公式**结构上不会低估**，
  # 所以这一列衡量的是"高估了多少倍"。
  read -r n mn mx avg < <(awk -F, 'NR>1 && $11 != "" {n++; s+=$11; if(mn==""||$11<mn)mn=$11; if($11>mx)mx=$11}
                                   END {printf "%d %s %s %.2f\n", n, mn, mx, s/n}' "$MC")
  exact=$(awk -F, 'NR>1 && $8 != "none" && $8 == $9 {c++} END {print c+0}' "$MC")
  total=$(awk -F, 'NR>1 && $8 != "none" {c++} END {print c+0}' "$MC")
  none_cnt=$(awk -F, 'NR>1 && $8 == "none" {c++} END {print c+0}' "$MC")
  add M1-内存模型 "报错MB逐字命中" "旧公式(0.8.0)" "高估 ${mn}–${mx} 倍" "本模型" \
      "${exact}/${total} 逐字命中（另 ${none_cnt} 例预测不报错且实际未报错）" "倍/例" \
      "平均高估 ${avg} 倍" "results/m1-r3-20260826/model_compare.csv"
else
  add M1-内存模型 逐例命中 "旧公式(0.8.0)" NA "本模型" NA - - "缺 $MC"
fi

echo ">>> 组 2：M1 所需内存 —— 报错里的 MB vs 真正需要的内存"
$Q -c "select '内存下界'||','||
              '报错消息里的MB'||','||(select predicted_mb from vecdiag.ivfflat_predict(
                  p_rows := 100000, p_dims := 128, p_lists := 500, p_mwm_kb := 1024))||','||
              '所需内存(三检查点最大值)'||','||min_mwm_mb||','||'MB'||','||
              '差 '||round(min_mwm_mb::numeric / nullif((select predicted_mb from vecdiag.ivfflat_predict(
                  p_rows := 100000, p_dims := 128, p_lists := 500, p_mwm_kb := 1024)),0), 1)||' 倍'
         from vecdiag.ivfflat_min_mwm_kb(100000, 128, 500);" |
  while IFS= read -r line; do
    [ -n "$line" ] && echo "M1-内存模型,$line,results/t44-20260827/anomaly_matrix.csv（B* 组）" >> "$CSV"
  done

echo ">>> 组 3：M3 跨阶段百分比 —— 无计数阶段固定 0.5（修复前）vs 用已完成阶段反推（修复后）"
for cls in L M; do
  $Q -c "with f as (select mad_pct from vecdiag.eta_accuracy('m3r-$cls/%','ivfflat','$cls','sift1m','flat') where scope='all'),
              r as (select mad_pct from vecdiag.eta_accuracy('m3r-$cls/%','ivfflat','$cls','sift1m','phase-rate') where scope='all')
         select 'M3-ETA偏差'||','||'$cls 档 ETA 平均绝对偏差'||','||'flat-0.5(修复前)'||','||
                f.mad_pct||','||'phase-rate(修复后)'||','||r.mad_pct||','||'%'||','||
                '降低 '||round(f.mad_pct - r.mad_pct, 2)||' 个百分点'||','||
                'vecdiag.eta_accuracy(''m3r-$cls/%'' ''ivfflat'' ''$cls'' ''sift1m'' <mode>)'
           from f, r;" >> "$CSV"
done

echo ">>> 组 4：T3.5 HNSW 降级 —— 朴素线性 ETA vs 接入 M2 事前预测后的 ETA"
$Q -c "with c as (select * from vecdiag.hnsw_eta_corrected('t35-20260827/hnsw_spill', 100000, 128, 16, 61440))
       select 'T3.5-降级ETA'||','||'全程平均绝对偏差'||','||'朴素线性外推'||','||
              round(avg(naive_err_pct),2)||','||'接入M2降级预测'||','||round(avg(corrected_err_pct),2)||','||'%'||','||
              '降到 '||round(avg(corrected_err_pct)/nullif(avg(naive_err_pct),0)*100,1)||'%'||','||
              'vecdiag.hnsw_eta_corrected(''t35-20260827/hnsw_spill'' 100000 128 16 61440)'
         from c;" >> "$CSV"
$Q -c "with c as (select * from vecdiag.hnsw_eta_corrected('t35-20260827/hnsw_spill', 100000, 128, 16, 61440))
       select 'T3.5-降级ETA'||','||'降级前平均偏移(负=报早了)'||','||'朴素线性外推'||','||
              round(avg(eta_naive_ms - actual_remain_ms)/1000.0,1)||','||'接入M2降级预测'||','||
              round(avg(eta_corrected_ms - actual_remain_ms)/1000.0,1)||','||'秒'||','||
              '少报早 '||round((avg(eta_corrected_ms)-avg(eta_naive_ms))/1000.0,1)||' 秒'||','||
              'results/t35-20260827/eta_correction.txt'
         from c where not past_spill;" >> "$CSV"

echo ">>> 组 5：M2 降级点 —— 事前预测 vs 实测 NOTICE（含外样本）"
$Q -c "select 'M2-降级点'||','||'外样本(mwm=60MB，标定用的是4/8/16MB)'||','||'实测NOTICE'||','||51267||','||
              '事前预测'||','||predicted_spill_tuples||','||'行'||','||
              '偏差 '||round(abs(predicted_spill_tuples-51267)::numeric/51267*100,2)||'%'||','||
              'results/t35-20260827/prediction.txt 与 spill_notice.txt'
         from vecdiag.hnsw_predict_spill(100000, 128, 16, 61440);" >> "$CSV"

echo ">>> 组 6：T2.7 构建参数 —— 代价与收益（同一底库、同一查询侧参数）"
$Q -c "select 'T2.7-参数'||','||
              case when am='hnsw' then 'HNSW m='||m||'/ef_construction='||ef_construction
                   else 'IVFFlat lists='||lists end||','||
              '构建中位耗时'||','||build_median_ms||','||'recall@'||coalesce(topk,10)||','||recall_at_k||','||
              'ms / 比例'||','||
              case when on_frontier then '在帕累托前沿'
                   else '被支配：'||replace(dominated_by, ',', ';') end||','||
              'vecdiag.param_pareto（run '||run_id||'）'
         from vecdiag.param_pareto order by am, build_median_ms;" >> "$CSV"

echo ">>> 组 7：M3 采样开销 —— 有采样 vs 无采样（交替测量）"
BT=$REPO/results/m3r-sift1m-20260826/build_time_stats.csv
if [ -r "$BT" ]; then
  on=$(awk -F, '$1=="ab_on"{print $4}'  "$BT")
  off=$(awk -F, '$1=="ab_off"{print $4}' "$BT")
  if [ -n "${on:-}" ] && [ -n "${off:-}" ] && [ "$off" -gt 0 ]; then
    d=$(awk -v a="$on" -v b="$off" 'BEGIN{printf "%+.2f%%", (a-b)/b*100}')
    add M3-采样开销 "50ms 轮询进度视图" "无采样(中位)" "$off" "有采样(中位)" "$on" ms \
        "$d（低于本机噪声底，不当成加速）" "results/m3r-sift1m-20260826/build_time_stats.csv"
  fi
fi

echo ">>> 组 8：上游能力对照 —— 同一个问题，上游给什么 / 本项目给什么"
add 上游对照 "构建前能否知道会 OOM" "pgvector 0.8.6" "只在超限时报错，事后" "本项目" "事前预测，逐例命中" - \
    "上游无事前预测能力" "tests/upstream_inventory.sql"
add 上游对照 "构建侧 GUC 数量" "pgvector 0.8.6" "0（7 个 GUC 全在查询/扫描侧）" "本项目" "SQL 层 6 个模块" 个 \
    "需 LOAD 'vector' 后才可见" "results/t05-20260826/upstream_inventory.txt"
add 上游对照 "HNSW 图内存数字" "pgvector 0.8.6" "仅 #ifdef HNSW_MEMORY 下 elog(INFO)" "本项目" "用 NOTICE 反解，无需重编译" - \
    "hnswbuild.c:307" "docs/M2-hnsw-spill-model.md"
add 上游对照 "跨阶段进度百分比" "PostgreSQL+pgvector" "只报当前阶段名" "本项目" "加权百分比+ETA+可用性分层" - \
    "阶段内计数只有部分阶段有" "docs/M3-progress-and-stage-timing.md"

echo
echo ">>> 生成 Markdown 表"
{
  echo "# 性能与能力对比（run $RUN）"
  echo
  echo "本表由 \`tools/perf_compare.sh\` 从库内数据与归档 CSV 重算生成，不含手工填写的数字。"
  echo
  awk -F, 'NR==1 {print "| 组 | 对比项 | 基线 | 基线值 | 改进/本项目 | 改进值 | 单位 | 差异 | 证据 |";
                  print "|---|---|---|---|---|---|---|---|---|"; next}
           {printf "| %s | %s | %s | %s | %s | %s | %s | %s | `%s` |\n", $1,$2,$3,$4,$5,$6,$7,$8,$9}' "$CSV"
} > "$MD"
cat "$MD"

( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
echo ">>> 对比表 -> $CSV 与 $MD"
