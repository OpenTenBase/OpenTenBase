#!/usr/bin/env bash
# T3.5 · HNSW 降级（图转磁盘）造成的耗时突变，ETA 能不能反映出来
#
# 为什么这个实验有意义：HNSW 只有一个构建阶段，进度百分比等于
# tuples_done/tuples_total，也就是**按元组线性推进**。但降级之后每元组的耗时会变，
# 线性外推的 ETA 必然出偏差。M2 能事先算出"第几行降级"，所以这里要回答两件事：
#   1) 降级前后每元组耗时差多少（量化，不是"变慢了"）；
#   2) ETA 在降级点附近偏了多少，方向是偏高还是偏低。
# 能反映就写成亮点，不能反映就写清原因——不许含糊过去。
#
# 用法（以 postgres 身份）：bash tools/hnsw_eta_spill.sh [run_id] [行数] [mwm]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
RUN=${1:-m3s-$(date +%Y%m%d-%H%M%S)}
ROWS=${2:-100000}
MWM=${3:-60MB}                  # 让降级点落在构建中段（预测 ≈ 51000 行处）
M=${M:-16}
OUT=${OUTDIR:-/data/artifacts/$RUN}
SAMPLER=$(dirname "$0")/progress_sampler.sh
mkdir -p "$OUT"
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=0"

case "$ROWS" in ''|*[!0-9]*) echo "[FAIL] 行数必须是整数" >&2; exit 2;; esac
case "$M"    in ''|*[!0-9]*) echo "[FAIL] m 必须是整数" >&2; exit 2;; esac
case "$MWM"  in *[!0-9A-Za-z]*) echo "[FAIL] mwm 格式不合法" >&2; exit 2;; esac

echo ">>> 底库：$ROWS 行 × 128 维（公开数据集子集）"
$PSQL -c "drop table if exists es_base;
          create table es_base as select id, v from sift_base order by id limit $ROWS;
          analyze es_base;" || exit 1

echo ">>> 事前预测（M2）：mwm=$MWM 下会不会降级、在第几行"
$PSQL -c "select will_spill, predicted_spill_tuples, estimated_graph_mb,
                 recommended_mwm_mb, per_element_bytes, confidence
            from vecdiag.hnsw_predict_spill($ROWS, 128, $M,
                 (select vecdiag.parse_mem_kb('$MWM')));" | tee "$OUT/prediction.txt"

echo ">>> 采样 + 构建（$MWM，采样 50 ms）"
SP=$(bash "$SAMPLER" start "$OUT/samples.csv" 50)
T0=$(date +%s%3N)
$PSQL -c "set maintenance_work_mem='$MWM';
          create index es_ix on es_base using hnsw (v vector_l2_ops) with (m=$M);" \
      >"$OUT/build.out" 2>"$OUT/build.err"
T1=$(date +%s%3N)
bash "$SAMPLER" stop "$SP"
echo "    总耗时 $((T1 - T0)) ms"
grep -i 'no longer fits' "$OUT/build.err" | tee "$OUT/spill_notice.txt" || echo "（没有降级 NOTICE）"

SPILL_AT=$(sed -n 's/.*after \([0-9]*\) tuples.*/\1/p' "$OUT/spill_notice.txt" | head -1)
echo "    实测降级行号：${SPILL_AT:-无}"

echo ">>> 采样序列落库"
# 采样 CSV 的第一行是 "# sampler_start_epoch_ms=..." 注释，第二行才是表头。
# COPY 不认注释行，必须先剥掉，否则表头会被当成数据、报 invalid input syntax。
tail -n +2 "$OUT/samples.csv" > "$OUT/samples_clean.csv"
$PSQL -c "delete from vecdiag.progress_sample where run_id = '$RUN/hnsw_spill';" >/dev/null
$PSQL -c "create temp table s_stg (elapsed_ms bigint, pid int, phase text,
            blocks_total bigint, blocks_done bigint, tuples_total bigint,
            tuples_done bigint, relid oid, index_relid oid);
          copy s_stg from '$OUT/samples_clean.csv' with (format csv, header true, null '');
          insert into vecdiag.progress_sample
            select '$RUN/hnsw_spill', * from s_stg;" || exit 1
$PSQL -Atc "select count(*) from vecdiag.progress_sample where run_id = '$RUN/hnsw_spill';"

echo
echo ">>> 降级前后每元组耗时（用采样序列的相邻差分算速率）"
$PSQL -c "with s as (
            select elapsed_ms, tuples_done,
                   lag(elapsed_ms) over (order by elapsed_ms) as pe,
                   lag(tuples_done) over (order by elapsed_ms) as pt
            from vecdiag.progress_sample
            where run_id = '$RUN/hnsw_spill' and tuples_done is not null
          ),
          d as (
            select tuples_done,
                   (elapsed_ms - pe)::numeric / nullif(tuples_done - pt, 0) as ms_per_tuple
            from s where tuples_done > pt
          )
          select case when tuples_done <= ${SPILL_AT:-0} then 'before_spill' else 'after_spill' end as seg,
                 count(*) as n_intervals,
                 round(avg(ms_per_tuple), 5) as avg_ms_per_tuple,
                 round(percentile_cont(0.5) within group (order by ms_per_tuple)::numeric, 5) as median
            from d group by 1 order by 1 desc;" | tee "$OUT/rate_by_segment.txt"

echo
echo ">>> ETA 偏差：整体 vs 降级点之后"
$PSQL -c "with e as (
            select * from vecdiag.eta_error('$RUN/hnsw_spill', 'hnsw', 'pooled', 'sift1m')
          ),
          j as (
            select e.*, p.tuples_done
            from e join vecdiag.progress_sample p
              on p.run_id = '$RUN/hnsw_spill' and p.elapsed_ms = e.elapsed_ms
          )
          select case when tuples_done <= ${SPILL_AT:-0} then '1_before_spill' else '2_after_spill' end as seg,
                 count(*) n,
                 round(avg(abs_err_pct), 2) as mad_pct,
                 round(avg(eta_ms - actual_remain_ms), 0) as signed_bias_ms,
                 round(avg(case when eta_ms < actual_remain_ms then 1 else 0 end) * 100, 1) as pct_underestimate
            from j group by 1 order by 1;" | tee "$OUT/eta_by_segment.txt"

$PSQL -c "drop index if exists es_ix; drop table if exists es_base;" >/dev/null 2>&1
( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
echo ">>> 产物 -> $OUT"
