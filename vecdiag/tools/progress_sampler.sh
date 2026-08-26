#!/usr/bin/env bash
# M3 · 构建进度采样器
#
# 上游已有能力（**不得宣称原创**）：
#   pg_stat_progress_create_index 是 PostgreSQL 自带视图；
#   pgvector 自 0.2.3 起就在 CREATE INDEX 期间上报子阶段（ivfflat 的 kmeans/assign/load、
#   hnsw 的 load/load-in-memory 等）。
# 本采样器做的只是：把这个视图按固定间隔抽样落成时间序列，供阶段耗时分解与 ETA 使用。
#
# 用法：
#   progress_sampler.sh start <采样输出.csv> [间隔毫秒]   → 后台采样，打印 sampler pid
#   progress_sampler.sh stop  <pid>
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -At -F,"

cmd=${1:?用法: progress_sampler.sh start <csv> [间隔毫秒] | stop <pid>}

case "$cmd" in
start)
  out=${2:?缺少输出文件}
  interval_ms=${3:-200}
  # 时间轴对齐：首行记录采样起点，后续 elapsed_ms 都相对它
  echo "# sampler_start_epoch_ms=$(($(date +%s%3N)))" > "$out"
  echo "elapsed_ms,pid,phase,blocks_total,blocks_done,tuples_total,tuples_done,relid,index_relid" >> "$out"
  # 后台采样循环必须把 stdout/stderr 从继承的管道上摘掉。
  # 否则调用方用 sp=$(... start ...) 取 pid 时，命令替换会一直等这个管道关闭 —— 直接挂死。
  (
    t0=$(date +%s%3N)
    while :; do
      now=$(date +%s%3N)
      # 只取正在建索引的后端；没有就跳过这一拍，不写空行
      $PSQL -c "select $((now - t0))||','||pid||','||coalesce(phase,'')||','||
                       coalesce(blocks_total,0)||','||coalesce(blocks_done,0)||','||
                       coalesce(tuples_total,0)||','||coalesce(tuples_done,0)||','||
                       relid||','||coalesce(index_relid,0)
                  from pg_stat_progress_create_index;" 2>/dev/null >> "$out"
      # sleep 支持小数；间隔用毫秒表达便于换算
      sleep "$(awk -v m="$interval_ms" 'BEGIN{printf "%.3f", m/1000}')"
    done
  ) >/dev/null 2>&1 &
  echo $!
  ;;
stop)
  pid=${2:?缺少 pid}
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  ;;
*)
  echo "未知子命令：$cmd" >&2; exit 2 ;;
esac
