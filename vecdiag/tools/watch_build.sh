#!/usr/bin/env bash
# 实时看板：一边建索引，一边看阶段 / 百分比 / 剩余时间 / 降级预警
#
# 必须周期性调用 vecdiag.build_monitor()——它在每次调用时顺手把"阶段何时变的"
# 记进 vecdiag.monitor_state，因为 pg_stat_progress_create_index 不提供阶段开始时刻。
# 没人调用时阶段起点只能退化成"第一次被观测到的时刻"，输出里的 observed_from 会暴露这一点。
#
# 用法（另开一个会话，以 postgres 身份）：
#   bash tools/watch_build.sh              # 每 500 ms 刷一次，直到没有构建在跑
#   INTERVAL_MS=200 LOG=/tmp/live.csv bash tools/watch_build.sh
#   MWM_KB=81920 bash tools/watch_build.sh # 构建方的 maintenance_work_mem 与本会话不同时
#                                          # 必须传进来，否则降级预警按本会话的值算，会误报
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
INTERVAL_MS=${INTERVAL_MS:-500}
IDLE_EXIT=${IDLE_EXIT:-20}          # 连续这么多轮没有构建就退出
LOG=${LOG:-}
# 字段分隔符踩过两次坑，两条都记下来：
#   1) 双引号里写 $'\t' 不会被 bash 当成 ANSI-C 转义，psql 会拿到 5 个字面字符；
#   2) 改成真正的制表符之后更糟——PSQL 变量是**不加引号展开**的，制表符在 IFS 里，
#      于是 "-F<TAB>" 被切成 "-F" 和空串，psql 收到一个孤零零的 -F，整条命令报错，
#      而错误又被 2>/dev/null 吞掉，表现为"看板永远空白"。
# 所以分隔符必须是**不在 IFS 里**的字符。这里用 \001（不会出现在标识符或阶段名里）。
SEP=$(printf '\001')
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -A -t -F$SEP"

case "$INTERVAL_MS" in ''|*[!0-9]*) echo "[FAIL] INTERVAL_MS 必须是整数" >&2; exit 2;; esac
case "$IDLE_EXIT"   in ''|*[!0-9]*) echo "[FAIL] IDLE_EXIT 必须是整数" >&2; exit 2;; esac

SLEEP=$(awk -v ms="$INTERVAL_MS" 'BEGIN{printf "%.3f", ms/1000}')
[ -n "$LOG" ] && echo "ts,pid,index,am,am_source,elapsed_source,phase,intra_pct,intra_source,pct,elapsed_s,eta_s,weight_basis,risk,eta_basis" > "$LOG"

printf '%-8s %-16s %-34s %-9s %-7s %-9s %-8s %s\n' \
       PID INDEX PHASE INTRA% PCT ELAPSED_S ETA_S RISK
idle=0
while :; do
  out=$($PSQL -c "select pid, index_name, phase, coalesce(intra_pct::text,'-'),
                         coalesce(pct::text,'-'), elapsed_s, coalesce(eta_s::text,'-'),
                         risk, intra_source, weight_basis, am, am_source, elapsed_source,
                         eta_basis
                    from vecdiag.build_monitor(${MWM_KB:-null}) order by pid;" 2>>"${ERRLOG:-/dev/null}")
  if [ -z "$out" ]; then
    idle=$((idle + 1))
    [ "$idle" -ge "$IDLE_EXIT" ] && { echo "（连续 $IDLE_EXIT 轮没有构建，退出）"; break; }
  else
    idle=0
    while IFS="$SEP" read -r pid idx phase intra pct el eta risk isrc wbasis am amsrc elsrc etab; do
      [ -z "${pid:-}" ] && continue
      printf '%-8s %-16s %-34s %-9s %-7s %-9s %-8s %s\n' \
             "$pid" "${idx:0:16}" "${phase:0:34}" "$intra" "$pct" "$el" "$eta" "$risk"
      [ -n "$LOG" ] && printf '%s,%s,%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s,"%s","%s","%s"\n' \
             "$(date +%s%3N)" "$pid" "$idx" "$am" "$amsrc" "$elsrc" "$phase" "$intra" "$isrc" \
             "$pct" "$el" "$eta" "$wbasis" "$risk" "$etab" >> "$LOG"
    done <<<"$out"
  fi
  sleep "$SLEEP"
done
[ -n "$LOG" ] && echo ">>> 实时序列已存 $LOG（可作为报告里"实时数据"的原始产物）"
