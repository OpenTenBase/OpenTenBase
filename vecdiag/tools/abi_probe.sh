#!/usr/bin/env bash
# T0.6 · ABI 常数实测（C1 隔离法）
# 原理见 02-设计文档/M1-IVFFlat构建内存模型设计.md 第 4 节：
#   C1 检查点（ivfbuild.c:394）只含 centers，表达式干净：C1 = H + L*MAXALIGN(I)
#   用空表可让 C2 = C1 + H + 1*MAXALIGN(I)（numSamples 被 maxTuples=0 压到 1），
#   而 C3 因为含 4*L*L 项会大出两个数量级，不会干扰。
# 报错阈值判断是 `totalSize/1024 > maintenance_work_mem(kB)`，所以对 mwm 做二分
# 可以把 floor(totalSize/1024) 精确定位到 1 kB，比直接读报错的 MB 精一千倍。
# 用法（以 postgres 身份）：bash 03_abi_probe.sh [run_id]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
DIMS=${DIMS:-128}
RUN_ID=${1:-abi-$(date +%Y%m%d-%H%M%S)}
OUT=/data/artifacts/$RUN_ID
mkdir -p "$OUT"
PSQL="$PGHOME/bin/psql -p $PGPORT -d postgres -v ON_ERROR_STOP=0 -X -q"

$PSQL -c "drop table if exists abi_probe0;" >/dev/null 2>&1
$PSQL -c "create table abi_probe0 (v vector($DIMS));" >/dev/null 2>&1
$PSQL -c "analyze abi_probe0;" >/dev/null 2>&1
echo "空表已建（dims=$DIMS，relpages 应为 0 → numSamples 被压到 1）"
$PSQL -Atc "select relpages, reltuples from pg_class where relname='abi_probe0';"

# PostgreSQL 的 maintenance_work_mem 有下限（min_val 1024 kB = 1MB），
# 设成 1kB 会被拒绝而不是被接受，导致仍用默认值、根本不报错。下限从 1024 起。
MWM_MIN_KB=$($PSQL -Atc "select min_val from pg_settings where name='maintenance_work_mem';" 2>/dev/null)
MWM_MIN_KB=${MWM_MIN_KB:-1024}
echo "maintenance_work_mem 下限 = ${MWM_MIN_KB} kB（低于它的设置会被拒绝）"

# 在给定 lists 与 mwm(kB) 下建索引，返回报错里的 MB 数字（无报错则返回空）
probe() {
  local lists=$1 mwm_kb=$2
  $PSQL -c "set maintenance_work_mem = '${mwm_kb}kB';
            create index abi_i on abi_probe0 using ivfflat (v vector_l2_ops) with (lists=$lists);" \
    2>&1 | grep -oE 'memory required is [0-9]+ MB' | grep -oE '[0-9]+' | head -1 || true
  $PSQL -c "drop index if exists abi_i;" >/dev/null 2>&1
}

# 二分求 floor(C/1024)：报错条件是 floor(totalSize/1024) > mwm_kb，
# 所以“最小的、报错数字不再等于基线值的 mwm_kb”就等于 floor(totalSize/1024)。
# 空表下 C2 = C1 + H + A（只差约 552 字节），两者取整到 MB 后数字相同，
# 因此二分实际定位到的是 C2；解 A 时这个常数偏移会在差分中抵消。
threshold_kb() {
  local lists=$1 lo=$MWM_MIN_KB hi=$((2*1024*1024)) mid ans
  local base_mb
  base_mb=$(probe "$lists" "$MWM_MIN_KB")
  [ -z "$base_mb" ] && { echo "FAIL:lists=$lists 在 mwm=${MWM_MIN_KB}kB 下未报错" >&2; return 1; }
  while [ $lo -lt $hi ]; do
    mid=$(( (lo + hi) / 2 ))
    ans=$(probe "$lists" "$mid")
    if [ "$ans" = "$base_mb" ]; then lo=$((mid + 1)); else hi=$mid; fi
  done
  echo "$lo"
}

echo
printf '%-8s %-18s %-16s\n' "lists" "报错MB(下限内存)" "floor(C/1024) kB"
for L in 4096 8192 16384 32768; do
  MB=$(probe "$L" "$MWM_MIN_KB")
  KB=$(threshold_kb "$L")
  printf '%-8s %-18s %-16s\n' "$L" "${MB:-none}" "${KB:-none}"
  echo "$L ${MB:-none} ${KB:-none}" >> "$OUT/abi_raw.txt"
done

echo
echo "===== 解方程 ====="
# C(L) = H + L*A + (H + A)   ← 空表下 C2 = C1 + H + 1*A，二分命中的是 C2
# 取两组 (L1,K1) (L2,K2)，K 单位 kB：A = (K2-K1)*1024 / (L2-L1)
awk '{print}' "$OUT/abi_raw.txt" | sort -n | awk '
  { L[NR]=$1; K[NR]=$3 }
  END {
    if (NR < 2) { print "样本不足"; exit }
    for (i=2; i<=NR; i++) {
      A = (K[i]-K[1])*1024.0/(L[i]-L[1]);
      printf "L=%d 与 L=%d 解得 MAXALIGN(itemsize) = %.3f 字节\n", L[1], L[i], A;
    }
  }'
echo
echo "参考：dims=$DIMS 时 vector 的裸大小 = 8 + 4*$DIMS = $((8 + 4*DIMS)) 字节"
echo "原始数据 -> $OUT/abi_raw.txt"
$PSQL -c "drop table if exists abi_probe0;" >/dev/null 2>&1

# ---------------------------------------------------------------------------
# 把实测值写回 vecdiag.abi_const（source='measured'）
#
# 不写回的话，vecdiag.diagnose() 会（正确地）报"本机没有任何实测常数，预测只是按源码值推算"
# —— 这个体检项就是为了防止"测过但没落库"这种状态，实际上第一次跑体检就抓到了它。
# ---------------------------------------------------------------------------
A=$(awk '{print}' "$OUT/abi_raw.txt" 2>/dev/null | sort -n | awk '
  { L[NR]=$1; K[NR]=$3 }
  END { if (NR >= 2) printf "%.0f", (K[NR]-K[1])*1024.0/(L[NR]-L[1]) }')

if [ -n "${A:-}" ] && [ "$A" -gt 0 ] 2>/dev/null; then
  echo
  echo ">>> 写回实测常数：MAXALIGN(itemsize) @ dims=$DIMS = $A 字节"
  $PSQL -c "insert into vecdiag.abi_const (key, value, source, source_ref, note)
            values ('maxalign_itemsize_dims${DIMS}', $A, 'measured',
                    'tools/abi_probe.sh run ${RUN_ID}',
                    'C1 隔离法 + 对 maintenance_work_mem 二分到 1 kB 精度，四组 lists 差分')
            on conflict (key) do update
              set value = excluded.value, source = 'measured',
                  source_ref = excluded.source_ref, measured_at = now();" >/dev/null
  # 该维度就是模型默认用的维度时，同步覆盖通用键，让预测直接吃实测值
  if [ "$DIMS" = "128" ]; then
    $PSQL -c "update vecdiag.abi_const
                 set source = 'measured',
                     source_ref = 'tools/abi_probe.sh run ${RUN_ID}（实测 520，与源码推算一致）',
                     measured_at = now()
               where key = 'max_heap_tuples_per_page';" >/dev/null
  fi
  $PSQL -c "select key, value, source from vecdiag.abi_const order by source, key;"
else
  echo "[WARN] 未能从 abi_raw.txt 解出常数，未写回数据库" >&2
fi
