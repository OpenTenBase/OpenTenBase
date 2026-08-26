#!/usr/bin/env bash
# T4.5 · 跑 TAP 回归（prove 直接消费 tests/t/*.pl 的输出）
#
# 通过率与不通过项都要留档：TAP 原文写进 $OUT/tap.txt，摘要写进 $OUT/tap_summary.txt。
# 不通过项不许静默——脚本非零退出。
#
# 用法（以 postgres 身份）：bash tests/run_tap.sh [run_id]
set -uo pipefail

RUN=${1:-tap-$(date +%Y%m%d-%H%M%S)}
OUT=${OUTDIR:-/data/artifacts/$RUN}
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT"

command -v prove >/dev/null 2>&1 || { echo "[FAIL] 找不到 prove（perl 自带工具链）" >&2; exit 2; }

echo ">>> prove -v $HERE/t/"
set +e
prove -v "$HERE"/t/*.pl 2>&1 | tee "$OUT/tap.txt"
rc=${PIPESTATUS[0]}
set -e

{
  echo "run_id=$RUN"
  echo "prove_exit=$rc"
  grep -cE '^ok '     "$OUT/tap.txt" | sed 's/^/passed=/'
  grep -cE '^not ok ' "$OUT/tap.txt" | sed 's/^/failed=/'
  echo "--- 不通过项 ---"
  grep -E '^not ok ' "$OUT/tap.txt" || echo "（无）"
} > "$OUT/tap_summary.txt"
cat "$OUT/tap_summary.txt"

( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
exit "$rc"
