#!/usr/bin/env bash
# vecdiag · 一键复现（从零到全部证据）
#
# 目的：审查者在一台干净的 CentOS 7 / Rocky 8 机器上只需要这一条命令，
# 就能把仓库里所有结论重新跑一遍并生成自己的证据目录。
#
# 用法（root 执行即可，脚本自己降权到 postgres）：
#   bash vecdiag/reproduce.sh                 # 全流程
#   bash vecdiag/reproduce.sh --skip-bootstrap  # 环境已就绪，只跑验证
#
# 前置：能访问 github.com（拉 PG 与 pgvector 源码）。若装了 EPEL 的 openssl11-devel，
# 编译会自动带 SSL；没装则需先 `yum -y install openssl11-devel`（PG18 要求 >= 1.1.1）。
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
RUN_ID=${RUN_ID:-repro-$(date +%Y%m%d-%H%M%S)}
OUT=${OUT:-/data/artifacts/$RUN_ID}
SKIP_BOOTSTRAP=0
[ "${1:-}" = "--skip-bootstrap" ] && SKIP_BOOTSTRAP=1

step() { printf '\n\033[1m===== %s =====\033[0m\n' "$1"; }
asp()  { su - postgres -c "$1"; }        # 以 postgres 身份执行（PG 拒绝 root 跑实例）

mkdir -p "$OUT"

if [ "$SKIP_BOOTSTRAP" = "0" ]; then
  step "1/7 搭建 PostgreSQL 18.6 + pgvector 0.8.6（约 15-25 分钟）"
  bash "$HERE/tools/bootstrap_env.sh" 2>&1 | tee "$OUT/bootstrap.log"
else
  step "1/7 跳过环境搭建（--skip-bootstrap）"
fi

step "2/7 环境快照（门禁 K1/K2 要求：结果必须能对上环境）"
asp "bash $HERE/tools/env_check.sh" > "$OUT/env.txt" 2>&1
grep -E "PostgreSQL|devtoolset|selinux|Mem:|Swap:" "$OUT/env.txt" | head -8

step "3/7 复现两个诊断对象（IVFFlat 内存报错 / HNSW 落盘降级）"
asp "bash $HERE/tools/verify_phenomena.sh $RUN_ID" 2>&1 | tee "$OUT/phenomena.log" | grep -E "\[OK\]|\[WARN\]"

step "4/7 安装 vecdiag SQL 层"
asp "bash $HERE/tools/install.sh" 2>&1 | tail -3

step "5/7 ABI 常数实测（换机器必做，不能沿用他机数值）"
asp "bash $HERE/tools/abi_probe.sh ${RUN_ID}-abi" 2>&1 | tee "$OUT/abi.log" | tail -8

step "6/7 M1 · 20 组验证矩阵 + 回归用例"
asp "bash $HERE/tools/validate_memory_model.sh $HERE/tests/matrix_m1.tsv ${RUN_ID}-m1" \
  2>&1 | tee "$OUT/m1.log" | grep -E "PASS|FAIL|BLOCKED|AUTO"
asp "$PGHOME/bin/psql -p $PGPORT -d postgres -X -f $HERE/tests/test_m1_model.sql" \
  > "$OUT/test_m1.txt" 2>&1
echo "  回归用例中的 f（失败）个数：$(grep -c ' f ' "$OUT/test_m1.txt" || true)"

step "7/7 M2 · HNSW 标定与外样本验证"
asp "bash $HERE/tools/hnsw_spill_probe.sh ${RUN_ID}-m2" 2>&1 | tee "$OUT/m2.log" | grep -E "SPILL|NO |per_element"
asp "bash $HERE/tools/hnsw_validate.sh ${RUN_ID}-m2v" 2>&1 | tee "$OUT/m2v.log" | grep -E "PASS|FAIL|误差"
asp "$PGHOME/bin/psql -p $PGPORT -d postgres -X -f $HERE/tests/test_m2_spill.sql" \
  > "$OUT/test_m2.txt" 2>&1
echo "  区间未覆盖（in_range=f）的个数：$(grep -c ' f ' "$OUT/test_m2.txt" || true)"

step "完成"
cat <<EOF
证据目录：$OUT 以及 /data/artifacts/${RUN_ID}-{abi,m1,m2,m2v}/
判定标准：
  · M1 矩阵 20 组全部 PASS，BLOCKED 必须为 0
  · M1/M2 回归用例里不应出现 f
  · ABI 实测的 MAXALIGN(itemsize) 在 dims=128 下应为 520
换机器时注意：ABI 常数与编译器/块大小绑定，两台机器的结果必须分开归档，
不要把本仓库 results/ 里的数字当成新机器上的结论。
EOF
