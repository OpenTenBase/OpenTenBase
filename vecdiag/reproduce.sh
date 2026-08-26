#!/usr/bin/env bash
# vecdiag · 一键复现（从零到全部证据）
#
# 目的：审查者在一台干净的 **CentOS 7** 机器上只需要这一条命令，
# 就能把仓库里所有结论重新跑一遍并生成自己的证据目录。
#
# 平台范围（不要扩大声明）：只在 CentOS 7 + devtoolset-11 上实测过。
# Rocky/RHEL 8+ 用 gcc-toolset 而不是 devtoolset，bootstrap_env.sh 那条路**没测过**，
# 需要自行调整工具链启用命令。
#
# 用法（root 执行即可，脚本自己降权到 postgres）：
#   bash vecdiag/reproduce.sh                 # 全流程
#   bash vecdiag/reproduce.sh --skip-bootstrap  # 环境已就绪，只跑验证
#
# 前置：能访问 github.com（拉源码）与 EPEL 源（装 openssl11）。
# 缺 devtoolset-11 或 openssl11-devel 时 bootstrap_env.sh 会**自动 yum 安装**
# （以 root 执行时；非 root 则打印命令并退出）。
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
  step "1/9 搭建 PostgreSQL 18.6 + pgvector 0.8.6（约 15-25 分钟）"
  bash "$HERE/tools/bootstrap_env.sh" 2>&1 | tee "$OUT/bootstrap.log"
else
  step "1/9 跳过环境搭建（--skip-bootstrap）"
fi

step "2/9 环境快照（门禁 K1/K2 要求：结果必须能对上环境）"
asp "bash $HERE/tools/env_check.sh" > "$OUT/env.txt" 2>&1
grep -E "PostgreSQL|devtoolset|selinux|Mem:|Swap:" "$OUT/env.txt" | head -8

step "3/9 复现两个诊断对象（IVFFlat 内存报错 / HNSW 落盘降级）"
asp "bash $HERE/tools/verify_phenomena.sh $RUN_ID" 2>&1 | tee "$OUT/phenomena.log" | grep -E "\[OK\]|\[WARN\]"

step "4/9 安装 vecdiag SQL 层"
asp "bash $HERE/tools/install.sh" 2>&1 | tail -3

step "5/9 ABI 常数实测（换机器必做，不能沿用他机数值）"
asp "bash $HERE/tools/abi_probe.sh ${RUN_ID}-abi" 2>&1 | tee "$OUT/abi.log" | tail -8

step "6/9 M1 · 20 组验证矩阵 + 回归用例"
asp "bash $HERE/tools/validate_memory_model.sh $HERE/tests/matrix_m1.tsv ${RUN_ID}-m1" \
  2>&1 | tee "$OUT/m1.log" | grep -E "PASS|FAIL|BLOCKED|AUTO"
asp "$PGHOME/bin/psql -p $PGPORT -d postgres -X -f $HERE/tests/test_m1_model.sql" \
  > "$OUT/test_m1.txt" 2>&1
echo "  回归用例中的 f（失败）个数：$(grep -c ' f ' "$OUT/test_m1.txt" || true)"

step "7/9 M2 · HNSW 标定与外样本验证"
asp "bash $HERE/tools/hnsw_spill_probe.sh ${RUN_ID}-m2" 2>&1 | tee "$OUT/m2.log" | grep -E "SPILL|NO |per_element"
asp "bash $HERE/tools/hnsw_validate.sh ${RUN_ID}-m2v" 2>&1 | tee "$OUT/m2v.log" | grep -E "PASS|FAIL|误差"
asp "$PGHOME/bin/psql -p $PGPORT -d postgres -X -f $HERE/tests/test_m2_spill.sql" \
  > "$OUT/test_m2.txt" 2>&1
echo "  区间未覆盖（in_range=f）的个数：$(grep -c ' f ' "$OUT/test_m2.txt" || true)"

step "8/9 M3 · 阶段耗时、阶段权重与加权进度"
# 这一步会真实建 4 个配置各 4 次（含 warm-up），耗时约 10-15 分钟
asp "bash $HERE/tools/measure_build_time.sh ${RUN_ID}-m3 3" 2>&1 \
  | tee "$OUT/m3.log" | grep -E "min=|开销|第 [0-9] 轮"
chmod -R a+rX "/data/artifacts/${RUN_ID}-m3" 2>/dev/null || true
asp "bash $HERE/tools/load_stage_weights.sh /data/artifacts/${RUN_ID}-m3/samples ${RUN_ID}-m3" \
  2>&1 | tee "$OUT/m3_weights.log" | grep -E "已载入|weight|total"
asp "$PGHOME/bin/psql -p $PGPORT -d postgres -X -f $HERE/tests/test_m3_progress.sql" \
  > "$OUT/test_m3.txt" 2>&1
echo "  单调性断言 ok_monotone：$(grep -A2 '单调' "$OUT/test_m3.txt" | grep -oE '\| t$|\| f$' | head -1)"
echo "  回归用例中的 f（失败）个数：$(grep -c ' f ' "$OUT/test_m3.txt" || true)"

step "9/9 M4 · 零参数体检与可用性分层"
asp "$PGHOME/bin/psql -p $PGPORT -d postgres -X -x -c 'select * from vecdiag.diagnose()'" \
  > "$OUT/diagnose.txt" 2>&1
echo "  体检发现条数：$(grep -c '^severity' "$OUT/diagnose.txt" || true)"
asp "$PGHOME/bin/psql -p $PGPORT -d postgres -X -f $HERE/tests/test_m4_diagnose.sql" \
  > "$OUT/test_m4.txt" 2>&1
echo "  回归用例中的 f（失败）个数：$(grep -c ' f ' "$OUT/test_m4.txt" || true)"

step "完成"
cat <<EOF
证据目录：$OUT 以及 /data/artifacts/${RUN_ID}-{abi,m1,m2,m2v,m3}/
判定标准（任一条不满足就不要认这轮结果）：
  · M1 矩阵 20 组全部 PASS，BLOCKED 必须为 0
  · M1 / M2 / M3 的回归用例里都不应出现 f
  · ABI 实测的 MAXALIGN(itemsize) 在 dims=128 下应为 520
  · M2 的 11 组实测降级点全部落在预测区间内（in_range 全 t）
  · M3 的加权进度单调性断言 ok_monotone = t，且每个访问方法的权重求和为 1
  · M3 的采样开销 < 2%（交替测量得出；顺序测量会得到无意义的负值）
  · M4 体检的每条输出都带齐问题/原因/调整方法/验证方式四要素，且修复建议可直接执行
  · 阶段权重的消费视图 vecdiag.stage_weight_usable 里不含 pooled、不含超限组；
    被排除的组仍留在 vecdiag.stage_weight_audit 里并附不可用原因（证据不删、结论不混）
换机器时注意：ABI 常数与编译器/块大小绑定，两台机器的结果必须分开归档，
不要把本仓库 results/ 里的数字当成新机器上的结论。
EOF

