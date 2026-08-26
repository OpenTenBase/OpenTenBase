#!/usr/bin/env bash
# 环境自检：只读，不修改任何状态。输出可直接作为实验 manifest 的环境快照。
# 用法：bash 00_env_check.sh            （在服务器上执行）
#      bash 00_env_check.sh > env.txt  （存档）
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGDATA_DIR=${PGDATA_DIR:-/data/pg18/data}
PGPORT=${PGPORT:-5518}
SRC_PG=${SRC_PG:-/data/pg18/src/OpenTenBase}
SRC_VEC=${SRC_VEC:-/data/pg18/src/pgvector}

line() { printf '\n===== %s =====\n' "$1"; }

line "采集时间"
date -Is

line "主机与内核"
hostname
uname -a
cat /etc/os-release | head -4

line "CPU / 内存 / 磁盘"
echo "nproc: $(nproc)"
free -m | head -3          # 必须含 Swap 行：耗时类结论要声明 swap 在位
df -h / /data 2>/dev/null | sed 's/^/  /'
echo "/data 占用明细:"; du -sh /data/* 2>/dev/null | sed 's/^/  /'

line "内核与安全策略（影响 initdb / 文件访问）"
echo "selinux: $(getenforce 2>/dev/null || echo N/A)"
echo "ulimit -n: $(ulimit -n)"
echo "vm.overcommit_memory: $(cat /proc/sys/vm/overcommit_memory 2>/dev/null)"

line "编译器"
echo "默认 gcc: $(gcc --version 2>/dev/null | head -1)"
if [ -f /opt/rh/devtoolset-11/enable ]; then
  # shellcheck disable=SC1091
  source /opt/rh/devtoolset-11/enable
  echo "devtoolset-11 gcc: $(gcc --version | head -1)"
else
  echo "devtoolset-11: 未找到 /opt/rh/devtoolset-11/enable"
fi
echo "make: $(make --version | head -1)"
echo "git:  $(git --version)"
echo "python3: $(python3 --version 2>&1)"

line "构建依赖"
rpm -q bison flex readline-devel zlib-devel openssl-devel libicu-devel \
       perl-ExtUtils-Embed perl-Test-Simple 2>&1 | sed 's/^/  /'
echo "缺失的可选库（本项目不需要，configure 勿带对应开关）:"
for p in zstd-devel libuuid-devel tcl-devel; do
  rpm -q "$p" >/dev/null 2>&1 || echo "  $p 未安装 → 勿用 --with-${p%%-devel}"
done

line "Perl TAP 依赖（make prove_installcheck 前置）"
for m in IPC::Run Test::More Time::HiRes TAP::Harness; do
  if perl -M"$m" -e1 >/dev/null 2>&1; then echo "  $m OK"; else echo "  $m MISSING（prove_installcheck 会失败）"; fi
done
echo "  prove: $(command -v prove || echo 未找到)"

line "基线源码版本（写入 manifest 必需）"
# 本机 git 1.8.3.1 不支持 `git -C <dir>`（1.8.5 才有），统一用子 shell cd
if [ -d "$SRC_PG/.git" ]; then
  ( cd "$SRC_PG" && git rev-parse HEAD && git rev-parse --abbrev-ref HEAD )
  grep -m1 'AC_INIT' "$SRC_PG/configure.ac" 2>/dev/null
  echo "src/gtm 存在数（应为 0）: $(ls "$SRC_PG/src" 2>/dev/null | grep -c '^gtm$')"
  echo "contrib 含 vector 数（应为 0）: $(ls "$SRC_PG/contrib" 2>/dev/null | grep -ci vector)"
else
  echo "未找到 PG 源码：$SRC_PG"
fi

line "pgvector 源码版本"
if [ -d "$SRC_VEC/.git" ]; then
  ( cd "$SRC_VEC" && git rev-parse HEAD && (git describe --tags 2>/dev/null || true) )
else
  echo "未找到 pgvector 源码：$SRC_VEC"
fi

line "已安装的 PG18"
if [ -x "$PGHOME/bin/pg_config" ]; then
  "$PGHOME/bin/pg_config" --version
  "$PGHOME/bin/pg_config" --configure
else
  echo "未安装：$PGHOME/bin/pg_config"
fi

line "编译产物哈希（对照实验必需）"
VEC_SO="$($PGHOME/bin/pg_config --pkglibdir 2>/dev/null)/vector.so"
[ -f "$VEC_SO" ] && sha256sum "$VEC_SO" || echo "未找到 vector.so"

line "端口占用（5518 是我方；5432 上原先的 PG15 已于 2026-08-25 停止并禁用自启）"
ss -lntp 2>/dev/null | grep -E ':(5432|5518|5519)' || echo "无匹配监听"

line "实例状态与关键参数"
if [ -x "$PGHOME/bin/psql" ] && "$PGHOME/bin/pg_ctl" -D "$PGDATA_DIR" status >/dev/null 2>&1; then
  "$PGHOME/bin/psql" -p "$PGPORT" -d postgres -Atc 'select version();'
  "$PGHOME/bin/psql" -p "$PGPORT" -d postgres -c \
    "select name, setting, unit from pg_settings where name in
     ('shared_buffers','work_mem','maintenance_work_mem',
      'max_parallel_maintenance_workers','max_parallel_workers_per_gather',
      'log_min_messages','log_temp_files','track_io_timing') order by name;"
  "$PGHOME/bin/psql" -p "$PGPORT" -d postgres -c \
    "select extname, extversion from pg_extension where extname='vector';"
  "$PGHOME/bin/psql" -p "$PGPORT" -d postgres -c \
    "select name, setting from pg_settings where name like 'ivfflat.%' or name like 'hnsw.%' order by name;"
else
  echo "实例未运行或未安装。启动：$PGHOME/bin/pg_ctl -D $PGDATA_DIR -l $PGDATA_DIR/startup.log start"
fi

line "自检结论"
echo "以上输出请整份存入 /data/artifacts/<run_id>/env.txt，与结果文件同目录。"
echo "门禁要求：每份结果须能对应 version() / pg_config --configure / git rev-parse HEAD 三项。"
