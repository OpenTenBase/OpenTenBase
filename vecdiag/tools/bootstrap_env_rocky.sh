#!/usr/bin/env bash
# 一键搭建（Rocky Linux 8/9 变体，草稿）：
#   PG18(REL_18_STABLE) + pgvector 0.8.6，端口 5518
#
# ⚠ 状态：**未在目标机实测**（2026-08-27，虚拟机尚不可达，待密钥与网络就绪后
# 在本机验证；验证后才会进仓库并替换 reproduce.sh 里的平台声明）。
# 与 CentOS 7 版（vecdiag/tools/bootstrap_env.sh）的差异：
#   1) 包管理器 dnf（Rocky 8+，yum 只是别名，这里直接用 dnf）；
#   2) 编译器：Rocky 9 自带 gcc 11.4 直接用；Rocky 8 默认 gcc 8.5，
#      若不足 10 则装 gcc-toolset-12 并启用（等价于 CentOS 7 的 devtoolset）；
#   3) OpenSSL：Rocky 8 自带 1.1.1k、Rocky 9 自带 3.0.7，均满足 PG18 的 >=1.1.1，
#      不需要 EPEL 的 openssl11 那一套；
#   4) 不依赖 SCL（centos-release-scl），gcc-toolset 直接来自 AppStream。
#
# 用法（可以用 root 或 sudo 调用，脚本自动降权到 postgres）：
#   bash bootstrap_env_rocky.sh 2>&1 | tee /data/artifacts/bootstrap.log
# 数据目录默认 /data/pg18；虚拟机没有 /data 时用 ROOT=$HOME/pg18 bash bootstrap_env_rocky.sh
set -euo pipefail

ROOT=${ROOT:-/data/pg18}
SRC="$ROOT/src"
PGHOME="$ROOT/install"
PGDATA_DIR="$ROOT/data"
PGPORT=${PGPORT:-5518}
JOBS=${JOBS:-"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"}
LOGDIR=${LOGDIR:-/data/artifacts/bootstrap}
PG_BRANCH=REL_18_STABLE
VEC_TAG=v0.8.6
RUNUSER=${RUNUSER:-postgres}

step() { printf '\n\033[1m>>> %s\033[0m\n' "$1"; }
die()  { printf '\n[FAIL] %s\n' "$1" >&2; [ -n "${2:-}" ] && tail -20 "$2" >&2; exit 1; }

# GitHub https 直连在部分网络下会被重置（SSL_ERROR_SYSCALL errno 104），
# 这是**网络抖动不是代码问题**——clone 失败过要重试而不是直接 die。
# 2026-08-27 实跑：PG 源码 clone 成功，pgvector clone 在虚拟机→GitHub 上被 reset，
# 挂在这一步前整个引导白跑。这里最多重试 5 次、退避递增。
retry() {
  local n=1
  until "$@"; do
    if [ "$n" -ge 5 ]; then echo "[FAIL] 重试 $n 次仍失败：$*" >&2; return 1; fi
    echo "[retry] 第 $n 次失败，${n}0 秒后重试：$*" >&2
    sleep "$((n * 10))"
    n=$((n + 1))
  done
}

# 平台自检：只认 Rocky/RHEL 8 或 9（用 os-release 判定，不猜）
if [ -f /etc/os-release ]; then
  . /etc/os-release
  case "$ID-$VERSION_ID" in
    rocky-8*|rhel-8*|rocky-9*|rhel-9*) : ;;
    *) die "本脚本只在 Rocky/RHEL 8 与 9 上适配过；当前是 $ID $VERSION_ID" ;;
  esac
else
  die "找不到 /etc/os-release，无法确认发行版"
fi

# PostgreSQL 硬规则：initdb 与 postmaster 拒绝以 root 运行（initdb: cannot be run as root）。
# 用 root 跑时先装好依赖、建用户、chown，再降权重执行自己。
if [ "$(id -u)" -eq 0 ]; then
  step "以 root 检查并补齐编译依赖（Rocky $VERSION_ID）"
  need_pkgs=()
  # 编译器：maj < 10 才需要 gcc-toolset。Rocky 9 自带 gcc 11 不会走到这条。
  # gcc --version 首行是 "gcc (GCC) 8.5.0 20210514 ..."，用 -dumpversion 拿纯版本号再取主版本。
  # 早先 sed 匹配 "gcc [0-9]" 在 "gcc (GCC) 8.5.0" 上匹配不到，落到非数字串，
  # `[ -lt ]` 报 integer expression expected 并**静默走了 else 分支**——gcc-toolset 永远没装。
  gcc_maj=$(gcc -dumpversion 2>/dev/null | cut -d. -f1)
  if [ -z "$gcc_maj" ] || [ "$gcc_maj" -lt 10 ]; then
    rpms="gcc gcc-c++"
    need_pkgs+=(gcc-toolset-12)     # 提供 gcc-toolset-12-gcc/-gcc-c++/binutils
  else
    rpms="gcc gcc-c++"
  fi
  # perl-IPC-Run 在 EPEL（prove_installcheck 的依赖），Rocky 8/9 先启用 EPEL
  rpm -q epel-release >/dev/null 2>&1 || dnf -y install epel-release >/dev/null 2>&1 || true
  for p in $rpms bison flex readline-devel zlib-devel libicu-devel \
           perl-ExtUtils-Embed perl-Test-Simple perl-IPC-Run python3; do
    rpm -q "$p" >/dev/null 2>&1 || need_pkgs+=("$p")
  done
  if [ "${#need_pkgs[@]}" -gt 0 ]; then
    echo "缺少：${need_pkgs[*]}"
    dnf -y install "${need_pkgs[@]}" || die "依赖安装失败（检查 dnf 源与网络，EPEL 未启用时 perl-IPC-Run 可能装不上）"
    echo "已安装：${need_pkgs[*]}"
  else
    echo "依赖齐全，无需安装"
  fi

  getent passwd "$RUNUSER" >/dev/null 2>&1 || useradd -m -s /bin/bash "$RUNUSER"
  mkdir -p "$ROOT" "$LOGDIR" /data/datasets /data/artifacts 2>/dev/null || mkdir -p "$ROOT" "$LOGDIR"
  # postgres 后续要写 LOGDIR（/data/artifacts）与 /data/datasets，这里一并给属主
  chown -R "$RUNUSER:$RUNUSER" "$ROOT" /data/artifacts /data/datasets 2>/dev/null || true
  SELF=$(readlink -f "$0")
  chmod a+r "$SELF"
  # ⚠ 降权后 postgres 要能**穿越整个路径**读到本脚本：SELF 所在目录及其所有父目录
  # 都必须对 postgres 可读/可执行（典型反例：/data/opentenbase 是 0700，降权后
  # bash 报 "Permission denied"）。部署时请把本脚本放到 /data/artifacts/ 或 /tmp 下执行。
  step "检测到以 root 运行：改用 $RUNUSER 身份继续（PG 不允许 root 跑实例）"
  exec su - "$RUNUSER" -c \
    "ROOT='$ROOT' PGPORT='$PGPORT' JOBS='$JOBS' LOGDIR='$LOGDIR' RUNUSER='$RUNUSER' bash '$SELF'"
fi

mkdir -p "$SRC" "$PGHOME" "$LOGDIR"
echo "运行身份：$(id -un)（uid $(id -u)）"
echo "发行版：$PRETTY_NAME"

step "启用编译器"
# gcc-toolset 与 devtoolset 一样，enable 脚本在 /opt/rh/<name>/enable
# Rocky 8 的 gcc-toolset-12 装好后：source /opt/rh/gcc-toolset-12/enable
gcc_maj=$(gcc -dumpversion 2>/dev/null | cut -d. -f1)
if [ "$gcc_maj" -lt 10 ]; then
  if [ -f /opt/rh/gcc-toolset-12/enable ]; then
    # shellcheck disable=SC1091
    source /opt/rh/gcc-toolset-12/enable
  else
    die "gcc 版本过低且没有 gcc-toolset-12。以 root 重跑本脚本可自动安装"
  fi
fi
gcc --version | head -1

step "拉取 PostgreSQL 基线源码（$PG_BRANCH）"
if [ ! -d "$SRC/OpenTenBase/.git" ]; then
  retry git clone --branch "$PG_BRANCH" --single-branch --depth 50 \
    https://github.com/OpenTenBase/OpenTenBase.git "$SRC/OpenTenBase" \
    > "$LOGDIR/clone_pg.log" 2>&1 || die "clone 失败" "$LOGDIR/clone_pg.log"
fi
PG_SHA=$(cd "$SRC/OpenTenBase" && git rev-parse HEAD)
echo "PG commit: $PG_SHA"
grep -m1 AC_INIT "$SRC/OpenTenBase/configure.ac"

step "确认基线是纯社区 PG（无 gtm、contrib 无 vector）"
[ "$(ls "$SRC/OpenTenBase/src" | grep -c '^gtm$')" = "0" ] || die "src/gtm 存在，分支拉错"
[ "$(ls "$SRC/OpenTenBase/contrib" | grep -ci vector)" = "0" ] || die "contrib 含 vector，分支拉错"

step "编译安装 PostgreSQL（约 15-25 分钟）"
# Rocky 8 自带 OpenSSL 1.1.1k、Rocky 9 自带 3.0.7，均满足 PG18 configure 的 >=1.1.1，
# 不需要像 CentOS 7 那样装 openssl11 并指头文件路径。
if [ ! -x "$PGHOME/bin/pg_config" ]; then
  cd "$SRC/OpenTenBase"
  ./configure --prefix="$PGHOME" --with-openssl --with-icu --with-readline \
      --enable-tap-tests \
      > "$LOGDIR/configure.log" 2>&1 || die "configure 失败" "$LOGDIR/configure.log"
  make -j"$JOBS" > "$LOGDIR/make.log" 2>&1 || die "make 失败" "$LOGDIR/make.log"
  make install > "$LOGDIR/make_install.log" 2>&1 || die "make install 失败" "$LOGDIR/make_install.log"
fi
"$PGHOME/bin/pg_config" --version

step "安装 contrib/pg_stat_statements"
if [ ! -f "$("$PGHOME/bin/pg_config" --pkglibdir)/pg_stat_statements.so" ]; then
  make -C "$SRC/OpenTenBase/contrib/pg_stat_statements" install \
    > "$LOGDIR/contrib_pgss.log" 2>&1 || die "pg_stat_statements 安装失败" "$LOGDIR/contrib_pgss.log"
fi
ls -l "$("$PGHOME/bin/pg_config" --pkglibdir)/pg_stat_statements.so"

step "初始化数据目录"
if [ ! -f "$PGDATA_DIR/PG_VERSION" ]; then
  "$PGHOME/bin/initdb" -D "$PGDATA_DIR" -E UTF8 --locale=C \
      > "$LOGDIR/initdb.log" 2>&1 || die "initdb 失败" "$LOGDIR/initdb.log"
  cat >> "$PGDATA_DIR/postgresql.conf" <<EOF

# ---- project2 baseline (Rocky VM) ----
port = $PGPORT
listen_addresses = 'localhost'
shared_buffers = 512MB
work_mem = 16MB
maintenance_work_mem = 256MB
max_parallel_maintenance_workers = 2
max_parallel_workers_per_gather = 2
logging_collector = on
log_directory = 'log'
log_min_messages = notice
log_statement = 'ddl'
log_temp_files = 0
track_io_timing = on
shared_preload_libraries = 'pg_stat_statements'
EOF
fi

step "启动实例（端口 $PGPORT）"
if ! "$PGHOME/bin/pg_ctl" -D "$PGDATA_DIR" status >/dev/null 2>&1; then
  "$PGHOME/bin/pg_ctl" -D "$PGDATA_DIR" -l "$PGDATA_DIR/startup.log" start \
      || die "启动失败（SELinux Enforcing 下若 AVC 拦 postmaster 端口，需 setenforce 0 并记入 env.txt）" "$PGDATA_DIR/startup.log"
  sleep 2
fi
"$PGHOME/bin/psql" -p "$PGPORT" -d postgres -Atc 'select version();'

step "拉取并编译 pgvector $VEC_TAG"
if [ ! -d "$SRC/pgvector/.git" ]; then
  retry git clone --branch "$VEC_TAG" --single-branch --depth 1 \
    https://github.com/pgvector/pgvector.git "$SRC/pgvector" \
    > "$LOGDIR/clone_vec.log" 2>&1 || die "clone pgvector 失败" "$LOGDIR/clone_vec.log"
fi
VEC_SHA=$(cd "$SRC/pgvector" && git rev-parse HEAD)
echo "pgvector commit: $VEC_SHA"
cd "$SRC/pgvector"
export PG_CONFIG="$PGHOME/bin/pg_config"
make clean > /dev/null 2>&1 || true
make > "$LOGDIR/pgvector_make.log" 2>&1 || die "pgvector make 失败" "$LOGDIR/pgvector_make.log"
make install > "$LOGDIR/pgvector_install.log" 2>&1 || die "pgvector install 失败" "$LOGDIR/pgvector_install.log"
sha256sum "$($PG_CONFIG --pkglibdir)/vector.so"

step "启用扩展并核对版本"
"$PGHOME/bin/psql" -p "$PGPORT" -d postgres -c 'create extension if not exists vector;' >/dev/null
"$PGHOME/bin/psql" -p "$PGPORT" -d postgres -Atc \
  "select extname||' '||extversion from pg_extension where extname='vector';"

step "存档环境快照"
{
  echo "os=$PRETTY_NAME"
  echo "pg_commit=$PG_SHA"
  echo "pgvector_commit=$VEC_SHA"
  echo "pg_configure=$($PGHOME/bin/pg_config --configure)"
  "$PGHOME/bin/psql" -p "$PGPORT" -d postgres -Atc 'select version();'
} > "$LOGDIR/manifest-basic.txt"
cat "$LOGDIR/manifest-basic.txt"

printf '\n\033[1m搭建完成。\033[0m 下一步：bash vecdiag/tools/verify_phenomena.sh 确认两个诊断对象可复现。\n'
printf '连接方式：%s/bin/psql -p %s -d postgres\n' "$PGHOME" "$PGPORT"