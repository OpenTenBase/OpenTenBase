#!/usr/bin/env bash
# 一键搭建：PG18(REL_18_STABLE) + pgvector 0.8.6，端口 5518。
# 幂等：已存在的步骤会跳过。失败即退出并打印日志尾部。
# 用法（可直接用 root 调用，脚本会自己降权）：
#   bash 01_bootstrap.sh 2>&1 | tee /data/artifacts/bootstrap.log
set -euo pipefail

ROOT=${ROOT:-/data/pg18}
SRC="$ROOT/src"
PGHOME="$ROOT/install"
PGDATA_DIR="$ROOT/data"
PGPORT=${PGPORT:-5518}
JOBS=${JOBS:-4}
LOGDIR=${LOGDIR:-/data/artifacts/bootstrap}
PG_BRANCH=REL_18_STABLE
VEC_TAG=v0.8.6
RUNUSER=${RUNUSER:-postgres}

step() { printf '\n\033[1m>>> %s\033[0m\n' "$1"; }
die()  { printf '\n[FAIL] %s\n' "$1" >&2; [ -n "${2:-}" ] && tail -20 "$2" >&2; exit 1; }

# PostgreSQL 硬规则：initdb 与 postmaster 拒绝以 root 运行
#   initdb: error: cannot be run as root
# 所以即使用 root 登录，实际跑实例的必须是非 root 用户。这里自动降权到 $RUNUSER，
# 免得开发人员卡在这一步或者去搜"怎么让 PG 以 root 运行"（没有这种办法）。
if [ "$(id -u)" -eq 0 ]; then
  getent passwd "$RUNUSER" >/dev/null 2>&1 || useradd -m -s /bin/bash "$RUNUSER"
  mkdir -p "$ROOT" "$LOGDIR" /data/datasets /data/artifacts
  chown -R "$RUNUSER:$RUNUSER" "$ROOT" /data/artifacts /data/datasets
  SELF=$(readlink -f "$0")
  chmod a+r "$SELF"
  step "检测到以 root 运行：改用 $RUNUSER 身份继续（PG 不允许 root 跑实例）"
  exec su - "$RUNUSER" -c \
    "ROOT='$ROOT' PGPORT='$PGPORT' JOBS='$JOBS' LOGDIR='$LOGDIR' RUNUSER='$RUNUSER' bash '$SELF'"
fi

mkdir -p "$SRC" "$PGHOME" "$LOGDIR" /data/datasets /data/artifacts
echo "运行身份：$(id -un)（uid $(id -u)）"

step "切换到 devtoolset-11（GCC 11）"
[ -f /opt/rh/devtoolset-11/enable ] || die "缺少 devtoolset-11，请先 yum install devtoolset-11-gcc devtoolset-11-gcc-c++"
# shellcheck disable=SC1091
source /opt/rh/devtoolset-11/enable
gcc --version | head -1

step "拉取 PostgreSQL 基线源码（$PG_BRANCH）"
if [ ! -d "$SRC/OpenTenBase/.git" ]; then
  git clone --branch "$PG_BRANCH" --single-branch --depth 50 \
    https://github.com/OpenTenBase/OpenTenBase.git "$SRC/OpenTenBase" \
    > "$LOGDIR/clone_pg.log" 2>&1 || die "clone 失败" "$LOGDIR/clone_pg.log"
fi
# 注意：本机 git 是 1.8.3.1，**不支持 `git -C <dir>`**（该选项 1.8.5 才加入），
# 所以下面统一用子 shell cd 的写法。改脚本时不要顺手写回 git -C。
PG_SHA=$(cd "$SRC/OpenTenBase" && git rev-parse HEAD)
echo "PG commit: $PG_SHA"
grep -m1 AC_INIT "$SRC/OpenTenBase/configure.ac"

step "确认基线是纯社区 PG（无 gtm、contrib 无 vector）"
[ "$(ls "$SRC/OpenTenBase/src" | grep -c '^gtm$')" = "0" ] || die "src/gtm 存在，分支拉错"
[ "$(ls "$SRC/OpenTenBase/contrib" | grep -ci vector)" = "0" ] || die "contrib 含 vector，分支拉错"

step "编译安装 PostgreSQL（约 15-25 分钟）"
# CentOS 7 自带 OpenSSL 1.0.2k，而 PG18 的 configure 要求 >= 1.1.1
#   configure: error: OpenSSL version >= 1.1.1 is required for SSL support
# 解决方式：用 EPEL 的 openssl11（1.1.1k），头文件与库在独立前缀下，需显式指路。
# 若不想装包，也可以直接去掉 --with-openssl（本项目全程本地连接，不需要 SSL），
# 但那样 pg_config --configure 会与常见生产构建不同，报告里要说明。
SSL_CPPFLAGS=""
SSL_LDFLAGS=""
if [ -d /usr/include/openssl11 ] && [ -d /usr/lib64/openssl11 ]; then
  SSL_CPPFLAGS="-I/usr/include/openssl11"
  SSL_LDFLAGS="-L/usr/lib64/openssl11"
  echo "使用 openssl11：$SSL_CPPFLAGS $SSL_LDFLAGS"
else
  echo "[提示] 未找到 openssl11，请先以 root 执行：yum -y install openssl11-devel"
fi
if [ ! -x "$PGHOME/bin/pg_config" ]; then
  cd "$SRC/OpenTenBase"
  ./configure --prefix="$PGHOME" --with-openssl --with-icu --with-readline \
      --enable-tap-tests \
      CPPFLAGS="$SSL_CPPFLAGS" LDFLAGS="$SSL_LDFLAGS" \
      > "$LOGDIR/configure.log" 2>&1 || die "configure 失败" "$LOGDIR/configure.log"
  make -j"$JOBS" > "$LOGDIR/make.log" 2>&1 || die "make 失败" "$LOGDIR/make.log"
  make install > "$LOGDIR/make_install.log" 2>&1 || die "make install 失败" "$LOGDIR/make_install.log"
fi
"$PGHOME/bin/pg_config" --version

step "安装 contrib/pg_stat_statements"
# postgresql.conf 里写了 shared_preload_libraries='pg_stat_statements'，
# 而核心 make install 不包含 contrib。缺这个库实例根本起不来：
#   FATAL: could not access file "pg_stat_statements": No such file or directory
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

# ---- project2 baseline (host: 3788MB RAM + 2GB swap, 4 vCPU) ----
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
      || die "启动失败" "$PGDATA_DIR/startup.log"
  sleep 2
fi
"$PGHOME/bin/psql" -p "$PGPORT" -d postgres -Atc 'select version();'

step "拉取并编译 pgvector $VEC_TAG"
if [ ! -d "$SRC/pgvector/.git" ]; then
  git clone --branch "$VEC_TAG" --single-branch --depth 1 \
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
  echo "pg_commit=$PG_SHA"
  echo "pgvector_commit=$VEC_SHA"
  echo "pg_configure=$($PGHOME/bin/pg_config --configure)"
  "$PGHOME/bin/psql" -p "$PGPORT" -d postgres -Atc 'select version();'
} > "$LOGDIR/manifest-basic.txt"
cat "$LOGDIR/manifest-basic.txt"

printf '\n\033[1m搭建完成。\033[0m 下一步：bash 02_verify_phenomena.sh 确认两个诊断对象可复现。\n'
printf '连接方式：%s/bin/psql -p %s -d postgres\n' "$PGHOME" "$PGPORT"
