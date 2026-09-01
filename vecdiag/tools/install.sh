#!/usr/bin/env bash
# vecdiag · 安装/重装 SQL 层
#
# 幂等：所有对象都是 create or replace / if not exists。
# 用法：PGHOME=/data/pg18/install PGPORT=5518 bash tools/install.sh
set -euo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
SQLDIR=$(cd "$(dirname "$0")/../sql" && pwd)
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=1"

echo ">>> 目标实例"
$PSQL -Atc 'select version();'
$PSQL -Atc "select 'pgvector '||extversion from pg_extension where extname='vector';" \
  || { echo "[FAIL] 未安装 vector 扩展，先 create extension vector;" >&2; exit 1; }

for f in "$SQLDIR"/[0-9]*.sql; do
  echo ">>> 应用 $(basename "$f")"
  $PSQL -f "$f"
done

echo ">>> 已注册的函数"
$PSQL -c "\df vecdiag.*"

echo ">>> ABI 常数（换机器必须重测，见 tools/abi_probe.sh）"
$PSQL -c "select key, value, source, source_ref from vecdiag.abi_const order by key;"

echo "安装完成。"
