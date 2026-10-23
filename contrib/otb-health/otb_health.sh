#!/bin/bash
# ============================================================
# otb-health -- one-shot health check for an OpenTenBase cluster
# ------------------------------------------------------------
# Verified on: OpenTenBase v5.0 (PostgreSQL 10.0 @ OpenTenBase_v5.0)
#              single-node minimal cluster: 1 GTM + 1 CN + 1 DN
#              Ubuntu 24.04, install prefix /usr/local/opentenbase
#
# Usage:  bash otb_health.sh
#         bash otb_health.sh --no-write     # skip the DDL smoke test
#
# What it checks, in order:
#   [1] cluster processes          (pgxc_ctl monitor all)
#   [2] node registration          (pgxc_node)
#   [3] node groups                (pgxc_group)
#   [4] distributed DDL/DML loop   (create / insert / select / drop)
#
# Exit code: 0 = all four sections produced output, 1 = psql unreachable
# ============================================================

PG_HOME=${PG_HOME:-/usr/local/opentenbase}
CN_PORT=${CN_PORT:-11003}
DB_USER=${DB_USER:-opentenbase}
DB_NAME=${DB_NAME:-postgres}
PGXC_CONF=${PGXC_CONF:-$HOME/pgxc_ctl/pgxc_ctl.conf}

export PATH="$PG_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$PG_HOME/lib:$LD_LIBRARY_PATH"

PSQL=(psql -h 127.0.0.1 -p "$CN_PORT" -U "$DB_USER" -d "$DB_NAME")

echo "===== [1] Cluster processes ====="
if [ -f "$PGXC_CONF" ]; then
    printf 'monitor all\n' | pgxc_ctl -c "$PGXC_CONF" 2>&1 \
        | grep -E "Running|not running|Stopped" || echo "(pgxc_ctl returned nothing)"
else
    echo "(no pgxc_ctl.conf at $PGXC_CONF -- skipping)"
fi

echo ""
echo "===== [2] Node registration ====="
"${PSQL[@]}" -c "select node_name, node_type, node_port, node_host from pgxc_node order by node_type" 2>&1 \
    || { echo "FATAL: cannot reach CN on port $CN_PORT"; exit 1; }

echo ""
echo "===== [3] Node groups ====="
"${PSQL[@]}" -c "select * from pgxc_group" 2>&1

echo ""
echo "===== [4] Distributed DDL/DML smoke test ====="
if [ "$1" = "--no-write" ]; then
    echo "(skipped: --no-write)"
else
    "${PSQL[@]}" <<'SQL' 2>&1
drop table if exists otb_health_check;
create table otb_health_check(id int, name text)
    distribute by shard(id) to group default_group;
insert into otb_health_check values (1,'ok'),(2,'good');
select * from otb_health_check order by id;
drop table otb_health_check;
SQL
fi

echo ""
echo "===== Health check finished. All green above = cluster usable. ====="
