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
#   [4] distributed DDL/DML loop   (create / insert / select / rollback)
#
# Exit code: 0 = requested SQL checks succeeded, 1 = a SQL check failed.
# The process monitor is informational.
# ============================================================

PG_HOME=${PG_HOME:-/usr/local/opentenbase}
CN_PORT=${CN_PORT:-11003}
DB_USER=${DB_USER:-opentenbase}
DB_NAME=${DB_NAME:-postgres}
PGXC_CONF=${PGXC_CONF:-$HOME/pgxc_ctl/pgxc_ctl.conf}

export PATH="$PG_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$PG_HOME/lib:$LD_LIBRARY_PATH"

PSQL=(psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$CN_PORT" -U "$DB_USER" -d "$DB_NAME")

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
    || { echo "FATAL: node registration query failed on CN port $CN_PORT" >&2; exit 1; }

echo ""
echo "===== [3] Node groups ====="
"${PSQL[@]}" -c "select * from pgxc_group" 2>&1 \
    || { echo "FATAL: node group query failed" >&2; exit 1; }

echo ""
echo "===== [4] Distributed DDL/DML smoke test ====="
if [ "$1" = "--no-write" ]; then
    echo "(skipped: --no-write)"
else
    # Use a per-run name and roll back the smoke test so it leaves no table behind.
    if ! "${PSQL[@]}" -v "health_table=otb_health_check_${BASHPID}_${RANDOM}" <<'SQL' 2>&1
begin;
create table :"health_table"(id int, name text)
    distribute by shard(id) to group default_group;
insert into :"health_table" values (1,'ok'),(2,'good');
select * from :"health_table" order by id;
rollback;
SQL
    then
        echo "FATAL: distributed DDL/DML smoke test failed" >&2
        exit 1
    fi
fi

echo ""
echo "===== Requested SQL checks passed. Review process and catalog output above. ====="
