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
#   [1] cluster processes          (pgxc_ctl monitor all, diagnostic only)
#   [2] node registration          (pgxc_node)
#   [3] node groups                (pgxc_group)
#   [4] distributed DDL/DML loop   (create / insert / select / drop)
#
# Exit code: 0 = all required checks passed (any skipped section is
#                marked in the output),
#            1 = at least one required check failed (connection or SQL)
# ============================================================

PG_HOME=${PG_HOME:-/usr/local/opentenbase}
CN_PORT=${CN_PORT:-11003}
DB_USER=${DB_USER:-opentenbase}
DB_NAME=${DB_NAME:-postgres}
PGXC_CONF=${PGXC_CONF:-$HOME/pgxc_ctl/pgxc_ctl.conf}

export PATH="$PG_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$PG_HOME/lib:$LD_LIBRARY_PATH"

# ON_ERROR_STOP makes psql reflect SQL errors in its exit status, so a
# failed statement can fail this script instead of being silently
# swallowed (psql exits 0 on SQL errors without it).
PSQL=(psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$CN_PORT" -U "$DB_USER" -d "$DB_NAME")

echo "===== [1] Cluster processes ====="
if [ -f "$PGXC_CONF" ]; then
    printf 'monitor all\n' | pgxc_ctl -c "$PGXC_CONF" 2>&1 \
        | grep -E "Running|not running|Stopped" || echo "(pgxc_ctl returned nothing)"
else
    echo "(no pgxc_ctl.conf at $PGXC_CONF -- skipping)"
fi

echo ""
echo "===== [2] Node registration ====="
if ! "${PSQL[@]}" -c "select node_name, node_type, node_port, node_host from pgxc_node order by node_type" 2>&1; then
    echo "FATAL: node registration query failed on port $CN_PORT (unreachable or SQL error)" >&2
    exit 1
fi

echo ""
echo "===== [3] Node groups ====="
if ! "${PSQL[@]}" -c "select * from pgxc_group" 2>&1; then
    echo "FAIL: node group query failed" >&2
    exit 1
fi

echo ""
echo "===== [4] Distributed DDL/DML smoke test ====="
smoke_skipped=0
if [ "${1-}" = "--no-write" ]; then
    echo "(skipped: --no-write)"
    smoke_skipped=1
else
    if ! "${PSQL[@]}" <<'SQL' 2>&1
drop table if exists otb_health_check;
create table otb_health_check(id int, name text)
    distribute by shard(id) to group default_group;
insert into otb_health_check values (1,'ok'),(2,'good');
select * from otb_health_check order by id;
drop table otb_health_check;
SQL
    then
        echo "FAIL: distributed DDL/DML smoke test" >&2
        exit 1
    fi
fi

echo ""
echo "===== Health check finished. All green above = cluster usable. ====="
if [ "$smoke_skipped" = "1" ]; then
    echo "Note: smoke test was skipped (--no-write); read-only checks only."
fi
