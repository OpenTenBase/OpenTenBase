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
#   [1] cluster processes          (pgxc_ctl monitor all -- diagnostic only,
#                                   its output does not affect the exit code)
#   [2] node registration          (pgxc_node)
#   [3] node groups                (pgxc_group)
#   [4] distributed DDL/DML loop   (create / insert / select / drop)
#
# Exit code: 0 = all required SQL checks passed (sections skipped on
#                purpose are marked SKIP in the output),
#            1 = at least one required SQL check failed (connection or SQL)
#
# psql is always invoked with -X (ignore .psqlrc / $PSQLRC startup files)
# and -v ON_ERROR_STOP=1, so neither a startup file nor a multi-statement
# stdin script can hide a SQL error from the exit status.
# ============================================================

PG_HOME=${PG_HOME:-/usr/local/opentenbase}
CN_PORT=${CN_PORT:-11003}
DB_USER=${DB_USER:-opentenbase}
DB_NAME=${DB_NAME:-postgres}
PGXC_CONF=${PGXC_CONF:-$HOME/pgxc_ctl/pgxc_ctl.conf}

export PATH="$PG_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$PG_HOME/lib:$LD_LIBRARY_PATH"

# -X: do not read startup files, which could otherwise reset ON_ERROR_STOP.
# -v ON_ERROR_STOP=1: make psql report SQL errors through its exit status.
# (A stdin script without it keeps going after an error and can exit 0;
# a single -c statement fails with status 1 regardless, so both modes are
# checked explicitly below.)
PSQL=(psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$CN_PORT" -U "$DB_USER" -d "$DB_NAME")

echo "===== [1] Cluster processes (diagnostic only, not part of the exit status) ====="
if [ -f "$PGXC_CONF" ]; then
    printf 'monitor all\n' | pgxc_ctl -c "$PGXC_CONF" 2>&1 \
        | grep -E "Running|not running|Stopped" || echo "(pgxc_ctl returned nothing)"
else
    echo "SKIP: no pgxc_ctl.conf at $PGXC_CONF"
fi

echo ""
echo "===== [2] Node registration ====="
"${PSQL[@]}" -c "select node_name, node_type, node_port, node_host from pgxc_node order by node_type" 2>&1
rc=$?
if [ "$rc" -ne 0 ]; then
    echo "FAIL: stage=node-registration psql_exit=$rc" >&2
    exit 1
fi

echo ""
echo "===== [3] Node groups ====="
"${PSQL[@]}" -c "select * from pgxc_group" 2>&1
rc=$?
if [ "$rc" -ne 0 ]; then
    echo "FAIL: stage=node-groups psql_exit=$rc" >&2
    exit 1
fi

echo ""
echo "===== [4] Distributed DDL/DML smoke test ====="
if [ "${1-}" = "--no-write" ]; then
    echo "SKIP: write test disabled by --no-write"
    echo ""
    echo "PASS: read-only SQL queries completed; write capability was not tested."
else
    "${PSQL[@]}" <<'SQL' 2>&1
drop table if exists otb_health_check;
create table otb_health_check(id int, name text)
    distribute by shard(id) to group default_group;
insert into otb_health_check values (1,'ok'),(2,'good');
select * from otb_health_check order by id;
drop table otb_health_check;
SQL
    rc=$?
    if [ "$rc" -ne 0 ]; then
        echo "FAIL: stage=smoke psql_exit=$rc" >&2
        exit 1
    fi
    echo ""
    echo "PASS: required SQL checks completed; distributed DDL/DML smoke test passed."
fi
