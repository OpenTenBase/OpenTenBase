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
# Exit code: 0 = every check that has failure detection passed,
#            1 = a psql connection/SQL error or a checked failure was detected
#
# Each failure prints "FAIL: stage=<name> psql_exit=<status>" on stderr, where
# <status> is the status of that psql invocation (2 = bad connection, 3 = SQL
# error under ON_ERROR_STOP, 127 = psql not found, ...).  On the 0 path the last
# line is a "PASS:" summary that states exactly what was verified.
#
# Known limits (deliberately not covered here):
#   [1] only prints the pgxc_ctl status; it does not fail the run yet.
#   [3] does not treat an empty pgxc_group result as a failure.
# ============================================================

PG_HOME=${PG_HOME:-/usr/local/opentenbase}
CN_PORT=${CN_PORT:-11003}
DB_USER=${DB_USER:-opentenbase}
DB_NAME=${DB_NAME:-postgres}
PGXC_CONF=${PGXC_CONF:-$HOME/pgxc_ctl/pgxc_ctl.conf}

export PATH="$PG_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$PG_HOME/lib:$LD_LIBRARY_PATH"

# -X: do not read the system psqlrc or ~/.psqlrc.  psql processes those files
#     after the -v assignments (src/bin/psql/startup.c: process_psqlrc() runs
#     before any command is executed), so a "\set ON_ERROR_STOP off" there
#     would silently turn SQL errors back into exit status 0.
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
if "${PSQL[@]}" -c "select node_name, node_type, node_port, node_host from pgxc_node order by node_type" 2>&1; then
    :   # rows already printed by psql above
else
    rc=$?
    echo "FAIL: stage=node-registration psql_exit=$rc" >&2
    exit 1
fi

echo ""
echo "===== [3] Node groups ====="
if "${PSQL[@]}" -c "select * from pgxc_group" 2>&1; then
    :   # rows already printed by psql above
else
    rc=$?
    echo "FAIL: stage=node-groups psql_exit=$rc" >&2
    exit 1
fi

echo ""
echo "===== [4] Distributed DDL/DML smoke test ====="
if [ "$1" = "--no-write" ]; then
    echo "(skipped: --no-write)"
    WRITE_CHECKED=0
elif "${PSQL[@]}" <<'SQL' 2>&1
drop table if exists otb_health_check;
create table otb_health_check(id int, name text)
    distribute by shard(id) to group default_group;
insert into otb_health_check values (1,'ok'),(2,'good');
select * from otb_health_check order by id;
drop table otb_health_check;
SQL
then
    WRITE_CHECKED=1
else
    rc=$?
    echo "FAIL: stage=ddl-dml psql_exit=$rc" >&2
    exit 1
fi

echo ""
if [ "$WRITE_CHECKED" -eq 1 ]; then
    echo "===== PASS: read-only catalog checks (pgxc_node, pgxc_group) and the DDL/DML smoke test succeeded. ====="
else
    echo "===== PASS: read-only catalog checks (pgxc_node, pgxc_group) succeeded; write capability NOT verified (--no-write). ====="
fi
exit 0
