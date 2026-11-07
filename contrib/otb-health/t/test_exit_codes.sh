#!/bin/bash
# ============================================================
# t/test_exit_codes.sh -- behavioural tests for ../otb_health.sh
# ------------------------------------------------------------
# Runs the health check against mock `psql` / `pgxc_ctl` commands injected
# through PATH, so no OpenTenBase cluster, VM, container or network access is
# required.
#
# The mocks reproduce the exit statuses documented for psql
# (doc/src/sgml/ref/psql-ref.sgml):
#   0 = finished normally, 1 = fatal error of its own,
#   2 = connection to the server went bad, 3 = script error + ON_ERROR_STOP.
# A script error therefore returns 0 unless ON_ERROR_STOP is set, which is the
# bug fixed in otb_health.sh.
#
# The mock psql also models psqlrc processing: without -X, psql loads
# $PSQLRC (or ~/.psqlrc) *after* the -v assignments
# (src/bin/psql/startup.c: process_psqlrc() runs before any command is
# executed), so a "\set ON_ERROR_STOP off" there re-enables error swallowing.
# With -X the file is never read.
#
# Usage:  bash t/test_exit_codes.sh
#         TARGET=/path/to/other.sh bash t/test_exit_codes.sh   # e.g. old version
# Exit code: 0 = all assertions passed, 1 = at least one failed.
# ============================================================

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TARGET=${TARGET:-$SCRIPT_DIR/../otb_health.sh}
BASH_BIN=${BASH:-$(command -v bash)}

if [ ! -f "$TARGET" ]; then
    echo "FATAL: cannot find $TARGET" >&2
    exit 1
fi
if [ -z "$BASH_BIN" ] || [ ! -x "$BASH_BIN" ]; then
    echo "FATAL: cannot find a usable bash to run the target" >&2
    exit 1
fi

LAB=$(mktemp -d "${TMPDIR:-/tmp}/otb-health-test.XXXXXX") || exit 1
# Normalize to the shell's own path form so that PATH entries and rm(1) work
# even when TMPDIR is a native Windows path (e.g. under Git Bash).
LAB=$(cd "$LAB" && pwd) || exit 1
trap 'rm -rf "$LAB"' EXIT
trap 'exit 130' INT TERM

BIN=$LAB/bin
EMPTY_BIN=$LAB/emptybin
HOME_PLAIN=$LAB/home-plain
HOME_PSQLRC=$LAB/home-psqlrc
LOG=$LAB/calls.log
RC_PLAIN=$LAB/conf/no-psqlrc-here
RC_OVERRIDE=$LAB/conf/psqlrc-override

mkdir -p "$BIN" "$EMPTY_BIN" "$HOME_PLAIN" "$HOME_PSQLRC" "$LAB/conf" "$LAB/no-pg-home"
: > "$LAB/conf/pgxc_ctl.conf"
printf '\\set ON_ERROR_STOP off\n' > "$RC_OVERRIDE"
# The same override placed in a throwaway HOME, to prove -X really ignores it.
printf '\\set ON_ERROR_STOP off\n' > "$HOME_PSQLRC/.psqlrc"

# --- mock psql ------------------------------------------------------------
cat > "$BIN/psql" <<'MOCK_PSQL'
#!/bin/bash
# Mock psql: distinguishes "-c <query>" (command mode) from stdin (script mode),
# honours -X, and lets a psqlrc file override ON_ERROR_STOP like a real psql.
on_error_stop=0
command_mode=0
no_psqlrc=0
sql=""

while [ "$#" -gt 0 ]; do
    arg=$1
    shift
    case "$arg" in
        -X|--no-psqlrc)
            no_psqlrc=1
            ;;
        -v|--set)
            if [ "$#" -gt 0 ]; then
                case "$1" in
                    ON_ERROR_STOP=1)   on_error_stop=1 ;;
                    ON_ERROR_STOP=off|ON_ERROR_STOP=0) on_error_stop=0 ;;
                esac
                shift
            fi
            ;;
        -c|--command)
            if [ "$#" -gt 0 ]; then
                command_mode=1
                sql=$1
                shift
            fi
            ;;
        -h|--host|-p|--port|-U|--username|-d|--dbname)
            [ "$#" -gt 0 ] && shift
            ;;
    esac
done

# psqlrc is processed after the -v assignments unless -X was given.
rc_file=${PSQLRC:-$HOME/.psqlrc}
if [ "$no_psqlrc" -eq 0 ] && [ -n "$rc_file" ] && [ -f "$rc_file" ]; then
    if [ "${MOCK_RC_HONOUR:-1}" = "1" ] && grep -qi 'on_error_stop' "$rc_file" &&
       grep -qi 'off' "$rc_file"; then
        on_error_stop=0
    fi
fi

if [ -n "${MOCK_LOG:-}" ]; then
    if [ "$command_mode" -eq 1 ]; then
        echo "psql:cmd" >> "$MOCK_LOG"
    else
        echo "psql:stdin" >> "$MOCK_LOG"
    fi
fi

case "${MOCK_FAIL:-none}" in
    conn)
        echo "psql: error: could not connect to server: Connection refused" >&2
        exit 2
        ;;
esac

if [ "$command_mode" -eq 1 ]; then
    case "$sql" in
        *pgxc_node*)
            if [ "${MOCK_FAIL:-none}" = "node" ]; then
                echo 'ERROR:  relation "pgxc_node" does not exist' >&2
                exit 1
            fi
            printf '%s\n' \
                ' node_name | node_type | node_port | node_host' \
                '-----------+-----------+-----------+-----------' \
                ' gtm       | G         |      6666 | 127.0.0.1' \
                ' cn001     | C         |     11003 | 127.0.0.1' \
                '(2 rows)' ''
            exit 0
            ;;
        *pgxc_group*)
            if [ "${MOCK_FAIL:-none}" = "group" ]; then
                echo 'ERROR:  permission denied for relation pgxc_group' >&2
                exit 1
            fi
            printf '%s\n' \
                ' oid   | grpname       | node_names' \
                '-------+---------------+-------------' \
                ' 16385 | default_group | {cn001,dn001}' \
                '(1 row)' ''
            exit 0
            ;;
    esac
    echo "mock psql: unexpected -c query: $sql" >&2
    exit 1
fi

# script mode: consume the whole heredoc
cat > /dev/null

if [ "${MOCK_FAIL:-none}" = "ddl" ]; then
    echo 'ERROR:  distribute group "no_such_group" does not exist' >&2
    if [ "$on_error_stop" -eq 1 ]; then
        exit 3
    fi
    echo '(psql: statements after the error were skipped, exit status stays 0)' >&2
    exit 0
fi

printf '%s\n' 'DROP TABLE' 'CREATE TABLE' \
    ' id | name' '----+------' '  1 | ok' '  2 | good' '(2 rows)' '' 'DROP TABLE'
exit 0
MOCK_PSQL

# --- mock pgxc_ctl --------------------------------------------------------
cat > "$BIN/pgxc_ctl" <<'MOCK_PGXC'
#!/bin/bash
[ -n "${MOCK_LOG:-}" ] && echo "pgxc_ctl" >> "$MOCK_LOG"
cat > /dev/null
printf '%s\n' 'gtm_master: Running' 'cn001: Running' 'dn001: Running'
exit 0
MOCK_PGXC

chmod +x "$BIN/psql" "$BIN/pgxc_ctl"

# --- assertions -----------------------------------------------------------
PASS=0
FAIL=0
OUT=""
RC=0

pass() { PASS=$((PASS + 1)); printf 'PASS  %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); printf 'FAIL  %s\n' "$1"; }

assert_rc_eq() {
    if [ "$2" -eq "$3" ]; then
        pass "$1"
    else
        fail "$1 (expected exit $2, got $3)"
    fi
}

assert_rc_nonzero() {
    if [ "$2" -ne 0 ]; then
        pass "$1"
    else
        fail "$1 (expected non-zero exit, got 0)"
    fi
}

assert_count_eq() {
    if [ "$2" -eq "$3" ]; then
        pass "$1"
    else
        fail "$1 (expected count $2, got $3)"
    fi
}

assert_contains() {
    case "$2" in
        *"$3"*) pass "$1" ;;
        *) fail "$1 (output does not contain: $3)" ;;
    esac
}

assert_not_contains() {
    case "$2" in
        *"$3"*) fail "$1 (output unexpectedly contains: $3)" ;;
        *) pass "$1" ;;
    esac
}

# number of psql invocations recorded in the call log (pgxc_ctl also logs there)
count_psql() {
    if [ -s "$LOG" ]; then
        local n
        n=$(grep -c '^psql:' "$LOG" 2>/dev/null) || n=0
        echo "${n:-0}"
    else
        echo 0
    fi
}

# number of log lines matching a prefix, e.g. "psql:stdin"
count_matching() {
    if [ -s "$LOG" ]; then
        local n
        n=$(grep -c "^$1" "$LOG" 2>/dev/null) || n=0
        echo "${n:-0}"
    else
        echo 0
    fi
}

# number of final "PASS:" summary lines in the captured output
count_pass() {
    local n
    n=$(printf '%s\n' "$1" | grep -c 'PASS: ') || n=0
    echo "${n:-0}"
}

# run_target <mock-failure-mode> [script args...]
# Honours CASE_HOME / CASE_PATH / CASE_PGXC_CONF for the environmental cases.
run_target() {
    local mode=$1
    shift
    : > "$LOG"
    OUT=$(
        export PATH="${CASE_PATH:-$BIN:$PATH}"
        export MOCK_FAIL="$mode"
        export MOCK_LOG="$LOG"
        export PSQLRC=""
        export HOME="${CASE_HOME:-$HOME_PLAIN}"
        export PG_HOME="$LAB/no-pg-home"
        export PGXC_CONF="${CASE_PGXC_CONF-$LAB/conf/pgxc_ctl.conf}"
        export CN_PORT=11003
        export DB_USER=opentenbase
        export DB_NAME=postgres
        "$BASH_BIN" "$TARGET" "$@" 2>&1
    )
    RC=$?
}

# Direct mock invocation through stdin, used to pin the mock's own fidelity.
MOCK_PSQLRC=""
run_mock_psql_stdin() {
    local mode=$1
    shift
    printf 'create table t(i int);\n' | \
        MOCK_FAIL="$mode" PSQLRC="${MOCK_PSQLRC:-$RC_PLAIN}" MOCK_LOG="" \
        "$BIN/psql" "$@" >/dev/null 2>&1
    return $?
}

echo "target: $TARGET"
echo "lab   : $LAB"
echo ""

# --- case 1: healthy cluster ---------------------------------------------
run_target none
assert_rc_eq "healthy cluster exits 0" 0 "$RC"
assert_count_eq "healthy cluster prints the PASS summary exactly once" 1 "$(count_pass "$OUT")"
assert_contains "write-mode summary states the DDL/DML smoke test passed" \
    "$OUT" "DDL/DML smoke test succeeded"
assert_not_contains "write-mode summary does not disclaim the write test" \
    "$OUT" "NOT verified"
assert_count_eq "healthy cluster makes 3 psql calls" 3 "$(count_psql)"
assert_count_eq "healthy cluster runs 2 command-mode queries" 2 "$(count_matching 'psql:cmd')"
assert_count_eq "healthy cluster runs 1 script-mode call" 1 "$(count_matching 'psql:stdin')"

# --- case 2: psql cannot connect -----------------------------------------
run_target conn
assert_rc_nonzero "connection failure exits non-zero" "$RC"
assert_count_eq "connection failure prints no PASS summary" 0 "$(count_pass "$OUT")"
assert_contains "connection failure reports psql's own status (2)" \
    "$OUT" "FAIL: stage=node-registration psql_exit=2"
assert_count_eq "connection failure stops after the first call" 1 "$(count_psql)"

# --- case 3: pgxc_node query fails (fail-fast) ---------------------------
run_target node
assert_rc_nonzero "pgxc_node failure exits non-zero" "$RC"
assert_count_eq "pgxc_node failure prints no PASS summary" 0 "$(count_pass "$OUT")"
assert_contains "pgxc_node failure reports stage and psql exit status" \
    "$OUT" "FAIL: stage=node-registration psql_exit=1"
assert_count_eq "pgxc_node failure makes exactly 1 psql call" 1 "$(count_psql)"

# --- case 4: pgxc_group query fails (fail-fast) --------------------------
run_target group
assert_rc_nonzero "pgxc_group failure exits non-zero" "$RC"
assert_count_eq "pgxc_group failure prints no PASS summary" 0 "$(count_pass "$OUT")"
assert_contains "pgxc_group failure reports stage and psql exit status" \
    "$OUT" "FAIL: stage=node-groups psql_exit=1"
assert_count_eq "pgxc_group failure makes exactly 2 psql calls" 2 "$(count_psql)"

# --- case 5: SQL error inside the DDL/DML heredoc ------------------------
run_target ddl
assert_rc_nonzero "DDL/DML SQL error exits non-zero" "$RC"
assert_count_eq "DDL/DML SQL error prints no PASS summary" 0 "$(count_pass "$OUT")"
assert_contains "DDL/DML SQL error reports stage and psql exit status (3)" \
    "$OUT" "FAIL: stage=ddl-dml psql_exit=3"
assert_count_eq "DDL/DML SQL error makes exactly 3 psql calls" 3 "$(count_psql)"
assert_count_eq "DDL/DML SQL error leaves no further SQL call" 1 "$(count_matching 'psql:stdin')"

# --- case 6: --no-write skips the write test -----------------------------
run_target none --no-write
assert_rc_eq "--no-write on a healthy cluster exits 0" 0 "$RC"
assert_contains "--no-write is announced" "$OUT" "skipped: --no-write"
assert_count_eq "--no-write prints the PASS summary exactly once" 1 "$(count_pass "$OUT")"
assert_contains "--no-write summary says write capability was not verified" \
    "$OUT" "write capability NOT verified"
assert_not_contains "--no-write summary does not claim the smoke test passed" \
    "$OUT" "DDL/DML smoke test succeeded"
assert_count_eq "--no-write makes only the 2 read-only psql calls" 2 "$(count_psql)"
assert_count_eq "--no-write never runs the heredoc" 0 "$(count_matching 'psql:stdin')"

run_target ddl --no-write
assert_rc_eq "--no-write skips a broken DDL/DML test" 0 "$RC"
assert_count_eq "--no-write does not reach the failing write call" 2 "$(count_psql)"
assert_contains "--no-write still reports the read-only result honestly" \
    "$OUT" "write capability NOT verified"

# --- case 7: psqlrc must not be able to re-enable error swallowing -------
# HOME_PSQLRC contains "\\set ON_ERROR_STOP off". Without -X the mock applies
# it after -v and the heredoc error would exit 0 again.
CASE_HOME=$HOME_PSQLRC
run_target ddl
assert_rc_nonzero "psqlrc override cannot silence a SQL error (-X in place)" "$RC"
assert_count_eq "psqlrc override still prints no PASS summary" 0 "$(count_pass "$OUT")"
CASE_HOME=""

MOCK_PSQLRC=$RC_OVERRIDE
run_mock_psql_stdin ddl -v ON_ERROR_STOP=1
assert_rc_eq "mock: psqlrc overrides ON_ERROR_STOP without -X" 0 "$?"
run_mock_psql_stdin ddl -X -v ON_ERROR_STOP=1
assert_rc_eq "mock: -X ignores psqlrc, error still returns 3" 3 "$?"
MOCK_PSQLRC=""

# --- case 8: psql binary missing -----------------------------------------
# PATH is an empty directory, so a psql installed on the host can never be
# picked up by accident.
CASE_PATH=$EMPTY_BIN
CASE_PGXC_CONF=$LAB/conf/absent-pgxc_ctl.conf
run_target none
assert_rc_nonzero "missing psql exits non-zero" "$RC"
assert_count_eq "missing psql prints no PASS summary" 0 "$(count_pass "$OUT")"
assert_contains "missing psql reports the shell's not-found status (127)" \
    "$OUT" "FAIL: stage=node-registration psql_exit=127"
assert_count_eq "missing psql makes no psql call at all" 0 "$(count_psql)"
CASE_PATH=""
CASE_PGXC_CONF=""

# --- case 9: no pgxc_ctl.conf --------------------------------------------
CASE_PGXC_CONF=$LAB/conf/absent-pgxc_ctl.conf
run_target none
assert_rc_eq "missing pgxc_ctl.conf still exits 0 when SQL checks pass" 0 "$RC"
assert_contains "missing pgxc_ctl.conf is announced as skipped" "$OUT" "(no pgxc_ctl.conf"
assert_count_eq "missing pgxc_ctl.conf still prints one PASS summary" 1 "$(count_pass "$OUT")"
assert_count_eq "missing pgxc_ctl.conf never invokes pgxc_ctl" 0 "$(count_matching 'pgxc_ctl')"
assert_count_eq "missing pgxc_ctl.conf still runs the 3 SQL checks" 3 "$(count_psql)"
CASE_PGXC_CONF=""

# --- case 10: ON_ERROR_STOP mock fidelity --------------------------------
# Case 5 only fails because the mock returns 0 without the flag; these pin it.
MOCK_PSQLRC=""
run_mock_psql_stdin ddl
assert_rc_eq "mock: script error without ON_ERROR_STOP returns 0" 0 "$?"
run_mock_psql_stdin ddl -v ON_ERROR_STOP=1
assert_rc_eq "mock: script error with ON_ERROR_STOP returns 3" 3 "$?"

echo ""
echo "===== $PASS passed, $FAIL failed ====="

[ "$FAIL" -eq 0 ] || exit 1
exit 0
