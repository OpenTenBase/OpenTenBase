#!/bin/bash
# ============================================================
# test_otb_health.sh -- black-box contract tests for otb_health.sh
# ------------------------------------------------------------
# No external database required: a fake `psql` is installed into a
# temporary PG_HOME/bin, and the script under test runs against it.
# The fake records argv, call order and heredoc stdin, and only
# simulates a SQL error when it detects `-v ON_ERROR_STOP=1` -- so a
# regression that drops ON_ERROR_STOP is caught (psql exits 0 on SQL
# errors without it).
#
# Usage:
#   bash test_otb_health.sh              # test the fixed script
#   EXPECT_BASELINE=1 bash test_otb_health.sh
#                                        # re-run against the OLD
#                                        # (pre-fix) script: U04-U07
#                                        # are expected to REPRODUCE
#                                        # the bug (exit 0 + success
#                                        # banner despite SQL errors)
#
# Environment overrides:
#   SCRIPT_UNDER_TEST=<path>   script to test
#                              (default: ../otb_health.sh)
# ============================================================
set -u

SCRIPT_UNDER_TEST="${SCRIPT_UNDER_TEST:-$(cd "$(dirname "$0")/.." && pwd)/otb_health.sh}"
EXPECT_BASELINE="${EXPECT_BASELINE:-0}"
WORK_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/otb_health_test.XXXXXX")
trap 'rm -rf "$WORK_ROOT"' EXIT

pass=0
fail=0

ok()   { pass=$((pass+1)); echo "  ok  : $1"; }
bad()  { fail=$((fail+1)); echo "  FAIL: $1"; }

# assert_contains <haystack-var-name> <needle> <what>
assert_contains() {
    if grep -qF -- "$2" "$1"; then ok "$3"; else bad "$3 (missing: $2)"; fi
}
assert_not_contains() {
    if grep -qF -- "$2" "$1"; then bad "$3 (unexpected: $2)"; else ok "$3"; fi
}

make_fake_psql() {
    local d="$1"
    mkdir -p "$d/bin"
    cat > "$d/bin/psql" <<'FAKE'
#!/bin/bash
# Test stub psql. Env:
#   FAKE_DIR       per-case state dir (call log)
#   FAKE_FAIL_AT   Nth psql call fails (1=registration, 2=groups, 3=smoke)
#   FAKE_FAIL_MODE conn | sql
#   FAKE_FAIL_STMT keyword that must appear in the heredoc stdin for the
#                  failure to trigger (create|insert|select); empty = any
d=${FAKE_DIR:?}
n=$(cat "$d/count" 2>/dev/null || echo 0); n=$((n+1)); echo "$n" > "$d/count"
if [ ! -t 0 ] && [ -r /dev/stdin ]; then cat > "$d/stdin.$n" 2>/dev/null || true; fi
{
    echo "call $n args: $*"
    if [ -f "$d/stdin.$n" ]; then
        echo "call $n stdin: $(tr '\n' ' ' < "$d/stdin.$n")"
    fi
} >> "$d/log"
oes=0
a=("$@")
for i in "${!a[@]}"; do
    if [ "${a[$i]}" = "-v" ] && [ "${a[$((i+1))]:-}" = "ON_ERROR_STOP=1" ]; then
        oes=1
    fi
done
echo "call $n oes=$oes" >> "$d/log"
fail_at=${FAKE_FAIL_AT:-0}
fail_mode=${FAKE_FAIL_MODE:-sql}
stmt=${FAKE_FAIL_STMT:-}
hit=0
if [ "$n" -eq "$fail_at" ] 2>/dev/null; then hit=1; fi
if [ "$fail_at" -eq 3 ] && [ -n "$stmt" ] && [ -f "$d/stdin.$n" ] \
   && tr 'A-Z' 'a-z' < "$d/stdin.$n" | grep -q "$stmt"; then
    hit=1
fi
if [ "$hit" = "1" ]; then
    if [ "$fail_mode" = "conn" ]; then
        echo "psql: could not connect to server: Connection refused"
        echo "Is the server running on host \"127.0.0.1\" and accepting"
        echo "connections on TCP/IP port?"
        exit 2
    fi
    echo 'ERROR:  relation "no_such_group" does not exist (simulated by test stub)'
    # Real psql: SQL error -> exit 3 only when ON_ERROR_STOP is set,
    # otherwise it keeps going and exits 0.
    if [ "$oes" = "1" ]; then exit 3; else exit 0; fi
fi
case "$n" in
    1) printf ' node_name | node_type   | node_port | node_host\n'
       printf '-----------+-------------+-----------+-----------\n'
       printf ' cn0001    | coordinator |     16432 | 127.0.0.1\n'
       printf ' dn0001    | datanode    |     16435 | 127.0.0.1\n' ;;
    2) printf ' oid  |  group_name   | group_members\n'
       printf '------+---------------+---------------\n'
       printf ' 16384 | default_group | {dn0001}\n' ;;
    3) printf ' id | name \n'
       printf '----+------\n'
       printf '  1 | ok\n'
       printf '  2 | good\n' ;;
esac
exit 0
FAKE
    chmod +x "$d/bin/psql"
}

# run_case <id> <env-assignments> <script-args...>
#   Sets up a fresh case dir with fake psql, runs the script under
#   test with `timeout`, then checks $RC and output file $OUT.
run_case() {
    local id="$1"; shift
    local envs="$1"; shift
    local d="$WORK_ROOT/$id"
    mkdir -p "$d/home"
    make_fake_psql "$d"
    : > "$d/out"
    echo "--- $id ---"
    RC=99
    eval "timeout 20 env HOME='$d/home' PG_HOME='$d' FAKE_DIR='$d' $envs \
        bash '$SCRIPT_UNDER_TEST' $* < /dev/null > '$d/out' 2>&1"
    RC=$?
    OUT="$d/out"
    LOG="$d/log"
}

banner='Health check finished. All green above = cluster usable.'
skipped_note='Note: smoke test was skipped (--no-write); read-only checks only.'

echo "Script under test: $SCRIPT_UNDER_TEST"
if [ ! -f "$SCRIPT_UNDER_TEST" ]; then
    echo "FATAL: script not found" >&2
    exit 9
fi

# ------------------------------------------------------------
# U01: no args, all SQL succeeds -> exit 0, three SQL calls, one banner
# ------------------------------------------------------------
run_case U01 "CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres"
check_rc=0; [ "$RC" -eq 0 ] || check_rc=1
if [ "$EXPECT_BASELINE" = "1" ]; then
    # Baseline behaves the same here (no failure injected).
    [ "$RC" -eq 0 ] && ok "U01 exit 0" || bad "U01 exit 0 (got $RC)"
else
    [ "$RC" -eq 0 ] && ok "U01 exit 0" || bad "U01 exit 0 (got $RC)"
fi
n_calls=$(grep -c '^call [0-9]* args:' "$LOG" 2>/dev/null || echo 0)
[ "$n_calls" -eq 3 ] && ok "U01 exactly 3 psql calls" || bad "U01 3 psql calls (got $n_calls)"
if [ "$EXPECT_BASELINE" != "1" ]; then
    assert_contains "$LOG" "oes=1" "U01 ON_ERROR_STOP present"
fi
assert_contains "$LOG" "stdin: " "U01 smoke heredoc delivered"
c=$(grep -cF "$banner" "$OUT"); [ "$c" -eq 1 ] && ok "U01 banner exactly once" || bad "U01 banner once (got $c)"
if grep -q '^call 3 stdin: drop table if exists otb_health_check' "$LOG" 2>/dev/null; then ok "U01 smoke SQL in heredoc"; else bad "U01 smoke heredoc content"; fi

# ------------------------------------------------------------
# U02: --no-write -> exit 0, only 2 SQL calls, no smoke, skip marked
# ------------------------------------------------------------
run_case U02 "CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres" --no-write
if [ "$EXPECT_BASELINE" = "1" ]; then
    echo "  SKIP: U02 (baseline mode)"
else
[ "$RC" -eq 0 ] && ok "U02 exit 0" || bad "U02 exit 0 (got $RC)"
n_calls=$(grep -c '^call [0-9]* args:' "$LOG" 2>/dev/null || echo 0)
[ "$n_calls" -eq 2 ] && ok "U02 only 2 psql calls (no smoke)" || bad "U02 2 psql calls (got $n_calls)"
assert_contains "$OUT" "(skipped: --no-write)" "U02 skip marked"
assert_contains "$OUT" "$banner" "U02 banner present"
assert_contains "$OUT" "$skipped_note" "U02 skip note present"
fi

# ------------------------------------------------------------
# U03: registration query connection failure -> exit 1, no banner
# ------------------------------------------------------------
run_case U03 "CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres FAKE_FAIL_AT=1 FAKE_FAIL_MODE=conn"
if [ "$EXPECT_BASELINE" = "1" ]; then
    echo "  SKIP: U03 (baseline mode)"
else
[ "$RC" -eq 1 ] && ok "U03 exit 1" || bad "U03 exit 1 (got $RC)"
assert_contains "$OUT" "FATAL" "U03 FATAL stage error"
n_calls=$(grep -c '^call [0-9]* args:' "$LOG" 2>/dev/null || echo 0)
[ "$n_calls" -eq 1 ] && ok "U03 no later queries" || bad "U03 only 1 call (got $n_calls)"
assert_not_contains "$OUT" "$banner" "U03 no banner"
fi

# ------------------------------------------------------------
# U04: registration query SQL error -> exit 1, no banner
#      (baseline: swallows it, exit 0 + banner => REPRODUCED)
# ------------------------------------------------------------
run_case U04 "CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres FAKE_FAIL_AT=1 FAKE_FAIL_MODE=sql"
if [ "$EXPECT_BASELINE" = "1" ]; then
    if [ "$RC" -eq 0 ] && grep -qF "$banner" "$OUT"; then
        ok "U04 REPRODUCED: baseline exits 0 with banner despite SQL error"
    else
        bad "U04 baseline reproduction expected (rc=$RC)"
    fi
else
    [ "$RC" -ne 0 ] && ok "U04 exit non-zero" || bad "U04 exit non-zero (got $RC)"
    assert_contains "$OUT" "FATAL" "U04 stage error"
    assert_not_contains "$OUT" "$banner" "U04 no banner"
fi

# ------------------------------------------------------------
# U05: group query SQL error -> exit 1, smoke never runs, no banner
#      (baseline: REPRODUCED)
# ------------------------------------------------------------
run_case U05 "CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres FAKE_FAIL_AT=2 FAKE_FAIL_MODE=sql"
if [ "$EXPECT_BASELINE" = "1" ]; then
    if [ "$RC" -eq 0 ] && grep -qF "$banner" "$OUT"; then
        ok "U05 REPRODUCED: baseline exits 0 with banner despite SQL error"
    else
        bad "U05 baseline reproduction expected (rc=$RC)"
    fi
else
    [ "$RC" -eq 1 ] && ok "U05 exit 1" || bad "U05 exit 1 (got $RC)"
    n_calls=$(grep -c '^call [0-9]* args:' "$LOG" 2>/dev/null || echo 0)
    [ "$n_calls" -eq 2 ] && ok "U05 smoke not reached" || bad "U05 only 2 calls (got $n_calls)"
    assert_not_contains "$OUT" "$banner" "U05 no banner"
fi

# ------------------------------------------------------------
# U06: smoke CREATE fails -> exit 1, no banner
#      (baseline: REPRODUCED)
# ------------------------------------------------------------
run_case U06 "CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres FAKE_FAIL_AT=3 FAKE_FAIL_MODE=sql FAKE_FAIL_STMT='create table otb_health_check'"
if [ "$EXPECT_BASELINE" = "1" ]; then
    if [ "$RC" -eq 0 ] && grep -qF "$banner" "$OUT"; then
        ok "U06 REPRODUCED: baseline exits 0 with banner despite smoke CREATE error"
    else
        bad "U06 baseline reproduction expected (rc=$RC)"
    fi
else
    [ "$RC" -eq 1 ] && ok "U06 exit 1" || bad "U06 exit 1 (got $RC)"
    assert_contains "$OUT" "FAIL: distributed DDL/DML smoke test" "U06 smoke failure message"
    assert_contains "$OUT" "simulated by test stub" "U06 raw SQL error preserved"
    assert_not_contains "$OUT" "$banner" "U06 no banner"
fi

# ------------------------------------------------------------
# U07: smoke INSERT/SELECT fails -> exit 1, no banner
#      (baseline: REPRODUCED)
# ------------------------------------------------------------
run_case U07 "CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres FAKE_FAIL_AT=3 FAKE_FAIL_MODE=sql FAKE_FAIL_STMT='insert into otb_health_check'"
if [ "$EXPECT_BASELINE" = "1" ]; then
    if [ "$RC" -eq 0 ] && grep -qF "$banner" "$OUT"; then
        ok "U07 REPRODUCED: baseline exits 0 with banner despite smoke INSERT error"
    else
        bad "U07 baseline reproduction expected (rc=$RC)"
    fi
else
    [ "$RC" -eq 1 ] && ok "U07 exit 1" || bad "U07 exit 1 (got $RC)"
    assert_not_contains "$OUT" "$banner" "U07 no banner"
fi

# ------------------------------------------------------------
# U08: PGXC_CONF missing -> process section skipped, exit 0 on success
# ------------------------------------------------------------
run_case U08 "CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres PGXC_CONF=$WORK_ROOT/does-not-exist.conf"
if [ "$EXPECT_BASELINE" = "1" ]; then
    echo "  SKIP: U08 (baseline mode)"
else
[ "$RC" -eq 0 ] && ok "U08 exit 0" || bad "U08 exit 0 (got $RC)"
assert_contains "$OUT" "(no pgxc_ctl.conf" "U08 process section skipped"
assert_contains "$OUT" "$banner" "U08 banner present"
fi

# ------------------------------------------------------------
# U09: psql missing entirely -> non-zero, no banner
# ------------------------------------------------------------
d="$WORK_ROOT/U09"; mkdir -p "$d/emptybin" "$d/home"
: > "$d/out"
if [ "$EXPECT_BASELINE" = "1" ]; then
    echo "  SKIP: U09 (baseline mode)"
else
timeout 20 env HOME="$d/home" PG_HOME="$d/emptybin" CN_PORT=16432 \
    bash "$SCRIPT_UNDER_TEST" > "$d/out" 2>&1
rc=$?
if [ "$rc" -ne 0 ]; then ok "U09 exit non-zero ($rc)"; else bad "U09 exit non-zero (got 0)"; fi
assert_not_contains "$d/out" "$banner" "U09 no banner"
fi

echo ""
echo "==================================================="
echo "RESULTS: pass=$pass fail=$fail  (mode: $([ "$EXPECT_BASELINE" = "1" ] && echo baseline-reproduction || echo fixed))"
echo "==================================================="
[ "$fail" -eq 0 ] || exit 1
exit 0
