#!/bin/bash
# ============================================================
# test_otb_health.sh -- black-box contract tests for otb_health.sh
# ------------------------------------------------------------
# No external database and no host dependency: each case runs in a
# fresh temp dir with a controlled HOME, PATH and PGXC_CONF, and a
# stub `psql` installed into PG_HOME/bin.
#
# The stub models the real client:
#   * `-c <sql>`       : a SQL error exits 1 immediately (no ON_ERROR_STOP
#                        needed -- this mirrors psql's EXIT_FAILURE for a
#                        single -c statement);
#   * stdin script     : a SQL error exits 3 with ON_ERROR_STOP, and is
#                        swallowed (final exit 0) without it.
# A failure is injected only when BOTH the call number matches AND the
# requested target statement is present; a missing target is reported as
# a stub configuration error (exit 90) instead of silently passing.
#
# Usage:
#   bash test_otb_health.sh              # test the fixed script
#   EXPECT_BASELINE=1 bash test_otb_health.sh
#                                        # run against the PRE-FIX script:
#                                        # U05-U07c are expected to
#                                        # REPRODUCE the bug (exit 0 +
#                                        # success banner despite errors)
#
# Environment overrides:
#   SCRIPT_UNDER_TEST=<path>   script to test (default: ../otb_health.sh)
# ============================================================
set -u

SCRIPT_UNDER_TEST="${SCRIPT_UNDER_TEST:-$(cd "$(dirname "$0")/.." && pwd)/otb_health.sh}"
EXPECT_BASELINE="${EXPECT_BASELINE:-0}"
WORK_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/otb_health_test.XXXXXX")
trap 'rm -rf "$WORK_ROOT"' EXIT

pass=0
fail=0
cases=0
RUN_ARGS=()

ok()  { pass=$((pass+1)); echo "  ok  : $1"; }
bad() { fail=$((fail+1)); echo "  FAIL: $1"; }

assert_contains()     { if grep -qF -- "$2" "$1"; then ok "$3"; else bad "$3 (missing: $2)"; fi; }
assert_not_contains() { if grep -qF -- "$2" "$1"; then bad "$3 (unexpected: $2)"; else ok "$3"; fi; }

# make_stub_psql <dir>  -- writes <dir>/psql
make_stub_psql() {
    local bin="$1"
    mkdir -p "$bin"
    cat > "$bin/psql" <<'FAKE'
#!/bin/bash
d=${FAKE_DIR:?}
n=$(cat "$d/count" 2>/dev/null || echo 0); n=$((n+1)); echo "$n" > "$d/count"

# record every argv on its own line so -X / -v / ON_ERROR_STOP=1 can be asserted
: > "$d/argv.$n"
for a in "$@"; do printf '%s\n' "$a" >> "$d/argv.$n"; done

# detect mode: `-c <sql>` vs stdin script
mode=stdin
sql=""
prev=""
for a in "$@"; do
    if [ "$prev" = "-c" ]; then sql="$a"; mode=c; fi
    prev="$a"
done
if [ "$mode" = "stdin" ]; then
    cat > "$d/stdin.$n" 2>/dev/null || true
    sql=$(cat "$d/stdin.$n" 2>/dev/null)
fi
printf '%s\n' "call $n mode=$mode" >> "$d/log"
printf '%s\n' "call $n sql=$(printf '%s' "$sql" | tr '\n' ' ')" >> "$d/log"

# failure injection: call number AND target statement must both match
fail_at=${FAKE_FAIL_AT:-0}
fail_mode=${FAKE_FAIL_MODE:-sql}
stmt=${FAKE_FAIL_STMT:-}
hit=0
if [ "$n" -eq "$fail_at" ] 2>/dev/null; then
    if [ -z "$stmt" ]; then
        hit=1
    elif printf '%s' "$sql" | tr 'A-Z' 'a-z' | grep -qF -- "$stmt"; then
        hit=1
    else
        printf '%s\n' "FAKE ERROR: call $n does not contain target \"$stmt\"" >&2
        exit 90
    fi
fi

if [ "$hit" = "1" ]; then
    if [ "$fail_mode" = "conn" ]; then
        printf '%s\n' "psql: could not connect to server: Connection refused"
        exit 2
    fi
    printf '%s\n' 'ERROR:  relation "no_such_group" does not exist (simulated by test stub)'
    if [ "$mode" = "c" ]; then
        # real psql: a failed single -c command exits 1 regardless of ON_ERROR_STOP
        exit 1
    fi
    # stdin script: ON_ERROR_STOP -> 3, otherwise the error is swallowed (exit 0)
    if grep -qxF -- '-v' "$d/argv.$n" && grep -qxF -- 'ON_ERROR_STOP=1' "$d/argv.$n"; then
        exit 3
    fi
    exit 0
fi

# normal output
case "$mode" in
    c)
        case "$sql" in
            *pgxc_node*)
                printf '%s\n' ' node_name | node_type   | node_port | node_host' \
                    '-----------+-------------+-----------+-----------' \
                    ' cn0001    | coordinator |     16432 | 127.0.0.1' \
                    ' dn0001    | datanode    |     16435 | 127.0.0.1' ;;
            *pgxc_group*)
                printf '%s\n' ' group_name    | group_members' \
                    '---------------+---------------' \
                    ' default_group | {dn0001}' ;;
            *)
                printf '%s\n' ' ?column? ' '----------' '        1' ;;
        esac ;;
    stdin)
        printf '%s\n' 'BEGIN' 'CREATE TABLE' 'INSERT 0 2' \
            ' id | name ' '----+------' '  1 | ok' '  2 | good' '(2 rows)' 'DROP TABLE' 'COMMIT' ;;
esac
exit 0
FAKE
    chmod +x "$bin/psql"
}

# run_case <id> [KEY=VALUE ...]
#   Script arguments come from the global RUN_ARGS array.
#   Sets $RC, $OUT (stdout+stderr file), $LOG (stub call log), $D (case dir).
run_case() {
    local id="$1"; shift
    local d="$WORK_ROOT/$id"
    mkdir -p "$d/home" "$d/bin"
    make_stub_psql "$d/bin"
    : > "$d/out"
    cases=$((cases+1))
    timeout 20 env HOME="$d/home" PG_HOME="$d" FAKE_DIR="$d" \
        PATH="$d/bin:/usr/local/bin:/usr/bin:/bin" \
        PSQLRC= PGSERVICEFILE= PGSERVICE= PGOPTIONS= PGSSLMODE= \
        "$@" bash "$SCRIPT_UNDER_TEST" "${RUN_ARGS[@]}" \
        < /dev/null > "$d/out" 2>&1
    RC=$?
    OUT="$d/out"
    LOG="$d/log"
    D="$d"
}

# assert_psql_flags <case-dir> <n-calls> <label>
assert_psql_flags() {
    local d="$1" n="$2" label="$3"
    local i miss=0
    for i in $(seq 1 "$n"); do
        { grep -qxF -- '-X' "$d/argv.$i" && grep -qxF -- '-v' "$d/argv.$i" \
            && grep -qxF -- 'ON_ERROR_STOP=1' "$d/argv.$i"; } || miss=1
    done
    if [ "$miss" = "0" ]; then ok "$label"; else bad "$label (missing -X or ON_ERROR_STOP in some call)"; fi
}

n_calls() { grep -c '^call [0-9]* mode=' "$1" 2>/dev/null || echo 0; }

echo "Script under test: $SCRIPT_UNDER_TEST"
[ -f "$SCRIPT_UNDER_TEST" ] || { echo "FATAL: script not found" >&2; exit 9; }

# ------------------------------------------------------------
# U01: write mode, all SQL succeeds
# ------------------------------------------------------------
RUN_ARGS=()
run_case U01 CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres
if [ "$EXPECT_BASELINE" = "1" ]; then
    echo "-- U01: SKIP (baseline mode)"
else
    [ "$RC" -eq 0 ] && ok "U01 exit 0" || bad "U01 exit 0 (got $RC)"
    [ "$(n_calls "$LOG")" -eq 3 ] && ok "U01 exactly 3 psql calls" || bad "U01 3 calls (got $(n_calls "$LOG"))"
    assert_psql_flags "$D" 3 "U01 all calls carry -X and ON_ERROR_STOP=1"
    s3="$D/stdin.3"
    if [ -f "$s3" ]; then ok "U01 smoke delivered on stdin"; else bad "U01 smoke delivered on stdin"; fi
    l1=$(grep -n '^begin;$' "$s3" 2>/dev/null | cut -d: -f1)
    l2=$(grep -n '^create table otb_health_check' "$s3" 2>/dev/null | cut -d: -f1)
    l3=$(grep -n '^insert into otb_health_check' "$s3" 2>/dev/null | cut -d: -f1)
    l4=$(grep -n '^select \* from otb_health_check' "$s3" 2>/dev/null | cut -d: -f1)
    l5=$(grep -n '^drop table otb_health_check' "$s3" 2>/dev/null | cut -d: -f1)
    l6=$(grep -n '^commit;$' "$s3" 2>/dev/null | cut -d: -f1)
    if [ -n "$l1" ] && [ -n "$l2" ] && [ -n "$l3" ] && [ -n "$l4" ] && [ -n "$l5" ] && [ -n "$l6" ] \
       && [ "$l1" -eq 1 ] && [ "$l1" -lt "$l2" ] && [ "$l2" -lt "$l3" ] \
       && [ "$l3" -lt "$l4" ] && [ "$l4" -lt "$l5" ] && [ "$l5" -lt "$l6" ] \
       && [ "$l6" -eq "$(wc -l < "$s3")" ]; then
        ok "U01 smoke transaction wraps CREATE/INSERT/SELECT/DROP before COMMIT"
    else
        bad "U01 smoke statement order"
    fi
    assert_not_contains "$s3" "drop table if exists" "U01 never pre-drops an existing relation"
    assert_not_contains "$s3" "create table if not exists" "U01 name collision must fail rather than reuse a relation"
    assert_contains "$OUT" "PASS: required SQL checks completed; distributed DDL/DML smoke test passed." "U01 narrowed success line"
    assert_not_contains "$OUT" "FAIL:" "U01 no FAIL line"
    assert_not_contains "$OUT" "All green" "U01 old over-broad banner removed"
fi

# ------------------------------------------------------------
# U02: --no-write
# ------------------------------------------------------------
RUN_ARGS=(--no-write)
run_case U02 CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres
if [ "$EXPECT_BASELINE" = "1" ]; then
    echo "-- U02: SKIP (baseline mode)"
else
    [ "$RC" -eq 0 ] && ok "U02 exit 0" || bad "U02 exit 0 (got $RC)"
    [ "$(n_calls "$LOG")" -eq 2 ] && ok "U02 only 2 psql calls (no smoke)" || bad "U02 2 calls (got $(n_calls "$LOG"))"
    [ -f "$D/stdin.3" ] && bad "U02 no smoke stdin" || ok "U02 no smoke stdin"
    assert_contains "$OUT" "SKIP: write test disabled by --no-write" "U02 SKIP marker"
    assert_contains "$OUT" "PASS: read-only SQL queries completed; write capability was not tested." "U02 read-only success line"
    assert_not_contains "$OUT" "smoke test passed" "U02 does not claim write was tested"
fi

# ------------------------------------------------------------
# U03: registration query connection failure
# ------------------------------------------------------------
RUN_ARGS=()
run_case U03 CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres FAKE_FAIL_AT=1 FAKE_FAIL_MODE=conn
[ "$RC" -eq 1 ] && ok "U03 exit 1" || bad "U03 exit 1 (got $RC)"
if [ "$EXPECT_BASELINE" = "1" ]; then
    assert_not_contains "$OUT" "PASS:" "U03 no success line"
else
    assert_contains "$OUT" "FAIL: stage=node-registration psql_exit=2" "U03 stage + raw exit code"
    assert_not_contains "$OUT" "PASS:" "U03 no success line"
    [ "$(n_calls "$LOG")" -eq 1 ] && ok "U03 no later queries" || bad "U03 only 1 call (got $(n_calls "$LOG"))"
fi

# ------------------------------------------------------------
# U04: registration query SQL error (-c mode: exits 1 either way;
#      this verifies the script still reports it with the raw code)
# ------------------------------------------------------------
RUN_ARGS=()
run_case U04 CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres FAKE_FAIL_AT=1 FAKE_FAIL_MODE=sql
[ "$RC" -eq 1 ] && ok "U04 exit 1" || bad "U04 exit 1 (got $RC)"
if [ "$EXPECT_BASELINE" = "1" ]; then
    assert_not_contains "$OUT" "PASS:" "U04 baseline: no success line"
else
    assert_contains "$OUT" "FAIL: stage=node-registration psql_exit=1" "U04 stage + raw exit code"
    assert_not_contains "$OUT" "PASS:" "U04 no success line"
fi

# ------------------------------------------------------------
# U05: node-group query SQL error (-c mode returned 1, old script
#      ignored the status and continued => REPRODUCED on baseline)
# ------------------------------------------------------------
RUN_ARGS=()
run_case U05 CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres FAKE_FAIL_AT=2 FAKE_FAIL_MODE=sql
if [ "$EXPECT_BASELINE" = "1" ]; then
    if [ "$RC" -eq 0 ] && grep -qF "All green" "$OUT"; then
        ok "U05 REPRODUCED: baseline exits 0 with banner despite -c SQL error"
    else
        bad "U05 baseline reproduction expected (rc=$RC)"
    fi
else
    [ "$RC" -eq 1 ] && ok "U05 exit 1" || bad "U05 exit 1 (got $RC)"
    assert_contains "$OUT" "FAIL: stage=node-groups psql_exit=1" "U05 stage + raw exit code"
    [ "$(n_calls "$LOG")" -eq 2 ] && ok "U05 smoke not reached" || bad "U05 only 2 calls (got $(n_calls "$LOG"))"
    assert_not_contains "$OUT" "PASS:" "U05 no success line"
fi

# ------------------------------------------------------------
# U06/U07/U07b/U07c: each smoke statement fails in turn (stdin mode)
#      old script has no ON_ERROR_STOP => stub exits 0 => REPRODUCED
# ------------------------------------------------------------
smoke_case() {
    local id="$1" stmt="$2" label="$3"
    RUN_ARGS=()
    run_case "$id" CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres \
        FAKE_FAIL_AT=3 FAKE_FAIL_MODE=sql "FAKE_FAIL_STMT=$stmt"
    if [ "$EXPECT_BASELINE" = "1" ]; then
        if [ "$RC" -eq 0 ] && grep -qF "All green" "$OUT"; then
            ok "$id REPRODUCED: baseline exits 0 with banner despite $label failure"
        else
            bad "$id baseline reproduction expected (rc=$RC)"
        fi
    else
        [ "$RC" -eq 1 ] && ok "$id exit 1 ($label)" || bad "$id exit 1 (got $RC)"
        assert_contains "$OUT" "FAIL: stage=smoke psql_exit=3" "$id stage + raw exit code"
        assert_contains "$OUT" "simulated by test stub" "$id raw SQL error preserved"
        assert_not_contains "$OUT" "PASS:" "$id no success line"
    fi
}

smoke_case U06 'create table otb_health_check' "CREATE"
smoke_case U07 'insert into otb_health_check' "INSERT"
smoke_case U07b 'select * from otb_health_check' "SELECT"
smoke_case U07c 'drop table otb_health_check' "final DROP"
if [ "$EXPECT_BASELINE" != "1" ]; then
    smoke_case U07d 'commit;' "COMMIT"
fi

# ------------------------------------------------------------
# U08: PGXC_CONF missing -> process section SKIP, SQL success -> 0
# ------------------------------------------------------------
RUN_ARGS=()
run_case U08 CN_PORT=16432 DB_USER=opentenbase DB_NAME=postgres PGXC_CONF="$WORK_ROOT/does-not-exist.conf"
if [ "$EXPECT_BASELINE" = "1" ]; then
    echo "-- U08: SKIP (baseline mode)"
else
    [ "$RC" -eq 0 ] && ok "U08 exit 0" || bad "U08 exit 0 (got $RC)"
    assert_contains "$OUT" "SKIP: no pgxc_ctl.conf" "U08 process section marked SKIP"
    assert_contains "$OUT" "PASS: required SQL checks completed" "U08 success line"
fi

# ------------------------------------------------------------
# U09: psql absent -> controlled PATH without any psql
# ------------------------------------------------------------
RUN_ARGS=()
if [ "$EXPECT_BASELINE" = "1" ]; then
    echo "-- U09: SKIP (baseline mode)"
else
    cases=$((cases+1))
    d="$WORK_ROOT/U09"; mkdir -p "$d/home" "$d/empty"; : > "$d/out"
    BASH_BIN=$(command -v bash || echo /bin/bash)
    timeout 20 env HOME="$d/home" PG_HOME="$d/empty" PATH="$d/empty" \
        PSQLRC= PGSERVICEFILE= PGSERVICE= \
        "$BASH_BIN" "$SCRIPT_UNDER_TEST" < /dev/null > "$d/out" 2>&1
    rc=$?
    [ "$rc" -eq 1 ] && ok "U09 exit exactly 1 (got $rc)" || bad "U09 exit exactly 1 (got $rc)"
    assert_contains "$d/out" "FAIL: stage=node-registration psql_exit=127" "U09 stage + 127"
    assert_not_contains "$d/out" "PASS:" "U09 no success line"
fi

echo ""
echo "==================================================="
echo "RESULTS: cases=$cases pass=$pass fail=$fail (mode: $([ "$EXPECT_BASELINE" = "1" ] && echo baseline-reproduction || echo fixed))"
echo "==================================================="
[ "$fail" -eq 0 ] || exit 1
exit 0
