#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXTENDED=0
KEEP_DB="${KEEP_DB:-0}"
PG_CONFIG="${PG_CONFIG:-/data/opentenbase/install/pg18/bin/pg_config}"
# Default to the PostgreSQL runtime role used by the local OpenTenBase lab.
# Reviewers can override this with PGUSER=... when needed.
export PGUSER="${PGUSER:-postgres}"
DB_PREFIX="rhino_ivfflat_repro"
DB_NAME="${DB_PREFIX}_$$_$(date +%s)"
TMPDIR_CREATED="$(mktemp -d "${TMPDIR:-/tmp}/rhino_ivfflat_repro.XXXXXX")"
PSQL_LOG="$TMPDIR_CREATED/psql.log"
FALLBACK_LOG="$TMPDIR_CREATED/fallback.log"
EXPLAIN_LOG="$TMPDIR_CREATED/explain.log"
QUERY_LOG="$TMPDIR_CREATED/query.log"
DIAG_LOG="$TMPDIR_CREATED/diagnostics.log"
BOTH_FAIL_LOG="$TMPDIR_CREATED/both_fail.log"
CREATED_DB=0
OVERALL_STATUS=1

usage() {
    cat <<USAGE
Usage: $0 [--extended]

Environment overrides:
  PG_CONFIG=/path/to/pg_config
  PGHOST, PGPORT, PGUSER standard libpq connection variables
  KEEP_DB=1 keeps the temporary database for inspection
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --extended)
            EXTENDED=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
    shift
done

fail() {
    echo "FAIL: $*" >&2
    echo "OVERALL: FAIL" >&2
    exit 1
}

pass() {
    echo "PASS: $*"
}

cleanup() {
    local rc=$?
    if [ "$CREATED_DB" -eq 1 ] && [ "$KEEP_DB" != "1" ]; then
        if command -v "$DROPDB" >/dev/null 2>&1; then
            "$DROPDB" --if-exists "$DB_NAME" >/dev/null 2>&1 || true
        fi
    fi
    if [ "$KEEP_DB" != "1" ]; then
        rm -rf "$TMPDIR_CREATED"
    else
        echo "KEEP_DB=1: database kept: $DB_NAME"
        echo "KEEP_DB=1: temp files kept: $TMPDIR_CREATED"
    fi
    exit "$rc"
}
trap cleanup EXIT

require_file() {
    [ -x "$1" ] || fail "required executable not found: $1"
}

assert_file_contains() {
    local file="$1"
    local pattern="$2"
    local message="$3"
    if grep -Eq "$pattern" "$file"; then
        pass "$message"
    else
        echo "--- $file ---" >&2
        sed -n '1,220p' "$file" >&2 || true
        fail "$message not observed"
    fi
}

assert_sql_scalar_equals() {
    local sql="$1"
    local expected="$2"
    local message="$3"
    local actual
    actual="$($PSQL -X -v ON_ERROR_STOP=1 -d "$DB_NAME" -Atqc "$sql")"
    if [ "$actual" = "$expected" ]; then
        pass "$message"
    else
        fail "$message expected '$expected' got '$actual'"
    fi
}

if [ ! -x "$PG_CONFIG" ]; then
    fail "PG_CONFIG is not executable: $PG_CONFIG"
fi

BINDIR="$($PG_CONFIG --bindir)"
PSQL="$BINDIR/psql"
CREATEDB="$BINDIR/createdb"
DROPDB="$BINDIR/dropdb"

require_file "$PSQL"
require_file "$CREATEDB"
require_file "$DROPDB"

echo "Using PG_CONFIG: $PG_CONFIG"
echo "Using psql: $PSQL"
echo "Temporary database: $DB_NAME"

# Fast connection check before creating any database.
if ! "$PSQL" -X -d postgres -Atqc "SELECT 1" > "$TMPDIR_CREATED/connect_check.out" 2> "$TMPDIR_CREATED/connect_check.err"; then
    cat "$TMPDIR_CREATED/connect_check.err" >&2 || true
    fail "could not connect to PostgreSQL; check PGHOST/PGPORT/PGUSER"
fi

"$CREATEDB" "$DB_NAME"
CREATED_DB=1

run_sql_file() {
    local file="$1"
    local out="$2"
    "$PSQL" -X -v ON_ERROR_STOP=1 -d "$DB_NAME" -f "$file" > "$out" 2>&1
}

cat > "$TMPDIR_CREATED/prepare.sql" <<'SQL'
CREATE EXTENSION vector;
CREATE TABLE demo_vectors (
    id bigint PRIMARY KEY,
    v vector(3)
);
INSERT INTO demo_vectors
SELECT i,
       ARRAY[
           (i % 100)::real / 100,
           ((i * 7) % 100)::real / 100,
           ((i * 13) % 100)::real / 100
       ]::vector
FROM generate_series(1, 10000) AS i;
SELECT count(*) AS row_count FROM demo_vectors;
SQL

echo "[1/4] Preparing deterministic dataset"
if run_sql_file "$TMPDIR_CREATED/prepare.sql" "$PSQL_LOG"; then
    assert_file_contains "$PSQL_LOG" 'CREATE EXTENSION' 'vector extension available'
    assert_sql_scalar_equals "SELECT count(*) FROM demo_vectors" "10000" "dataset ready"
else
    cat "$PSQL_LOG" >&2 || true
    fail "dataset preparation failed; vector extension may be unavailable"
fi

cat > "$TMPDIR_CREATED/fallback.sql" <<'SQL'
SET client_min_messages = debug1;
SET maintenance_work_mem = '16MB';
SET max_parallel_maintenance_workers = 0;
CREATE INDEX demo_vectors_ivfflat_idx
ON demo_vectors
USING ivfflat (v vector_l2_ops)
WITH (lists = 1000);
SQL

echo "[2/4] Reproducing memory-aware fallback"
if run_sql_file "$TMPDIR_CREATED/fallback.sql" "$FALLBACK_LOG"; then
    assert_file_contains "$FALLBACK_LOG" 'Elkan exceeds maintenance_work_mem and Yinyang fits' 'automatic Yinyang fallback selected'
    assert_file_contains "$FALLBACK_LOG" 'using Yinyang k-means' 'Yinyang selected by builder'
    assert_file_contains "$FALLBACK_LOG" 'Elkan=[0-9]+ kB/[0-9]+ MB' 'Elkan estimated memory reported'
    assert_file_contains "$FALLBACK_LOG" 'Yinyang=[0-9]+ kB/[0-9]+ MB' 'Yinyang estimated memory reported'
    assert_file_contains "$FALLBACK_LOG" 'maintenance_work_mem=[0-9]+ kB/[0-9]+ MB' 'maintenance_work_mem budget reported'
    assert_file_contains "$FALLBACK_LOG" 'CREATE INDEX' 'CREATE INDEX succeeded'
else
    cat "$FALLBACK_LOG" >&2 || true
    fail "CREATE INDEX failed in fallback scenario"
fi

cat > "$TMPDIR_CREATED/explain.sql" <<'SQL'
SET enable_seqscan = off;
SET ivfflat.probes = 1;
EXPLAIN
SELECT id
FROM demo_vectors
ORDER BY v <-> '[0.5,0.5,0.5]'::vector
LIMIT 5;
SQL

cat > "$TMPDIR_CREATED/query.sql" <<'SQL'
SET enable_seqscan = off;
SET ivfflat.probes = 1;
SELECT count(*) FROM (
    SELECT id
    FROM demo_vectors
    ORDER BY v <-> '[0.5,0.5,0.5]'::vector
    LIMIT 5
) q;
SQL

echo "[3/4] Verifying IVFFlat index usability"
run_sql_file "$TMPDIR_CREATED/explain.sql" "$EXPLAIN_LOG"
assert_file_contains "$EXPLAIN_LOG" 'Index Scan' 'planner uses an Index Scan'
assert_file_contains "$EXPLAIN_LOG" 'demo_vectors_ivfflat_idx' 'planner uses demo_vectors_ivfflat_idx'
rows_returned="$($PSQL -X -v ON_ERROR_STOP=1 -d "$DB_NAME" -Atqf "$TMPDIR_CREATED/query.sql")"
if [ "$rows_returned" = "5" ]; then
    pass "query returned 5 rows"
else
    fail "query returned $rows_returned rows, expected 5"
fi

cat > "$TMPDIR_CREATED/diagnostics.sql" <<'SQL'
ANALYZE demo_vectors;
SET ivfflat.probes = 1;
SELECT severity, issue, current_value, recommended_value
FROM ivfflat_index_diagnostics('demo_vectors_ivfflat_idx'::regclass)
ORDER BY issue, severity;
SQL

echo "[4/4] Running IVFFlat diagnostics"
run_sql_file "$TMPDIR_CREATED/diagnostics.sql" "$DIAG_LOG"
assert_file_contains "$DIAG_LOG" 'WARNING[[:space:]]*\| lists_high[[:space:]]*\|' 'lists_high detected with WARNING severity'
assert_file_contains "$DIAG_LOG" 'WARNING[[:space:]]*\| low_recall_risk[[:space:]]*\|' 'low_recall_risk detected with WARNING severity'
assert_file_contains "$DIAG_LOG" 'INFO[[:space:]]*\| build_memory[[:space:]]*\|' 'build_memory reported with INFO severity'

if [ "$EXTENDED" -eq 1 ]; then
    echo "[extended] Verifying both-fail diagnostic path"
    cat > "$TMPDIR_CREATED/both_fail.sql" <<'SQL'
DROP INDEX IF EXISTS demo_vectors_ivfflat_idx;
SET client_min_messages = debug1;
SET maintenance_work_mem = '4MB';
SET max_parallel_maintenance_workers = 0;
CREATE INDEX demo_vectors_ivfflat_idx
ON demo_vectors
USING ivfflat (v vector_l2_ops)
WITH (lists = 3000);
SQL
    set +e
    "$PSQL" -X -v ON_ERROR_STOP=1 -d "$DB_NAME" -f "$TMPDIR_CREATED/both_fail.sql" > "$BOTH_FAIL_LOG" 2>&1
    both_rc=$?
    set -e
    if [ "$both_rc" -eq 0 ]; then
        cat "$BOTH_FAIL_LOG" >&2 || true
        fail "both-fail scenario unexpectedly succeeded"
    fi
    assert_file_contains "$BOTH_FAIL_LOG" 'memory required is .*maintenance_work_mem is' 'expected both-fail memory error observed'
    assert_file_contains "$BOTH_FAIL_LOG" 'DETAIL:.*Elkan=.*Yinyang=' 'both-fail DETAIL includes Elkan and Yinyang estimates'
    assert_file_contains "$BOTH_FAIL_LOG" 'HINT:.*Increase maintenance_work_mem.*reduce lists' 'both-fail HINT suggests memory or lists action'
    pass "expected both-fail diagnostic"
fi

OVERALL_STATUS=0
echo "========================================"
echo "Rhino-Bird IVFFlat reproduction: PASS"
echo "========================================"
