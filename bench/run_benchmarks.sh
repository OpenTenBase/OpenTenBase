#!/bin/bash
# =============================================================================
# OpenTenBase / PostgreSQL Centralized Benchmark Suite — Runner Script
# =============================================================================
# Usage:
#   ./run_benchmarks.sh setup          Create tables (setup.sql)
#   ./run_benchmarks.sh load           Load data (data_load.sql, ~1-3 min)
#   ./run_benchmarks.sh all            Setup + load + run all benchmarks
#   ./run_benchmarks.sh run            Run benchmarks only (assumes setup+load done)
#   ./run_benchmarks.sh single <name>  Run a single benchmark by name
#
# Environment variables (with defaults):
#   PGHOST=127.0.0.1      PGPORT=11000
#   PGUSER=opentenbase     PGDATABASE=benchdb
#   PGBENCH=${PG_HOME}/bin/pgbench   (falls back to pgbench on PATH)
#   PSQL=${PG_HOME}/bin/psql         (falls back to psql on PATH)
#   CLIENTS="1 4 8 16 32"            Concurrency levels
#   DURATION=60                      Seconds per pgbench run
#   WARMUP=10                        Warmup seconds per run
#   RUNS=3                           Runs per concurrency level (median taken)
#   BENCH_DIR                        Directory containing sql/ (default: script dir)
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="${BENCH_DIR:-$SCRIPT_DIR}"
SQL_DIR="${BENCH_DIR}/sql"
RESULT_DIR="${BENCH_DIR}/results"
METRIC_DIR="${BENCH_DIR}/metrics"
mkdir -p "$RESULT_DIR" "$METRIC_DIR"

# ---------------------------------------------------------------------------
# Connection parameters
# ---------------------------------------------------------------------------
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-11000}"
PGUSER="${PGUSER:-opentenbase}"
PGDATABASE="${PGDATABASE:-benchdb}"
PGBENCH="${PGBENCH:-pgbench}"
PSQL="${PSQL:-psql}"

# Common psql/pgbench args
PSQL_ARGS="-h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE"
PGB_ARGS="-h $PGHOST -p $PGPORT -U $PGUSER"

# ---------------------------------------------------------------------------
# Benchmark parameters
# ---------------------------------------------------------------------------
CLIENTS="${CLIENTS:-1 4 8 16 32}"
DURATION="${DURATION:-60}"
WARMUP="${WARMUP:-10}"
RUNS="${RUNS:-3}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

# Results CSV
RESULTS_CSV="${RESULT_DIR}/results_${TIMESTAMP}.csv"

# ---------------------------------------------------------------------------
# Helper: log with timestamp
# ---------------------------------------------------------------------------
log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ---------------------------------------------------------------------------
# Helper: median of space-separated numbers (max 20 values)
# ---------------------------------------------------------------------------
median() {
    printf '%s\n' $* | sort -n | awk '{
        a[NR]=$1
    } END {
        if (NR % 2 == 1) print a[int(NR/2)+1];
        else print (a[NR/2] + a[NR/2+1]) / 2.0;
    }'
}

# ---------------------------------------------------------------------------
# Step 1: Setup schema
# ---------------------------------------------------------------------------
do_setup() {
    log "=== Setup: creating benchmark tables ==="
    $PSQL $PSQL_ARGS -f "$SQL_DIR/setup.sql"
    log "Setup complete."
}

# ---------------------------------------------------------------------------
# Step 2: Load data
# ---------------------------------------------------------------------------
do_load() {
    log "=== Data Load: populating benchmark tables ==="
    $PSQL $PSQL_ARGS -f "$SQL_DIR/data_load.sql"
    log "Data load complete."
}

# ---------------------------------------------------------------------------
# Internal: run pgbench for one script + one concurrency level, N times.
# Prints lines to stdout; results CSV is written by the caller.
# ---------------------------------------------------------------------------
run_pgbench_trials() {
    local label="$1"       # e.g. "point_select"
    local script="$2"      # path to pgbench .sql file
    local clients="$3"
    local threads="$4"
    local extra_args="${5:-}"

    local tps_vals=""
    local lat_vals=""
    local p50_vals=""
    local p95_vals=""
    local p99_vals=""

    for run in $(seq 1 $RUNS); do
        log "    Run $run/$RUNS: $label  clients=$clients  threads=$threads"

        local log_prefix="${RESULT_DIR}/${label}_c${clients}_r${run}"

        # Run pgbench with latency logging
        local outfile="${log_prefix}_out.txt"
        set +e
        $PGBENCH $PGB_ARGS -d "$PGDATABASE" \
            -f "$script" \
            -T "$DURATION" \
            -P 5 \
            -c "$clients" \
            -j "$threads" \
            --log --log-prefix="${log_prefix}_" \
            $extra_args \
            > "$outfile" 2>&1
        local rc=$?
        set -e

        if [ $rc -ne 0 ]; then
            log "    WARNING: pgbench exited with code $rc (may be connection limit)"
            echo "NA,NA,NA,NA,NA"
            return 0
        fi

        # Parse pgbench summary output for key metrics
        local tps=$(grep 'tps = ' "$outfile" | awk '{print $3}' | head -1)
        local avg_lat=$(grep 'latency average' "$outfile" | awk '{print $4}')
        local stddev=$(grep 'latency stddev' "$outfile" | grep -o '[0-9.]*' | tail -1)

        [ -z "$tps" ]     && tps="0"
        [ -z "$avg_lat" ] && avg_lat="0"

        tps_vals="$tps_vals $tps"
        lat_vals="$lat_vals $avg_lat"

        # Sleep briefly between runs
        sleep 2
    done

    local med_tps=$(median $tps_vals)
    local med_lat=$(median $lat_vals)

    echo "$med_tps,$med_lat"
}

# ---------------------------------------------------------------------------
# Run a single benchmark scenario across all concurrency levels
# ---------------------------------------------------------------------------
run_scenario() {
    local label="$1"       # short name for CSV
    local script="$2"      # .sql file
    local threads="${3:-4}"
    local extra_args="${4:-}"

    log "--- Scenario: $label ---"

    for c in $CLIENTS; do
        local result
        result=$(run_pgbench_trials "$label" "$script" "$c" "$threads" "$extra_args")
        local med_tps=$(echo "$result" | cut -d, -f1)
        local med_lat=$(echo "$result" | cut -d, -f2)

        echo "${label},${c},${med_tps},${med_lat},${TIMESTAMP}" >> "$RESULTS_CSV"
        log "    clients=${c}  median_tps=${med_tps}  median_lat_ms=${med_lat}"

        # Cool-down between concurrency levels
        sleep 5
    done
}

# ---------------------------------------------------------------------------
# psql \timing mode: single-run timing for a query
# ---------------------------------------------------------------------------
run_psql_timed() {
    local label="$1"
    local script="$2"
    local outfile="${RESULT_DIR}/${label}_timed_${TIMESTAMP}.txt"

    log "--- Timed run: $label (psql \\timing mode) ---"

    local sql="\\timing on\n\\i ${script}\n\\timing off"

    # Use time command wrapper as well for wall-clock
    {
        echo "============================================="
        echo " Benchmark: $label"
        echo " Date: $(date)"
        echo "============================================="
        /usr/bin/time -f "WallClockSec=%e UserSec=%U SysSec=%S MaxRssKB=%M" \
            $PSQL $PSQL_ARGS -c "\timing on" -f "$script" 2>&1
    } | tee "$outfile"
}

# ---------------------------------------------------------------------------
# Individual benchmark drivers
# ---------------------------------------------------------------------------

bench_single_insert() {
    run_scenario "single_insert" "${SQL_DIR}/bench_single_insert.sql" 4
}

bench_batch_insert() {
    run_scenario "batch_insert" "${SQL_DIR}/bench_batch_insert.sql" 2
}

bench_point_select() {
    run_scenario "point_select" "${SQL_DIR}/bench_point_select.sql" 4
}

bench_aggregation() {
    run_scenario "aggregation" "${SQL_DIR}/bench_aggregation.sql" 4
}

bench_aggregation_txn() {
    run_scenario "aggregation_txn" "${SQL_DIR}/bench_aggregation_txn.sql" 4
}

bench_join() {
    run_scenario "join" "${SQL_DIR}/bench_join.sql" 4
}

bench_mixed() {
    run_scenario "mixed" "${SQL_DIR}/bench_mixed.sql" 4
}

# ---------------------------------------------------------------------------
# Run all benchmarks
# ---------------------------------------------------------------------------
do_run_all() {
    log "=== Starting benchmark suite at $(date) ==="
    log "Host: $PGHOST:$PGPORT  DB: $PGDATABASE  Duration: ${DURATION}s"
    log "Clients: $CLIENTS  Runs per level: $RUNS"
    log "Results: $RESULTS_CSV"
    log ""

    # Write CSV header
    echo "scenario,clients,median_tps,median_latency_ms,timestamp" > "$RESULTS_CSV"

    # Pre-warm: run a quick scan to bring data into shared buffers (optional)
    log "=== Pre-warm: loading hot data into cache ==="
    $PSQL $PSQL_ARGS -c "SELECT count(*) FROM bench_transactions;" 2>/dev/null || true
    $PSQL $PSQL_ARGS -c "SELECT count(*) FROM bench_accounts;"    2>/dev/null || true
    $PSQL $PSQL_ARGS -c "SELECT count(*) FROM bench_items;"       2>/dev/null || true
    log ""

    # 1. Single-row INSERT
    log "========== Scenario 1: Single-row INSERT =========="
    bench_single_insert
    sleep 10

    # 2. Batch INSERT
    log "========== Scenario 2: Batch INSERT =========="
    bench_batch_insert
    sleep 10

    # 3. Point SELECT
    log "========== Scenario 3: Point SELECT (PK lookup) =========="
    bench_point_select
    sleep 10

    # 4. Aggregation (accounts)
    log "========== Scenario 4: Aggregation (accounts GROUP BY) =========="
    bench_aggregation
    sleep 10

    # 5. Aggregation (transactions)
    log "========== Scenario 5: Aggregation (transactions GROUP BY) =========="
    bench_aggregation_txn
    sleep 10

    # 6. Two-table JOIN
    log "========== Scenario 6: Two-table JOIN =========="
    bench_join
    sleep 10

    # 7. Mixed workload
    log "========== Scenario 7: Mixed workload =========="
    bench_mixed

    log ""
    log "=== Benchmark suite complete at $(date) ==="
    log "Results saved to: $RESULTS_CSV"

    # Print summary table
    print_summary
}

# ---------------------------------------------------------------------------
# Print a readable summary of the CSV results
# ---------------------------------------------------------------------------
print_summary() {
    log ""
    log "=============================================="
    log " RESULTS SUMMARY"
    log "=============================================="
    if [ -f "$RESULTS_CSV" ]; then
        column -t -s, "$RESULTS_CSV"
    fi
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
case "${1:-help}" in
    setup)
        do_setup
        ;;
    load)
        do_load
        ;;
    run)
        do_run_all
        ;;
    all)
        do_setup
        do_load
        do_run_all
        ;;
    single)
        if [ $# -lt 2 ]; then
            echo "Usage: $0 single <benchmark_name>"
            echo "Available: single_insert batch_insert point_select aggregation aggregation_txn join mixed"
            exit 1
        fi

        # Write CSV header
        echo "scenario,clients,median_tps,median_latency_ms,timestamp" > "$RESULTS_CSV"

        case "$2" in
            single_insert)    bench_single_insert ;;
            batch_insert)     bench_batch_insert ;;
            point_select)     bench_point_select ;;
            aggregation)      bench_aggregation ;;
            aggregation_txn)  bench_aggregation_txn ;;
            join)             bench_join ;;
            mixed)            bench_mixed ;;
            *)
                echo "Unknown benchmark: $2"
                echo "Available: single_insert batch_insert point_select aggregation aggregation_txn join mixed"
                exit 1
                ;;
        esac
        print_summary
        ;;
    timed)
        # Run a single query via psql with \timing (no pgbench, single execution)
        if [ $# -lt 2 ]; then
            echo "Usage: $0 timed <script_name>"
            echo "Available: single_insert batch_insert point_select aggregation aggregation_txn join mixed"
            exit 1
        fi
        case "$2" in
            single_insert)    run_psql_timed "single_insert"    "${SQL_DIR}/bench_single_insert.sql" ;;
            batch_insert)     run_psql_timed "batch_insert"     "${SQL_DIR}/bench_batch_insert.sql" ;;
            point_select)     run_psql_timed "point_select"     "${SQL_DIR}/bench_point_select.sql" ;;
            aggregation)      run_psql_timed "aggregation"       "${SQL_DIR}/bench_aggregation.sql" ;;
            aggregation_txn)  run_psql_timed "aggregation_txn"  "${SQL_DIR}/bench_aggregation_txn.sql" ;;
            join)             run_psql_timed "join"             "${SQL_DIR}/bench_join.sql" ;;
            mixed)            run_psql_timed "mixed"            "${SQL_DIR}/bench_mixed.sql" ;;
            *) echo "Unknown benchmark: $2"; exit 1 ;;
        esac
        ;;
    help|--help|-h)
        echo "OpenTenBase Benchmark Runner"
        echo ""
        echo "Usage: $0 <command> [args]"
        echo ""
        echo "Commands:"
        echo "  setup            Create benchmark tables"
        echo "  load             Populate tables with test data"
        echo "  all              Setup + load + run all benchmarks"
        echo "  run              Run all benchmarks (requires setup+load already done)"
        echo "  single <name>    Run a single benchmark by name"
        echo "  timed <name>     Run once via psql with \\timing"
        echo "  help             This message"
        echo ""
        echo "Benchmark names:"
        echo "  single_insert    Single-row INSERT"
        echo "  batch_insert     Batch INSERT (100 rows/txn)"
        echo "  point_select     Point SELECT by PK"
        echo "  aggregation      GROUP BY aggregation on accounts"
        echo "  aggregation_txn  GROUP BY aggregation on transactions"
        echo "  join             Two-table JOIN (transactions + accounts)"
        echo "  mixed            Mixed read/write workload"
        echo ""
        echo "Environment:"
        echo "  PGHOST=$PGHOST  PGPORT=$PGPORT  PGUSER=$PGUSER"
        echo "  PGDATABASE=$PGDATABASE  CLIENTS=\"$CLIENTS\""
        echo "  DURATION=$DURATION  RUNS=$RUNS"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Run '$0 help' for usage."
        exit 1
        ;;
esac
