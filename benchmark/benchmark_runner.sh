#!/usr/bin/env bash
# OpenTenBase benchmark 的统一运行器。
# 负责串联 schema、load、pgbench workload、分析和清理阶段。
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

MODE=""
HOST="${CN_HOST:-127.0.0.1}"
PORT="${CN_PORT:-5432}"
DB_USER_NAME="${DB_USER:-opentenbase}"
DATABASE="${DB_NAME:-benchmark}"
CLIENTS="${CLIENTS:-1,4,8,16,32,64}"
WARMUP_CLIENTS="${WARMUP_CLIENTS:-1,4}"
DURATION="${DURATION:-60}"
WARMUP_DURATION="${WARMUP_DURATION:-10}"
JOBS="${JOBS:-0}"
RESULT_DIR="${RESULT_DIR:-benchmark_results_$(date +%Y%m%d_%H%M%S)}"
SECTION=""
SUITE_SECTION=""
WORKLOADS="w1_insert,w2_dist_key_lookup,w3_non_dist_filter,w4_dist_key_aggregate,w5_non_dist_aggregate,w6_colocated_join,w7_replication_join,w8_gtm_short_tx"
SCALE_FACTOR="${SCALE_FACTOR:-1}"
PSQL_BIN="${PSQL_BIN:-psql}"
PGBENCH_BIN="${PGBENCH_BIN:-pgbench}"

CLIENTS_SET=0
DURATION_SET=0

usage() {
    cat <<'EOF'
Usage:
  bash benchmark_runner.sh <mode> [options]

Modes:
  setup       Run schema.sql
  load        Run load_data.sql
  warmup      Run a short pgbench warmup
  run         Run the full workload matrix
  analyze     Run explain/distribution or a named section
  cleanup     Run the cleanup section from workload.sql
  all         Run setup -> load -> warmup -> run -> analyze

Options:
  --host HOST
  --port PORT
  --user USER
  --database DB
  --clients "1,4,8"
  --duration SECONDS
  --jobs N
  --result-dir DIR
  --section NAME
  --suite-section NAME
  --workloads "w1_insert,w2_dist_key_lookup"
  --scale-factor N
  --help
EOF
}

if [[ $# -gt 0 && "$1" != --* ]]; then
    MODE="$1"
    shift
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --user)
            DB_USER_NAME="$2"
            shift 2
            ;;
        --database)
            DATABASE="$2"
            shift 2
            ;;
        --clients)
            CLIENTS="$2"
            CLIENTS_SET=1
            shift 2
            ;;
        --duration)
            DURATION="$2"
            DURATION_SET=1
            shift 2
            ;;
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --result-dir)
            RESULT_DIR="$2"
            shift 2
            ;;
        --section)
            SECTION="$2"
            shift 2
            ;;
        --suite-section)
            SUITE_SECTION="$2"
            shift 2
            ;;
        --workloads)
            WORKLOADS="$2"
            shift 2
            ;;
        --scale-factor)
            SCALE_FACTOR="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z "${MODE}" ]]; then
    usage
    exit 1
fi

WORKLOADS="${WORKLOADS// /}"
CLIENTS="${CLIENTS// /}"
WARMUP_CLIENTS="${WARMUP_CLIENTS// /}"

if [[ "${RESULT_DIR}" != /* ]]; then
    RESULT_DIR="${SCRIPT_DIR}/${RESULT_DIR}"
fi

RAW_DIR="${RESULT_DIR}/raw"
TMP_DIR="${RESULT_DIR}/tmp"
SUMMARY_FILE="${RESULT_DIR}/run_summary.tsv"
CONTEXT_FILE="${RESULT_DIR}/run_context.txt"

mkdir -p "${RAW_DIR}" "${TMP_DIR}"

record_context() {
    cat > "${CONTEXT_FILE}" <<EOF
mode=${MODE}
host=${HOST}
port=${PORT}
user=${DB_USER_NAME}
database=${DATABASE}
clients=${CLIENTS}
warmup_clients=${WARMUP_CLIENTS}
duration=${DURATION}
warmup_duration=${WARMUP_DURATION}
jobs=${JOBS}
result_dir=${RESULT_DIR}
section=${SECTION}
suite_section=${SUITE_SECTION}
workloads=${WORKLOADS}
scale_factor=${SCALE_FACTOR}
timestamp=$(date '+%Y-%m-%d %H:%M:%S')
EOF
}

init_summary() {
    if [[ ! -f "${SUMMARY_FILE}" ]]; then
        printf "label\tclients\tduration_s\ttransactions\tlatency_avg_ms\tp50_ms\tp95_ms\tp99_ms\ttps\tlog_path\n" > "${SUMMARY_FILE}"
    fi
}

extract_section() {
    local section_name="$1"
    local output_file="$2"

    awk -v section_name="${section_name}" '
        $0 == "-- @section " section_name { in_section = 1; found = 1; next }
        $0 == "-- @end" {
            if (in_section) {
                in_section = 0
                exit
            }
        }
        in_section { print }
        END {
            if (!found) {
                exit 2
            }
        }
    ' "${SCRIPT_DIR}/workload.sql" > "${output_file}" || {
        echo "Failed to extract section: ${section_name}" >&2
        exit 1
    }
}

run_psql_file() {
    local file_path="$1"
    local label="$2"
    local log_file="${RAW_DIR}/${label}.log"

    "${PSQL_BIN}" \
        -v ON_ERROR_STOP=1 \
        -h "${HOST}" \
        -p "${PORT}" \
        -U "${DB_USER_NAME}" \
        -d "${DATABASE}" \
        -f "${file_path}" \
        > "${log_file}" 2>&1
}

run_psql_section() {
    local section_name="$1"
    local label="${2:-${section_name}}"
    local section_file="${TMP_DIR}/${section_name}.sql"

    extract_section "${section_name}" "${section_file}"
    run_psql_file "${section_file}" "${label}"
}

selected_workload() {
    local workload_label="$1"
    [[ ",${WORKLOADS}," == *",${workload_label},"* ]]
}

effective_jobs() {
    local clients="$1"
    local jobs_value="${JOBS}"

    if [[ "${jobs_value}" -le 0 ]]; then
        jobs_value="${clients}"
        if command -v nproc >/dev/null 2>&1; then
            local cpu_count
            cpu_count="$(nproc)"
            if [[ "${jobs_value}" -gt "${cpu_count}" ]]; then
                jobs_value="${cpu_count}"
            fi
        fi
    fi

    printf "%s" "${jobs_value}"
}

append_summary() {
    local label="$1"
    local clients="$2"
    local duration="$3"
    local log_path="$4"
    local transactions=""
    local latency_avg=""
    local p50_ms=""
    local p95_ms=""
    local p99_ms=""
    local tps=""
    local run_dir
    run_dir="$(dirname "${log_path}")"

    if [[ -f "${log_path}" ]]; then
        transactions="$(awk -F': ' '/number of transactions actually processed/ {print $2; exit}' "${log_path}")"
        latency_avg="$(awk -F'= ' '/latency average/ {print $2; exit}' "${log_path}" | awk '{print $1}')"
        tps="$(awk -F'= ' '/tps =/ {value = $2} END {print value}' "${log_path}" | awk '{print $1}')"
    fi

    if compgen -G "${run_dir}/txlog.*" >/dev/null || compgen -G "${run_dir}/pgbench_log*" >/dev/null; then
        local latency_file="${TMP_DIR}/$(basename "${run_dir}")_latency_us.txt"
        local sorted_latency_file="${TMP_DIR}/$(basename "${run_dir}")_latency_us_sorted.txt"
        local tx_count=""
        local p50_rank=""
        local p95_rank=""
        local p99_rank=""

        awk '
            $3 != "skipped" && $3 ~ /^[0-9]+$/ { print $3 }
        ' "${run_dir}"/txlog.* "${run_dir}"/pgbench_log* 2>/dev/null > "${latency_file}" || true

        tx_count="$(wc -l < "${latency_file}" | tr -d ' ')"

        if [[ "${tx_count}" -gt 0 ]]; then
            sort -n "${latency_file}" > "${sorted_latency_file}"

            p50_rank=$(( (tx_count * 50 + 99) / 100 ))
            p95_rank=$(( (tx_count * 95 + 99) / 100 ))
            p99_rank=$(( (tx_count * 99 + 99) / 100 ))

            p50_ms="$(awk -v target="${p50_rank}" 'NR == target { printf "%.3f", $1 / 1000.0; exit }' "${sorted_latency_file}")"
            p95_ms="$(awk -v target="${p95_rank}" 'NR == target { printf "%.3f", $1 / 1000.0; exit }' "${sorted_latency_file}")"
            p99_ms="$(awk -v target="${p99_rank}" 'NR == target { printf "%.3f", $1 / 1000.0; exit }' "${sorted_latency_file}")"
        fi
    fi

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "${label}" \
        "${clients}" \
        "${duration}" \
        "${transactions}" \
        "${latency_avg}" \
        "${p50_ms}" \
        "${p95_ms}" \
        "${p99_ms}" \
        "${tps}" \
        "${log_path}" \
        >> "${SUMMARY_FILE}"
}

run_pgbench_section() {
    local section_name="$1"
    local clients="$2"
    local duration="$3"
    local label="${section_name#pgbench_}"
    local jobs_value
    local section_file="${TMP_DIR}/${section_name}.sql"
    local run_dir="${RAW_DIR}/${label}_c${clients}"
    local log_file="${run_dir}/pgbench.log"

    jobs_value="$(effective_jobs "${clients}")"

    mkdir -p "${run_dir}"
    extract_section "${section_name}" "${section_file}"

    cat > "${run_dir}/context.txt" <<EOF
section=${section_name}
label=${label}
clients=${clients}
duration=${duration}
jobs=${jobs_value}
timestamp=$(date '+%Y-%m-%d %H:%M:%S')
EOF

    (
        cd "${run_dir}"
        "${PGBENCH_BIN}" \
            -h "${HOST}" \
            -p "${PORT}" \
            -U "${DB_USER_NAME}" \
            -n \
            -r \
            -l \
            --log-prefix=txlog \
            -c "${clients}" \
            -j "${jobs_value}" \
            -T "${duration}" \
            -f "${section_file}" \
            "${DATABASE}"
    ) > "${log_file}" 2>&1

    append_summary "${label}" "${clients}" "${duration}" "${log_file}"
}

run_workload_matrix() {
    local clients_csv="$1"
    local duration="$2"
    local workload_sections=(
        "pgbench_w1_insert"
        "pgbench_w2_dist_key_lookup"
        "pgbench_w3_non_dist_filter"
        "pgbench_w4_dist_key_aggregate"
        "pgbench_w5_non_dist_aggregate"
        "pgbench_w6_colocated_join"
        "pgbench_w7_replication_join"
        "pgbench_w8_gtm_short_tx"
    )
    local client
    local section_name
    IFS=',' read -r -a client_values <<< "${clients_csv}"

    for client in "${client_values[@]}"; do
        for section_name in "${workload_sections[@]}"; do
            local label="${section_name#pgbench_}"
            if selected_workload "${label}"; then
                run_pgbench_section "${section_name}" "${client}" "${duration}"
            fi
        done
    done
}

run_analyze_mode() {
    if [[ -n "${SECTION}" ]]; then
        run_psql_section "${SECTION}" "${SECTION}"
        return
    fi

    if [[ -n "${SUITE_SECTION}" ]]; then
        run_psql_section "${SUITE_SECTION}" "${SUITE_SECTION}"
        return
    fi

    run_psql_section "explain" "explain"
    run_psql_section "distribution" "distribution"
}

record_context
init_summary

case "${MODE}" in
    setup)
        run_psql_file "${SCRIPT_DIR}/schema.sql" "setup_schema"
        ;;
    load)
        run_psql_file "${SCRIPT_DIR}/load_data.sql" "load_data"
        ;;
    warmup)
        local_clients="${CLIENTS}"
        local_duration="${DURATION}"
        if [[ "${CLIENTS_SET}" -eq 0 ]]; then
            local_clients="${WARMUP_CLIENTS}"
        fi
        if [[ "${DURATION_SET}" -eq 0 ]]; then
            local_duration="${WARMUP_DURATION}"
        fi
        if [[ -n "${SUITE_SECTION}" ]]; then
            run_psql_section "${SUITE_SECTION}" "${SUITE_SECTION}"
        else
            run_workload_matrix "${local_clients}" "${local_duration}"
        fi
        ;;
    run)
        if [[ -n "${SUITE_SECTION}" ]]; then
            run_psql_section "${SUITE_SECTION}" "${SUITE_SECTION}"
        else
            run_workload_matrix "${CLIENTS}" "${DURATION}"
        fi
        ;;
    analyze)
        run_analyze_mode
        ;;
    cleanup)
        run_psql_section "cleanup" "cleanup"
        ;;
    all)
        run_psql_file "${SCRIPT_DIR}/schema.sql" "setup_schema"
        run_psql_file "${SCRIPT_DIR}/load_data.sql" "load_data"
        run_workload_matrix "${WARMUP_CLIENTS}" "${WARMUP_DURATION}"
        run_workload_matrix "${CLIENTS}" "${DURATION}"
        run_psql_section "explain" "explain"
        run_psql_section "distribution" "distribution"
        ;;
    *)
        echo "Unknown mode: ${MODE}" >&2
        usage
        exit 1
        ;;
esac
