#!/usr/bin/env bash
#
# Reproducible benchmark for the pgvector index build diagnostics.
#
# The task asks for a way to reproduce the reported numbers, so this script
# walks through the whole story in five steps:
#
#   1. collect the environment
#   2. build an index with the default maintenance_work_mem and read
#      vector_index_build_report()
#   3. ask the tools for a recommended setting and rebuild with it
#   4. verify: estimate vs measured, spill, recall@k, and the diagnose output
#   5. write a markdown report
#
# Everything is driven by environment variables, so a run is repeatable:
#
#   ROWS=100000 DIMS=128 M=16 REPEATS=3 ./run_bench.sh
#
# Notes that matter when running this:
#
#   * vector_index_build_report() is backend-local.  CREATE INDEX and the
#     report query MUST be sent in the same psql session, hence the heredoc.
#   * this build also emits a pre-build NOTICE, so every data row is tagged
#     with a BENCHROW| prefix and pulled back out with grep.
#   * only POSIX-ish shell and awk are used, no bash 4 constructs.

set -uo pipefail

ROWS="${ROWS:-100000}"
DIMS="${DIMS:-128}"
M="${M:-16}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-64}"
EF_SEARCH="${EF_SEARCH:-40}"
K="${K:-10}"
SAMPLES="${SAMPLES:-20}"
REPEATS="${REPEATS:-3}"
START_MEM="${START_MEM:-64MB}"

PGHOST_DIR="${PGHOST_DIR:-/tmp/pg18-sock3}"
PGPORT="${PGPORT:-55435}"
PGUSER="${PGUSER:-yzy}"
PGDATABASE="${PGDATABASE:-postgres}"

BENCHDIR="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-$BENCHDIR/report.md}"
WORK="$(mktemp -d)"
BASE_FILE="$WORK/baseline.txt"
REC_FILE="$WORK/recommended.txt"
: > "$BASE_FILE"
: > "$REC_FILE"

PSQL=(psql -h "$PGHOST_DIR" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -tA -v ON_ERROR_STOP=1)

# ---------------------------------------------------------------- helpers

# Median of the numbers read from stdin.
median() {
	sort -n | awk '{ a[NR] = $1 } END {
		if (NR == 0) { print "NA"; exit }
		if (NR % 2) printf "%.1f", a[(NR + 1) / 2]
		else        printf "%.1f", (a[NR / 2] + a[NR / 2 + 1]) / 2
	}'
}

# Pull one key out of a BENCHROW line: row_value "$row" load -> 4387.0
row_value() {
	printf '%s\n' "$1" | tr '|' '\n' | awk -F= -v key="$2" '$1 == key { print $2 }'
}

# Median of one key across every row of a results file.
median_of() {
	local file="$1"
	local key="$2"
	local line
	while IFS= read -r line; do
		[ -n "$line" ] && row_value "$line" "$key"
	done < "$file" | median
}

# Keep only the tagged data row out of whatever psql printed.
bench_row() {
	grep '^BENCHROW|' | tail -1
}

log() {
	printf '==> %s\n' "$*"
}

# Build the index with a given maintenance_work_mem and print one BENCHROW.
build_and_report() {
	local mem="$1"
	psql -h "$PGHOST_DIR" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -tA -v ON_ERROR_STOP=1 <<SQL | bench_row
-- CREATE INDEX takes its workers from max_parallel_maintenance_workers, not
-- from max_parallel_workers_per_gather, so both have to be off for the run to
-- really be serial.
SET max_parallel_maintenance_workers = 0;
SET max_parallel_workers_per_gather = 0;
SET hnsw.ef_search = ${EF_SEARCH};
SET maintenance_work_mem = '${mem}';
DROP INDEX IF EXISTS bench_build_idx;
CREATE INDEX bench_build_idx ON bench_vec USING hnsw (v vector_l2_ops) WITH (m = ${M}, ef_construction = ${EF_CONSTRUCTION});
SELECT 'BENCHROW|total=' || round(total_ms::numeric, 1)
    || '|load='   || round(load_ms::numeric, 1)
    || '|flush='  || round(flush_ms::numeric, 1)
    || '|fin='    || round(finalize_ms::numeric, 1)
    || '|peak='   || memory_peak
    || '|budget=' || memory_total
    || '|spill='  || flushed_to_disk
FROM vector_index_build_report();
SQL
}

# Render a results file as one markdown table row.
table_row() {
	local label="$1"
	local file="$2"
	if [ ! -s "$file" ]; then
		printf '| %s | NA | NA | NA | NA | NA | NA | NA |\n' "$label"
		return
	fi
	local total load flush fin peak budget spill
	total="$(median_of "$file" total)"
	load="$(median_of "$file" load)"
	flush="$(median_of "$file" flush)"
	fin="$(median_of "$file" fin)"
	peak="$(median_of "$file" peak)"
	budget="$(median_of "$file" budget)"
	spill="$(awk -F'|' 'NR == 1 { sub(/^spill=/, "", $8); print $8 }' "$file")"
	printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' \
		"$label" "$total" "$load" "$flush" "$fin" "$peak" "$budget" "$spill"
}

# ------------------------------------------------------- step 1: environment

log "step 1/5: collecting the environment"

PG_VERSION="$("${PSQL[@]}" -c 'SELECT version();')"
KERNEL="$(uname -srm)"
CPU_MODEL="$(grep -m1 'model name' /proc/cpuinfo | sed 's/.*: //')"
CPU_CORES="$(nproc)"
RAM_GB="$(free -g | awk '/^Mem:/ { print $2 }')"
COMPILER="$(gcc --version | head -1)"
BUILD_FLAGS="${BUILD_FLAGS:-make OPTFLAGS=\"\" (see bench/README.md)}"

log "    ${CPU_CORES} cores, ${RAM_GB} GB RAM, $(printf '%s' "$PG_VERSION" | cut -d, -f1)"

# --------------------------------------------------- step 2: baseline build

log "step 2/5: baseline build with maintenance_work_mem=${START_MEM}"

"${PSQL[@]}" <<SQL > /dev/null
DROP TABLE IF EXISTS bench_vec;
CREATE TABLE bench_vec (id serial PRIMARY KEY, v vector(${DIMS}));
INSERT INTO bench_vec (v)
SELECT ARRAY(SELECT random() FROM generate_series(1, ${DIMS}))::vector(${DIMS})
FROM generate_series(1, ${ROWS});
ANALYZE bench_vec;
SQL

i=1
while [ "$i" -le "$REPEATS" ]; do
	log "    run ${i}/${REPEATS}"
	build_and_report "$START_MEM" >> "$BASE_FILE"
	i=$((i + 1))
done

# --------------------------------------------- step 3: rebuild as recommended

log "step 3/5: asking the tools for a recommended maintenance_work_mem"

ESTIMATE_LINE="$("${PSQL[@]}" -c \
	"SELECT estimated_bytes || '|' || estimated_pretty || '|' || suggested_work_mem FROM vector_index_memory_estimate(${ROWS}, ${DIMS}, ${M});")"
EST_BYTES="$(printf '%s\n' "$ESTIMATE_LINE" | awk -F'|' '{ print $1 }')"
EST_PRETTY="$(printf '%s\n' "$ESTIMATE_LINE" | awk -F'|' '{ print $2 }')"
REC_MEM="$(printf '%s\n' "$ESTIMATE_LINE" | awk -F'|' '{ print $3 }')"

log "    estimate=${EST_PRETTY}, suggested maintenance_work_mem=${REC_MEM}"

ADVISE_LINE="$("${PSQL[@]}" -c \
	"SELECT rows_est || '|' || dims || '|' || estimated_bytes || '|' || current_work_mem || '|' || suggested_work_mem || '|' || fits FROM vector_index_memory_advise('bench_vec', ${M});")"

i=1
while [ "$i" -le "$REPEATS" ]; do
	log "    rebuild run ${i}/${REPEATS} with ${REC_MEM}"
	build_and_report "$REC_MEM" >> "$REC_FILE"
	i=$((i + 1))
done

# -------------------------------------------------------- step 4: verify

log "step 4/5: verifying"

RECALL_LINE="$("${PSQL[@]}" -c \
	"SET hnsw.ef_search = ${EF_SEARCH}; SELECT k_out || '|' || samples_used || '|' || recall || '|' || assessment FROM vector_index_recall_check('bench_build_idx', ${K}, ${SAMPLES});" | tail -1)"
RECALL_K="$(printf '%s\n' "$RECALL_LINE" | awk -F'|' '{ print $1 }')"
RECALL_SAMPLES="$(printf '%s\n' "$RECALL_LINE" | awk -F'|' '{ print $2 }')"
RECALL_VALUE="$(printf '%s\n' "$RECALL_LINE" | awk -F'|' '{ print $3 }')"
RECALL_ASSESS="$(printf '%s\n' "$RECALL_LINE" | awk -F'|' '{ print $4 }')"

DIAGNOSE_TABLE="$("${PSQL[@]}" -c \
	"SELECT '| ' || aspect || ' | ' || severity || ' | ' || detail || ' | ' || suggestion || ' |' FROM vector_index_diagnose('bench_build_idx', true);" | grep '^| ' || true)"

BASE_SPILL="$(awk -F'|' 'NR == 1 { sub(/^spill=/, "", $8); print $8 }' "$BASE_FILE")"
REC_SPILL="$(awk -F'|' 'NR == 1 { sub(/^spill=/, "", $8); print $8 }' "$REC_FILE")"

MEASURED_PEAK="$(median_of "$REC_FILE" peak)"
if [ -n "$MEASURED_PEAK" ] && [ "$MEASURED_PEAK" != "NA" ] && [ -n "$EST_BYTES" ] && [ "$EST_BYTES" != "NA" ]; then
	DEVIATION="$(awk -v e="$EST_BYTES" -v m="$MEASURED_PEAK" 'BEGIN { printf "%.1f", (m - e) * 100.0 / e }')"
else
	DEVIATION="NA"
fi

# ---------------------------------------------------- step 5: write report

log "step 5/5: writing ${OUT}"

{
	printf '# pgvector index build diagnostics: reproduction report\n\n'
	printf 'Generated by `bench/run_bench.sh` on %s.\n\n' "$(date -u '+%Y-%m-%d %H:%M UTC')"

	printf '## 1. Environment\n\n'
	printf '| Item | Value |\n| --- | --- |\n'
	printf '| PostgreSQL | %s |\n' "$PG_VERSION"
	printf '| OS / kernel | %s |\n' "$KERNEL"
	printf '| CPU | %s (%s cores) |\n' "$CPU_MODEL" "$CPU_CORES"
	printf '| Memory | %s GB |\n' "$RAM_GB"
	printf '| Compiler | %s |\n' "$COMPILER"
	printf '| Build flags | %s |\n' "$BUILD_FLAGS"
	printf '| Dataset | %s rows x %s dimensions |\n' "$ROWS" "$DIMS"
	printf '| HNSW parameters | m = %s, ef_construction = %s, hnsw.ef_search = %s |\n' "$M" "$EF_CONSTRUCTION" "$EF_SEARCH"
	printf '| Repeats | %s per configuration |\n' "$REPEATS"
	printf '| Statistics | median of %s runs for timings and peak memory |\n' "$REPEATS"
	printf '| Parallelism | serial build: max_parallel_maintenance_workers = 0 and max_parallel_workers_per_gather = 0 |\n\n'

	printf '## 2. Baseline build (maintenance_work_mem = %s)\n\n' "$START_MEM"
	printf '| Configuration | total ms | load ms | flush ms | finalize ms | memory peak (B) | memory budget (B) | spilled to disk |\n'
	printf '| --- | --- | --- | --- | --- | --- | --- | --- |\n'
	table_row "baseline (${START_MEM})" "$BASE_FILE"
	printf '\n'

	printf '## 3. Rebuild with the recommended setting\n\n'
	printf '`vector_index_memory_estimate(%s, %s, %s)` reports **%s** (%s bytes) and suggests\n' "$ROWS" "$DIMS" "$M" "$EST_PRETTY" "$EST_BYTES"
	printf '`maintenance_work_mem = %s`.\n\n' "$REC_MEM"
	printf '`vector_index_memory_advise(%s, %s)` reports:\n\n' "'bench_vec'" "$M"
	printf '```\n%s\n```\n' "$ADVISE_LINE"
	printf '(rows_est | dims | estimated_bytes | current_work_mem | suggested_work_mem | fits)\n\n'
	printf '| Configuration | total ms | load ms | flush ms | finalize ms | memory peak (B) | memory budget (B) | spilled to disk |\n'
	printf '| --- | --- | --- | --- | --- | --- | --- | --- |\n'
	table_row "recommended (${REC_MEM})" "$REC_FILE"
	printf '\n'

	printf '## 4. Verification\n\n'
	printf '| Check | Result |\n| --- | --- |\n'
	printf '| Estimated memory | %s (%s bytes) |\n' "$EST_PRETTY" "$EST_BYTES"
	printf '| Measured peak memory | %s bytes |\n' "$MEASURED_PEAK"
	printf '| Estimate vs measured | %s%% (negative means the measured peak is below the estimate, i.e. the estimate is conservative) |\n' "$DEVIATION"
	printf '| Spilled to disk (baseline) | %s |\n' "$BASE_SPILL"
	printf '| Spilled to disk (recommended) | %s |\n' "$REC_SPILL"
	printf '| recall@%s over %s queries (recommended build) | %s (%s) |\n' \
		"${RECALL_K:-NA}" "${RECALL_SAMPLES:-NA}" "${RECALL_VALUE:-NA}" "${RECALL_ASSESS:-NA}"
	printf '\n'
	printf '### vector_index_diagnose() output\n\n'
	printf '| aspect | severity | detail | suggestion |\n| --- | --- | --- | --- |\n'
	if [ -n "$DIAGNOSE_TABLE" ]; then
		printf '%s\n' "$DIAGNOSE_TABLE"
	else
		printf '| NA | NA | diagnose produced no rows | NA |\n'
	fi
	printf '\n'

	printf '## 5. How to reproduce\n\n'
	printf '```\ncd contrib/pgvector/bench\n'
	printf 'ROWS=%s DIMS=%s M=%s REPEATS=%s ./run_bench.sh\n```\n\n' "$ROWS" "$DIMS" "$M" "$REPEATS"
	printf 'See `bench/README.md` for the prerequisites and for the known caveats\n'
	printf '(backend-local report, pre-build NOTICE on stdout, and the\n'
	printf '`-march=native` build issue).\n'
} > "$OUT"

rm -rf "$WORK"

log "done: ${OUT}"
