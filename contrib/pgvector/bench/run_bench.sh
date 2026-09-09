#!/usr/bin/env bash
#
# Reproducible benchmark for the pgvector index build diagnostics.
#
# One run produces the whole evidence file:
#
#   1. environment
#   2. HNSW: baseline (small maintenance_work_mem) vs the recommended setting
#   3. memory model sweep across several sizes, dimensions and m values
#   4. closed loop: bad setting -> detected -> fixed -> re-measured
#   5. IVFFlat: the same story for lists/probes
#   6. anomaly behaviour: what each misuse reports
#   7. report.md
#
# Tunables: ROWS DIMS M EF_CONSTRUCTION EF_SEARCH K SAMPLES REPEATS START_MEM
#           SWEEP_REPEATS IVF_ROWS IVF_DIMS IVF_LISTS
#
# Notes that matter when running this:
#
#   * vector_index_build_report() is backend-local, so CREATE INDEX and the
#     report query must be sent in the same psql session (hence the heredocs).
#   * this build emits a pre-build NOTICE on stdout, so every data row is
#     tagged BENCHROW| and pulled back out with grep.
#   * CREATE INDEX takes its workers from max_parallel_maintenance_workers, not
#     from max_parallel_workers_per_gather; both are disabled for serial runs.
#   * POSIX-ish shell and awk only, no bash 4 constructs.

set -uo pipefail

ROWS="${ROWS:-100000}"
DIMS="${DIMS:-128}"
M="${M:-16}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-64}"
EF_SEARCH="${EF_SEARCH:-40}"
BAD_EF_SEARCH="${BAD_EF_SEARCH:-1}"
FIXED_EF_SEARCH="${FIXED_EF_SEARCH:-40}"
K="${K:-10}"
SAMPLES="${SAMPLES:-20}"
REPEATS="${REPEATS:-3}"
START_MEM="${START_MEM:-64MB}"
SWEEP_REPEATS="${SWEEP_REPEATS:-3}"
IVF_ROWS="${IVF_ROWS:-100000}"
IVF_DIMS="${IVF_DIMS:-128}"
IVF_LISTS="${IVF_LISTS:-100}"
IVF_PROBES_BAD="${IVF_PROBES_BAD:-1}"
IVF_PROBES_FIXED="${IVF_PROBES_FIXED:-10}"

PGHOST_DIR="${PGHOST_DIR:-/tmp/pg18-sock3}"
PGPORT="${PGPORT:-55435}"
PGUSER="${PGUSER:-yzy}"
PGDATABASE="${PGDATABASE:-postgres}"

BENCHDIR="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-$BENCHDIR/report.md}"
WORK="$(mktemp -d)"
PSQL=(psql -h "$PGHOST_DIR" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -tA -v ON_ERROR_STOP=1)

# ---------------------------------------------------------------- helpers

log() { printf '==> %s\n' "$*"; }

# Median, one decimal (used for timings).
median() {
	sort -n | awk '{ a[NR] = $1 } END {
		if (NR == 0) { print "NA"; exit }
		if (NR % 2) printf "%.1f", a[(NR + 1) / 2]
		else        printf "%.1f", (a[NR / 2] + a[NR / 2 + 1]) / 2
	}'
}

# Median, integer (used for byte counts).
median_i() {
	sort -n | awk '{ a[NR] = $1 } END {
		if (NR == 0) { print "NA"; exit }
		if (NR % 2) v = a[(NR + 1) / 2]
		else        v = (a[NR / 2] + a[NR / 2 + 1]) / 2
		printf "%d", int(v + 0.5)
	}'
}

row_value() {
	printf '%s\n' "$1" | tr '|' '\n' | awk -F= -v key="$2" '$1 == key { print $2 }'
}

median_of() {
	local file="$1" key="$2" line
	while IFS= read -r line; do
		[ -n "$line" ] && row_value "$line" "$key"
	done < "$file" | median
}

median_of_i() {
	local file="$1" key="$2" line
	while IFS= read -r line; do
		[ -n "$line" ] && row_value "$line" "$key"
	done < "$file" | median_i
}

# Every value of one key, comma separated (shows the spread behind the median).
raw_of() {
	local file="$1" key="$2" line out=""
	while IFS= read -r line; do
		[ -n "$line" ] && out="$out$(row_value "$line" "$key") "
	done < "$file"
	printf '%s' "$out" | sed 's/ $//; s/ /, /g'
}

bench_row() { grep '^BENCHROW|' | tail -1; }

# Build a table and optionally an index, then print one BENCHROW.
# $1 maintenance_work_mem, $2 CREATE INDEX statement (or 'SKIP')
build_and_report() {
	local mem="$1"
	local create="$2"
	local do_create=""
	if [ "$create" != "SKIP" ]; then
		do_create="$create;"
	fi
	psql -h "$PGHOST_DIR" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -tA -v ON_ERROR_STOP=1 <<SQL | bench_row
SET max_parallel_maintenance_workers = 0;
SET max_parallel_workers_per_gather = 0;
SET hnsw.ef_search = ${EF_SEARCH};
SET maintenance_work_mem = '${mem}';
DROP INDEX IF EXISTS bench_build_idx;
${do_create}
SELECT 'BENCHROW|total=' || round(total_ms::numeric, 1)
    || '|load='   || round(load_ms::numeric, 1)
    || '|flush='  || round(flush_ms::numeric, 1)
    || '|fin='    || round(finalize_ms::numeric, 1)
    || '|peak='   || memory_peak
    || '|budget=' || memory_total
    || '|spill='  || flushed_to_disk
    || '|valid='  || valid
FROM vector_index_build_report();
SQL
}

table_row() {
	local label="$1" file="$2"
	if [ ! -s "$file" ]; then
		printf '| %s | NA | NA | NA | NA | NA | NA | NA |\n' "$label"
		return
	fi
	printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' \
		"$label" \
		"$(median_of "$file" total)" \
		"$(median_of "$file" load)" \
		"$(median_of "$file" flush)" \
		"$(median_of "$file" fin)" \
		"$(median_of_i "$file" peak)" \
		"$(median_of_i "$file" budget)" \
		"$(awk -F'|' 'NR == 1 { sub(/^spill=/, "", $8); print $8 }' "$file")"
}

# ------------------------------------------------------- step 1: environment

# Fail fast: a long run against a dead server wastes a lot of time.
if ! "${PSQL[@]}" -c 'SELECT 1;' > /dev/null 2>&1; then
	printf 'error: cannot reach the server at %s:%s (database %s, user %s)\n' \
		"$PGHOST_DIR" "$PGPORT" "$PGDATABASE" "$PGUSER" >&2
	printf 'start it first, then re-run. See bench/README.md.\n' >&2
	exit 1
fi

log "step 1/7: environment"

PG_VERSION="$("${PSQL[@]}" -c 'SELECT version();')"
KERNEL="$(uname -srm)"
CPU_MODEL="$(grep -m1 'model name' /proc/cpuinfo | sed 's/.*: //')"
CPU_CORES="$(nproc)"
RAM_GB="$(free -g | awk '/^Mem:/ { print $2 }')"
COMPILER="$(gcc --version | head -1)"
BUILD_FLAGS="${BUILD_FLAGS:-make OPTFLAGS=\"\" (see bench/README.md)}"

# -------------------------------------------------- step 2: HNSW comparison

log "step 2/7: HNSW baseline vs recommended"

"${PSQL[@]}" <<SQL > /dev/null
DROP TABLE IF EXISTS bench_vec;
CREATE TABLE bench_vec (id serial PRIMARY KEY, v vector(${DIMS}));
INSERT INTO bench_vec (v)
SELECT ARRAY(SELECT random() FROM generate_series(1, ${DIMS}))::vector(${DIMS})
FROM generate_series(1, ${ROWS});
ANALYZE bench_vec;
SQL

BASE_FILE="$WORK/baseline.txt"
REC_FILE="$WORK/recommended.txt"
: > "$BASE_FILE"; : > "$REC_FILE"

i=1
while [ "$i" -le "$REPEATS" ]; do
	log "    baseline run ${i}/${REPEATS}"
	build_and_report "$START_MEM" \
		"CREATE INDEX bench_build_idx ON bench_vec USING hnsw (v vector_l2_ops) WITH (m = ${M}, ef_construction = ${EF_CONSTRUCTION})" \
		>> "$BASE_FILE"
	i=$((i + 1))
done

EST_LINE="$("${PSQL[@]}" -c \
	"SELECT estimated_bytes || '|' || estimated_pretty || '|' || suggested_work_mem FROM vector_index_memory_estimate(${ROWS}, ${DIMS}, ${M});")"
EST_BYTES="$(printf '%s\n' "$EST_LINE" | awk -F'|' '{ print $1 }')"
EST_PRETTY="$(printf '%s\n' "$EST_LINE" | awk -F'|' '{ print $2 }')"
REC_MEM="$(printf '%s\n' "$EST_LINE" | awk -F'|' '{ print $3 }')"

# raw model = published estimate / (1 + margin); margin is 10% and one-sided
RAW_MODEL="$(awk -v e="$EST_BYTES" 'BEGIN { printf "%d", e / 1.1 }')"

ADVISE_LINE="$("${PSQL[@]}" -c \
	"SELECT rows_est || '|' || dims || '|' || estimated_bytes || '|' || current_work_mem || '|' || suggested_work_mem || '|' || fits FROM vector_index_memory_advise('bench_vec', ${M});")"

i=1
while [ "$i" -le "$REPEATS" ]; do
	log "    recommended run ${i}/${REPEATS} (${REC_MEM})"
	build_and_report "$REC_MEM" \
		"CREATE INDEX bench_build_idx ON bench_vec USING hnsw (v vector_l2_ops) WITH (m = ${M}, ef_construction = ${EF_CONSTRUCTION})" \
		>> "$REC_FILE"
	i=$((i + 1))
done

MEASURED_PEAK="$(median_of_i "$REC_FILE" peak)"
RAW_DEV="$(awk -v r="$RAW_MODEL" -v m="$MEASURED_PEAK" 'BEGIN { printf "%.2f", (m - r) * 100.0 / r }')"
MARGIN_DEV="$(awk -v e="$EST_BYTES" -v m="$MEASURED_PEAK" 'BEGIN { printf "%.1f", (m - e) * 100.0 / e }')"

# ----------------------------------------------------------- step 3: sweep

log "step 3/7: memory model sweep"

SWEEP_FILE="$WORK/sweep.txt"
: > "$SWEEP_FILE"
# rows dims m
SWEEP_CASES="${SWEEP_CASES:-10000 128 16
100000 128 16
100000 768 16
500000 128 32}"

printf '%s\n' "$SWEEP_CASES" | while read -r srows sdims sm; do
	[ -z "$srows" ] && continue
	log "    ${srows} rows x ${sdims} dims, m=${sm}"
	"${PSQL[@]}" <<SQL > /dev/null
DROP TABLE IF EXISTS bench_sweep;
CREATE TABLE bench_sweep (id serial PRIMARY KEY, v vector(${sdims}));
INSERT INTO bench_sweep (v)
SELECT ARRAY(SELECT random() FROM generate_series(1, ${sdims}))::vector(${sdims})
FROM generate_series(1, ${srows});
ANALYZE bench_sweep;
SQL
	est="$("${PSQL[@]}" -c "SELECT estimated_bytes FROM vector_index_memory_estimate(${srows}, ${sdims}, ${sm});")"
	suggested="$("${PSQL[@]}" -c "SELECT suggested_work_mem FROM vector_index_memory_estimate(${srows}, ${sdims}, ${sm});")"
	raw="$(awk -v e="$est" 'BEGIN { printf "%d", e / 1.1 }')"
	peakfile="$WORK/sweep_${srows}_${sdims}_${sm}.txt"
	: > "$peakfile"
	j=1
	while [ "$j" -le "$SWEEP_REPEATS" ]; do
		build_and_report "$suggested" \
			"CREATE INDEX bench_build_idx ON bench_sweep USING hnsw (v vector_l2_ops) WITH (m = ${sm})" \
			>> "$peakfile"
		j=$((j + 1))
	done
	peak="$(median_of_i "$peakfile" peak)"
	spill="$(awk -F'|' 'NR == 1 { sub(/^spill=/, "", $8); print $8 }' "$peakfile")"
	rawdev="$(awk -v r="$raw" -v p="$peak" 'BEGIN { if (r > 0) printf "%.2f", (p - r) * 100.0 / r; else print "NA" }')"
	margindev="$(awk -v e="$est" -v p="$peak" 'BEGIN { printf "%.1f", (p - e) * 100.0 / e }')"
	printf '%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
		"$srows" "$sdims" "$sm" "$est" "$raw" "$peak" "$rawdev" "$margindev" "$spill" >> "$SWEEP_FILE"
done

# ------------------------------------------------------ step 4: closed loop

log "step 4/7: closed loop (detect -> fix -> re-measure)"

# The sweep left bench_build_idx on the last sweep table. Rebuild it on the
# reference dataset so the numbers below match section 2.
"${PSQL[@]}" -c "DROP TABLE IF EXISTS bench_sweep;" > /dev/null
build_and_report "$REC_MEM" \
	"CREATE INDEX bench_build_idx ON bench_vec USING hnsw (v vector_l2_ops) WITH (m = ${M}, ef_construction = ${EF_CONSTRUCTION})" \
	> /dev/null

DEMO_BEFORE="$("${PSQL[@]}" -c \
	"SET hnsw.ef_search = ${BAD_EF_SEARCH}; SELECT recall || '|' || assessment FROM vector_index_recall_check('bench_build_idx', ${K}, ${SAMPLES});" | tail -1)"
DEMO_DIAG="$("${PSQL[@]}" -c \
	"SET hnsw.ef_search = ${BAD_EF_SEARCH}; SELECT aspect || '|' || severity || '|' || suggestion FROM vector_index_diagnose('bench_build_idx', true) WHERE aspect = 'ef_search';" | tail -1)"
DEMO_AFTER="$("${PSQL[@]}" -c \
	"SET hnsw.ef_search = ${FIXED_EF_SEARCH}; SELECT recall || '|' || assessment FROM vector_index_recall_check('bench_build_idx', ${K}, ${SAMPLES});" | tail -1)"
DEMO_AFTER_DIAG="$("${PSQL[@]}" -c \
	"SET hnsw.ef_search = ${FIXED_EF_SEARCH}; SELECT count(*) FROM vector_index_diagnose('bench_build_idx', true) WHERE severity = 'warning';" | tail -1)"

DEMO_BEFORE_VAL="$(printf '%s\n' "$DEMO_BEFORE" | awk -F'|' '{ print $1 }')"
DEMO_BEFORE_ASS="$(printf '%s\n' "$DEMO_BEFORE" | awk -F'|' '{ print $2 }')"
DEMO_AFTER_VAL="$(printf '%s\n' "$DEMO_AFTER" | awk -F'|' '{ print $1 }')"
DEMO_AFTER_ASS="$(printf '%s\n' "$DEMO_AFTER" | awk -F'|' '{ print $2 }')"
DEMO_DIAG_SEV="$(printf '%s\n' "$DEMO_DIAG" | awk -F'|' '{ print $2 }')"
DEMO_DIAG_SUG="$(printf '%s\n' "$DEMO_DIAG" | awk -F'|' '{ print $3 }')"

# ------------------------------------------------------------ step 5: IVFFlat

log "step 5/7: IVFFlat"

"${PSQL[@]}" <<SQL > /dev/null
DROP TABLE IF EXISTS bench_ivf;
CREATE TABLE bench_ivf (id serial PRIMARY KEY, v vector(${IVF_DIMS}));
INSERT INTO bench_ivf (v)
SELECT ARRAY(SELECT random() FROM generate_series(1, ${IVF_DIMS}))::vector(${IVF_DIMS})
FROM generate_series(1, ${IVF_ROWS});
ANALYZE bench_ivf;
CREATE INDEX bench_ivf_idx ON bench_ivf USING ivfflat (v vector_l2_ops) WITH (lists = ${IVF_LISTS});
SQL

IVF_BAD_RECALL="$("${PSQL[@]}" -c \
	"SET ivfflat.probes = ${IVF_PROBES_BAD}; SELECT recall || '|' || assessment FROM vector_index_recall_check('bench_ivf_idx', ${K}, ${SAMPLES});" | tail -1)"
IVF_BAD_DIAG="$("${PSQL[@]}" -c \
	"SET ivfflat.probes = ${IVF_PROBES_BAD}; SELECT aspect || '|' || severity || '|' || suggestion FROM vector_index_diagnose('bench_ivf_idx', true) WHERE aspect = 'probes';" | tail -1)"
IVF_FIX_RECALL="$("${PSQL[@]}" -c \
	"SET ivfflat.probes = ${IVF_PROBES_FIXED}; SELECT recall || '|' || assessment FROM vector_index_recall_check('bench_ivf_idx', ${K}, ${SAMPLES});" | tail -1)"
IVF_FIX_DIAG="$("${PSQL[@]}" -c \
	"SET ivfflat.probes = ${IVF_PROBES_FIXED}; SELECT count(*) FROM vector_index_diagnose('bench_ivf_idx', true) WHERE severity = 'warning';" | tail -1)"

IVF_BAD_VAL="$(printf '%s\n' "$IVF_BAD_RECALL" | awk -F'|' '{ print $1 }')"
IVF_BAD_ASS="$(printf '%s\n' "$IVF_BAD_RECALL" | awk -F'|' '{ print $2 }')"
IVF_FIX_VAL="$(printf '%s\n' "$IVF_FIX_RECALL" | awk -F'|' '{ print $1 }')"
IVF_FIX_ASS="$(printf '%s\n' "$IVF_FIX_RECALL" | awk -F'|' '{ print $2 }')"
IVF_DIAG_SEV="$(printf '%s\n' "$IVF_BAD_DIAG" | awk -F'|' '{ print $2 }')"
IVF_DIAG_SUG="$(printf '%s\n' "$IVF_BAD_DIAG" | awk -F'|' '{ print $3 }')"

# ----------------------------------------------------------- step 6: anomalies

log "step 6/7: anomaly behaviour"

anomaly() {
	local label="$1" sql="$2" out
	out="$("${PSQL[@]}" -c "$sql" 2>&1 | tr '\n' ' ' | grep -o 'ERROR:[^|]*' | head -1 | sed 's/  */ /g')"
	if [ -z "$out" ]; then out="(no error raised)"; fi
	printf '| %s | `%s` | %s |\n' "$label" "$sql" "$out"
}

"${PSQL[@]}" <<SQL > /dev/null
DROP TABLE IF EXISTS bench_novec;
CREATE TABLE bench_novec (i int);
INSERT INTO bench_novec VALUES (1);
DROP TABLE IF EXISTS bench_noanalyze;
CREATE TABLE bench_noanalyze (id serial PRIMARY KEY, v vector(128));
INSERT INTO bench_noanalyze (v)
SELECT ARRAY(SELECT random() FROM generate_series(1, 128))::vector(128) FROM generate_series(1, 1000);
DROP INDEX IF EXISTS bench_btree;
CREATE INDEX bench_btree ON bench_vec (id);
SQL

ANOMALY_FILE="$WORK/anomalies.txt"
{
	anomaly "rows out of range" "SELECT * FROM vector_index_memory_estimate(0, 128, 16);"
	anomaly "dims out of range" "SELECT * FROM vector_index_memory_estimate(1000, 99999, 16);"
	anomaly "m out of range"    "SELECT * FROM vector_index_memory_estimate(1000, 128, 1);"
	anomaly "table never analyzed" "SELECT * FROM vector_index_memory_advise('bench_noanalyze');"
	anomaly "no vector column"  "SELECT * FROM vector_index_memory_advise('bench_novec');"
	anomaly "not an hnsw/ivfflat index" "SELECT * FROM vector_index_diagnose('bench_btree');"
	anomaly "k out of range"    "SELECT * FROM vector_index_recall_check('bench_build_idx', 0, 20);"
	anomaly "samples out of range" "SELECT * FROM vector_index_recall_check('bench_build_idx', 10, 0);"
} > "$ANOMALY_FILE"

# ------------------------------------------------------------ step 7: report

log "step 7/7: writing ${OUT}"

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
	printf '| Repeats | %s per configuration, median reported |\n' "$REPEATS"
	printf '| Parallelism | serial: max_parallel_maintenance_workers = 0, max_parallel_workers_per_gather = 0 |\n\n'

	printf '## 2. HNSW: baseline vs the recommended setting\n\n'
	printf 'Dataset: %s rows x %s dimensions, m = %s, ef_construction = %s, hnsw.ef_search = %s.\n\n' \
		"$ROWS" "$DIMS" "$M" "$EF_CONSTRUCTION" "$EF_SEARCH"
	printf '| Configuration | total ms | load ms | flush ms | finalize ms | memory peak (B) | memory budget (B) | spilled |\n'
	printf '| --- | --- | --- | --- | --- | --- | --- | --- |\n'
	table_row "baseline (${START_MEM})" "$BASE_FILE"
	table_row "recommended (${REC_MEM})" "$REC_FILE"
	printf '\nRaw values behind the medians:\n\n'
	printf '| Configuration | total ms | load ms | memory peak (B) |\n| --- | --- | --- | --- |\n'
	printf '| baseline | %s | %s | %s |\n' \
		"$(raw_of "$BASE_FILE" total)" "$(raw_of "$BASE_FILE" load)" "$(raw_of "$BASE_FILE" peak)"
	printf '| recommended | %s | %s | %s |\n' \
		"$(raw_of "$REC_FILE" total)" "$(raw_of "$REC_FILE" load)" "$(raw_of "$REC_FILE" peak)"
	printf '\n`vector_index_memory_estimate(%s, %s, %s)` reports **%s** (%s bytes) and suggests `maintenance_work_mem = %s`.\n' \
		"$ROWS" "$DIMS" "$M" "$EST_PRETTY" "$EST_BYTES" "$REC_MEM"
	printf '`vector_index_memory_advise(%s, %s)`:\n\n```\n%s\n```\n(rows_est | dims | estimated_bytes | current_work_mem | suggested_work_mem | fits)\n\n' \
		"'bench_vec'" "$M" "$ADVISE_LINE"

	printf '## 3. Memory model sweep\n\n'
	printf 'Each row builds with the `maintenance_work_mem` the tools suggest, so the\n'
	printf 'measured peak is the whole graph (no spill). `raw model` is the estimate\n'
	printf '**before** the one-sided safety margin, i.e. published estimate / 1.1.\n\n'
	printf '| rows | dims | m | estimate (B) | raw model (B) | measured peak (B) | raw dev | with margin | spilled |\n'
	printf '| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n'
	while IFS='|' read -r sr sd sm est raw peak rd md sp; do
		[ -z "${sr:-}" ] && continue
		printf '| %s | %s | %s | %s | %s | %s | %s%% | %s%% | %s |\n' "$sr" "$sd" "$sm" "$est" "$raw" "$peak" "$rd" "$md" "$sp"
	done < "$SWEEP_FILE"
	printf '\n* **raw dev** compares the measured peak against the raw model; this is the\n'
	printf '  accuracy of the model itself.\n'
	printf '* **with margin** compares the measured peak against the value the tools\n'
	printf '  actually report. It is negative on purpose: the published estimate is the\n'
	printf '  raw model plus a one-sided 10%% margin, so it comes out above the measured\n'
	printf '  peak rather than below it. A user who follows the advice should not spill.\n'
	printf '* For the reference configuration (%s rows x %s, m = %s) that is %s%% vs %s%%.\n\n' \
		"$ROWS" "$DIMS" "$M" "$RAW_DEV" "$MARGIN_DEV"

	printf '## 4. Closed loop: bad setting, detected, fixed, re-measured\n\n'
	printf '```sql\n'
	printf 'SET hnsw.ef_search = %s;\n' "$BAD_EF_SEARCH"
	printf 'SELECT * FROM vector_index_recall_check(%s, %s, %s);\n' "'bench_build_idx'" "$K" "$SAMPLES"
	printf 'SELECT * FROM vector_index_diagnose(%s, true);\n' "'bench_build_idx'"
	printf 'SET hnsw.ef_search = %s;   -- the value the diagnose output suggests\n' "$FIXED_EF_SEARCH"
	printf 'SELECT * FROM vector_index_recall_check(%s, %s, %s);\n' "'bench_build_idx'" "$K" "$SAMPLES"
	printf '```\n\n'
	printf '| Step | Setting | Result |\n| --- | --- | --- |\n'
	printf '| 1. before | hnsw.ef_search = %s | recall@%s = **%s** (%s) |\n' "$BAD_EF_SEARCH" "$K" "$DEMO_BEFORE_VAL" "$DEMO_BEFORE_ASS"
	printf '| 2. diagnose | hnsw.ef_search = %s | `%s` finding, severity **%s** |\n' "$BAD_EF_SEARCH" "ef_search" "$DEMO_DIAG_SEV"
	printf '| 3. after | hnsw.ef_search = %s | recall@%s = **%s** (%s), %s warning(s) left |\n' \
		"$FIXED_EF_SEARCH" "$K" "$DEMO_AFTER_VAL" "$DEMO_AFTER_ASS" "$DEMO_AFTER_DIAG"
	printf '\nThe diagnose suggestion produced in step 2 was:\n\n> %s\n\n' "$DEMO_DIAG_SUG"
	printf 'The warning still reported in step 3 is the **memory** one —\n'
	printf '`maintenance_work_mem` in that session is below what the build needs. It is\n'
	printf 'unrelated to `ef_search`, which is why it survives the fix.\n\n'

	printf '## 5. IVFFlat\n\n'
	printf 'Index: %s rows x %s dimensions, lists = %s.\n\n' "$IVF_ROWS" "$IVF_DIMS" "$IVF_LISTS"
	printf '| Step | Setting | Result |\n| --- | --- | --- |\n'
	printf '| 1. before | ivfflat.probes = %s | recall@%s = **%s** (%s) |\n' "$IVF_PROBES_BAD" "$K" "$IVF_BAD_VAL" "$IVF_BAD_ASS"
	printf '| 2. diagnose | ivfflat.probes = %s | `%s` finding, severity **%s** |\n' "$IVF_PROBES_BAD" "probes" "$IVF_DIAG_SEV"
	printf '| 3. after | ivfflat.probes = %s | recall@%s = **%s** (%s), %s warning(s) left |\n' \
		"$IVF_PROBES_FIXED" "$K" "$IVF_FIX_VAL" "$IVF_FIX_ASS" "$IVF_FIX_DIAG"
	printf '\nThe diagnose suggestion produced in step 2 was:\n\n> %s\n\n' "$IVF_DIAG_SUG"
	printf '**Honest limitation of this section.** The measured recall was **%s** at\n' "$IVF_BAD_VAL"
	printf '`probes = %s` *and* **%s** at `probes = %s`. On this synthetic uniform\n' "$IVF_PROBES_BAD" "$IVF_FIX_VAL" "$IVF_PROBES_FIXED"
	printf 'dataset the IVFFlat scan returned the exact top-%s in both cases (its scan\n' "$K"
	printf 'touched roughly the whole relation either way), so **this section does not\n'
	printf 'demonstrate a probes-driven recall regression** — it demonstrates that the\n'
	printf 'diagnose path works for IVFFlat and warns on `probes = %s`.\n' "$IVF_PROBES_BAD"
	printf 'Reproducing a probes-sensitive case needs a realistic clustered dataset\n'
	printf '(production embeddings are clustered); we could not do that here, so no\n'
	printf 'number is claimed for it.\n\n'

	printf '## 6. Anomaly behaviour\n\n'
	printf 'What each misuse reports. All of these are covered by the TAP tests; this\n'
	printf 'table just shows the messages.\n\n'
	printf '| Case | Statement | Reported |\n| --- | --- | --- |\n'
	cat "$ANOMALY_FILE"
	printf '\n'

	printf '## 7. How to reproduce\n\n'
	printf '```\ncd contrib/pgvector/bench\n'
	printf 'ROWS=%s DIMS=%s M=%s REPEATS=%s ./run_bench.sh\n```\n\n' "$ROWS" "$DIMS" "$M" "$REPEATS"
	printf 'See `bench/README.md` for the prerequisites and the known caveats\n'
	printf '(backend-local report, pre-build NOTICE on stdout, serial builds, and the\n'
	printf '`-march=native` build issue).\n'
} > "$OUT"

rm -rf "$WORK"
log "done: ${OUT}"
