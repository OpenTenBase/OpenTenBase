#!/usr/bin/env bash
# Run this script with Bash.

set -euo pipefail
# -e: exit immediately on command failure.
# -u: exit when an undefined variable is used.
# pipefail: propagate failures from pipelines.

export LC_ALL=C

baseline_file=
candidate_file=
output_file=

expected_header='profile,metric,lists,probes,clients,jobs,repeat,latency_avg_ms,tps,transactions,failed_transactions,query_count,top_k,returned_items,matched_items,expected_items,recall_at_k,min_query_recall,max_query_recall'
comparison_header='status,metric,lists,probes,clients,jobs,repeat,baseline_profile,candidate_profile,baseline_latency_avg_ms,candidate_latency_avg_ms,latency_change_pct,baseline_tps,candidate_tps,tps_change_pct,baseline_recall_at_k,candidate_recall_at_k,recall_change,baseline_failed_transactions,candidate_failed_transactions,baseline_returned_items,candidate_returned_items,baseline_matched_items,candidate_matched_items'


print_usage()
{
	cat <<EOF
Usage:
  $0 --baseline FILE --candidate FILE [--output FILE]

Compare two pgvector benchmark CSV files.

Options:
  --baseline FILE
      CSV produced by the baseline benchmark.

  --candidate FILE
      CSV produced by the candidate or optimized benchmark.

  --output FILE
      Write comparison CSV to FILE.
      Default: comparison_YYYYmmdd_HHMMSS.csv

  -h, --help
      Show this help message and exit.
EOF
}

while (($# > 0)); do
	case "$1" in
		--baseline)
			if (($# < 2)); then
				printf '%s\n' '--baseline requires a value' >&2
				print_usage >&2
				exit 1
			fi
			baseline_file=$2
			shift 2
			;;
		--candidate)
			if (($# < 2)); then
				printf '%s\n' '--candidate requires a value' >&2
				print_usage >&2
				exit 1
			fi
			candidate_file=$2
			shift 2
			;;
		--output)
			if (($# < 2)); then
				printf '%s\n' '--output requires a value' >&2
				print_usage >&2
				exit 1
			fi
			output_file=$2
			shift 2
			;;
		-h|--help)
			print_usage
			exit 0
			;;
		*)
			printf 'unknown option: %s\n' "$1" >&2
			print_usage >&2
			exit 1
			;;
	esac
done

if [[ -z "$baseline_file" || -z "$candidate_file" ]]; then
	printf '%s\n' '--baseline and --candidate are required' >&2
	print_usage >&2
	exit 1
fi


resolve_file()
{
	local file_path=$1

	if [[ "$file_path" == /* ]]; then
		printf '%s\n' "$file_path"
	else
		printf '%s/%s\n' "$PWD" "$file_path"
	fi
}

baseline_file=$(resolve_file "$baseline_file")
candidate_file=$(resolve_file "$candidate_file")

if [[ ! -f "$baseline_file" ]]; then
	printf 'baseline file not found: %s\n' "$baseline_file" >&2
	exit 1
fi

if [[ ! -f "$candidate_file" ]]; then
	printf 'candidate file not found: %s\n' "$candidate_file" >&2
	exit 1
fi

# Validate that an input file uses the benchmark CSV schema.

validate_header()
{
	local file_path=$1
	local actual_header

	actual_header=$(head -n 1 "$file_path")
	if [[ "$actual_header" != "$expected_header" ]]; then
		printf 'invalid CSV header: %s\n' "$file_path" >&2
		exit 1
	fi
}

validate_header "$baseline_file"
validate_header "$candidate_file"

if [[ -z "$output_file" ]]; then
	output_file="comparison_$(date '+%Y%m%d_%H%M%S').csv"
fi

if [[ "$output_file" != /* ]]; then
	output_file="$PWD/$output_file"
fi

mkdir -p "$(dirname -- "$output_file")"
printf '%s\n' "$comparison_header" >"$output_file"

# Match rows and calculate latency, throughput, and recall differences.
awk \
	-v baseline_file="$baseline_file" \
	-v candidate_file="$candidate_file" \
'
BEGIN {
	FS = ","
}

FNR == 1 {
	next
}

NF != 19 {
	printf "invalid CSV row: %s:%d has %d fields\n", FILENAME, FNR, NF > "/dev/stderr"
	invalid_row = 1
	next
}

{
	comparison_key = $2 SUBSEP $3 SUBSEP $4 SUBSEP $5 SUBSEP $6 SUBSEP $7
	if (!(comparison_key in known_key)) {
		known_key[comparison_key] = 1
		ordered_key[++key_count] = comparison_key
	}

	if (FILENAME == baseline_file) {
		baseline_count[comparison_key]++
		baseline_profile[comparison_key] = $1
		baseline_latency[comparison_key] = $8
		baseline_tps[comparison_key] = $9
		baseline_failed[comparison_key] = $11
		baseline_returned[comparison_key] = $14
		baseline_matched[comparison_key] = $15
		baseline_recall[comparison_key] = $17
	} else if (FILENAME == candidate_file) {
		candidate_count[comparison_key]++
		candidate_profile[comparison_key] = $1
		candidate_latency[comparison_key] = $8
		candidate_tps[comparison_key] = $9
		candidate_failed[comparison_key] = $11
		candidate_returned[comparison_key] = $14
		candidate_matched[comparison_key] = $15
		candidate_recall[comparison_key] = $17
	}
}

function percent_change(before_value, after_value) {
	if (before_value == "" || after_value == "" || before_value == 0)
		return ""
	return sprintf("%.6f", (after_value - before_value) * 100 / before_value)
}

function absolute_change(before_value, after_value) {
	if (before_value == "" || after_value == "")
		return ""
	return sprintf("%.6f", after_value - before_value)
}

END {
	if (invalid_row)
		exit 1

	for (key_index = 1; key_index <= key_count; key_index++) {
		comparison_key = ordered_key[key_index]
		split(comparison_key, key_fields, SUBSEP)

		if (baseline_count[comparison_key] == 0)
			row_status = "missing_baseline"
		else if (candidate_count[comparison_key] == 0)
			row_status = "missing_candidate"
		else if (baseline_count[comparison_key] != 1 || candidate_count[comparison_key] != 1)
			row_status = "duplicate_key"
		else
			row_status = "matched"

		printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
			row_status,
			key_fields[1], key_fields[2], key_fields[3], key_fields[4], key_fields[5], key_fields[6],
			baseline_profile[comparison_key], candidate_profile[comparison_key],
			baseline_latency[comparison_key], candidate_latency[comparison_key], percent_change(baseline_latency[comparison_key], candidate_latency[comparison_key]),
			baseline_tps[comparison_key], candidate_tps[comparison_key], percent_change(baseline_tps[comparison_key], candidate_tps[comparison_key]),
			baseline_recall[comparison_key], candidate_recall[comparison_key], absolute_change(baseline_recall[comparison_key], candidate_recall[comparison_key]),
			baseline_failed[comparison_key], candidate_failed[comparison_key],
			baseline_returned[comparison_key], candidate_returned[comparison_key],
			baseline_matched[comparison_key], candidate_matched[comparison_key]
	}
}
' "$baseline_file" "$candidate_file" >>"$output_file"


matched_count=$(awk -F ',' 'NR > 1 && $1 == "matched" { count++ } END { print count + 0 }' "$output_file")
missing_baseline_count=$(awk -F ',' 'NR > 1 && $1 == "missing_baseline" { count++ } END { print count + 0 }' "$output_file")
missing_candidate_count=$(awk -F ',' 'NR > 1 && $1 == "missing_candidate" { count++ } END { print count + 0 }' "$output_file")
duplicate_key_count=$(awk -F ',' 'NR > 1 && $1 == "duplicate_key" { count++ } END { print count + 0 }' "$output_file")

printf 'Comparison completed successfully.\n'
printf 'Matched rows: %s\n' "$matched_count"
printf 'Missing baseline rows: %s\n' "$missing_baseline_count"
printf 'Missing candidate rows: %s\n' "$missing_candidate_count"
printf 'Duplicate-key rows: %s\n' "$duplicate_key_count"
printf 'Comparison CSV: %s\n' "$output_file"
