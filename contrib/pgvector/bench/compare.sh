#!/usr/bin/env bash

set -euo pipefail

if (($# != 3)); then
	printf 'usage: %s BASELINE_RESULTS.csv OPTIMIZED_RESULTS.csv REPORT.md\n' "$0" >&2
	exit 1
fi

baseline=$1
optimized=$2
report=$3
bench_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for input in "$baseline" "$optimized"; do
	if [[ ! -s "$input" ]]; then
		printf 'missing result file: %s\n' "$input" >&2
		exit 1
	fi
done

{
	printf '# pgvector IVFFlat performance comparison\n\n'
	printf 'Generated: %s\n\n' "$(date --iso-8601=seconds)"
	printf 'Acceptance gates: Recall@10 may drop by at most 0.1 percentage points; '
	printf 'at least one matched configuration must improve P95 or QPS by at least 10%%; '
	printf 'no core configuration may regress by more than 3%%.\n\n'
	gawk -f "$bench_dir/compare.awk" "$baseline" "$optimized"
} >"$report"

printf 'comparison written to %s\n' "$report"
