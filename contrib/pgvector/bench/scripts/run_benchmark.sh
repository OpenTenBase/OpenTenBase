#!/usr/bin/env bash
# Run this script with Bash.

set -euo pipefail
# -e: exit immediately when a command fails.
# -u: exit immediately when an undefined variable is used.
# pipefail: make a pipeline fail when any command in it fails.

# Resolve the absolute path of this script's directory.
script_dir=$(
	cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
	pwd
)


bench_dir=$(
	cd -- "$script_dir/.."
	pwd
)

config_path="$bench_dir/config/smoke.conf"
output_dir=
metrics=(l2 ip cosine)
reuse_index=0


print_usage()
{
	cat <<EOF
Usage:
  $0 [OPTIONS]

Run the pgvector IVFFlat benchmark matrix.

Options:
  --config FILE
      Read benchmark parameters from FILE.
      Default: $bench_dir/config/smoke.conf

  --output DIR
      Write logs and CSV results to DIR.
      Default: $bench_dir/results/PROFILE_NAME

  --metrics LIST
      Run a comma-separated metric list.
      Supported metrics: l2, ip, cosine
      Default: l2,ip,cosine

  --reuse-index
      Reuse vector_bench.items_embedding_idx instead of rebuilding it.
      This requires exactly one metric and one LISTS_VALUES entry.

  -h, --help
      Show this help message and exit.

The dataset and exact ground truth must already be prepared.
The target PostgreSQL database must already be running.
EOF
}

while (($# > 0)); do
	case "$1" in
		--config)
			if (($# < 2)); then
				printf '%s\n' '--config requires a value' >&2
				print_usage >&2
				exit 1
			fi

			config_path=$2
			shift 2
			;;

		--output)
			if (($# < 2)); then
				printf '%s\n' '--output requires a value' >&2
				print_usage >&2
				exit 1
			fi

			output_dir=$2
			shift 2
			;;

		--metrics)
			if (($# < 2)); then
				printf '%s\n' '--metrics requires a value' >&2
				print_usage >&2
				exit 1
			fi

			IFS=',' read -r -a metrics <<<"$2"
			shift 2
			;;

		--reuse-index)
			reuse_index=1
			shift
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

# Convert a relative configuration path to an absolute path.
if [[ "$config_path" != /* ]]; then
	if [[ -f "$config_path" ]]; then
		config_path=$(
			cd -- "$(dirname -- "$config_path")"
			printf '%s/%s\n' "$PWD" "$(basename -- "$config_path")"
		)
	else
		config_path="$bench_dir/$config_path"
	fi
fi

if [[ ! -f "$config_path" ]]; then
	printf 'configuration file not found: %s\n' "$config_path" >&2
	exit 1
fi


set -a
# shellcheck disable=SC1090
source "$config_path"
set +a

VECTOR_TYPE=${VECTOR_TYPE:-vector}
QUERY_SHAPE=${QUERY_SHAPE:-transaction}

if [[ "$VECTOR_TYPE" != vector && "$VECTOR_TYPE" != halfvec ]]; then
	printf 'VECTOR_TYPE must be vector or halfvec: %s\n' "$VECTOR_TYPE" >&2
	exit 1
fi

if [[ "$QUERY_SHAPE" != transaction && "$QUERY_SHAPE" != single ]]; then
	printf 'QUERY_SHAPE must be transaction or single: %s\n' "$QUERY_SHAPE" >&2
	exit 1
fi

# Check whether a required configuration variable is defined.
require_variable()
{
	local variable_name=$1

	if [[ -z "${!variable_name:-}" ]]; then
		printf 'required configuration variable is missing: %s\n' \
			"$variable_name" >&2
		exit 1
	fi
}

required_variables=(
	PROFILE_NAME
	ROWS
	DIMENSIONS
	QUERY_COUNT
	RECALL_QUERY_COUNT
	TOP_K
	LISTS_VALUES
	PROBES_VALUES
	CLIENT_VALUES
	JOBS
	WARMUP_SECONDS
	DURATION_SECONDS
	REPEATS
	JIT
	ENABLE_SEQSCAN
	WORK_MEM
	DB_HOST
	DB_PORT
	DB_USER
	DB_NAME
)

for variable_name in "${required_variables[@]}"; do
	require_variable "$variable_name"
done

require_positive_integer()
{
	local variable_name=$1
	local value=${!variable_name}

	if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
		printf '%s must be a positive integer: %s\n' \
			"$variable_name" "$value" >&2
		exit 1
	fi
}

for variable_name in \
	ROWS \
	DIMENSIONS \
	QUERY_COUNT \
	RECALL_QUERY_COUNT \
	TOP_K \
	JOBS \
	WARMUP_SECONDS \
	DURATION_SECONDS \
	REPEATS \
	DB_PORT; do
	require_positive_integer "$variable_name"
done

if ((RECALL_QUERY_COUNT > QUERY_COUNT)); then
	printf '%s\n' \
		'RECALL_QUERY_COUNT must not exceed QUERY_COUNT' >&2
	exit 1
fi

# Resolve an executable from an environment override, PATH, or build tree.
resolve_executable()
{
	local override_name=$1
	local command_name=$2
	local installed_path=$3
	local build_path=$4

	if [[ -n "${!override_name:-}" ]]; then
		printf '%s\n' "${!override_name}"
	elif command -v "$command_name" >/dev/null 2>&1; then
		command -v "$command_name"
	elif [[ -x "$installed_path" ]]; then
		printf '%s\n' "$installed_path"
	elif [[ -x "$build_path" ]]; then
		printf '%s\n' "$build_path"
	else
		printf '%s executable was not found; set %s explicitly\n' \
			"$command_name" "$override_name" >&2
		exit 1
	fi
}

pgbench_command=$(resolve_executable \
	PGBENCH \
	pgbench \
	"$bench_dir/../../../build/tmp_install/opt/opentenbase-pg19/bin/pgbench" \
	"$bench_dir/../../../build/src/bin/pgbench/pgbench")

psql_command=$(resolve_executable \
	PSQL \
	psql \
	"$bench_dir/../../../build/tmp_install/opt/opentenbase-pg19/bin/psql" \
	"$bench_dir/../../../build/src/bin/psql/psql")

if [[ -z "$output_dir" ]]; then
	output_dir="$bench_dir/results/$PROFILE_NAME"
fi

mkdir -p "$output_dir/logs" "$output_dir/plans"
output_dir=$(
	cd -- "$output_dir"
	pwd
)

timestamp=$(date '+%Y%m%d_%H%M%S')
csv_file="$output_dir/benchmark_${timestamp}.csv"

# Configure the client connection used by psql and pgbench.
export PGHOST="$DB_HOST"
export PGPORT="$DB_PORT"
export PGUSER="$DB_USER"
export PGDATABASE="$DB_NAME"
export LC_ALL=C


pgoptions=${PGOPTIONS:-}
pgoptions+=" -c jit=$JIT"
pgoptions+=" -c enable_seqscan=$ENABLE_SEQSCAN"
pgoptions+=" -c work_mem=$WORK_MEM"
export PGOPTIONS="$pgoptions"


printf '%s\n' \
	'profile,metric,lists,probes,clients,jobs,repeat,latency_avg_ms,latency_p50_ms,latency_p95_ms,tps,transactions,failed_transactions,query_count,top_k,returned_items,matched_items,expected_items,recall_at_k,min_query_recall,max_query_recall' \
	>"$csv_file"

# Return the operator class associated with a distance metric.
operator_class_for_metric()
{
	case "$1" in
		l2)
			printf '%s_l2_ops\n' "$VECTOR_TYPE"
			;;
		ip)
			printf '%s_ip_ops\n' "$VECTOR_TYPE"
			;;
		cosine)
			printf '%s_cosine_ops\n' "$VECTOR_TYPE"
			;;
		*)
			printf 'unsupported metric: %s\n' "$1" >&2
			exit 1
			;;
	esac
}

# Run the SQL file that creates the current IVFFlat index.
create_index()
{
	local metric=$1
	local lists=$2
	local opclass
	local log_file="$output_dir/logs/create_index_${metric}_lists_${lists}.log"
	local psql_arguments

	opclass=$(operator_class_for_metric "$metric")

	psql_arguments=(
		-X \
		-v ON_ERROR_STOP=1 \
		-v "metric=$metric" \
		-v "vector_type=$VECTOR_TYPE" \
		-v "opclass=$opclass" \
		-v "lists=$lists" \
		-f "$bench_dir/sql/create_index.sql"
	)

	if [[ -n "${MAINTENANCE_WORK_MEM:-}" ]]; then
		psql_arguments+=(
			-v "maintenance_work_mem=$MAINTENANCE_WORK_MEM"
		)
	fi

	"$psql_command" \
		"${psql_arguments[@]}" \
		>"$log_file" 2>&1
}

# Verify that reuse mode points at the expected physical index.
verify_existing_index()
{
	local metric=$1
	local lists=$2
	local expected_opclass
	local actual_definition

	expected_opclass=$(operator_class_for_metric "$metric")
	actual_definition=$("$psql_command" -X -A -t -v ON_ERROR_STOP=1 -c "
SELECT opc.opcname || ',' || COALESCE(
           (SELECT option_value
            FROM pg_options_to_table(index_relation.reloptions)
            WHERE option_name = 'lists'),
           '100')
FROM pg_class AS index_relation
JOIN pg_index AS index_catalog
  ON index_catalog.indexrelid = index_relation.oid
JOIN pg_opclass AS opc
  ON opc.oid = index_catalog.indclass[0]
WHERE index_relation.oid = 'vector_bench.items_embedding_idx'::regclass;")

	if [[ "$actual_definition" != "$expected_opclass,$lists" ]]; then
		printf 'existing index mismatch: expected %s,%s, got %s\n' \
			"$expected_opclass" "$lists" "${actual_definition:-<none>}" >&2
		exit 1
	fi
}

# Run one warmup or measured pgbench invocation.
run_pgbench()
{
	local metric=$1
	local clients=$2
	local probes=$3
	local duration=$4
	local phase=$5
	local repeat_number=$6
	local workload_file="$bench_dir/sql/workload_${metric}.sql"
	local log_file="$output_dir/logs/pgbench_${timestamp}_${metric}_clients_${clients}_probes_${probes}_${phase}_${repeat_number}.log"
	local arguments=(
		-h "$DB_HOST"
		-p "$DB_PORT"
		-U "$DB_USER"
		-n
		-M simple
		-c "$clients"
		-j "$JOBS"
		-T "$duration"
		-D "query_count=$QUERY_COUNT"
		-D "top_k=$TOP_K"
		-D "probes=$probes"
		-f "$workload_file"
	)

	if [[ "$QUERY_SHAPE" == single ]]; then
		workload_file="$bench_dir/sql/workload_single_${metric}.sql"
		arguments[${#arguments[@]}-1]="$workload_file"
	fi

	if [[ ! -s "$workload_file" ]]; then
		printf 'workload SQL file is missing or empty: %s\n' \
			"$workload_file" >&2
		exit 1
	fi

	if [[ "$phase" == measured ]]; then
		arguments+=(
			-l
			--log-prefix="${log_file%.log}_transactions"
		)
	fi

	PGOPTIONS="$PGOPTIONS -c ivfflat.probes=$probes -c enable_indexscan=on -c enable_bitmapscan=off" \
		"$pgbench_command" "${arguments[@]}" "$DB_NAME" \
		>"$log_file" 2>&1

	printf '%s\n' "$log_file"
}

# Calculate discrete P50 and P95 latency from per-transaction pgbench logs.
parse_latency_percentiles()
{
	local measured_log=$1
	local transaction_prefix="${measured_log%.log}_transactions"
	local transaction_logs=("$transaction_prefix".*)

	if ((${#transaction_logs[@]} == 0)) || [[ ! -f "${transaction_logs[0]}" ]]; then
		printf 'transaction log not found for measured run: %s\n' "$measured_log" >&2
		exit 1
	fi

	awk '$3 ~ /^[0-9]+$/ { print $3 / 1000.0 }' "${transaction_logs[@]}" |
		sort -n |
		awk '
			{ latency[++count] = $1 }
			END {
				if (count == 0)
					exit 1
				p50_rank = int(count * 0.50)
				if (p50_rank < count * 0.50)
					p50_rank++
				p95_rank = int(count * 0.95)
				if (p95_rank < count * 0.95)
					p95_rank++
				printf "%.3f %.3f\n", latency[p50_rank], latency[p95_rank]
			}
		'
}

# Run the Recall@K SQL file and return its comma-separated result row.
measure_recall()
{
	local metric=$1
	local probes=$2
	local output_file="$output_dir/logs/recall_${metric}_probes_${probes}.out"
	local error_file="$output_dir/logs/recall_${metric}_probes_${probes}.err"
	local recall_row

	if ! "$psql_command" \
		-X \
		-A \
		-t \
		-F ',' \
		-v ON_ERROR_STOP=1 \
		-v "metric=$metric" \
		-v "vector_type=$VECTOR_TYPE" \
		-v "probes=$probes" \
		-v "recall_query_count=$RECALL_QUERY_COUNT" \
		-v "top_k=$TOP_K" \
		-f "$bench_dir/sql/measure_recall.sql" \
		>"$output_file" \
		2>"$error_file"; then
		printf 'Recall@K SQL failed for metric=%s probes=%s\n' \
			"$metric" "$probes" >&2
		cat "$error_file" >&2
		cat "$output_file" >&2
		return 1
	fi

	# Select the final row with all expected Recall@K columns.

	recall_row=$(awk -F ',' '
		NF == 11 { row = $0 }
		END {
			if (row != "")
				print row
		}
	' "$output_file")

	if [[ -z "$recall_row" ]]; then
		printf 'Recall@K SQL returned no 11-column result row for metric=%s probes=%s\n' \
			"$metric" "$probes" >&2
		printf '%s\n' '--- Recall stdout ---' >&2
		cat "$output_file" >&2
		printf '%s\n' '--- Recall stderr ---' >&2
		cat "$error_file" >&2
		return 1
	fi

	printf '%s\n' "$recall_row"
}

# Parse a comma-separated Recall@K result row.
parse_recall_row()
{
	local recall_row=$1

	if [[ -z "$recall_row" ]] || ! awk -F ',' 'NF == 11 { found = 1 } END { exit !found }' \
		<<<"$recall_row"; then
		printf '%s\n' 'Recall@K SQL returned an invalid 11-column result row' >&2
		exit 1
	fi
}


printf 'Profile: %s\n' "$PROFILE_NAME"
printf 'Vector type: %s\n' "$VECTOR_TYPE"
printf 'Query shape: %s\n' "$QUERY_SHAPE"
printf 'Reuse index: %s\n' "$reuse_index"
printf 'Configuration: %s\n' "$config_path"
printf 'Output CSV: %s\n' "$csv_file"
printf 'pgbench: %s\n' "$pgbench_command"
printf 'psql: %s\n' "$psql_command"

if ((reuse_index)); then
	read -r -a configured_lists <<<"$LISTS_VALUES"
	if ((${#metrics[@]} != 1 || ${#configured_lists[@]} != 1)); then
		printf '%s\n' '--reuse-index requires exactly one metric and one LISTS_VALUES entry' >&2
		exit 1
	fi
fi

for metric in "${metrics[@]}"; do
	operator_class_for_metric "$metric" >/dev/null

	for lists in $LISTS_VALUES; do
		if ((reuse_index)); then
			verify_existing_index "$metric" "$lists"
		else
			create_index "$metric" "$lists"
		fi

		for probes in $PROBES_VALUES; do
			if ((probes > lists)); then
				printf 'Skipping metric=%s lists=%s probes=%s: probes exceeds lists\n' \
					"$metric" "$lists" "$probes"
				continue
			fi

			for clients in $CLIENT_VALUES; do
				if [[ ! "$clients" =~ ^[1-9][0-9]*$ ]]; then
					printf 'CLIENT_VALUES contains an invalid value: %s\n' \
						"$clients" >&2
					exit 1
				fi

				for repeat_number in $(seq 1 "$REPEATS"); do
					if ((WARMUP_SECONDS > 0)); then
						run_pgbench \
							"$metric" \
							"$clients" \
							"$probes" \
							"$WARMUP_SECONDS" \
							warmup \
							"$repeat_number" \
							>/dev/null
					fi

					measured_log=$(run_pgbench \
						"$metric" \
						"$clients" \
						"$probes" \
						"$DURATION_SECONDS" \
						measured \
						"$repeat_number")

					summary_values=$(awk '
						/^number of transactions actually processed:/ {
							sub(/^number of transactions actually processed: /, "")
							split($0, fields, "/")
							transactions = fields[1]
						}
						/^number of failed transactions:/ {
							sub(/^number of failed transactions: /, "")
							split($0, fields, " ")
							failed_transactions = fields[1]
						}
						/^latency average = / {
							sub(/^latency average = /, "")
							split($0, fields, " ")
							latency_avg_ms = fields[1]
						}
						/^tps = / {
							sub(/^tps = /, "")
							split($0, fields, " ")
							tps = fields[1]
						}
						END {
							printf "%s %s %s %s\n", latency_avg_ms, tps, transactions, failed_transactions
						}
					' "$measured_log")

					read -r latency_avg_ms tps transactions failed_transactions \
						<<<"$summary_values"
					read -r latency_p50_ms latency_p95_ms \
						<<<"$(parse_latency_percentiles "$measured_log")"

					if [[ -z "$latency_avg_ms" || -z "$tps" || \
						-z "$transactions" || -z "$failed_transactions" ]]; then
						printf 'could not parse pgbench summary: %s\n' \
							"$measured_log" >&2
						printf 'parsed values: latency=%s tps=%s transactions=%s failed=%s\n' \
							"$latency_avg_ms" \
							"$tps" \
							"$transactions" \
							"$failed_transactions" >&2
						exit 1
					fi

					recall_row=$(measure_recall "$metric" "$probes")
					parse_recall_row "$recall_row"

					IFS=',' read -r \
						recall_metric \
						recall_lists \
						recall_probes \
						recall_query_count \
						recall_top_k \
						returned_items \
						matched_items \
						expected_items \
						recall_at_k \
						min_query_recall \
						max_query_recall \
						<<<"$recall_row"

					printf '%s\n' \
						"$PROFILE_NAME,$metric,$lists,$probes,$clients,$JOBS,$repeat_number,$latency_avg_ms,$latency_p50_ms,$latency_p95_ms,$tps,$transactions,$failed_transactions,$recall_query_count,$recall_top_k,$returned_items,$matched_items,$expected_items,$recall_at_k,$min_query_recall,$max_query_recall" \
						>>"$csv_file"

					printf 'metric=%s lists=%s probes=%s clients=%s repeat=%s latency_ms=%s p50_ms=%s p95_ms=%s tps=%s recall=%s\n' \
						"$metric" \
						"$lists" \
						"$probes" \
						"$clients" \
						"$repeat_number" \
						"$latency_avg_ms" \
						"$latency_p50_ms" \
						"$latency_p95_ms" \
						"$tps" \
						"$recall_at_k"
				 done
			done
		done
	done
done

printf 'Benchmark completed successfully.\n'
printf 'CSV: %s\n' "$csv_file"
