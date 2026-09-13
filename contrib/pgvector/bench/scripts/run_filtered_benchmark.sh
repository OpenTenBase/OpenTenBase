#!/usr/bin/env bash
# Run this script with Bash.

set -euo pipefail

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
filters=(percent_1 percent_0_1)
modes=(off relaxed_order)
probe_values="1"
max_probe_values="100"
reuse_index=0

print_usage()
{
	cat <<EOF
Usage:
  $0 [OPTIONS]

Run filtered pgvector IVFFlat benchmark profiles.

Options:
  --config FILE
      Read benchmark parameters from FILE.

  --output DIR
      Write filtered CSV and logs to DIR.

  --metrics LIST
      Comma-separated metric list: l2,ip,cosine

  --filters LIST
      Comma-separated filter list: percent_1,percent_0_1

  --modes LIST
      Comma-separated scan modes: off,relaxed_order

  --probes VALUE
      Initial IVFFlat probe count. Default: 1

  --max-probes VALUE
      Maximum iterative-scan probe count. Default: 100

  --reuse-index
      Reuse vector_bench.items_embedding_idx instead of rebuilding it.
      This requires exactly one metric and one LISTS_VALUES entry.

  -h, --help
      Show this help message and exit.
EOF
}

while (($# > 0)); do
	case "$1" in
		--config)
			if (($# < 2)); then
				printf '%s\n' '--config requires a value' >&2
				exit 1
			fi
			config_path=$2
			shift 2
			;;
		--output)
			if (($# < 2)); then
				printf '%s\n' '--output requires a value' >&2
				exit 1
			fi
			output_dir=$2
			shift 2
			;;
		--metrics)
			if (($# < 2)); then
				printf '%s\n' '--metrics requires a value' >&2
				exit 1
			fi
			IFS=',' read -r -a metrics <<<"$2"
			shift 2
			;;
		--filters)
			if (($# < 2)); then
				printf '%s\n' '--filters requires a value' >&2
				exit 1
			fi
			IFS=',' read -r -a filters <<<"$2"
			shift 2
			;;
		--modes)
			if (($# < 2)); then
				printf '%s\n' '--modes requires a value' >&2
				exit 1
			fi
			IFS=',' read -r -a modes <<<"$2"
			shift 2
			;;
		--probes)
			if (($# < 2)); then
				printf '%s\n' '--probes requires a value' >&2
				exit 1
			fi
			probe_values=$2
			shift 2
			;;
		--max-probes)
			if (($# < 2)); then
				printf '%s\n' '--max-probes requires a value' >&2
				exit 1
			fi
			max_probe_values=$2
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

if [[ "$VECTOR_TYPE" != vector && "$VECTOR_TYPE" != halfvec ]]; then
	printf 'VECTOR_TYPE must be vector or halfvec: %s\n' "$VECTOR_TYPE" >&2
	exit 1
fi

for variable_name in \
	PROFILE_NAME ROWS QUERY_COUNT RECALL_QUERY_COUNT TOP_K CATEGORY_COUNT \
	LISTS_VALUES CLIENT_VALUES JOBS WARMUP_SECONDS DURATION_SECONDS REPEATS \
	JIT ENABLE_SEQSCAN WORK_MEM DB_HOST DB_PORT DB_USER DB_NAME; do
	if [[ -z "${!variable_name:-}" ]]; then
		printf 'required configuration variable is missing: %s\n' "$variable_name" >&2
		exit 1
	fi
done

if ((CATEGORY_COUNT < 1000 || CATEGORY_COUNT % 1000 != 0)); then
	printf '%s\n' 'CATEGORY_COUNT must be at least 1000 and divisible by 1000' >&2
	exit 1
fi

if [[ -z "$output_dir" ]]; then
	output_dir="$bench_dir/results/filtered_$PROFILE_NAME"
fi

mkdir -p "$output_dir/logs"
output_dir=$(
	cd -- "$output_dir"
	pwd
)

timestamp=$(date '+%Y%m%d_%H%M%S')
csv_file="$output_dir/filtered_benchmark_${timestamp}.csv"

printf '%s\n' \
	'profile,metric,filter_name,filter_limit,lists,probes,iterative_scan,max_probes,clients,jobs,repeat,latency_avg_ms,latency_p50_ms,latency_p95_ms,tps,transactions,failed_transactions,query_count,top_k,returned_items,matched_items,expected_items,recall_at_k,min_query_recall,max_query_recall' \
	>"$csv_file"

# Resolve a benchmark executable from an override, PATH, or the build tree.

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

export PGHOST="$DB_HOST"
export PGPORT="$DB_PORT"
export PGUSER="$DB_USER"
export PGDATABASE="$DB_NAME"

pgoptions=${PGOPTIONS:-}
pgoptions+=" -c jit=$JIT"
pgoptions+=" -c enable_seqscan=$ENABLE_SEQSCAN"
pgoptions+=" -c work_mem=$WORK_MEM"
export PGOPTIONS="$pgoptions"

operator_class_for_metric()
{
	case "$1" in
		l2) printf '%s_l2_ops\n' "$VECTOR_TYPE" ;;
		ip) printf '%s_ip_ops\n' "$VECTOR_TYPE" ;;
		cosine) printf '%s_cosine_ops\n' "$VECTOR_TYPE" ;;
		*) printf 'unsupported metric: %s\n' "$1" >&2; exit 1 ;;
	esac
}

# Return the category limit for a filter profile.
filter_limit_for_name()
{
	case "$1" in
		percent_1) printf '%s\n' "$((CATEGORY_COUNT / 100))" ;;
		percent_0_1) printf '%s\n' "$((CATEGORY_COUNT / 1000))" ;;
		*) printf 'unsupported filter: %s\n' "$1" >&2; exit 1 ;;
	esac
}

create_index()
{
	local metric=$1
	local lists=$2
	local log_file="$output_dir/logs/create_index_${metric}_lists_${lists}.log"
	local arguments=(
		-X
		-v ON_ERROR_STOP=1
		-v "metric=$metric"
		-v "vector_type=$VECTOR_TYPE"
		-v "opclass=$(operator_class_for_metric "$metric")"
		-v "lists=$lists"
		-f "$bench_dir/sql/create_index.sql"
	)

	if [[ -n "${MAINTENANCE_WORK_MEM:-}" ]]; then
		arguments+=(
			-v "maintenance_work_mem=$MAINTENANCE_WORK_MEM"
		)
	fi

	"$psql_command" "${arguments[@]}" >"$log_file" 2>&1
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

# Run one warmup or measured filtered pgbench invocation.
run_pgbench()
{
	local metric=$1
	local filter_name=$2
	local filter_limit=$3
	local probes=$4
	local iterative_scan=$5
	local max_probes=$6
	local clients=$7
	local duration=$8
	local phase=$9
	local repeat_number=${10}
	local workload_file="$bench_dir/sql/workload_filtered_${metric}.sql"
	local log_file="$output_dir/logs/pgbench_${timestamp}_${metric}_${filter_name}_probes_${probes}_${iterative_scan}_max_${max_probes}_clients_${clients}_${phase}_${repeat_number}.log"
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
		-D "filter_limit=$filter_limit"
		-D "iterative_scan=$iterative_scan"
		-D "max_probes=$max_probes"
		-f "$workload_file"
	)

	if [[ "$phase" == measured ]]; then
		arguments+=(
			-l
			--log-prefix="${log_file%.log}_transactions"
		)
	fi

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

parse_pgbench_summary()
{
	local log_file=$1

	awk '
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
	' "$log_file"
}

# Run filtered Recall@K and return its 15-column result row.
measure_recall()
{
	local metric=$1
	local filter_name=$2
	local filter_limit=$3
	local probes=$4
	local iterative_scan=$5
	local max_probes=$6
	local output_file="$output_dir/logs/recall_${metric}_${filter_name}_${probes}_${iterative_scan}_${max_probes}.out"
	local error_file="$output_dir/logs/recall_${metric}_${filter_name}_${probes}_${iterative_scan}_${max_probes}.err"
	local recall_row

	if ! "$psql_command" \
		-X \
		-A \
		-t \
		-F ',' \
		-v ON_ERROR_STOP=1 \
		-v "metric=$metric" \
		-v "vector_type=$VECTOR_TYPE" \
		-v "filter_name=$filter_name" \
		-v "filter_limit=$filter_limit" \
		-v "probes=$probes" \
		-v "iterative_scan=$iterative_scan" \
		-v "max_probes=$max_probes" \
		-v "recall_query_count=$RECALL_QUERY_COUNT" \
		-v "top_k=$TOP_K" \
		-f "$bench_dir/sql/measure_filtered_recall.sql" \
		>"$output_file" \
		2>"$error_file"; then
		printf 'filtered Recall failed: metric=%s filter=%s mode=%s\n' \
			"$metric" "$filter_name" "$iterative_scan" >&2
		cat "$error_file" "$output_file" >&2
		exit 1
	fi

	recall_row=$(awk -F ',' 'NF == 15 { row = $0 } END { if (row != "") print row }' "$output_file")
	if [[ -z "$recall_row" ]]; then
		printf 'filtered Recall returned no 15-column row: %s\n' "$output_file" >&2
		cat "$output_file" >&2
		exit 1
	fi

	printf '%s\n' "$recall_row"
}

printf 'Profile: %s\n' "$PROFILE_NAME"
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

		for filter_name in "${filters[@]}"; do
			filter_limit=$(filter_limit_for_name "$filter_name")

			for probes in $probe_values; do
				if ((probes > lists)); then
					printf 'Skipping metric=%s lists=%s probes=%s\n' \
						"$metric" "$lists" "$probes"
					continue
				fi

				for max_probes in $max_probe_values; do
					if ((max_probes < probes)); then
						printf 'Skipping max_probes=%s because it is lower than probes=%s\n' \
							"$max_probes" "$probes"
						continue
					fi

					for iterative_scan in "${modes[@]}"; do
						for clients in $CLIENT_VALUES; do
							for repeat_number in $(seq 1 "$REPEATS"); do
								if ((WARMUP_SECONDS > 0)); then
									run_pgbench \
										"$metric" "$filter_name" "$filter_limit" \
										"$probes" "$iterative_scan" "$max_probes" \
										"$clients" "$WARMUP_SECONDS" warmup "$repeat_number" \
										>/dev/null
								fi

								measured_log=$(run_pgbench \
									"$metric" "$filter_name" "$filter_limit" \
									"$probes" "$iterative_scan" "$max_probes" \
									"$clients" "$DURATION_SECONDS" measured "$repeat_number")

								read -r latency_avg_ms tps transactions failed_transactions \
									<<<"$(parse_pgbench_summary "$measured_log")"
								read -r latency_p50_ms latency_p95_ms \
									<<<"$(parse_latency_percentiles "$measured_log")"

								recall_row=$(measure_recall \
									"$metric" "$filter_name" "$filter_limit" \
									"$probes" "$iterative_scan" "$max_probes")

								IFS=',' read -r \
									recall_filter \
									recall_metric \
									recall_lists \
									recall_filter_limit \
									recall_probes \
									recall_mode \
									recall_max_probes \
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
									"$PROFILE_NAME,$metric,$filter_name,$filter_limit,$lists,$probes,$iterative_scan,$max_probes,$clients,$JOBS,$repeat_number,$latency_avg_ms,$latency_p50_ms,$latency_p95_ms,$tps,$transactions,$failed_transactions,$recall_query_count,$recall_top_k,$returned_items,$matched_items,$expected_items,$recall_at_k,$min_query_recall,$max_query_recall" \
									>>"$csv_file"

								printf 'metric=%s filter=%s lists=%s probes=%s mode=%s max_probes=%s clients=%s repeat=%s latency_ms=%s p50_ms=%s p95_ms=%s tps=%s recall=%s\n' \
									"$metric" "$filter_name" "$lists" "$probes" "$iterative_scan" \
									"$max_probes" "$clients" "$repeat_number" \
									"$latency_avg_ms" "$latency_p50_ms" "$latency_p95_ms" \
									"$tps" "$recall_at_k"
							done
						done
					done
				done
			done
		 done
	done
	done

printf 'Filtered benchmark completed successfully.\n'
printf 'CSV: %s\n' "$csv_file"
