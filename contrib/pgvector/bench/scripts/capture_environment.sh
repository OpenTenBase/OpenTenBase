#!/usr/bin/env bash
# Run this script with Bash.

set -euo pipefail
# -e: exit immediately when a command fails.
# -u: exit immediately when an undefined variable is used.
# pipefail: make a pipeline fail when any command in it fails.


script_dir=$(
	cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
	pwd
)


bench_dir=$(
	cd -- "$script_dir/.."
	pwd
)

repo_dir=$(
	cd -- "$bench_dir/../../.."
	pwd
)

config_path="$bench_dir/config/smoke.conf"
output_dir=


print_usage()
{
	cat <<EOF
Usage:
  $0 [OPTIONS]

Capture the environment used by the pgvector benchmark.

Options:
  --config FILE
      Read benchmark parameters from FILE.
      Default: $bench_dir/config/smoke.conf

  --output DIR
      Write environment metadata to DIR.
      Default: $bench_dir/results/PROFILE_NAME

  -h, --help
      Show this help message and exit.
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

if [[ -z "${PROFILE_NAME:-}" ]]; then
	printf '%s\n' 'PROFILE_NAME is missing from the configuration' >&2
	exit 1
fi

if [[ -z "$output_dir" ]]; then
	output_dir="$bench_dir/results/$PROFILE_NAME"
fi

mkdir -p "$output_dir"
output_dir=$(
	cd -- "$output_dir"
	pwd
)

environment_file="$output_dir/environment.txt"

# Capture a command result without making optional system tools fatal.
capture_command()
{
	local label=$1
	shift

	printf '%s\n' "[$label]"
	if "$@" 2>&1; then
		:
	else
		printf '%s\n' '<unavailable>'
	fi
}

# Resolve benchmark client binaries from overrides, PATH, or the build tree.
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
		printf '%s\n' '<unavailable>'
	fi
}

pgbench_command=$(resolve_executable \
	PGBENCH \
	pgbench \
	"$repo_dir/build/tmp_install/opt/opentenbase-pg19/bin/pgbench" \
	"$repo_dir/build/src/bin/pgbench/pgbench")

psql_command=$(resolve_executable \
	PSQL \
	psql \
	"$repo_dir/build/tmp_install/opt/opentenbase-pg19/bin/psql" \
	"$repo_dir/build/src/bin/psql/psql")

pg_config_command=${PG_CONFIG:-}
if [[ -z "$pg_config_command" ]]; then
	if command -v pg_config >/dev/null 2>&1; then
		pg_config_command=$(command -v pg_config)
	else
		pg_config_command="$repo_dir/build/tmp_install/opt/opentenbase-pg19/bin/pg_config"
	fi
fi

# Capture PostgreSQL metadata when the target database is reachable.。
database_metadata_file="$output_dir/database_environment.txt"
database_metadata_status=0
database_extensions_sql="SELECT extname, extversion FROM pg_extension WHERE extname IN ('vector', 'pg_prewarm') ORDER BY extname;"
database_settings_sql="SELECT name, setting FROM pg_settings WHERE name IN ('shared_buffers', 'work_mem', 'maintenance_work_mem', 'max_parallel_workers', 'max_parallel_maintenance_workers') ORDER BY name;"

if [[ "$psql_command" != '<unavailable>' ]]; then
	set +e
	PGHOST="$DB_HOST" \
	PGPORT="$DB_PORT" \
	PGUSER="$DB_USER" \
	PGDATABASE="$DB_NAME" \
	"$psql_command" \
		-X \
		-P pager=off \
		-v ON_ERROR_STOP=1 \
		-c 'SELECT current_database() AS database, current_user AS user_name;' \
		-c 'SELECT version();' \
		-c 'SHOW server_version;' \
		-c "$database_extensions_sql" \
		-c "$database_settings_sql" \
		>"$database_metadata_file" \
		2>&1
	database_metadata_status=$?
	set -e
else
	printf '%s\n' 'psql executable is unavailable' >"$database_metadata_file"
	database_metadata_status=1
fi

# Write host, build, benchmark, and database connection metadata.
{
	printf '%s\n' '# pgvector benchmark environment'
	printf '%s\n' '# pgvector benchmark 环境信息'
	printf '\n'
	printf 'captured_at=%s\n' "$(date --iso-8601=seconds)"
	printf 'hostname=%s\n' "$(hostname)"
	printf 'working_directory=%s\n' "$PWD"
	printf 'benchmark_directory=%s\n' "$bench_dir"
	printf 'repository_directory=%s\n' "$repo_dir"
	printf 'config_file=%s\n' "$config_path"
	printf 'output_directory=%s\n' "$output_dir"
	printf '\n'

	printf '%s\n' '[benchmark_configuration]'
	printf 'profile_name=%s\n' "${PROFILE_NAME:-<unset>}"
	printf 'vector_type=%s\n' "$VECTOR_TYPE"
	printf 'rows=%s\n' "${ROWS:-<unset>}"
	printf 'dimensions=%s\n' "${DIMENSIONS:-<unset>}"
	printf 'query_count=%s\n' "${QUERY_COUNT:-<unset>}"
	printf 'recall_query_count=%s\n' "${RECALL_QUERY_COUNT:-<unset>}"
	printf 'top_k=%s\n' "${TOP_K:-<unset>}"
	printf 'lists_values=%s\n' "${LISTS_VALUES:-<unset>}"
	printf 'probes_values=%s\n' "${PROBES_VALUES:-<unset>}"
	printf 'client_values=%s\n' "${CLIENT_VALUES:-<unset>}"
	printf 'jobs=%s\n' "${JOBS:-<unset>}"
	printf 'warmup_seconds=%s\n' "${WARMUP_SECONDS:-<unset>}"
	printf 'duration_seconds=%s\n' "${DURATION_SECONDS:-<unset>}"
	printf 'repeats=%s\n' "${REPEATS:-<unset>}"
	printf 'jit=%s\n' "${JIT:-<unset>}"
	printf 'enable_seqscan=%s\n' "${ENABLE_SEQSCAN:-<unset>}"
	printf 'work_mem=%s\n' "${WORK_MEM:-<unset>}"
	printf 'maintenance_work_mem=%s\n' "${MAINTENANCE_WORK_MEM:-<unset>}"
	printf 'random_seed=%s\n' "${RANDOM_SEED:-<unset>}"
	printf 'query_random_seed=%s\n' "${QUERY_RANDOM_SEED:-<unset>}"
	printf 'clusters=%s\n' "${CLUSTERS:-<unset>}"
	printf 'category_count=%s\n' "${CATEGORY_COUNT:-<unset>}"
	printf '\n'

	printf '%s\n' '[database_connection]'
	printf 'database_host=%s\n' "${DB_HOST:-<unset>}"
	printf 'database_port=%s\n' "${DB_PORT:-<unset>}"
	printf 'database_user=%s\n' "${DB_USER:-<unset>}"
	printf 'database_name=%s\n' "${DB_NAME:-<unset>}"
	printf 'database_metadata_file=%s\n' "$database_metadata_file"
	printf 'database_metadata_status=%s\n' "$database_metadata_status"
	printf '\n'

	printf '%s\n' '[source_revision]'
	if git -C "$repo_dir" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
		printf 'repository_commit=%s\n' "$(git -C "$repo_dir" rev-parse HEAD)"
		printf 'repository_branch=%s\n' "$(git -C "$repo_dir" branch --show-current)"
		printf 'repository_status=%s\n' "$(git -C "$repo_dir" status --short | tr '\n' ';')"
	else
		printf '%s\n' 'repository_commit=<unavailable>'
		printf '%s\n' 'repository_branch=<unavailable>'
		printf '%s\n' 'repository_status=<unavailable>'
	fi
	printf '\n'

	printf '%s\n' '[client_tools]'
	printf 'pgbench_path=%s\n' "$pgbench_command"
	printf 'psql_path=%s\n' "$psql_command"
	printf 'pg_config_path=%s\n' "$pg_config_command"
	printf '\n'

	printf '%s\n' '[host]'
	capture_command uname uname -a
	capture_command os_release sh -c 'if test -r /etc/os-release; then cat /etc/os-release; else exit 1; fi'
	capture_command cpu_count sh -c 'if command -v nproc >/dev/null 2>&1; then nproc --all; else getconf _NPROCESSORS_ONLN; fi'
	capture_command cpu_info sh -c 'if command -v lscpu >/dev/null 2>&1; then lscpu; else exit 1; fi'
	capture_command memory sh -c 'if command -v free >/dev/null 2>&1; then free -h; elif test -r /proc/meminfo; then cat /proc/meminfo; else exit 1; fi'
	capture_command filesystem sh -c 'df -h "$0"' "$bench_dir"
	capture_command compiler cc --version
	printf '\n'

	printf '%s\n' '[tool_versions]'
	if [[ "$pgbench_command" != '<unavailable>' ]]; then
		capture_command pgbench_version "$pgbench_command" --version
	else
		printf '%s\n' '[pgbench_version]'
		printf '%s\n' '<unavailable>'
	fi
	if [[ "$psql_command" != '<unavailable>' ]]; then
		capture_command psql_version "$psql_command" --version
	else
		printf '%s\n' '[psql_version]'
		printf '%s\n' '<unavailable>'
	fi
	if [[ -x "$pg_config_command" ]]; then
		capture_command pg_config_version "$pg_config_command" --version
		capture_command pg_config_configure "$pg_config_command" --configure
	else
		printf '%s\n' '[pg_config_version]'
		printf '%s\n' '<unavailable>'
	fi
} >"$environment_file"

printf 'Environment captured successfully.\n'
printf 'Environment file: %s\n' "$environment_file"
printf 'Database metadata: %s\n' "$database_metadata_file"
