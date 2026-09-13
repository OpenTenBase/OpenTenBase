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

# Resolve the absolute path of the benchmark root directory.
bench_dir=$(
	cd -- "$script_dir/.."
	pwd
)

config_path="$bench_dir/config/smoke.conf"

# Do not select an output directory until the configuration is loaded.
output_dir=

# Whether dropping and recreating the vector_bench schema is allowed.
recreate=0

# Print command-line usage information.
print_usage()
{
	cat <<EOF
Usage:
  $0 [OPTIONS]

Prepare a deterministic dataset for the pgvector benchmark.

Options:
  --config FILE
      Read benchmark parameters from FILE.
      Default: $bench_dir/config/smoke.conf

  --output DIR
      Write preparation logs and metadata to DIR.
      Default: $bench_dir/results/PROFILE_NAME

  --recreate
      Drop and recreate the vector_bench schema.
      This option is required because the script replaces benchmark data.

  -h, --help
      Show this help message and exit.

The target PostgreSQL database must already exist.
This script does not create or drop the database itself.
EOF
}

# Parse command-line arguments.
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

		--recreate)
			recreate=1
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

# Load the benchmark configuration.

# The configuration file is a trusted Bash file stored in the repository.
set -a
# shellcheck disable=SC1090
source "$config_path"
set +a

# Preserve compatibility with existing profiles while allowing a caller to
# select halfvec with VECTOR_TYPE=halfvec.
VECTOR_TYPE=${VECTOR_TYPE:-vector}


# Check whether all required configuration variables are defined.
required_variables=(
	PROFILE_NAME
	ROWS
	DIMENSIONS
	QUERY_COUNT
	RECALL_QUERY_COUNT
	TOP_K
	RANDOM_SEED
	QUERY_RANDOM_SEED
	CLUSTERS
	CATEGORY_COUNT
	DB_HOST
	DB_PORT
	DB_USER
	DB_NAME
)

for variable_name in "${required_variables[@]}"; do
	if [[ -z "${!variable_name:-}" ]]; then
		printf 'required configuration variable is missing: %s\n' \
			"$variable_name" >&2
		exit 1
	fi
done

case "$VECTOR_TYPE" in
	vector)
		max_ivfflat_dimensions=2000
		;;
	halfvec)
		max_ivfflat_dimensions=4000
		;;
	*)
		printf 'VECTOR_TYPE must be vector or halfvec: %s\n' "$VECTOR_TYPE" >&2
		exit 1
		;;
esac

# Check whether a configuration variable is a positive integer.
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

numeric_variables=(
	ROWS
	DIMENSIONS
	QUERY_COUNT
	RECALL_QUERY_COUNT
	TOP_K
	RANDOM_SEED
	QUERY_RANDOM_SEED
	CLUSTERS
	CATEGORY_COUNT
	DB_PORT
)

for variable_name in "${numeric_variables[@]}"; do
	require_positive_integer "$variable_name"
done

# Enforce the IVFFlat dimension limit for the selected data type.
if ((DIMENSIONS > max_ivfflat_dimensions)); then
	printf 'DIMENSIONS must not exceed %s for IVFFlat %s: %s\n' \
		"$max_ivfflat_dimensions" "$VECTOR_TYPE" "$DIMENSIONS" >&2
	exit 1
fi

# The recall query count must not exceed the total query count.
if ((RECALL_QUERY_COUNT > QUERY_COUNT)); then
	printf '%s\n' \
		'RECALL_QUERY_COUNT must not exceed QUERY_COUNT' >&2
	exit 1
fi

# Top-K must not exceed the total number of item rows.
if ((TOP_K > ROWS)); then
	printf '%s\n' 'TOP_K must not exceed ROWS' >&2
	exit 1
fi

# The category count must not exceed the number of item rows.
if ((CATEGORY_COUNT > ROWS)); then
	printf '%s\n' 'CATEGORY_COUNT must not exceed ROWS' >&2
	exit 1
fi

# The PostgreSQL port must be within the valid range.
if ((DB_PORT > 65535)); then
	printf 'DB_PORT must not exceed 65535: %s\n' "$DB_PORT" >&2
	exit 1
fi

# The item dataset and query dataset should use different seeds.
if ((RANDOM_SEED == QUERY_RANDOM_SEED)); then
	printf '%s\n' \
		'RANDOM_SEED and QUERY_RANDOM_SEED must be different' >&2
	exit 1
fi

# Require explicit confirmation before dropping the benchmark schema.
if ((!recreate)); then
	printf '%s\n' \
		'--recreate is required because this script replaces vector_bench' >&2
	exit 1
fi


if [[ -z "$output_dir" ]]; then
	output_dir="$bench_dir/results/$PROFILE_NAME"
fi

# Create and normalize the output directory.
mkdir -p "$output_dir"
output_dir=$(
	cd -- "$output_dir"
	pwd
)


# Define the file paths used by this preparation step.
build_script="$bench_dir/scripts/build_tools.sh"
initialize_sql="$bench_dir/sql/initialize.sql"
generator_binary="$bench_dir/build/generate_vectors"

# Check whether the benchmark tool build script exists and is executable.
if [[ ! -x "$build_script" ]]; then
	printf 'build script is not executable: %s\n' "$build_script" >&2
	exit 1
fi


if [[ ! -s "$initialize_sql" ]]; then
	printf 'initialization SQL is missing or empty: %s\n' \
		"$initialize_sql" >&2
	exit 1
fi


if ! command -v psql >/dev/null 2>&1; then
	printf '%s\n' 'psql was not found in PATH' >&2
	exit 1
fi

# Build the benchmark helper programs.
"$build_script"

if [[ ! -x "$generator_binary" ]]; then
	printf 'vector generator was not built: %s\n' \
		"$generator_binary" >&2
	exit 1
fi

# Define the common psql command.
#
# -X: do not read the user's .psqlrc.
# ON_ERROR_STOP: stop immediately when any SQL statement fails.
psql_command=(
	psql
	-X
	-v ON_ERROR_STOP=1
	--host "$DB_HOST"
	--port "$DB_PORT"
	--username "$DB_USER"
	--dbname "$DB_NAME"
)

# Check whether the target database is reachable.
if ! "${psql_command[@]}" --quiet --tuples-only --command 'SELECT 1;' \
	>/dev/null; then
	printf 'could not connect to database %s at %s:%s as %s\n' \
		"$DB_NAME" "$DB_HOST" "$DB_PORT" "$DB_USER" >&2
	exit 1
fi


prepare_started_at=$(date --iso-8601=seconds)

# Recreate the benchmark schema and data tables.

"${psql_command[@]}" \
	-v "vector_type=$VECTOR_TYPE" \
	-v "dimensions=$DIMENSIONS" \
	-v "category_count=$CATEGORY_COUNT" \
	-f "$initialize_sql"

# Generate and import item vectors as a stream.
# Standard output contains only TSV data.
# Generator metadata is written to a separate log.
"$generator_binary" \
	--rows "$ROWS" \
	--dimensions "$DIMENSIONS" \
	--seed "$RANDOM_SEED" \
	--clusters "$CLUSTERS" \
	--output - \
	2>"$output_dir/items-generator.log" |
	"${psql_command[@]}" \
		--command '\copy vector_bench.items (id, embedding) FROM STDIN WITH (FORMAT text)'

"$generator_binary" \
	--rows "$QUERY_COUNT" \
	--dimensions "$DIMENSIONS" \
	--seed "$QUERY_RANDOM_SEED" \
	--clusters "$CLUSTERS" \
	--output - \
	2>"$output_dir/queries-generator.log" |
	"${psql_command[@]}" \
		--command '\copy vector_bench.queries (id, embedding) FROM STDIN WITH (FORMAT text)'

# Update planner statistics for the dataset tables.
"${psql_command[@]}" \
	--command 'ANALYZE vector_bench.items; ANALYZE vector_bench.queries;'

validation_result=$(
	"${psql_command[@]}" \
		--no-align \
		--tuples-only \
		--field-separator '|' \
		--command '
SELECT
    (SELECT count(*) FROM vector_bench.items),
    (SELECT count(*) FROM vector_bench.queries),
    (SELECT min(vector_dims(embedding)) FROM vector_bench.items),
	    (SELECT max(vector_dims(embedding)) FROM vector_bench.items),
	    (SELECT min(vector_dims(embedding)) FROM vector_bench.queries),
	    (SELECT max(vector_dims(embedding)) FROM vector_bench.queries),
	    (SELECT min(category_id) FROM vector_bench.items),
	    (SELECT max(category_id) FROM vector_bench.items),
	    (SELECT count(DISTINCT category_id) FROM vector_bench.items),
	    (SELECT min(category_rows) FROM (
	        SELECT count(*) AS category_rows
	        FROM vector_bench.items
	        GROUP BY category_id
	    ) AS category_distribution),
	    (SELECT max(category_rows) FROM (
	        SELECT count(*) AS category_rows
	        FROM vector_bench.items
	        GROUP BY category_id
	    ) AS category_distribution);
'
)

# Split the validation query result into individual variables.
IFS='|' read -r \
	actual_item_count \
	actual_query_count \
	min_item_dimensions \
	max_item_dimensions \
	min_query_dimensions \
	max_query_dimensions \
	min_category_id \
	max_category_id \
	actual_category_count \
	min_category_rows \
	max_category_rows \
	<<<"$validation_result"

# Check whether the item row count is correct.
if [[ "$actual_item_count" != "$ROWS" ]]; then
	printf 'item row count mismatch: expected %s, got %s\n' \
		"$ROWS" "$actual_item_count" >&2
	exit 1
fi


if [[ "$actual_query_count" != "$QUERY_COUNT" ]]; then
	printf 'query row count mismatch: expected %s, got %s\n' \
		"$QUERY_COUNT" "$actual_query_count" >&2
	exit 1
fi


if [[ "$min_item_dimensions" != "$DIMENSIONS" ||
	  "$max_item_dimensions" != "$DIMENSIONS" ]]; then
	printf 'item dimension mismatch: expected %s, got %s..%s\n' \
		"$DIMENSIONS" "$min_item_dimensions" "$max_item_dimensions" >&2
	exit 1
fi


if [[ "$min_query_dimensions" != "$DIMENSIONS" ||
	  "$max_query_dimensions" != "$DIMENSIONS" ]]; then
	printf 'query dimension mismatch: expected %s, got %s..%s\n' \
		"$DIMENSIONS" "$min_query_dimensions" "$max_query_dimensions" >&2
	exit 1
fi

# Check the generated category range and distinct category count.
if [[ "$min_category_id" != 0 ||
	  "$max_category_id" != "$((CATEGORY_COUNT - 1))" ||
	  "$actual_category_count" != "$CATEGORY_COUNT" ]]; then
	printf 'category mismatch: expected range 0..%s with %s categories, got %s..%s with %s categories\n' \
		"$((CATEGORY_COUNT - 1))" \
		"$CATEGORY_COUNT" \
		"$min_category_id" \
		"$max_category_id" \
		"$actual_category_count" >&2
	exit 1
fi

# Deterministic modulo assignment keeps category sizes within one row.
if ((max_category_rows - min_category_rows > 1)); then
	printf 'category distribution is unbalanced: %s..%s rows per category\n' \
		"$min_category_rows" "$max_category_rows" >&2
	exit 1
fi

# Read the PostgreSQL and pgvector versions.
postgres_version=$(
	"${psql_command[@]}" \
		--no-align \
		--tuples-only \
		--command 'SHOW server_version;'
)

vector_version=$(
	"${psql_command[@]}" \
		--no-align \
		--tuples-only \
		--command \
		"SELECT extversion FROM pg_extension WHERE extname = 'vector';"
)

# Read the current Git revision.
source_revision=$(
	git -C "$bench_dir" rev-parse HEAD 2>/dev/null ||
	printf '%s\n' unknown
)

prepare_finished_at=$(date --iso-8601=seconds)

# Save the effective dataset configuration and validation result.
metadata_file="$output_dir/dataset_metadata.txt"

{
	printf 'profile_name=%s\n' "$PROFILE_NAME"
	printf 'vector_type=%s\n' "$VECTOR_TYPE"
	printf 'rows=%s\n' "$ROWS"
	printf 'dimensions=%s\n' "$DIMENSIONS"
	printf 'query_count=%s\n' "$QUERY_COUNT"
	printf 'recall_query_count=%s\n' "$RECALL_QUERY_COUNT"
	printf 'top_k=%s\n' "$TOP_K"
	printf 'random_seed=%s\n' "$RANDOM_SEED"
	printf 'query_random_seed=%s\n' "$QUERY_RANDOM_SEED"
	printf 'clusters=%s\n' "$CLUSTERS"
	printf 'category_count=%s\n' "$CATEGORY_COUNT"
	printf 'database_host=%s\n' "$DB_HOST"
	printf 'database_port=%s\n' "$DB_PORT"
	printf 'database_user=%s\n' "$DB_USER"
	printf 'database_name=%s\n' "$DB_NAME"
	printf 'postgres_version=%s\n' "$postgres_version"
	printf 'vector_version=%s\n' "$vector_version"
	printf 'source_revision=%s\n' "$source_revision"
	printf 'actual_item_count=%s\n' "$actual_item_count"
	printf 'actual_query_count=%s\n' "$actual_query_count"
	printf 'item_dimensions=%s..%s\n' \
		"$min_item_dimensions" "$max_item_dimensions"
	printf 'query_dimensions=%s..%s\n' \
		"$min_query_dimensions" "$max_query_dimensions"
	printf 'category_id_range=%s..%s\n' \
		"$min_category_id" "$max_category_id"
	printf 'actual_category_count=%s\n' "$actual_category_count"
	printf 'category_rows=%s..%s\n' \
		"$min_category_rows" "$max_category_rows"
	printf 'prepare_started_at=%s\n' "$prepare_started_at"
	printf 'prepare_finished_at=%s\n' "$prepare_finished_at"
} >"$metadata_file"


printf '%s\n' 'Prepared dataset successfully.'
printf 'Items: %s\n' "$actual_item_count"
printf 'Queries: %s\n' "$actual_query_count"
printf 'Dimensions: %s\n' "$DIMENSIONS"
printf 'Metadata: %s\n' "$metadata_file"
