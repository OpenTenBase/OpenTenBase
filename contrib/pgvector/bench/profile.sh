#!/usr/bin/env bash

set -euo pipefail

if (($# != 5)); then
	printf 'usage: %s POSTGRES PGDATA DBNAME l2|ip|cosine OUTPUT_DIR\n' "$0" >&2
	exit 1
fi

postgres=$1
pgdata=$2
dbname=$3
metric=$4
output_dir=$5

if [[ -f "$pgdata/postmaster.pid" ]]; then
	printf 'the cluster must be stopped before single-user profiling: %s\n' "$pgdata" >&2
	exit 1
fi

case "$metric" in
	l2)
		operator='<->'
		opclass=vector_l2_ops
		;;
	ip)
		operator='<#>'
		opclass=vector_ip_ops
		;;
	cosine)
		operator='<=>'
		opclass=vector_cosine_ops
		;;
	*)
		printf 'unknown metric: %s\n' "$metric" >&2
		exit 1
		;;
esac

profile_lists=${PROFILE_LISTS:-100}
profile_queries=${PROFILE_QUERIES:-1000}
if [[ ! "$profile_lists" =~ ^[1-9][0-9]*$ ]] || ((profile_lists > 32768)); then
	printf 'PROFILE_LISTS must be an integer from 1 to 32768\n' >&2
	exit 1
fi
if [[ ! "$profile_queries" =~ ^[1-9][0-9]*$ ]]; then
	printf 'PROFILE_QUERIES must be a positive integer\n' >&2
	exit 1
fi

mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
setup_sql="$output_dir/setup.sql"
sql="$output_dir/hotspot.sql"

printf '%s\n' \
	"DROP INDEX IF EXISTS vector_bench.items_embedding_idx;" \
	"CREATE INDEX items_embedding_idx ON vector_bench.items" \
	"  USING ivfflat (embedding $opclass) WITH (lists = $profile_lists);" \
	"ANALYZE vector_bench.items;" >"$setup_sql"

"$postgres" --single -j -D "$pgdata" "$dbname" <"$setup_sql" \
	>"$output_dir/setup.log" 2>&1

printf '%s\n' \
	"SET enable_seqscan = off;" \
	"SET ivfflat.probes = 32;" \
	"SET work_mem = '64MB';" \
	"SELECT count(*) FROM (" \
	"  SELECT q.id AS query_id, n.id" \
	"  FROM (SELECT id, embedding FROM vector_bench.queries WHERE id <= $profile_queries) q" \
	"  CROSS JOIN LATERAL (" \
	"    SELECT id FROM vector_bench.items" \
	"    ORDER BY embedding $operator q.embedding LIMIT 10" \
	"  ) n" \
	") results;" >"$sql"

experiment="$output_dir/experiment.er"
gprofng collect app -F off -o "$experiment" \
	"$postgres" --single -j -D "$pgdata" "$dbname" <"$sql" >"$output_dir/postgres.log" 2>&1
gprofng display text -functions "$experiment" >"$output_dir/functions.txt"
gprofng display text -calltree "$experiment" >"$output_dir/calltree.txt"

printf 'profile written to %s\n' "$output_dir"
