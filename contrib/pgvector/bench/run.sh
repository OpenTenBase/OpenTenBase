#!/usr/bin/env bash

set -euo pipefail

bench_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
profile=quick
variant=optimized
output_dir=
prepare=0

usage() {
	printf 'usage: %s --profile quick|memory|disk --variant NAME --output DIR [--prepare]\n' "$0"
}

while (($# > 0)); do
	case "$1" in
		--profile)
			profile=$2
			shift 2
			;;
		--variant)
			variant=$2
			shift 2
			;;
		--output)
			output_dir=$2
			shift 2
			;;
		--prepare)
			prepare=1
			shift
			;;
		--help|-h)
			usage
			exit 0
			;;
		*)
			usage >&2
			exit 1
			;;
	esac
done

if [[ -z "$output_dir" ]]; then
	usage >&2
	exit 1
fi

case "$profile" in
	quick)
		rows=${ROWS:-100000}
		dimensions=${DIMENSIONS:-128}
		recall_queries=${RECALL_QUERIES:-50}
		query_count=${QUERY_COUNT:-1000}
		clusters=${CLUSTERS:-100}
		lists_values=${LISTS_VALUES:-100}
		;;
	memory)
		rows=${ROWS:-1000000}
		dimensions=${DIMENSIONS:-768}
		recall_queries=${RECALL_QUERIES:-200}
		query_count=${QUERY_COUNT:-10000}
		clusters=${CLUSTERS:-1024}
		lists_values=${LISTS_VALUES:-"100 500 1000"}
		;;
	disk)
		rows=${ROWS:-8000000}
		dimensions=${DIMENSIONS:-384}
		recall_queries=${RECALL_QUERIES:-20}
		query_count=${QUERY_COUNT:-10000}
		clusters=${CLUSTERS:-1024}
		lists_values=${LISTS_VALUES:-1000}
		;;
	*)
		printf 'unknown profile: %s\n' "$profile" >&2
		exit 1
		;;
esac

pguri=${PGURI:-postgresql:///postgres}
seed=${SEED:-20260825}
k=${K:-10}
warmup_seconds=${WARMUP_SECONDS:-15}
duration_seconds=${DURATION_SECONDS:-60}
repeats=${REPEATS:-3}
throughput_clients=${THROUGHPUT_CLIENTS:-12}
jobs=${JOBS:-12}
cc=${CC:-cc}
pg_config_bin=${PG_CONFIG:-pg_config}

mkdir -p "$output_dir/bin" "$output_dir/logs"
output_dir=$(cd "$output_dir" && pwd)
generator="$output_dir/bin/gen_vectors"
evictor="$output_dir/bin/evict_cache"

"$cc" -O3 -std=c11 -Wall -Wextra -Werror "$bench_dir/gen_vectors.c" -o "$generator"
"$cc" -O2 -std=c11 -Wall -Wextra -Werror "$bench_dir/evict_cache.c" -o "$evictor"

psql_cmd=(psql -X -v ON_ERROR_STOP=1 "$pguri")
pgbench_cmd=(pgbench -n "$pguri")

metric_operator() {
	case "$1" in
		l2) printf '<->' ;;
		ip) printf '<#>' ;;
		cosine) printf '<=>' ;;
	esac
}

metric_opclass() {
	case "$1" in
		l2) printf 'vector_l2_ops' ;;
		ip) printf 'vector_ip_ops' ;;
		cosine) printf 'vector_cosine_ops' ;;
	esac
}

probe_values() {
	case "$profile:$1" in
		quick:*) printf '%s' "${PROBES_VALUES:-1 10 20}" ;;
		memory:100) printf '%s' "${PROBES_VALUES:-1 5 10 20 100}" ;;
		memory:500) printf '%s' "${PROBES_VALUES:-1 11 22 44 50 500}" ;;
		memory:1000) printf '%s' "${PROBES_VALUES:-1 16 32 64 100 1000}" ;;
		disk:*) printf '%s' "${PROBES_VALUES:-16 32 64}" ;;
		*) printf '%s' "${PROBES_VALUES:-1}" ;;
	esac
}

capture_environment() {
	{
		printf 'captured_at=%s\n' "$(date --iso-8601=seconds)"
		printf 'variant=%s\nprofile=%s\nrows=%s\ndimensions=%s\n' "$variant" "$profile" "$rows" "$dimensions"
		printf '\n[uname]\n'
		uname -a
		printf '\n[lscpu]\n'
		lscpu
		printf '\n[memory]\n'
		free -h
		printf '\n[storage]\n'
		lsblk -o NAME,TYPE,SIZE,ROTA,MODEL,MOUNTPOINTS
		df -hT "$output_dir"
		printf '\n[compiler]\n'
		"$cc" --version
		printf '\n[postgres]\n'
		"${psql_cmd[@]}" -Atc "SELECT version(); SHOW server_version_num; SHOW shared_buffers; SHOW work_mem; SHOW effective_cache_size; SHOW max_parallel_workers_per_gather; SELECT extversion FROM pg_extension WHERE extname = 'vector';"
		printf '\n[module]\n'
		sha256sum "$("$pg_config_bin" --pkglibdir)/vector.so"
		printf '\n[source]\n'
		git -C "$bench_dir" rev-parse HEAD 2>/dev/null || true
	} >"$output_dir/machine.txt"
}

prepare_data() {
	"${psql_cmd[@]}" <<SQL
DROP SCHEMA IF EXISTS vector_bench CASCADE;
CREATE SCHEMA vector_bench;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
CREATE TABLE vector_bench.items (id bigint PRIMARY KEY, embedding vector($dimensions) NOT NULL);
CREATE TABLE vector_bench.queries (id bigint PRIMARY KEY, embedding vector($dimensions) NOT NULL);
CREATE TABLE vector_bench.truth (
	metric text NOT NULL,
	query_id bigint NOT NULL,
	item_id bigint NOT NULL,
	rank integer NOT NULL,
	PRIMARY KEY (metric, query_id, rank)
);
SQL

	"$generator" "$rows" "$dimensions" "$seed" "$clusters" |
		"${psql_cmd[@]}" -c '\copy vector_bench.items FROM STDIN WITH (FORMAT binary)'
	"$generator" "$query_count" "$dimensions" "$((seed + 1))" "$clusters" |
		"${psql_cmd[@]}" -c '\copy vector_bench.queries FROM STDIN WITH (FORMAT binary)'
	"${psql_cmd[@]}" -c 'ANALYZE vector_bench.items; ANALYZE vector_bench.queries;'

	for metric in l2 ip cosine; do
		operator=$(metric_operator "$metric")
		"${psql_cmd[@]}" <<SQL
SET enable_indexscan = off;
SET enable_bitmapscan = off;
INSERT INTO vector_bench.truth (metric, query_id, item_id, rank)
SELECT '$metric', q.id, nearest.id, nearest.rank
FROM (SELECT id, embedding FROM vector_bench.queries WHERE id <= $recall_queries) q
CROSS JOIN LATERAL (
	SELECT id, row_number() OVER (ORDER BY distance, id)::integer AS rank
	FROM (
		SELECT i.id, i.embedding $operator q.embedding AS distance
		FROM vector_bench.items i
		ORDER BY i.embedding $operator q.embedding
		LIMIT $k
	) ordered
) nearest;
SQL
	done
}

measure_recall() {
	local metric=$1
	local operator=$2
	local probes=$3

	"${psql_cmd[@]}" -At -F '|' <<SQL
SET enable_seqscan = off;
SET ivfflat.probes = $probes;
WITH approximate AS (
	SELECT q.id AS query_id, nearest.id AS item_id, nearest.rank
	FROM (SELECT id, embedding FROM vector_bench.queries WHERE id <= $recall_queries) q
	CROSS JOIN LATERAL (
		SELECT id, row_number() OVER (ORDER BY distance, id)::integer AS rank
		FROM (
			SELECT i.id, i.embedding $operator q.embedding AS distance
			FROM vector_bench.items i
			ORDER BY i.embedding $operator q.embedding
			LIMIT $k
		) ordered
	) nearest
), scored AS (
	SELECT a.*, (t.item_id IS NOT NULL)::integer AS recalled
	FROM approximate a
	LEFT JOIN vector_bench.truth t
		ON t.metric = '$metric'
		AND t.query_id = a.query_id
		AND t.item_id = a.item_id
)
SELECT round(sum(recalled)::numeric / ($recall_queries * $k), 6),
	md5(string_agg(query_id || ':' || rank || ':' || item_id, ',' ORDER BY query_id, rank)),
	count(*)
FROM scored;
SQL
}

evict_disk_cache() {
	local table_path=$1
	local index_path=$2

	if [[ "$profile" != disk ]]; then
		return
	fi
	if [[ -z "${PGDATA:-}" || -z "${PG_CTL:-}" || ! -f "$PGDATA/PG_VERSION" ]]; then
		printf 'disk profile requires PGDATA and PG_CTL for controlled cache eviction\n' >&2
		exit 1
	fi
	"$PG_CTL" -D "$PGDATA" -m fast -w stop
	sync
	"$evictor" "$PGDATA/$table_path" "$PGDATA/$index_path"
	"$PG_CTL" -D "$PGDATA" -w start
}

if ((prepare)); then
	prepare_data
else
	"${psql_cmd[@]}" -Atc "SELECT count(*) FROM vector_bench.truth" >/dev/null
fi

capture_environment

csv="$output_dir/results.csv"
printf 'variant,profile,metric,lists,probes,clients,repeat,recall,result_hash,p50_ms,p95_ms,qps,table_bytes,index_bytes,result_count\n' >"$csv"

for metric in l2 ip cosine; do
	operator=$(metric_operator "$metric")
	opclass=$(metric_opclass "$metric")
	query_script="$bench_dir/query_${metric}.sql"

	for lists in $lists_values; do
		"${psql_cmd[@]}" <<SQL
DROP INDEX IF EXISTS vector_bench.items_embedding_idx;
SET maintenance_work_mem = '${MAINTENANCE_WORK_MEM:-2GB}';
SET max_parallel_maintenance_workers = ${MAX_PARALLEL_MAINTENANCE_WORKERS:-4};
CREATE INDEX items_embedding_idx ON vector_bench.items
USING ivfflat (embedding $opclass) WITH (lists = $lists);
ANALYZE vector_bench.items;
SQL

		read -r table_bytes index_bytes table_path index_path < <(
			"${psql_cmd[@]}" -At -F ' ' -c "SELECT pg_relation_size('vector_bench.items'), pg_relation_size('vector_bench.items_embedding_idx'), pg_relation_filepath('vector_bench.items'), pg_relation_filepath('vector_bench.items_embedding_idx');")

		if [[ "$profile" == memory ]]; then
			"${psql_cmd[@]}" -c "SELECT pg_prewarm('vector_bench.items'); SELECT pg_prewarm('vector_bench.items_embedding_idx');" >"$output_dir/logs/prewarm_${metric}_${lists}.txt"
		fi

		for probes in $(probe_values "$lists"); do
			IFS='|' read -r recall result_hash result_count < <(measure_recall "$metric" "$operator" "$probes" | tail -n 1)

			for clients in 1 "$throughput_clients"; do
				client_jobs=$clients
				if ((client_jobs > jobs)); then
					client_jobs=$jobs
				fi

				for repeat in $(seq 1 "$repeats"); do
					export PGOPTIONS="-c ivfflat.probes=$probes -c enable_seqscan=off -c jit=off -c work_mem=${WORK_MEM:-64MB}"
					"${pgbench_cmd[@]}" -c "$clients" -j "$client_jobs" -T "$warmup_seconds" \
						-D "query_count=$query_count" -D "k=$k" -f "$query_script" >/dev/null
					# The disk sample starts immediately after relation cache eviction.
					evict_disk_cache "$table_path" "$index_path"

					prefix="$output_dir/logs/${variant}_${profile}_${metric}_l${lists}_p${probes}_c${clients}_r${repeat}"
					summary="$prefix-summary.txt"
					"${pgbench_cmd[@]}" -c "$clients" -j "$client_jobs" -T "$duration_seconds" -l \
						--log-prefix="$prefix" -D "query_count=$query_count" -D "k=$k" \
						-f "$query_script" >"$summary"

					qps=$(awk '/^tps =/{print $3; exit}' "$summary")
					latencies=$(awk '{print $3}' "$prefix".[0-9]* | LC_ALL=C sort -n | gawk -f "$bench_dir/latency.awk")
					IFS=',' read -r p50 p95 <<<"$latencies"
					printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
						"$variant" "$profile" "$metric" "$lists" "$probes" "$clients" "$repeat" \
						"$recall" "$result_hash" "$p50" "$p95" "$qps" "$table_bytes" "$index_bytes" "$result_count" >>"$csv"
				done
			done
		done
	done
done

"${psql_cmd[@]}" --csv -c "SELECT * FROM pg_stat_io ORDER BY backend_type, object, context" >"$output_dir/pg_stat_io.csv"
printf 'results written to %s\n' "$output_dir"
