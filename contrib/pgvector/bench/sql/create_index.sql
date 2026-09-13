-- Create an IVFFlat index with the specified parameters.

-- This script requires the following variables to be passed with psql -v.
-- metric: distance metric, one of l2, ip, or cosine.
-- vector_type: data type, vector or halfvec (defaults to vector).
-- opclass: the pgvector operator class matching the distance metric.
-- lists: the lists parameter of the IVFFlat index.
--
-- Example:
--
-- psql -X -v ON_ERROR_STOP=1 \
--     -v metric=l2 \
--     -v opclass=vector_l2_ops \
--     -v lists=10 \
--     -f sql/create_index.sql

-- Stop immediately when any SQL command fails.
\set ON_ERROR_STOP on

\if :{?vector_type}
\else
\set vector_type vector
\endif

-- Check whether the metric variable is defined.
\if :{?metric}
\else
DO $$
BEGIN
	RAISE EXCEPTION 'missing required psql variable: metric';
END
$$;
\endif


\if :{?opclass}
\else
DO $$
BEGIN
	RAISE EXCEPTION 'missing required psql variable: opclass';
END
$$;
\endif


\if :{?lists}
\else
DO $$
BEGIN
	RAISE EXCEPTION 'missing required psql variable: lists';
END
$$;
\endif


SELECT CASE
		   WHEN :'metric' IN ('l2', 'ip', 'cosine')
		   THEN 1
		   ELSE 0
	   END AS valid_metric \gset

-- Stop execution when metric is invalid.
\if :valid_metric
\else
DO $$
BEGIN
	RAISE EXCEPTION 'metric must be one of: l2, ip, cosine';
END
$$;
\endif

SELECT CASE
		   WHEN :'vector_type' = 'vector'
				AND :'metric' = 'l2'
				AND :'opclass' = 'vector_l2_ops'
		   THEN 1

		   WHEN :'vector_type' = 'vector'
				AND :'metric' = 'ip'
				AND :'opclass' = 'vector_ip_ops'
		   THEN 1

		   WHEN :'vector_type' = 'vector'
				AND :'metric' = 'cosine'
				AND :'opclass' = 'vector_cosine_ops'
		   THEN 1

		   WHEN :'vector_type' = 'halfvec'
				AND :'metric' = 'l2'
				AND :'opclass' = 'halfvec_l2_ops'
		   THEN 1

		   WHEN :'vector_type' = 'halfvec'
				AND :'metric' = 'ip'
				AND :'opclass' = 'halfvec_ip_ops'
		   THEN 1

		   WHEN :'vector_type' = 'halfvec'
				AND :'metric' = 'cosine'
				AND :'opclass' = 'halfvec_cosine_ops'
		   THEN 1

		   ELSE 0
	   END AS valid_index_definition \gset

-- Stop execution when metric and opclass do not match.
\if :valid_index_definition
\else
DO $$
BEGIN
	RAISE EXCEPTION 'vector_type, metric, and opclass do not match';
END
$$;
\endif

-- Check whether lists is a positive integer between 1 and 32768.
SELECT CASE
		   WHEN :'lists' ~ '^[1-9][0-9]*$'
				AND CAST(:'lists' AS bigint) BETWEEN 1 AND 32768
		   THEN 1
		   ELSE 0
	   END AS valid_lists \gset

\if :valid_lists
\else
DO $$
BEGIN
	RAISE EXCEPTION 'lists must be an integer between 1 and 32768';
END
$$;
\endif

-- Optionally set maintenance_work_mem used during index creation.
\if :{?maintenance_work_mem}
SET maintenance_work_mem TO :'maintenance_work_mem';
\endif

-- Optionally set the number of parallel index-build workers.
\if :{?max_parallel_maintenance_workers}
SELECT CASE
		   WHEN :'max_parallel_maintenance_workers' ~ '^[0-9]+$'
		   THEN 1
		   ELSE 0
	   END AS valid_parallel_workers \gset

\if :valid_parallel_workers
SET max_parallel_maintenance_workers TO :max_parallel_maintenance_workers;
\else
DO $$
BEGIN
	RAISE EXCEPTION 'max_parallel_maintenance_workers must be a non-negative integer';
END
$$;
\endif
\endif


DROP INDEX IF EXISTS vector_bench.items_embedding_idx;

-- Create the new IVFFlat index.
CREATE INDEX items_embedding_idx
	ON vector_bench.items
	USING ivfflat (embedding :opclass)
	WITH (lists = :lists);


ANALYZE vector_bench.items;

SELECT
	:'metric' AS metric,
	:'opclass' AS opclass,
	:lists::integer AS lists,
	pg_size_pretty(
		pg_relation_size('vector_bench.items_embedding_idx')
	) AS index_size,
	pg_relation_size('vector_bench.items_embedding_idx') AS index_bytes,
	pg_relation_filepath(
		'vector_bench.items_embedding_idx'
	) AS index_filepath;
