-- Measure Recall@K of IVFFlat approximate results against exact ground truth.

-- Example:
--
-- psql -X -v ON_ERROR_STOP=1 \
--     -v metric=cosine \
--     -v probes=2 \
--     -v recall_query_count=100 \
--     -v top_k=10 \
--     -f sql/measure_recall.sql


\set ON_ERROR_STOP on

\if :{?vector_type}
\else
\set vector_type vector
\endif


-- Check whether the required psql variables are defined.
\if :{?metric}
\else
DO $$
BEGIN
	RAISE EXCEPTION 'missing required psql variable: metric';
END
$$;
\endif

\if :{?probes}
\else
DO $$
BEGIN
	RAISE EXCEPTION 'missing required psql variable: probes';
END
$$;
\endif

\if :{?recall_query_count}
\else
DO $$
BEGIN
	RAISE EXCEPTION 'missing required psql variable: recall_query_count';
END
$$;
\endif

\if :{?top_k}
\else
DO $$
BEGIN
	RAISE EXCEPTION 'missing required psql variable: top_k';
END
$$;
\endif

-- Validate the distance metric and determine its operator class.
SELECT
	CASE
		WHEN :'metric' IN ('l2', 'ip', 'cosine') THEN 1
		ELSE 0
	END AS valid_metric,
	(:'vector_type' IN ('vector', 'halfvec'))::integer AS valid_vector_type,
	CASE :'metric'
		WHEN 'l2' THEN :'vector_type' || '_l2_ops'
		WHEN 'ip' THEN :'vector_type' || '_ip_ops'
		WHEN 'cosine' THEN :'vector_type' || '_cosine_ops'
		ELSE ''
	END AS expected_opclass
\gset

\if :valid_metric
\else
DO $$
BEGIN
	RAISE EXCEPTION 'metric must be one of: l2, ip, cosine';
END
$$;
\endif

\if :valid_vector_type
\else
DO $$
BEGIN
	RAISE EXCEPTION 'vector_type must be vector or halfvec';
END
$$;
\endif


-- Validate probes, query count, and Top-K as valid positive integers.
SELECT
	CASE
		WHEN :'probes' ~ '^[1-9][0-9]*$' THEN
			(CAST(:'probes' AS bigint) BETWEEN 1 AND 32768)::integer
		ELSE 0
	END AS valid_probes,
	CASE
		WHEN :'recall_query_count' ~ '^[1-9][0-9]*$' THEN
			(CAST(:'recall_query_count' AS bigint) <= 2147483647)::integer
		ELSE 0
	END AS valid_recall_query_count,
	CASE
		WHEN :'top_k' ~ '^[1-9][0-9]*$' THEN
			(CAST(:'top_k' AS bigint) <= 2147483647)::integer
		ELSE 0
	END AS valid_top_k
\gset

\if :valid_probes
\else
DO $$
BEGIN
	RAISE EXCEPTION 'probes must be an integer between 1 and 32768';
END
$$;
\endif

\if :valid_recall_query_count
\else
DO $$
BEGIN
	RAISE EXCEPTION 'recall_query_count must be a positive integer';
END
$$;
\endif

\if :valid_top_k
\else
DO $$
BEGIN
	RAISE EXCEPTION 'top_k must be a positive integer';
END
$$;
\endif

-- Read the current test index type and lists parameter from system catalogs.
SELECT
	CASE WHEN count(*) = 1 THEN 1 ELSE 0 END AS valid_index,
	COALESCE(max(operator_class.opcname), '') AS current_opclass,
	COALESCE(max(index_option.option_value::integer), 0) AS index_lists
FROM pg_class AS index_relation
JOIN pg_namespace AS index_namespace
	ON index_namespace.oid = index_relation.relnamespace
JOIN pg_index AS index_metadata
	ON index_metadata.indexrelid = index_relation.oid
JOIN pg_am AS access_method
	ON access_method.oid = index_relation.relam
JOIN pg_opclass AS operator_class
	ON operator_class.oid = index_metadata.indclass[0]
LEFT JOIN LATERAL pg_options_to_table(index_relation.reloptions) AS index_option
	ON index_option.option_name = 'lists'
WHERE index_namespace.nspname = 'vector_bench'
	AND index_relation.relname = 'items_embedding_idx'
	AND access_method.amname = 'ivfflat'
\gset

\if :valid_index
\else
DO $$
BEGIN
	RAISE EXCEPTION 'vector_bench.items_embedding_idx must be an IVFFlat index';
END
$$;
\endif


-- Prevent misleading results from an index with the wrong distance metric.
SELECT (:'current_opclass' = :'expected_opclass')::integer AS matching_opclass
\gset

\if :matching_opclass
\else
DO $$
BEGIN
	RAISE EXCEPTION 'current IVFFlat operator class does not match metric';
END
$$;
\endif


-- Reject probes greater than lists because pgvector would clamp the actual value.
SELECT (CAST(:'probes' AS integer) <= :index_lists)::integer AS probes_within_lists
\gset

\if :probes_within_lists
\else
DO $$
BEGIN
	RAISE EXCEPTION 'probes must not exceed the current index lists value';
END
$$;
\endif

-- Check that complete exact Top-K ground truth exists for the selected metric.
SELECT
	(
		count(*) = CAST(:'recall_query_count' AS bigint) * CAST(:'top_k' AS bigint)
		AND count(DISTINCT query_id) = CAST(:'recall_query_count' AS bigint)
		AND min(rank) = 1
		AND max(rank) = CAST(:'top_k' AS integer)
	)::integer AS valid_ground_truth
FROM vector_bench.truth
WHERE metric = :'metric'
	AND query_id <= CAST(:'recall_query_count' AS bigint)
	AND rank <= CAST(:'top_k' AS integer)
\gset

\if :valid_ground_truth
\else
DO $$
BEGIN
	RAISE EXCEPTION 'complete ground truth is not available for the requested metric, query count, and top_k';
END
$$;
\endif


BEGIN;

-- Force approximate queries to use the current IVFFlat index.
SET LOCAL enable_seqscan = off;
SET LOCAL enable_indexscan = on;
SET LOCAL enable_bitmapscan = off;
SET LOCAL ivfflat.probes TO :probes;

-- Store approximate Top-K items returned for each query.
CREATE TEMPORARY TABLE approximate_results
(
	query_id bigint NOT NULL,
	item_id bigint NOT NULL,
	PRIMARY KEY (query_id, item_id)
)
ON COMMIT DROP;

-- Execute approximate queries with the distance operator for the selected metric.
\if :{?valid_metric}
SELECT
	(:'metric' = 'l2')::integer AS use_l2,
	(:'metric' = 'ip')::integer AS use_ip
\gset
\endif

\if :use_l2
INSERT INTO approximate_results (query_id, item_id)
SELECT
	query_vector.id,
	neighbor.id
FROM vector_bench.queries AS query_vector
CROSS JOIN LATERAL (
	SELECT item.id
	FROM vector_bench.items AS item
	ORDER BY item.embedding <-> query_vector.embedding
	LIMIT :top_k
) AS neighbor
WHERE query_vector.id <= :recall_query_count;
\elif :use_ip
INSERT INTO approximate_results (query_id, item_id)
SELECT
	query_vector.id,
	neighbor.id
FROM vector_bench.queries AS query_vector
CROSS JOIN LATERAL (
	SELECT item.id
	FROM vector_bench.items AS item
	ORDER BY item.embedding <#> query_vector.embedding
	LIMIT :top_k
) AS neighbor
WHERE query_vector.id <= :recall_query_count;
\else
INSERT INTO approximate_results (query_id, item_id)
SELECT
	query_vector.id,
	neighbor.id
FROM vector_bench.queries AS query_vector
CROSS JOIN LATERAL (
	SELECT item.id
	FROM vector_bench.items AS item
	ORDER BY item.embedding <=> query_vector.embedding
	LIMIT :top_k
) AS neighbor
WHERE query_vector.id <= :recall_query_count;
\endif

-- Count matches for each query and aggregate the overall Recall@K.
WITH per_query AS
(
	SELECT
		ground_truth.query_id,
		count(approximate.item_id)::integer AS matched_items,
		count(*)::integer AS expected_items,
		count(approximate.item_id)::numeric / count(*)::numeric AS recall
	FROM vector_bench.truth AS ground_truth
	LEFT JOIN approximate_results AS approximate
		ON approximate.query_id = ground_truth.query_id
		AND approximate.item_id = ground_truth.item_id
	WHERE ground_truth.metric = :'metric'
		AND ground_truth.query_id <= :recall_query_count
		AND ground_truth.rank <= :top_k
	GROUP BY ground_truth.query_id
),
approximate_summary AS
(
	SELECT count(*)::bigint AS returned_items
	FROM approximate_results
)
SELECT
	:'metric' AS metric,
	:index_lists::integer AS lists,
	:probes::integer AS probes,
	count(*)::integer AS query_count,
	:top_k::integer AS top_k,
	approximate_summary.returned_items,
	sum(per_query.matched_items)::bigint AS matched_items,
	sum(per_query.expected_items)::bigint AS expected_items,
	round(avg(per_query.recall), 6) AS recall_at_k,
	round(min(per_query.recall), 6) AS min_query_recall,
	round(max(per_query.recall), 6) AS max_query_recall
FROM per_query
CROSS JOIN approximate_summary
GROUP BY approximate_summary.returned_items;

-- Commit the transaction and automatically drop the temporary result table.
COMMIT;
