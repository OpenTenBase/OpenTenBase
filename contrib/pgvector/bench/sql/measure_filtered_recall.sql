-- Measure filtered IVFFlat Recall@K against exact filtered ground truth.

-- Example:
--
-- psql -X -v ON_ERROR_STOP=1 \
--     -v metric=l2 \
--     -v filter_name=percent_1 \
--     -v filter_limit=10 \
--     -v probes=1 \
--     -v iterative_scan=relaxed_order \
--     -v max_probes=1000 \
--     -v recall_query_count=100 \
--     -v top_k=10 \
--     -f sql/measure_filtered_recall.sql

\set ON_ERROR_STOP on

\if :{?vector_type}
\else
\set vector_type vector
\endif

-- Check that all required psql variables are defined.
\if :{?metric}
\else
\echo 'missing required psql variable: metric'
\quit 1
\endif

\if :{?filter_name}
\else
\echo 'missing required psql variable: filter_name'
\quit 1
\endif

\if :{?filter_limit}
\else
\echo 'missing required psql variable: filter_limit'
\quit 1
\endif

\if :{?probes}
\else
\echo 'missing required psql variable: probes'
\quit 1
\endif

\if :{?iterative_scan}
\else
\echo 'missing required psql variable: iterative_scan'
\quit 1
\endif

\if :{?max_probes}
\else
\echo 'missing required psql variable: max_probes'
\quit 1
\endif

\if :{?recall_query_count}
\else
\echo 'missing required psql variable: recall_query_count'
\quit 1
\endif

\if :{?top_k}
\else
\echo 'missing required psql variable: top_k'
\quit 1
\endif

-- Validate metric, filter, scan mode, and numeric parameters.
SELECT
    (:'metric' IN ('l2', 'ip', 'cosine'))::integer AS valid_metric,
    (:'vector_type' IN ('vector', 'halfvec'))::integer AS valid_vector_type,
    (:'filter_name' IN ('percent_1', 'percent_0_1'))::integer AS valid_filter,
    (:'iterative_scan' IN ('off', 'relaxed_order'))::integer AS valid_iterative_scan,
    (:'filter_limit' ~ '^[1-9][0-9]*$')::integer AS valid_filter_limit,
    (:'probes' ~ '^[1-9][0-9]*$')::integer AS valid_probes,
    (:'max_probes' ~ '^[1-9][0-9]*$')::integer AS valid_max_probes,
    (:'recall_query_count' ~ '^[1-9][0-9]*$')::integer AS valid_recall_query_count,
    (:'top_k' ~ '^[1-9][0-9]*$')::integer AS valid_top_k
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

\if :valid_filter
\else
DO $$
BEGIN
    RAISE EXCEPTION 'filter_name must be percent_1 or percent_0_1';
END
$$;
\endif

\if :valid_iterative_scan
\else
DO $$
BEGIN
    RAISE EXCEPTION 'iterative_scan must be off or relaxed_order';
END
$$;
\endif

\if :valid_filter_limit
\else
DO $$
BEGIN
    RAISE EXCEPTION 'filter_limit must be a positive integer';
END
$$;
\endif

\if :valid_probes
\else
DO $$
BEGIN
    RAISE EXCEPTION 'probes must be a positive integer';
END
$$;
\endif

\if :valid_max_probes
\else
DO $$
BEGIN
    RAISE EXCEPTION 'max_probes must be a positive integer';
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

-- Determine the expected operator class for the selected metric.
SELECT CASE :'metric'
    WHEN 'l2' THEN :'vector_type' || '_l2_ops'
    WHEN 'ip' THEN :'vector_type' || '_ip_ops'
    ELSE :'vector_type' || '_cosine_ops'
END AS expected_opclass
\gset

-- Read the current IVFFlat index metadata.
SELECT
    (count(*) = 1)::integer AS valid_index,
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

SELECT
    (CAST(:'probes' AS integer) <= :index_lists)::integer AS probes_within_lists,
    (CAST(:'max_probes' AS integer) >= CAST(:'probes' AS integer))::integer AS max_probes_valid
\gset

\if :probes_within_lists
\else
DO $$
BEGIN
    RAISE EXCEPTION 'probes must not exceed index lists';
END
$$;
\endif

\if :max_probes_valid
\else
DO $$
BEGIN
    RAISE EXCEPTION 'max_probes must not be lower than probes';
END
$$;
\endif

-- Verify that the exact filtered Top-K ground truth is complete.
SELECT
    (
        count(*) = CAST(:'recall_query_count' AS bigint) * CAST(:'top_k' AS bigint)
        AND count(DISTINCT query_id) = CAST(:'recall_query_count' AS bigint)
        AND min(rank) = 1
        AND max(rank) = CAST(:'top_k' AS integer)
    )::integer AS valid_ground_truth
FROM vector_bench.filtered_truth
WHERE filter_name = :'filter_name'
    AND metric = :'metric'
    AND query_id <= CAST(:'recall_query_count' AS bigint)
    AND rank <= CAST(:'top_k' AS integer)
\gset

\if :valid_ground_truth
\else
DO $$
BEGIN
    RAISE EXCEPTION 'complete filtered ground truth is not available';
END
$$;
\endif

BEGIN;

-- Keep approximate scan settings local to this measurement.
SET LOCAL enable_seqscan = off;
SET LOCAL enable_indexscan = on;
SET LOCAL enable_bitmapscan = off;
SET LOCAL ivfflat.probes = :'probes';
SET LOCAL ivfflat.iterative_scan = :'iterative_scan';
SET LOCAL ivfflat.max_probes = :'max_probes';

CREATE TEMPORARY TABLE approximate_results
(
    query_id bigint NOT NULL,
    item_id bigint NOT NULL,
    PRIMARY KEY (query_id, item_id)
)
ON COMMIT DROP;

SELECT
    (:'metric' = 'l2')::integer AS use_l2,
    (:'metric' = 'ip')::integer AS use_ip
\gset

\if :use_l2
INSERT INTO approximate_results (query_id, item_id)
SELECT query_vector.id, neighbor.id
FROM vector_bench.queries AS query_vector
CROSS JOIN LATERAL
(
    SELECT item.id
    FROM vector_bench.items AS item
    WHERE item.category_id < CAST(:'filter_limit' AS integer)
    ORDER BY item.embedding <-> query_vector.embedding
    LIMIT :top_k
) AS neighbor
WHERE query_vector.id <= :recall_query_count;
\elif :use_ip
INSERT INTO approximate_results (query_id, item_id)
SELECT query_vector.id, neighbor.id
FROM vector_bench.queries AS query_vector
CROSS JOIN LATERAL
(
    SELECT item.id
    FROM vector_bench.items AS item
    WHERE item.category_id < CAST(:'filter_limit' AS integer)
    ORDER BY item.embedding <#> query_vector.embedding
    LIMIT :top_k
) AS neighbor
WHERE query_vector.id <= :recall_query_count;
\else
INSERT INTO approximate_results (query_id, item_id)
SELECT query_vector.id, neighbor.id
FROM vector_bench.queries AS query_vector
CROSS JOIN LATERAL
(
    SELECT item.id
    FROM vector_bench.items AS item
    WHERE item.category_id < CAST(:'filter_limit' AS integer)
    ORDER BY item.embedding <=> query_vector.embedding
    LIMIT :top_k
) AS neighbor
WHERE query_vector.id <= :recall_query_count;
\endif

-- Aggregate filtered Recall@K statistics.
WITH per_query AS
(
    SELECT
        ground_truth.query_id,
        count(approximate.item_id)::integer AS matched_items,
        count(*)::integer AS expected_items,
        count(approximate.item_id)::numeric / count(*)::numeric AS recall
    FROM vector_bench.filtered_truth AS ground_truth
    LEFT JOIN approximate_results AS approximate
        ON approximate.query_id = ground_truth.query_id
        AND approximate.item_id = ground_truth.item_id
    WHERE ground_truth.filter_name = :'filter_name'
        AND ground_truth.metric = :'metric'
        AND ground_truth.query_id <= :recall_query_count
        AND ground_truth.rank <= :top_k
    GROUP BY ground_truth.query_id
),
approximate_summary AS
(
    SELECT count(*)::bigint AS returned_items
    FROM approximate_results
),
recall_summary AS
(
    SELECT
        count(*)::integer AS query_count,
        coalesce(sum(per_query.matched_items), 0)::bigint AS matched_items,
        coalesce(sum(per_query.expected_items), 0)::bigint AS expected_items,
        coalesce(avg(per_query.recall), 0)::numeric AS recall_at_k,
        coalesce(min(per_query.recall), 0)::numeric AS min_query_recall,
        coalesce(max(per_query.recall), 0)::numeric AS max_query_recall
    FROM per_query
)
SELECT
    :'filter_name' AS filter_name,
    :'metric' AS metric,
    :index_lists::integer AS lists,
    :'filter_limit'::integer AS filter_limit,
    :'probes'::integer AS probes,
    :'iterative_scan' AS iterative_scan,
    :'max_probes'::integer AS max_probes,
    recall_summary.query_count,
    :'top_k'::integer AS top_k,
    approximate_summary.returned_items,
    recall_summary.matched_items,
    recall_summary.expected_items,
    round(recall_summary.recall_at_k, 6) AS recall_at_k,
    round(recall_summary.min_query_recall, 6) AS min_query_recall,
    round(recall_summary.max_query_recall, 6) AS max_query_recall
FROM recall_summary
CROSS JOIN approximate_summary;

COMMIT;
