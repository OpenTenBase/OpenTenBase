-- Generate exact filtered nearest-neighbor results for Recall@K.

-- Example:
--
-- psql -X -v ON_ERROR_STOP=1 \
--     -v recall_query_count=100 \
--     -v top_k=10 \
--     -v category_count=1000 \
--     -f sql/exact_search_filtered.sql

\set ON_ERROR_STOP on

-- Check whether all required psql variables are defined.
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

\if :{?category_count}
\else
\echo 'missing required psql variable: category_count'
\quit 1
\endif

-- Validate positive integer inputs before using them in SQL expressions.
SELECT
    CASE
        WHEN :'recall_query_count' ~ '^[1-9][0-9]*$' THEN 1
        ELSE 0
    END AS valid_recall_query_count,
    CASE
        WHEN :'top_k' ~ '^[1-9][0-9]*$' THEN 1
        ELSE 0
    END AS valid_top_k,
    CASE
        WHEN :'category_count' ~ '^[1-9][0-9]*$' THEN 1
        ELSE 0
    END AS valid_category_count
\gset

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

\if :valid_category_count
\else
DO $$
BEGIN
    RAISE EXCEPTION 'category_count must be a positive integer';
END
$$;
\endif

-- Require category counts that represent 1% and 0.1% exactly.
SELECT
    (
        CAST(:'category_count' AS integer) >= 1000
        AND CAST(:'category_count' AS integer) % 1000 = 0
    )::integer AS supported_category_count
\gset

\if :supported_category_count
\else
DO $$
BEGIN
    RAISE EXCEPTION 'category_count must be at least 1000 and divisible by 1000';
END
$$;
\endif

-- Verify that the requested query count and category layout exist.
SELECT
    (
        (SELECT count(*) FROM vector_bench.queries)
            >= CAST(:'recall_query_count' AS bigint)
    )::integer AS enough_queries,
    (
        SELECT
            count(DISTINCT category_id) = CAST(:'category_count' AS bigint)
            AND min(category_id) = 0
            AND max(category_id) = CAST(:'category_count' AS integer) - 1
        FROM vector_bench.items
    )::integer AS matching_categories
\gset

\if :enough_queries
\else
DO $$
BEGIN
    RAISE EXCEPTION 'recall_query_count exceeds the available query rows';
END
$$;
\endif

\if :matching_categories
\else
DO $$
BEGIN
    RAISE EXCEPTION 'category_count does not match vector_bench.items';
END
$$;
\endif

-- Use a transaction so all filtered truth rows are generated atomically.
BEGIN;

TRUNCATE TABLE vector_bench.filtered_truth;

-- Define deterministic 1% and 0.1% filter profiles.
CREATE TEMPORARY TABLE filter_profiles
(
    filter_name text PRIMARY KEY,
    category_limit integer NOT NULL
)
ON COMMIT DROP;

INSERT INTO filter_profiles (filter_name, category_limit)
VALUES
    ('percent_1', CAST(:'category_count' AS integer) / 100),
    ('percent_0_1', CAST(:'category_count' AS integer) / 1000);

-- Materialize filtered items without a vector index.
--
-- This guarantees that the later Top-K ordering is exact even if an IVFFlat
-- index already exists on vector_bench.items.
CREATE TEMPORARY TABLE filtered_items
ON COMMIT DROP
AS
SELECT
    filter_profile.filter_name,
    item.id,
    item.embedding
FROM vector_bench.items AS item
JOIN filter_profiles AS filter_profile
    ON item.category_id < filter_profile.category_limit;

CREATE INDEX filtered_items_filter_name_idx
    ON filtered_items (filter_name);

ANALYZE filtered_items;

-- Ensure every filter profile contains enough rows for a complete Top-K.
SELECT
    (min(filtered_count) >= CAST(:'top_k' AS bigint))::integer
        AS filters_have_enough_rows
FROM
(
    SELECT
        filter_profile.filter_name,
        count(filtered_item.id) AS filtered_count
    FROM filter_profiles AS filter_profile
    LEFT JOIN filtered_items AS filtered_item
        ON filtered_item.filter_name = filter_profile.filter_name
    GROUP BY filter_profile.filter_name
) AS filter_counts
\gset

\if :filters_have_enough_rows
\else
DO $$
BEGIN
    RAISE EXCEPTION 'a filtered profile contains fewer rows than top_k';
END
$$;
\endif

-- Define the three supported distance metrics.
CREATE TEMPORARY TABLE metric_profiles
(
    metric text PRIMARY KEY
)
ON COMMIT DROP;

INSERT INTO metric_profiles (metric)
VALUES ('l2'), ('ip'), ('cosine');

-- Generate exact Top-K rows for every filter and distance metric.
INSERT INTO vector_bench.filtered_truth
    (filter_name, metric, query_id, item_id, rank)
SELECT
    filter_name,
    metric,
    query_id,
    item_id,
    rank
FROM
(
    SELECT
        filter_profile.filter_name,
        metric_profile.metric,
        query_vector.id AS query_id,
        neighbor.id AS item_id,
        row_number() OVER
        (
            PARTITION BY
                filter_profile.filter_name,
                metric_profile.metric,
                query_vector.id
            ORDER BY neighbor.distance, neighbor.id
        )::integer AS rank
    FROM vector_bench.queries AS query_vector
    CROSS JOIN filter_profiles AS filter_profile
    CROSS JOIN metric_profiles AS metric_profile
    CROSS JOIN LATERAL
    (
        SELECT
            filtered_item.id,
            CASE metric_profile.metric
                WHEN 'l2' THEN
                    filtered_item.embedding <-> query_vector.embedding
                WHEN 'ip' THEN
                    filtered_item.embedding <#> query_vector.embedding
                ELSE
                    filtered_item.embedding <=> query_vector.embedding
            END AS distance
        FROM filtered_items AS filtered_item
        WHERE filtered_item.filter_name = filter_profile.filter_name
        ORDER BY
            CASE metric_profile.metric
                WHEN 'l2' THEN
                    filtered_item.embedding <-> query_vector.embedding
                WHEN 'ip' THEN
                    filtered_item.embedding <#> query_vector.embedding
                ELSE
                    filtered_item.embedding <=> query_vector.embedding
            END,
            filtered_item.id
        LIMIT :top_k
    ) AS neighbor
    WHERE query_vector.id <= :recall_query_count
) AS ranked_results
WHERE rank <= :top_k;

COMMIT;

-- Print row counts for manual verification.
SELECT
    filter_name,
    metric,
    count(*) AS result_count,
    count(DISTINCT query_id) AS query_count,
    min(rank) AS min_rank,
    max(rank) AS max_rank
FROM vector_bench.filtered_truth
GROUP BY filter_name, metric
ORDER BY filter_name, metric;
