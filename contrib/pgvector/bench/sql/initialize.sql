-- Initialize the vector benchmark environment.

-- Drop the previous benchmark schema.
-- Only vector_bench is removed; other database schemas are not affected.
DROP SCHEMA IF EXISTS vector_bench CASCADE;

CREATE SCHEMA vector_bench;

--Install the pgvector extension.

CREATE EXTENSION IF NOT EXISTS vector;

--Install the pgvector extension.

CREATE EXTENSION IF NOT EXISTS pg_prewarm;


-- Create the table containing searchable item vectors.

-- :vector_type, :dimensions, and :category_count are supplied by
-- prepare_dataset.sh through psql -v.
-- Example:
--      :vector_type(:dimensions)
--      halfvec(128)
CREATE TABLE vector_bench.items
(
    id bigint PRIMARY KEY,

    embedding :vector_type(:dimensions) NOT NULL,

    -- Derive a deterministic category without changing the vector generator output.
    category_id integer GENERATED ALWAYS AS
            (((id - 1) % :category_count)::integer) STORED,

    CONSTRAINT items_embedding_dimensions_check
            CHECK (vector_dims(embedding) = :dimensions),

    CONSTRAINT items_id_positive_check
            CHECK (id > 0),

    CONSTRAINT items_category_range_check
            CHECK (category_id >= 0 AND category_id < :category_count)
);


-- Create the table containing query vectors.
CREATE TABLE vector_bench.queries
(
    id bigint PRIMARY KEY,

    embedding :vector_type(:dimensions) NOT NULL,

    CONSTRAINT queries_embedding_dimensions_check
            CHECK (vector_dims(embedding) = :dimensions),

    CONSTRAINT queries_id_positive_check
            CHECK (id > 0)
);


-- Create the ground-truth table for exact nearest-neighbor search.

-- Each row represents one exact result for one query and one metric.
CREATE TABLE vector_bench.truth
(
    -- Three distance metrics are currently supported.
    metric text NOT NULL,
    query_id bigint NOT NULL,
    item_id bigint NOT NULL,
    rank integer NOT NULL,

    CONSTRAINT truth_primary_key
            PRIMARY KEY (metric, query_id, rank),

    CONSTRAINT truth_mertic_key
            CHECK (metric IN ('l2', 'ip', 'cosine')),

    CONSTRAINT truth_query_id_positive_check
            CHECK (query_id > 0),

    CONSTRAINT truth_item_id_positive_check
            CHECK (item_id > 0),
    
    CONSTRAINT truth_rank_positive_check
            CHECK (rank > 0),

    CONSTRAINT truth_item_unique_per_query
            UNIQUE (metric, query_id, item_id),

    CONSTRAINT truth_query_foreign_key
            FOREIGN KEY (query_id)
            REFERENCES vector_bench.queries (id),

    CONSTRAINT truth_item_foreign_key
            FOREIGN KEY (item_id)
            REFERENCES vector_bench.items (id)
);

-- Add an auxiliary index for ground-truth lookups.
CREATE INDEX truth_lookup_idx
    ON vector_bench.truth (metric, query_id, item_id);

-- Create the ground-truth table for filtered nearest-neighbor searches.
CREATE TABLE vector_bench.filtered_truth
(
    filter_name text NOT NULL,
    metric text NOT NULL,
    query_id bigint NOT NULL,
    item_id bigint NOT NULL,
    rank integer NOT NULL,

    CONSTRAINT filtered_truth_primary_key
            PRIMARY KEY (filter_name, metric, query_id, rank),

    CONSTRAINT filtered_truth_filter_name_check
            CHECK (filter_name IN ('percent_1', 'percent_0_1')),

    CONSTRAINT filtered_truth_metric_check
            CHECK (metric IN ('l2', 'ip', 'cosine')),

    CONSTRAINT filtered_truth_query_id_positive_check
            CHECK (query_id > 0),

    CONSTRAINT filtered_truth_item_id_positive_check
            CHECK (item_id > 0),

    CONSTRAINT filtered_truth_rank_positive_check
            CHECK (rank > 0),

    CONSTRAINT filtered_truth_item_unique_per_query
            UNIQUE (filter_name, metric, query_id, item_id),

    CONSTRAINT filtered_truth_query_foreign_key
            FOREIGN KEY (query_id)
            REFERENCES vector_bench.queries (id),

    CONSTRAINT filtered_truth_item_foreign_key
            FOREIGN KEY (item_id)
            REFERENCES vector_bench.items (id)
);

-- Add an auxiliary index for filtered ground-truth lookups.
CREATE INDEX filtered_truth_lookup_idx
    ON vector_bench.filtered_truth
    (filter_name, metric, query_id, item_id);
