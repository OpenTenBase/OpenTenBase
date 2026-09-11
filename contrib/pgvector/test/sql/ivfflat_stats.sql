SET client_min_messages = warning;

-- full list and index statistics
CREATE TABLE ivfstats_t (id int, val vector(3));
INSERT INTO ivfstats_t VALUES
    (1, '[0,0,0]'), (2, '[1,1,1]'), (3, '[2,2,2]'), (4, NULL);
CREATE INDEX ivfstats_idx ON ivfstats_t USING ivfflat (val vector_l2_ops)
    WITH (lists = 1);

SELECT format_version, dimensions, declared_lists, list_id,
       list_page >= 1 AS valid_list_page,
       list_offset >= 1 AS valid_list_offset,
       start_page IS NOT NULL AS has_start_page,
       insert_page IS NOT NULL AS has_insert_page,
       entry_pages >= 1 AS has_entry_page,
       physical_tuple_count,
       used_bytes + free_bytes = usable_bytes AS valid_space
FROM ivfflat_list_stats('ivfstats_idx');

SELECT format_version, dimensions, declared_lists,
       list_pages >= 1 AS has_list_page,
       entry_pages >= 1 AS has_entry_page,
       physical_indexed_tuples, empty_lists,
       used_bytes + free_bytes = usable_bytes AS valid_space,
       scanned_pages = 1 + list_pages + entry_pages AS valid_page_count,
       scan_elapsed_ms >= 0 AS valid_elapsed
FROM ivfflat_index_stats('ivfstats_idx');

-- physical tuple count is not an MVCC visibility count
DELETE FROM ivfstats_t WHERE id = 1;
SELECT physical_indexed_tuples FROM ivfflat_index_stats('ivfstats_idx');
VACUUM ivfstats_t;
SELECT physical_indexed_tuples FROM ivfflat_index_stats('ivfstats_idx');

-- empty lists are valid structure facts
CREATE TABLE ivfstats_empty (val vector(3));
CREATE INDEX ivfstats_empty_idx ON ivfstats_empty USING ivfflat (val vector_l2_ops)
    WITH (lists = 3);
SELECT count(*) AS lists, sum(physical_tuple_count) AS tuples,
       count(*) FILTER (WHERE physical_tuple_count = 0) AS empty_lists
FROM ivfflat_list_stats('ivfstats_empty_idx');
SELECT declared_lists, physical_indexed_tuples, empty_lists
FROM ivfflat_index_stats('ivfstats_empty_idx');

-- list sampling is deterministic and scans selected chains completely
CREATE TABLE ivfstats_sample (val vector(3));
INSERT INTO ivfstats_sample
SELECT ARRAY[g, g + 1, g + 2]::vector FROM generate_series(1, 100) g;
CREATE INDEX ivfstats_sample_idx ON ivfstats_sample USING ivfflat (val vector_l2_ops)
    WITH (lists = 10);
SELECT (SELECT array_agg(list_id ORDER BY list_id)
          FROM ivfflat_list_stats('ivfstats_sample_idx', 0.5)) =
       (SELECT array_agg(list_id ORDER BY list_id)
          FROM ivfflat_list_stats('ivfstats_sample_idx', 0.5)) AS deterministic;
SELECT count(*) BETWEEN 1 AND 9 AS sampled_subset
FROM ivfflat_list_stats('ivfstats_sample_idx', 0.5);

-- invalid inputs and relation types
SELECT * FROM ivfflat_list_stats('ivfstats_idx', 0);
SELECT * FROM ivfflat_list_stats('ivfstats_idx', 1.1);
SELECT * FROM ivfflat_list_stats('ivfstats_t');
CREATE INDEX ivfstats_btree_idx ON ivfstats_t (id);
SELECT * FROM ivfflat_index_stats('ivfstats_btree_idx');
SELECT count(*) FROM ivfflat_list_stats('ivfstats_idx', NULL);

-- invoker must have SELECT on the indexed table
CREATE ROLE ivfstats_reader;
CREATE TABLE ivfstats_acl (val vector(3));
INSERT INTO ivfstats_acl VALUES ('[1,2,3]');
CREATE INDEX ivfstats_acl_idx ON ivfstats_acl USING ivfflat (val vector_l2_ops);
SET ROLE ivfstats_reader;
SELECT count(*) FROM ivfflat_list_stats('ivfstats_acl_idx');
RESET ROLE;
GRANT SELECT ON ivfstats_acl TO ivfstats_reader;
SET ROLE ivfstats_reader;
SELECT count(*) FROM ivfflat_list_stats('ivfstats_acl_idx');
RESET ROLE;

DROP TABLE ivfstats_t, ivfstats_empty, ivfstats_sample, ivfstats_acl;
DROP ROLE ivfstats_reader;
RESET client_min_messages;
