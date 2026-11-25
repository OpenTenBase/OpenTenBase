-- Diagnostic helper functions: ivfflat_index_info / hnsw_index_info, plus the
-- query-time parameter recommendation helpers.

CREATE TABLE diag_items (id int, v vector(3)) DISTRIBUTE BY REPLICATION;
INSERT INTO diag_items VALUES (1, '[1,2,3]'), (2, '[4,5,6]'), (3, '[7,8,9]'), (4, '[1,1,1]');

CREATE INDEX diag_ivfflat_idx ON diag_items USING ivfflat (v vector_l2_ops) WITH (lists = 4);
CREATE INDEX diag_hnsw_idx ON diag_items USING hnsw (v vector_l2_ops) WITH (m = 8, ef_construction = 32);

SET ivfflat.probes = 2;
SET hnsw.ef_search = 40;

SELECT * FROM ivfflat_index_info('diag_ivfflat_idx');
SELECT * FROM hnsw_index_info('diag_hnsw_idx');

-- an index created without WITH (...) falls back to the extension defaults
CREATE INDEX diag_ivfflat_default_idx ON diag_items USING ivfflat (v vector_l2_ops);
SELECT * FROM ivfflat_index_info('diag_ivfflat_default_idx');

-- recommendation helpers
SELECT * FROM ivfflat_recommend_probes('diag_ivfflat_idx');
SELECT * FROM ivfflat_recommend_probes('diag_ivfflat_idx', 0.95);
SELECT * FROM hnsw_recommend_ef_search('diag_hnsw_idx', 10, 0.95);

-- the wrong index type is rejected
SELECT * FROM ivfflat_index_info('diag_hnsw_idx');
SELECT * FROM hnsw_index_info('diag_ivfflat_idx');

DROP TABLE diag_items;