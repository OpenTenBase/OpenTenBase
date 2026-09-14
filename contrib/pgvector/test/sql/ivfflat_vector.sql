SET enable_seqscan = off;

-- L2

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 1);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <-> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <-> (SELECT NULL::vector)) t2;
SELECT COUNT(*) FROM t;

TRUNCATE t;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

DROP TABLE t;

-- inner product

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING ivfflat (val vector_ip_ops) WITH (lists = 1);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <#> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <#> (SELECT NULL::vector)) t2;

DROP TABLE t;

-- cosine

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING ivfflat (val vector_cosine_ops) WITH (lists = 1);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <=> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> '[0,0,0]') t2;
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> (SELECT NULL::vector)) t2;
SELECT * FROM t CROSS JOIN LATERAL (SELECT * FROM t t2 ORDER BY val <=> t.val LIMIT 1) t2 WHERE t.val != '[0,0,0]' ORDER BY t.val;

DROP TABLE t;

-- iterative

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 3);

SET ivfflat.iterative_scan = relaxed_order;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

SET ivfflat.max_probes = 1;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

SET ivfflat.max_probes = 2;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

TRUNCATE t;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

RESET ivfflat.iterative_scan;
RESET ivfflat.max_probes;
DROP TABLE t;

-- unlogged

CREATE UNLOGGED TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 1);

SELECT * FROM t ORDER BY val <-> '[3,3,3]';

DROP TABLE t;

-- options

CREATE TABLE t (val vector(3));
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 0);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 32769);
DROP TABLE t;

SHOW ivfflat.probes;
SET ivfflat.probes = 0;
SET ivfflat.probes = 32769;

SHOW ivfflat.iterative_scan;
SET ivfflat.iterative_scan = on;

SHOW ivfflat.max_probes;
SET ivfflat.max_probes = 0;
SET ivfflat.max_probes = 32769;

-- dimensions

CREATE TABLE t (val vector(2000));
CREATE INDEX ON t USING ivfflat (val vector_l2_ops);
DROP TABLE t;

CREATE TABLE t (val vector(2001));
CREATE INDEX ON t USING ivfflat (val vector_l2_ops);
DROP TABLE t;

-- memory

SET maintenance_work_mem = '1MB';
CREATE TABLE t (val vector(2000));
CREATE INDEX ON t USING ivfflat (val vector_l2_ops);
DROP TABLE t;
RESET maintenance_work_mem;

SET maintenance_work_mem = '5MB';
CREATE TABLE t (val vector(2000));
INSERT INTO t (val) VALUES (array_fill(0, ARRAY[2000]));
CREATE INDEX ON t USING ivfflat (val vector_l2_ops);
DROP TABLE t;
RESET maintenance_work_mem;

-- diagnostic helper: ivfflat_index_info() and ivfflat_recommend_probes()

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX t_val_idx ON t USING ivfflat (val vector_l2_ops) WITH (lists = 4);

-- default probes (not yet set)
SELECT * FROM ivfflat_index_info('t_val_idx'::regclass);

-- custom probes
SET ivfflat.probes = 3;
SELECT * FROM ivfflat_index_info('t_val_idx'::regclass);
RESET ivfflat.probes;

-- recommender: default target_recall (0.9)
SELECT * FROM ivfflat_recommend_probes('t_val_idx'::regclass);

-- recommender: target_recall=0.99 -> 1.5x multiplier
SELECT * FROM ivfflat_recommend_probes('t_val_idx'::regclass, 0.99);

-- recommender: target_recall=0.999 -> 2.5x multiplier
SELECT * FROM ivfflat_recommend_probes('t_val_idx'::regclass, 0.999);

DROP TABLE t;

-- error: ivfflat_recommend_probes rejects non-ivfflat indexes
CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX t_val_idx ON t USING hnsw (val vector_l2_ops);
SELECT ivfflat_recommend_probes('t_val_idx'::regclass);
DROP TABLE t;
