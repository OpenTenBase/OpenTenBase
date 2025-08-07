CREATE TABLE test1 (a int, b text) DISTRIBUTE BY REPLICATION;
INSERT INTO test1 VALUES (1, 'one');
CREATE INDEX test1_a_idx ON test1 USING brin (a);

SELECT brin_page_type(get_raw_page('test1_a_idx', 0));
SELECT brin_page_type(get_raw_page('test1_a_idx', 1));
SELECT brin_page_type(get_raw_page('test1_a_idx', 2));

SELECT * FROM brin_metapage_info(get_raw_page('test1_a_idx', 0));
SELECT * FROM brin_metapage_info(get_raw_page('test1_a_idx', 1));

SELECT * FROM brin_revmap_data(get_raw_page('test1_a_idx', 0)) LIMIT 5;
SELECT * FROM brin_revmap_data(get_raw_page('test1_a_idx', 1)) LIMIT 5;

SELECT itemoffset, blknum, attnum, hasnulls, placeholder FROM brin_page_items(get_raw_page('test1_a_idx', 2), 'test1_a_idx')
    ORDER BY blknum, attnum LIMIT 5;

DROP TABLE test1;

create database contrib_ora_pageinspect sql mode opentenbase_ora;
\c contrib_ora_pageinspect

CREATE TABLE test1 (a int, b text) DISTRIBUTE BY REPLICATION;
INSERT INTO test1 VALUES (1, 'one');
CREATE INDEX test1_a_idx ON test1 USING brin (a);

SELECT brin_page_type(get_raw_page('TEST1_A_IDX', 0));
SELECT brin_page_type(get_raw_page('TEST1_A_IDX', 1));
SELECT brin_page_type(get_raw_page('TEST1_A_IDX', 2));

SELECT * FROM brin_metapage_info(get_raw_page('TEST1_A_IDX', 0));
SELECT * FROM brin_metapage_info(get_raw_page('TEST1_A_IDX', 1));

SELECT * FROM brin_revmap_data(get_raw_page('TEST1_A_IDX', 0)) LIMIT 5;
SELECT * FROM brin_revmap_data(get_raw_page('TEST1_A_IDX', 1)) LIMIT 5;

SELECT itemoffset, blknum, attnum, hasnulls, placeholder FROM brin_page_items(get_raw_page('TEST1_A_IDX', 2), 'TEST1_A_IDX')
ORDER BY blknum, attnum LIMIT 5;

DROP TABLE test1;
