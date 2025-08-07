--
-- VACUUM
--

CREATE TABLE vactst (i INT) DISTRIBUTE BY REPLICATION;
INSERT INTO vactst VALUES (1);
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
SELECT * FROM vactst;
VACUUM FULL vactst;
UPDATE vactst SET i = i + 1;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
VACUUM (FULL) vactst;
DELETE FROM vactst;
SELECT * FROM vactst;

VACUUM (FULL, FREEZE) vactst;
VACUUM (ANALYZE, FULL) vactst;

CREATE TABLE vaccluster (i INT PRIMARY KEY);
ALTER TABLE vaccluster CLUSTER ON vaccluster_pkey;
CLUSTER vaccluster;

CREATE FUNCTION do_analyze() RETURNS VOID VOLATILE LANGUAGE SQL
	AS 'ANALYZE pg_am';
CREATE FUNCTION wrap_do_analyze(c INT) RETURNS INT IMMUTABLE LANGUAGE SQL
	AS 'SELECT $1 FROM do_analyze()';
CREATE INDEX ON vaccluster(wrap_do_analyze(i));
INSERT INTO vaccluster VALUES (1), (2);
ANALYZE vaccluster;

VACUUM FULL pg_am;
VACUUM FULL pg_class;
VACUUM FULL pg_catalog.pg_database;
VACUUM FULL vaccluster;
VACUUM FULL vactst;

VACUUM (DISABLE_PAGE_SKIPPING) vaccluster;

DROP TABLE vaccluster;
DROP TABLE vactst;

-- partitioned table
CREATE TABLE vacparted (a int, b char) PARTITION BY LIST (a);
CREATE TABLE vacparted1 PARTITION OF vacparted FOR VALUES IN (1);
INSERT INTO vacparted VALUES (1, 'a');
UPDATE vacparted SET b = 'b';
VACUUM (ANALYZE) vacparted;
VACUUM (FULL) vacparted;
VACUUM (FREEZE) vacparted;
DROP TABLE vacparted;

-- schema and table name with special character 
CREATE SCHEMA "S-test";
CREATE TABLE "eg_sync-cc0vf52i" (id int);
CREATE TABLE "2ac" (id int);
CREATE TABLE "ASD2" (id int);
CREATE TABLE "S-test"."2ac" (id int);
ANALYZE "eg_sync-cc0vf52i";
ANALYZE public."eg_sync-cc0vf52i";
ANALYZE "public"."eg_sync-cc0vf52i";
ANALYZE "2ac";
ANALYZE "ASD2";
ANALYZE "S-test"."2ac";
DROP TABLE "eg_sync-cc0vf52i","2ac","ASD2","S-test"."2ac";
DROP SCHEMA "S-test";

\c regression_ora
-- online gathering for bulk loads
CREATE TABLE tab_is_20230628(sno INT, sname VARCHAR(10), ssex INT);
ALTER TABLE tab_is_20230628 SET (autovacuum_enabled = off);

-- no effect when default online_gathering_threshold is  -1 
TRUNCATE tab_is_20230628;
INSERT INTO tab_is_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 2000) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
ANALYZE tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';

ALTER TABLE tab_is_20230628 SET (autovacuum_enabled = off, online_gathering_threshold = 500);
TRUNCATE tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
ANALYZE tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';

-- insert select > online_gathering_threshold(500)
ALTER TABLE tab_is_20230628 SET (autovacuum_enabled = off, online_gathering_threshold = 500);
TRUNCATE tab_is_20230628;
INSERT INTO tab_is_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 2000) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
ANALYZE tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';

-- no effect on delete/update
UPDATE tab_is_20230628 SET ssex = 12;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
DELETE FROM tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';

-- insert select = online_gathering_threshold(500)
TRUNCATE tab_is_20230628;
INSERT INTO tab_is_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 500) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
ANALYZE tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';

-- insert select < online_gathering_threshold(500)
TRUNCATE tab_is_20230628;
INSERT INTO tab_is_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 499) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
ANALYZE tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';

-- online_gathering_threshold->100
TRUNCATE tab_is_20230628;
ALTER TABLE tab_is_20230628 SET (autovacuum_enabled = off, online_gathering_threshold = 100);
INSERT INTO tab_is_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 499) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
ANALYZE tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';

-- no effect on other plan(insert)
TRUNCATE tab_is_20230628;
ALTER TABLE tab_is_20230628 SET (autovacuum_enabled = off, online_gathering_threshold = 1);
INSERT INTO tab_is_20230628 VALUES (10, 'AAA');
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
INSERT INTO tab_is_20230628 VALUES (20, 'CCC'),(30, 'DDD'),(40, 'EEE');
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
INSERT INTO tab_is_20230628 VALUES (generate_series(1,10), 'DDD', 1);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
-- insert other plan: update
INSERT INTO tab_is_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 10) t1(i), generate_series(1, 10) t2(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
INSERT INTO tab_is_20230628 SELECT * FROM tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';
ANALYZE tab_is_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_20230628';

-- copy
CREATE TABLE tab_copy_20230628(sno INT, sname VARCHAR(10), ssex INT);
ALTER TABLE tab_copy_20230628 SET (autovacuum_enabled = off);

COPY tab_copy_20230628 from stdin;
1	jack	20
2	jack	30
3	jack	40
\.
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_COPY_20230628';
ALTER TABLE tab_copy_20230628 SET (autovacuum_enabled = off, online_gathering_threshold = 5);
COPY tab_copy_20230628 from stdin;
1	jack	20
2	jack	30
3	jack	40
\.
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_COPY_20230628';
COPY tab_copy_20230628 from stdin;
1	jack	20
2	jack	30
3	jack	40
4	jack	50
5	jack	60
\.
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_COPY_20230628';
ANALYZE tab_copy_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_COPY_20230628';


-- temp table
CREATE TEMP TABLE tab_is_tmp_20230628(sno INT, sname VARCHAR(10), ssex INT);
ALTER TABLE tab_is_tmp_20230628 SET (autovacuum_enabled = off);

TRUNCATE tab_is_tmp_20230628;
INSERT INTO tab_is_tmp_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 2000) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_TMP_20230628';

ALTER TABLE tab_is_tmp_20230628 SET (autovacuum_enabled = off, online_gathering_threshold = 500);

INSERT INTO tab_is_tmp_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 2000) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_TMP_20230628';

INSERT INTO tab_is_tmp_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 499) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_TMP_20230628';
ANALYZE tab_is_tmp_20230628;
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_IS_TMP_20230628';

-- partition table(analyze only affect child table, currently not supported)
CREATE TABLE tab_part_20230628 (city_id INT NOT NULL, logdate DATE NOT NULL) PARTITION BY RANGE (logdate);
CREATE TABLE tab_part_20230628_y2006m01 PARTITION OF tab_part_20230628 FOR VALUES FROM ('2006-01-01') TO ('2006-02-01');
CREATE TABLE tab_part_20230628_y2006m02 PARTITION OF tab_part_20230628 FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');
ALTER TABLE tab_part_20230628_y2006m01 SET (autovacuum_enabled = off);
ALTER TABLE tab_part_20230628_y2006m02 SET (autovacuum_enabled = off);
-- no effect
TRUNCATE tab_part_20230628;
INSERT INTO tab_part_20230628 SELECT t1.i, '2006-01-11' FROM generate_series(1, 500) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_PART_20230628';
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_PART_20230628_Y2006M01';
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_PART_20230628_Y2006M02';
-- no effect no mater online_gathering_threshold
ALTER TABLE tab_part_20230628_y2006m01 SET (autovacuum_enabled = off, online_gathering_threshold = 500);
TRUNCATE tab_part_20230628;
INSERT INTO tab_part_20230628 SELECT t1.i, '2006-01-11' FROM generate_series(1, 500) t1(i);
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_PART_20230628';
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_PART_20230628_Y2006M01';
SELECT relpages, reltuples FROM pg_class WHERE relname = 'TAB_PART_20230628_Y2006M02';
-- global temp table preserve rows
CREATE GLOBAL TEMP TABLE tab_gtt_preserve_20230628(sno INT, sname VARCHAR(10), ssex INT) WITH (autovacuum_enabled = off, online_gathering_threshold = 500) ON COMMIT PRESERVE ROWS;
CREATE TABLE tab_gtt_preserve_compare_20230628(sno INT, sname VARCHAR(10), ssex INT) WITH (autovacuum_enabled = off, online_gathering_threshold = 500);
INSERT INTO tab_gtt_preserve_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 499) t1(i);
INSERT INTO tab_gtt_preserve_compare_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 499) t1(i);
SELECT relname, relpages, reltuples FROM pg_class WHERE relname in ('TAB_GTT_PRESERVE_20230628', 'TAB_GTT_PRESERVE_COMPARE_20230628') order by relname;
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;
EXPLAIN SELECT * FROM tab_gtt_preserve_compare_20230628;
-- rows=830 is exact
ANALYZE tab_gtt_preserve_20230628;
ANALYZE tab_gtt_preserve_compare_20230628;
SELECT relname, relpages, reltuples FROM pg_class WHERE relname in ('TAB_GTT_PRESERVE_20230628', 'TAB_GTT_PRESERVE_COMPARE_20230628') order by 1,2,3;
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;
EXPLAIN SELECT * FROM tab_gtt_preserve_compare_20230628;

TRUNCATE tab_gtt_preserve_20230628;
TRUNCATE tab_gtt_preserve_compare_20230628;
SELECT relname, relpages, reltuples FROM pg_class WHERE relname in ('TAB_GTT_PRESERVE_20230628', 'TAB_GTT_PRESERVE_COMPARE_20230628') order by relname;
-- rows=830 same as above
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;

-- preserve in transaction blocks
BEGIN;
-- online gathering
INSERT INTO tab_gtt_preserve_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 500) t1(i);
SELECT COUNT(*) FROM tab_gtt_preserve_20230628;
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;
-- no online gathering
INSERT INTO tab_gtt_preserve_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 400) t1(i);
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;
-- online gathering
INSERT INTO tab_gtt_preserve_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 600) t1(i);
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;
-- get exact rows
ANALYZE tab_gtt_preserve_20230628;
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;
ABORT;
SELECT COUNT(*) FROM tab_gtt_preserve_20230628;
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;
-- online gathering
INSERT INTO tab_gtt_preserve_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 600) t1(i);
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;
ANALYZE tab_gtt_preserve_20230628;
EXPLAIN SELECT * FROM tab_gtt_preserve_20230628;

-- global temp table delete rows
CREATE GLOBAL TEMP TABLE tab_gtt_delete_20230628(sno INT, sname VARCHAR(10), ssex INT) WITH (autovacuum_enabled = off, online_gathering_threshold = 500) ON COMMIT DELETE ROWS;
-- online gathering but clear at xact commit
INSERT INTO tab_gtt_delete_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 1000) t1(i);
SELECT COUNT(*) FROM tab_gtt_delete_20230628;
EXPLAIN SELECT * FROM tab_gtt_delete_20230628;
ANALYZE tab_gtt_delete_20230628;
EXPLAIN SELECT * FROM tab_gtt_delete_20230628;

TRUNCATE tab_gtt_delete_20230628;
BEGIN;
INSERT INTO tab_gtt_delete_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 1000) t1(i);
SELECT COUNT(*) FROM tab_gtt_delete_20230628;
EXPLAIN SELECT * FROM tab_gtt_delete_20230628;
ANALYZE tab_gtt_delete_20230628;
EXPLAIN SELECT * FROM tab_gtt_delete_20230628;
INSERT INTO tab_gtt_delete_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 100) t1(i);
EXPLAIN SELECT * FROM tab_gtt_delete_20230628;
INSERT INTO tab_gtt_delete_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 500) t1(i);
EXPLAIN SELECT * FROM tab_gtt_delete_20230628;
ANALYZE tab_gtt_delete_20230628;
EXPLAIN SELECT * FROM tab_gtt_delete_20230628;
COMMIT;
-- clear at xact commit
EXPLAIN SELECT * FROM tab_gtt_delete_20230628;

-- column stats
CREATE GLOBAL TEMP TABLE tab_gtt_delete_cs_20230628(sno INT, sname VARCHAR(10), ssex INT) WITH (autovacuum_enabled = off) ON COMMIT DELETE ROWS;
CREATE TABLE tab_20230628(sno INT, sname VARCHAR(10), ssex INT) WITH (autovacuum_enabled = off);
BEGIN;
INSERT INTO tab_gtt_delete_cs_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 1000) t1(i);
INSERT INTO tab_gtt_delete_cs_20230628 SELECT t1.i, 'E', t1.i % 10 FROM generate_series(1001, 1001) t1(i);
INSERT INTO tab_20230628 SELECT t1.i, 'Q', t1.i % 10 FROM generate_series(1, 1000) t1(i);
INSERT INTO tab_20230628 SELECT t1.i, 'E', t1.i % 10 FROM generate_series(1001, 1001) t1(i);
EXPLAIN SELECT * FROM tab_gtt_delete_cs_20230628 WHERE sname = 'E';
EXPLAIN SELECT * FROM tab_20230628 WHERE sname = 'E';
ANALYZE tab_gtt_delete_cs_20230628;
ANALYZE tab_20230628;
EXPLAIN SELECT * FROM tab_gtt_delete_cs_20230628 WHERE sname = 'E';
EXPLAIN SELECT * FROM tab_20230628 WHERE sname = 'E';
END;

-- online gathering in transaction block
DROP TABLE tab_is_20230628;
DROP TABLE tab_copy_20230628;
DROP TABLE tab_is_tmp_20230628;
DROP TABLE tab_part_20230628;
DROP TABLE tab_gtt_preserve_20230628;
DROP TABLE tab_gtt_preserve_compare_20230628;
DROP TABLE tab_gtt_delete_20230628;
DROP TABLE tab_gtt_delete_cs_20230628;
DROP TABLE tab_20230628;
