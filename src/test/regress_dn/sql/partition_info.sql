--
-- Tests for pg_partition_tree
--
SELECT * FROM pg_partition_tree(NULL);
SELECT * FROM pg_partition_tree(0);
SELECT pg_partition_root(NULL);
SELECT pg_partition_root(0);
SELECT * FROM pg_partition_ancestors(NULL);
SELECT * FROM pg_partition_ancestors(0);

-- Test table partition trees
CREATE TABLE ptif_test (a int, b int) PARTITION BY range (a);
CREATE TABLE ptif_test0 PARTITION OF ptif_test
  FOR VALUES FROM (minvalue) TO (0) PARTITION BY list (b);
CREATE TABLE ptif_test01 PARTITION OF ptif_test0 FOR VALUES IN (1);
CREATE TABLE ptif_test1 PARTITION OF ptif_test
  FOR VALUES FROM (0) TO (100) PARTITION BY list (b);
CREATE TABLE ptif_test11 PARTITION OF ptif_test1 FOR VALUES IN (1);
CREATE TABLE ptif_test2 PARTITION OF ptif_test
  FOR VALUES FROM (100) TO (200);
-- This partitioned table should remain with no partitions.
CREATE TABLE ptif_test3 PARTITION OF ptif_test
  FOR VALUES FROM (200) TO (maxvalue) PARTITION BY list (b);  

-- Test pg_partition_root for tables
SELECT pg_partition_root('ptif_test');
SELECT pg_partition_root('ptif_test0');
SELECT pg_partition_root('ptif_test01');
SELECT pg_partition_root('ptif_test3');

-- Test index partition tree
CREATE INDEX ptif_test_index ON ONLY ptif_test (a);
CREATE INDEX ptif_test0_index ON ONLY ptif_test0 (a);
ALTER INDEX ptif_test_index ATTACH PARTITION ptif_test0_index;
CREATE INDEX ptif_test01_index ON ptif_test01 (a);
ALTER INDEX ptif_test0_index ATTACH PARTITION ptif_test01_index;
CREATE INDEX ptif_test1_index ON ONLY ptif_test1 (a);
ALTER INDEX ptif_test_index ATTACH PARTITION ptif_test1_index;
CREATE INDEX ptif_test11_index ON ptif_test11 (a);
ALTER INDEX ptif_test1_index ATTACH PARTITION ptif_test11_index;
CREATE INDEX ptif_test2_index ON ptif_test2 (a);
ALTER INDEX ptif_test_index ATTACH PARTITION ptif_test2_index;
CREATE INDEX ptif_test3_index ON ptif_test3 (a);
ALTER INDEX ptif_test_index ATTACH PARTITION ptif_test3_index;

-- Test pg_partition_root for indexes
SELECT pg_partition_root('ptif_test_index');
SELECT pg_partition_root('ptif_test0_index');
SELECT pg_partition_root('ptif_test3_index');

-- List all tables members of the tree
SELECT relid, parentrelid, "level", isleaf
  FROM pg_partition_tree('ptif_test');
-- List tables from an intermediate level
SELECT relid, parentrelid, "level", isleaf
  FROM pg_partition_tree('ptif_test0') p
  JOIN pg_class c ON (p.relid = c.oid);
-- List from leaf table
SELECT relid, parentrelid, "level", isleaf
  FROM pg_partition_tree('ptif_test01') p
  JOIN pg_class c ON (p.relid = c.oid);
-- List from partitioned table with no partitions
SELECT relid, parentrelid, "level", isleaf
  FROM pg_partition_tree('ptif_test3') p
  JOIN pg_class c ON (p.relid = c.oid);
-- List all members using pg_partition_root with leaf table reference
SELECT relid, parentrelid, "level", isleaf
  FROM pg_partition_tree(pg_partition_root('ptif_test01')) p
  JOIN pg_class c ON (p.relid = c.oid);
-- List all ancestors of root and leaf tables
SELECT * FROM pg_partition_ancestors('ptif_test01');
SELECT * FROM pg_partition_ancestors('ptif_test');

DROP TABLE ptif_test;

-- A table not part of a partition tree works is not listed.
CREATE TABLE ptif_normal_table(a int);
SELECT relid, parentrelid, "level", isleaf
  FROM pg_partition_tree('ptif_normal_table');
SELECT pg_partition_root('ptif_normal_table');
SELECT * FROM pg_partition_ancestors('ptif_normal_table');
DROP TABLE ptif_normal_table;

-- Views and materialized viewS cannot be part of a partition tree.
CREATE VIEW ptif_test_view AS SELECT 1;
CREATE MATERIALIZED VIEW ptif_test_matview AS SELECT 1;
SELECT * FROM pg_partition_tree('ptif_test_view');
SELECT * FROM pg_partition_tree('ptif_test_matview');
SELECT pg_partition_root('ptif_test_view');
SELECT pg_partition_root('ptif_test_matview');
SELECT * FROM pg_partition_ancestors('ptif_test_view');
SELECT * FROM pg_partition_ancestors('ptif_test_matview');
DROP VIEW ptif_test_view;
DROP MATERIALIZED VIEW ptif_test_matview;

-- partition table without partitioned with index, can select normally
CREATE TABLE p_t(stat_month VARCHAR(6), organ_flag CHAR(1)) PARTITION BY LIST (stat_month) with (oids = false);
SELECT * FROM p_t;
CREATE INDEX p_t_idx ON p_t USING btree(organ_flag);
SELECT * FROM p_t;
DROP TABLE p_t cascade;
