--
-- 测试分区函数
--
SELECT * FROM pg_partition_tree(NULL);
SELECT * FROM pg_partition_tree(0);
SELECT * FROM pg_partition_ancestors(NULL);
SELECT * FROM pg_partition_ancestors(0);
SELECT pg_partition_root(NULL);
SELECT pg_partition_root(0);

-- 测试分区树
CREATE TABLE ptif_test (a int, b int) PARTITION BY range (a);
CREATE TABLE ptif_test0 PARTITION OF ptif_test
  FOR VALUES FROM (minvalue) TO (0) PARTITION BY list (b);
CREATE TABLE ptif_test01 PARTITION OF ptif_test0 FOR VALUES IN (1);
CREATE TABLE ptif_test1 PARTITION OF ptif_test
  FOR VALUES FROM (0) TO (100) PARTITION BY list (b);
CREATE TABLE ptif_test11 PARTITION OF ptif_test1 FOR VALUES IN (1);
CREATE TABLE ptif_test2 PARTITION OF ptif_test
  FOR VALUES FROM (100) TO (200);
-- 此分区表应保留为没有分区。
CREATE TABLE ptif_test3 PARTITION OF ptif_test
  FOR VALUES FROM (200) TO (maxvalue) PARTITION BY list (b);

-- 测试OpenTenBase分区表
SELECT pg_partition_root('ptif_test');
SELECT pg_partition_root('ptif_test0');
SELECT pg_partition_root('ptif_test01');
SELECT pg_partition_root('ptif_test3');

-- 测试索引分区树
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

-- 测试pg_partition_root的索引
SELECT pg_partition_root('ptif_test_index');
SELECT pg_partition_root('ptif_test0_index');
SELECT pg_partition_root('ptif_test01_index');
SELECT pg_partition_root('ptif_test3_index');

-- 列出树的所有表成员
SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree('ptif_test');
-- 列出中间级别的表
SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree('ptif_test0') p
  JOIN pg_class c ON (p.relid = c.oid);

SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree('ptif_test01') p
  JOIN pg_class c ON (p.relid = c.oid);

SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree('ptif_test3') p
  JOIN pg_class c ON (p.relid = c.oid);

SELECT * FROM pg_partition_ancestors('ptif_test01');
SELECT * FROM pg_partition_ancestors('ptif_test');

SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree(pg_partition_root('ptif_test01')) p
  JOIN pg_class c ON (p.relid = c.oid);

SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree('ptif_test_index');

SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree('ptif_test0_index') p
  JOIN pg_class c ON (p.relid = c.oid);

SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree('ptif_test01_index') p
  JOIN pg_class c ON (p.relid = c.oid);

SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree('ptif_test3_index') p
  JOIN pg_class c ON (p.relid = c.oid);

SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree(pg_partition_root('ptif_test01_index')) p
  JOIN pg_class c ON (p.relid = c.oid);

SELECT * FROM pg_partition_ancestors('ptif_test01_index');
SELECT * FROM pg_partition_ancestors('ptif_test_index');

DROP TABLE ptif_test;

CREATE TABLE ptif_normal_table(a int);
SELECT relid, parentrelid, level, isleaf
  FROM pg_partition_tree('ptif_normal_table');
SELECT * FROM pg_partition_ancestors('ptif_normal_table');
SELECT pg_partition_root('ptif_normal_table');
DROP TABLE ptif_normal_table;

--如果传递了关系，则与分区相关的各种函数返回空/NULL
--不能成为分区树一部分的类型；例如视图，
--物化视图、遗产继承子级或父级等
CREATE VIEW ptif_test_view AS SELECT 1;
CREATE MATERIALIZED VIEW ptif_test_matview AS SELECT 1;
CREATE TABLE ptif_li_parent ();
CREATE TABLE ptif_li_child () INHERITS (ptif_li_parent);
SELECT * FROM pg_partition_tree('ptif_test_view');
SELECT * FROM pg_partition_tree('ptif_test_matview');
SELECT * FROM pg_partition_tree('ptif_li_parent');
SELECT * FROM pg_partition_tree('ptif_li_child');
SELECT * FROM pg_partition_ancestors('ptif_test_view');
SELECT * FROM pg_partition_ancestors('ptif_test_matview');
SELECT * FROM pg_partition_ancestors('ptif_li_parent');
SELECT * FROM pg_partition_ancestors('ptif_li_child');
SELECT pg_partition_root('ptif_test_view');
SELECT pg_partition_root('ptif_test_matview');
SELECT pg_partition_root('ptif_li_parent');
SELECT pg_partition_root('ptif_li_child');
DROP VIEW ptif_test_view;
DROP MATERIALIZED VIEW ptif_test_matview;
DROP TABLE ptif_li_parent, ptif_li_child;
