-- Regression tests for prepareable statements. We query the content
-- of the pg_prepared_statements view as prepared statements are
-- created and removed.

SELECT name, statement, parameter_types FROM pg_prepared_statements;

PREPARE q1 AS SELECT 1 AS a;
EXECUTE q1;

SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- should fail
PREPARE q1 AS SELECT 2;

-- should succeed
DEALLOCATE q1;
PREPARE q1 AS SELECT 2;
EXECUTE q1;

PREPARE q2 AS SELECT 2 AS b;
SELECT name, statement, parameter_types FROM pg_prepared_statements ORDER BY name;

-- sql92 syntax
DEALLOCATE PREPARE q1;

SELECT name, statement, parameter_types FROM pg_prepared_statements;

DEALLOCATE PREPARE q2;
-- the view should return the empty set again
SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- parameterized queries
PREPARE q2(text) AS
	SELECT datname, datistemplate, datallowconn
	FROM pg_catalog.pg_database WHERE datname = $1;

EXECUTE q2('postgres');

PREPARE q3(text, int, float, boolean, oid, smallint) AS
	SELECT * FROM tenk1 WHERE string4 = $1 AND (four = $2 OR
	ten = $3::bigint OR true = $4 OR oid = $5 OR odd = $6::int)
	ORDER BY unique1;

EXECUTE q3('AAAAxx', 5::smallint, 10.5::float, false, 500::oid, 4::bigint);

-- too few params
EXECUTE q3('bool');

-- too many params
EXECUTE q3('bytea', 5::smallint, 10.5::float, false, 500::oid, 4::bigint, true);

-- wrong param types
EXECUTE q3(5::smallint, 10.5::float, false, 500::oid, 4::bigint, 'bytea');

-- invalid type
PREPARE q4(nonexistenttype) AS SELECT $1;

-- create table as execute
PREPARE q5(int, text) AS
	SELECT * FROM tenk1 WHERE unique1 = $1 OR stringu1 = $2
	ORDER BY unique1;
CREATE TEMPORARY TABLE q5_prep_results AS EXECUTE q5(200, 'DTAAAA');
SELECT * FROM q5_prep_results;

-- unknown or unspecified parameter types: should succeed
PREPARE q6 AS
    SELECT * FROM tenk1 WHERE unique1 = $1 AND stringu1 = $2;
PREPARE q7(unknown) AS
    SELECT * FROM road WHERE thepath = $1;

SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

-- test DEALLOCATE ALL;
DEALLOCATE ALL;
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

--
-- search_path test
--
CREATE DATABASE search_path_db;
\c search_path_db

CREATE TABLE tbl_test(
    id int primary key,
    name  varchar(30)
);

INSERT INTO tbl_test VALUES (1, 'public 01');
INSERT INTO tbl_test VALUES (2, 'public 02');
INSERT INTO tbl_test VALUES (3, 'public 03');

select * from tbl_test order by id;

-- create schema
CREATE SCHEMA sch01;
CREATE SCHEMA sch02;

-- set schema to sch01
SET search_path TO sch01;

CREATE TABLE IF NOT EXISTS tbl_test(
    id int primary key,
    name  varchar(30)
);

BEGIN;
INSERT INTO tbl_test VALUES (11, 'sch01 11');
INSERT INTO tbl_test VALUES (12, 'sch01 12');
INSERT INTO tbl_test VALUES (13, 'sch01 13');
COMMIT;

select * from tbl_test order by id;

-- set schema to sch02
SET search_path TO sch02;

CREATE TABLE IF NOT EXISTS tbl_test(
    id int primary key,
    name  varchar(30)
);

BEGIN;
INSERT INTO tbl_test VALUES (21, 'sch02 21');
INSERT INTO tbl_test VALUES (22, 'sch02 22');
INSERT INTO tbl_test VALUES (23, 'sch02 23');
ROLLBACK;

select * from tbl_test order by id;

-- set schema to sch01
SET search_path = sch01;
SHOW search_path;

PREPARE ps_test_insert (int, varchar) AS INSERT INTO tbl_test VALUES ($1, $2);;
PREPARE ps_test_select (int) AS select * from tbl_test where id < $1 order by id;

BEGIN;
EXECUTE ps_test_insert(14, 'sch01 14');
EXECUTE ps_test_select(50);
ROLLBACK;
EXECUTE ps_test_select(50);

SHOW search_path;

BEGIN;
EXECUTE ps_test_insert(15, 'sch01 15');
EXECUTE ps_test_select(50);
COMMIT;
EXECUTE ps_test_select(50);

SHOW search_path;

EXECUTE ps_test_insert(16, 'sch01 16');
EXECUTE ps_test_select(50);

SHOW search_path;

DEALLOCATE PREPARE ps_test_insert;
DEALLOCATE PREPARE ps_test_select;

-- test insert fqs in prepare
CREATE TABLE insert_fsq_test(id serial primary key, name  varchar(30));
PREPARE ps_test_insert (varchar) AS INSERT INTO insert_fsq_test (name) VALUES ($1);
EXECUTE ps_test_insert('1');
EXECUTE ps_test_insert('2');
EXECUTE ps_test_insert('3');
EXECUTE ps_test_insert('4');
EXECUTE ps_test_insert('5');
SELECT * from insert_fsq_test order by id;
DEALLOCATE PREPARE ps_test_insert;
DROP TABLE insert_fsq_test cascade;
-- test non-distribute key with function, no need rewrite
CREATE TABLE insert_fsq_test1(v int, w int);
CREATE SEQUENCE test_seq;
PREPARE ps_test_insert1(int) AS INSERT INTO insert_fsq_test1 values($1, nextval('test_seq'));
EXECUTE ps_test_insert1(1);
EXECUTE ps_test_insert1(2);
EXECUTE ps_test_insert1(3);
EXECUTE ps_test_insert1(4);
EXECUTE ps_test_insert1(5);
SELECT * from insert_fsq_test1 order by v;
DEALLOCATE PREPARE ps_test_insert1;
DROP TABLE insert_fsq_test1 cascade;

--
-- gb18030 test
--
CREATE DATABASE gb18030_db template template0_ora encoding = gb18030 LC_COLLATE = 'zh_CN.gb18030' LC_CTYPE = 'zh_CN.gb18030';
\c gb18030_db;

-- set client_encoding
SET client_encoding = utf8;

CREATE TABLE tbl_test(id int primary key, name varchar(3));

INSERT INTO tbl_test VALUES (3, '张三');
BEGIN;
INSERT INTO tbl_test VALUES (4, '李四');
INSERT INTO tbl_test VALUES (5, '王五');
COMMIT;
BEGIN;
INSERT INTO tbl_test VALUES (6, '丁六');
INSERT INTO tbl_test VALUES (7, '方七');
ROLLBACK;
SELECT * FROM tbl_test ORDER BY id;

SHOW client_encoding;

PREPARE ps_test (int) AS select * from tbl_test where id < $1 order by id;
EXECUTE ps_test(20);
SHOW client_encoding;
EXECUTE ps_test(20);
SHOW client_encoding;
EXECUTE ps_test(20);
SHOW client_encoding;
DEALLOCATE PREPARE ps_test;

drop table if exists INT8_TBL;
CREATE TABLE INT8_TBL(q1 int8, q2 int8);
INSERT INTO INT8_TBL VALUES('+4567890123456789','-4567890123456789');
CREATE TEMP TABLE pcachetest AS SELECT * FROM int8_tbl;
PREPARE prepstmt AS SELECT * FROM pcachetest;
EXECUTE prepstmt;
DROP TABLE pcachetest;
EXECUTE prepstmt;
CREATE TEMP TABLE pcachetest AS SELECT * FROM int8_tbl ORDER BY 2;
EXECUTE prepstmt;
DEALLOCATE PREPARE prepstmt;
DROP TABLE pcachetest;
DROP TABLE INT8_TBL;
-- bugfix(ID116410473): gts optimize cause error
DROP TABLE IF EXISTS gts_opt_test_tbl;
CREATE TABLE gts_opt_test_tbl(f1 int,f2 float,f3 interval,f4 int) distribute by shard(f1,f2,f3);

INSERT INTO gts_opt_test_tbl values (1,1.11,'1 day'::interval,0);
INSERT INTO gts_opt_test_tbl values (2,2.22,'2 days'::interval,0);
INSERT INTO gts_opt_test_tbl values (3,3.33,'3 days'::interval,0);

SELECT f1,f2,f4 FROM gts_opt_test_tbl ORDER BY 1,2,3;

PREPARE gts_opt_test_tbl_update(int, int, int) AS UPDATE gts_opt_test_tbl SET f4=f4+1 WHERE f1=$1 AND f2=($2 % 200)*1.11 AND f3=$3*'1 day'::interval;
EXECUTE gts_opt_test_tbl_update(1, 1, 1);

SELECT f1,f2,f4 FROM gts_opt_test_tbl ORDER BY 1,2,3;

DEALLOCATE PREPARE gts_opt_test_tbl_update;
DROP TABLE gts_opt_test_tbl;

drop table IF EXISTS cpt1;
drop table IF EXISTS cpt2;
create table cpt1(c0 int, c1 int, c2 int);
create table cpt2(c0 int, c1 int, c2 int);

insert into cpt2 select n, n * 10, n * 10 from generate_series(1,100) n;
begin;
prepare pretest(int,int,int) as select * from cpt2 where c1 = 20;
execute pretest (1,1,1);
rollback;

begin;
DEALLOCATE ALL;
prepare pretest(int,int,int) as select * from cpt1 where c1 = 20;
execute pretest (1,1,1);
select * from cpt1 where c1 = 20;
rollback;

DEALLOCATE ALL;
drop table IF EXISTS cpt1;
drop table IF EXISTS cpt2;
