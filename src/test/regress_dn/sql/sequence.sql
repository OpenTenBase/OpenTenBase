--
-- CREATE SEQUENCE
--

CREATE DATABASE db_seq1;
CREATE DATABASE db_seq2;
\c db_seq1
CREATE SEQUENCE my_seq;
\c db_seq2
CREATE SEQUENCE my_seq;
\c regression

CREATE SEQUENCE seq_test_local;
SELECT seqlocal FROM pg_class c JOIN pg_sequence s ON (s.seqrelid = c.oid) WHERE c.relname = 'seq_test_local';
DROP SEQUENCE seq_test_local;

drop sequence if exists seq_1;
create sequence seq_1 INCREMENT BY 1 MINVALUE 1 START WITH 1;
create table test_serial
(
    id serial primary key,
    name varchar(100)
) distribute by shard(id);

create table test_identiy_1
(
    id int generated always as identity (cache 100 START WITH 1 INCREMENT BY 1)  primary key,
    name varchar(100)
);
\d+ seq_1
\d+ test_serial_id_seq
\d+ test_identiy_1_id_seq

drop sequence seq_1;
drop table test_serial;
drop table test_identiy_1;
-- various error cases
CREATE UNLOGGED SEQUENCE sequence_testx;
CREATE SEQUENCE sequence_testx INCREMENT BY 0;
CREATE SEQUENCE sequence_testx INCREMENT BY -1 MINVALUE 20;
CREATE SEQUENCE sequence_testx INCREMENT BY 1 MAXVALUE -20;
CREATE SEQUENCE sequence_testx INCREMENT BY -1 START 10;
CREATE SEQUENCE sequence_testx INCREMENT BY 1 START -10;
CREATE SEQUENCE sequence_testx CACHE 0;

-- OWNED BY errors
CREATE SEQUENCE sequence_testx OWNED BY nobody;  -- nonsense word
CREATE SEQUENCE sequence_testx OWNED BY pg_class_oid_index.oid;  -- not a table
CREATE SEQUENCE sequence_testx OWNED BY pg_class.relname;  -- not same schema
CREATE TABLE sequence_test_table (a int);
CREATE SEQUENCE sequence_testx OWNED BY sequence_test_table.b;  -- wrong column
DROP TABLE sequence_test_table;

-- sequence data types
CREATE SEQUENCE sequence_test5 AS integer;
CREATE SEQUENCE sequence_test6 AS smallint;
CREATE SEQUENCE sequence_test7 AS bigint;
CREATE SEQUENCE sequence_test8 AS integer MAXVALUE 100000;
CREATE SEQUENCE sequence_test9 AS integer INCREMENT BY -1;
CREATE SEQUENCE sequence_test10 AS integer MINVALUE -100000 START 1;
CREATE SEQUENCE sequence_test11 AS smallint;
CREATE SEQUENCE sequence_test12 AS smallint INCREMENT -1;
CREATE SEQUENCE sequence_test13 AS smallint MINVALUE -32768;
CREATE SEQUENCE sequence_test14 AS smallint MAXVALUE 32767 INCREMENT -1;
CREATE SEQUENCE sequence_testx AS text;
CREATE SEQUENCE sequence_testx AS nosuchtype;

CREATE SEQUENCE sequence_testx AS smallint MAXVALUE 100000;
CREATE SEQUENCE sequence_testx AS smallint MINVALUE -100000;

ALTER SEQUENCE sequence_test5 AS smallint;  -- success, max will be adjusted
ALTER SEQUENCE sequence_test8 AS smallint;  -- fail, max has to be adjusted
ALTER SEQUENCE sequence_test8 AS smallint MAXVALUE 20000;  -- ok now
ALTER SEQUENCE sequence_test9 AS smallint;  -- success, min will be adjusted
ALTER SEQUENCE sequence_test10 AS smallint;  -- fail, min has to be adjusted
ALTER SEQUENCE sequence_test10 AS smallint MINVALUE -20000;  -- ok now

ALTER SEQUENCE sequence_test11 AS int;  -- max will be adjusted
ALTER SEQUENCE sequence_test12 AS int;  -- min will be adjusted
ALTER SEQUENCE sequence_test13 AS int;  -- min and max will be adjusted
ALTER SEQUENCE sequence_test14 AS int;  -- min and max will be adjusted

---
--- test creation of SERIAL column
---
SET sequence_range = 1;
CREATE TABLE serialTest1 (f1 text, f2 serial);

INSERT INTO serialTest1 VALUES ('foo');
INSERT INTO serialTest1 VALUES ('bar');
INSERT INTO serialTest1 VALUES ('force', 100);
INSERT INTO serialTest1 VALUES ('wrong', NULL);

SELECT * FROM serialTest1 ORDER BY f1, f2;

-- test smallserial / bigserial
CREATE TABLE serialTest2 (f1 text, f2 serial, f3 smallserial, f4 serial2,
  f5 bigserial, f6 serial8);

INSERT INTO serialTest2 (f1)
  VALUES ('test_defaults');

INSERT INTO serialTest2 (f1, f2, f3, f4, f5, f6)
  VALUES ('test_max_vals', 2147483647, 32767, 32767, 9223372036854775807,
          9223372036854775807),
         ('test_min_vals', -2147483648, -32768, -32768, -9223372036854775808,
          -9223372036854775808);

-- All these INSERTs should fail:
INSERT INTO serialTest2 (f1, f3)
  VALUES ('bogus', -32769);

INSERT INTO serialTest2 (f1, f4)
  VALUES ('bogus', -32769);

INSERT INTO serialTest2 (f1, f3)
  VALUES ('bogus', 32768);

INSERT INTO serialTest2 (f1, f4)
  VALUES ('bogus', 32768);

INSERT INTO serialTest2 (f1, f5)
  VALUES ('bogus', -9223372036854775809);

INSERT INTO serialTest2 (f1, f6)
  VALUES ('bogus', -9223372036854775809);

INSERT INTO serialTest2 (f1, f5)
  VALUES ('bogus', 9223372036854775808);

INSERT INTO serialTest2 (f1, f6)
  VALUES ('bogus', 9223372036854775808);

SELECT * FROM serialTest2 ORDER BY f2 ASC;

SELECT nextval('serialTest2_f2_seq');
SELECT nextval('serialTest2_f3_seq');
SELECT nextval('serialTest2_f4_seq');
SELECT nextval('serialTest2_f5_seq');
SELECT nextval('serialTest2_f6_seq');

-- basic sequence operations using both text and oid references
CREATE SEQUENCE sequence_test;
CREATE SEQUENCE IF NOT EXISTS sequence_test;

SELECT nextval('sequence_test'::text);
SELECT nextval('sequence_test'::regclass);
SELECT currval('sequence_test'::text);
SELECT currval('sequence_test'::regclass);
SELECT setval('sequence_test'::text, 32);
SELECT nextval('sequence_test'::regclass);
SELECT setval('sequence_test'::text, 99, false);
SELECT nextval('sequence_test'::regclass);
SELECT setval('sequence_test'::regclass, 32);
SELECT nextval('sequence_test'::text);
SELECT setval('sequence_test'::regclass, 99, false);
SELECT nextval('sequence_test'::text);
DISCARD SEQUENCES;
SELECT currval('sequence_test'::regclass);

DROP SEQUENCE sequence_test;

-- renaming sequences
CREATE SEQUENCE foo_seq;
ALTER TABLE foo_seq RENAME TO foo_seq_new;
SELECT * FROM foo_seq_new;
SELECT nextval('foo_seq_new');
SELECT nextval('foo_seq_new');
-- log_cnt can be higher if there is a checkpoint just at the right
-- time, so just test for the expected range
SELECT last_value, log_cnt IN (31, 32) AS log_cnt_ok, is_called FROM foo_seq_new;
DROP SEQUENCE foo_seq_new;

-- renaming serial sequences
ALTER TABLE serialtest1_f2_seq RENAME TO serialtest1_f2_foo;
INSERT INTO serialTest1 VALUES ('more');
SELECT * FROM serialTest1 ORDER BY f1, f2;

--
-- Check dependencies of serial and ordinary sequences
--

CREATE TEMP SEQUENCE myseq2;
CREATE TEMP SEQUENCE myseq3;
CREATE TEMP TABLE t1 (
  f1 serial,
  f2 int DEFAULT nextval('myseq2'),
  f3 int DEFAULT nextval('myseq3'::text)
);
-- Both drops should fail, but with different error messages:
DROP SEQUENCE t1_f1_seq;
DROP SEQUENCE myseq2;
-- This however will work:
DROP SEQUENCE myseq3;
DROP TABLE t1;
-- Fails because no longer existent:
DROP SEQUENCE t1_f1_seq;
-- Now OK:
DROP SEQUENCE myseq2;

--
-- Alter sequence
--

ALTER SEQUENCE IF EXISTS sequence_test2 RESTART WITH 24
  INCREMENT BY 4 MAXVALUE 36 MINVALUE 5 CYCLE;

ALTER SEQUENCE serialTest1 CYCLE;  -- error, not a sequence

CREATE SEQUENCE sequence_test2 START WITH 32;
CREATE SEQUENCE sequence_test4 INCREMENT BY -1;

SELECT nextval('sequence_test2');
SELECT nextval('sequence_test4');

ALTER SEQUENCE sequence_test2 RESTART;
SELECT nextval('sequence_test2');

ALTER SEQUENCE sequence_test2 RESTART WITH 0;  -- error
ALTER SEQUENCE sequence_test4 RESTART WITH 40;  -- error

-- test CYCLE and NO CYCLE
ALTER SEQUENCE sequence_test2 RESTART WITH 24
  INCREMENT BY 4 MAXVALUE 36 MINVALUE 5 CYCLE;
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');  -- cycled

ALTER SEQUENCE sequence_test2 RESTART WITH 24
  NO CYCLE;
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');  -- error

ALTER SEQUENCE sequence_test2 RESTART WITH -24 START WITH -24
  INCREMENT BY -4 MINVALUE -36 MAXVALUE -5 CYCLE;
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');  -- cycled

ALTER SEQUENCE sequence_test2 RESTART WITH -24
  NO CYCLE;
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');  -- error

-- reset
ALTER SEQUENCE IF EXISTS sequence_test2 RESTART WITH 32 START WITH 32
  INCREMENT BY 4 MAXVALUE 36 MINVALUE 5 CYCLE;

SELECT setval('sequence_test2', -100);  -- error
SELECT setval('sequence_test2', 100);  -- error
SELECT setval('sequence_test2', 5);

CREATE SEQUENCE sequence_test3;  -- not read from, to test is_called


-- Information schema
SELECT * FROM information_schema.sequences
  WHERE sequence_name ~ ANY(ARRAY['sequence_test', 'serialtest'])
  ORDER BY sequence_name ASC;

SELECT schemaname, sequencename, start_value, min_value, max_value, increment_by, cycle, cache_size, last_value
FROM pg_sequences
WHERE sequencename ~ ANY(ARRAY['sequence_test', 'serialtest'])
  ORDER BY sequencename ASC;


SELECT * FROM pg_sequence_parameters('sequence_test4'::regclass);


-- Test comments
COMMENT ON SEQUENCE asdf IS 'won''t work';
COMMENT ON SEQUENCE sequence_test2 IS 'will work';
COMMENT ON SEQUENCE sequence_test2 IS NULL;

-- Test lastval()
CREATE SEQUENCE seq;
SELECT nextval('seq');
SELECT lastval();
SELECT setval('seq', 99);
SELECT lastval();
DISCARD SEQUENCES;
SELECT lastval();

CREATE SEQUENCE seq2;
SELECT nextval('seq2');
SELECT lastval();

DROP SEQUENCE seq2;
-- should fail
SELECT lastval();

CREATE USER regress_seq_user;

-- Test sequences in read-only transactions
CREATE TEMPORARY SEQUENCE sequence_test_temp1;
START TRANSACTION READ ONLY;
SELECT nextval('sequence_test_temp1');  -- ok
SELECT nextval('sequence_test2');  -- error
ROLLBACK;
START TRANSACTION READ ONLY;
SELECT setval('sequence_test_temp1', 1);  -- ok
SELECT setval('sequence_test2', 1);  -- error
ROLLBACK;

-- test global temp sequence,  consider global temporary sequence as regular temporary sequence.
create global temp sequence my_temp_seq0801;
select nextval('my_temp_seq0801');
select lastval();
select setval('my_temp_seq0801', 99);
select lastval();

-- privileges tests

-- nextval
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT SELECT ON seq3 TO regress_seq_user;
SELECT nextval('seq3');
ROLLBACK;

BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT UPDATE ON seq3 TO regress_seq_user;
SELECT nextval('seq3');
ROLLBACK;

BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT USAGE ON seq3 TO regress_seq_user;
SELECT nextval('seq3');
ROLLBACK;

-- currval
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT SELECT ON seq3 TO regress_seq_user;
SELECT currval('seq3');
ROLLBACK;

BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT UPDATE ON seq3 TO regress_seq_user;
SELECT currval('seq3');
ROLLBACK;

BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT USAGE ON seq3 TO regress_seq_user;
SELECT currval('seq3');
ROLLBACK;

-- lastval
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT SELECT ON seq3 TO regress_seq_user;
SELECT lastval();
ROLLBACK;

BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT UPDATE ON seq3 TO regress_seq_user;
SELECT lastval();
ROLLBACK;

BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT USAGE ON seq3 TO regress_seq_user;
SELECT lastval();
ROLLBACK;

-- setval
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
REVOKE ALL ON seq3 FROM regress_seq_user;
SELECT setval('seq3', 5);
ROLLBACK;

BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
CREATE SEQUENCE seq3;
SELECT setval('seq3', 5);
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT UPDATE ON seq3 TO regress_seq_user;
SELECT setval('seq3', 5);
SELECT nextval('seq3');
ROLLBACK;

-- ALTER SEQUENCE
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
ALTER SEQUENCE sequence_test2 START WITH 1;
ROLLBACK;

-- Sequences should get wiped out as well:
DROP TABLE serialTest1, serialTest2;

-- Make sure sequences are gone:
SELECT * FROM information_schema.sequences WHERE sequence_name IN
  ('sequence_test2', 'serialtest2_f2_seq', 'serialtest2_f3_seq',
   'serialtest2_f4_seq', 'serialtest2_f5_seq', 'serialtest2_f6_seq')
  ORDER BY sequence_name ASC;

DROP USER regress_seq_user;
DROP SEQUENCE seq;

create table test_seqtab (unique1 int, unique2 int);
insert into test_seqtab select i, i from generate_series(1,1000) s(i);

create temp sequence testseq;
select distinct(nextval('testseq'))
  from test_seqtab order by 1 limit 10;
drop table test_seqtab;

-- cache tests
CREATE SEQUENCE test_seq1 CACHE 10;
SELECT nextval('test_seq1');
SELECT nextval('test_seq1');
SELECT nextval('test_seq1');

DROP SEQUENCE test_seq1;

CREATE SEQUENCE test_seq2 INCREMENT BY 5 MINVALUE 20 MAXVALUE 60 CACHE 4;
SELECT nextval('test_seq2');
SELECT nextval('test_seq2');
SELECT nextval('test_seq2');
SELECT nextval('test_seq2');
SELECT nextval('test_seq2');
SELECT nextval('test_seq2');
SELECT nextval('test_seq2');
SELECT nextval('test_seq2');
SELECT nextval('test_seq2');
SELECT nextval('test_seq2'); -- error: overflow
DROP SEQUENCE test_seq2;

CREATE SEQUENCE test_seq3 INCREMENT BY -5 MINVALUE 20 MAXVALUE 60 CACHE 4;
SELECT nextval('test_seq3');
SELECT nextval('test_seq3');
SELECT nextval('test_seq3');
SELECT nextval('test_seq3');
SELECT nextval('test_seq3');
SELECT nextval('test_seq3');
SELECT nextval('test_seq3');
SELECT nextval('test_seq3');
SELECT nextval('test_seq3');
SELECT nextval('test_seq3'); -- error: underflow
DROP SEQUENCE test_seq3;

CREATE SEQUENCE test_seq4 INCREMENT BY -5 MINVALUE -60 MAXVALUE -20 CACHE 4;
SELECT nextval('test_seq4');
SELECT nextval('test_seq4');
SELECT nextval('test_seq4');
SELECT nextval('test_seq4');
SELECT nextval('test_seq4');
SELECT nextval('test_seq4');
SELECT nextval('test_seq4');
SELECT nextval('test_seq4');
SELECT nextval('test_seq4');
SELECT nextval('test_seq4'); -- error: underflow
DROP SEQUENCE test_seq4;

CREATE SEQUENCE test_seq5 INCREMENT BY 17 MINVALUE 20 MAXVALUE 60 CACHE 4;
SELECT nextval('test_seq5');
SELECT nextval('test_seq5');
SELECT nextval('test_seq5');
SELECT nextval('test_seq5'); -- error: overflow
DROP SEQUENCE test_seq5;

CREATE SEQUENCE test_seq6 INCREMENT BY -17 MINVALUE 20 MAXVALUE 60 CACHE 4;
SELECT nextval('test_seq6');
SELECT nextval('test_seq6');
SELECT nextval('test_seq6');
SELECT nextval('test_seq6'); -- error: underflow
DROP SEQUENCE test_seq6;

CREATE SEQUENCE test_seq7 INCREMENT BY 17 MINVALUE 20 MAXVALUE 60 CACHE 4 CYCLE;
SELECT nextval('test_seq7');
SELECT nextval('test_seq7');
SELECT nextval('test_seq7');
SELECT nextval('test_seq7');
SELECT nextval('test_seq7');
SELECT nextval('test_seq7');
SELECT nextval('test_seq7');
SELECT nextval('test_seq7');
SELECT nextval('test_seq7');
SELECT nextval('test_seq7');
DROP SEQUENCE test_seq7;

CREATE SEQUENCE test_seq8 INCREMENT BY -17 MINVALUE 20 MAXVALUE 60 CACHE 4 CYCLE;
SELECT nextval('test_seq8');
SELECT nextval('test_seq8');
SELECT nextval('test_seq8');
SELECT nextval('test_seq8');
SELECT nextval('test_seq8');
SELECT nextval('test_seq8');
SELECT nextval('test_seq8');
SELECT nextval('test_seq8');
SELECT nextval('test_seq8');
SELECT nextval('test_seq8');
DROP SEQUENCE test_seq8;

CREATE SEQUENCE test_seq9 MAXVALUE 5 CACHE 4;
SELECT nextval('test_seq9');
SELECT nextval('test_seq9');
SELECT nextval('test_seq9');
SELECT nextval('test_seq9');
SELECT nextval('test_seq9');
SELECT nextval('test_seq9'); -- error: overflow
DROP SEQUENCE test_seq9;

-- Test sequece when alter database
ALTER DATABASE db_seq1 RENAME TO db_seq3;
ALTER DATABASE db_seq2 RENAME TO db_seq1;
\c db_seq1
DROP SEQUENCE my_seq;
CREATE SEQUENCE my_seq;
DROP SEQUENCE my_seq;
\c db_seq3
DROP SEQUENCE my_seq;
CREATE SEQUENCE my_seq;
DROP SEQUENCE my_seq;

-- Test sequece when drop database
\c db_seq1
create table t1(f1 serial,f2 int);
create table t2(f1 serial,f2 int);
create table t3(f1 serial,f2 int);
insert into t1(f2) values(1);
insert into t2(f2) values(2);
insert into t3(f2) values(3);
create database db_seq1_bak;

\c db_seq1_bak
create table t1(f1 serial,f2 int);
create table t2(f1 serial,f2 int);
create table t3(f1 serial,f2 int);
insert into t1(f2) values(1);
insert into t2(f2) values(2);
insert into t3(f2) values(3);
drop database db_seq1;

\c db_seq1_bak
insert into t1(f2) values(4);
insert into t2(f2) values(5);
insert into t3(f2) values(6);
select gsk_key from pg_list_storage_sequence() where gsk_key like '%db_seq1_bak.%';
create sequence if not exists seq_test as smallint increment by 1 start with 1 CACHE 1000000000 cycle;
drop sequence seq_test;
\q
