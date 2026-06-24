--
-- TEMP
-- Test temp relations and indexes
--

-- test temp table/index masking

begin;
savepoint sp1;
create temp table test1(id serial);
rollback to sp1;
end;

create temp table test1(id serial);
drop table test1;

CREATE TABLE temptest(col int);

CREATE INDEX i_temptest ON temptest(col);

CREATE TEMP TABLE temptest(tcol int);

CREATE INDEX i_temptest ON temptest(tcol);

SELECT * FROM temptest;

DROP INDEX i_temptest;

DROP TABLE temptest;

SELECT * FROM temptest;

DROP INDEX i_temptest;

DROP TABLE temptest;

-- test temp table selects

CREATE TABLE temptest(col int);

INSERT INTO temptest VALUES (1);

CREATE TEMP TABLE temptest(tcol float);

INSERT INTO temptest VALUES (2.1);

SELECT * FROM temptest;

DROP TABLE temptest;

SELECT * FROM temptest;

DROP TABLE temptest;

-- test temp table deletion

CREATE TEMP TABLE temptest(col int);

\c

SELECT * FROM temptest;

-- Test ON COMMIT DELETE ROWS

CREATE TEMP TABLE temptest(col int) ON COMMIT DELETE ROWS;

-- while we're here, verify successful truncation of index with SQL function
CREATE INDEX ON temptest(bit_length(''));

BEGIN;
INSERT INTO temptest VALUES (1);
INSERT INTO temptest VALUES (2);

SELECT * FROM temptest  ORDER BY 1;
COMMIT;

SELECT * FROM temptest;

DROP TABLE temptest;

BEGIN;
CREATE TEMP TABLE temptest(col) ON COMMIT DELETE ROWS AS SELECT 1;

SELECT * FROM temptest;
COMMIT;

--SELECT * FROM temptest;

DROP TABLE temptest;

-- Test ON COMMIT DROP

BEGIN;

CREATE TEMP TABLE temptest(col int) ON COMMIT DROP;

INSERT INTO temptest VALUES (1);
INSERT INTO temptest VALUES (2);

SELECT * FROM temptest ORDER BY 1;
COMMIT;

SELECT * FROM temptest;

BEGIN;
CREATE TEMP TABLE temptest(col) ON COMMIT DROP AS SELECT 1;

SELECT * FROM temptest;
COMMIT;

SELECT * FROM temptest;

-- ON COMMIT is only allowed for TEMP

CREATE TABLE temptest(col int) ON COMMIT DELETE ROWS;
CREATE TABLE temptest(col) ON COMMIT DELETE ROWS AS SELECT 1;

-- Test foreign keys
BEGIN;
CREATE TEMP TABLE temptest1(col int PRIMARY KEY);
CREATE TEMP TABLE temptest2(col int REFERENCES temptest1)
  ON COMMIT DELETE ROWS;
INSERT INTO temptest1 VALUES (1);
INSERT INTO temptest2 VALUES (1);
COMMIT;
SELECT * FROM temptest1;
--SELECT * FROM temptest2;

BEGIN;
CREATE TEMP TABLE temptest3(col int PRIMARY KEY) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE temptest4(col int REFERENCES temptest3);
COMMIT;

-- Test manipulation of temp schema's placement in search path

create table public.whereami (f1 text);
insert into public.whereami values ('public');

create temp table whereami (f1 text);
insert into whereami values ('temp');

create function public.whoami() returns text
  as $$select 'public'::text$$ language sql;

create function pg_temp.whoami() returns text
  as $$select 'temp'::text$$ language sql;

-- default should have pg_temp implicitly first, but only for tables
select * from whereami;
select whoami();

-- can list temp first explicitly, but it still doesn't affect functions
set search_path = pg_temp, public;
select * from whereami;
select whoami();

-- or put it last for security
set search_path = public, pg_temp;
select * from whereami;
select whoami();

-- you can invoke a temp function explicitly, though
select pg_temp.whoami();

drop table public.whereami;

-- Test local temp table with distribute by clause
CREATE LOCAL TEMP TABLE localtemptest(a int,b varchar(10)) distribute by shard(a);

INSERT INTO localtemptest VALUES(1,'abc'),(2,'def'),(3,'GHI');

SELECT * FROM localtemptest order by a, b;

DROP TABLE localtemptest;

CREATE LOCAL TEMP TABLE localtemptest(a int,b varchar(10)) distribute by replication;

INSERT INTO localtemptest values(1,'abc'),(2,'def'),(3,'GHI');

SELECT * FROM localtemptest order by a, b;

DROP TABLE localtemptest;
-- Check dependencies between ON COMMIT actions with a partitioned
-- table and its partitions.  Using ON COMMIT DROP on a parent removes
-- the whole set.
begin;
create temp table temp_parted_oncommit_test (a int)
  partition by list (a) on commit drop;
create temp table temp_parted_oncommit_test1
  partition of temp_parted_oncommit_test
  for values in (1) on commit delete rows;
create temp table temp_parted_oncommit_test2
  partition of temp_parted_oncommit_test
  for values in (2) on commit drop;
insert into temp_parted_oncommit_test values (1), (2);
commit;
-- no relations remain in this case.
select relname from pg_class where relname like 'temp_parted_oncommit_test%';
-- Using ON COMMIT DELETE on a partitioned table does not remove
-- all rows if partitions preserve their data.
begin;
create temp table temp_parted_oncommit_test (a int)
  partition by list (a) on commit preserve rows;
create temp table temp_parted_oncommit_test1
  partition of temp_parted_oncommit_test
  for values in (1) on commit preserve rows;
create temp table temp_parted_oncommit_test2
  partition of temp_parted_oncommit_test
  for values in (2) on commit drop;
insert into temp_parted_oncommit_test values (1), (2);
commit;
-- Data from the remaining partition is still here as its rows are
-- preserved.
select * from temp_parted_oncommit_test;
-- two relations remain in this case.
select relname from pg_class where relname like 'temp_parted_oncommit_test%';
drop table temp_parted_oncommit_test;

-- Check dependencies between ON COMMIT actions with inheritance trees.
-- Using ON COMMIT DROP on a parent removes the whole set.
begin;
create temp table temp_inh_oncommit_test (a int) on commit drop;
create temp table temp_inh_oncommit_test1 ()
  inherits(temp_inh_oncommit_test) on commit delete rows;
insert into temp_inh_oncommit_test1 values (1);
commit;
-- no relations remain in this case
select relname from pg_class where relname like 'temp_inh_oncommit_test%';
-- Data on the parent is removed, and the child goes away.
begin;
create temp table temp_inh_oncommit_test (a int) on commit delete rows;
create temp table temp_inh_oncommit_test1 ()
  inherits(temp_inh_oncommit_test) on commit drop;
insert into temp_inh_oncommit_test1 values (1);
insert into temp_inh_oncommit_test values (1);
commit;
select * from temp_inh_oncommit_test;
-- one relation remains
select relname from pg_class where relname like 'temp_inh_oncommit_test%';
drop table temp_inh_oncommit_test;

-- For partitioned temp tables, ON COMMIT actions ignore storage-less
-- partitioned tables.
begin;
create temp table temp_parted_oncommit (a int)
  partition by list (a) on commit delete rows;
create temp table temp_parted_oncommit_1
  partition of temp_parted_oncommit
  for values in (1) on commit delete rows;
insert into temp_parted_oncommit values (1);
commit;
-- partitions are emptied by the previous commit
select * from temp_parted_oncommit;
drop table temp_parted_oncommit;

-- test create function with temp table
drop table if exists users_20220715;
drop function if exists get_first_user;
create temp table users_20220715 (userid text, seq int, email text, todrop bool, moredrop int, enabled bool);
insert into users_20220715 values ('id',1,'email',true,11,true);
insert into users_20220715 values ('id2',2,'email2',true,12,true);
create function get_first_user() returns users_20220715 as
$$ SELECT * FROM users_20220715 ORDER BY userid LIMIT 1; $$
language sql stable;
select count(1) from get_first_user();
drop table users_20220715 cascade;

---
-- local temp table bug
---
drop table if exists ltb1;
create local temp table ltb1(a int, b int);
insert into ltb1 values(1,1);
insert into ltb1 values(2,2);
insert into ltb1 values(3,3);
insert into ltb1 values(4,4);
insert into ltb1 values(5,5);
select * from ltb1 order by a;
drop table  ltb1;

-- recreate the local temp table
create local temp table ltb1(a int, b int);
drop table ltb1;
-- switch to a new session, and create the local temp table
\c 
create local temp table ltb1(a int, b int);
drop table ltb1;

-- local temp table CTAS bug
drop table if exists ltt;
drop table if exists ltb2;
create table ltt(a int, b int);
insert into ltt values(1,1),(2,2),(3,3),(4,4);
create local temp table ltb2 as select * from ltt;
select * from ltb2 order by a;
drop table ltb2;
create local temp table ltb2 as select * from ltt;
select * from ltb2 order by a;

-- cleanup
drop table ltb2;
drop table ltt;
-- test clean temp table schema
create temp table test_clean(id int);
\c postgres
\c regression
create temp table test_clean(id int);
select pg_sleep(5);
\c postgres
\c regression

select pg_clean_unused_temp_schema(null);
select count(*) from pg_namespace where nspname like 'pg_temp_%' or nspname like 'pg_toast_temp_%';
create temp table test_clean(id int);
select count(*) from pg_namespace where nspname like 'pg_temp_%' or nspname like 'pg_toast_temp_%';
-- can not clean the temp schema
select pg_clean_unused_temp_schema(null);
select count(*) from pg_namespace where nspname like 'pg_temp_%' or nspname like 'pg_toast_temp_%';



--To resolve the issue of residual temporary table spaces in distributed scenarios, including DN and other executing CNs, the code logic has been corrected. 
drop table if exists "tmp-sswfxwdj";
create temp table "tmp-sswfxwdj" (f1 int);
insert into "tmp-sswfxwdj" values (1),(2),(3);
\c
create temp table "tmp-sswfxwdj" (f1 int);
\c
create temp table "tmp-sswfxwdj" (f1 int);
\c
--Temporary tables are cleaned up when the previous process exits, so please wait for a moment.
select pg_sleep(3);
-- expected 0 rows
select n.nspname,c.relname  from pg_class c, pg_namespace n where n.oid=c.relnamespace  and c.relname='tmp-sswfxwdj';
EXECUTE DIRECT ON (coord1) 'select n.nspname,c.relname  from pg_class c, pg_namespace n where n.oid=c.relnamespace  and c.relname=''tmp-sswfxwdj''';
EXECUTE DIRECT ON (coord2) 'select n.nspname,c.relname  from pg_class c, pg_namespace n where n.oid=c.relnamespace  and c.relname=''tmp-sswfxwdj''';
EXECUTE DIRECT ON (datanode_1) 'select n.nspname,c.relname  from pg_class c, pg_namespace n where n.oid=c.relnamespace  and c.relname=''tmp-sswfxwdj''';
EXECUTE DIRECT ON (datanode_2) 'select n.nspname,c.relname  from pg_class c, pg_namespace n where n.oid=c.relnamespace  and c.relname=''tmp-sswfxwdj''';
