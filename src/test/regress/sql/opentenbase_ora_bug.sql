\c regression_ora
-- START TAPD: 887262065
-- case: table 
create table tbl_20230920_1(id int, name varchar2(20));
insert into tbl_20230920_1 values(1, 'xxx');
insert into tbl_20230920_1 values(2, 'yyy');
insert into tbl_20230920_1 values(0, 'mmm');
select 1+2 as col1, 'kkk' as col2, id, name from tbl_20230920_1 order by to_number(col1), 3;
select 1+2 as col1, 'kkk' as col2, id, name from tbl_20230920_1 order by to_number(col1) || id, 3;
select 1+2 as col1, 'kkk' as col2, id, name from tbl_20230920_1 order by to_number(col1) + 1, 3;
select 1+2 as col1, 'kkk' as col2, id, name from tbl_20230920_1 order by col1 + 1, 3;
select 1+2 as col1, to_number('333.44') as col2, id, name from tbl_20230920_1 order by to_number(col1), to_number(col2) || id, 3;
select (select 1 from dual) as col1 from dual order by (select id from tbl_20230920_1) || col1;
select id + 1 as col1 from tbl_20230920_1 order by (select max(id) from tbl_20230920_1) || col1;

-- expected error
select (select 1 from dual) as col1 from dual order by (select col1 from dual);
select (select 1 from dual) as col1 from dual order by (select to_number(col1) from dual);
select (select 1 from dual) as col1 from dual order by (select id from tbl_20230920_1 order by col1);
select 1+2 as col1, name || '_x' as col2 from tbl_20230920_1 where col1 = 3;

-- case: sublink
select (select 1 from dual) as col1 from dual order by to_number(col1);
select (select 1 from dual) as col1 from dual order by to_number(col1), to_number(col1);
select (select 1 from dual) as col1, (select 'aaa' from dual) as col2 from dual order by to_number(col1), to_char(col2);
select 'xxxx' as col1, '123' as col2 from dual order by to_number(col1), col2;

-- case: view
create view v_orderby_20230920 as select (select 1 from dual) as col1, (select 'aaa' from dual) as col2 from dual order by to_number(col1), to_char(col2);
select * from v_orderby_20230920;
select * from v_orderby_20230920 order by to_number(col1);
drop view v_orderby_20230920;
drop table tbl_20230920_1;
-- END TAPD: 887262065

set skip_check_same_relname to on;
-- TEST TAPD: 887132191 
create table a0413(a1 int, a2 int, a3 int);
create table b0413(b1 int, b2 int, b3 int);
create table c0413(c1 int, c2 int, c3 int);

insert into a0413 values(1,null,111);
insert into a0413 values(1,11,111);
insert into a0413 values(1,99,111);
insert into a0413 values(2,null,222);
insert into a0413 values(2,22,222);
insert into a0413 values(2,88,222);

insert into b0413 values(1,null,111);
insert into b0413 values(1,88,111);
insert into b0413 values(2,null,222);
insert into b0413 values(2,11,222);

insert into c0413 values(1,null,111);
insert into c0413 values(1,88,111);
insert into c0413 values(2,null,222);
insert into c0413 values(2,11,222);
-- same rels
explain (costs off, verbose) select 1 from a0413 s, b0413 s where  s.b1 = 1;

create table tb1_20230906(a int);
create table tb2_20230906(b int);
create table tb3_20230906(a int);
select * from tb1_20230906, tb2_20230906, tb3_20230906;
select t.a from tb1_20230906 t, tb2_20230906 t;
select t.b from tb1_20230906 t, tb2_20230906 t;
select t.a from tb1_20230906 t, tb3_20230906 t;
select a from tb1_20230906 t, tb2_20230906 t;
select b from tb1_20230906 t, tb2_20230906 t;
select a from tb1_20230906 t, tb3_20230906 t;
select t.* from tb1_20230906 t, tb2_20230906 t; -- error

drop table a0413;
drop table b0413;
drop table c0413;
drop table tb1_20230906;
drop table tb2_20230906;
drop table tb3_20230906;
reset skip_check_same_relname;
--END TEST 887132191

-- tapd:114400655
create schema s1;
create function s1.func1() return void as
begin
    raise notice '%', 'schema_s1_func1';
end;
/

select s1.func1();

create public synonym synonym1 for s1;
--expect error
select synonym1.func1();

create package s1 AS
end s1;
/
--expect error
select synonym1.func1();
create or replace function s1(a int) return int is
begin return a; end;
/
--expect error
select synonym1.func1();
drop package s1;
--expect error
select synonym1.func1();

drop schema s1 cascade;
drop function s1;
--end tapd:114400655

--tapd: 115860571
create table tbl_20240110(a int, b int);
select count(a.*) from dual;
select count(a.*) from tbl_20240110;
select count(b.*) from tbl_20240110;
select count(a.b) from tbl_20240110;
drop table tbl_20240110;
-- end tapd:115860571

-- begin: 119236711
create role r_allow_unmatched_block_comments_tester;
set session role r_allow_unmatched_block_comments_tester;
show allow_unmatched_block_comments;
select name,setting,context from pg_settings where name = 'allow_unmatched_block_comments';
set allow_unmatched_block_comments to off;
select /* aaa */ /* a */ 'unmatch example' AS test;			-- ok
select /* /* aaa */ */ /* a */ 'unmatch example' AS test;	-- nested block comment ok
select /* /* /* a */ 'unmatch example' AS test;				-- failed
set allow_unmatched_block_comments to on;
select /* /* /* a */ 'unmatch example' AS test;				-- ok
select /* /* aaa */ /* a */ 'unmatch example' AS test;		-- ok
select /* aaa */ /* a */ 'unmatch example' AS test;			-- ok
select /* /* aaa */ */ /* a */ 'unmatch example' AS test;	-- nested block comment failed
\c -
drop role r_allow_unmatched_block_comments_tester;
-- end: 119236711

-- bug: 119377019
CREATE TABLE bug_tbl_20240124(
c0 int,
c1 int,
c2 int,
c3 varchar(20),
c4 varchar(20),
c5 date,
c6 date,
c7 numeric,
c8 varchar(20),
c9 date)    PARTITION BY hash( c3) distribute by replication;
create TABLE bug_tbl_20240124_p0 partition of bug_tbl_20240124 for values with(modulus 4,remainder 0);
create TABLE bug_tbl_20240124_p1 partition of bug_tbl_20240124 for values with(modulus 4,remainder 1);
create TABLE bug_tbl_20240124_p2 partition of bug_tbl_20240124 for values with(modulus 4,remainder 2);
create TABLE bug_tbl_20240124_p3 partition of bug_tbl_20240124 for values with(modulus 4,remainder 3);
alter table bug_tbl_20240124 alter column c0 drop not null;
INSERT INTO bug_tbl_20240124 VALUES
(2, 1, 4, 'bar', 'bar', '1988-05-23', '1979-12-16', NULL, 'd', '2006-05-20 16:59:20') , 
(5, 3, 5, 'sx', 'bar', '2031-02-01', '2021-10-28', 9, 'bar', '1991-09-12 07:43:01') ,
(3, 4, 2, 'xtclceqc', 'bar', '1974-01-13', '1995-01-26 19:59:45', NULL, 'tclceqcjcu', '2021-11-12 23:54:13') ,
(2, 1, 2, 'clceqcjcug', 'lce', '2031-08-20', '1973-05-05 10:10:26', 6, 'ceqcjcu', '2003-06-16') ,
(4, 3, 5, 'eqc', 'qcjcugfmha', '2010-11-27', '2033-08-23 10:37:20', 1, 'bar', '1986-08-10');

CREATE OR REPLACE FUNCTION INSERT_RETURN_20231205() RETURNS INT AS
$$ DECLARE 
n INT;
BEGIN
UPDATE bug_tbl_20240124 SET c7 = 1086 % -2692 WHERE c1 = 1 RETURNING c0 INTO n ;
RETURN 1;
END; $$;
ALTER FUNCTION INSERT_RETURN_20231205 PUSHDOWN;

SELECT INSERT_RETURN_20231205();

CREATE OR REPLACE FUNCTION INSERT_RETURN_20231205_1() RETURNS INT AS
$$ DECLARE 
n INT;
BEGIN
UPDATE bug_tbl_20240124 SET c7 = 1086 % -2692 WHERE c1 = 4 RETURNING c0 INTO n ;
raise notice 'n: %', n;
RETURN 1;
END; $$;
ALTER FUNCTION INSERT_RETURN_20231205_1 PUSHDOWN;

SELECT INSERT_RETURN_20231205_1();

DROP FUNCTION INSERT_RETURN_20231205;
DROP FUNCTION INSERT_RETURN_20231205_1;
DROP TABLE bug_tbl_20240124;
-- end bug: 119377019

-- start bug: 122976613
CREATE TABLE IF NOT EXISTS "bug122976613"(
    "ID" NUMERIC(10,2) NOT NULL,
    "1" BIGINT,
    "EXT_COL_1" BIGINT,
    "ext_col_2" BIGINT,
    "EXT_COL_3" BIGINT,
    "EXT_COL_4" NUMERIC(10,2),
    "ext_col_5" NUMERIC(10,2),
    CONSTRAINT "SHARD_TABLE_PKEY" PRIMARY KEY ("ID")
) WITH (OIDS = FALSE) DISTRIBUTE BY SHARD("ID");
-- OPENTENBASE_ORA converts all objects without double quotes to uppercase
ALTER TABLE "bug122976613" ALTER COLUMN "ext_col_5" TYPE "NUMERIC"(10, 3);
ALTER TABLE "bug122976613" ALTER COLUMN ext_col_4 TYPE "NUMERIC"(10, 3);
ALTER TABLE "bug122976613" ALTER COLUMN "EXT_COL_4" TYPE "NUMERIC"(10, 2);
ALTER TABLE "bug122976613" ALTER COLUMN EXT_COL_1 TYPE INT;
-- ERROR
ALTER TABLE bug122976613 ALTER COLUMN "ext_col_2" TYPE INT;
ALTER TABLE "bug122976613" ALTER COLUMN "ext_col_3" TYPE INT;
ALTER TABLE "bug122976613" ALTER COLUMN ext_col_5 TYPE "NUMERIC"(10, 4);
DROP TABLE "bug122976613";
-- end bug: 122976613

-- position for update clause and order by clause in query can swap each other.
drop table if exists test_position_swap;
-- 1. simple select
create table test_position_swap(a int, b int);
select * from test_position_swap for update order by a; --ok
select * from test_position_swap order by a for update; --ok
select * from test_position_swap for update; --ok
select * from test_position_swap order by a for update order by b; -- error

-- 2. with clause
with cte1 as (select * from test_position_swap) select * from cte1 for update; -- ok
with cte1 as (select * from test_position_swap) select * from cte1 order by a; -- ok
with cte1 as (select * from test_position_swap) select * from cte1 for update order by a; -- ok
with cte1 as (select * from test_position_swap) select * from cte1 order by a for update; -- ok
with cte1 as (select * from test_position_swap) select * from cte1 order by a for update order by a; -- error

drop table test_position_swap;

drop table t1_20250103;
drop table t2_20250103;
-- 复制表+rowid，报错：
create table t1_20250103(k1 int, k2 varchar)   distribute by replication;
insert into t1_20250103 values(1, generate_series(1, 8));
select * from t1_20250103 order by 1;

-- 复制表+不带rowid，不报错
create table t2_20250103(k1 int, k2 varchar) distribute by replication;
insert into t2_20250103 values(1, generate_series(1, 8));
select * from t2_20250103 order by 1;
drop table t1_20250103;
drop table t2_20250103;

set transform_insert_to_copy to on;
-- 复制表+rowid，报错：
create table t1_20250103(k1 int, k2 varchar)   distribute by replication;
insert into t1_20250103 values(1, generate_series(1, 8));
select * from t1_20250103 order by 1;

-- 复制表+不带rowid，不报错
create table t2_20250103(k1 int, k2 varchar) distribute by replication;
insert into t2_20250103 values(1, generate_series(1, 8));
select * from t2_20250103 order by 1;
drop table t1_20250103;
drop table t2_20250103;
reset transform_insert_to_copy;
