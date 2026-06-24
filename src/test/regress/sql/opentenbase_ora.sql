\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;

DROP DATABASE IF EXISTS regression_sort;
CREATE DATABASE regression_sort WITH TEMPLATE = template0_ora ENCODING='SQL_ASCII' LC_COLLATE='C' LC_CTYPE='C';

\c regression_ora

-- test opentenbase_ora namespace
select oid, nspname, nspowner 
    from pg_namespace 
    where nspname = 'opentenbase_ora';

set search_path = foo, public, not_there_initially;
select current_schemas(true);
create schema not_there_initially;
select current_schemas(true);
drop schema not_there_initially;
select current_schemas(true);
reset search_path;

select current_schemas(true);

reset search_path;
select current_schemas(true);

-- test Alias
select * from (VALUES(1,2,3)) as v0;
select * from (VALUES(1,2,3));

drop table if exists test1;
create table test1(tc1 int2, tc2 int4, tc3 int8, tc4 float4, tc5 float8);
drop table if exists test2;
create table test2(tc1 int2, tc2 int4, tc3 int8, tc4 float4, tc5 float8);

insert into test1 values(1,2,3,4,5);
insert into test1 values(6,7,8,9,0);

insert into test2 values(1,2,3,4,5);
insert into test2 values(6,7,8,9,0);

select * 
    from (select * from test1 order by tc1, tc2, tc3, tc4, tc5),
         (select * from test2 order by tc5, tc4, tc3, tc2, tc1);

drop table test1;
drop table test2;

-- test number/binary_float/binary_double
drop table if exists test1;
create table test1(tc1 binary_float, tc2 binary_double, tc3 number, tc4 float4, tc5 float8, tc6 numeric);
create index idx_test1_tc1 on test1(tc1);
create index idx_test1_tc2 on test1(tc2);
create index idx_test1_tc3 on test1(tc3);
create index idx_test1_tc4 on test1(tc4);
create index idx_test1_tc5 on test1(tc5);
create index idx_test1_tc6 on test1(tc6);

\d+ test1

insert into test1 values(10.0123456789, 11.0123456789, 12.0123456789, 13.0123456789, 14.0123456789, 15.0123456789);
insert into test1 values(20.0123456789, 22.0123456789, 22.0123456789, 23.0123456789, 24.0123456789, 25.0123456789);
insert into test1 values(30.0123456789, 33.0123456789, 32.0123456789, 33.0123456789, 34.0123456789, 35.0123456789);
insert into test1 values(40.0123456789, 44.0123456789, 42.0123456789, 43.0123456789, 44.0123456789, 45.0123456789);
insert into test1 values(50.0123456789, 55.0123456789, 52.0123456789, 53.0123456789, 54.0123456789, 55.0123456789);
insert into test1 values(60.0123456789, 66.0123456789, 62.0123456789, 63.0123456789, 64.0123456789, 65.0123456789);
insert into test1 values(70.0123456789, 77.0123456789, 72.0123456789, 73.0123456789, 74.0123456789, 75.0123456789);
insert into test1 values(80.0123456789, 88.0123456789, 82.0123456789, 83.0123456789, 84.0123456789, 85.0123456789);
insert into test1 values(90.0123456789, 99.0123456789, 92.0123456789, 93.0123456789, 94.0123456789, 95.0123456789);

-- explain select * from test1 where tc1 > 20.0123456789 and tc1 < 70.0123456789 order by tc1;
-- explain select * from test1 where tc2 > 20.0123456789 and tc2 < 70.0123456789 order by tc2;
-- explain select * from test1 where tc3 > 20.0123456789 and tc3 < 70.0123456789 order by tc3;
-- explain select * from test1 where tc4 > 20.0123456789 and tc4 < 70.0123456789 order by tc4;
-- explain select * from test1 where tc5 > 20.0123456789 and tc5 < 70.0123456789 order by tc5;
-- explain select * from test1 where tc6 > 20.0123456789 and tc6 < 70.0123456789 order by tc6;

select * from test1 where tc1 > 20.0123456789 and tc1 < 70.0123456789 order by tc1;
select * from test1 where tc2 > 20.0123456789 and tc2 < 70.0123456789 order by tc2;
select * from test1 where tc3 > 20.0123456789 and tc3 < 70.0123456789 order by tc3;
select * from test1 where tc4 > 20.0123456789 and tc4 < 70.0123456789 order by tc4;
select * from test1 where tc5 > 20.0123456789 and tc5 < 70.0123456789 order by tc5;
select * from test1 where tc6 > 20.0123456789 and tc6 < 70.0123456789 order by tc6;

drop table test1;

--test number(*,0)
create table t_number_38(id number(38,5));
\d+ t_number_38
insert into t_number_38 values (12345678901234567890123456789012345678.123456789);
insert into t_number_38 values (123456789012345678901234567890123.123456789);
select * from t_number_38;

create table t_number_star(id number(*));
\d+ t_number_star
insert into t_number_star values (1234567890123456789012345678901234567890.123456789);
insert into t_number_star values (123456789012345678901234567890123.123456789);
select * from t_number_star;

create table t_number_star_5(id number(*,5));
\d+ t_number_star_5
insert into t_number_star_5 values (12345678901234567890123456789012345678.123456789);
insert into t_number_star_5 values (123456789012345678901234567890123.123456789);
select * from t_number_star_5;

drop table t_number_38;
drop table t_number_star;
drop table t_number_star_5;

-- test int4div
select 1/2 as div;
select 1/2::int2 as div;
select 1/2::int8 as div;
select 1/2 as div;
select 1/2::int2 as div;
select 1/2::int8 as div;

-- test sysdate
select sysdate - sysdate as mi;

-- test dual;
select * from dual;
select 1 as t from dual;
select sysdate - sysdate AS now from dual;

drop table if exists test;
create table test(tc1 int4, tc2 int4);
insert into test values(1,2);
insert into test values(1,3);
insert into test values(1,4);
insert into test values(2,2);
insert into test values(3,2);
insert into test values(4,2);
insert into test values(5,2);
insert into test values(null, null);
insert into test values(null, 1);
insert into test values(1, null);

\d+ test

select tc1 / tc2 as div1,
        tc2 / tc1 as div2,
        (tc1 + tc2) / (tc1) as div3,
        (tc1::int2 + tc2::int2) / (tc1::int2) as div4, 
        tc1::int4 / tc2::int8 as div5,
        tc2::int2 / tc1::int2 as div6
    from test 
    order by div1, div2, div3, div4, div5, div6;

select tc1 / tc2 as div1,
        tc2 / tc1 as div2,
        (tc1 + tc2) / (tc1) as div3,
        (tc1::int2 + tc2::int2) / (tc1::int2) as div4, 
        tc1::int4 / tc2::int8 as div5,
        tc2::int2 / tc1::int2 as div6
    from test 
    order by div1, div2, div3, div4, div5, div6;

drop table test;

--test sequence
create sequence public.my_serial start 11;

select nextval('public.my_serial');
select currval('public.my_serial');
select lastval();

select setval('public.my_serial', 12);
select currval('public.my_serial');
select nextval('public.my_serial');
select lastval();

select setval('public.my_serial', 22, true);
select currval('public.my_serial');
select nextval('public.my_serial');
select lastval();

select setval('public.my_serial', 33, false);
select currval('public.my_serial');
select nextval('public.my_serial');
select lastval();

select currval('postgres.public.my_serial');
select nextval('postgres.public.my_serial');
select lastval();

select currval('my_serial');
select nextval('my_serial');
select lastval();

drop sequence public.my_serial;
create sequence public.my_serial start 11;

select public.my_serial.nextval;
select public.my_serial.currval;
select lastval();

select setval('public.my_serial', 12);
select public.my_serial.currval;
select public.my_serial.nextval;
select lastval();

select setval('public.my_serial', 22, true);
select public.my_serial.currval;
select public.my_serial.nextval;
select lastval();

select setval('public.my_serial', 33, false);
select public.my_serial.currval;
select public.my_serial.nextval;
select lastval();

select postgres.public.my_serial.currval;
select postgres.public.my_serial.nextval;
select lastval();

select my_serial.currval;
select my_serial.nextval;
select lastval();

drop sequence public.my_serial;
-- test using index
drop table if exists t cascade;
create table t (f1 integer not null,nc text);
alter table t add constraint t_f1_PK primary key (f1) using index;
drop table t cascade;

drop table if exists t;
create table t (f1 integer not null,nc text);
alter table t add constraint t_f1_PK primary key (f1);
drop table t cascade;

drop table if exists t;
create table t (f1 integer not null primary key using index,nc text);
drop table t cascade;

drop table if exists t;
create table t (f1 integer not null primary key,nc text);
drop table t cascade;

-- test varchar2
    -- ERROR (typmod >= 1)
    CREATE TABLE foo (a VARCHAR2(0));
    -- ERROR (number of typmods = 1)
    CREATE TABLE foo (a VARCHAR2(10, 1));
    -- OK
    CREATE TABLE foo (a VARCHAR(5000));
    -- cleanup
    DROP TABLE foo;

    -- OK
    CREATE TABLE foo (a VARCHAR2(5));
    CREATE INDEX ON foo(a);

    --
    -- test that no value longer than maxlen is allowed
    --
    -- ERROR (length > 5)
    INSERT INTO foo VALUES ('abcdef');

    -- ERROR (length > 5);
    -- VARCHAR2 does not truncate blank spaces on implicit coercion
    INSERT INTO foo VALUES ('abcde  ');

    -- OK
    INSERT INTO foo VALUES ('abcde');
    -- OK
    INSERT INTO foo VALUES ('abcdef'::VARCHAR2(5));
    -- OK
    INSERT INTO foo VALUES ('abcde  '::VARCHAR2(5));
    --OK
    INSERT INTO foo VALUES ('abc'::VARCHAR2(5));

    --
    -- test whitespace semantics on comparison
    --
    -- equal
    SELECT 'abcde   '::VARCHAR2(10) = 'abcde   '::VARCHAR2(10);
    -- not equal
    SELECT 'abcde  '::VARCHAR2(10) = 'abcde   '::VARCHAR2(10);

    -- cleanup
    DROP TABLE foo;

-- test string
set client_min_messages = 'NOTICE'; 
--  1. test string in anonymous block
declare
  var_string string(15);
begin
  var_string := 'testString';
  raise notice '%',var_string;
end;
/
--  2. test function param using string
create or replace function f_test_string_20221018(a string)
return integer
is
  res integer;
  b   string;
begin
  res := 0;
  raise notice 'look:%',a;
  return res;
end;
/

declare
  res number;
  var_string STRING(20);
begin
  var_string := 'testString';
  res := f_test_string_20221018(var_string);
end;
/

select proargtypes,prosrc from pg_proc where proname='f_test_string_20221018';
drop function if exists f_test_string_20221018;
--  3. test string typecast
declare
  a text;
  b string(20);
  c integer;
begin
  a := '12345';
  b := a;
  c := b;
  raise notice '%',b;
  raise notice '%',c;
end;
/
--  4. test string%type
declare
  a string(20);
  b a%type;
begin
  b := 'test %type';
  raise notice '%',b;
end;
/
--5.test function return type
create or replace function f_test_string_20221018(a string)
return string
is
  res string;
  b   string;
begin
  res := a;
  raise notice 'look:%',a;
  return res;
end;
/

declare
  res string(20);
  var_string STRING(20);
begin
  var_string := 'testString';
  res := f_test_string_20221018(var_string);
  raise notice 'res:%',var_string;
end;
/
drop function if exists f_test_string_20221018;

--6. test string(2) as function param
create or replace function f_test_string_20221018(a string(2))
return string
is
  res string;
  b   string;
begin
  res := a;
  raise notice 'look:%',a;
  return res;
end;
/

declare
  res string(20);
  var_string STRING(20);
begin
  var_string := 'testString';
  res := f_test_string_20221018(var_string);
  raise notice 'res:%',var_string;
end;
/
drop function if exists f_test_string_20221018;

--7. test ddl/dml
create table t_test_string_20221018(id int,name string(20));
insert into t_test_string_20221018 values(1,'haha');
drop table t_test_string_20221018;
create table t_test_string_20221018(id int,name string(20));
insert into t_test_string_20221018 values(1,'haha'::string);
insert into t_test_string_20221018 values(2,CAST('haha' as string));
drop table t_test_string_20221018;
create table t_test_string_20221018(id int, name string);
insert into t_test_string_20221018 values(1,'haha');
drop table t_test_string_20221018;
create table t_test_string_20221018(id int, name string(32767));
insert into t_test_string_20221018 values(1,'haha');
drop table t_test_string_20221018;
create table t_test_string_20221018(id int, name string(10485760));
insert into t_test_string_20221018 values(1,'haha');
drop table t_test_string_20221018;

set client_min_messages = warning; 
    -- varchar2 specified unit is a character or byte (default is byte)
    create table test_varchar2_1(c1 varchar(5), c2 varchar2(5), c3 varchar2(5 char), c4 varchar2(5 byte));
    create table test_varchar2_2(c1 varchar(5), c2 varchar2(5), c3 varchar2(5 CHAR), c4 varchar2(5 BYTE));
    create table test_varchar2_3(c1 varchar(5), c2 varchar2(5), c3 varchar2(5 CHARACTER), c4 varchar2(5 byte));
    create table test_varchar2_4(c1 varchar(5), c2 varchar2(5), c3 varchar2(5 character), c4 varchar2(5 BYTE));

    insert into test_varchar2_1 values('中国中国中', '中', '中国中国中', '中');   -- should OK (c3 maxlen is 5 char)
    insert into test_varchar2_2 values('中国中国中', '中国中国中', '中国中国中', '中');   -- should failed (c2 byte longer than maxlen 5)
    insert into test_varchar2_3 values('中国中国中', '中', '中国中国中', '中国中国中');   -- should failed (c4 byte longer than maxlen 5)
    insert into test_varchar2_4 values('中国中国中', '中', '中国中国中', 'abcde');    -- should OK (c3 maxlen is 5 char, c2/c4 maxlen is 5 byte)

    drop table test_varchar2_1, test_varchar2_2, test_varchar2_3, test_varchar2_4;

-- test nvarchar2
    --
    -- test type modifier related rules
    --
    -- ERROR (typmod >= 1)
    CREATE TABLE bar (a NVARCHAR2(0));
    -- ERROR (number of typmods = 1)
    CREATE TABLE bar (a NVARCHAR2(10, 1));

    -- OK
    CREATE TABLE bar (a VARCHAR(5000));
    CREATE INDEX ON bar(a);

    -- cleanup
    DROP TABLE bar;

    -- OK
    CREATE TABLE bar (a NVARCHAR2(5));

    --
    -- test that no value longer than maxlen is allowed
    --
    -- ERROR (length > 5)
    INSERT INTO bar VALUES ('abcdef');

    -- ERROR (length > 5);
    -- NVARCHAR2 does not truncate blank spaces on implicit coercion
    INSERT INTO bar VALUES ('abcde  ');
    -- OK
    INSERT INTO bar VALUES ('abcde');
    -- OK
    INSERT INTO bar VALUES ('abcdef'::NVARCHAR2(5));
    -- OK
    INSERT INTO bar VALUES ('abcde  '::NVARCHAR2(5));
    --OK
    INSERT INTO bar VALUES ('abc'::NVARCHAR2(5));

    --
    -- test whitespace semantics on comparison
    --
    -- equal
    SELECT 'abcde   '::NVARCHAR2(10) = 'abcde   '::NVARCHAR2(10);
    -- not equal
    SELECT 'abcde  '::NVARCHAR2(10) = 'abcde   '::NVARCHAR2(10);

    -- cleanup
    DROP TABLE bar;

-- test cast string to numeric
drop table if exists test_t;
create table test_t(id varchar2(20));

insert into test_t values('1'); 
insert into test_t values(23);
insert into test_t values(24);
insert into test_t values(25);


select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

insert into test_t values('xxx');


select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;


select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

drop table if exists test_t;

select 'xxx' || 1 as r;
select 1 || 'xxxx' as r;

drop table if exists test_t;
create table test_t (n int);
insert into test_t values(1);
insert into test_t values(2);
insert into test_t values(3);
insert into test_t values(4);
insert into test_t values(5);
insert into test_t values(6);
insert into test_t values(7);
insert into test_t values(8);
insert into test_t values(9);

select r from (SELECT '7' as r from dual UNION ALL SELECT n+1 as r FROM test_t WHERE n < 7 ) order by r;
drop table if exists test_t;

-- test nlssort
\c regression_sort
DROP TABLE IF EXISTS test_sort;
CREATE TABLE test_sort (name TEXT);
INSERT INTO test_sort VALUES ('red'), ('brown'), ('yellow'), ('Purple');
INSERT INTO test_sort VALUES ('guangdong'), ('shenzhen'), ('Tencent'), ('OpenTenBase');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'en_US.utf8');
SELECT * FROM test_sort ORDER BY NLSSORT(name, '');
SELECT set_nls_sort('invalid');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('en_US.utf8');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
INSERT INTO test_sort VALUES(NULL);
SELECT * FROM test_sort ORDER BY NLSSORT(name);

SELECT set_nls_sort('nls_sort = russian');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('nls_sortr = pt_PT.iso885915@euro');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('nls_sortr = en_US.iso885915');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sort =   ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sort =  zh_CN.gb18030 ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sort =  wa_BE.iso885915@euro ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'NLS_SORT =  tt_RU.utf8@iqtelif ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sortR =  tt_RU.utf8@iqtelif ');
SELECT * FROM test_sort ORDER BY NLSSORT(name,'NLS_SORT = SCHINESE_PINYIN_M');
SELECT * FROM test_sort ORDER BY NLSSORT(name,'NLS_SORT = SCHINESE_STROKE_M');
SELECT * FROM test_sort ORDER BY NLSSORT(name,'NLS_SORT = SCHINESE_RADICAL_M');
DROP TABLE test_sort;

\c regression_ora
SET client_encoding to default;

-- test !=- operator
drop table if exists test cascade;
create table test(tc1 int);

insert into test values(1),(-1),(null);
insert into test values(2),(-2),(null);

select * from test where tc1 != -1  order by tc1 asc;
select * from test where tc1 !=-1  order by tc1 asc;  -- error
select * from test where tc1 <> -1  order by tc1 asc;
select * from test where tc1 <>-1  order by tc1 asc;

select * from test where tc1 != -1  order by tc1 asc;
select * from test where tc1 !=-1  order by tc1 asc;
select * from test where tc1 <> -1  order by tc1 asc;
select * from test where tc1 <>-1  order by tc1 asc;

CREATE OPERATOR opentenbase_ora.!=- (
  LEFTARG   = INTEGER,
  RIGHTARG  = INTEGER,
  PROCEDURE = pg_catalog.int4div
);

select * from test where tc1 != -1  order by tc1 asc;
select * from test where tc1 !=-1  order by tc1 asc;
select * from test where tc1 <> -1  order by tc1 asc;
select * from test where tc1 <>-1  order by tc1 asc;

select * from test where tc1 != -1  order by tc1 asc;
select * from test where tc1 !=-1  order by tc1 asc;
select * from test where tc1 <> -1  order by tc1 asc;
select * from test where tc1 <>-1  order by tc1 asc;

drop operator opentenbase_ora.!=-(integer, integer);
drop operator opentenbase_ora.!=-(integer, integer);

CREATE OPERATOR opentenbase_ora.!=- (
  LEFTARG   = INTEGER,
  RIGHTARG  = INTEGER,
  PROCEDURE = pg_catalog.int4div
);

drop table if exists test cascade;


\c regression
SELECT add_months ('2008-01-31', -80640);
SELECT add_months ('2008-01-31 11:32:12', -80640);

\c regression_ora
select instr('Tech on the net', 'e') = 2;
select instr('Tech on the net', 'e', 1, 1) = 2;
select instr('Tech on the net', 'e', 1, 2) = 11;
select instr('Tech on the net', 'e', 1, 3) = 14;
select instr('Tech on the net', 'e', -3, 2) = 2;
select instr('abc', NULL) IS NULL;
select instr('abc', '') IS NULL;
select instr('', 'a') IS NULL;
select 1 = instr('abc', 'a');
select 3 = instr('abc', 'c');
select 0 = instr('abc', 'z');
select 1 = instr('abcabcabc', 'abca', 1);
select 4 = instr('abcabcabc', 'abca', 2);
select 0 = instr('abcabcabc', 'abca', 7);
select 0 = instr('abcabcabc', 'abca', 9);
select 4 = instr('abcabcabc', 'abca', -1);
select 1 = instr('abcabcabc', 'abca', -8);
select 1 = instr('abcabcabc', 'abca', -9);
select 0 = instr('abcabcabc', 'abca', -10);
select 1 = instr('abcabcabc', 'abca', 1, 1);
select 4 = instr('abcabcabc', 'abca', 1, 2);
select 0 = instr('abcabcabc', 'abca', 1, 3);
select 2 = instr('张三','三') from dual;
select 0 = instr('张三',' ') from dual;
select instr('张三','') is NULL from dual;
select instr('','三') is NULL from dual;

--test to_char
select to_char(22);
select to_char(99::smallint);
select to_char(-44444);
select to_char(1234567890123456::bigint);
select to_char(123.456::real);
select to_char(1234.5678::double precision);
select to_char(12345678901234567890::numeric);
select to_char(1234567890.12345);
select to_char('4.00'::numeric);
select to_char('4.0010'::numeric);

-- test special case of '$' in int_to_char fmt
select TO_CHAR(123,'$99,999.9') from dual;
select length(TO_CHAR(123,'$99,999.9')) from dual;
select TO_CHAR(123,'$99,9,999.9') from dual;
select length(TO_CHAR(123,'$99,9,999.9')) from dual;
select TO_CHAR(123,'99,9,9$99.9') from dual;
select length(TO_CHAR(123,'99,9,9$99.9')) from dual;
select TO_CHAR(123,'99,9,999.9$') from dual;
select length(TO_CHAR(123,'99,9,999.9$')) from dual;

-- with sign
select TO_CHAR(+123,'$99,999.9') from dual;
select length(TO_CHAR(+123,'$99,999.9')) from dual;
select TO_CHAR(-123,'$99,999.9') from dual;
select length(TO_CHAR(-123,'$99,999.9')) from dual;

-- numeric_to_char
select TO_CHAR(123.99,'$99,999.9') from dual;
select length(TO_CHAR(123.99,'$99,999.9')) from dual;
select TO_CHAR(123.99,'$99,9,999.9') from dual;
select length(TO_CHAR(123.99,'$99,9,999.9')) from dual;
select TO_CHAR(123.99,'99,9,9$99.9') from dual;
select length(TO_CHAR(123.99,'99,9,9$99.9')) from dual;
select TO_CHAR(123.99,'99,9,999.9$') from dual;
select length(TO_CHAR(123.99,'99,9,999.9$')) from dual;

-- with sign
select TO_CHAR(-123.99,'$99,999.9') from dual;
select length(TO_CHAR(-123.99,'$99,999.9')) from dual;
select TO_CHAR(+123.99,'$99,999.9') from dual;
select length(TO_CHAR(+123.99,'$99,999.9')) from dual;

---------------------------------------------------------------------------------------------------------------------------------------
--add to_char to convert the integer, float, numeric to hex digit string 
select to_char(20, 'aa') from dual;
select to_char(22221., 'xxxxxxx') from dual;
select to_char(22221., 'XXXXXXX') from dual;
select to_char(22221.4, 'xxxxxxx') from dual;
select to_char(22221.5, 'xxxxxxx') from dual;
select to_char(-22221.4, 'xxxxxxx') from dual;

select to_char(912222, 'xxxxx') from dual;
select to_char(912222, 'XXXXX') from dual;

select to_char(912222, 'xxxx') from dual;

select to_char(-912222, 'xxxxxxx') from dual;

select to_char(2e4, 'XXXx') from dual;
select to_char(2e-4, 'XXXx') from dual;
select to_char(2e0, 'XXXx') from dual;
select to_char(-2e0, 'XXXx') from dual;

select to_char(912222, 'XxxxxxxxxxxxxxxxxxxxxxxxxxXxxxxxxxxxxxxxXxxxxxxxxxxxxxxxxxxxxxX') from dual;
select to_char(912222, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') from dual;
select to_char(912222, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') from dual;

create table tr_tbl_to_char(f1 integer, f2 float, f3 double precision, f4 numeric(10,2));
insert into tr_tbl_to_char values(255, 255.4, 65535.9999999, -32765.89);
insert into tr_tbl_to_char values(254, 253.4, 65534.9999999, 32765.89);
insert into tr_tbl_to_char values(-254, 253.4, 65534.9999999, 32765.89);
select to_char(f1, 'XXXX'), to_char(f2, 'xxxx'), to_char(f3, 'xxxX'), to_char(f4, 'XXXXX') from tr_tbl_to_char order by f1;





------------------------------------------------------------------------------------------------------------------------------

--test to_number
SELECT to_number('-34,338,492', '99G999G999') from dual;
SELECT to_number('-34,338,492.654,878', '99G999G999D999G999') from dual;
SELECT to_number('<564646.654564>', '999999.999999PR') from dual;
SELECT to_number('0.00001-', '9.999999S') from dual;
SELECT to_number('5.01-', 'FM9.999999S') from dual;
SELECT to_number('5.01-', 'FM9.999999MI') from dual;
SELECT to_number('5 4 4 4 4 8 . 7 8', '9 9 9 9 9 9 . 9 9') from dual;
SELECT to_number('.01', 'FM9.99') from dual;
SELECT to_number('.0', '99999999.99999999') from dual;
SELECT to_number('0', '99.99') from dual;
SELECT to_number('.-01', 'S99.99') from dual;
SELECT to_number('.01-', '99.99S') from dual;
SELECT to_number(' . 0 1-', ' 9 9 . 9 9 S') from dual;

SELECT to_number('-34338492') from dual;
SELECT to_number('-34338492.654878') from dual;
SELECT to_number('564646.654564') from dual;
SELECT to_number('0.00001') from dual;
SELECT to_number('5.01') from dual;
SELECT to_number('.01') from dual;
SELECT to_number('.0') from dual;
SELECT to_number('0') from dual;
SELECT to_number('01') from dual;

select to_number(123.09e34::float4) from dual;
select to_number(1234.094e23::float8) from dual;

SELECT to_number('123'::text);
SELECT to_number('123.456'::text);
SELECT to_number(123);
SELECT to_number(123::smallint);
SELECT to_number(123::int);
SELECT to_number(123::bigint);
SELECT to_number(123::numeric);
SELECT to_number(123.456);

-- test to_multi_byte
-- SELECT to_multi_byte('123$test') from dual;

-- SELECT octet_length('abc') from dual;
-- SELECT octet_length(to_multi_byte('abc')) from dual;

-- testto_single_byte
-- SELECT to_single_byte('123$test') from dual;
-- SELECT to_single_byte('１２３＄ｔｅｓｔ') from dual;

-- SELECT octet_length('ａｂｃ');
-- SELECT octet_length(to_single_byte('ａｂｃ')) from dual;

-- test sinh cosh tanh
SELECT sinh(1.570796), cosh(1.570796), tanh(4);
SELECT sinh(1.570796::numeric), cosh(1.570796::numeric), tanh(4::numeric);

-- Test nanvl
SELECT nanvl(12345, 1), nanvl('NaN', 1) FROM DUAL;
SELECT nanvl(12345::float4, 1), nanvl('NaN'::float4, 1) FROM DUAL;
SELECT nanvl(12345::float8, 1), nanvl('NaN'::float8, 1) FROM DUAL;
SELECT nanvl(12345::numeric, 1), nanvl('NaN'::numeric, 1) FROM DUAL;
SELECT nanvl(12345, '1'::varchar), nanvl('NaN', 1::varchar) FROM DUAL;
SELECT nanvl(12345::float4, '1'::varchar), nanvl('NaN'::float4, '1'::varchar) FROM DUAL;
SELECT nanvl(12345::float8, '1'::varchar), nanvl('NaN'::float8, '1'::varchar) FROM DUAL;
SELECT nanvl(12345::numeric, '1'::varchar), nanvl('NaN'::numeric, '1'::varchar) FROM DUAL;
SELECT nanvl(12345, '1'::char), nanvl('NaN', 1::char) FROM DUAL;
SELECT nanvl(12345::float4, '1'::char), nanvl('NaN'::float4, '1'::char) FROM DUAL;
SELECT nanvl(12345::float8, '1'::char), nanvl('NaN'::float8, '1'::char) FROM DUAL;
SELECT nanvl(12345::numeric, '1'::char), nanvl('NaN'::numeric, '1'::char) FROM DUAL;
SELECT nanvl(binary_float_nan, 1), nanvl('NaN', 1) FROM DUAL;
SELECT nanvl(binary_float_nan, 1), nanvl('NaN'::float4, 1) FROM DUAL;
SELECT nanvl(binary_float_nan, 1), nanvl('NaN'::float8, 1) FROM DUAL;
SELECT nanvl(binary_float_nan, 1), nanvl('NaN'::numeric, 1) FROM DUAL;
SELECT nanvl(binary_float_nan, '1'::varchar), nanvl('NaN', 1::varchar) FROM DUAL;
SELECT nanvl(binary_float_nan, '1'::varchar), nanvl('NaN'::float4, '1'::varchar) FROM DUAL;
SELECT nanvl(binary_float_nan, '1'::varchar), nanvl('NaN'::float8, '1'::varchar) FROM DUAL;
SELECT nanvl(binary_float_nan, '1'::varchar), nanvl('NaN'::numeric, '1'::varchar) FROM DUAL;
SELECT nanvl(binary_float_nan, '1'::char), nanvl('NaN', 1::char) FROM DUAL;
SELECT nanvl(binary_float_nan, '1'::char), nanvl('NaN'::float4, '1'::char) FROM DUAL;
SELECT nanvl(binary_float_nan, '1'::char), nanvl('NaN'::float8, '1'::char) FROM DUAL;
SELECT nanvl(binary_float_nan, '1'::char), nanvl('NaN'::numeric, '1'::char) FROM DUAL;
SELECT nanvl(binary_double_nan, 1), nanvl('NaN', 1) FROM DUAL;
SELECT nanvl(binary_double_nan, 1), nanvl('NaN'::float4, 1) FROM DUAL;
SELECT nanvl(binary_double_nan, 1), nanvl('NaN'::float8, 1) FROM DUAL;
SELECT nanvl(binary_double_nan, 1), nanvl('NaN'::numeric, 1) FROM DUAL;
SELECT nanvl(binary_double_nan, '1'::varchar), nanvl('NaN', 1::varchar) FROM DUAL;
SELECT nanvl(binary_double_nan, '1'::varchar), nanvl('NaN'::float4, '1'::varchar) FROM DUAL;
SELECT nanvl(binary_double_nan, '1'::varchar), nanvl('NaN'::float8, '1'::varchar) FROM DUAL;
SELECT nanvl(binary_double_nan, '1'::varchar), nanvl('NaN'::numeric, '1'::varchar) FROM DUAL;
SELECT nanvl(binary_double_nan, '1'::char), nanvl('NaN', 1::char) FROM DUAL;
SELECT nanvl(binary_double_nan, '1'::char), nanvl('NaN'::float4, '1'::char) FROM DUAL;
SELECT nanvl(binary_double_nan, '1'::char), nanvl('NaN'::float8, '1'::char) FROM DUAL;
SELECT nanvl(binary_double_nan, '1'::char), nanvl('NaN'::numeric, '1'::char) FROM DUAL;
SELECT nanvl(NULL, NULL) from dual;
SELECT nanvl(NULL, 1), nanvl(binary_double_nan, NULL) from dual;

-- Test sign
select sign(binary_float_nan), sign(binary_double_nan) from dual;

-- Test atan2
select atan2(-0, -0) from dual;

-- Test nvl
SELECT nvl('A'::text, 'B') FROM DUAL;
SELECT nvl(NULL::text, 'B') FROM DUAL;
SELECT nvl(NULL::text, NULL) FROM DUAL;
SELECT nvl(1, 2) FROM DUAL;
SELECT nvl(NULL, 2) FROM DUAL;

-- Test nvl2
SELECT nvl2('A'::text, 'B', 'C') FROM DUAL;
SELECT nvl2(NULL::text, 'B', 'C') FROM DUAL;
SELECT nvl2('A'::text, NULL, 'C') FROM DUAL;
SELECT nvl2(NULL::text, 'B', NULL) FROM DUAL;
SELECT nvl2(1, 2, 3) FROM DUAL;
SELECT nvl2(NULL, 2, 3) FROM DUAL;
SELECT nvl2(' ', 1, 2) FROM DUAL;
SELECT nvl2('', 1, 2) FROM DUAL;

-- Test lnnvl
SELECT lnnvl(true) FROM DUAL;
SELECT lnnvl(false) FROM DUAL;
SELECT lnnvl(NULL) FROM DUAL;

-- Test NULL
SELECT dump('') FROM DUAL;
SELECT nvl('',3) FROM DUAL;
SELECT nvl2('','1','2') FROM DUAL ;
SELECT 1 FROM DUAL WHERE '' is NULL;
SELECT NULL||'opentenbase' FROM DUAL;
SELECT NULL+1 FROM DUAL;

DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null(f1 int,f2 varchar2(10));
INSERT INTO t_null VALUES(1,NULL);
INSERT INTO t_null VALUES(2,'');
SELECT * FROM t_null WHERE f2 is NULL;
SELECT * FROM t_null WHERE f2 ='' ;

INSERT INTO t_null VALUES(3,'1');
INSERT INTO t_null VALUES(4,'2');
SELECT * FROM t_null ORDER BY f2;
DROP TABLE t_null;

-- Test DUMP
SELECT DUMP('Yellow dog'::text) ~ E'^Typ=25 Len=(\\d+): \\d+(,\\d+)*$' AS t;
SELECT DUMP('Yellow dog'::text, 10) ~ E'^Typ=25 Len=(\\d+): \\d+(,\\d+)*$' AS t;
SELECT DUMP('Yellow dog'::text, 17) ~ E'^Typ=25 Len=(\\d+): .(,.)*$' AS t;
SELECT DUMP(10::int2) ~ E'^Typ=21 Len=2: \\d+(,\\d+){1}$' AS t;
SELECT DUMP(10::int4) ~ E'^Typ=23 Len=4: \\d+(,\\d+){3}$' AS t;
SELECT DUMP(10::int8) ~ E'^Typ=20 Len=8: \\d+(,\\d+){7}$' AS t;
SELECT DUMP(10.23::float4) ~ E'^Typ=700 Len=4: \\d+(,\\d+){3}$' AS t;
SELECT DUMP(10.23::float8) ~ E'^Typ=701 Len=8: \\d+(,\\d+){7}$' AS t;
SELECT DUMP(10.23::numeric) ~ E'^Typ=1700 Len=(\\d+): \\d+(,\\d+)*$' AS t;
SELECT DUMP('2008-10-10'::date) ~ E'^Typ=8774 Len=8: \\d+(,\\d+){7}$' AS t;
SELECT DUMP('2008-10-10'::timestamp) ~ E'^Typ=8776 Len=12: \\d+(,\\d+){11}$' AS t;
SELECT DUMP('2009-10-10'::timestamp) ~ E'^Typ=8776 Len=12: \\d+(,\\d+){11}$' AS t;
SELECT DUMP('Yellow dog'::text, 1010) ~ E'^Typ=25 Len=(\\d+) CharacterSet=(\\w+): \\d+(,\\d+)*$' AS t;
SELECT DUMP('Yellow dog'::text, 1017) ~ E'^Typ=25 Len=(\\d+) CharacterSet=(\\w+): .(,.)*$' AS t;
SELECT DUMP('2009-10-10'::timestamp, 10, 1, 5) ~ E'^Typ=8776 Len=12: \\d+(,\\d+){4}$' AS t;
SELECT DUMP('2009-10-10'::timestamp, 10, 0, 0) ~ E'^Typ=8776 Len=12: \\d+(,\\d+){11}$' AS t;
SELECT DUMP('2009-10-10'::timestamp, 16, 1, 5) ~ E'^Typ=8776 Len=12: [0-9a-fA-F]+(,[0-9a-fA-F]+){4}$' AS t;
SELECT DUMP('2009-10-10'::timestamp, 8, 1, 5) ~ E'^Typ=8776 Len=12: [0-7]+(,[0-7]+){4}$' AS t;


-- test substr
select substr('This is a test', 6, 2) = 'is';
select substr('This is a test', 6) =  'is a test';
select substr('TechOnTheNet', 1, 4) =  'Tech';
select substr('TechOnTheNet', -3, 3) =  'Net';
select substr('TechOnTheNet', -6, 3) =  'The';
select substr('TechOnTheNet', -8, 2) =  'On';
select substr('TechOnTheNet', -8, 0) =  '';
select substr('TechOnTheNet', -8, -1) =  '';
select substr(1234567,3.6::smallint)='4567';
select substr(1234567,3.6::int)='4567';
select substr(1234567,3.6::bigint)='4567';
select substr(1234567,3.6::numeric)='34567';
select substr(1234567,-1)='7';
select substr(1234567,3.6::smallint,2.6)='45';
select substr(1234567,3.6::smallint,2.6::smallint)='456';
select substr(1234567,3.6::smallint,2.6::int)='456';
select substr(1234567,3.6::smallint,2.6::bigint)='456';
select substr(1234567,3.6::smallint,2.6::numeric)='45';
select substr(1234567,3.6::int,2.6::smallint)='456';
select substr(1234567,3.6::int,2.6::int)='456';
select substr(1234567,3.6::int,2.6::bigint)='456';
select substr(1234567,3.6::int,2.6::numeric)='45';
select substr(1234567,3.6::bigint,2.6::smallint)='456';
select substr(1234567,3.6::bigint,2.6::int)='456';
select substr(1234567,3.6::bigint,2.6::bigint)='456';
select substr(1234567,3.6::bigint,2.6::numeric)='45';
select substr(1234567,3.6::numeric,2.6::smallint)='345';
select substr(1234567,3.6::numeric,2.6::int)='345';
select substr(1234567,3.6::numeric,2.6::bigint)='345';
select substr(1234567,3.6::numeric,2.6::numeric)='34';
select substr('abcdef'::varchar,3.6::smallint)='def';
select substr('abcdef'::varchar,3.6::int)='def';
select substr('abcdef'::varchar,3.6::bigint)='def';
select substr('abcdef'::varchar,3.6::numeric)='cdef';
select substr('abcdef'::varchar,3.5::int,3.5::int)='def';
select substr('abcdef'::varchar,3.5::numeric,3.5::numeric)='cde';
select substr('abcdef'::varchar,3.5::numeric,3.5::int)='cdef';

-- test lengthb
select length('opentenbase_ora'),lengthB('opentenbase_ora') from dual;

-- test strposb
select strposb('abc', '') from dual;
select strposb('abc', 'a') from dual;
select strposb('abc', 'c') from dual;
select strposb('abc', 'z') from dual;
select strposb('abcabcabc', 'abca') from dual;

--test decode
select decode(1, 1, 100, 2, 200);
select decode(2, 1, 100, 2, 200);
select decode(3, 1, 100, 2, 200);
select decode(3, 1, 100, 2, 200, 300);
select decode(NULL, 1, 100, NULL, 200, 300);
select decode('1'::text, '1', 100, '2', 200);
select decode(2, 1, 'ABC', 2, 'DEF');



-- Test sysdate - (column)::sysdate
CREATE TABLE eh_etl_log_main (
      schedule_id character varying(20) not null,
      execute_period character varying(8)  ,
      start_time timestamp(0) without time zone,
      end_time  timestamp(0) without time zone,
      time_consuming numeric(14,2),
      execute_status character varying(10));
create or replace procedure insert_into_tableA_prococedure ( )as
$$
DECLARE  
v_schedule_id text;
begin 
v_schedule_id := to_char(sysdate,'yyyymmddhh24miss');
insert into eh_etl_log_main values (
                v_schedule_id,
                20211031,
                sysdate,
                sysdate,
                0,
                'NONE');
update eh_etl_log_main SET end_time = sysdate,time_consuming = (((sysdate - start_time::date)*24*60)::numeric),execute_status = 'ERROR' where schedule_id= v_schedule_id;
end;
$$language default_plsql;

call insert_into_tableA_prococedure();
call insert_into_tableA_prococedure();
call insert_into_tableA_prococedure();
call insert_into_tableA_prococedure();
call insert_into_tableA_prococedure();
drop table eh_etl_log_main;
drop procedure insert_into_tableA_prococedure;

-- For type 'bpchar'
select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar);
select DECODE('c'::bpchar, 'a'::bpchar,'postgres'::bpchar);
select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'default value'::bpchar);
select DECODE('c', 'a'::bpchar,'postgres'::bpchar,'default value'::bpchar);

select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar);
select DECODE('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar);
select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar,'default value'::bpchar);
select DECODE('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar,'default value'::bpchar);

select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar);
select DECODE('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar);
select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar,'default value'::bpchar);
select DECODE('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar,'default value'::bpchar);

select DECODE(NULL, 'a'::bpchar, 'postgres'::bpchar, NULL,'database'::bpchar);
select DECODE(NULL, 'a'::bpchar, 'postgres'::bpchar, 'b'::bpchar,'database'::bpchar);
select DECODE(NULL, 'a'::bpchar, 'postgres'::bpchar, NULL,'database'::bpchar,'default value'::bpchar);
select DECODE(NULL, 'a'::bpchar, 'postgres'::bpchar, 'b'::bpchar,'database'::bpchar,'default value'::bpchar);

-- For type 'bigint'
select DECODE(2147483651::bigint, 2147483650::bigint,2147483650::bigint);
select DECODE(2147483653::bigint, 2147483651::bigint,2147483650::bigint);
select DECODE(2147483653::bigint, 2147483651::bigint,2147483650::bigint,9999999999::bigint);
select DECODE(2147483653::bigint, 2147483651::bigint,2147483650::bigint,9999999999::bigint);

select DECODE(2147483651::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint);
select DECODE(2147483654::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint);
select DECODE(2147483651::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint,9999999999::bigint);
select DECODE(2147483654::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint,9999999999::bigint);

select DECODE(2147483651::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint);
select DECODE(2147483654::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint);
select DECODE(2147483651::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint,9999999999::bigint);
select DECODE(2147483654::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint,9999999999::bigint);

select DECODE(NULL, 2147483651::bigint, 2147483650::bigint, NULL,2147483651::bigint);
select DECODE(NULL, 2147483651::bigint, 2147483650::bigint, 2147483652::bigint,2147483651::bigint);
select DECODE(NULL, 2147483651::bigint, 2147483650::bigint, NULL,2147483651::bigint,9999999999::bigint);
select DECODE(NULL, 2147483651::bigint, 2147483650::bigint, 2147483652::bigint,2147483651::bigint,9999999999::bigint);

-- For type 'numeric'
select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4));
select DECODE(12.003::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4));
select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),999999.9999::numeric(10,4));
select DECODE(12.003::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),999999.9999::numeric(10,4));

select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4));
select DECODE(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4));
select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4),999999.9999::numeric(10,4));
select DECODE(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4),999999.9999::numeric(10,4));

select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4));
select DECODE(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4));
select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4),999999.9999::numeric(10,4));
select DECODE(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4),999999.9999::numeric(10,4));

select DECODE(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), NULL,214748.3651::numeric(10,4));
select DECODE(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), 12.002::numeric(5,3),214748.3651::numeric(10,4));
select DECODE(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), NULL,214748.3651::numeric(10,4),999999.9999::numeric(10,4));
select DECODE(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), 12.002::numeric(5,3),214748.3651::numeric(10,4),999999.9999::numeric(10,4));

--For type 'date'
select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date);
select DECODE('2020-01-03'::date, '2020-01-01'::date,'2012-12-20'::date);
select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2012-12-21'::date);
select DECODE('2020-01-03'::date, '2020-01-01'::date,'2012-12-20'::date,'2012-12-21'::date);

select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date);
select DECODE('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date);
select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date,'2012-12-31'::date);
select DECODE('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date,'2012-12-31'::date);

select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date);
select DECODE('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date);
select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date,'2013-01-01'::date);
select DECODE('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date,'2013-01-01'::date);

select DECODE(NULL, '2020-01-01'::date, '2012-12-20'::date, NULL,'2012-12-21'::date);
select DECODE(NULL, '2020-01-01'::date, '2012-12-20'::date, '2020-01-02'::date,'2012-12-21'::date);
select DECODE(NULL, '2020-01-01'::date, '2012-12-20'::date, NULL,'2012-12-21'::date,'2012-12-31'::date);
select DECODE(NULL, '2020-01-01'::date, '2012-12-20'::date, '2020-01-02'::date,'2012-12-21'::date,'2012-12-31'::date);

-- For type 'time'
select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time);
select DECODE('01:00:03'::time, '01:00:01'::time,'09:00:00'::time);
select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'00:00:00'::time);
select DECODE('01:00:03'::time, '01:00:01'::time,'09:00:00'::time,'00:00:00'::time);

select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time);
select DECODE('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time);
select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time,'00:00:00'::time);
select DECODE('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:01'::time,'12:00:00'::time,'00:00:00'::time);

select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time);
select DECODE('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time);
select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time,'00:00:00'::time);
select DECODE('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time,'00:00:00'::time);

select DECODE(NULL, '01:00:01'::time, '09:00:00'::time, NULL,'12:00:00'::time);
select DECODE(NULL, '01:00:01'::time, '09:00:00'::time, '01:00:02'::time,'12:00:00'::time);
select DECODE(NULL, '01:00:01'::time, '09:00:00'::time, NULL,'12:00:00'::time,'00:00:00'::time);
select DECODE(NULL, '01:00:01'::time, '09:00:00'::time, '01:00:02'::time,'12:00:00'::time,'00:00:00'::time);

-- For type 'timestamp'
select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp);
select DECODE('2020-01-03 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp);
select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select DECODE('2020-01-03 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp);
select DECODE('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp);
select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select DECODE('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp);
select DECODE('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp);
select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select DECODE('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

select DECODE(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, NULL,'2012-12-20 12:00:00'::timestamp);
select DECODE(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, '2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp);
select DECODE(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, NULL,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select DECODE(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, '2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

-- For type 'timestamptz'
select DECODE('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz);
select DECODE('2020-01-03 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz);
select DECODE('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);
select DECODE('2020-01-03 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);

select DECODE('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz);
select DECODE('2020-01-04 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz);
select DECODE('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);
select DECODE('2020-01-04 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);

select DECODE('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz, '2020-01-03 01:00:01-08'::timestamptz, '2012-12-20 15:00:00-08'::timestamptz);
select DECODE('2020-01-04 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz, '2020-01-03 01:00:01-08'::timestamptz, '2012-12-20 15:00:00-08'::timestamptz);
select DECODE('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz, '2020-01-03 01:00:01-08'::timestamptz, '2012-12-20 15:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);
select DECODE(4, 1,'2012-12-20 09:00:00-08'::timestamptz,2,'2012-12-20 12:00:00-08'::timestamptz, 3, '2012-12-20 15:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);

select DECODE(NULL, '2020-01-01 01:00:01-08'::timestamptz, '2012-12-20 09:00:00-08'::timestamptz, NULL,'2012-12-20 12:00:00-08'::timestamptz);
select DECODE(NULL, '2020-01-01 01:00:01-08'::timestamptz, '2012-12-20 09:00:00-08'::timestamptz, '2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz);
select DECODE(NULL, '2020-01-01 01:00:01-08'::timestamptz, '2012-12-20 09:00:00-08'::timestamptz, NULL,'2012-12-20 12:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);
select DECODE(NULL, '2020-01-01 01:00:01-08'::timestamptz, '2012-12-20 09:00:00-08'::timestamptz, '2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);

select DECODE(1, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(2, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(19, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(20, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(21, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(22, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(23, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(20, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');
select DECODE(21, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');
select DECODE(22, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');
select DECODE(23, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');

select DECODE(1.0::numeric,2::bigint,3::bigint,10::numeric);
select DECODE(1.0::numeric,2::bigint,3::bigint,4::bigint,5::bigint,1::bigint,10::bigint);
select DECODE(1.0::numeric,2::bigint,'3'::text,1::bigint,'10'::text);
select DECODE(1::bigint,2::integer,3::integer,10.0::double precision);
select DECODE(1::bigint,2::bigint,3::bigint,10.0::double precision);
select DECODE(1::bigint,2::integer,3::integer,1::integer,10.0::integer);
select DECODE(1::bigint,2::integer,3::integer,10::integer);
select DECODE(1.0::numeric,2::integer,'3'::text,10::numeric);
SELECT DECODE(null::text, 1, 'a', null, 'b', 'default');
SELECT DECODE(1, null::text, 'a', 2, 'b', 1, 'c', 'default');
WITH v_tmp AS
         (SELECT NULL col1, 'x' col2 FROM dual)
SELECT DECODE(tan(null),col1, 'a', 'default')
FROM v_tmp;
WITH v_tmp AS
         (SELECT NULL col1, 'x' col2 FROM dual)
SELECT DECODE(col1,'null', 'a', 'b')
FROM v_tmp;
WITH v_tmp AS
         (SELECT NULL col1, 'x' col2 FROM dual)
SELECT DECODE(null,col1, 'a', 'default')
FROM v_tmp;
WITH v_tmp AS
         (SELECT NULL col1, 'x' col2 FROM dual)
SELECT DECODE(1, col1,'a', 'default')
FROM v_tmp;

-- pre-process constant expression.
drop table if exists t2;
create table t2(f1 int,f2 int);
insert into t2 values(1,0);
select DECODE(f2,0,0,12/f2*100) from t2;
select DECODE(f2,0,0,12/0*100) from t2;
select DECODE(f2,0,0,12/f2*100,0) from t2;
select DECODE(f2,0,0,12/0*100,0) from t2;
drop table if exists t2;

-- complicate case
drop table if exists t_student;
create table t_student(id int, name varchar2(20), age int, sex int);
insert into t_student values(1, '张三', 15, 1);
insert into t_student values(2, '李四', 20, 3);
insert into t_student values(3, '王五', 19, 2);
insert into t_student values(4, '李六', 30, 1);
select count(*) from t_student order by 1;    --should pass
select count(*) from t_student order by 2;    --should error
select count(*) from t_student order by id1;  --should error
insert into t_student select * from t_student where id < 3;
select distinct count(1) over(partition by name) from t_student order by 1;
select distinct count(1) over(partition by name) from t_student order by 1 desc;
drop table t_student;

\c regression
drop table if exists t_student;
create table t_student(id int, name varchar(20), age int, sex int);
insert into t_student values(1, '张三', 15, 1);
insert into t_student values(2, '李四', 20, 3);
insert into t_student values(3, '王五', 19, 2);
insert into t_student values(4, '李六', 30, 1);
create or replace function errorout() returns int as
$$
begin
    return 1/0;
end; 
$$ language default_plsql;
select count(*) from t_student order by errorout(); --function should not be executed
drop function errorout;
drop table t_student;

\c regression_ora
-- test lpad
SELECT '|' || lpad('X123bcd'::char(8), 10) || '|';
SELECT '|' || lpad('X123bcd'::char(8),  5) || '|';
SELECT '|' || lpad('X123bcd'::char(8), 1) || '|';

SELECT '|' || lpad('X123bcd'::char(5), 10, '321X'::char(3)) || '|';
SELECT '|' || lpad('X123bcd'::char(5),  5, '321X'::char(3)) || '|';

SELECT '|' || lpad('X123bcd'::char(5), 10, '321X'::text) || '|';
SELECT '|' || lpad('X123bcd'::char(5), 10, '321X'::varchar2(5)) || '|';
SELECT '|' || lpad('X123bcd'::char(5), 10, '321X'::nvarchar2(3)) || '|';

SELECT '|' || lpad('X123bcd'::text, 10, '321X'::char(3)) || '|';
SELECT '|' || lpad('X123bcd'::text,  5, '321X'::char(3)) || '|';

SELECT '|' || lpad('X123bcd'::varchar2(5), 10, '321X'::char(3)) || '|';
SELECT '|' || lpad('X123bcd'::varchar2(5),  5, '321X'::char(3)) || '|';

SELECT '|' || lpad('X123bcd'::nvarchar2(5), 10, '321X'::char(3)) || '|';
SELECT '|' || lpad('X123bcd'::nvarchar2(5),  5, '321X'::char(3)) || '|';

SELECT '|' || lpad('X123bcd'::text, 10) || '|';
SELECT '|' || lpad('X123bcd'::text,  5) || '|';

SELECT '|' || lpad('X123bcd'::varchar2(10), 10) || '|';
SELECT '|' || lpad('X123bcd'::varchar2(10), 5) || '|';

SELECT '|' || lpad('X123bcd'::nvarchar2(10), 10) || '|';
SELECT '|' || lpad('X123bcd'::nvarchar2(10), 5) || '|';

SELECT '|' || lpad('X123bcd'::text, 10, '321X'::text) || '|';
SELECT '|' || lpad('X123bcd'::text, 10, '321X'::varchar2(5)) || '|';
SELECT '|' || lpad('X123bcd'::text, 10, '321X'::nvarchar2(3)) || '|';

SELECT '|' || lpad('X123bcd'::varchar2(5), 10, '321X'::text) || '|';
SELECT '|' || lpad('X123bcd'::varchar2(5), 10, '321X'::varchar2(5)) || '|';
SELECT '|' || lpad('X123bcd'::varchar2(5), 10, '321X'::nvarchar2(5)) || '|';

SELECT '|' || lpad('X123bcd'::nvarchar2(5), 10, '321X'::text) || '|';
SELECT '|' || lpad('X123bcd'::nvarchar2(5), 10, '321X'::varchar2(5)) || '|';
SELECT '|' || lpad('X123bcd'::nvarchar2(5), 10, '321X'::nvarchar2(5)) || '|';

-- test lpad/rpad for multibyte character.
SELECT '甲骨文' ORA_STR, LENGTHB('甲骨文') ORA_STR_LENGTH,
       LPAD('甲骨文', 10, '$') ORA_LPAD_STR,
       LENGTHB(LPAD('甲骨文', 10, '$')) ORA_LPAD_STR_LENGTH
FROM dual;

SELECT '甲骨文' ORA_STR, LENGTHB('甲骨文') ORA_STR_LENGTH,
       RPAD('甲骨文', 10, '$') ORA_RPAD_STR,
       LENGTHB(RPAD('甲骨文', 10, '$')) ORA_RPAD_STR_LENGTH
FROM dual;

-- test lpad/rpad data type
SELECT lpad('0ab', 10, '1cd') from dual;
SELECT lpad(0, 10, 1) from dual;
SELECT lpad(0, 10, '1') from dual;
SELECT lpad('0', 10, 1) from dual;
SELECT lpad('1', 10) from dual;
SELECT lpad(1, 10) from dual;


-- test lpad/rpad/instr. string/numeric input fot int4 params.
SELECT lpad('hi', '5'::text, 'xy');
SELECT lpad('hi', '5'::varchar, 'xy');
SELECT lpad('hi', 5.12, 'xy') from dual;
SELECT lpad('hi', 5.89, 'xy') from dual;
SELECT lpad('hi', '-5a', 'xy');

SELECT rpad('hi', '5'::text, 'xy');
SELECT rpad('hi', '5'::varchar, 'xy');
SELECT rpad('hi', 5.12, 'xy');
SELECT rpad('hi', 5.89, 'xy') from dual;
SELECT rpad('hi', '-5a', 'xy');

select instr('Tech on the net', 'e', '1', '1') = 2;
select instr('Tech on the net', 'e', '1'::text, '2'::text) = 11;
select instr('Tech on the net', 'e', 1.12, 2.65) = 11;
select instr('Tech on the net', 'e', 1.12, 3.12) = 14;
select instr('Tech on the net', 'e', '-3'::varchar, '2'::varchar) = 2;
select instr('Tech on the net', 'e', '1', '4a') from dual;

--test lpad/rpad, parm2 <= 0
select nvl(rpad('你好,china', -1, 'acd'), '-1') from dual;
select nvl(lpad('你好, china', -1, 'acd' ), '- 1') from dual;

--test regexp_replace function
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,2) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,'1') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,to_number(2)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,2.1::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,to_char(1)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,to_char(1)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,power(1,1)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',4) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',5) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1000000) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@','5') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',5.5::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',to_char(5)) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',power(1,1)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,'i') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,'c') output  from dual;
SELECT regexp_replace('abc1
def2', '[[:digit:]].d','@',1,1,'n') output  from dual;
SELECT regexp_replace('abc1
def2', '[[:digit:]].d','#',1,1,'xic') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]] d','@',1,1,'x') output  from dual;
select regexp_replace('abcxxx#%
adfbc','^a','@',1,2,'n') from dual;
select regexp_replace('abcxxx#%
adfbc','^a','@',1,2,'c') from dual;
SELECT regexp_replace('', '', '1', 1, 0)
regexp_replace FROM DUAL;
SELECT opentenbase_ora.regexp_replace('', '', '', 1, 0) regexp_replace FROM DUAL;
select opentenbase_ora.REGEXP_REPLACE('i,aaaaa,bbi,ccc', ',', '') from dual;
select opentenbase_ora.REGEXP_REPLACE('i,aaaaa,bbi,ccc', ',', null) from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,'') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,null) output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]','@',1,'') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@', null, 1) output  from dual;
select opentenbase_ora.regexp_replace('abcxxx', 'xx') from dual;
select opentenbase_ora.regexp_replace('abcxxx', '') from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]','@', '') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@', null) output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@','', 1,'x') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@',3, null,'x') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@',3, '','x') output  from dual;

-- fix customer issue:877165291
\c regression
select regexp_replace('aaaaaaaaa', '[^0-9]',',') from dual;
select regexp_replace('aaaaaaaaa', '[^0-9]','') from dual;
select regexp_replace('aaaaaaaaa', null,',') from dual;
select regexp_replace('aaaaaaaaa', null, null) from dual;
select regexp_replace(null, null, null) from dual;
select regexp_replace(null, 'a', null) from dual;
select regexp_replace(null, 'a', 'a') from dual;

\c regression_ora
select regexp_replace('aaaaaaaaa', '[^0-9]',',') from dual;
select regexp_replace('aaaaaaaaa', '[^0-9]','') from dual;
select regexp_replace('aaaaaaaaa', null,',') from dual;
select regexp_replace('aaaaaaaaa', null, null) from dual;
select regexp_replace(null, null, null) from dual;
select regexp_replace(null, 'a', null) from dual;
select regexp_replace(null, 'a', 'a') from dual;

--test regexp_count function
select regexp_count('abcdfbc','Bc',1,'i') from dual;
select regexp_count('abcdfBc','Bc',1,'c') from dual;
select regexp_count('ab
cdfbc','b.c',1,'n') from dual;
select regexp_count('ab
cdfbc','b.c',1,'i') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'m') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'i') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'n') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'x') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'c') from dual;
select regexp_count('abcvvbcvvb c','b c',1,'x') from dual;
select regexp_count('abcvvbcvvb c','b c',1,'n') from dual;
select regexp_count('abcvvbcvvBC','bc',1,'ic') from dual;
select regexp_count('abcvvbcvvBC','bc',1,'ci') from dual;
select regexp_count('abcvvbcvvBC','b c',1,'ix') from dual;
select regexp_count('abcvvb
cvvB
C','b.c',1,'in') from dual;
select regexp_count('abcvvb cvvB C','b c') from dual;
select regexp_count('abacvvb
cvvB C','b.c') from dual;
select regexp_count('abc
abc','bc?') from dual;
select regexp_count('abcvvbcvvbc','bc',2.9::int,'c') from dual;
select regexp_count('abcvvbcvvbc','bc',exp(2)::int,'c') from dual;
select regexp_count('abcvvbcvvbc','bc','1','c') from dual;
select regexp_count('abcvvbcvvbc','bc',-1,'c') from dual;
select regexp_count('abcvvbcvvbc','bc',1000000,'c') from dual;
select regexp_count('12345','123',1) from dual;
-- TODO: enable this case after opentenbase_ora datetime functions are merged
select regexp_count(to_date('2016-01-31', 'yyyy-mm-dd'), '31', 1) from dual;
select regexp_count(to_timestamp('2016-01-31', 'yyyy-mm-dd'), '31', 1) from dual;
select regexp_count('abcvvbcvvbc','bc',2.1::int,'c') from dual;
select regexp_count(null,'',1,'i') from dual;
select regexp_count('','',1,'i') from dual;

--test regexp_substr function
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,2::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1::bigint,'1') output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,to_number(2)::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,2.1::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,to_char(1)::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,power(2,1)::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',4::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',5::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1000000) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]','5'::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',5.5::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',to_char(5)::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',power(5,1)::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]D',1,1,'i') output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]D',1,1,'c') output  from dual;
SELECT regexp_substr('abc1
def2', '[[:digit:]].d',1,1,'n') output  from dual;
SELECT regexp_substr('abc1
def2', '[[:digit:]].d',1,1,'i') output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]] d',1,1,'x') output  from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'m') from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'n') from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'i') from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'x') from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'c') from dual;
SELECT REGEXP_SUBSTR('123', '12', 1, 1)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 4)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 1)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 9::bigint)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 0)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 4.5::bigint) FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', to_char(4)::int)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i',power(2,2)::bigint)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', '4')  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890abcdefg', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)(a)(b)', 1, 1, 'i', 10::bigint)  FROM DUAL;
SELECT REGEXP_SUBSTR('', '', 1, 1, '0') FROM DUAL;
SELECT REGEXP_SUBSTR(null, null, 1, 1, '0') FROM DUAL;

\c regression_ora
--test nchr function
select nchr(256);
select nchr(257);
select nchr(258);
select nchr(420);
select nchr(-1);
select nchr(10241234);
select nchr(1024123);
select nchr('1234');
select nchr(to_char(97)::int);

-- test number like text
select 9999 like '9%' from dual;
select 9999 like '09%' from dual;
select 9999 ilike '9%' from dual;
select 9999 ilike '09%' from dual;
select 9999 not like '9%' from dual;
select 9999 not like '09%' from dual;
select 9999 not ilike '9%' from dual;
select 9999 not ilike '09%' from dual;

create table t_nlssort_regress (f1 integer,f2 varchar2(10),f3 varchar(255));
insert into t_nlssort_regress values(1,'深圳','深圳abcdefghijklmnopqrstuvwxyz');
insert into t_nlssort_regress values(2,'中国','中国ABCDEFGHIJKLMNOPQRSTUVWXYZ');
insert into t_nlssort_regress values(3,'PG','PG_ABCDEFGHIJKLMNOPQRSTUVWXYZ');
insert into t_nlssort_regress values(4,'OpenTenBase','OpenTenBase_abcdefghijklmnopqrstuvwxyz');
insert into t_nlssort_regress values(5,'PG中国','PG中国_abcdefghijklmnopqrstuvwxyz');

--test function NLS_UPPER
SELECT nls_upper(f3) FROM t_nlssort_regress order by f1;
SELECT nls_upper(f3,'NLS_SORT = zh_CN.gb2312') FROM t_nlssort_regress order by f1;
SELECT nls_upper(f3,'NLS_SORT = zh_CN.UTF8') FROM t_nlssort_regress order by f1;
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = SCHINESE_PINYIN_M') ;
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = zh_CN.UTF8') ;
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = zh_CN.gb2312') ;
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = zh_CN.gb18030') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = SCHINESE_PINYIN_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = SCHINESE_STROKE_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = SCHINESE_RADICAL_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = zh_CN.UTF8') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = zh_CN.gb2312') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = zh_CN.gb18030') ;
insert into t_nlssort_regress values (6,nls_upper('aBCCCaaaa'));
select * from t_nlssort_regress order by f1;

--test function NLS_LOWER
SELECT nls_lower(f3) FROM t_nlssort_regress order by f1;
SELECT nls_lower(f3,'zh_CN.gb2312') FROM t_nlssort_regress order by f1;;
SELECT nls_lower(f3,'zh_CN.UTF8') FROM t_nlssort_regress order by f1;
SELECT * FROM t_nlssort_regress order by nls_lower(f3,'NLS_SORT = SCHINESE_PINYIN_M') ;
SELECT * FROM t_nlssort_regress order by nls_lower(f3,'NLS_SORT = zh_CN.UTF8') ;
SELECT * FROM t_nlssort_regress order by nls_lower(f3,'NLS_SORT = zh_CN.gb2312') ;
SELECT * FROM t_nlssort_regress order by nls_lower(f3,'NLS_SORT = zh_CN.gb18030') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = SCHINESE_PINYIN_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = SCHINESE_STROKE_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = SCHINESE_RADICAL_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = zh_CN.UTF8') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = zh_CN.gb2312') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = zh_CN.gb18030') ;
insert into t_nlssort_regress values (7,nls_lower('aBCCCaaaa'));
select * from t_nlssort_regress order by f1;

--get correct encoding
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = zh_CN.18030') ;
drop table t_nlssort_regress;

-- test offset n
SET client_min_messages = warning;
drop table if exists test_t;
create table test_t(id varchar2(20));
insert into test_t values('1');
insert into test_t values(23);
insert into test_t values(24);
insert into test_t values(25);
select * from test_t order by id;


select * from test_t order by id limit 4 offset 0;
select * from test_t order by id limit 4 offset 1;
select * from test_t order by id limit 4 offset 2;

drop table test_t;

-- test order/group by non-int const
create table test_ord(a int, b int);
explain (costs off) select * from test_ord order by 'any char'; -- no order by
explain (costs off) select a from test_ord group by 'any char', 1;
explain (costs off) select * from test_ord order by 1;
drop table test_ord;

--
select substring(ltrim(to_char(sysdate,'yyyy')), 1,2) FROM dual;
select to_char('1234', 'xxxxx') from dual;
select to_char('1234.23', 'xxxxx') from dual;
select to_char(NULL, 'xxxxx') from dual;
select to_char('1234.23', NULL) from dual;
select to_char(NULL, NULL) from dual;
-- test remainder
\c regression_ora
select mod(3, 0), remainder(3, 0) from dual;
select mod(3, 1), remainder(3, 1) from dual;
select mod(1, 3), remainder(1, 3) from dual;
select mod(32767, 32767), remainder(32767, 32767) from dual;
select mod(0.0, 1234.5678), remainder(0.0, 1234.5678) from dual;
select mod(1234.5678, 0.0), remainder(1234.5678, 0.0) from dual;
select mod(11234.5678, 9638.5632), remainder(11234.5678, 9638.5632) from dual;
select mod(-0.0, 1234.5678), remainder(-0.0, 1234.5678) from dual;
select mod(-1234.5678, 0.0), remainder(-1234.5678, 0.0) from dual;
select mod(-11234.5678, 9638.5632), remainder(-11234.5678, 9638.5632) from dual;
select mod(1.5, 2), remainder(1.5, 2) from dual;
select mod(1.522322, 2), remainder(1.522322, 2) from dual;
select mod(1.322322, 2), remainder(1.322322, 2) from dual;
select mod(-32768, 3), remainder(-32768, 3) from dual;
select mod(-5, 3), remainder(-5, 3) from dual;
select mod(-5, -3), remainder(-5, -3) from dual;
select mod(-6, 3), remainder(-6, 3) from dual;
-- test remainder insert data
create table t_id(id int);
insert into t_id values(generate_series(1, 5));
create table t_data(a int, b int, c text , d text);
insert into t_data select mod(id,2), remainder(id,3), id, id from t_id;
select * from t_data order by d;
drop table t_id;
drop table t_data;
-- remainder other combinations
select remainder(6.7, 2) from dual;
select remainder(6.7, 2.3) from dual;
select remainder(6.7, 2.6) from dual;
select remainder(6.7, 1.5) from dual;
select remainder(6.7, 1.3) from dual;
select remainder(6.7, 1.6) from dual;
select remainder(6.7, -2) from dual;
select remainder(6.7, -2.3) from dual;
select remainder(6.7, -2.6) from dual;
select remainder(6.7, -1.5) from dual;
select remainder(6.7, -1.3) from dual;
select remainder(6.7, -1.6) from dual;
select remainder(6.9, 3) from dual;
select remainder(6.8, 2) from dual;
select remainder(5.8, 2) from dual;
select remainder(4.8, 2) from dual;
select remainder(67, 2) from dual;
select remainder(57, 2) from dual;
select remainder(6.9, 3) from dual;
select remainder(7, 2) from dual;
select remainder(5, 2) from dual;
select remainder(7, -2) from dual;
select remainder(5, -2) from dual;
select remainder(-7, 2) from dual;
select remainder(-5, 2) from dual;
select remainder(-7, -2) from dual;
select remainder(-5, -2) from dual;
-- test text_remainder
select remainder(7, 2) from dual;
select remainder('7', 2) from dual;
select remainder('7', '2') from dual;
select remainder(7, '2') from dual;
--support BINARY_DOUBLE/BINARY_FLOAT
drop table if exists float_point_demo;
CREATE TABLE float_point_demo(dec_num NUMBER(10,2), bin_double BINARY_DOUBLE, bin_float BINARY_FLOAT);
INSERT INTO float_point_demo VALUES (12345,12345,12345);
INSERT INTO float_point_demo VALUES (1.235E+003,1.235E+003,1.235E+003);
SELECT bin_float, bin_double, REMAINDER(bin_float, bin_double) FROM float_point_demo order by 1;;
drop table float_point_demo;
\c regression
select mod(3, 1), remainder(3, 1) from dual;

-- test order by with only agg functions in targetlist
\c regression_ora
create table test_agg_ord(a int, b int);
-- complicate case
\c regression
-- test trim
-- usage1
SELECT trim('    aaa  bbb  ccc     ') ltrim FROM dual;
SELECT ltrim('    aaa  bbb  ccc     ') ltrim FROM dual;
SELECT rtrim('    aaa  bbb  ccc     ') ltrim FROM dual;
-- usage2:
SELECT trim(leading 'd' from 'dfssa') FROM dual; 
SELECT trim(both '1' from '123sfd111') FROM dual;
SELECT trim(trailing '2' from '213dsq12') FROM dual;
-- usage3:
SELECT trim('    aaa  bbb  ccc     ') ltrim FROM dual;
SELECT ltrim('    aaa  bbb  ccc     ') ltrim FROM dual;
SELECT rtrim('    aaa  bbb  ccc     ') ltrim FROM dual;
select ltrim('10900111000991110224323','109') from dual;
SELECT rtrim('aaaaminb','main') FROM dual;
SELECT rtrim('aaaaminb','mainb') FROM dual;
SELECT ltrim('ccbcminb','cb') FROM dual;
SELECT rtrim('abcdssscb','adscb') FROM dual;
SELECT rtrim('abcdssscb','badsc') FROM dual;
-- usage4
SELECT trim(leading 'df' from 'dfssa') FROM dual; 
SELECT trim(both '13' from '123sfd111') FROM dual;
SELECT trim(trailing '23' from '213dsq12') FROM dual;
-- usage5
\c regression_ora
SELECT trim(leading 'df' from 'dfssa') FROM dual;
SELECT trim(both '13' from '123sfd111') FROM dual;
SELECT trim(trailing '23' from '213dsq12') FROM dual;
-- trim(' ') is null
select 'abcd' col_name from dual where trim(' ') is null;
select nvl(trim(both '' from ' '),'-1') from dual;
\c regression
-- alias trim
SELECT ltrim(trim(both 'l' from lpad(SUBSTR('32.8,63.5',1,INSTR('32.8,63.5',',', 1, 1)+1), 123, 'lpad')), 'lpad') trim FROM DUAL;

\c regression_ora
-- test char(n)
DROP TABLE IF EXISTS CHAR_TBL;
CREATE TABLE CHAR_TBL(key int, f1 char(4));
INSERT INTO CHAR_TBL (key, f1) VALUES (1,'a');
INSERT INTO CHAR_TBL (key, f1) VALUES (2, 'ab');
INSERT INTO CHAR_TBL (key, f1) VALUES (3, 'abcd');
INSERT INTO CHAR_TBL (key, f1) VALUES (4, 'abcde');
INSERT INTO CHAR_TBL (key, f1) VALUES (5, 'abcd    ');
SELECT key, f1, length(f1) FROM CHAR_TBL ORDER BY f1;

DROP TABLE IF EXISTS VARCHAR_TBL;
CREATE TABLE VARCHAR_TBL(key int, f1 varchar(4));
INSERT INTO VARCHAR_TBL (key, f1) VALUES (1, 'a');
INSERT INTO VARCHAR_TBL (key, f1) VALUES (2, 'ab');
INSERT INTO VARCHAR_TBL (key, f1) VALUES (3, 'abcd');
INSERT INTO VARCHAR_TBL (key, f1) VALUES (4, 'abcde');
INSERT INTO VARCHAR_TBL (key, f1) VALUES (5, 'abcd    ');
SELECT key, f1, length(f1) FROM VARCHAR_TBL ORDER BY f1;

-- cast char(n) to text
SELECT CAST(f1 AS text) AS c_text, length(CAST(f1 AS text)) len FROM CHAR_TBL ORDER BY f1;
-- cast char(n) to varchar
SELECT CAST(f1 AS varchar(4)) c_varchar, length(CAST(f1 AS varchar(4))) len FROM CHAR_TBL ORDER BY f1;
DROP TABLE CHAR_TBL;
DROP TABLE VARCHAR_TBL;

SELECT CAST('characters' AS char(20)) || ' and text' AS "Concat char to unknown type";
SELECT CAST('text' AS text) || CAST(' and characters' AS char(20)) AS "Concat text to char";
SELECT '|' || lpad(cast('X123bcd' as char(8)), 10) || '|' from dual;

\c regression_ora
drop table if exists t5;
create table t5(f1 int,f2 text);
insert into t5 values(1,'opentenbase');
insert into t5 values(2,'opentenbase2');
insert into t5 values(3,'opentenbase3');
select * from t5 where f2 like'opentenbase%'and f1=1;
PREPARE usrrptplan(text, int) AS  SELECT * FROM t5 WHERE f2 like$1and  f1=$2;
execute usrrptplan('opentenbase', 1);
PREPARE usrrptplan2(int, int) as SELECT * FROM t5 WHERE f1 between$1and$2;
execute usrrptplan2(1,2);
PREPARE usrrptplan3(int, int) as SELECT * FROM t5 WHERE f1 in($1,$2) order by f1;
execute usrrptplan3(1,2);
drop table t5;
\c regression
reset client_min_messages;
reset datestyle;
reset intervalstyle;
reset client_encoding;

-- Test TZ_OFFSET
\c regression_ora

-- test raw
drop table if exists t1;
create table t1 (id number, doc raw(2001));
drop table if exists t1;
create table t1 (a int, b raw(3), c long raw);
\d t1
insert into t1 values(1 ,'123', '123');
insert into t1 values(1 ,'1234', '123');
insert into t1 values(1 ,'123', '1234');
select * from t1;
drop table if exists t1;
-- agg
drop table if exists tb4;
create table tb4(a raw(10), b raw(10), c int);
insert into tb4 values('123', '456', 1);
insert into tb4 values('123', '456', 1);
insert into tb4 values('123a', '456a', 1);
insert into tb4 values('123a', '456a', 2);
select max(a) from tb4;
select min(a) from tb4;
drop table tb4;

-- hash
drop table if exists raw_test2;
create table raw_test2(a raw(10), b varchar(20), c int);
create index raw_test2_index on raw_test2 using btree (a);
create index raw_test2_index2 on raw_test2 using hash (a);
\d raw_test2
drop table if exists raw_test2;
-- base type
create table test_raw (r raw(20));
insert into test_raw values ('a');
insert into test_raw values ('b');
insert into test_raw values ('s');
insert into test_raw values ('as');
insert into test_raw values ('c');
insert into test_raw values ('f');
insert into test_raw values ('dd');
insert into test_raw values ('d');
insert into test_raw values ('e');
insert into test_raw values ('12');
select r from test_raw order by r desc;
select r from test_raw order by r asc;
drop table test_raw;

create table test_raw (a raw(1), b raw(1));
insert into test_raw values ('a', 'a');
insert into test_raw values ('b', 'c');
insert into test_raw values ('d', '9');
insert into test_raw values ('6', '6');
insert into test_raw values ('5', 'f');
select * from test_raw where a < b order by a desc;
select * from test_raw where a > b order by b asc;
select * from test_raw where a < b or a > b order by a desc;
select * from test_raw where a < b or a > b order by a asc;
select * from test_raw where a = b order by a desc;
select * from test_raw where a = b order by a asc;
select * from test_raw where a >= b order by a desc;
select * from test_raw where a >= b order by a asc;
select * from test_raw where a <= b order by a desc;
select * from test_raw where a <= b order by a asc;
drop table test_raw;
create table test_raw1 (a raw(1), b raw(1));
create table test_raw2 (a raw(1), b raw(1));
insert into test_raw1 values ('a', 'a');
insert into test_raw1 values ('b', '4');
insert into test_raw1 values ('2', '9');
insert into test_raw1 values ('6', '6');
insert into test_raw1 values ('5', 'e');
insert into test_raw2 values ('a', 'a');
insert into test_raw2 values ('d', 'c');
insert into test_raw2 values ('d', '9');
insert into test_raw2 values ('2', '6');
insert into test_raw2 values ('1', 'f');
select * from test_raw1 where a like 'd';
select * from test_raw1 test1 cross join test_raw2 test2 where test1.a = test2.b order by 1,2;
select * from test_raw1 test1 join test_raw2 test2 using(a) order by 1;
select * from test_raw1 test1 full join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 left join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 right join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 inner join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 inner join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 natural join test_raw2 test2;
drop table test_raw1;
drop table test_raw2;

-- opentenbase_ora raw support '\\'
select rawtohex('\') from dual;
select rawtohex('\\') from dual;
select rawtohex('\-') from dual;
-- opentenbase_ora rawtohex support number/int
select rawtohex(0) from dual;
select rawtohex(1) from dual;
select rawtohex(2) from dual;
select rawtohex(3) from dual;
select rawtohex(4) from dual;
select rawtohex(5) from dual;
select rawtohex(-1) from dual;
select rawtohex(-2) from dual;
select rawtohex(-3) from dual;
select rawtohex(-4) from dual;
select rawtohex(-5) from dual;
select rawtohex(1) from dual;
select rawtohex(11) from dual;
select rawtohex(111) from dual;
select rawtohex(1111) from dual;
select rawtohex(-1) from dual;
select rawtohex(-11) from dual;
select rawtohex(-111) from dual;
select rawtohex(-1111) from dual;
select rawtohex(1) from dual;
select rawtohex(12) from dual;
select rawtohex(123) from dual;
select rawtohex(1234) from dual;
select rawtohex(12345) from dual;
select rawtohex(-1) from dual;
select rawtohex(-12) from dual;
select rawtohex(-123) from dual;
select rawtohex(-1234) from dual;
select rawtohex(-12345) from dual;
select rawtohex(1.1) from dual;
select rawtohex(1.12) from dual;
select rawtohex(1.123) from dual;
select rawtohex(1.1234) from dual;
select rawtohex(1.1) from dual;
select rawtohex(-1.1) from dual;
select rawtohex(-1.12) from dual;
select rawtohex(-1.123) from dual;
select rawtohex(-1.1234) from dual;
select rawtohex(-1.1) from dual;
select rawtohex(0.1) from dual;
select rawtohex(0.12) from dual;
select rawtohex(0.123) from dual;
select rawtohex(0.1234) from dual;
select rawtohex(-0.1) from dual;
select rawtohex(-0.12) from dual;
select rawtohex(-0.123) from dual;
select rawtohex(-0.1234) from dual;
select rawtohex(-0.1) from dual;
reset timezone;

-- Only one long(long raw) type is allowed in a table
drop table if exists t_src;
drop table if exists t_dst;
create table t_src(a int, b raw);
create table t_src(a int, b long raw, c long raw);
create table t_src(a int, b raw(100));
create table t_dst as select * from t_src;
alter table t_dst add column c raw;
alter table t_dst add column d long raw;
alter table t_dst add column e long raw;
drop table t_src;
drop table t_dst;

-- type long raw no support op '||'
drop table if exists test_raw;
create table test_raw(f1 int,f2 long raw);
insert into test_raw values(1,'1');
select count(f2) from test_raw;
select f2 || 'opentenbase' from test_raw;
select f2 || f2 from test_raw;
select * from test_raw;
select f1 from test_raw;
drop table test_raw;

drop table if exists t_test;
create table t_test(c1 INTERVAL YEAR TO MONTH, c2 interval day to second, c3 blob, c4 LONG RAW, c5 raw(20));
insert into t_test values(7||'-'||8||' ', 5||' '||4||':'||3||':'||2.246601||' ', ''||'10', 'AC'||'b', 'AC'||'b');
drop table t_test;

drop table if exists emp cascade;
CREATE TABLE emp (  
empno    NUMBER(4) CONSTRAINT pk_emp PRIMARY KEY,  
ename    VARCHAR2(10),  
job      VARCHAR2(9),  
mgr      NUMBER(4),  
hiredate DATE,  
sal      NUMBER(7,2),  
comm     NUMBER(7,2),  
deptno   NUMBER(2)
);
INSERT INTO emp VALUES (7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO emp VALUES (7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO emp VALUES (7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO emp VALUES (7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO emp VALUES (7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO emp VALUES (7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO emp VALUES (7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
INSERT INTO emp VALUES (7788,'SCOTT','ANALYST',7566,to_date('13-12-1987','dd-mm-rr')-85,3000,NULL,20);
INSERT INTO emp VALUES (7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO emp VALUES (7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
INSERT INTO emp VALUES (7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-rr')-51,1100,NULL,20);
INSERT INTO emp VALUES (7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO emp VALUES (7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO emp VALUES (7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);
-- test RATIO_TO_REPORT
select EMPNO,ENAME,sal,RATIO_TO_REPORT(sal) OVER() as rr from emp where JOB='CLERK' order by 1;
select EMPNO,ENAME,
RATIO_TO_REPORT(sal) OVER() as rsal,
RATIO_TO_REPORT(comm) OVER() as rcomm,
RATIO_TO_REPORT(deptno) OVER() as rdeptno
from emp order by 1;
select ratio_to_report(1) over() from dual;
-- should fail
select RATIO_TO_REPORT(sal) from emp;
select ratio_to_report() over() from dual;
select ratio_to_report(1,2) over() from dual;

-- test vsize
select vsize(NULL);
select ename, vsize(ename) from emp order by 1;
select vsize('opentenbase'::text);
select vsize('opentenbase'::cstring);
select vsize('opentenbase'::varchar(20));
select vsize('opentenbase'::char(20));

-- test approx_count_distinct
select approx_count_distinct(deptno) from emp;

drop table emp;

-- [BUGFIX] Partitioning tables support expression partitioning pruning
\c regression_ora
Drop table if exists t_time;
create table t_time
(f1 int, f2 timestamp) 
partition by range (f2) -- begin (timestamp without time zone '2021-06-01 0:0:0')     
-- step (interval '5 day') 
--partitions (5)
distribute by shard(f1) 
to group default_group;
create table t_time_part_1 partition of t_time for values from ('2021-06-01 0:0:0') to ('2021-06-06 0:0:0');
create table t_time_part_2 partition of t_time for values from ('2021-06-06 0:0:0') to ('2021-06-11 0:0:0');
create table t_time_part_3 partition of t_time for values from ('2021-06-11 0:0:0') to ('2021-06-16 0:0:0');
create table t_time_part_4 partition of t_time for values from ('2021-06-16 0:0:0') to ('2021-06-21 0:0:0');
create table t_time_part_5 partition of t_time for values from ('2021-06-21 0:0:0') to ('2021-06-26 0:0:0');

insert into t_time values(1, '2021-06-01 0:0:0'::timestamp without time zone);
insert into t_time values(1, '2021-06-05 0:0:0'::timestamp without time zone);
insert into t_time values(2, '2021-06-06 0:0:0'::timestamp without time zone);
insert into t_time values(2, '2021-06-10 0:0:0'::timestamp without time zone);
insert into t_time values(3, '2021-06-11 0:0:0'::timestamp without time zone);
insert into t_time values(3, '2021-06-15 0:0:0'::timestamp without time zone);
insert into t_time values(4, '2021-06-16 0:0:0'::timestamp without time zone);
insert into t_time values(4, '2021-06-20 0:0:0'::timestamp without time zone);
insert into t_time values(5, '2021-06-21 0:0:0'::timestamp without time zone);
insert into t_time values(5, '2021-06-25 0:0:0'::timestamp without time zone);
Drop table t_time;

-- test long type
create table t1(v int, w long, f long);
create table t1 (v int, w int, f long);
alter table t1 add column k long;
alter table t1 alter column w type long;
alter table t1 alter column f set default 'test';
create table t2 (v int, w long default 'test');
drop table if exists t2;
create table t2 (v int, w long unique);
create table t2 (v int, w long not null);
select * from t1 where f = 'test';
create index longidx on t1(f);
select * from t1 where f ~ 'test';
select * from t1 group by f;
select * from t1 order by f;
select distinct f from t1;
create materialized view mview as select v, f from t1;
create table t3 as select v, f from t1;
insert into t2 (select v, f from t1);
drop table t1;
drop table t2;

create table test_long(f1 int, f2 long);
insert into test_long values(1,'long1');
select f2,lpad(f2,10,'long') from test_long;
select f2,rpad(f2,10,'long') from test_long;
select f2,trim(f2||' ') from test_long;
select f2||'opentenbase' from test_long;
drop table test_long;
CREATE TABLE test_long
   (f1    NUMBER  CONSTRAINT test_long_c1
              CHECK (f1 BETWEEN 10 AND 99),
    f2    long  CONSTRAINT test_long_c2
              CHECK (f2 is not null));
drop table test_long;


CREATE or replace function test_long_fun(acc_no IN NUMBER)
   RETURN int
   IS acc_bal long :='test_long';
   BEGIN
   raise notice '%',acc_bal;
   return acc_no;
    END;
/
drop function test_long_fun(acc_no IN NUMBER);

CREATE or replace PROCEDURE test_long_pro AS
   tot_emps long;
   BEGIN
   tot_emps := 'test_long';
   raise notice '%',tot_emps;
   END;
/
drop procedure test_long_pro;

-- test nclob type
create table test_nclob(f1 int primary key not null,f2 nclob);
create index test_nclob_idx on test_nclob(f2);
insert into test_nclob values (1,'nclob1'),(2,'nclob2');
create table test_nclob1(f1 int primary key not null,f2 nclob);
insert into test_nclob1 values (1,'nclob1'),(4,'nclob2');
select * from test_nclob union select f1,f2 from test_nclob1;
select * from test_nclob intersect select f1,f2 from test_nclob1;
select * from test_nclob except select f1,f2 from test_nclob1;
select * from test_nclob right join test_nclob1 on test_nclob.f2=test_nclob1.f2;
select * from test_nclob inner join test_nclob1 on test_nclob.f2=test_nclob1.f2;
select * from test_nclob left join test_nclob1 on test_nclob.f2=test_nclob1.f2;
select * from test_nclob full join test_nclob1 on test_nclob.f2=test_nclob1.f2;
drop table test_nclob;
drop table test_nclob1;

-- Expand fun/pro named arguments and defaults args in the parsing phase
Drop table if exists tb1;
Drop procedure if exists p60(_errno int, _debug bool);
Drop function if exists f30(a int);
Create table tb1(a int);
Insert into tb1 select generate_series(1,10);
create or replace procedure p60(_errno int, _debug bool  default false)
as
$$
begin
    select * from tb1 where a = _errno;
end;
$$ language default_plsql;
CREATE OR REPLACE function f30(a int) returns void
    language default_plsql
    as $$
BEGIN
    call p60(-20344);
END;
$$;
select f30(1);
select f30(1);
Drop table tb1;
Drop procedure  p60(_errno int, _debug bool);
Drop function f30(a int);

-- insert into from dual, the verification for dual is ignored
drop table if exists tb1;
create table tb1(a date);
insert into tb1 select sysdate from dual;
drop table tb1;

-- Subtransaction commits should not reset session information
drop table if exists t_cif_organ_info;
drop table if exists t_client_info_pre;
drop FUNCTION if exists get_age(i_birth_date character varying, i_busi_date character varying);
CREATE TABLE t_cif_organ_info
(
    ds_date               character(8)          NOT NULL,
    unique_id             character varying(64) NOT NULL,
    branch_no             character varying(10) NOT NULL
) DISTRIBUTE BY SHARD (branch_no) to GROUP default_group;

insert into t_cif_organ_info(ds_date, branch_no, unique_id)
values ('20210803', '@', '45123135'),
       ('20210804', '123', '45123136'),
       ('20210805', '@', '45123132'),
       ('20210806', '125', '45123134'),
       ('20210802', '128', '45123133');

CREATE TABLE t_client_info_pre
(
    unique_id       character varying(64) NOT NULL,
	id_no           character varying(8),
    benefit_person  varchar2(10),
    zipcode         varchar2(10),
    open_type       varchar(10),
    is_idcard       char(1)
) DISTRIBUTE BY SHARD (unique_id) to GROUP default_group;

insert into t_client_info_pre
values ('45123135', '20210803', '1', 'dasda', 'asI~KasJs', '0');
insert into t_client_info_pre
values ('45123134', '20210804', '2', 'dasdaa', 'asGsasJs', '1');
insert into t_client_info_pre
values ('45123136', '20210805', '3', 'dasda', 'asI~KasJs', '0');
insert into t_client_info_pre
values ('45123134', '20210806', '4', 'dasda', 'asGsasJs', '2');
insert into t_client_info_pre
values ('45123132', '20210802', '5', 'ddasda', 'asI~KasJs', '0');

CREATE OR REPLACE FUNCTION get_age(i_birth_date character varying, i_busi_date character varying) RETURNS integer
    LANGUAGE default_plsql
AS
$$
declare
    out_age integer;
begin
    select 10
    into out_age
    from dual;

    return out_age;
exception
    when others then
        out_age := null;
        return out_age;
end;
$$;

SELECT
  a.benefit_person,
  get_age(a.id_no, ('20210806') :: character varying) AS age,
  a.zipcode
FROM
  (
    t_client_info_pre a
    LEFT JOIN t_cif_organ_info c ON (((a.unique_id) :: text = (c.unique_id) :: text))
  )
order by a.benefit_person;

drop table t_cif_organ_info;
drop table t_client_info_pre;
drop FUNCTION if exists get_age(i_birth_date character varying, i_busi_date character varying);


-- Procedure checks the validity of a word when it records the previous word.
\c regression_ora
Drop table if exists t_core;
Drop function if exists f6();
Create table t_core(a int);
create or replace function f6() returns void
    language default_plsql
    as $$
declare
v1 int;
cur_ref REFCURSOR;
v_sql char(200);
begin
     v_sql='select sal from t_core';
     OPEN cur_ref FOR  EXECUTE v_sql;
     LOOP
        FETCH cur_ref INTO v1;
        EXIT WHEN cur_ref % notfound;
        if v1 >= 1200 then
            res:='exception';
        end if;
     end loop;
end;
$$;
Drop table t_core;
Drop function if exists f6();

-- trigger support subtransaction
drop table if exists tb1 cascade;
drop table if exists tb3 cascade;
drop function if exists fun_fbjfyj();
create table tb1(a int, b int, c1 varchar(50), c2 varchar(50) COLLATE "pg_catalog"."default", primary key(c1));
create table tb3(
    a int,
    d1 varchar(18) COLLATE "pg_catalog"."default",
    d2 varchar(600) COLLATE "pg_catalog"."default"
);
CREATE OR REPLACE FUNCTION fun_fbjfyj()
  RETURNS trigger AS $BODY$
    DECLARE
        TF              integer :=0;
    BEGIN
        begin
            select NVL2(MAX(a), '1', '0') INTO TF from tb1 where a = 7;
            IF TF = '1' THEN RETURN new; END IF;
            new.d1 := '11';
            new.d2 := '111';
            insert into tb1 values(12, 12, new.d1, new.d2);
        end;
        RETURN new;
exception
    when others then
    return new;
    END
$BODY$
LANGUAGE default_plsql VOLATILE
COST 100;
create trigger tb3_insert after insert on tb3
FOR EACH ROW
EXECUTE PROCEDURE fun_fbjfyj();
insert into tb3 values(1, '11', '111'), (2, '22', '222'), (3,'33','333');
insert into tb3 values(1, '11', '111'), (2, '22', '222'), (3,'33','333');
select count(*) from tb3;
drop table tb1 cascade;
drop table tb3 cascade;
drop function fun_fbjfyj();

-- 1020421696091652701
create table tbl_a(a int) distribute by shard(a);
insert into tbl_a values(1);
create view v_a as select * from tbl_a;
insert into tbl_a select * from v_a;
select * from tbl_a;
drop table tbl_a cascade;

-- user case
 
drop table cib_form_fee_adj_tbl cascade;
drop table ps_rc_case cascade;
drop table ps_cib_bo_name_vw cascade;
drop table ps_cib_case_more cascade;

create table cib_form_fee_adj_tbl(case_id text,
         first_name text,
         atm_card_no text,
         cib_account_id text,
         cib_tzzklx text,
         cib_ori_tran_dt text,
         cib_ori_tran_no text,
         cib_adj_value text,
         cib_feemark text,
         row_added_oprid text,
         row_added_dttm text,
         closed_attm text,
         cib_tz_reason text,
         rc_status text,
         znjtzz text,
         dzfyzz text,
         cib_app1_id text,
         cib_approver text,
         cib_ori_tran_amt text,
         cib_flag text)distribute by shard(case_id);
create table ps_rc_case(case_id text, cib_app1_id text,
         cib_approver text,
         cib_ori_tan_amt text, closed_date text,
         rc_category text,
         rc_detail text,
         cib_cr_order_type text,
         cib_rc_case_flag text,
         bo_id_cust text)distribute by shard(case_id);

create view v_ps_rc_case as select * from ps_rc_case;

create table ps_cib_bo_name_vw(first_name text, bo_id text)distribute by shard(first_name);
create table ps_cib_case_more(
         cib_tzzklx text,
         cib_ori_tran_dt text,
         cib_ori_tran_no text,
         cib_adj_value text,
         cib_feemark text,
         cib_tz_reason text,
         cib_znjtzz text,
         cib_dxfytzz text,
         case_id text,
         rc_type text,
         atm_card_no text,
         cib text,
         row_added_oprid text,
                  row_added_dttm text,
         closed_dttm text,
          rc_status text
         )distribute by shard(cib_tzzklx);
create or replace procedure cib_set_date_mark(a varchar, c date, d varchar) is
begin
null;
end;
/
create or replace procedure cib_log_prc(a int, b varchar)
is
  begin
  null;
  end;
/
drop function if exists cib_get_datemark;
create or replace function cib_get_datemark (a varchar) return date
is
  begin
    return null;
  end;
/

drop procedure cib_log_prc;
drop procedure cib_set_date_mark;
drop function cib_get_datemark;
drop view v_ps_rc_case;
drop package cib_load_ps_pck;
-- to_date returns null by default
drop table if exists t_date1;
CREATE TABLE t_date1
       (EMPNO NUMBER(4) CONSTRAINT PK_EMP PRIMARY KEY,
        ENAME VARCHAR2(10),
        JOB VARCHAR2(9),
        MGR NUMBER(4),
        HIREDATE DATE,
        SAL NUMBER(7,2),
        COMM NUMBER(7,2),
        DEPTNO NUMBER(2));

INSERT INTO t_date1 VALUES
(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO t_date1 VALUES
(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO t_date1 VALUES
(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO t_date1 VALUES
(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO t_date1 VALUES
(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO t_date1 VALUES
(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO t_date1 VALUES
(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
INSERT INTO t_date1 VALUES
(7788,'SCOTT','ANALYST',7566,to_date('19-04-87','dd-mm-rr'),3000,NULL,20);
INSERT INTO t_date1 VALUES
(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO t_date1 VALUES
(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
INSERT INTO t_date1 VALUES
(7876,'ADAMS','CLERK',7788,to_date('23-05-87', 'dd-mm-rr'),1100,NULL,20);
INSERT INTO t_date1 VALUES
(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO t_date1 VALUES
(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO t_date1 VALUES
(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);
drop table t_date1;

-- test varchar/varchar2 operator
DROP TABLE IF EXISTS char_t;
CREATE TABLE char_t (id int, col1 varchar2(100), col2 varchar(100));
insert into char_t values (1, '12', '3');
insert into char_t values (2, '13', '3');
insert into char_t values (3, '1.2345678e-8', '1.2345678e-2');
select id, col1 + col1 varchar_add, col2 + col2 varchar2_add, col1 + col2 t_v_add,
       col1 - col1 varchar_sub, col2 - col2 varchar2_sub, col2 - col1 v_t_sub,
       col1 * col1 varchar_mul, col2 * col2 varchar2_mul, col1 + col2 t_v_mul,
       col1 / col1 varchar_div, col2 / col2 varchar2_div, col2 / col1 v_t_div from char_t order by id;
insert into char_t values (4, 'varchar', 'varchar2');
select id, col1 + col1 varchar_add, col2 + col2 varchar2_add, col1 + col2 t_v_add,
       col1 - col1 varchar_sub, col2 - col2 varchar2_sub, col2 - col1 v_t_sub,
       col1 * col1 varchar_mul, col2 * col2 varchar2_mul, col1 + col2 t_v_mul,
       col1 / col1 varchar_div, col2 / col2 varchar2_div, col2 / col1 v_t_div from char_t order by id;
DROP TABLE IF EXISTS char_t;

-- test upper(numeric)
DROP TABLE IF EXISTS upper_numeric_t;
CREATE TABLE upper_numeric_t(v int, w numeric, f float8);
insert into upper_numeric_t values(1, 2.005, -4.33435545);
select upper(v) as f1, upper(w) as f2, upper(f) as f3 from upper_numeric_t;
drop table upper_numeric_t;
select upper(2.1) as f1 from dual;

-- test 'select unique xxx from table'
create table t_unique(f1 varchar(10),f2 int);
insert into t_unique values('test1',1);
insert into t_unique values('test2',1);
insert into t_unique values('test1',1);
insert into t_unique values('test3',1);
insert into t_unique values('test2',1);
select unique f1 from t_unique order by 1;
select count(unique f1) from t_unique;
drop table t_unique;

create table test_unique1(id int, c0 int, c1 number , c2 varchar(100), c3 varchar2(100));
insert into test_unique1(id, c0, c1, c2, c3) values(1,123, 123.123, 'abc', '123');
insert into test_unique1(id, c0, c1, c2, c3) values(2,234, 234.234, 'bcd', '234');
insert into test_unique1(id, c0, c1, c2, c3) values(3,345, 345.345, 'cde', '345');
insert into test_unique1(id, c0, c1, c2, c3) values(4,456, 234.234, 'def', '456');
insert into test_unique1(id, c0, c1, c2, c3) values(5,567, 567.567, 'efg', '567');
insert into test_unique1(id, c0, c1, c2, c3) values(6,567, 567.567, 'fgh', '567');
insert into test_unique1(id, c0, c1, c2, c3) values(7,678, 678.678, 'ghi', '789');
insert into test_unique1(id, c0, c1, c2, c3) values(8,678, 678.678, '', '789');
insert into test_unique1(id, c0, c1, c2, c3) values(9,678, 678678.8123, '', '8423');
insert into test_unique1(id, c0, c1, c2) values(10,789, 789.789, 'hij');
insert into test_unique1(id, c0, c1, c2) values(11,890, 123.123, 'ijk');
insert into test_unique1(id, c0, c1, c2) values(12,901, 901.901, 'abc');
create table test_unique2(id int, c0 int, c1 number , c2 varchar(100), c3 varchar2(100));
insert into test_unique2(id, c0, c1, c2, c3) values(1,123, 123.123, 'abc', '123');
insert into test_unique2(id, c0, c1, c2, c3) values(2,234, 234.234, 'bcd', '234');
insert into test_unique2(id, c0, c1, c2, c3) values(3,345, 345.345, 'cde', '345');
insert into test_unique2(id, c0, c1, c2, c3) values(4,456, 234.234, 'def', '456');
insert into test_unique2(id, c0, c1, c2, c3) values(5,567, 567.567, 'efg', '567');
insert into test_unique2(id, c0, c1, c2, c3) values(6,567, 567.567, 'fgh', '567');
insert into test_unique2(id, c0, c1, c2, c3) values(7,678, 678.678, 'ghi', '789');
insert into test_unique2(id, c0, c1, c2, c3) values(8,678, 678.678, '', '789');
insert into test_unique2(id, c0, c1, c2, c3) values(9,678, 678678.8123, '', '8423');
insert into test_unique2(id, c0, c1, c2) values(10,789, 789.789, 'hij');
insert into test_unique2(id, c0, c1, c2) values(11,890, 123.123, 'ijk');
insert into test_unique2(id, c0, c1, c2) values(12,901, 901.901, 'abc');
select * from test_unique2 where c3 in (select unique c3 from test_unique1) order by id;
select unique sin(c3) from (select unique c3 from test_unique1) order by 1;
drop table test_unique1;
drop table test_unique2;

select chr(67)||chr(0)||chr(65)||chr(0)||chr(84) "dog" from dual;

-- test pg_catalog.to_char()
-- opentenbase_ora compatible on
select pg_catalog.to_char(20, 'aa') from dual;
select pg_catalog.to_char(22221., 'xxxxxxx') from dual;
select pg_catalog.to_char(22221., 'XXXXXXX') from dual;
select pg_catalog.to_char(22221.4, 'xxxxxxx') from dual;
select pg_catalog.to_char(22221.5, 'xxxxxxx') from dual;
select pg_catalog.to_char(-22221.4, 'xxxxxxx') from dual;

select pg_catalog.to_char(912222, 'xxxxx') from dual;
select pg_catalog.to_char(912222, 'XXXXX') from dual;

select pg_catalog.to_char(912222, 'xxxx') from dual;

select pg_catalog.to_char(-912222, 'xxxxxxx') from dual;

select pg_catalog.to_char(2e4, 'XXXx') from dual;
select pg_catalog.to_char(2e-4, 'XXXx') from dual;
select pg_catalog.to_char(2e0, 'XXXx') from dual;
select pg_catalog.to_char(-2e0, 'XXXx') from dual;

select pg_catalog.to_char(912222, 'XxxxxxxxxxxxxxxxxxxxxxxxxxXxxxxxxxxxxxxxXxxxxxxxxxxxxxxxxxxxxxX') from dual;
select pg_catalog.to_char(912222, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') from dual;
select pg_catalog.to_char(912222, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') from dual;

drop table if exists tr_tbl_to_char;
create table tr_tbl_to_char(f1 integer, f2 float, f3 double precision, f4 numeric(10,2));
insert into tr_tbl_to_char values(255, 255.4, 65535.9999999, -32765.89);
insert into tr_tbl_to_char values(254, 253.4, 65534.9999999, 32765.89);
insert into tr_tbl_to_char values(-254, 253.4, 65534.9999999, 32765.89);
select pg_catalog.to_char(f1, 'XXXX'), pg_catalog.to_char(f2, 'xxxx'), pg_catalog.to_char(f3, 'xxxX'), pg_catalog.to_char(f4, 'XXXXX') from tr_tbl_to_char order by f1;
drop table if exists tr_tbl_to_char;

-- opentenbase_ora compatible off
\c regression
select pg_catalog.to_char(20, 'aa') from dual;
select pg_catalog.to_char(22221., 'xxxxxxx') from dual;
select pg_catalog.to_char(22221., 'XXXXXXX') from dual;
select pg_catalog.to_char(22221.4, 'xxxxxxx') from dual;
select pg_catalog.to_char(22221.5, 'xxxxxxx') from dual;
select pg_catalog.to_char(-22221.4, 'xxxxxxx') from dual;

select pg_catalog.to_char(912222, 'xxxxx') from dual;
select pg_catalog.to_char(912222, 'XXXXX') from dual;

select pg_catalog.to_char(912222, 'xxxx') from dual;

select pg_catalog.to_char(-912222, 'xxxxxxx') from dual;

select pg_catalog.to_char(2e4, 'XXXx') from dual;
select pg_catalog.to_char(2e-4, 'XXXx') from dual;
select pg_catalog.to_char(2e0, 'XXXx') from dual;
select pg_catalog.to_char(-2e0, 'XXXx') from dual;

select pg_catalog.to_char(912222, 'XxxxxxxxxxxxxxxxxxxxxxxxxxXxxxxxxxxxxxxxXxxxxxxxxxxxxxxxxxxxxxX') from dual;
select pg_catalog.to_char(912222, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') from dual;
select pg_catalog.to_char(912222, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') from dual;

create table tr_tbl_to_char(f1 integer, f2 float, f3 double precision, f4 numeric(10,2));
insert into tr_tbl_to_char values(255, 255.4, 65535.9999999, -32765.89);
insert into tr_tbl_to_char values(254, 253.4, 65534.9999999, 32765.89);
insert into tr_tbl_to_char values(-254, 253.4, 65534.9999999, 32765.89);
select pg_catalog.to_char(f1, 'XXXX'), pg_catalog.to_char(f2, 'xxxx'), pg_catalog.to_char(f3, 'xxxX'), pg_catalog.to_char(f4, 'XXXXX') from tr_tbl_to_char order by f1;
drop table if exists tr_tbl_to_char;

-- opentenbase_ora treat '' as null
\c regression_ora
drop table if exists t1;
create table t1(f1 int, f2 varchar(10), f3 text, f4 char(30), f5 nvarchar2);
insert into t1 values(1, '', '', '', ''), (2, null, null, null, null);
copy t1 from stdin;
3				
4	\N	\N	\N	\N
\.
select char_length(f2), char_length(f3), char_length(f4), char_length(f5), * from t1 order by f1;
truncate table t1;
drop table t1;
\c regression
drop table if exists t1;
create table t1(f1 int, f2 varchar(10), f3 text, f4 char(30), f5 varchar(100));
insert into t1 values(1, '', '', '', ''), (2, null, null, null, null);
copy t1 from stdin;
3				
4	\N	\N	\N	\N
\.
select char_length(f2), char_length(f3), char_length(f4), char_length(f5), * from t1 order by f1;
drop table t1;

-- test clob
\c regression_ora
create table lob_table(v int, c1 clob);
insert into lob_table values (1, 'clob');
select trim(trailing ' ' from c1) as clob from lob_table;
select to_clob(c1) from lob_table;
select to_char(c1) from lob_table;
select ascii(c1) from lob_table;
insert into lob_table values (2, '1');
insert into lob_table values (2, '2');
select avg(c1) from lob_table where v = 2;
select ratio_to_report(c1) over() from lob_table where v = 2;
drop table lob_table;

set allow_limit_ident to on;
create temporary table limit(id int, limit CLOB);
insert into limit(id, limit) values(1,'abc'),(2,'dcb');
select * from limit order by limit,id;
drop table limit;
reset allow_limit_ident;


-- test left as column
drop table if exists left_t;
create table left_t(v int, left int, w int);
insert into left_t values(1, 1, 1);
insert into left_t(v, left) values(2, 3);
select left from left_t order by v;
select left from left_t order by left;
select * from left_t order by left;
drop table left_t;

-- test unique_column_name
set unique_column_name to on;
select '','' as alias_column,substr('opentenbase',5,2),substr('opentenbase',-3),null,null,'opentenbase' as alias_column;
create table t_unique_column_name (a int, a_1 int);
select a,a from t_unique_column_name;
select a,a,a_1,a from t_unique_column_name;
select a,a_1,a_1,a from t_unique_column_name;
select *,a as a from t_unique_column_name;
drop table t_unique_column_name;
reset unique_column_name;

-- opentenbase_ora,to_data/to_timestamp/to_timestamp_tz relax delimiter checking in opentenbase_ora compatibility mode
\c regression_ora

-- 1020421696875393133
/*
 * test insert with table alias.
 * 1. INSERT INTO table t1(c1,c2, t1.c3)
 * 2. INSERT INTO table (test.c1)
 * 3. INSERT INTO table t1 VALUES
 * 4. INSERT INTO table t1 select
 * exception:
 * 1. INSERT INTO table t1(t1.t2.c1)
 * 2. INSERT INTO table t1(t1.*)
 */
create table table_102(c1 int, c2 int, c3 int, c4 int);
-- 1. INSERT INTO table t1(c1,c2, t1.c3)
insert into table_102 t(t.c1,t.c2) values(1, 2);
insert into table_102 t(t.c1,t.c2) select 1, 22;
select * from table_102 order by 1, 2;
delete from table_102;

-- 2. INSERT INTO table (test.c1)
insert into table_102(table_102.c1, table_102.c2) values(2, 3);
select * from table_102;
delete from table_102;

-- 3. INSERT INTO table t1 VALUES
insert into table_102 t1 values(1,2 );
insert into table_102 t2 select 1,2 ;
select * from table_102;
delete from table_102;
--mixed
insert into table_102 t(t.c1,t.c2, c3) select 1, 2, 12;
select * from table_102;
delete from table_102;

-- 4. INSERT INTO table t1 select
-- tested above
-- exception:
-- 1. INSERT INTO table t1(t1.t2.c1)
insert into table_102 t(t.t.c1,t.c2) select 1, 22;
insert into table_102 t(table_102.c1,t.c2) select 1, 22;
-- 2. INSERT INTO table t1(t1.*)
insert into table_102 t(t.*) select 1;
drop table table_102;

-- 1020421696095179659
/*
 * use 'link' as column identifier
 * use 'link' as table identifier
 * use 'link' as alias name
 */
create table table_103 (id int, link int);
create table link (id int, link int);
select id as link from table_103;
drop table table_103;
drop table link;

-- 1020421696880130617
-- test nvl with diff length type
create table t_fix_nvl_ret_cut_20221201(id int,name varchar(6),name2 timestamp);
insert into t_fix_nvl_ret_cut_20221201 values (1,'202210','2022-12-04 23:48:35.636675');
insert into t_fix_nvl_ret_cut_20221201(id,name2) values (2,'2022-12-04 23:48:35.638837');
insert into t_fix_nvl_ret_cut_20221201(id,name2) values (3,'2022-12-04 23:48:35.640332');
insert into t_fix_nvl_ret_cut_20221201 values (4,'202012','2022-12-04 23:48:35.642297');
select * from t_fix_nvl_ret_cut_20221201 order by id;
select nvl(name,to_char(name2,'yyyy:mm:dd hh24:mi:ss')) from t_fix_nvl_ret_cut_20221201 order by id;
drop table t_fix_nvl_ret_cut_20221201;
select nvl(NULL::bfile, 111111111111111111111111111111111111);
select nvl(NULL::bpchar(1), 111111111111111111111111111111111111);
select nvl(NULL::char(1), 111111111111111111111111111111111111);
select nvl(NULL::information_schema.character_data, 111111111111111111111111111111111111);
select nvl(NULL::clob, 111111111111111111111111111111111111);
select nvl(NULL::long, 111111111111111111111111111111111111);
select nvl(NULL::name, 111111111111111111111111111111111111);
select nvl(NULL::nclob, 111111111111111111111111111111111111);
select nvl(NULL::varchar2(1), 111111111111111111111111111111111111);
select nvl(NULL::pg_dependencies, 111111111111111111111111111111111111);
select nvl(NULL::pg_ndistinct, 111111111111111111111111111111111111);
select nvl(NULL::pg_node_tree, 111111111111111111111111111111111111);
select nvl(NULL::information_schema.sql_identifier, 111111111111111111111111111111111111);
select nvl(NULL::text, 111111111111111111111111111111111111);
select nvl(NULL::varchar(1), 111111111111111111111111111111111111);
select nvl(NULL::varchar2(1), 111111111111111111111111111111111111);
select nvl(NULL::information_schema.yes_or_no, 111111111111111111111111111111111111);
-- test nvl2 with diff length type
select nvl2(NULL, NULL::bfile, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::bpchar(1), 111111111111111111111111111111111111);
select nvl2(NULL, NULL::char(1), 111111111111111111111111111111111111);
select nvl2(NULL, NULL::information_schema.character_data, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::clob, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::long, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::name, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::nclob, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::varchar2(1), 111111111111111111111111111111111111);
select nvl2(NULL, NULL::pg_dependencies, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::pg_ndistinct, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::pg_node_tree, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::information_schema.sql_identifier, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::text, 111111111111111111111111111111111111);
select nvl2(NULL, NULL::varchar(1), 111111111111111111111111111111111111);
select nvl2(NULL, NULL::varchar2(1), 111111111111111111111111111111111111);
select nvl2(NULL, NULL::information_schema.yes_or_no, 111111111111111111111111111111111111);

-- test update pull up
create table test1 (
                       c11 integer,
                       c12 integer,
                       c13 integer,
                       c14 integer,
                       c15 integer
);

create table test2 (
                       c21 integer,
                       c22 integer,
                       c23 integer,
                       c24 integer,
                       c25 integer
);


create table test3 (
                       c31 bigint,
                       c32 bigint,
                       c33 bigint,
                       c34 bigint,
                       c35 bigint
);


create table test4 (
                       c41 char(10),
                       c42 char(10),
                       c43 char(10),
                       c44 char(10),
                       c45 char(10)
);

insert into test1
select  i,i,i,i,i
from generate_series(1, 10) as i;

insert into test2
select  i,i,i+1,i,i
from generate_series(1, 10) as i;

insert into test3
select  i,i,i+1,i,i
from generate_series(1, 10) as i;

insert into test4
select  i,i,i+1,i,i
from generate_series(1, 10) as i;

explain (costs off) update test1 set c13=(select test2.c23 from test2 where test1.c12=test2.c22);

update test1 set c13=(select test2.c23 from test2 where test1.c12=test2.c22);

select * from test1 order by c11;

delete from test1;

insert into test1
select  i,i,i,i,i
from generate_series(1, 10) as i;

explain (costs off) update test1 set c13=(select test3.c33 from test3 where test1.c12=test3.c32);

update test1 set c13=(select test3.c33 from test3 where test1.c12=test3.c32);

select * from test1 order by c11;

delete from test1;

insert into test1
select  i,i,i,i,i
from generate_series(1, 10) as i;

explain (costs off) update test1 set c13=(select t.c43 from test4 as t where test1.c12=t.c42);

update test1 set c13=(select t.c43 from test4 as t where test1.c12=t.c42);

select * from test1 order by c11;

drop table test1;
drop table test2;
drop table test3;
drop table test4;

reset client_min_messages;
reset datestyle;
reset intervalstyle;
reset client_encoding;

-- 1020421696882003165
-- test column name like opentenbase_ora
set enable_opentenbase_ora_column_name to on;
create table t_opentenbase_ora_colname_20230220("Id " int, "I D" int, num int[]);
insert into t_opentenbase_ora_colname_20230220 values (1, 2, array[1,2,3]), (2, 3, array[1,2,3]), (3, 4, array[1,2,3]);
-- test column name of node T_ColumnRef
select "Id " as id1, "I D" from t_opentenbase_ora_colname_20230220  order by "Id ";
-- test column name of node T_A_Indirection (opentenbase_ora doesn't support)
select num[2] from t_opentenbase_ora_colname_20230220  order by "Id ";
-- test column name of node T_FuncCall
create or replace function f_opentenbase_ora_colname_20230220(id int)
return int AS
begin
return id;
end;
/
select f_opentenbase_ora_colname_20230220(num[1]) from t_opentenbase_ora_colname_20230220 order by "Id ";
select sum("Id "), avg("I D") from t_opentenbase_ora_colname_20230220;
-- test column name of node T_A_Expr
select sum("Id ") + 3, avg("I D") / 5 from t_opentenbase_ora_colname_20230220;
select 1 + 1, 'a,' || 'b''' || 'c from' from dual;
-- test column name of node T_TypeCast (opentenbase_ora doesn't support)
select "Id "::text, "I D"::varchar(2) from t_opentenbase_ora_colname_20230220 order by "Id ";
-- test column name of node T_collateClause (opentenbase_ora doesn't support)
select "Id "::text collate "C" from t_opentenbase_ora_colname_20230220 order by "Id ";
-- test column name of node T_GroupingFunc
select grouping("Id ") from t_opentenbase_ora_colname_20230220 group by "Id ";
select grouping_id("I D") from t_opentenbase_ora_colname_20230220 group by "I D";
-- test column name of node T_SubLink (opentenbase_ora doesn't support exists, array, all, any)
select exists(select "Id " from t_opentenbase_ora_colname_20230220 order by "Id ") from t_opentenbase_ora_colname_20230220;
select array(select "I D" from t_opentenbase_ora_colname_20230220 order by "I D") from t_opentenbase_ora_colname_20230220;
select (select "I D" from t_opentenbase_ora_colname_20230220 order by "I D" limit 1) from dual;
select "Id " = all(select "Id " from t_opentenbase_ora_colname_20230220 order by "Id ") from t_opentenbase_ora_colname_20230220;
select "I D" > any(select "I D" from t_opentenbase_ora_colname_20230220 order by "I D",1) from t_opentenbase_ora_colname_20230220 order by 1;
-- test column name of node T_CaseExpr
select case when "Id " = 1 then 1 else 0 end from t_opentenbase_ora_colname_20230220;
-- test column name of node T_A_ArrayExpr (opentenbase_ora doesn't support)
select array['1', '2', '3'] from t_opentenbase_ora_colname_20230220;
-- test column name of node T_RowExpr (opentenbase_ora doesn't support)
select row(1, 2, '3') from t_opentenbase_ora_colname_20230220;
-- test column name of node T_CoalesceExpr
select coalesce(NULL, NULL, 3, 4, 5) FROM dual;
select coalesce(3) FROM dual;
-- test column name of node T_MinMaxExpr
select greatest(1, 2, 3, 4) from dual;
select least(1, 2, 3, 4) from dual;
-- test column name of node T_SQLValueFunction
select current_date from dual where 1=0;
select current_timestamp from dual where 1=0;
select localtimestamp from dual where 1=0;
select localtimestamp(1) from dual where 1=0;
select uid from dual where 1=0;
-- opentenbase_ora doesn't support the following T_SQLValueFunction
select current_time from dual where 1=0;
select current_time(1) from dual where 1=0;
select localtime from dual where 1=0;
select localtime(1) from dual where 1=0;
select current_role from dual where 1=0;
select current_user from dual where 1=0;
select user from dual where 1=0;
select session_user from dual where 1=0;
select current_catalog from dual where 1=0;
select current_schema from dual where 1=0;
-- test column name of node T_XmlExpr (opentenbase_ora doesn't support is document)
SELECT xmlcomment('hello') from dual;
SELECT xmlconcat(xmlcomment('hello'), xmlcomment('world')) from dual;
SELECT xmlforest('abc' AS "FOO", 123 AS "BAR") from dual;
SELECT xmlparse(content '<abc>x</abc>') from dual;
SELECT xmlpi(name foo) from dual;
SELECT xmlroot( XMLType('<poid>143598</poid>'), VERSION '1.0', STANDALONE YES) from dual;
SELECT xmlserialize(CONTENT xmlconcat(xmlcomment('hello'), xmlcomment('world')) as varchar2(30)) from dual;
SELECT xmlforest('abc' AS foo, 123 AS bar) is document from dual;
-- test column name of node T_XmlSerialize
SELECT xmlserialize(CONTENT XMLTYPE('<Owner>Grandco</Owner>') as varchar2(30)) from dual;
select 1 from dual;
select 1.2 from dual;
select ' 12 2 aaaa' from dual;
select ' 1.3 nnn? 大' from dual;
drop table t_opentenbase_ora_colname_20230220;
drop function f_opentenbase_ora_colname_20230220;
reset enable_opentenbase_ora_column_name;

-- save pg function for upgrade case in opentenbase_ora mode issue: ID884752731
\c regression
-- arg 3
SELECT regexp_replace('hello world', 'o', '0');
-- Output: 'hell0 world'
SELECT regexp_replace('123-456-789', '\d+', 'X');
-- Output: 'X-456-789'

-- arg 4
SELECT regexp_replace('hello world', 'o', '', 'g');
-- Output: 'hell wrld'
SELECT regexp_replace('hello world', 'o', 'O', 'g');
-- Output: 'hellO wOrld'
select regexp_replace('aaaaaaaaa', '[^0-9]',',') from dual;
select regexp_replace('aaaaaaaaa', '[^0-9]','') from dual;
select regexp_replace('aaaaaaaaa', null,',') from dual;
select regexp_replace('aaaaaaaaa', null, null) from dual;
select regexp_replace(null, null, null) from dual;
select regexp_replace(null, 'a', null) from dual;
select regexp_replace(null, 'a', 'a') from dual;

\c regression_ora
-- arg 3
SELECT regexp_replace('hello world', 'o', '0') from dual;
-- Output: 'hell0 w0rld'
SELECT regexp_replace('123-456-789', '\d+', 'X') from dual;
-- Output: 'X-X-X'

-- arg 4
SELECT regexp_replace('hello world', 'o', '', 'g') from dual;
-- ERROR: invalid number
SELECT regexp_replace('hello world', 'o', 'O', 'g') from dual;
-- ERROR: invalid number
select regexp_replace('abcd(def)','\(|\)','\1','g') from dual;
-- ERROR: invalid number

-- const test
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','1a') from dual;
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','gggg') from dual;
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','ggggiii') from dual;
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','ggggiii12') from dual;
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','1g') from dual;

select regexp_replace('1g3fd6f8dyf', '[0-9]',',','1') from dual;
select regexp_replace('1g3fd6f8dyf', null,',',1) from dual;
select regexp_replace('1g3fd6f8dyf', null, null,'5') from dual;
select regexp_replace('1g3fd6f8dyf', null, null,5) from dual;

-- postion err
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','-1') from dual;
select regexp_replace('1g3fd6f8dyf', null,',',-1) from dual;
select regexp_replace('1g3fd6f8dyf', null, null,'0') from dual;
select regexp_replace('1g3fd6f8dyf', null, null,0) from dual;

select regexp_replace('1g3fd6f8dyf', '[0-9]',',','100000') from dual;
select regexp_replace('1g3fd6f8dyf', null,',',100000) from dual;
select regexp_replace('1g3fd6f8dyf', null, null,'100000') from dual;
select regexp_replace('1g3fd6f8dyf', null, null,100000) from dual;

-- opentenbase_ora create table with physical_attributes_clause
CREATE TABLE phy_attr_storage_test_table (a numeric)
    PCTFREE    10
    PCTUSED    90
    INITRANS   2
    MAXTRANS   255
    STORAGE (INITIAL 100K NEXT 100K MINEXTENTS 1 MAXEXTENTS 100 PCTINCREASE 0 FREELISTS 4 FREELIST GROUPS 1 OPTIMAL 10K BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT ENCRYPT)
;

ALTER TABLE phy_attr_storage_test_table
    ADD d int,
    ADD e int,
    PCTFREE    10
    PCTUSED    90
    INITRANS   2
    MAXTRANS   255
    STORAGE (INITIAL 100T NEXT 100K MINEXTENTS 1 MAXEXTENTS 100 PCTINCREASE 0 FREELISTS 4 FREELIST GROUPS 1 OPTIMAL 10K BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT ENCRYPT)
;

CREATE INDEX phy_attr_storage_test_index ON phy_attr_storage_test_table (a)
    PCTFREE    10
    PCTUSED    90
    INITRANS   2
    MAXTRANS   255
    STORAGE (INITIAL 100K NEXT 100K MINEXTENTS 1 MAXEXTENTS 100 PCTINCREASE 0 FREELISTS 4 FREELIST GROUPS 1 OPTIMAL 10K BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT ENCRYPT);

ALTER INDEX phy_attr_storage_test_index
    PCTFREE    10
    PCTUSED    0
    INITRANS   2
    MAXTRANS   255
    STORAGE (INITIAL 100K NEXT 100K MINEXTENTS 1 MAXEXTENTS 100 PCTINCREASE 0 FREELISTS 4 FREELIST GROUPS 1 OPTIMAL 10K BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT ENCRYPT);

CREATE MATERIALIZED VIEW phy_attr_storage_test_matview
    PCTFREE    10
    PCTUSED    90
    INITRANS   2
    MAXTRANS   255
    STORAGE (INITIAL 100K NEXT 100K MINEXTENTS 1 MAXEXTENTS 100 PCTINCREASE 0 FREELISTS 4 FREELIST GROUPS 1 OPTIMAL 10K BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT ENCRYPT)
AS SELECT * FROM phy_attr_storage_test_table;

alter MATERIALIZED VIEW phy_attr_storage_test_matview
    PCTFREE    10
    PCTUSED    90
    INITRANS   2
    MAXTRANS   255
    STORAGE (INITIAL 100K NEXT 100K MINEXTENTS 1 MAXEXTENTS 100 PCTINCREASE 0 FREELISTS 4 FREELIST GROUPS 1 OPTIMAL 10K BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT ENCRYPT)
;

drop table phy_attr_storage_test_table cascade;

-- test unique_column_name
set unique_column_name to on;
select '','' as alias_column,substr('opentenbase',5,2),substr('opentenbase',-3),null,null,'opentenbase' as alias_column;
create table t_unique_column_name (a int, a_1 int);
select a,a from t_unique_column_name;
select a,a,a_1,a from t_unique_column_name;
select a,a_1,a_1,a from t_unique_column_name;
select *,a as a from t_unique_column_name;
drop table t_unique_column_name;
reset unique_column_name;

-- Remove group by in EXISTS subquery
set enable_opentenbase_ora_compatible = on;
create table hoo(o1 int, o2 int);
insert into hoo values(1,2);
insert into hoo values(1,3);
insert into hoo values(1,4);
create table hii(i1 int, i2 int);
insert into hii values(1,2);
insert into hii values(1,3);
insert into hii values(1,4);

create or replace function ftest(a int) return int
as
begin
  return a;
end;
/

select * from hoo where exists (select i2 from hii group by 1);

select 1 from dual where exists (select i1, sum(i2) from hii group by i1);
select 1 from hoo where exists (select i1, sum(i1) from hii group by i1);
select 1 from hoo where exists (select sum(i2) over() from hii group by i2);
select 1 from hoo where exists (select rank() over(order by i2) from hii group by i2);

update hoo set o1 = o2+10 where exists (select i1 from hii group by i1 having sum(i2)>1);
update hoo set o1 = o2+10 where exists (select i1 from hii group by cube(i1));

--  invalid cases
select 1 from hoo where exists (select i1, sum(i2) from hii group by i2);
select 1 from hoo where exists (select i1, sum(i2) from hii group by 'any char');
select 1 from hoo where exists (select sum(i1) over() from hii group by i2);
select 1 from hoo where exists (select rank() over(order by i1) from hii group by i2);
update hoo set o1 = o2+10 where exists (select i2 from hii group by i1 having 1=1);
update hoo set o1 = o2+10 where exists (select i2 from hii group by cube(i1));

drop table hoo;
drop table hii;
drop function ftest;

-- Support the judge whether the PARAM is null in PREPARE stmt
create table ttest_20230607 (a int, b int);
insert into ttest_20230607 values(1,1);
insert into ttest_20230607 values('',1);

prepare s as select * from ttest_20230607 where $1 is null;
execute s('1');
execute s(1);
execute s('');
execute s(NULL);
deallocate s;

prepare s as
select $1 is null from ttest_20230607;
execute s(1);
execute s('');
execute s(NULL);
deallocate s;

prepare s as select decode(a,$1,'first','default') from ttest_20230607;
execute s(1);
execute s(2);
execute s('');
execute s(NULL);
deallocate s;

prepare s as select decode(a,$1,'first',$2,'second') from ttest_20230607;
execute s(-1,2);
execute s(2,1);
execute s('',2);
execute s(1,'');
execute s('','');
execute s(NULL,NULL);
deallocate s;

prepare s as select decode(a,$1 + $2,'first','default') from ttest_20230607;
execute s(1,0);
execute s(2,1);
execute s(1+'',1);
execute s('','');
execute s(NULL,NULL);
deallocate s;

prepare s as select decode(a,$1 + 2,'first','default') from ttest_20230607;
execute s(-1);
execute s(0);
execute s(1+'');
execute s(1+NULL);
execute s('');
execute s(NULL);
deallocate s;

prepare s as
select case when $1 is null then 'first' else 'else' end from ttest_20230607;
execute s('');
execute s(NULL);
execute s(1);
deallocate s;

prepare s as
select case when $1 is null then 'first'
            when $2 is not null then 'second'
            else 'else' end from ttest_20230607;
execute s('',1);
execute s(1,1);
execute s(1,'');
execute s('','');
execute s(NULL,1);
execute s(1,NULL);
execute s(NULL,NULL);
deallocate s;

-- failed
prepare s as
select $1 is distinct from null as "not null" from ttest_20230607;
drop table ttest_20230607;

-- inner COMMIT with output arguments
drop PROCEDURE if exists test_proc7cc(_x int);
drop PROCEDURE if exists test_proc7c(x int, INOUT a int, INOUT b numeric);
CREATE PROCEDURE test_proc7c(x int, INOUT a int, INOUT b numeric)
AS
BEGIN
  a := x / 10;
  b := x / 2;
  COMMIT;
END;
/
CREATE PROCEDURE test_proc7cc(_x int)
AS
DECLARE _a int; _b numeric;
BEGIN
  CALL test_proc7c(_x, _a, _b);
  RAISE NOTICE '_x: %,_a: %, _b: %', _x, _a, _b;
END;
/
CALL test_proc7cc(10);
-- Fix error-cleanup mistakes in exec_stmt_call()
create or replace procedure test_proc7cc(_x int)
as
declare _a int; _b numeric;
begin
  call test_proc7c(_x, _a, 1);
  raise notice '_x: %,_a: %, _b: %', _x, _a, _b;
end;
/
call test_proc7cc(10);
create or replace procedure test_proc7cc(_x int)
language default_plsql
as $$
declare _a int; _b numeric;
begin
  call test_proc7c(_x, _a, 1);
  raise notice '_x: %,_a: %, _b: %', _x, _a, _b;
end
$$;
call test_proc7cc(10);
drop PROCEDURE test_proc7cc(_x int);
drop PROCEDURE test_proc7c(x int, INOUT a int, INOUT b numeric);

create or replace function ftype_str_88(v1 text, v2 varchar2, v3 char(10), v4 varchar)
return int is
begin
 raise notice '%, %,%,%', v1, v2, v3,v4;
 return 1;
end;
/
create or replace function ftype_num_88(v1 number, v2 int, v3 float, v4 bigint)
return int is
begin
 raise notice '%,%,%,%', v1, v2, v3, v4;
 return 1;
end;
/

declare
  vtxt text default 1;
  vvar2 varchar2 default 2;
  vvar varchar default 3;
  vcar char(20) default 44;

  vi int default 4;
  vf float default 10.0;
  vb bigint default 10;
  vnum number default 10.12;
  ret int;
begin
-- string -> number
-- text
  ret = ftype_num_88(vtxt,vtxt,vtxt,vtxt);
-- varchar2/varchar
  ret = ftype_num_88(vvar,vvar,vvar,vvar);
  ret = ftype_num_88(vvar2,vvar2,vvar2,vvar2);
-- char
  ret = ftype_num_88(vcar,vcar,vcar,vcar);

-- number -> string 
-- int
  ret = ftype_str_88(vi, vi, vi, vi);
-- float
  ret = ftype_str_88(vf, vf, vf, vf);
-- bigint
  ret = ftype_str_88(vb, vb, vb, vb);
-- number
  ret = ftype_str_88(vnum, vnum, vnum, vnum);
end;
/
drop function ftype_str_88;
drop function ftype_num_88;

-- test tapd:ID884788269
create table lbw_0101(c1 varchar2(10));
insert into lbw_0101 values (1);
select least(0,c1 ) from lbw_0101;
select greatest(0,c1 ) from lbw_0101;
drop table lbw_0101;

-- test string and numeric
create table test_20230904(c1 int,c2 bigint,c3 numeric,c4 char,c5 varchar(100),c6 varchar2(100),c7 text);
insert into test_20230904 values(100,200,300,'1','100','1000','10000');
select least(c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select greatest(c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select least('0',c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select greatest(0,c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select greatest('0',c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select least(0,c1,c2,c3,c4,c5,c6,c7) from test_20230904;
delete from test_20230904;


-- test error
insert into test_20230904 values(100,200,300,'a','abc00','1000','10000');
select least(c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select greatest(c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select least('0',c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select greatest(0,c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select greatest('0',c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select least(0,c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select least(c4,c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select least(c5,c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select least(c6,c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select least(c7,c1,c2,c3,c4,c5,c6,c7) from test_20230904;
drop table test_20230904;

-- test float
create table test_20230904 (c1 float,c2 float8,c3 numeric,c4 char,c5 varchar(100),c6 varchar2(100),c7 text);
insert into test_20230904 values(3.33,1.11,22.2,'1','9.9','2.3','4.4');
select least(c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select least('9',c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select greatest(c1,c2,c3,c4,c5,c6,c7) from test_20230904;
select greatest('1',c1,c2,c3,c4,c5,c6,c7) from test_20230904;
drop table test_20230904;

-- test other type
create table test_20230904(c1 date,c2 varchar(200));
insert into test_20230904 values('2023-09-04 11:11:11','2023-09-04 11:11:12');
select least(c1,c2) from test_20230904;
select least(c2,c1) from test_20230904;
select least('0',c2,c1) from test_20230904;
drop table test_20230904;

create table test_cb_20220816_1(id varchar2(10),mc varchar2(50));
insert into test_cb_20220816_1 values('1','11111');
insert into test_cb_20220816_1 values('1','22222');
insert into test_cb_20220816_1 values('2','11111');
insert into test_cb_20220816_1 values('2','22222');
insert into test_cb_20220816_1 values('3','11111');
insert into test_cb_20220816_1 values('3','22222');
insert into test_cb_20220816_1 values('3','33333');
drop table test_cb_20220816_1;

set disable_empty_to_null to on;
create table test_null_20231213(c1 int,c2 varchar2(10) not null,c3 text not null,c4 varchar(10) not null);
insert into test_null_20231213 values(1,'','','');
select * from test_null_20231213;
set disable_empty_to_null to off;
\copy test_null_20231213(c1,c2,c3,c4) from stdin;
2		qq	qq
3		qq	qq
4		qq	qq
\.
set disable_empty_to_null to on;
\copy test_null_20231213(c1,c2,c3,c4) from stdin;
2		qq	qq
3		qq	qq
4		qq	qq
\.
select * from test_null_20231213 where c2 is not null order by 1;
drop table test_null_20231213;
create table t_20231210(id int,eno int);
drop table t_20231210;
-- empty string as null

--
-- BUGID: 116117311 BEGIN
--
reset disable_empty_to_null;
CREATE TABLE tbl_orders_20230526 (
	  order_id int,
	  salesperson_id INTEGER NOT NULL,
	  customer_id INTEGER NOT NULL
);
ALTER TABLE tbl_orders_20230526 ENABLE ROW LEVEL SECURITY;
CREATE POLICY tbl_orders_20230526_policy ON tbl_orders_20230526
USING (salesperson_id = 10)
WITH CHECK (salesperson_id = 10);
select * from pg_policies where POLICYNAME='TBL_ORDERS_20230526_POLICY';
drop policy tbl_orders_20230526_policy on tbl_orders_20230526;
drop table tbl_orders_20230526;
select string_to_array('public', '') from dual;
select string_to_array('public', null) from dual;
--
-- BUGID: 116117311 END 
--

--
-- Fixed the incompatibility issue with opentenbase_ora in text equality comparison.
--

--TestPoint:1.char
create table table_t4_20240601(name char(50));
insert into table_t4_20240601 values('Hello');
select * from table_t4_20240601 where upper(name)=upper('hello');
select * from table_t4_20240601 where lower(name)=lower('hello');
select * from table_t4_20240601 where nls_lower(name)=nls_lower('hello');
select * from table_t4_20240601 where nls_upper(name)=nls_upper('hello');
drop table table_t4_20240601;


--TestPoint:2.nchar
drop table table_t4_20240601;
create table table_t4_20240601(name nchar(50));
insert into table_t4_20240601 values('Hello');
select * from table_t4_20240601 where upper(name)=upper('hello');
select * from table_t4_20240601 where lower(name)=lower('hello');
select * from table_t4_20240601 where nls_lower(name)=nls_lower('hello');
select * from table_t4_20240601 where nls_upper(name)=nls_upper('hello');
drop table table_t4_20240601;
/* Fix update syntax issue when updated column bracketed */
drop table if exists t_20240520_1;
drop table if exists t_20240520_2;
create table t_20240520_1(a int, b int);
update t_20240520_1 set (b) = 1;
update t_20240520_1 set (b) = '123';
create table t_20240520_2(x int, y int);
update t_20240520_1 set (b) = y from t_20240520_2 where a = x;
update t_20240520_1 set (b) = (y) from t_20240520_2 where a = x;
drop table t_20240520_1;
drop table t_20240520_2;
drop table if exists t_20240520;
create table t_20240520(a int, b int);
update t_20240520 x set x.x = 1;
update t_20240520 x set x.b = 1;
update t_20240520 x set x.y.z = 1;
drop table t_20240520;
