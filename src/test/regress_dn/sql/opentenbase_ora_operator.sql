-- test prefix match
\c regression_ora
DROP TABLE IF EXISTS t5;
create table t5(f1 int,f2 text);
insert into t5 values(1, 'opentenbase');
insert into t5 values(2, 'opentenbase2');
insert into t5 values(3, 'opentenbase3');
select * from t5 where f2 like'opentenbase%'and f1=1;
PREPARE usrrptplan(text, int) AS  SELECT * FROM t5 WHERE f2 like$1 and  f1=$2 order by f1;
execute usrrptplan('opentenbase', 1);
PREPARE usrrptplan2(int, int) as SELECT * FROM t5 WHERE f1 between$1 and$2 order by f1;
execute usrrptplan2(1,2);
PREPARE usrrptplan3(int, int) as SELECT * FROM t5 WHERE f1 in($1,$2) order by f1;
execute usrrptplan3(1,2);
PREPARE usrrptplan4(text, int) AS  SELECT * FROM t5 WHERE f2 LiKe$1 and  f1=$2 order by f1;
execute usrrptplan4('opentenbase', 1);
PREPARE usrrptplan5(int, int) as SELECT * FROM t5 WHERE f1 BetWeen$1 and$2 order by f1;
execute usrrptplan5(1,2);
PREPARE usrrptplan6(int, int) as SELECT * FROM t5 WHERE f1 IN($1,$2) order by f1;
execute usrrptplan6(1,2);
PREPARE usrrptplan7(text) AS SELECT * FROM t5 WHERE f2 like$1 order by f1;
execute usrrptplan7('opentenbase%');
PREPARE usrrptplan8(text) AS SELECT * FROM t5 WHERE f2 like$01 and 1=1 order by f1;
execute usrrptplan8('opentenbase%');
drop table t5;

------------------------------------------
-- Test cases for operator MOD.
------------------------------------------

CREATE or replace PROCEDURE op_mod_test_20230530(a  INT default 5 mod 3)
AS
$$
DECLARE
  i1 INT;
  i3 INT default 8 mod 5; -- 3
  i4 INT;
  mod INT default 9 mod 5;  -- 4
BEGIN
  mod = 4;
  i3 = 3;
  raise notice 'in: %', a;
  raise notice 'i3: %', i3;
  raise notice 'mod: %', mod;
  -- space
  i1 := 17 mod 9 mod 5;
  raise notice 'i1: %', i1;
  raise notice '%', 7 mod(9 mod (15 mod 10));
  raise notice '%', 7%(9 mod (15% 10));
  -- mix: mod function
  raise notice '%', 7 mod mod(20,16);
  raise notice '%', 7 mod (mod(25,16) mod 5);
  raise notice '%', 7 mod (mod(51 mod 26, mod(33,17)) mod 5);
  raise notice '%', mod(15,8) mod (mod(51 mod 26, mod(33,17)) mod 5);
  -- mix: mod var
  raise notice '%', 7 mod mod;
  raise notice '%', 7 mod mod mod 3;
  raise notice '%', 7 mod mod mod mod(15 mod 8, mod);
  raise notice '%', mod    (15 mod 8, mod) mod(mod);
  -- type conversion
  raise notice '%', 7 mod '3';
  raise notice '%', '5' mod 3;
  raise notice '%', 4.5 mod '2';
  raise notice '%', '5' mod mod;
  raise notice '%', '1e2'::numeric mod 30;
END;
$$
language default_plsql;
call op_mod_test_20230530();

CREATE or replace PROCEDURE op_mod_test_20230530(a in integer default 5 mod 3)
LANGUAGE default_plsql
AS $$
DECLARE
  i1 int;
  i3 int default 17mod 9mod 5; -- 3
  i4 int;
  mod int default 20mod 11%5;  -- 4
BEGIN
  raise notice 'in: %', a;
  raise notice 'i3: %', i3;
  raise notice 'mod: %', mod;
  -- space
  i1 := 17 mod 9 mod 5;
  raise notice 'i1: %', i1;
  raise notice 'i1: %', 17mod 9mod 5;
  -- brackets
  raise notice '%', 17mod(9);
  raise notice '%', 17mod (9);
  raise notice '%', 17mod (9);
  raise notice '%', 7mod(9mod (15mod 10));
  raise notice '%', 7%(9mod (15% 10));
  -- mix: mod function
  raise notice '%', 7mod mod(20,16);
  raise notice '%', 7mod (mod(25,16) mod 5);
  raise notice '%', 7mod (mod(51 mod 26, mod(33,17)) mod 5);
  raise notice '%', mod(15,8) mod (mod(51 mod 26, mod(33,17)) mod 5);
  -- mix: mod var
  raise notice '%', 7mod mod;
  raise notice '%', 7mod mod mod 3;
  raise notice '%', 7mod mod mod mod(15 mod 8, mod);
  raise notice '%', mod    (15 mod 8, mod) mod(mod);
  -- type conversion
  raise notice '%', 7 mod 5;
  raise notice '%', 7mod 5;
  raise notice '%', 7 mod '3';
  raise notice '%', '5' mod 3;
  raise notice '%', 4.5 mod '2';
  raise notice '%', '5' mod mod;
  raise notice '%', '1e2'::numeric mod 30;
END;
$$;
call op_mod_test_20230530();

drop procedure op_mod_test_20230530;
------------------------------------------
-- Test cases for operator ||.
------------------------------------------
\c regression_ora
--TestPoint:1. test record.field using ||
create table table_b_20230316(id int,name varchar2(20));
insert into table_b_20230316 values(1,'da');
insert into table_b_20230316 values(2, 'ha');
declare
b1 varchar2(300);
rec record;
begin
for rec in (select * from table_b_20230316)
loop
 raise notice '%',rec.name;
 b1:=rec.name||rec.name||rec.name||rec.name;
 raise notice '%',b1;
 b1:=rec.name||(rec.name||rec.name||(rec.name||rec.name||(rec.name||rec.name)))||rec.name;
 raise notice '%',b1;
end loop;
end;
/

drop table table_b_20230316;

set timezone = 'PRC';
-- concat timestamp/timestamptz and text
select ('2021-01-01 14:38:48.680297'::timestamptz);
select '2021-01-01 14:38:48.680297'::timestamp || '';
select '2021-01-01 14:38:48.680297'::timestamptz || '';
select '2021-01-01 14:38:48.680297'::timestamp || 'abc';
select '2021-01-01 14:38:48.680297'::timestamptz || 'abc';
select '' || '2021-01-01 14:38:48.680297'::timestamp;
select '' || '2021-01-01 14:38:48.680297'::timestamptz;
select 'abc' || '2021-01-01 14:38:48.680297'::timestamp;
select 'abc' || '2021-01-01 14:38:48.680297'::timestamptz;
select '2021-01-01 14:38:48.680297'::timestamp || '2021-01-01 14:38:48.680297'::timestamptz;
select '2021-01-01 14:38:48.680297'::timestamptz || '2021-01-01 14:38:48.680297'::timestamp;
select '2021-01-01 14:38:48.680297'::timestamp || '2021-01-01 14:38:48.680297'::timestamp;
select '2021-01-01 14:38:48.680297'::timestamptz || '2021-01-01 14:38:48.680297'::timestamptz;

-- concat timestamp/timestamptz and date
select '2021-01-01 14:38:48'::timestamp || '2021-01-01 14:38:48'::date;
select '2021-01-01 14:38:48'::date || '2021-01-01 14:38:48'::timestamp;
select '2020-01-01 01:00:01-08'::timestamptz || '2021-01-01 14:38:48'::date;
select '2021-01-01 14:38:48'::date || '2021-01-01 14:38:48'::timestamptz;

-- concat date and text
select '2021-01-01 14:38:48'::date || '';
select '2021-01-01 14:38:48'::date || 'abc';
select '' || '2021-01-01 14:38:48'::date;
select 'abc' || '2021-01-01 14:38:48'::date;
select '2021-01-01 14:38:48'::date || '2021-01-01 14:38:48'::date;

-- concat interval and text
drop table if exists intervaltest_20230628 cascade;
create table intervaltest_20230628(id int,c1 interval, c2 timestamp, c3 timestamptz, c4 date);
insert into intervaltest_20230628 values(1,'2 year 2 month', '2023-06-28 12:12:12','2023-06-28 12:12:12','2023-06-28 12:12:12');
select c1 || c1 from intervaltest_20230628;
select c1 || c2 from intervaltest_20230628;
select c2 || c1 from intervaltest_20230628;
select c1 || c3 from intervaltest_20230628;
select c3 || c1 from intervaltest_20230628;
select c1 || c4 from intervaltest_20230628;
select c4 || c1 from intervaltest_20230628;
select c1 || '_c4' from intervaltest_20230628;
select c1 || '' from intervaltest_20230628;
select 'c4_' || c1 from intervaltest_20230628;
select '' || c1 from intervaltest_20230628;

drop table intervaltest_20230628;

-- test timestamptz || raw
select '2021-01-01 14:38:48'::timestamptz|| hextoraw('') from dual;
select '2021-01-01 14:38:48'::timestamptz|| hextoraw('12') from dual;
select '2023-06-23 18:00:00'::timestamptz || hextoraw('12') from dual;

-- test text || column, column is null
drop table if exists concat_test_table_20230628 cascade;
create table concat_test_table_20230628(
c1 bigint,
c2 bigserial,
c3 bit(5),
c4 bit varying(10),
c5 boolean,
c6 box,
c7 bytea,
c8 character(10),
c9 character varying(20),
c10 cidr,
c11 circle,
c12 date,
c13 double precision,
c14 inet,
c15 integer,
c16 interval,
c17 json,
c19 line,
c20 lseg,
c21 macaddr,
c22 macaddr8,
c23 money,
c24 numeric(10,4),
c25 path,
c26 pg_lsn,
c27 point,
c28 polygon,
c29 real,
c30 smallint,
c31 smallserial,
c32 serial,
c33 text,
c34 time(6) without time zone,
c35 time(6) with time zone,
c36 timestamp(6) without time zone,
c37 timestamp(6) with time zone,
c38 tsquery,
c39 tsvector,
c40 txid_snapshot,
c41 uuid,
c42 decimal,
c43 NCHAR,
c44 VARCHAR2,
c45 NVARCHAR2,
c46 NUMBER,
c47 BINARY_FLOAT,
c48 BINARY_DOUBLE,
c49 INTERVAL,
c50 CLOB ,
c52 BLOB,
c53 RAW(100)
);
insert into concat_test_table_20230628(c1) values(1);
select '01'||c1, '01'||c2, '01'||c3, '01'||c4, '01'||c5,
'01'||c6, '01'||c7, '01'||c8, '01'||c9, '01'||c10,
'01'||c11, '01'||c12, '01'||c13, '01'||c14, '01'||c15,
'01'||c16, '01'||c17, '01'||c19, '01'||c20, '01'||c21,
'01'||c22, '01'||c23, '01'||c24, '01'||c25, '01'||c26,
'01'||c27, '01'||c28, '01'||c29, '01'||c30, '01'||c31,
'01'||c32, '01'||c33, '01'||c34, '01'||c35, '01'||c36,
'01'||c37, '01'||c38, '01'||c39, '01'||c40, '01'||c41,
'01'||c42, '01'||c43, '01'||c44, '01'||c45, '01'||c46,
'01'||c47, '01'||c48, '01'||c49, '01'||c50,
'01'||c52, '01'||c53, '01' from concat_test_table_20230628;
drop table concat_test_table_20230628;

-- test text || raw
declare
	a long raw := '01';
begin
	raise notice '%','r = ' || a;
end;
/

declare
	a raw(20) := '01';
begin
	raise notice '%','r = ' || a;
end;
/

-- concat number with +/-
select 400||-100 from dual;
select 400||+100 from dual;
select '400'||-100 from dual;
select '400'||+100 from dual;
select 130 || 140 as aaaa from dual;
select 130 || -140 as aaaa from dual;
select 130.49 || -140 as aaaa from dual;
select 130 || -140.212 as aaaa from dual;
select 130 || +140 as aaaa from dual;
select 130.49 || +140 as aaaa from dual;
select 130 || +140.212 as aaaa from dual;
select -1303.33333 || -140.212 as aaaa from dual;
select -1303.33333::float8 || -140.212::float8 as aaaa from dual;
select -1303.33333::float4 || -140.212::float8 as aaaa from dual;
select -1303.33333::float8 || -140.212::float4 as aaaa from dual;
select -1303.33333::float4 || -140.212::float4 as aaaa from dual;
select -1303.33333::float8 || +140.212::float8 as aaaa from dual;
select -1303.33333::float4 || +140.212::float8 as aaaa from dual;
select -1303.33333::float8 || +140.212::float4 as aaaa from dual;
select -1303.33333::float4 || +140.212::float4 as aaaa from dual;
select 'wwwww'|| -153.33 as aaaa from dual;
select 'wwwww'|| (-153.33)::text as aaaa from dual;

-- concat two numeric
drop table if exists too_cat_num1_20230628 cascade;
create table too_cat_num1_20230628(f1 number,f2 number);
insert into too_cat_num1_20230628 values(1,2);
select f1||f2 as aa from too_cat_num1_20230628;
drop table too_cat_num1_20230628;

-- concat null and other type
select 'a' || ascii(null) || 'b';
select 'a' || substr(null,2,3) || 'b';
select 'a' || null || 'b';
select 1000 || null || 2000 from dual;
select  null || 2000 from dual;
select 1000.11 || null from dual;
select 1000.11 || null|| 2.00 from dual;
select INSTR('abc', 'bc')||INSTR('abc', '')||'avc' from dual;
select 1||INSTR('abc', '')||'avc' from dual;
select INSTR('abc', 'bc')||INSTR('abc', '')||INSTR('abc', 'bc') from dual;
select 7*16+13||ascii(null)||123 from dual;
select ascii(null)||7*16+13||123 from dual;
select 7*16+13||ascii(null) from dual;
select ascii(null) || ascii('1') || ascii(null) from dual;

select 'fat & rat'::tsquery || ''::tsquery;
select ''::tsquery||'fat & rat'::tsquery;
select ''::tsquery||''::tsquery;
select 'fat & rat'::tsquery || null::tsquery;
select null::tsquery||'fat & rat'::tsquery;
select null::tsquery||null::tsquery;
select 'fat & rat'::tsquery || 'lat & eat'::tsquery;

select 'a fat cat'::tsvector || ''::tsvector;
select ''::tsvector||'a fat cat'::tsvector;
select ''::tsvector||''::tsvector;
select 'a fat cat'::tsvector || null::tsvector;
select null::tsvector||'a fat cat'::tsvector;
select null::tsvector||null::tsvector;
select 'a fat cat'::tsvector || 'eat apple'::tsvector;

select '1101'::varbit || ''::varbit;
select ''::varbit||'1101'::varbit;
select ''::varbit||''::varbit;
select '1101'::varbit || null::varbit;
select null::varbit||'1101'::varbit;
select null::varbit||null::varbit;
select '1101'::varbit || '101'::varbit;

select '1'::bytea || ''::bytea;
select ''::bytea||'1'::bytea;
select ''::bytea||''::bytea;
select '1'::bytea || null::bytea;
select null::bytea||'1'::bytea;
select null::bytea||null::bytea;
select '1'::bytea || '2'::bytea;

-- concat null and ''
select NULL || NULL is NULL;
select '' || '' is NULL;
select '' || NULL is NULL;
select concat(NULL, NULL) is NULL;
select concat('', '') is NULL;
select concat('', NULL) is NULL;

-- concat keywords
-- PARTITIONS
drop table if exists partitions_int_20230628;
create table partitions_int_20230628(partitions integer, col text);
insert into partitions_int_20230628 values (1, 'r1'), (2, 'r2'), (3, 'r3');
select 'aaa'|| partitions ||'bbbb' as result from partitions_int_20230628 order by 1;
select 'aaa'||'bbbb' as partitions from dual;
select 'aaabbbb' as partitions from dual;
select ('aaa'||'bbbb') partitions from dual;
drop table partitions_int_20230628;

-- REPLACE
select 'abc'||replace(null,'a','b')||'d' from dual;
select 'abc'||replace('aaa','a','0')||'d' from dual;
select 'abc'||replace('','a','0')||'d' from dual;
select 'aaa' replace from dual;

-- NULLIF
select 'a'||NULLIF('', '1')||'b' from dual;
select 'a'||NULLIF('null', 'null')||'b' from dual;
select 'a'||NULLIF('null', '')||'b' from dual;
select 'a'||NULLIF('', '')||'b' from dual;
select 'a'||NULLIF('2', '')||'b' from dual;
select 'a'||NULLIF('2', '2')||'b' from dual;
select 'aaa' nullif from dual;

-- GREATEST and LEAST
select 'a'||least('11','22','33') from dual;
select 'a'||least('','','') from dual;
select 'a'||greatest('11','22','33') from dual;
select 'a'||greatest('','','') from dual;
select 'a' greatest from dual;
select 'a' least from dual;

-- SUBSTRING
select '2021-01-01 14:38:48.680297'::timestamp ||'----'||substring('happy children day!', 1, 5) as aaa from dual;
select 'aaa'||'bbbb' as substring from dual;
select 'aaabbbb' as substring from dual;
select ('aaa'||'bbbb') substring from dual;

-- NAMES
drop procedure if exists tblt1_20230628 cascade;
create or replace procedure tblt1_20230628(names varchar2 default 'abc') as
begin
	raise notice '%', 'name is default values:'||names;
end;
/
call tblt1_20230628();
drop procedure tblt1_20230628;
select names || '' || 'abc'||names from (select 1.111 as names from dual) as t;
select 'a' names from dual;

-- OLD
select 'a' || old;
select 'a' || old.x;
select old || '' || 'abc'||old from (select 1.111 as old from dual) as t;
select old.old || '' || 'abc'||old.old from (select 1.111 as old from dual) as old;
select 'a' old from dual;

drop function if exists f_trigger_hr_salary_items_20230628;
create or replace function f_trigger_hr_salary_items_20230628() returns trigger as
   log_content text;
begin
  if (tg_op = 'update') then
  log_content := 'item_id:'||new.item_id ;
  end if;
  return null;
end;
/

create or replace function f_trigger_hr_salary_items_20230628() returns trigger as
   log_content text;
begin
  if (tg_op = 'update') then
  log_content := 'item_id:'||old.item_id ;
  end if;
  return null;
end;
/
drop function f_trigger_hr_salary_items_20230628;

-- EVENT
select event || '' || 'abc'||event from (select 1.111 as event from dual) as t;
select 'a' event from dual;

drop table if exists olympic_medal_winners_20230628;
create table olympic_medal_winners_20230628 (
    olympic_year int,
    sport   varchar2( 30 ),
    gender  varchar2( 1 ),
    event   varchar2( 128 ),
    medal   varchar2( 10 ),
    noc varchar2( 3 ),
    athlete varchar2( 128 )
);

Insert into olympic_medal_winners_20230628 (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Individual','Gold','KOR','KU Bonchan');
Insert into olympic_medal_winners_20230628 (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Individual','Silver','FRA','VALLADONT Jean-Charles');
Insert into olympic_medal_winners_20230628 (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Individual','Bronze','USA','ELLISON Brady');
Insert into olympic_medal_winners_20230628 (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Team','Gold','KOR','Republic of Korea');
Insert into olympic_medal_winners_20230628 (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Team','Bronze','AUS','Australia');
Insert into olympic_medal_winners_20230628 (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Team','Silver','USA','United States');
Insert into olympic_medal_winners_20230628 (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Floor Exercise','Gold','GBR','WHITLOCK Max');
Insert into olympic_medal_winners_20230628 (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Floor Exercise','Bronze','BRA','MARIANO Arthur');
Insert into olympic_medal_winners_20230628 (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Floor Exercise','Silver','BRA','HYPOLITO Diego');

select noc || event from olympic_medal_winners_20230628 order by 1;
drop table olympic_medal_winners_20230628;

\c postgres
select 'fat & rat'::tsquery || ''::tsquery;
select ''::tsquery||'fat & rat'::tsquery;
select ''::tsquery||''::tsquery;
select 'fat & rat'::tsquery || null::tsquery;
select null::tsquery||'fat & rat'::tsquery;
select null::tsquery||null::tsquery;
select 'fat & rat'::tsquery || 'lat & eat'::tsquery;

select 'a fat cat'::tsvector || ''::tsvector;
select ''::tsvector||'a fat cat'::tsvector;
select ''::tsvector||''::tsvector;
select 'a fat cat'::tsvector || null::tsvector;
select null::tsvector||'a fat cat'::tsvector;
select null::tsvector||null::tsvector;
select 'a fat cat'::tsvector || 'eat apple'::tsvector;

select '1101'::varbit || ''::varbit;
select ''::varbit||'1101'::varbit;
select ''::varbit||''::varbit;
select '1101'::varbit || null::varbit;
select null::varbit||'1101'::varbit;
select null::varbit||null::varbit;
select '1101'::varbit || '101'::varbit;

select 'a'||3.23;

\c regression_ora
set IntervalStyle = 'sql_standard';
set TimeZone='GMT';

--test date mi
select '2023-7-18 12:00:00'::date - '2023-7-18 18:00:00'::date;
select '2023-7-19 18:00:00'::date - '2023-7-18 12:00:00'::date;
--test timestamp mi
select '2023-7-19 18:00:00.123456'::timestamp - '2023-7-18 12:00:00.123455'::timestamp;
select '2023-7-18 12:00:00.123455'::timestamp - '2023-7-19 18:00:00.123456'::timestamp;
--test timestamptz mi
select '2023-7-19 18:00:00.123456 +00:08'::timestamptz - '2023-7-19 18:00:00.123455 +00:07'::timestamptz;
select '2023-7-20 5:00:00.123456 +00:08'::timestamptz - '2023-7-19 18:00:00.123455 +00:07'::timestamptz;
--test timestamptz mi
select '2023-7-19 18:00:00.123456'::timestamptz - '2023-7-19 18:00:00.123455'::timestamptz;
select '2023-7-20 5:00:00.123456'::timestamptz - '2023-7-19 18:00:00.123455'::timestamptz;
--test date mi timestamptz
select '2023-7-18 12:00:00'::date - '2023-7-18 12:00:00 +00:02'::timestamptz;
--test timestamptz mi date
select '2023-7-18 12:00:00 +00:12'::timestamptz - '2023-7-18 12:00:00'::date;
--test date mi timestamptz
select '2023-7-18 12:00:00'::date - '2023-7-18 12:00:00'::timestamptz;
--test timestamptz mi date
select '2023-7-18 12:00:00'::timestamptz - '2023-7-18 12:00:00'::date;
--test date mi timestamp
select '2023-7-18 12:00:00'::date - '2023-7-18 12:00:00.123456'::timestamp;
--test timestamp mi date
select '2023-7-18 12:00:00.123456'::timestamp - '2023-7-18 12:00:00'::date;
--test timestamp mi timestamptz
select '2023-7-18 12:00:00.123456'::timestamp - '2023-7-18 12:00:00.123455 +00:08'::timestamptz;
--test timestamptz mi timestamp
select '2023-7-18 12:00:00.123455 + 00:08'::timestamptz - '2023-7-18 12:00:00.123456'::timestamp;
--test timestamp mi timestamptz
select '2023-7-18 12:00:00.123456'::timestamp - '2023-7-18 12:00:00.123455'::timestamptz;
--test timestamptz mi timestamp
select '2023-7-18 12:00:00.123455'::timestamptz - '2023-7-18 12:00:00.123456'::timestamp;
--test timestamptz mi timestamptz
select '2023-7-18 12:00:00.123455'::timestamptz - '2023-7-18 12:00:00.123456 +09:00'::timestamptz;

--test datetime mi/pl interval
reset IntervalStyle;
select '2022-1-1 12:00:00'::date - interval'1month 1days -12:00:00';
select '2022-1-1 12:00:00'::date + interval'1month 1days -12:00:00';
select '2022-1-1 12:00:00'::timestamp - interval'1month 1days -12:00:00';
select '2022-1-1 12:00:00'::timestamp + interval'1month 1days -12:00:00';

select '2022-1-1 12:00:00.000000 +08:00'::timestamptz - interval'1month 1days -12:00:00';
select '2022-1-1 12:00:00.000000 +08:00'::timestamptz + interval'1month 1days -12:00:00';
select '2022-1-1 12:00:00.000000'::timestamptz - interval'1month 1days -12:00:00';
select '2022-1-1 12:00:00.000000'::timestamptz + interval'1month 1days -12:00:00';


select '2022-3-28 01:00:00'::date - interval '1 year 1 month 1 day 1 hour 1 minute 864001 second';
select '2022-3-28 01:00:00'::timestamp - interval '1 year 1 month 1 day 1 hour 1 minute 864001 second';

set IntervalStyle = 'sql_standard';
--test text +.-.*./
select '1' + '10';
select '1' - '10';
select '1' / '10';
select '1' * '10';
select '1'::text + '10'::text;
select '1'::text - '10'::text;
select '1'::text / '10'::text;
select '1'::text * '10'::text;
--test return type
create table typ_tab as select '2022-1-1'::date - '2022-1-2'::date;
\d typ_tab;
drop table typ_tab;
create table typ_tab as select '2022-1-1'::date - 2;
\d typ_tab;
drop table typ_tab;
create table typ_tab as select '2022-1-1'::timestamp - '2022-1-1'::timestamp;
\d typ_tab;
drop table typ_tab;
create table typ_tab as select '2022-1-1'::timestamptz - '2022-1-1'::timestamp;
\d typ_tab;
drop table typ_tab;
create table typ_tab as select '2022-1-1'::timestamptz - '2022-1-1'::timestamptz;
\d typ_tab;
drop table typ_tab;
create table typ_tab as select '2022-1-1'::timestamp - '2022-1-1'::date;
\d typ_tab;
drop table typ_tab;
create table typ_tab as select '2022-1-1'::timestamptz - '2022-1-1'::date;
\d typ_tab;
drop table typ_tab;
create table typ_tab as select '2022-1-1'::date - '2022-1-1'::timestamp;
\d typ_tab;
drop table typ_tab;
create table typ_tab as select '2022-1-1'::date - '2022-1-1'::timestamptz;
\d typ_tab;
drop table typ_tab;
create table typ_tab as select '2022-1-1'::date - '2022-1-1'::date;
\d typ_tab;
drop table typ_tab;

drop table if exists t_date;
create table t_date (i1 date, i2 timestamp, i3 timestamptz, i4 timestamptz);
set timezone='+08';
insert into t_date values ('2022-1-1 12:00:00','2022-1-1 12:00:00.000000', '2022-1-1 12:00:00.000000 +09:00', '2022-1-1 12:00:00');

set timezone='+08';
select i1 - i2, i2 - i1 from t_date;
select i1 - i3, i3 - i1 from t_date;
select i1 - i4, i4 - i1 from t_date;
select i2 - i3, i3 - i2 from t_date;
select i2 - i4, i4 - i2 from t_date;
select i3 - i4, i4 - i3 from t_date;
select i1 - 2, i1 + 2 from t_date;


set timezone='+09';
select i1 - i2, i2 - i1 from t_date;
select i1 - i3, i3 - i1 from t_date;
select i1 - i4, i4 - i1 from t_date;
select i2 - i3, i3 - i2 from t_date;
select i2 - i4, i4 - i2 from t_date;
select i3 - i4, i4 - i3 from t_date;
select i1 - 2, i1 + 2 from t_date;


set timezone='+10';
select i1 - i2, i2 - i1 from t_date;
select i1 - i3, i3 - i1 from t_date;
select i1 - i4, i4 - i1 from t_date;
select i2 - i3, i3 - i2 from t_date;
select i2 - i4, i4 - i2 from t_date;
select i3 - i4, i4 - i3 from t_date;
select i1 - 2, i1 + 2 from t_date;


set timezone='+07';
select i1 - i2, i2 - i1 from t_date;
select i1 - i3, i3 - i1 from t_date;
select i1 - i4, i4 - i1 from t_date;
select i2 - i3, i3 - i2 from t_date;
select i2 - i4, i4 - i2 from t_date;
select i3 - i4, i4 - i3 from t_date;
select i1 - 2, i1 + 2 from t_date;


drop table t_date;

reset IntervalStyle;
reset TimeZone;
--test comparation operator > < <> =
--test cmp between timestamp  and text
create table timestamp_txt(
      c1 timestamp,
      c2 text
);
insert into timestamp_txt values ('2023-1-1 00:00:00', '1-1-23 10:00:00');
insert into timestamp_txt values ('2023-1-1 01:00:00', '1-1-23  10:00:00');
insert into timestamp_txt values ('2023-1-1 02:00:00', '1-1-23  10:00:00');
insert into timestamp_txt values ('2023-1-1 03:00:00', '1-1-23 03:00:00');
select c1 >= c2 from timestamp_txt order by c1;
select c1 = c2 from timestamp_txt order by c1;
select c1 < c2 from timestamp_txt order by c1;
select c1 <> c2 from timestamp_txt order by c1;
select * from timestamp_txt where c1 >= c2 order by c1;
select * from timestamp_txt where c1 < c2 order by c1;
select * from timestamp_txt where c1 = c2 order by c1;
select * from timestamp_txt where c1 <> c2 order by c1;
drop table timestamp_txt;
select to_char('1-1-22 00:00:00') > localtimestamp;
select to_char('1-1-22 00:00:00') < localtimestamp;
select to_char('1-1-22 00:00:00') <> localtimestamp;
select to_char('1-1-22 00:00:00') <= localtimestamp;
select to_char('1-1-22 00:00:00') <= localtimestamp;
select to_char('100') >= localtimestamp; --error
--test cmp between date and text
create table date_txt(
    c1 date,
    c2 text
);

insert into date_txt values ('2023-1-1', '1-1-24');
insert into date_txt values ('2023-1-1', '1-1-23');
insert into date_txt values ('2023-1-1', '1-3-23');
insert into date_txt values ('2023-1-1', '1-1-22');

select c1 >= c2 from date_txt order by c1;
select c1 = c2 from date_txt order by c1;
select c1 < c2 from date_txt order by c1;
select c1 <> c2 from date_txt order by c1;
select * from date_txt where c1 >= c2 order by c1;
select * from date_txt where c1 <> c2 order by c1;
select * from date_txt where c1 > c2 order by c1;
select * from date_txt where c1 = c2 order by c1;
select to_char('1-1-22') > sysdate;
select to_char('1-1-22') < sysdate;
select to_char('1-1-22') <> sysdate;
select to_char('1-1-22') <= sysdate;
select to_char('1-1-22') >= sysdate;
select to_char('100') >= sysdate; --error
drop table date_txt;
--test Arithmetic operator - + * /
create table op_timestamp_txt (
    c1 timestamp,
    c2 text,
    c3 numeric default 5
);

insert into op_timestamp_txt values ('2023-1-1 00:00:00', '5');
insert into op_timestamp_txt values ('2023-1-1 01:00:00', '6');
insert into op_timestamp_txt values ('2023-1-1 02:00:00', '7');
insert into op_timestamp_txt values ('2023-1-1 03:00:00', '8');
insert into op_timestamp_txt values ('2023-1-1 03:00:00', '9');

select to_char(c2) - c1, c1 - c3 from op_timestamp_txt order by c1; --error
select '1-1-22 00:00:00'::timestamp - '2';
select '1-1-22' - '1-1-22 00:00:00'::timestamp;
select '1-1-22 00:00:00'::timestamp - '1-1-23 00:00:00'; --error
drop table op_timestamp_txt;
create table op_date_txt (
    c1 date,
    c2 text,
    c3 numeric default 5
);

insert into op_date_txt values ('2023-1-1 00:00:00', '1');
insert into op_date_txt values ('2023-1-1 01:00:00', '2');
insert into op_date_txt values ('2023-1-1 02:00:00', '3');
insert into op_date_txt values ('2023-1-1 03:00:00', '4');
insert into op_date_txt values ('2023-1-1 03:00:00', '5');

select to_char(c2) - c1, c1 - c3 from op_date_txt order by c1;

select '2' - current_date; --error
drop table op_date_txt;
\c postgres

create table timestamp_txt(
    c1 timestamp,
    c2 text
);
insert into timestamp_txt values ('2023-1-1 00:00:00', '1-1-23 10:00:00');
insert into timestamp_txt values ('2023-1-1 01:00:00', '1-1-23 10:00:00');
insert into timestamp_txt values ('2023-1-1 02:00:00', '1-1-23 10:00:00');
insert into timestamp_txt values ('2023-1-1 03:00:00', '1-1-23 03:00:00');

set enable_lightweight_ora_syntax = on;
select c1 >= c2 from timestamp_txt order by c1;
select c1 = c2 from timestamp_txt order by c1;
select c1 < c2 from timestamp_txt order by c1;
select c1 <> c2 from timestamp_txt order by c1;
select * from timestamp_txt where c1 >= c2 order by c1;
select * from timestamp_txt where c1 < c2 order by c1;
select * from timestamp_txt where c1 = c2 order by c1;
select * from timestamp_txt where c1 <> c2 order by c1;
drop table timestamp_txt;
select '1-1-22 00:00:00'::text > localtimestamp;
select '1-1-22 00:00:00'::text < localtimestamp;
select '1-1-22 00:00:00'::text <> localtimestamp;
select '1-1-22 00:00:00'::text <= localtimestamp;
select '1-1-22 00:00:00'::text <= localtimestamp;
select '100'::text >= localtimestamp; --error
--test cmp between date and text
create table date_txt(
    c1 date,
    c2 text
);
insert into date_txt values ('2023-1-1', '1-1-24');
insert into date_txt values ('2023-1-1', '1-1-23');
insert into date_txt values ('2023-1-1', '1-3-23');
insert into date_txt values ('2023-1-1', '1-1-22');

select c1 >= c2 from date_txt order by c1;
select c1 = c2 from date_txt order by c1;
select c1 < c2 from date_txt order by c1;
select c1 <> c2 from date_txt order by c1;
select * from date_txt where c1 >= c2 order by c1;
select * from date_txt where c1 <> c2 order by c1;
select * from date_txt where c1 > c2 order by c1;
select * from date_txt where c1 = c2 order by c1;
select '1-1-22'::text > current_date;
select '1-1-22'::text < current_date;
select '1-1-22'::text <> current_date;
select '1-1-22'::text <= current_date;
select '1-1-22'::text >= current_date;
select '100'::text >= current_date;
drop table date_txt;
--test Arithmetic operator - + * /
create table op_timestamp_txt (
    c1 timestamp,
    c2 text,
    c3 numeric default 5
);

insert into op_timestamp_txt values ('2023-1-1 00:00:00', '7');

select c1 - c2, c1 - c3 from op_timestamp_txt order by c1;
select c2 - c1, c1 - c3 from op_timestamp_txt order by c1;
select c1 + c2 from op_timestamp_txt order by c1;
select c2 + c1 from op_timestamp_txt order by c1;
select '1-1-22 01:00:00'::timestamp - '2';
select '1-1-22'::text - '1-1-22'::timestamp;
select '1-1-23 00:00:00'::timestamp - '1-1-22 00:00:00';
drop table op_timestamp_txt;
create table op_date_txt (
    c1 date,
    c2 text,
    c3 numeric default 5
);

insert into op_date_txt values ('2023-1-1 00:00:00', '1');
insert into op_date_txt values ('2023-1-1 01:00:00', '2');
insert into op_date_txt values ('2023-1-1 02:00:00', '3');
insert into op_date_txt values ('2023-1-1 03:00:00', '4');
insert into op_date_txt values ('2023-1-1 03:00:00', '5');

select c1 - c2, c1 - c3 from op_date_txt order by c1;
select c2 - c1, c1 - c3 from op_date_txt where c2 = '1';
select '1-1-22'::date - '2';
select '2' - current_date; --error
select '1-1-22' - '1-1-22'::date;
drop table op_date_txt;
set enable_lightweight_ora_syntax = off;
\c regression_ora
set IntervalStyle = 'postgres'; 
set timezone = '+05:00';
select systimestamp - current_timestamp from dual;
-- test cross-type datetime join
create table t1_20231123(id int, c1 date);
create table t2_20231123(id int, c2 timestamp);
insert into t1_20231123 values(1, '2023-11-23 12:00:00');
insert into t2_20231123 values(1, '2023-11-23 12:00:00');
select * from t1_20231123 a where exists (select 1 from t2_20231123 b where a.id = b.id and a.c1 = b.c2);
drop table t1_20231123;
drop table t2_20231123;
