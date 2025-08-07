/*
 * test case for opentenbase_ora interval year(n) day(n)
 * 1. interval year to month
 *  1.1 year(0)xx - year(9)xx
 *   1.1.1 create table/insert/alter table
 *   1.1.2 type cast
 *  1.2 month/year
 *   1.2.1 create table/insert/alter table
 * 2. interval day to second
 *  2.1 day(0)xx - day(9)xx
 *   2.1.1 create table/insert/alter table
 *   2.1.2 type cast
 *  2.2 minute/second/hour
 *   2.2.1 create table/insert/alter table
 * 3. plsql testing
 * 4. lateral testing
 */

-- test case for opentenbase_ora interval year(n) day(n)
-- 1. interval year to month
--  1.1 year(0)xx - year(9)xx
--   1.1.1 create table/insert/alter table
\c regression_ora
set intervalstyle='sql_standard';

-- year
create table ty(y1 int, y2 interval year);
insert into ty values(1, '12');
select * from ty;
select interval '12' year;
select interval '120' year;
select interval '12' year(12);
select interval '12' year(2);
\d ty
drop table ty;

-- year(3)
create table ty(y1 int, y2 interval year(10));
drop table ty;
create table ty(y1 int, y2 interval year(3));
insert into ty values(1, '23');
select * from ty;
drop table ty;

-- alter table
create table ty(y1 int, y2 interval year(3));
insert into ty values(1, '23');
select * from ty;
alter table ty alter y2 type interval year(4);
select * from ty;
\d ty
drop table ty;

-- as shard key
create table ty(y1 interval year);
insert into ty values('12');
drop table ty;
create table ty(y1 interval year(2));
insert into ty values('12');
drop table ty;

--   1.1.2 type cast
select cast(interval '12' year(2) as interval day to second(2));

--  1.2 month/year
--   1.2.1 create table/insert/alter table
-- month
create table ty(y1 int, y2 interval month);
\d ty
insert into ty values(1, '3');
select * from ty;
drop table ty;

-- year to month - like opentenbase_ora (default)
create table ty(y1 int, y2 interval year to month);
\d ty
insert into ty values(1, '21-1');
select * from ty;
drop table ty;

-- year(n) to month
create table ty(y1 int, y2 interval year(4) to month);
\d ty
insert into ty values(1, '21-1');
select * from ty;
alter table ty alter y2 type interval year(1) to month;
alter table ty alter y2 type interval year(5) to month;
\d ty
select * from ty;
drop table ty;

-- 2. interval day to second
--  2.1 day(0)xx - day(9)xx
--   2.1.1 create table/insert/alter table
-- day -- default
create table td(a int, b interval day);
\d td
insert into td values(1, interval '12' day);
select * from td;
drop table td;

create table td(a int, b interval day(3));
\d td
insert into td values(1, interval '12' day);
select * from td;
drop table td;
-- hour
create table td(a int, b interval hour);
\d td
insert into td values(1, interval '12' hour);
select * from td;
drop table td;
-- minute
create table td(a int, b interval minute);
\d td
insert into td values(1, interval '12' minute);
select * from td;
drop table td;
-- second
create table td(a int, b interval second);
\d td
insert into td values(1, interval '12.001' second);
select * from td;
drop table td;
-- hour to second
create table td(a int, b interval hour to second);
\d td
insert into td values(1, interval '12 21:23:30.001' hour to second);
select * from td;
drop table td;

-- day to hour
create table td(a int, b interval day to hour);
\d td
insert into td values(1, interval '12 03' day to hour);
select * from td;
drop table td;
-- day to minute
create table td(a int, b interval day to minute);
\d td
insert into td values(1, interval '12 03:23:23' day to minute);
select * from td;
drop table td;
-- day to second - as opentenbase_ora
create table td(a int, b interval day to second);
\d td
insert into td values(1, interval '12 03:23:23.28' day to second);
select * from td;
drop table td;

create table td(a int, b interval day(4) to second);
\d td
insert into td values(1, interval '12 03:23:23.28' day to second);
select * from td;
select interval '12' day(4) to second;
drop table td;

create table td1(b interval day(9) to second);
\d td1
insert into td1 select i * interval '1 day' from generate_series(1, 5000) i;
select count(*) from td1;
drop table td1;

drop table if exists td2;
create table td2(c32 interval default '1 day') distribute by shard(c32);
insert into td2 select i * interval '1 day' from generate_series(1, 500) i;
select count(1) from td2;
drop table td2;

-- alter table
create table td(a int, b interval day(4) to second);
\d td
insert into td values(1, interval '12 03:23:23.28' day to second);
select * from td;
alter table td alter column b type interval day(1) to second;
alter table td alter column b type interval day(5) to second;
\d td
select * from td;
drop table td;

--   2.1.2 type cast
select cast(interval '12' day(4) to second as interval day(2) to second);

--  2.2 minute/second/hour
--   2.2.1 create table/insert/alter table
-- tested above
-- 3. plsql testing
create or replace function demo18() returns void as $$
declare
  v_d1 interval day to second ;
  v_d2 interval year to month;
  v_d13 interval day(3) to second ;
  v_d24 interval year(4) to month;
begin
  v_d1 = interval '99 15:15:14.1234567' day to second;
  v_d2 = interval '12-03' year to month;
  v_d13 = interval '1 13:23:23.132' day(3) to second;
  v_d24 = interval '12-03' year(4) to month;
  raise notice 'v_d1=% vd_d2=% v_d13=% v_d24=%',v_d1,v_d2,v_d13,v_d24;
end; $$ language default_plsql;
select demo18();

create or replace function demo19() returns interval day(3) to second as $$
declare
begin
  return interval '99 15:15:14.1234567' day to second;
end; $$ language default_plsql;
select demo19();

-- 4. lateral testing
-- day(n) range
select interval '12' day(0);
select interval '0' day(0);
select interval '12' day(9);
select interval '12' day(10);

select interval '12' hour;
select interval '12' minute;
select interval '12' second;

select interval '12:23:43' hour to minute;
select interval '12:23:43' hour to second;
select interval '12:23:43' minute to second;

-- day to second
select interval '12 23:32:42.01' day to second(2);
select interval '12 23:32:42.01' day(3) to second(2);

-- year(n)
select interval '12' year(0);
select interval '0' year(0);
select interval '12' year(9);
select interval '12' year(10);

select interval '3' month;
-- year to month
select interval '12-3' year(4);

-- free testing
select '1 1:1:1' :: interval day(2) to second;
select '-1 1:1:1' :: interval day(2) to second;
select interval '-12-3' year(4);
select interval '-12-3' year(4) to month;
select INTERVAL '15 1' day to hour from dual;

select interval '15:15' hour TO minute from dual;
select interval '12-1 12:00:00' day to minute from dual;
select interval '0 0:0' day to minute from dual;
select interval '0 0:1:1' day to minute from dual;
select interval '0 0:0:0' day to second from dual;
select interval '0' day from dual;
select interval '1.12323'  second(1) from dual;
select interval '1 4:4:1.12323' day to second(9) from dual;
select interval '1 4:4:1.12323' day to second(1) from dual;


create table test_interval(f1 int,f2 interval year to month,f3 interval day to second);
insert into test_interval values(1, interval '1' day, interval '1' year);
insert into test_interval values(1, INTERVAL '12' YEAR, INTERVAL '40 24' DAY(3) TO HOUR);
insert into test_interval values(1, INTERVAL '2' YEAR(2), INTERVAL '1' day);
insert into test_interval values(2, INTERVAL '1' YEAR(1), interval '1' hour);
insert into test_interval values(3, INTERVAL '0' month, INTERVAL '1' second);
select f2,instr(f2,'02',1,1) from test_interval order by f1;
select f3,instr(f3,'01',1,1) from test_interval order by f1;
select f2,substr(f2,1,3) from test_interval order by f1;
select f3,substr(f3,1,3) from test_interval order by f1;
select f2,length(f2) from test_interval order by f1;
select f3,length(f3) from test_interval order by f1;
select f2||'opentenbase' from test_interval order by f1;
select f3||'opentenbase' from test_interval order by f1;
select concat(f2,'opentenbase') from test_interval order by f1;
select concat(f3,'opentenbase') from test_interval order by f1;
select f2 + interval '1' year,f3 - interval '1' second from test_interval order by f1;
insert into test_interval values(1, 7||'-'||8||' ', 5||' '||4||':'||3||':'||2.246601||' '); 
drop table test_interval;

select INTERVAL '11:12:10.2222222' HOUR(3) TO SECOND(7) from dual;
select INTERVAL '11:20' HOUR(1) TO MINUTE from dual;
select INTERVAL '10:22' MINUTE(1) TO SECOND from dual;
select INTERVAL '25' HOUR(1) from dual;
select INTERVAL '40' MINUTE(1) from dual;
select INTERVAL '300000' SECOND(1) from dual;
select INTERVAL '20' HOUR(0) from dual;
select INTERVAL '40' MINUTE(0) from dual;
select INTERVAL '300000' SECOND(0) from dual;
select INTERVAL '123' month(2) from dual;
select INTERVAL '123' month(1) from dual;
select INTERVAL '3' month(0) from dual;
select INTERVAL '30.12345' SECOND(2,4) from dual;

set transform_insert_to_copy = on;
create table test_interval(id int, v INTERVAL YEAR TO MONTH); 
insert into test_interval(id, v) values(1, interval '1 month'), (2, interval '2 month');
select id,v from test_interval order by id;
drop table test_interval;

-- 
-- Fix interval inconsistency in pipeline
-- 
SELECT TO_CHAR(INTERVAL '123-2' YEAR(3) TO MONTH, 'YYYY-MM'), TO_CHAR(INTERVAL '13 15:20:33' DAY TO SECOND, 'DD HH24:MI:SS.FF6') FROM DUAL;
SELECT TO_CHAR(INTERVAL '11:12:10.2222', 'HH24:MI:SS.FF9');
select to_char(INTERVAL '11:12:00', 'HH24:MI:SS.FF1');
select to_char(interval '11:12', 'HH24:MI.FF1');
--Over

reset transform_insert_to_copy;

set intervalstyle = 'postgres';
create table tbl_interval_style_test(id int, c1 interval);
insert into tbl_interval_style_test values(1, '-99 23:59:59.999999');
set intervalstyle = 'sql_standard';
insert into tbl_interval_style_test values(2, '-99 23:59:59.999999');
select * from tbl_interval_style_test order by id;
drop table tbl_interval_style_test;
reset intervalstyle;
