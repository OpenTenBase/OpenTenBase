------------------------------------------
-- Test cases for opentenbase_ora raw type.
------------------------------------------

\c regression_ora

--------------------------
-- test raw
--------------------------
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
create table tb4(a raw(10), b raw(10), c int);
insert into tb4 values('123', '456', 1);
insert into tb4 values('123', '456', 1);
insert into tb4 values('123a', '456a', 1);
insert into tb4 values('123a', '456a', 2);
select max(a) from tb4;
select min(a) from tb4;

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

-- disable function has long raw parameter
create table test_raw(f1 int,f2 long raw);
insert into test_raw values(1, '123');
insert into test_raw (f1,f2) values(4,(select f2 from test_raw limit 1));
select f2 from test_raw;
select * from test_raw ;
select sum(f2) from test_raw;
select rawtohex(f2) from test_raw order by f1;
select hextoraw(rawtohex(f2)) from test_raw order by f1;
insert into test_raw values(17,'A');
select ascii(f2) from test_raw where f1=17;
create index test_raw_i1 on test_raw(lower(f2));
create index test_raw_i2 on test_raw(upper(f2));
-- disable craete materialized view as select long raw
create MATERIALIZED view test_raw_mv as select f2 as nc from test_raw;
drop view test_raw_mv;
drop table test_raw;

-- add from v5
select rawtohex(hextoraw('aaaa')) from dual;
select rawtohex('AA') from dual;
select rawtohex(AA) from dual;
select rawtohex('11') FROM dual;
select rawtohex(11) FROM dual;
select rawtohex() from dual;
select hextoraw('abcdef') from dual;
select hextoraw('abcdefg') from dual;
select hextoraw('') from dual;
select hextoraw(null) from dual;
select hextoraw('01234548787987897') from dual;
