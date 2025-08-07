\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;

-- prepare
select current_database();

-- create table
drop table rowtest;

create table rowtest(tc1 int, tc2 int, tc3 int, tc4 int, tc5 bigint, tc6 bigint, tc7 bigint, tc8 bigint, tc9 interval, tc10 interval, tc11 interval, tc12 interval, tc13 interval, tc14 text);

INSERT INTO rowtest SELECT i, i, i, i, i, i, i, i, i * interval '1 second', i * interval '1 second', i * interval '1 second', i * interval '1 second', i * interval '1 second', numeric_power(i, i)::text from generate_series(1, 1000) i;

-- test correctness
select count(*) from rowtest;

select * from rowtest order by tc14 desc limit 5;

-- expression test
-- int4 expression test
select * from rowtest where tc1 < 5 or (tc3 > 100 and tc2 <= 102) or not (tc4 >= 10) order by tc14;

select * from rowtest where tc1 + tc2 < 15 and (-tc1)*tc2 + 1 = 0 order by tc14;

select count(*) from rowtest where int4larger(int4inc(tc1), tc3) =  int4smaller(tc2, tc4) + 1;

select count(*) from rowtest where int4abs(-tc1) = tc2;

select * from rowtest where (tc1 << 2) >> 3 < 15 order by tc14;

select count(*) from rowtest where (~tc1) + 1 = -tc2;


-- int8 expression test
select * from rowtest where tc5 < int8(5) or (tc7 > int8(100) and tc6 <= int8(102)) or not (tc8 >= int8(10)) order by tc14;

select * from rowtest where tc5 + tc6 < int8(15) and (-tc5)*tc6 + int8(1) = int8(0) order by tc14;

select count(*) from rowtest where int8larger(int8inc(tc5), tc7) =  int8smaller(tc6, tc8) + int8(1);

select count(*) from rowtest where int8abs(-tc5) = tc6;

select * from rowtest where (tc5 << 2) >> 3 < int8(15) order by tc14;

select count(*) from rowtest where (~tc5) + int8(1) = -tc6;


-- test expression with text args
select * from rowtest where tc14 > numeric_power(99, 99)::text and tc14 < numeric_power(99, 98)::text or tc14 = numeric_power(99, 99)::text order by tc14;

select * from rowtest where textlen(tc14) > textlen(numeric_power(888, 888)::text) and textlen(tc14) < textlen(numeric_power(891, 891)::text) order by tc14;

-- drop table
drop table rowtest;

reset client_min_messages;
reset datestyle;
reset client_encoding;
