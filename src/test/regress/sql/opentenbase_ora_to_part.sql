------------------------------------------------------------
-- A part of opentenbase_ora.sql, test to functions related cases.
------------------------------------------------------------
\c regression_ora

\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;
SET intervalstyle to sql_standard;

--test TO_CHAR
select TO_CHAR(22);
select TO_CHAR(99::smallint);
select TO_CHAR(-44444);
select TO_CHAR(1234567890123456::bigint);
select TO_CHAR(123.456::real);
select TO_CHAR(1234.5678::double precision);
select TO_CHAR(12345678901234567890::numeric);
select TO_CHAR(1234567890.12345);
select TO_CHAR('4.00'::numeric);
select TO_CHAR('4.0010'::numeric);

-- test special case of '$' in int_to_char fmt
select TO_CHAR(123,'$99,999.9') from dual;
select LENGTH(TO_CHAR(123,'$99,999.9')) from dual;
select TO_CHAR(123,'$99,9,999.9') from dual;
select LENGTH(TO_CHAR(123,'$99,9,999.9')) from dual;
select TO_CHAR(123,'99,9,9$99.9') from dual;
select LENGTH(TO_CHAR(123,'99,9,9$99.9')) from dual;
select TO_CHAR(123,'99,9,999.9$') from dual;
select LENGTH(TO_CHAR(123,'99,9,999.9$')) from dual;

-- with sign
select TO_CHAR(+123,'$99,999.9') from dual;
select LENGTH(TO_CHAR(+123,'$99,999.9')) from dual;
select TO_CHAR(-123,'$99,999.9') from dual;
select LENGTH(TO_CHAR(-123,'$99,999.9')) from dual;

-- numeric_to_char
select TO_CHAR(123.99,'$99,999.9') from dual;
select LENGTH(TO_CHAR(123.99,'$99,999.9')) from dual;
select TO_CHAR(123.99,'$99,9,999.9') from dual;
select LENGTH(TO_CHAR(123.99,'$99,9,999.9')) from dual;
select TO_CHAR(123.99,'99,9,9$99.9') from dual;
select LENGTH(TO_CHAR(123.99,'99,9,9$99.9')) from dual;
select TO_CHAR(123.99,'99,9,999.9$') from dual;
select LENGTH(TO_CHAR(123.99,'99,9,999.9$')) from dual;

-- with sign
select TO_CHAR(-123.99,'$99,999.9') from dual;
select LENGTH(TO_CHAR(-123.99,'$99,999.9')) from dual;
select TO_CHAR(+123.99,'$99,999.9') from dual;
select LENGTH(TO_CHAR(+123.99,'$99,999.9')) from dual;

---------------------------------------------------------------------------------------------------------------------------------------
--add TO_CHAR to convert the integer, float, numeric to hex digit string
select TO_CHAR(20, 'aa') from dual;
select TO_CHAR(22221., 'xxxxxxx') from dual;
select TO_CHAR(22221., 'XXXXXXX') from dual;
select TO_CHAR(22221.4, 'xxxxxxx') from dual;
select TO_CHAR(22221.5, 'xxxxxxx') from dual;
select TO_CHAR(-22221.4, 'xxxxxxx') from dual;

select TO_CHAR(912222, 'xxxxx') from dual;
select TO_CHAR(912222, 'XXXXX') from dual;

select TO_CHAR(912222, 'xxxx') from dual;

select TO_CHAR(-912222, 'xxxxxxx') from dual;

select TO_CHAR(2e4, 'XXXx') from dual;
select TO_CHAR(2e-4, 'XXXx') from dual;
select TO_CHAR(2e0, 'XXXx') from dual;
select TO_CHAR(-2e0, 'XXXx') from dual;

select TO_CHAR(912222, 'XxxxxxxxxxxxxxxxxxxxxxxxxxXxxxxxxxxxxxxxXxxxxxxxxxxxxxxxxxxxxxX') from dual;
select TO_CHAR(912222, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') from dual;
select TO_CHAR(912222, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') from dual;

drop table if exists tr_tbl_to_char;
create table tr_tbl_to_char(f1 integer, f2 float, f3 double precision, f4 numeric(10,2));
insert into tr_tbl_to_char values(255, 255.4, 65535.9999999, -32765.89);
insert into tr_tbl_to_char values(254, 253.4, 65534.9999999, 32765.89);
insert into tr_tbl_to_char values(-254, 253.4, 65534.9999999, 32765.89);
select TO_CHAR(f1, 'XXXX'), TO_CHAR(f2, 'xxxx'), TO_CHAR(f3, 'xxxX'), TO_CHAR(f4, 'XXXXX') from tr_tbl_to_char order by f1;
drop table tr_tbl_to_char;

------------------------------------------------------------------------------------------------------------------------------
--test TO_NUMBER
SELECT TO_NUMBER('-34,338,492', '99G999G999') from dual;
SELECT TO_NUMBER('-34,338,492.654,878', '99G999G999D999G999') from dual;
SELECT TO_NUMBER('<564646.654564>', '999999.999999PR') from dual;
SELECT TO_NUMBER('0.00001-', '9.999999S') from dual;
SELECT TO_NUMBER('5.01-', 'FM9.999999S') from dual;
SELECT TO_NUMBER('5.01-', 'FM9.999999MI') from dual;
SELECT TO_NUMBER('5 4 4 4 4 8 . 7 8', '9 9 9 9 9 9 . 9 9') from dual;
SELECT TO_NUMBER('.01', 'FM9.99') from dual;
SELECT TO_NUMBER('.0', '99999999.99999999') from dual;
SELECT TO_NUMBER('0', '99.99') from dual;
SELECT TO_NUMBER('.-01', 'S99.99') from dual;
SELECT TO_NUMBER('.01-', '99.99S') from dual;
SELECT TO_NUMBER(' . 0 1-', ' 9 9 . 9 9 S') from dual;

SELECT TO_NUMBER('-34338492') from dual;
SELECT TO_NUMBER('-34338492.654878') from dual;
SELECT TO_NUMBER('564646.654564') from dual;
SELECT TO_NUMBER('0.00001') from dual;
SELECT TO_NUMBER('5.01') from dual;
SELECT TO_NUMBER('.01') from dual;
SELECT TO_NUMBER('.0') from dual;
SELECT TO_NUMBER('0') from dual;
SELECT TO_NUMBER('01') from dual;

select TO_NUMBER(123.09e34::float4) from dual;
select TO_NUMBER(1234.094e23::float8) from dual;

SELECT TO_NUMBER('123'::text);
SELECT TO_NUMBER('123.456'::text);
SELECT TO_NUMBER(123);
SELECT TO_NUMBER(123::smallint);
SELECT TO_NUMBER(123::int);
SELECT TO_NUMBER(123::bigint);
SELECT TO_NUMBER(123::numeric);
SELECT TO_NUMBER(123.456);

------------------------------------------------------------------------------------------------------------------------------
-- Tests for to_multi_byte
SELECT to_multi_byte('123$test');
-- Check internal representation difference
SELECT octet_length('abc');
SELECT octet_length(to_multi_byte('abc'));

-- Tests for to_single_byte
SELECT to_single_byte('123$test');
SELECT to_single_byte('１２３＄ｔｅｓｔ');
-- Check internal representation difference
SELECT octet_length('ａｂｃ');
SELECT octet_length(to_single_byte('ａｂｃ'));

RESET intervalstyle;
