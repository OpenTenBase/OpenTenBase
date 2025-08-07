---------------------------------------------
-- Test cases for opentenbase_ora to functions.
---------------------------------------------

\c regression_ora

----------------------------------
-- Test HEXTORAW function
----------------------------------
SELECT HEXTORAW('7ABCD') FROM DUAL;

DROP TABLE IF EXISTS tex_tow_t;
CREATE TABLE tex_tow_t(id int, c1 number(10, 2), c2 char(3), c3 varchar2(4), c4 nchar(3), c5 NVARCHAR2(4), c6 LONG, c7 float, c8 CLOB, c9 NCLOB, c10 BLOB, c11 raw(10));
INSERT INTO tex_tow_t VALUES(1, 100.98, 'abc', 'abce', 323, 'abce', 'asdd', '4565', 'dsa', '12da', 'dasas123', 'da4556');
SELECT id, hextoraw(rawtohex(c1)), hextoraw(rawtohex(c2)), hextoraw(c3), hextoraw(rawtohex(c4)), hextoraw(c5), hextoraw(c7), hextoraw(c11) from tex_tow_t;
DROP TABLE tex_tow_t;

----------------------------------
-- Test RAWTOHEX function
----------------------------------
SELECT RAWTOHEX('7') FROM DUAL;
SELECT RAWTOHEX('8') FROM DUAL;
SELECT RAWTOHEX('10') FROM DUAL;
SELECT RAWTOHEX(10) FROM DUAL;
SELECT RAWTOHEX(10112233) FROM DUAL;
SELECT RAWTOHEX(1011.2233) FROM DUAL;

drop table if exists tex_tow_t;
create table tex_tow_t(id int, c1 int, c2 number(10,2), c3 float, c4 double precision);
insert into tex_tow_t values(1, 12, 28.12, 892.1, 29839.298934);
insert into tex_tow_t values(2, 123, 283.123, 892.13, 29839.2398934);
select id, rawtohex(c1) from tex_tow_t order by id;
select id, rawtohex(c2) from tex_tow_t order by id;
select id, rawtohex(c3) from tex_tow_t order by id;
select id, rawtohex(c4) from tex_tow_t order by id;
drop table tex_tow_t;

----------------------------------
-- Test TO_CHAR function
----------------------------------
SELECT TO_CHAR('01111' + 1) FROM DUAL;
SELECT TO_CHAR('01111.123456' + 1) FROM DUAL;
SELECT TO_CHAR(-1234567890, '9999999999S'), TO_CHAR(+1234567890, '9999999999S'), TO_CHAR(0, '99.99'), TO_CHAR(+0.1, '99.99'), TO_CHAR(-0.2, '99.99'), TO_CHAR(0, '90.99'), TO_CHAR(+0.1, '90.99'), TO_CHAR(-0.2, '90.99') FROM DUAL;
SELECT TO_CHAR(0, '9999'), TO_CHAR(0, '9999'), TO_CHAR(1, '9999'), TO_CHAR(0, 'B9999'), TO_CHAR(1, 'B9999'), TO_CHAR(0, 'B90.99') FROM DUAL;
SELECT TO_CHAR(+123.456, '999.999'), TO_CHAR(-123.456, '999.999'), TO_CHAR(+123.456, 'FM999.009'), TO_CHAR(+123.456,'9.9EEEE') FROM DUAL;
SELECT TO_CHAR(+1E+12, '9.9EEEE'), TO_CHAR(+123.456, 'FM999.009'), TO_CHAR(+123.456,'FM999.009') FROM DUAL;
SELECT TO_CHAR(+123.45, 'L999.99'), TO_CHAR(+123.45,'FML999.99') FROM DUAL;
SELECT TO_CHAR(123456, '999,999') FROM DUAL;

SET TIMEZONE='00:00';
DROP TABLE IF EXISTS t_timestamp_test;
CREATE TABLE t_timestamp_test (ts_col TIMESTAMP, tstz_col TIMESTAMP WITH TIME ZONE);
INSERT INTO t_timestamp_test VALUES (TIMESTAMP'1999-12-01 10:00:00.05', TIMESTAMP WITH TIME ZONE'1999-12-01 10:00:00.05 +3:00');
INSERT INTO t_timestamp_test VALUES (TIMESTAMP'1999-12-02 10:00:00.05', TIMESTAMP WITH TIME ZONE'1999-12-02 10:00:00.05 +5:00');
SELECT TO_CHAR(ts_col, 'DD-MON-YYYY HH24:MI:SS.US') AS ts FROM t_timestamp_test ORDER BY ts;
SELECT TO_CHAR(tstz_col, 'DD-MON-YYYY HH24:MI:SS.US TZH:TZM') AS tstz FROM t_timestamp_test ORDER BY tstz;
DROP TABLE t_timestamp_test;
RESET TIMEZONE;

----------------------------------
-- Test TO_NUMBER function
----------------------------------
SELECT TO_NUMBER('2.001' + 1) "Value" FROM DUAL;
SELECT TO_NUMBER('2.01', '9.0') "Value" FROM DUAL;
SELECT TO_NUMBER(123.456, '999.000') "Value" FROM DUAL;

----------------------------------
-- Test TO_MULTI_BYTE function
----------------------------------
SELECT DUMP(TO_MULTI_BYTE( 'A')) FROM DUAL;
SELECT DUMP(TO_MULTI_BYTE('B')) FROM DUAL;
SELECT DUMP(TO_MULTI_BYTE('C')) FROM DUAL;

----------------------------------
-- Test TO_SINGLE_BYTE function
----------------------------------
SELECT DUMP(TO_SINGLE_BYTE('123数据库')) FROM DUAL;

-- Test to_single_byte as a function index
create table test_to_single_byte_20231222(t text);
create index test_index_20231222 on test_to_single_byte_20231222(to_single_byte(t));
insert into test_to_single_byte_20231222 values ('test1');
\d+ test_to_single_byte_20231222
drop table test_to_single_byte_20231222;

-- Test to_number as a function index
create table test_to_number_20231222(t text);
create index test_index_20231222 on test_to_number_20231222(to_single_byte(t));
insert into test_to_number_20231222 values ('test1');
\d+ test_to_number_20231222
drop table test_to_number_20231222;
