\c opentenbase_ora_package_function_regression_ora
\set ECHO none
set client_min_messages TO error;
CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;
set client_min_messages TO default;
\set ECHO all

--
-- test package utl_raw function
--
select utl_raw.cast_to_raw('A RAW Value,--sda') from dual;
select utl_raw.cast_to_raw('我是用例') from dual;
select utl_raw.cast_to_varchar2('E68891E698AFE794A8E4BE8B') from dual;
select utl_raw.cast_to_varchar2('41205241572056616C7565') from dual;
select utl_raw.cast_to_varchar2(utl_raw.cast_to_raw('A RAW Value')) from dual;
select utl_raw.cast_to_varchar2('41205241572056616C75652C2D2D736461') from dual;
select utl_raw.LENGTH('41205241572056616C7565') from dual;
drop table if exists tb1;
create table tb1(a int, b raw(10));
insert into tb1 values(1, '');
select '=='||utl_raw.cast_to_varchar2(b)||'==' from tb1 order by a;
insert into tb1 values(2, '11');
insert into tb1 values(3, '22');
insert into tb1 values(4, '20');
select '=='||utl_raw.cast_to_varchar2(b)||'==' from tb1 order by a;
select '=='||utl_raw.cast_to_varchar2('0100')||'==' from dual;
select '=='||utl_raw.cast_to_varchar2('01000100')||'==' from dual;
drop table tb1;

select utl_raw.bit_and('12344321','0F') from dual; 
select utl_raw.bit_and('12344321','0f') from dual;
select utl_raw.bit_or('12344321','0f') from dual;
select utl_raw.bit_xor('12344321','0f') from dual;
select utl_raw.bit_and('12344321','') from dual;
select utl_raw.bit_or('12344321','') from dual;
select utl_raw.bit_xor('12344321','') from dual;

SELECT utl_raw.bit_complement('0102F3') FROM dual;
SELECT utl_raw.compare(utl_raw.cast_to_raw('ABC'), utl_raw.cast_to_raw('aCC')) FROM dual;
SELECT utl_raw.compare(utl_raw.cast_to_raw('ABC'), utl_raw.cast_to_raw('ABC')) FROM dual;
SELECT utl_raw.compare(utl_raw.cast_to_raw('ABC'), utl_raw.cast_to_raw('')) FROM dual;
SELECT utl_raw.compare(utl_raw.cast_to_raw('abcsdsa'), utl_raw.cast_to_raw('absdsf')) FROM dual;

SELECT utl_raw.reverse('123') FROM dual;
SELECT utl_raw.reverse('1234') FROM dual;
SELECT utl_raw.reverse('12345') FROM dual;
SELECT utl_raw.reverse('123456') FROM dual;
SELECT utl_raw.reverse('01234567') FROM dual;
SELECT utl_raw.reverse('1234567') FROM dual;
SELECT utl_raw.reverse('01011') FROM dual;

select utl_raw.substr('1234567', 2, 3) from dual;
select utl_raw.substr('1234567zx', 2, 3) from dual;
select utl_raw.substr('12a3c4567', 2, 3) from dual;
select utl_raw.substr('12a3c4567', 2) from dual;
select utl_raw.substr('12a3c4567', 0) from dual;
select utl_raw.substr('12a3c4567', 1) from dual;
select utl_raw.substr('12a3c4567', 5) from dual;
select utl_raw.substr('12a3c4567', 6) from dual;
select utl_raw.substr('12a3c4567', -6) from dual;
select utl_raw.substr('12a3c4567', -5) from dual;
select utl_raw.substr('12a3c4567', -1) from dual;
select utl_raw.substr('12a3c4567', 2, 1) from dual;
select utl_raw.substr('12a3c4567', 2, 2) from dual;
select utl_raw.substr('12a3c4567', 2, 0) from dual;
select utl_raw.substr('12a3c4567', 2, -1) from dual;
select utl_raw.substr('12a3c4567', 0, -1) from dual;
select utl_raw.substr('12a3c4567', -1, -1) from dual;
select utl_raw.substr('1', 2, 2) from dual;
select utl_raw.substr('12', 2, 2) from dual;
select utl_raw.substr('', 2, 2) from dual;
select utl_raw.substr(' ', 2, 2) from dual;
select utl_raw.substr('12', 2, 1) from dual;
select utl_raw.substr('123', 2, 1) from dual;
select utl_raw.substr('12', 2, 0) from dual;
select utl_raw.substr('12', 1, 1) from dual;
select utl_raw.substr('12a3c4567', 0, 1) from dual;
select utl_raw.substr('1234567810', '', '') from dual;

SELECT utl_raw.copies('A', 6) FROM dual;
SELECT utl_raw.copies('1A', 6) FROM dual;
SELECT utl_raw.copies('A', 0) FROM dual;
SELECT utl_raw.copies('A', 1) FROM dual;
SELECT utl_raw.copies('A', 1000) FROM dual;
SELECT utl_raw.copies('A', 1.5) FROM dual;
SELECT utl_raw.copies('A', 1.22) FROM dual;
SELECT utl_raw.copies('A') FROM dual;
SELECT utl_raw.copies('A', 0.22) FROM dual;
SELECT utl_raw.copies('A', 0.5) FROM dual;

SELECT utl_raw.concat('A','41','B','42')FROM dual;
SELECT utl_raw.concat('')FROM dual;
SELECT utl_raw.concat(' ','ab')FROM dual;
SELECT utl_raw.concat('','ab','','c')FROM dual;

select utl_raw.cast_to_raw('aaaa') from dual;
select rawtohex(utl_raw.cast_to_raw('aaaa')) from dual;
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
select utl_raw.length(hextoraw('')) from dual;
select utl_raw.length(hextoraw(null)) from dual;

-- opentenbase_ora raw support '\\'
select utl_raw.cast_to_raw('\\x12') from dual;

-- p0 testcases
drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1 (id NUMBER, doc RAW(2000));
INSERT INTO t1 VALUES (1, utl_raw.cast_to_raw('A RAW Value'));
SELECT utl_raw.cast_to_varchar2(doc) FROM t1;
CREATE TABLE t2(id NUMBER, doc LONG RAW);
INSERT INTO t2 VALUES (1, utl_raw.cast_to_raw('Test to go into LONG RAW column'));
select * from t1;
select * from t2;
insert into t1 select * from t1 where doc like '63%';
insert into t2 select * from t2 where doc like '63%';
insert into t1 select * from t1 where doc like utl_raw.cast_to_raw('A RAW Value');
insert into t2 select * from t2 where doc like utl_raw.cast_to_raw('A RAW Value');
select * from t1;
drop table t1;
drop table t2;

-- p0 utl_raw
select utl_raw.bit_and('12344321','0') from dual;
select utl_raw.bit_and('','0') from dual;
select utl_raw.bit_and(null,null) from dual;

select utl_raw.bit_complement('abc') from dual; 
select utl_raw.bit_complement('') from dual; 
select utl_raw.bit_complement(null) from dual; 

drop table if exists bit_complement_test;
create table bit_complement_test (id number, raw_date raw(100));
insert into bit_complement_test values(1, 'abc');
insert into bit_complement_test values(2, '00100');
insert into bit_complement_test values(3, '125');
insert into bit_complement_test values(4, '');
insert into bit_complement_test values(5, null);
select utl_raw.bit_complement(raw_date) from bit_complement_test order by 1;
drop table bit_complement_test;

SELECT utl_raw.copies('123', 6) FROM dual;
SELECT utl_raw.copies('dadadfadadaaa', 1) FROM dual;
SELECT utl_raw.copies('', 10) FROM dual;
select utl_raw.COPIES('A', '') from dual;

drop table if exists copies_test;
create table copies_test (id number, raw_date raw(100));
insert into copies_test values(1, 'abc');
insert into copies_test values(2, '00100');
insert into copies_test values(3, '125');
SELECT raw_date, utl_raw.copies(raw_date, 2) from copies_test ORDER BY 1;
insert into copies_test values(4, '');
SELECT raw_date, utl_raw.copies(raw_date, 2) from copies_test ORDER BY 1;
insert into copies_test values(5, null);
SELECT raw_date, utl_raw.copies(raw_date, 2) from copies_test ORDER BY 1;
drop table copies_test;

SELECT utl_raw.compare(utl_raw.cast_to_raw(''),utl_raw.cast_to_raw(null)) FROM dual;
SELECT utl_raw.compare(utl_raw.cast_to_raw(123),utl_raw.cast_to_raw(123)) FROM dual;
SELECT utl_raw.compare('0','da') FROM dual;
SELECT utl_raw.cast_to_raw(utl_raw.compare('0','da')) FROM dual;

drop table if exists compare_test;
create table compare_test (id number, raw_date raw(100));
insert into compare_test values(1, 'abc');
insert into compare_test values(2, '00100');
insert into compare_test values(3, '125');
insert into compare_test values(4, '');
insert into compare_test values(5, null);
SELECT raw_date, utl_raw.compare(raw_date, utl_raw.cast_to_raw(id)) from compare_test order by 1;
drop table compare_test;

select utl_raw.concat('12','34') from dual;
select utl_raw.concat('12','34', '', null, 'abc') from dual;
select utl_raw.concat('12','34', '', null, 'abc')||utl_raw.concat('12','34', '', null) from dual;
-- concat with null args
drop table if exists utl_raw_t;
create table utl_raw_t(id int, c1 char(20), c2 nchar(100), c3 VARCHAR2(200), c4 NVARCHAR2(1000), c5 NUMBER, c6 NUMBER(10, 3), c7 INTEGER, c8 BINARY_FLOAT, c9 BINARY_DOUBLE, c10 FLOAT(5), c11 FLOAT(10), c12 CLOB, c13 NCLOB ,c14 BLOB, c15 raw(100), c16 ROWID);
insert into utl_raw_t values(1, 'AAAA', 'BBCC', '123BC', '12845BA', 4589, 12365, 100, 365, 459, 9999, 666, 'ACDF', 'FFDE', 'AEFD', 'AAAABBBB', '');
insert into utl_raw_t values(2, 'FFFFFFF', 'EEFD', '123BCAD', '128045BA', 45891, 120365, 1090, 1365, 4259, 93999, 6166, 'A22CDF', 'FFD3E', 'A1EFD', 'A45AAABBBB', '');
insert into utl_raw_t values(3, 'F02FFFFF', 'EEF01D', '123BC01AD', 'A128045BA', 425891, 1120365, 10290, 165, 425, 939999, 626, 'A2A2CDF', 'FAFD3E', 'AA1EFD', 'A45AABBBB', '');
insert into utl_raw_t values(4, 'F02FFFFF', 'EEF01D', '123BC01AD', 'A128045BA', 425891, 1120365, 10290, 165, 425, 939999, 626, 'A2A2CDF', 'FAFD3E', 'AA1EFD', 'A45AABBBB', '');
insert into utl_raw_t values(5, 'F02FFEEFF', 'EEF01DA', '123BC001AD', 'A128045B12A', 425891, 1120365, 10290, 165, 425, 939999, 626, 'A2A2CDF', 'FAFD3E', 'AA1EFD', 'A45AABBBB', '');
insert into utl_raw_t values(6, '', 'EEF01DA', '123BC001AD', '', 425891, '', 10290, 165, 425, '', 626, 'A2A2CDF', '', 'AA1EFD', '', '');
insert into utl_raw_t values(7, '', '', '123BC001AD', '', '', '', 10290, 165, 425, '', 626, '', '', 'AA1EFD', '', '');
insert into utl_raw_t values(8, '', '', '123BC001AD', '', '', '', 10290, '', 425, '', 626, '', '', 'AA1EFD', '', '');
insert into utl_raw_t values(9, 'F02FFEEFF', '', '123BC001AD', '', '', 10290, '', '', 425, '', 626, 'AA1EFD', '', '', '', '');
select id,utl_raw.concat(hextoraw(trim(t1.c3||'')), hextoraw(trim(t1.c2||''))) as ca from utl_raw_t t1 order by id;
drop table utl_raw_t;

drop table if exists length_test;
create table length_test (id number, raw_date raw(100));
insert into length_test values(1, 'abc');
insert into length_test values(2, '00100');
insert into length_test values(3, '125');
insert into length_test values(4, '');
insert into length_test values(5, null);
select * from length_test order by 1;
select utl_raw.length('') from dual ORDER BY 1;
select utl_raw.length(null) from dual ORDER BY 1;
SELECT raw_date, utl_raw.length(raw_date) from length_test ORDER BY 1;
drop table length_test;

SELECT utl_raw.reverse('') FROM dual ORDER BY 1;
SELECT utl_raw.reverse('111') FROM dual ORDER BY 1;
SELECT utl_raw.reverse('000') FROM dual ORDER BY 1;
SELECT utl_raw.reverse(utl_raw.reverse('ABC')) FROM dual ORDER BY 1;

drop table if exists substr_test;
create table substr_test (id number, raw_date raw(100));
insert into substr_test values(1, 'abcdef');
insert into substr_test values(2, '00100456');
insert into substr_test values(3, '1252431');
SELECT raw_date, utl_raw.substr(raw_date, 0, 1) from substr_test order by 1;
SELECT raw_date, utl_raw.substr(raw_date, 1, 1) from substr_test order by 1;
SELECT raw_date, utl_raw.substr(raw_date, -1, 1) from substr_test order by 1;
SELECT raw_date, utl_raw.substr(raw_date, -2, 1) from substr_test order by 1;
SELECT raw_date, utl_raw.substr(raw_date, -3, 1) from substr_test order by 1;
SELECT raw_date, utl_raw.substr(raw_date, -2, 1) from substr_test order by 1;
SELECT raw_date, utl_raw.substr(raw_date, -2, 2) from substr_test order by 1;
select utl_raw.substr('123456781', -6, 6) from dual;
select utl_raw.substr('123456781', -5, 6) from dual;
select utl_raw.substr('123456781', -5, 5) from dual;
select utl_raw.substr('123456781', 0, 6) from dual;
select utl_raw.substr('123456781', 0, 5) from dual;
select utl_raw.substr('123456781', 1, 0) from dual;

drop table substr_test;

-- type raw support procedure/function/curser
drop procedure if exists test_raw_pro(a_int in long raw);
create or replace procedure test_raw_pro(a_int in long raw)
as
  v1 long raw;
begin
  v1:=a_int;
  CALL dbms_output.serveroutput(true);
  CALL dbms_output.put_line(utl_raw.cast_to_varchar2(v1));
end;
/
call test_raw_pro('63');
drop procedure if exists test_raw_pro(a_int in raw);

drop function if exists  test_raw_fun(acc in long raw);
CREATE or replace FUNCTION test_raw_fun(acc in long raw)
   RETURN varchar2
   IS
   raw1 varchar2(200);
   BEGIN
   raw1 := utl_raw.cast_to_varchar2(acc);
   return raw1;
    END;
/
select test_raw_fun(utl_raw.cast_to_raw('raw3')) from dual;
drop function if exists test_raw_fun(acc in long raw);

drop table if exists test_raw;
create table test_raw (f1 int,f2 long raw);
insert into test_raw values(1,utl_raw.cast_to_raw('raw1'));
insert into test_raw values(2,utl_raw.cast_to_raw('raw2'));
insert into test_raw values(3,utl_raw.cast_to_raw('raw3'));

drop table test_raw;

-- long raw support create view, but is not allowed to select
drop table if exists t_pview;
drop view if exists t_pview_v;
drop view if exists t_pview_v2;
create table t_pview(f1 int primary key not null,f2 long raw);
insert into t_pview values(1,utl_raw.cast_to_raw('raw1'));
create view t_pview_v as select * from t_pview;
create view t_pview_v2 as select f2 from t_pview;
select * from t_pview;
select f1 from t_pview;
select f2 from t_pview;
-- support select into
declare
a long raw;
begin
select f2 into a from t_pview;
--CALL dbms_output.put_line(a);
end;
/
drop view t_pview_v;
drop view t_pview_v2;
drop table t_pview;

-- support those function
drop table IF EXISTS test_raw;
create table test_raw(f1 int,f2 raw(30));
insert into test_raw values(1,utl_raw.cast_to_raw('raw1'));
insert into test_raw values(2,utl_raw.cast_to_raw('raw2')),(3,utl_raw.cast_to_raw('raw3'));
select f2,instr(f2,'72',1,1) from test_raw order by f1;
select f2,substr(f2,1,3) from test_raw order by f1;
select f2,length(f2) from test_raw order by f1;
select f2,trim(f2||' ') from test_raw order by f1;
select f2,replace(f2,'72','raw') from test_raw order by f1;
select f2||'opentenbase' from test_raw order by f1;
select upper(f2),lower(f2) from test_raw order by f1;
insert into test_raw values(17,'A');
select ascii(f2) from test_raw where f1=17;
drop table test_raw;

-- string append
select to_clob('1')||utl_raw.bit_xor('A', 'B') from dual;
select to_binary_float(2.5)||utl_raw.bit_xor('A', 'B') from dual;
select 10.23||utl_raw.bit_xor('A', 'B') from dual;
select to_number(10.23)||utl_raw.bit_xor('A', 'B') from dual;
select to_char(10.23)||utl_raw.bit_xor('A', 'B') from dual;
select '2021-05-08'::date||utl_raw.bit_xor('A', 'B') from dual;
select utl_raw.bit_xor('A', 'B')||rawtohex(utl_raw.bit_xor('A', 'B')) from dual;
select 'abc='||utl_raw.bit_xor('A', 'B') from dual;
select 'abc='||utl_raw.cast_to_raw('FF11AA3344DDEEBBAA11998855367833FF11AA3344DDEEBBAA11998855367833') from dual;

-- hextoraw
drop table if exists raws;
create table raws(id int,  c1 raw(10));
insert into raws values (1, utl_raw.cast_to_raw('ds4556'));
insert into raws values (2, utl_raw.cast_to_raw('.@$$$%'));
select id, hextoraw(c1) from raws order by id;
drop table raws;

-- utl_raw with null args
drop table if exists utl_raw_t;
create table utl_raw_t(id int, c1 char(20), c2 nchar(100), c3 VARCHAR2(200), c4 NVARCHAR2(1000), c5 NUMBER, c6 NUMBER(10, 3), c7 INTEGER, c8 BINARY_FLOAT, c9 BINARY_DOUBLE, c10 FLOAT(5), c11 FLOAT(10), c12 CLOB, c13 NCLOB ,c14 BLOB, c15 raw(100), c16 ROWID);
insert into utl_raw_t values(1, 'AAAA', 'BBCC', '123BC', '12845BA', 4589, 12365, 100, 365, 459, 9999, 666, 'ACDF', 'FFDE', 'AEFD', 'AAAABBBB', '');
insert into utl_raw_t values(2, 'FFFFFFF', 'EEFD', '123BCAD', '128045BA', 45891, 120365, 1090, 1365, 4259, 93999, 6166, 'A22CDF', 'FFD3E', 'A1EFD', 'A45AAABBBB', '');
insert into utl_raw_t values(3, 'F02FFFFF', 'EEF01D', '123BC01AD', 'A128045BA', 425891, 1120365, 10290, 165, 425, 939999, 626, 'A2A2CDF', 'FAFD3E', 'AA1EFD', 'A45AABBBB', '');
insert into utl_raw_t values(4, 'F02FFFFF', 'EEF01D', '123BC01AD', 'A128045BA', 425891, 1120365, 10290, 165, 425, 939999, 626, 'A2A2CDF', 'FAFD3E', 'AA1EFD', 'A45AABBBB', '');
insert into utl_raw_t values(5, 'F02FFEEFF', 'EEF01DA', '123BC001AD', 'A128045B12A', 425891, 1120365, 10290, 165, 425, 939999, 626, 'A2A2CDF', 'FAFD3E', 'AA1EFD', 'A45AABBBB', '');
insert into utl_raw_t values(6, '', 'EEF01DA', '123BC001AD', '', 425891, '', 10290, 165, 425, '', 626, 'A2A2CDF', '', 'AA1EFD', '', '');
insert into utl_raw_t values(7, '', '', '123BC001AD', '', '', '', 10290, 165, 425, '', 626, '', '', 'AA1EFD', '', '');
insert into utl_raw_t values(8, '', '', '123BC001AD', '', '', '', 10290, '', 425, '', 626, '', '', 'AA1EFD', '', '');
insert into utl_raw_t values(9, 'F02FFEEFF', '', '123BC001AD', '', '', 10290, '', '', 425, '', 626, 'AA1EFD', '', '', '', '');
select id, utl_raw.cast_to_raw(c2),utl_raw.substr(utl_raw.cast_to_raw(c2), -2, 2)from utl_raw_t order by id;
select id, utl_raw.substr(utl_raw.cast_to_raw(c1), 1, 1),utl_raw.substr(utl_raw.cast_to_raw(c2), -2, 2) from utl_raw_t order by id;
drop table utl_raw_t;

-- fix cast_to_varchar2 core for invalid null args
drop table if exists test_bfile;
create table test_bfile (fId NUMBER, fName BFILE);
insert into test_bfile values (1, BFILENAME('bfiledir', 'bfile_test'));
\copy test_bfile to /tmp/bfile_test with csv;
create directory bfiledir as '/tmp';
DECLARE
     fil BFILE;
     pos INTEGER;
     amt integer;
     buf RAW(40);
BEGIN
    buf := '';
     CALL dbms_output.serveroutput(true);
     CALL dbms_output.put_line('trace buf' || utl_raw.cast_to_varchar2(buf) || 'end');
END;
/
DECLARE
     fil BFILE;
     pos INTEGER;
     amt integer;
     buf RAW(40);
BEGIN
     SELECT fname INTO fil FROM test_bfile WHERE fid = 1;
     perform dbms_lob.open(fil, 'r');
     amt := 40; pos := 1 + dbms_lob.getlength(fil); buf := '';
     CALL dbms_output.serveroutput(true);
     call dbms_lob.read(fil, amt, pos, buf);
     CALL dbms_output.put_line('Read F1 past EOF: '|| utl_raw.cast_to_varchar2(buf));
     perform dbms_lob.close(fil);
     exception
     WHEN no_data_found
     THEN
       BEGIN
         CALL dbms_output.put_line('End of File reached. Closing file');
         call dbms_lob.fileclose(fil);
         -- or dbms_lob.filecloseall if appropriate
       END;
END;
/
drop table test_bfile;
drop directory bfiledir;

-- support ascii
drop table if exists a;
create table a(id int, c20 long raw, c21 raw(100), c22 ROWID, c23 varchar2(50), c24 integer);
insert into a values (1, 'ABC', 'ABC', '', 'ABC', 0);
select ascii(c21),ascii(c22), ascii(c23), ascii(c24)  from a;
drop table a;

-- hextoraw/rawtohex support number
drop table if exists test_raw;
create table test_raw (f1 CHAR(1),f2 VARCHAR2(20),f3 NCHAR(1), f4 NVARCHAR2(20),f5 number,f6 int, f7 float);
insert into test_raw values('a','a','a','a',10,12, 11.2);
select hextoraw(f1),hextoraw(f2),hextoraw(f3),hextoraw(f4) from test_raw;
select f5, hextoraw(f5), rawtohex(f5), rawtohex(utl_raw.cast_to_raw(f5)) from test_raw;
select f6, hextoraw(f6), rawtohex(f6), rawtohex(utl_raw.cast_to_raw(f6)) from test_raw;
select f6, hextoraw(f7), rawtohex(f7), rawtohex(utl_raw.cast_to_raw(f7)) from test_raw;
drop table test_raw;

-- Supports long raw concatenation in stored procedures
declare
	a long raw := '01';
begin
	CALL dbms_output.serveroutput('t');
	CALL dbms_output.PUT_LINE('r = ' || a);	
end;
/
declare
	a raw(20) := '01';
begin
	CALL dbms_output.serveroutput('t');
	CALL dbms_output.PUT_LINE('r = ' || a);	
end;
/

-- support ctas by utl_raw
drop table if exists t_b;
drop table if exists t_a;
create table t_b (id int, c1 raw(100));
insert into t_b values(1, 'A128045B12A');
create table t_a as select id, utl_raw.bit_and(c1, 'AB') from t_b;
select * from t_a;
drop table t_b;
drop table t_a;

-- fix core
select utl_raw.substr('BEB37DA890800FD32D2A8659EBAA2CF4', 2142761564);
select utl_raw.substr('BEB37DA890800FD32D2A8659EBAA2CF4', -2142761564);
select utl_raw.substr('BEB37DA890800FD32D2A8659EBAA2CF4', -2142761564, 16);
select utl_raw.substr('BEB37DA890800FD32D2A8659EBAA2CF4', 2142761564, 16);
select utl_raw.substr('E7B703C44723FB6DEDF94B38426901BA', 1, -2142761564) from dual;
select utl_raw.substr('E7B703C44723FB6DEDF94B38426901BA', 1, 2142761564) from dual;
