/*
 * This file is mainly used to test opentenbase_ora function that can not distinguish its
 * category clearly.
 */
\c regression_ora

CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;

-- 1. test sys_guid
create table tbl_guid(a int, b raw(16));
insert into tbl_guid values(1, sys_guid());
insert into tbl_guid values(2, sys_guid());

begin
for i in 1..10 loop
insert into tbl_guid values(i, sys_guid());
end loop;
end;
/

select count(distinct(b)), count(*) from tbl_guid;

-- begin 122094827: to_number func
-- issue: dollar sign 
select to_number('$12345.678', '$999999.999') from dual;
select to_number('$1', '$0') from dual;
select to_number('-$12345.678', '$999999.999') from dual;
select to_number('-$1', '$0') from dual;

-- issue: leading space
select to_number('$1', '$0') from dual;
select to_number('   $1', '$0') from dual;
select to_number('   1', '0') from dual;
select to_number('   1 ', '0') from dual;	-- failed
select to_number('   1', ' 0') from dual;	-- failed

-- issue: E-
select to_number('1.230E-01', '999.999EEEE') from dual;
select to_number('1.230E-02', '999.999EEEE') from dual;
select to_number('1.230E+02', '999.999EEEE') from dual;

-- issue: MI
select to_number('0+', '9MI') from dual;
select to_number('123.321+', '999.999MI') from dual;
select to_number('0.123+', '0.000MI') from dual;
select to_number('0-', '9MI') from dual;

select to_number('-0 ', '9MI') from dual;			-- failed
select to_number('-0.123 ', '0.000MI') from dual;	-- failed

-- issue: U/L/C
select to_number('$12,345.678', 'U999999.999') from dual;
select to_number('$12,345.678', 'L999999.999') from dual;
select to_number('-$12,345.678', 'U999999.999') from dual;
select to_number('-$12,345.678', 'L999999.999') from dual;
select to_number('-USD123.456', 'C999.999') as c1 from dual;
select to_number('USD123.456', 'C999.999') as c1 from dual;

select to_number('-USD123.456', '999.999C') as c1 from dual;	-- failed
select to_number('USD123.456', '999.999C') as c1 from dual;		-- failed
select to_number('$12,345.678', '999999.999U') from dual;	-- failed
select to_number('$12,345.678', '999999.999C') from dual;	-- failed
select to_number('$12,345.678', '999999.999L') from dual;	-- failed
select to_number('-$12,345.678', '999999.999U') from dual;	-- failed
select to_number('-$12,345.678', '999999.999C') from dual;	-- failed
select to_number('-$12,345.678', '999999.999L') from dual;	-- failed

-- issue: negative value for PR
select to_number('12,345.543211', '99,999.99999PR') from dual;	-- failed
select to_number('<1>', '9PR') from dual;
select to_number('<12,345.54321>', '99,999.99999PR') from dual;
select to_number('-1', '9PR') from dual;			-- failed
select to_number('-12,345.54321', '99,999.99999PR') from dual; 	-- failed
-- end 122094827

-- begin 124560665
create user fn_test_admin_20240523 with superuser;
create user ua_20240523;
create user ub_20240523;
create schema sa_20240523;
create schema sb_20240523;
grant all on schema sa_20240523 to ua_20240523,ub_20240523;
grant all on schema sb_20240523 to ua_20240523,ub_20240523;
\c - UA_20240523
create or replace package sa_20240523.pkg_20240523 is
  procedure g();
end;
/

create or replace package body sa_20240523.pkg_20240523 is
  procedure g() is begin raise notice 'ua_20240523aaaaaaaaaaaaa'; end;
end;
/
\c - UB_20240523
create or replace package sb_20240523.pkg_20240523 is
  procedure g();
end;
/
create or replace package body sb_20240523.pkg_20240523 is
  procedure g() is begin raise notice 'ub_20240523bbbbbbbbb'; end;
end;
/
set search_path to sb_20240523;
call pkg_20240523.g();

\c - FN_TEST_ADMIN_20240523
drop schema sa_20240523 cascade;
drop schema sb_20240523 cascade;
drop user ua_20240523;
drop user ub_20240523;
-- end 124560665
-- begin 122030741:to_char(number, format) format: 'EEEE'
select r, length(r) len from (select to_char(0.9, '0EEEE') as r from dual);
select r, length(r) len from (select to_char(0.99, '0EEEE') as r from dual);
select r, length(r) len from (select to_char(1.9, '0EEEE') as r from dual);
select r, length(r) len from (select to_char(1.99, '0EEEE') as r from dual);
-- end 122030741
-- begin 121942923: to_char(number, format) format: '0.' '.' 'D' 'S'
-- decimal point before
select r, length(r) len from (select to_char(0.1, '.0') as r from dual);
select r, length(r) len from (select to_char(0.01, '.0') as r from dual);
-- decimal point end
select r, length(r) len from (select to_char(0.1, '0.') as r from dual);
select r, length(r) len from (select to_char(0.5, '0.') as r from dual);
-- decimal point only
select r, length(r) len from (select to_char(0.1, '.') as r from dual);
select r, length(r) len from (select to_char(.1, '.') as r from dual);

select r, length(r) len from (select to_char(0.1, 'D') as r from dual);
select r, length(r) len from (select to_char(.1, 'D') as r from dual);

select r, length(r) len from (select to_char(0, 'S') as r from dual);
select r, length(r) len from (select to_char(0.123, 'S') as r from dual);
select r, length(r) len from (select to_char(1, 'S') as r from dual);

select to_char(-0.0000009, '0.') as r from dual;
select to_char(-0.0000009, '.') as r from dual;
select to_char(-0.0000009, 'D') as r from dual;
select to_char(-0.0000009, 'S') as r from dual;
select to_char(-0.9, '0.') as r from dual;
select to_char(-0.9, '.') as r from dual;
select to_char(-0.9, 'D') as r from dual;
select to_char(-0.9, 'S') as r from dual;
-- end 121942923
-- begin 123115307: to_char(number,fmt): bigint out of range 
SELECt to_char(
		cast(55135582426434631181699136418973398693938386159612273425175282167211452172894766285847116673968556422636.6 as NUMBER),
		'999,999,999,999,999,999,999,999,999.99'
	) as col_4
FROm  dual;
-- end 123115307
-- begin 125917481
select r, length(r) len from (select to_char(10.4123::float4, 'TME') as r from dual);
select r, length(r) len from (select to_char(10.4123::float8, 'TME') as r from dual);
select r, length(r) len from (select to_char(10.4123, 'TME') as r from dual);
select TO_CHAR(12355555555555555555555555555555555555555555555555778899999, 'TM') FROM DUAL;
select TO_CHAR(12355555555555555555555555555555555555555555555555778899999, 'TME') FROM DUAL;
select TO_CHAR(12355555555555555555555555555555555555555555555555778899999.3333, 'TM') FROM DUAL;
select TO_CHAR(12355555555555555555555555555555555555555555555555778899999::float8, 'TM') FROM DUAL;
select TO_CHAR(12355555555555555555555555555555555555555555555555778899999.3333, 'TME') FROM DUAL;
-- end 125917481
-- begin 118159562
-- case 1: A.B
create package pkg_test_20240624 as
	function f1() return int;
end;
/
create or replace function pkg_test_20240624.f2() return int as	--failed
begin
	return 1;
end;
/
create package body pkg_test_20240624 as
	function f1() return int as
	begin
		return 100;
	end;
end;
/
create or replace function pkg_test_20240624.f2() return int as	--failed
begin
	return 1;
end;
/
drop package pkg_test_20240624;

-- case 2: A.B.C
create schema my_20240625;
create package my_20240625 as
	function f3() return int;
end;
/
create function my_20240625.f2() return int as	-- schema.func ok
begin
	return 1;
end;
/
create function my_202406251.f2() return int as	-- pkg.func failed
begin
	return 1;
end;
/
create package my_20240625.my_20240625 as
	function f1() return int;
end;
/
create function my_20240625.my_20240625.f2() return int as -- pkg.func failed
begin
	return 1;
end;
/
create function MY_202406252.f2() return int as -- pkg.func failed
begin
	return 1;
end;
/
drop package my_20240625.my_20240625;
drop package my_20240625;
drop function my_20240625.f2;
drop schema my_20240625;
-- end 118159562


-- begin 122607907
\c regression
RESET search_path;
\df test_call_with_subtran_ops
select test_call_with_subtran_ops();
-- end 122607907

\c regression_ora
select lengthb('2489662\2485765') from dual;

create table t1_20240805(c1 int, c2 int, c3 int);

CREATE OR REPLACE FUNCTION test_udf_20240805(inout v1 int) AS $$
DECLARE
 var1 int;
BEGIN
EXECUTE 'UPDATE t1_20240805 SET c1 = $1 WHERE col_numeric_key = $2' USING v1, var1;
END;
$$;
;

explain (costs off)
select test_udf_20240805(c2), c3 from t1_20240805 order by 1;

explain (costs off)
select test_udf_20240805(c2), c3 from t1_20240805 order by 2;

explain (costs off)
select test_udf_20240805(c2), c3 from t1_20240805 order by 1,2;

drop table t1_20240805;
