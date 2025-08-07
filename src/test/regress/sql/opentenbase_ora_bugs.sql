\c regression_ora

-- datetime to_char, format mixed with upper/lower character
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-dd hh24:Mi:ss') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'YyYy-Mm-dD hH24:Mi:sS') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'YyYy-Mm-dD hH12:Mi:sS') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'YyYy-Mm-dD hH12:Mi:sS.Ff') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'YyYy-Mm-dD hH12:Mi:sS.Ff.ff.FF.fF.ff1.ff2.ff3.ff4.ff5.ff6.ff7.ff8.ff9.Ff1.Ff2.Ff3.Ff4.Ff5.Ff6.Ff7.Ff8.Ff9.fF1.fF2.fF3.fF4.fF5.fF6.fF7.fF8.fF9.FF1.FF2.FF3.FF4.FF5.FF6.FF7.FF8.FF9') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'YyYy-Mm-dD hH12:Mi:sSsSs:SsSsS:ssss:Ssss:SSss:sSss:ss:Ss:sS:SS') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-ddD-Ddd-DdD-DDD hh24:Mi:ss') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-dd Hh24:Mi:mI:MI:mi:ss:Ss:SS') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'y,yyy-Y,yyy-Y,YYY-Y,Yyy-Y,yYY-mm-dd hh24:Mi:ss') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'yyy-Yyy-YyY-yYY-YYY-YyY-yy-YY-Yy-yY-y-Y-mm-dd hh24:Mi:ss') from dual;

select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'YyYy-Mm-dD') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'Dd-Mm-YyYY') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'Dd-Mm-Yy') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'Dd/Mm/Yy') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'Dd/Mm/Y,YYy') from dual;
select to_char(to_date('2023-01-03 15:04:10', 'yyyy-mm-dd hh24:mi:ss'), 'Dd/Mm/Yy wW Ww ww WW w W d D cc Cc cC CC') from dual;

CREATE TABLE varchar4000_20230707(
v1 VARCHAR(4000 CHAR),
v2 VARCHAR(4000 BYTE),
v3 VARCHAR(1CHAR),
v4 VARCHAR(10000BYTE),
v5 VARCHAR2(4000 CHAR),
v6 VARCHAR2(4000 BYTE),
v7 VARCHAR2(1CHAR),
v8 VARCHAR2(10000BYTE)
);
\d varchar4000_20230707

DO $$
DECLARE
v1 VARCHAR(4000 CHAR);
v2 VARCHAR(4000 BYTE);
v3 VARCHAR(1CHAR);
v4 VARCHAR(10000BYTE);
BEGIN
v1 := 'a'; v2 := 'b'; v3 := 'c'; v4 := 'd';
raise notice '% % % %', v1, v2, v3, v4;
END; $$;

DROP TABLE varchar4000_20230707;

-- TAPD: 113708041
\c regression
select to_char(to_timestamp('20230803151617','yyyyMMddHH24miss'), 'ssxff') from dual;
select to_char(to_timestamp('20230803151617','yyyyMMddHH24miss'), 'Ssxff') from dual;

\c regression_ora
select to_char(to_timestamp('20230803151617','yyyyMMddHH24miss'), 'ssxff') from dual;
select to_char(to_timestamp('20230803151617','yyyyMMddHH24miss'), 'Ssxff') from dual;
-- END TAPD: 113708041 

-- tapd:114541919

select to_date('', ' ') from dual;
select to_date(NULL, ' ') from dual;
select to_date('2023-08-23 11:59:59', '') from dual;
select to_date('2023-08-23 11:59:59', NULL) from dual;

--expected error
select to_date('2023-08-23 11:59:59', ' ') from dual;
select to_date('2023-08-23 11:59:59', '    ') from dual;

-- end taapd: 114541919

-- START TAPD: 887262271
select regexp_split_to_array(' ', ' ');
select regexp_split_to_array(' ', '');
select regexp_split_to_array('', ' ');
select regexp_split_to_array('   ', '  ');
select regexp_split_to_array('    ', '  ');
-- END TPAD: 887262271

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

-- tapd: 115928377
create or replace package s1 as
a int;
function func1 () return int;
end;
/

create or replace package body s1 as
function func1 () return int as
begin
raise notice '%', 'package s1 func1';
return 1;
end;
end;
/

--expected ok
select synonym1.func1();
drop package s1;
-- end tapd: 115928377

drop schema s1 cascade;
drop function s1;
--end tapd:114400655

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
-- START TAPD: 115868195
drop table if exists t_syntax_check07_20231001_5;
create table t_syntax_check07_20231001_5(employee_id number(6) primary key,
	first_name varchar2(20),
	last_name varchar2(25),
	email varchar2(25),
	phone_number varchar2(20),
	hire_date date,
	job_id varchar2(10),
	salary number(8,2),
	commission_pct number(2,2),
	manager_id number(6),
	department_id number(4)
);
-- Fix using non-existing columns to order by in listagg will produce core 
SELECT department_id "Dept.", LISTAGG(last_name, '; ') WITHIN GROUP (ORDER BY hire_date_no) "Employees"
FROM t_syntax_check07_20231001_5
GROUP BY department_id
ORDER BY department_id;
-- Using existing columns to order by in listagg will done. 
SELECT department_id "Dept.", LISTAGG(last_name, '; ') WITHIN GROUP (ORDER BY hire_date) "Employees"
FROM t_syntax_check07_20231001_5
GROUP BY department_id
ORDER BY department_id;
-- END TAPD: 115868195
