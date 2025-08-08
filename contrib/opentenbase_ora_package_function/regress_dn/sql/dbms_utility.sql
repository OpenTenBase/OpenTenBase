\c opentenbase_ora_package_function_regression_ora
-- Just create extension
create extension if not exists opentenbase_ora_package_function;
\set ECHO none
\pset format unaligned
/*
 * Test for dbms_utility.format_call_stack(char mode). 
 * Mode is hex. 
 * The callstack returned is passed to regex_replace function.
 * Regex_replace replaces the function oid from the stack with zero.
 * This is done to avoid random results due to different oids generated.
 * Also the line number and () of the function is removed since it is different
 * across different pg version.
 */

CREATE OR REPLACE FUNCTION checkHexCallStack() returns text  as $$
        DECLARE
             stack text;
        BEGIN
             select * INTO stack from dbms_utility.format_call_stack('o');
             select * INTO stack from regexp_replace(stack,'[ 0-9a-fA-F]{4}[0-9a-fA-F]{4}','       0',1);
             select * INTO stack from regexp_replace(stack,'[45()]',' ',1);
             return stack;
        END;
$$ LANGUAGE default_plsql;

/*
 * Test for dbms_utility.format_call_stack(char mode). 
 * Mode is integer.
 */

CREATE OR REPLACE FUNCTION checkIntCallStack() returns text  as $$
        DECLARE
             stack text;
        BEGIN
             select * INTO stack from dbms_utility.format_call_stack('p');
             select * INTO stack from regexp_replace(stack,'[ 0-9]{3}[0-9]{5}','       0',1);
             select * INTO stack from regexp_replace(stack,'[45()]',' ',1);
             return stack;
        END;
$$ LANGUAGE default_plsql;

/*
 * Test for dbms_utility.format_call_stack(char mode). 
 * Mode is integer with unpadded output.
 */

CREATE OR REPLACE FUNCTION checkIntUnpaddedCallStack() returns text  as $$
        DECLARE
             stack text;
        BEGIN
             select * INTO stack from dbms_utility.format_call_stack('s');
             select * INTO stack from regexp_replace(stack,'[0-9]{5,}','0',1);
             select * INTO stack from regexp_replace(stack,'[45()]',' ',1);
             return stack;
        END;
$$ LANGUAGE default_plsql;

select * from checkHexCallStack();
select * from checkIntCallStack();
select * from checkIntUnpaddedCallStack();

DROP FUNCTION checkHexCallStack();
DROP FUNCTION checkIntCallStack();
DROP FUNCTION checkIntUnpaddedCallStack();

--test for dbms_utility.get_hash_value
select dbms_utility.get_hash_value(null,0, 100) from dual;
select dbms_utility.get_hash_value(null,5, 100) from dual;
select dbms_utility.get_hash_value(null,-5, 100) from dual;
select dbms_utility.get_hash_value('null',null, 100) from dual;
select dbms_utility.get_hash_value('null',0, null) from dual;
select dbms_utility.get_hash_value('null',0, 100) from dual;
select dbms_utility.get_hash_value('opentenbase',-2, 100) from dual;
select dbms_utility.get_hash_value('opentenbase',-1, 100) from dual;
select dbms_utility.get_hash_value('opentenbase',0, 100) from dual;
select dbms_utility.get_hash_value('opentenbase',1, 100) from dual;
select dbms_utility.get_hash_value('opentenbase',2, 100) from dual;
select dbms_utility.get_hash_value('opentenbase',-200, 100) from dual;
select dbms_utility.get_hash_value('opentenbase',0,-2147483649) from dual;
select dbms_utility.get_hash_value('opentenbase',0,-2147483648) from dual;
select dbms_utility.get_hash_value('opentenbase',0,2147483647) from dual;
select dbms_utility.get_hash_value('opentenbase',0,2147483648) from dual;
select dbms_utility.get_hash_value('opentenbase',-2147483649,100) from dual;
select dbms_utility.get_hash_value('opentenbase',-2147483648,100) from dual;
select dbms_utility.get_hash_value('opentenbase',2147483647,100) from dual;
select dbms_utility.get_hash_value('opentenbase',2147483648,100) from dual;
select dbms_utility.get_hash_value('opentenbase',1000, 1024) from dual;
select dbms_utility.get_hash_value('opentenbase',1000, 1024.33333) from dual;
select dbms_utility.get_hash_value('opentenbase',1000, 1024.55555555) from dual;
select dbms_utility.get_hash_value('opentenbase',1000.33333, 1024) from dual;
select dbms_utility.get_hash_value('opentenbase',1000.55555555, 1024) from dual;
select dbms_utility.get_hash_value('opentenbaseopentenbaseopentenbaseopentenbaseopentenbaseopentenbaseopentenbaseopentenbase',1000, 1024) from dual;

-- test for dbms_utility.get_time
DECLARE
start_num NUMBER;
end_num   NUMBER;
time_elapsed NUMBER;
BEGIN
   start_num := dbms_utility.get_time;
   perform pg_sleep(1); -- 1s
   end_num := dbms_utility.get_time;
   time_elapsed := end_num - start_num;
   raise notice 'elapsed time is >= 100: %', time_elapsed >= 100;
END;
/

-- case 5

create or replace package patest_20240730_2 as
	procedure log();
end;
/
create or replace package body patest_20240730_2 as
	procedure log() as
		alllines varchar2(4000);
	begin
		allLines := dbms_utility.format_call_stack('o'); 
		allLines := regexp_replace(allLines,'[ 0-9a-fA-F]{4}[0-9a-fA-F]{4} ','       n ',1);
		CALL dbms_output.put_line(allLines);
	end;
end;
/
create or replace package patest_20240730 as
	procedure ftest();
end;
/
create or replace package body patest_20240730 as
	procedure ftest() as
	begin
		call patest_20240730_2.log();
	end;
end patest_20240730;
/
declare
begin
	call patest_20240730.ftest();
end;
/
drop package patest_20240730_2;
drop package patest_20240730;

-- tapd: 135749007
create or replace function fn_savelog_20241219(msg varchar2) return varchar2 is
begin
	  return 'Error Stack
	  ' || msg;
end;
/

CALL dbms_output.serveroutput('t');
declare /*- result:ignore */
	vNum number;
	vMsg varchar2(200);
begin
	select 1/0 into vNum from dual;
	exception
	when others then
		select fn_savelog_20241219( dbms_utility.format_call_stack ) into vMsg from dual;
		CALL dbms_output.put_line(vMsg);
		select 1/0 into vNum from dual;
end;
/
select/*- result:ignore */ fn_savelog_20241219( dbms_utility.format_call_stack ) "col1" from dual;
drop function fn_savelog_20241219;


--tapd:136795441
insert into "pg_catalog"."pg_pltemplate" values (cast(null as "pg_catalog".NAME), pg_catalog.XML_IS_WELL_FORMED(cast(UTL_FILE.TMPDIR() as "pg_catalog".TEXT)), true, default, default, default, DBMS_UTILITY.FORMAT_CALL_STACK(), default);
