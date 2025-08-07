\c opentenbase_ora_package_function_regression_ora

create user usr_dbms_job_test with superuser;
SET SESSION AUTHORIZATION usr_dbms_job_test;


-- Tests for package DBMS_LOB
select dbms_lob.compare('10010', '10010');
select dbms_lob.compare('10010', '1001');
select dbms_lob.compare('10010', '100101');
select dbms_lob.compare('10010', '100101', 5);
select dbms_lob.compare('20010', '10010', 5, 1, 1);
select dbms_lob.compare('20010', '10010', 9, 2, 2);
select dbms_lob.compare('20010', '10010', 9, -2, 2);
select dbms_lob.compare('20010', '10010', 9, 2, 0);
select dbms_lob.compare('20010', '10010', 0, 2, 2);
select dbms_lob.compare(empty_clob(), '10010', 1, 2, 2);
select dbms_lob.compare('', '10010', 1, 2, 2);
select dbms_lob.compare('', '', 1, 1, 2);

call dbms_lob.createtemporary('abc'::blob, true, 1);
call dbms_lob.createtemporary('abc'::clob, false, 1);
call dbms_lob.freetemporary('abc'::blob);
call dbms_lob.freetemporary('abc'::clob);

select dbms_lob.open(BFILENAME('notexisted', 'notexisted'), 'r') from dual;
call dbms_lob.read(BFILENAME('notexisted', 'notexisted'), 1, 1, null);
select dbms_lob.close(BFILENAME('notexisted', 'notexisted')) from dual;
select dbms_lob.filecloseall();
select dbms_lob.open(BFILENAME('notexisted', 'notexisted'), 'r') from dual;
call dbms_lob.read(BFILENAME('notexisted', 'notexisted'), 1, 1, null);
select dbms_lob.close(BFILENAME('notexisted', 'notexisted')) from dual;
select dbms_lob.filecloseall();
-- Test for DBMS_LOB BFILE
drop table if exists test_bfile;
create table test_bfile (fId NUMBER, fName BFILE);
insert into test_bfile values (1, BFILENAME('bfiledir', 'bfile_test'));
create directory bfiledir as '/tmp';
select DIRECTORY_NAME,NODENAME,DIRECTORY_OWNER,DIRECTORY_PATH from pg_directory;
execute direct on (datanode_1) $$ select DIRECTORY_NAME,NODENAME,DIRECTORY_OWNER,DIRECTORY_PATH from pg_directory $$;
drop function if exists test_bfile_fun(IN a_xm bfile);
CREATE OR REPLACE FUNCTION test_bfile_fun(IN a_xm bfile) RETURNS varchar2 AS
DECLARE
    varc1    BFILE;
    amount int;
    buff  raw(2000);
    res   varchar2;
BEGIN
    varc1 := a_xm;
    perform dbms_lob.open(varc1, 'r');
    select dbms_lob.getlength(varc1) into amount;
    call dbms_lob.read(varc1, amount, 2, buff);
    call dbms_output.serveroutput(true);
    res := utl_raw.cast_to_varchar2(buff);
    perform dbms_lob.close(varc1);
    return 'done';
END;
/
select test_bfile_fun(BFILENAME('bfiledir', 'bfile_test')) from dual;
select test_bfile_fun(fName) from test_bfile;
drop function if exists test_bfile_fun(a_xm bfile, b_xm bfile);
drop table test_bfile;
drop directory bfiledir;

CREATE OR REPLACE FUNCTION test_substr(IN a_xm bfile, IN pos int) RETURNS int AS
str clob;
bstr blob;
varc1 BFILE;
ssr raw(256);
ss varchar2(256);
len int;
BEGIN
str := 'abcdefghijklmnopqrst';
bstr := hextoraw('41424344454647484950515253545556575859');
len := dbms_lob.GETLENGTH(str);
raise notice 'clob length: %', len;
len := dbms_lob.GETLENGTH(bstr);
raise notice 'blob length: %', len;
ss := dbms_lob.substr(str, 10, pos);
raise notice 'clob substr: %', ss;
ssr := dbms_lob.substr(bstr, 16, pos);
raise notice 'blob substr: %', utl_raw.cast_to_varchar2(ssr);
bstr := hextoraw('414243444546474849505152535455565758593');
ssr := dbms_lob.substr(bstr, 16, pos);
bstr := NULL;
ssr := dbms_lob.substr(bstr, 16, pos);
raise notice 'blob substr: %', utl_raw.cast_to_varchar2(ssr);
return 0;
END;
/

select test_substr(BFILENAME('bfiledir', 'bfile_test'), 3) from dual;
select test_substr(BFILENAME('bfiledir', 'bfile_test'), -3) from dual;
select test_substr(BFILENAME('bfiledir', 'bfile_test'), 23) from dual;
select dbms_lob.substr('32333435', 2, 2) from dual;

--TAPD:136529061
select sample_0."lanispl" as c0, opentenbase_ora.EMPTY_BLOB() as c1, cast(coalesce(sample_0."lanacl", sample_0."lanacl") as "pg_catalog"._ACLITEM) as c2 
from "pg_catalog"."pg_language" as sample_0 
where DBMS_LOB.SUBSTR(cast(opentenbase_ora.EMPTY_BLOB() as "opentenbase_ora".BLOB), 
                      cast(66 as "pg_catalog".INT4), cast(DBMS_RANDOM.RANDOM() as "pg_catalog".INT4)) >= opentenbase_ora.SYS_GUID() 
limit 92;

select dbms_lob.substr(to_clob('abcdef'), 2, -1) from dual;

select dbms_lob.substr(to_clob('abcdef'), 2, 1817436796) from dual;
--END 136529061
