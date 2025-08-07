\c opentenbase_ora_package_function_regression_ora
create extension if not exists opentenbase_ora_package_function;
-- Test cases for function chr(text)
select chr('65'::text) from dual;
select chr('65'::varchar2) from dual;
select chr('65'::nvarchar2) from dual;
select chr('65'::char(2)) from dual;
select chr('65'::character(2)) from dual;
select chr('65'::character varying) from dual;
select chr('65'::nclob) from dual;
select chr('65'::clob) from dual;
