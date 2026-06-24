\c opentenbase_ora_package_function_regression_ora
-- test regexp_count(numeric, numeric)
select regexp_count(123423423466722341234, 123) from dual;
select regexp_count('123423423466722341234', '123') from dual;

select regexp_count(123423423466722341234, 123, 3) from dual;
select regexp_count('123423423466722341234', '123', 3) from dual;

select regexp_count(123423423466722341234, 123, 3, 'x') from dual;
select regexp_count('123423423466722341234', '123', 3, 'x') from dual;

-- test regexp_count(text, numeric)
select regexp_count('12341234', 34) from dual;
select regexp_count('12341234', 34, 3) from dual;
select regexp_count('12341234', 34, 4) from dual;
select regexp_count('12341234', 34, 1, 'x') from dual;

-- test regexp_count(numeric, text)
select regexp_count(12341234, '34') from dual;
select regexp_count(12341234, '34', 3) from dual;
select regexp_count(12341234, '34', 4) from dual;
select regexp_count(12341234, '34', 1, 'x') from dual;
