/*
 * test for opentenbase_ora compatible userenv
 */
\c regression_ora
select userenv(null) from dual;
select userenv('') from dual;
select userenv(1) from dual;
select userenv('xdj') from dual;

select to_number(userenv('sid')) > 0 from dual;

select 't' from dual where regexp_replace(userenv('sid'), '[0-9]', 'x') is not null;
select 't' from dual where regexp_replace(userenv('SID'), '[0-9]', 'x') is not null;

select 't' from dual where regexp_replace(userenv('schemaid'), '[0-9]', 'x') is not null;
select 't' from dual where regexp_replace(userenv('SCHEMAID'), '[0-9]', 'x') is not null;

select userenv('lang') from dual;
select userenv('LANG') from dual;

select userenv('language') from dual;
select userenv('LANGUAGE') from dual;
select userenv('LANGuage') from dual;

SELECT USERENV('CLIENT_INFO') FROM DUAL;
SELECT USERENV('ENTRYID') FROM DUAL;
SELECT USERENV('ISDBA') FROM DUAL;
SELECT USERENV('LANG') IS NOT NULL FROM DUAL;
SELECT USERENV('LANGUAGE') IS NOT NULL FROM DUAL;
SELECT USERENV('SESSIONID') IS NOT NULL FROM DUAL;
SELECT USERENV('SID') IS NOT NULL FROM DUAL;
SELECT USERENV('TERMINAL') FROM DUAL;

create schema sid_schema;

/* currently definer is not working. just as current_user */
create or replace function sid_schema.test_sid(val int) returns varchar2 as
$$
declare
res varchar2(60);
begin
	select userenv('schemaid') into res;
	return res;
end;
$$language default_plsql;

create or replace function sid_schema.test_sid1(val int) return varchar2 authid current_user is
res varchar2(60); 
begin
select userenv('schemaid') into res;
return res;
end;
/

select 't' from dual where sid_schema.test_sid(3) is not null;
select 't' from dual where sid_schema.test_sid1(5) is not null;

set search_path to sid_schema;

select 't' from dual where sid_schema.test_sid(3) is not null;
select 't' from dual where sid_schema.test_sid1(5) is not null;

--select userenv('sid') from dual;

drop function test_sid;
drop function test_sid1;
drop schema sid_schema;
