/*
 * Test case for raise_application_error()
 * 1. basic usage
 *  1.1 error code range
 *  1.2 call in function
 * 2. debug
 */
-- 1. basic usage
--  1.1 error code range
-- -20000 -20999
\c opentenbase_ora_package_function_regression_ora
call raise_application_error(-20000, 'application error');
call raise_application_error(-20999, 'application error');
call raise_application_error(-19999, 'application error');
call raise_application_error(-21000, 'application error');

--  1.2 call in function
create or replace function finner(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  a = 100/0;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20001, 'finner error message');
end;
/
select finner(1);

-- 2. debug
create or replace function finner(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  a = 100/0;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20001, 'finner test');
end;
/
create or replace function finner2(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20002, 'finner2 test');
end;
/
create or replace function finner3(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner2(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20003, 'finner3 test');
end;
/

create or replace function forc02(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner3(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20004, 'error test');
  
end;
/

select forc02(1); -- only last error

-- print error stack
create or replace function finner(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  a = 100/0;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20001, 'finner test', true);
end;
/
create or replace function finner2(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20002, 'finner2 test', true);
end;
/
create or replace function finner3(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner2(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20003, 'finner3 test', true);
end;
/

create or replace function forc02(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner3(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20004, 'error test', true);
  
end;
/

select forc02(1); -- error stack

-- last error
create or replace function forc02(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner3(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20004, 'error test', false);
  
end;
/
select forc02(1); -- only last error

-- break chain
create or replace function finner(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  a = 100/0;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20001, 'finner test', true);
end;
/
create or replace function finner2(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20002, 'finner2 test', false);
end;
/
create or replace function finner3(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner2(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20003, 'finner3 test', true);
end;
/

create or replace function forc02(a int) return int is
  cursor c(r1 int, r2 int)
       is select * from dual;
  a int;
begin
  select finner3(1) into a;
exception
  when others then
   RAISE_APPLICATION_ERROR(-20004, 'error test', true);
  
end;
/

select forc02(1); -- error stack

-- psql
--\set VERBOSITY verbose
set client_min_messages to error;
select forc02(1); -- error stack

-- test raise_application_error debug param
drop procedure if exists p_exception(x in number);
create or replace procedure p_exception(x in number)
is
begin
  if x<0 then
      RAISE_APPLICATION_ERROR( - 20001 ,  ' x 小于0' );
  end if;
end ;
/

drop procedure if exists p_call_exception(x number);
create or replace procedure  p_call_exception(x number) is
  v int;
begin
   p_exception(x);
exception
  when others then
    RAISE_APPLICATION_ERROR( -20003 ,  ' x 小于0',true );
end ;
/

call  p_call_exception(-1);
