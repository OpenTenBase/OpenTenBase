\c regression_ora
-- test implicit type cast from numeric to int
create or replace procedure test_call_assign_cast(a_int int)
as
$$
begin
   raise notice '%', a_int;
end;
$$
language default_plsql;

do
$$
declare
	a_numeric numeric:='1000';
begin
	call test_call_assign_cast(a_numeric);
end;
$$;

drop procedure test_call_assign_cast(int);

create or replace procedure test_call_assign_cast(a_int int, a_int1 int)
as
$$
begin
   raise notice '%','(int,int)';
end;
$$
language default_plsql;
create or replace procedure test_call_assign_cast(a_int numeric, a_int1 int)
as
$$
begin
   raise notice '%', '(numeric,int)';
end;
$$
language default_plsql;

do
$$
declare
        a_numeric numeric:='1000';
begin
        call test_call_assign_cast(a_numeric, a_numeric);
end;
$$;

create or replace procedure test_call_assign_cast(a_int int, a_int1 numeric)
as
$$
begin
   raise notice '%', '(int, numeric)';
end;
$$
language default_plsql;

do
$$
declare
        a_numeric numeric:='1000';
begin
        call test_call_assign_cast(a_numeric, a_numeric);
end;
$$;

drop procedure test_call_assign_cast(int,int);
drop procedure test_call_assign_cast(int,numeric);
drop procedure test_call_assign_cast(numeric,int);

-- test implicit from text/varchar/varchar2 to clob
create or replace procedure test_call_assign_cast(a_clob clob)
as
$$
begin
   raise notice '%', a_clob;
end;
$$
language default_plsql;

do
$$
declare
a_text text := 'text';
a_varchar varchar(20) := 'varchar';
a_varchar2 varchar2 := 'varchar2';
begin
call test_call_assign_cast(a_text);
call test_call_assign_cast(a_varchar);
call test_call_assign_cast(a_varchar2);
end;
$$;
drop procedure test_call_assign_cast(clob);

-- test implicit from binary_double to binary_float
create or replace procedure test_call_assign_cast(a_bfloat binary_float)
as
$$
begin
   raise notice '%', a_bfloat;
end;
$$
language default_plsql;

do
$$
declare
a_bdouble binary_double := 123.456;
begin
call test_call_assign_cast(a_bdouble);
end;
$$;
drop procedure test_call_assign_cast(binary_float);

--test implicit from binary_float/binary_double to int
create or replace procedure test_call_assign_cast(a_int int)
as
$$
begin
   raise notice '%', a_int;
       return 1;
end;
$$
language default_plsql;
do
$$
declare
a_bfloat binary_float := 123.456;
a_bdouble binary_double := 123.456;
begin
call test_call_assign_cast(a_bfloat);
call test_call_assign_cast(a_bdouble);
end;
$$;
drop procedure test_call_assign_cast(int);

\c regression
-- test implicit type cast from numeric to int
create or replace procedure test_call_assign_cast(a_int int)
as
$$
begin
   raise notice '%', a_int;
end;
$$
language default_plsql;

do
$$
declare
        a_numeric numeric:='1000';
begin
        call test_call_assign_cast(a_numeric);
end;
$$;

drop procedure test_call_assign_cast(int);

-- test implicit from timestamp to date
create or replace procedure test_call_assign_cast(a_date date)
as
$$
begin
   raise notice '%', a_date;
end;
$$
language default_plsql;
do
$$
declare
a_timestamp timestamp:='2022-1-1 10:00:00.000001';
begin
call test_call_assign_cast(a_timestamp);
end;
$$;
drop procedure test_call_assign_cast(date);

-- test implicit from text/varchar to clob
create or replace procedure test_call_assign_cast(a_clob clob)
as
$$
begin
   raise notice '%', a_clob;
end;
$$
language default_plsql;

do
$$
declare
a_text text := 'text';
a_varchar varchar(20) := 'varchar';
begin
call test_call_assign_cast(a_text);
call test_call_assign_cast(a_varchar);
end;
$$;
drop procedure test_call_assign_cast(clob);
