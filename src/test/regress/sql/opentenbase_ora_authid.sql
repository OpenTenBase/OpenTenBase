\c regression_ora
create extension if not exists opentenbase_ora_package_function;
create or replace function delt(tname varchar2) return int authid current_user
as
    sqlcmd varchar2(100);
begin
    sqlcmd := 'delete from '|| tname;
    execute sqlcmd;
    return 1;
end;
/
alter function delt pushdown;

create or replace function updt(tname varchar2, val int) return int authid current_user
as
    sqlcmd varchar2(100);
begin
    sqlcmd := 'update '|| tname || ' set b=' || val;
    execute sqlcmd;
    return 1;
end;
/
alter function updt pushdown;

create user nu_pre;

set role nu_pre;
create table tt1(a int, b int);
create table tt2(a int, b int);

insert into tt1 values (1,2);
insert into tt1 values (1,2);
insert into tt2 values (1,2);
insert into tt2 values (1,2);

reset role;
create user nu;
set role nu;
create table tt_nu1 (a int, b int);
insert into tt_nu1 values(1,2);
insert into tt_nu1 values(1,2);

create table tt_nu2 (a int, b int);
insert into tt_nu2 values(1,2);

set role nu_pre;
create or replace procedure prod1() authid definer
as
    val int;
begin
    select tt1.a into val from tt1,tt2 where tt1.a = 1 limit 1;
    raise notice '%',val;
end;
/

set role nu;
call prod1();

set role nu_pre;
create or replace procedure prod2() authid definer
as
    c1 refcursor;
    val int;
begin
    open c1 for select tt1.a from tt1,tt2 where tt1.a = 1;
    LOOP
        FETCH c1 into val;
        EXIT WHEN NOT FOUND;
        raise notice '%',val;
    END LOOP;
    raise notice '3';
end;
/

set role nu;
call prod2();


set role nu_pre;
create or replace procedure prod3(c1 inout refcursor) authid definer
as
    val int;
begin
    open c1 for select tt1.a from tt1,tt2 where tt1.a = 1;
    LOOP
        FETCH c1 into val;
        EXIT WHEN NOT FOUND;
        raise notice '%',val;
    END LOOP;
    raise notice '3';
end;
/

set role nu;
declare
    c1 refcursor;
begin
    call prod3(c1);
end;
/

set role nu_pre;
create or replace function func4(c1 in refcursor)  return int authid definer
as
    val int;
begin
    LOOP
        FETCH c1 into val;
        raise notice '%', val;
        EXIT WHEN 1=1;
    END LOOP;
    
    select a into val from tt1 limit 1;
    raise notice 'tt1 a: %', val;
    return val;
end;
/

set role nu;
insert into tt_nu1 values (1,2);
declare
    c1 refcursor;
    val int;
begin
	open c1 for select tt_nu1.a from tt_nu1,tt_nu2 where tt_nu1.a = 1;
    select func4(c1) into val from tt_nu1,tt_nu2 where tt_nu1.a = 1 limit 1;
    raise notice 'main sql is end.';
    FETCH c1 into val;
    raise notice '%', val;
end;
/

set role nu_pre;
create or replace function func5() return int authid definer
as
val int;
begin
    insert into tt1 values(1,3);
    delete from tt1;
    select 1/0 into val;
    return 0;
    exception when others then
        call dbms_output.put_line(SQLERRM);
        return 1;
end;
/

set role nu;
select func5();

reset role;
create user nu1;
set role nu1;
create table tnu1(a int,b int);
create or replace function fnu1() return int authid definer
as
begin
    insert into tnu1 values(1,1);
    update tnu1 set b=2;
    return 0;
end;
/

reset role;

create user nu2;
set role nu2;
create table tnu2(a int,b int);
create or replace function fnu2() return int authid definer
as
res int;
begin
    call dbms_output.serveroutput('t');
    insert into tnu2 values(1,1);
    update tnu2 set b=2;
    res := fnu1();
    call dbms_output.put_line(res);
    select delt('tnu1') into res;
    return 0;
    exception when others then
        call dbms_output.put_line(SQLERRM);
        return 1;
end;
/

reset role;
create user nu3;
set role nu3;
create table tnu3(a int,b int);
create or replace function fnu3() return int authid definer
as
res int;
begin
    call dbms_output.serveroutput('t');
    insert into tnu3 values(1,1);
    update tnu3 set b=2;
    res := fnu2();
    call dbms_output.put_line(res);
    select delt('tnu2') into res;
    return 0;
    exception when others then
        call dbms_output.put_line(SQLERRM);
        return 1;
end;
/


set role nu;
declare
res int;
begin
    call dbms_output.serveroutput('t');
    res := fnu2();
    call dbms_output.put_line(res);
end;
/

declare
res int;
begin
    call dbms_output.serveroutput('t');
    res := fnu3();
    call dbms_output.put_line(res);
end;
/

declare
res int;
begin
    call dbms_output.serveroutput('t');
    res := fnu1();
    call dbms_output.put_line(res);

    call dbms_output.serveroutput('t');
    res := fnu2();
    call dbms_output.put_line(res);

    call dbms_output.serveroutput('t');
    res := fnu3();
    call dbms_output.put_line(res);

    exception when others then
        call dbms_output.put_line(SQLERRM);
end;
/

set role nu1;
create table tnu11 (a int,b int);
insert into tnu11 values(1,2);
create table tnu12 (a int,b int);
insert into tnu12 values(1,2);
insert into tnu12 values(1,2);

create or replace function ffnu1(c1 in refcursor, t_name varchar2(20), p1 int) returns int authid definer
as
    val int;
begin
    open c1 for execute 'select updt('''|| t_name ||''', $1) from tnu11 t1,tnu12 t2 where t1.a=t2.a' using p1;
    fetch c1 into val;
    call dbms_output.put_line(val);
    return 0;
end;
/

set role nu;
-- execute update on read-only dn process, should error 
declare
    c1 refcursor;
    res int;
begin
    res := ffnu1(c1,'tnu1',3);
    fetch c1 into res;
    fetch c1 into res;
    call dbms_output.put_line(res);
end;
/

set role nu;
declare
    c1 refcursor;
    res int;
begin
    res := ffnu1(c1,'tnue',3);
    fetch c1 into res;
    fetch c1 into res;
    call dbms_output.put_line(res);
end;
/
-- execute update on read-only dn process, should error 
declare
    c1 refcursor;
    res int;
begin
    res := ffnu1(c1,'tnu1',3);
    reset role;
    call dbms_output.put_line(res);
end;
/

reset role;
select * from tnu1;
select * from tnu2;
select * from tnu3;

set role nu;
drop table tt_nu1;
drop table tt_nu2;
set role nu_pre;
drop table tt1;
drop table tt2;
drop procedure prod1;
drop procedure prod2;
drop procedure prod3;
drop function func4;
drop function func5;
set role nu1;
drop table tnu1;
drop table tnu11;
drop table tnu12;
drop function fnu1;
drop function ffnu1;
set role nu2;
drop table tnu2;
drop function fnu2;
set role nu3;
drop table tnu3;
drop function fnu3;
reset role;
drop role nu;
drop role nu_pre;
drop role nu1;
drop role nu2;
drop role nu3;
drop extension if exists opentenbase_ora_package_function;
