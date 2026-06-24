\c regression_ora

CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;

drop function if exists f3f;
drop procedure if exists p3p;
drop function if exists f1f;
drop procedure if exists p5p;

create or replace FUNCTION f3f (a int, b out number, c INOUT varchar2) return int is
begin
return a;
end;
/

create or replace procedure p3p(e int, f out number, g INOUT varchar2) is
aa int;
begin
aa := e;
end;
/

create or replace function f1f(a int, b number default 123) return int is
begin
return a;
end;
/

create or replace procedure p5p(a int, b varchar2 default 'abc', c number default 123, d number default NULL) is
e int;
begin
e := a;
end;
/


create or replace package pkg_fp is
function pf3(pa int, pb out number, pc INOUT varchar2) return int;
procedure pp3(pe int, pf out number, pg INOUT varchar2);
end;
/

create or replace package body pkg_fp is
function pf3(pa int, pb out number, pc INOUT varchar2) return int is
begin
return pa;
end;
procedure pp3(pe int, pf out number, pg INOUT varchar2) is
aa int;
begin
aa := pe;
end;
end;
/

select object_name, package_name, overload,
	   SUBPROGRAM_ID, ARGUMENT_NAME, position, sequence, 
	   DEFAULTED, DEFAULT_VALUE, IN_OUT, data_length, radix 
from all_arguments where object_name in ('F3F', 'P3P', 'PF3', 'PP3', 'P5P', 'F1F') order by object_name, position;

drop function f3f;
drop procedure p3p;
drop function f1f;
drop procedure p5p;
drop package pkg_fp;
