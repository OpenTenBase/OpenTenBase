--
-- some case for package bugs
--

\c regression_ora;

--
-- Fixed the issue of not reporting errors when replacing packages if there are dependencies.
--
set enable_opentenbase_ora_pkg_nested_table to on;
set enable_create_package_rec_typ to on;

create or replace package pkg_pkg1_20230321
is
type rec1 is record(id int, id1 int);
type tab1 is table of int;
var int;
var1 rec1;
var2 tab1;
function func1(var int)return int;
end;
/

create or replace package body pkg_pkg1_20230321
is
function func1(var int)return int
is
begin
return 1;
end;
end;
/

create or replace function func_func1_20230321(param1 pkg_pkg1_20230321.var%type, param2 pkg_pkg1_20230321.var1%type, param3 pkg_pkg1_20230321.var2%type) return pkg_pkg1_20230321.var%type
is
begin
raise notice '%',param1;
raise notice '%',param2;
raise notice '%',param3;
return param1;
end;
/

select pkg_pkg1_20230321.func1(1);

drop package pkg_pkg1_20230321;--should error;

create or replace package pkg_pkg1_20230321 --should error;
is
type rec1 is record(id int, id1 int);
type tab1 is table of int;
var int;
var1 rec1;
var2 tab1;
function func1(var int)return int;
end;
/
select pkg_pkg1_20230321.func1(1);

drop function func_func1_20230321;
create or replace package pkg_pkg1_20230321 --success
is
type rec1 is record(id int, id1 int);
type tab1 is table of int;
var int;
var1 rec1;
var2 tab1;
function func1(var int)return int;
end;
/
select pkg_pkg1_20230321.func1(1); --error

create or replace package body pkg_pkg1_20230321
is
function func1(var int)return int
is
begin
return 1;
end;
end;
/
select pkg_pkg1_20230321.func1(1);

drop package pkg_pkg1_20230321;

reset enable_create_package_rec_typ;
