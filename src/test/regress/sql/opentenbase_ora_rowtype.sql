\c regression_ora
-- 1 rowtype with namespace
drop schema if exists sch1_20230531 cascade;
create schema sch1_20230531;
create table sch1_20230531.tbl1_20230531(c1 int, c2 text);

CREATE PROCEDURE p_test_mxm_20230531(p1 sch1_20230531.tbl1_20230531%rowtype)
AS
DECLARE
  r1 sch1_20230531.tbl1_20230531%rowtype;
BEGIN
  raise notice 'r1: %, p1: %', r1, p1;
  r1 := p1;
  p1.c1 := 1000;
  r1.c1 := 1;
  raise notice 'r1: %, p1: %', r1, p1;
END; 
/

declare
  r sch1_20230531.tbl1_20230531%rowtype;
begin
  r.c2 = 'abc';
  call p_test_mxm_20230531(r);
end;
/
drop procedure p_test_mxm_20230531;

set search_path = sch1_20230531;
CREATE PROCEDURE p_test_mxm_20230531(p1 tbl1_20230531%rowtype)
AS
DECLARE
  r1 sch1_20230531.tbl1_20230531%rowtype;
BEGIN
  raise notice 'r1: %, p1: %', r1, p1;
  r1 := p1;
  p1.c1 := 1000;
  r1.c1 := 1;
  raise notice 'r1: %, p1: %', r1, p1;
END; 
/

declare
  r sch1_20230531.tbl1_20230531%rowtype;
begin
  r.c2 = 'abc';
  call p_test_mxm_20230531(r);
end;
/

reset search_path;
drop procedure sch1_20230531.p_test_mxm_20230531;
drop table sch1_20230531.tbl1_20230531;
drop schema sch1_20230531;

drop table if exists table5;
drop view if exists tv;
drop table if exists tblres;
create table table5(a int, b varchar2(50), c number);
create or replace view tv as select a, b from table5;
insert into table5 values(generate_series(1, 10), chr(generate_series(1,10) + 96), generate_series(1, 10));
create table tblres(key1 int, des1 varchar2(50));
-- 2 rowtype as parameter
drop function if exists f2f;
create or replace function f2f(e int, b table5%rowtype) return int is
res table5%rowtype;
begin
res := b;
select * into res from table5 where table5.a = e;
insert into tblres values(res.a, res.b);
return b.b;
end;
/

drop function if exists p2p;
create or replace procedure p2p(e int, b table5%rowtype) is
res table5%rowtype;
begin
res := b;
select * into res from table5 where table5.a = e;
insert into tblres values(res.a, res.b);
end;
/

drop function if exists p4p;
create or replace procedure p4p(e1 int, e2 int, f out table5%rowtype, g IN OUT table5%rowtype) is
res table5%rowtype;
begin
res := g;
insert into tblres values(res.a, 'inout :' || res.b);
select * into res from table5 where table5.a = e1;
f := res;
select * into res from table5 where table5.a = e2;
g := res;
end;
/

create or replace package pkg_rowtype is dft_rt table5%rowtype; end;
/

drop function if exists p3p;
-- pl/sql not support using package var as default parameter
-- create or replace procedure p3p(e int, a table5%rowtype default pkg_rowtype.dft_rt, b table5%rowtype default null) is
create or replace procedure p3p(e int, b table5%rowtype default null) is
buf table5%rowtype;
begin
if b is null then
select * into buf from table5 where table5.a = e;
insert into tblres values(buf.a, 'null test, defualt val:' || buf.b);
else 
insert into tblres values(-1, 'not support null test of record type');
end if;
end;
/

declare
p1 table5%rowtype;
p2 table5%rowtype;
res int;
begin
res := f2f(5, p1);
call p2p(3, p1);
p2.a := 12;
p2.b := 'z';
p2.c := 12;
call p4p(6,7,p1,p2);
insert into tblres values(p1.a, 'OUT: ' || p1.b);
insert into tblres values(p2.a, 'OUT: ' || p2.b);
pkg_rowtype.dft_rt.a := 13;
pkg_rowtype.dft_rt.b := 'x';
pkg_rowtype.dft_rt.c := 13;
call p3p(8);
end;
/
select * from tblres order by key1;

-- 3 rowtype as return
drop function if exists f3f;
create or replace function f3f(e int, b int, c int) return table5%rowtype is
res table5%rowtype;
begin
select * into res from table5 where table5.a = e;
return res;
end;
/
declare
p1 table5%rowtype;
begin
p1 := f3f(6, 2, 2);
insert into tblres values(p1.a, p1.b);
end;
/
select * from tblres order by key1;

-- 4 rowtype from view
drop function if exists f2f;
create or replace function f2f(e int, f tv%rowtype) return int is
res tv%rowtype;
begin
res := f;
select * into res from tv where tv.a = e;
insert into tblres values(res.a, res.b);
return res.a;
end;
/

drop procedure if exists p2p;
create or replace procedure p2p(e int, b tv%rowtype) is
res tv%rowtype;
begin
res := b;
select * into res from tv where tv.a = e;
insert into tblres values(res.a, res.b);
end;
/

drop function if exists f3f;
create or replace function f3f(e int, b int, c int) return tv%rowtype is
res tv%rowtype;
begin
select * into res from tv where tv.a = e;
return res;
end;
/

declare
p1 tv%rowtype;
p2 tv%rowtype;
res int;
begin
res := f2f(7, p1);
call p2p(8, p1);
p2 := f3f(9, 1, 1);
insert into tblres values(p2.a, p2.b);
end;
/
select * from tblres order by key1;
drop table table2;
drop table table3;
drop table table4;
drop table table5 cascade;
drop table tblres;
drop package pkg_rowtype;