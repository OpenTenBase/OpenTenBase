/*
 * Test cases for cursor ROWTYPE.
 * 1. declare cursor name and ROWTYPE
 *   1.1 execute many times
 * 2. FETCH INTO
 *   2.1 rowtype from same cursor
 *   2.2 rowtype from different cursor with same number field and same type
 *   2.3 rowtype from different cursor with different number field or different type
 * 3. SELECT INTO
 *   3.1 rowtype with same number field and same type
 *   3.2 rowtype with different number field or different type
 * 4. assignment
 *   4.1 rvalue not initialize
 *   4.2 lvalue and rvalue from same cursor
 *   4.3 lvalue and rvalue from different cursor with same number field and same type
 *   4.4 lvalue and rvalue from different cursor with different number field or different type
 *   4.5 lvalue or rvalue are table%rowtype and cursor%rowtype
 * 5. name of rowtype is same as declared cursor, even if assigned/FETCH INTO from different source
 * 6. refcursor testing / cursor with parameters
 * 7. exception testing
 *   7.1 no cursor or table in current namespace.
 * 8. memory leak?
 * 9. rowtype as parameter
 * 10. rowtype as return parameter
 * 11. rowtype from view
 * 12. rowtype with namespace
 */

\c regression_ora

drop table if exists table2;
create table table2(a int, b int);
insert into table2 values(generate_series(1, 10), generate_series(1, 10));

create table table3(a int);
insert into table3 values(generate_series(1, 10));

create table table4(a text, b text);
insert into table4 values(generate_series(1, 10), generate_series(1, 10));

drop table if exists table5;
drop view if exists tv;
drop table if exists tblres;
create table table5(a int, b varchar2(50), c number);
create or replace view tv as select a, b from table5;
insert into table5 values(generate_series(1, 10), chr(generate_series(1,10) + 96), generate_series(1, 10));
create table tblres(key1 int, des1 varchar2(50));

-- 1. declare cursor name and ROWTYPE
--   1.1 execute many times
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select * from table2 order by 1;
	rc c%rowtype;
BEGIN
	open c;
	fetch c into rc;
	while found loop
		raise notice 'value % %', rc.a, rc.b;
		fetch c into rc;
	end loop;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- 2. FETCH INTO
--   2.1 rowtype from same cursor
-- tested above
--   2.2 rowtype from different cursor with same number field and same type
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select * from table2 order by 1;
	c_o cursor for select * from table2 order by 1;
	rc c_o%rowtype;
BEGIN
	open c;
	fetch c into rc;
	while found loop
		raise notice 'value % %', rc.a, rc.b;
		fetch c into rc;
	end loop;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- with different name
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table2 order by 1;
	c_o cursor for select a aa, b bb from table2 order by 1;
	rc c_o%rowtype;
BEGIN
	open c;
	fetch c into rc;
	while found loop
		raise notice 'value % %', rc.aa, rc.bb;
		fetch c into rc;
	end loop;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

--   2.3 rowtype from different cursor with different number field or different type
-- different field number
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a from table2 order by 1;
	c_o cursor for select a aa, b bb from table2 order by 1;
	rc c_o%rowtype;
BEGIN
	open c;
	fetch c into rc;
	while found loop
		raise notice 'value % %', rc.aa, rc.bb;
		fetch c into rc;
	end loop;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- different field number
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table2 order by 1;
	c_o cursor for select a aa from table2 order by 1;
	rc c_o%rowtype;
BEGIN
	open c;
	fetch c into rc;
	while found loop
		raise notice 'value % %', rc.aa, rc.bb;
		fetch c into rc;
	end loop;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- different field number
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table2 order by 1;
	c_o cursor for select a aa from table2 order by 1;
	rc c_o%rowtype;
BEGIN
	open c;
	fetch c into rc;
	while found loop
		raise notice 'value %', rc.aa;
		fetch c into rc;
	end loop;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- different field type
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b::text || '55' from table2 order by 1;
	c_o cursor for select a aa, b bb from table2 order by 1;
	rc c_o%rowtype;
BEGIN
	open c;
	fetch c into rc;
	while found loop
		raise notice 'value % %', rc.aa, rc.bb;
		fetch c into rc;
	end loop;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- different field type
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table2 order by 1;
	c_o cursor for select a aa, b::text || '55' bb from table2 order by 1;
	rc c_o%rowtype;
BEGIN
	open c;
	fetch c into rc;
	while found loop
		raise notice 'value % %', rc.aa, rc.bb;
		fetch c into rc;
	end loop;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- 3. SELECT INTO
--   3.1 rowtype with same number field and same type
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table2 order by 1;
	rc c%rowtype;
BEGIN
	select a, b into rc from table2 where a = 1;
	raise notice 'value % %', rc.a, rc.b;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

--   3.2 rowtype with different number field or different type
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a from table2 order by 1;
	rc c%rowtype;
BEGIN
	select a, b into rc from table2 where a = 1;
	raise notice 'value %', rc.a;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table2 order by 1;
	rc c%rowtype;
BEGIN
	select a into rc from table3 where a = 1;
	raise notice 'value % %', rc.a, rc.b;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table2 order by 1;
	rc c%rowtype;
BEGIN
	select a into rc from table4 where a = 1;
	raise notice 'value % %', rc.a, rc.b;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table4 order by 1;
	rc c%rowtype;
BEGIN
	select a, b into rc from table2 where a = 1;
	raise notice 'value % %', rc.a, rc.b;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table4 order by 1;
	rc c%rowtype;
BEGIN
	select a, 'txt' into rc from table2 where a = 1;
	raise notice 'value % %', rc.a, rc.b;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor for select a, b from table4 order by 1;
	rc c%rowtype;
BEGIN
	select a, b||'txt' into rc from table2 where a = 1;
	raise notice 'value % %', rc.a, rc.b;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- 4. assignment
--   4.1 rvalue not initialize
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select * from table2 order by 1;
	rc c%rowtype;
BEGIN
	raise notice 'value % %', rc.a, rc.b;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select * from table2 order by 1;
	rc c%rowtype;
	rc_1 c%rowtype;
BEGIN
	rc = rc_1;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

--   4.2 lvalue and rvalue from same cursor
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select * from table2 order by 1;
	rc c%rowtype;
	rc_1 c%rowtype;
BEGIN
	open c;
	fetch c into rc_1;
	rc = rc_1;
	raise notice 'value % %', rc.a, rc.b;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

--   4.3 lvalue and rvalue from different cursor with same number field and same type
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select * from table2 order by 1;
	c_1 cursor  for select * from table2 order by 1;
	rc c%rowtype;
	rc_1 c_1%rowtype;
BEGIN
	open c;
	fetch c into rc;
	rc_1 = rc;
	raise notice 'value % %', rc_1.a, rc_1.b;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

--   4.4 lvalue and rvalue from different cursor with different number field or different type
-- different number
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select a, b from table2 order by 1;
	c_1 cursor  for select a from table2 order by 1;
	rc c%rowtype;
	rc_1 c_1%rowtype;
BEGIN
	open c;
	fetch c into rc;
	rc_1 = rc;
	raise notice 'value %', rc_1.a;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- different type
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select a, b from table2 order by 1;
	c_1 cursor  for select a from table4 order by 1;
	rc c%rowtype;
	rc_1 c_1%rowtype;
BEGIN
	open c;
	fetch c into rc;
	rc_1 = rc;
	raise notice 'value %', rc_1.a;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

--   4.5 lvalue or rvalue are table%rowtype and cursor%rowtype
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select a, b from table2 order by 1;
	rc c%rowtype;
	rc_1 table4%rowtype;
BEGIN
	open c;
	fetch c into rc;
	rc_1 = rc;
	raise notice 'value % %', rc_1.a, rc_1.b;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select a, b from table2 order by 1;
	rc c%rowtype;
	rc_1 table4%rowtype;
BEGIN
	select * INTO rc_1 from table4 where a = 1 order by 1;
	rc = rc_1;
	raise notice 'value % %', rc.a, rc.b;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- 5. name of rowtype is same as declared cursor, even if assigned/FETCH INTO from different source
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select a, b from table2 order by 1;
	c_1 cursor  for select a an, b bn from table2 order by 1;
	rc_1 c_1%rowtype;
BEGIN
	open c;
	fetch c into rc_1;
	raise notice 'value %', rc_1.a;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor  for select a, b from table2 order by 1;
	c_1 cursor  for select a an, b bn from table2 order by 1;
	rc_1 c_1%rowtype;
BEGIN
	open c;
	fetch c into rc_1;
	raise notice 'value %', rc_1.an;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- 6. refcursor testing / cursor with parameters
create or replace function row_type_func() returns text as $$
declare
 	c refcursor;
 	x integer;
	vr c%rowtype;
begin
	open c scroll for select a from table2 order by 1;
	fetch c into vr;
	close c;
	RETURN 'OK' || vr.a;
end;
$$ language default_plsql;
select row_type_func();

create or replace function row_type_func() returns text as $$
declare
 	c refcursor;
 	d refcursor;
 	x integer;
	vr c%rowtype;
	vdr d%rowtype;
begin
	open c scroll for select a from table2 order by 1;
	fetch c into vdr;
	close c;
	RETURN 'OK' || vr.a;
end;
$$ language default_plsql;
select row_type_func();

-- cursor with parameter
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor (p int) for select a, b from table2 where a=p order by 1;
	rc c%rowtype;
BEGIN
	open c(2);
	fetch c into rc;
	raise notice 'value % %', rc.a, rc.b;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor (p int, q int) for select a, b from table2 where a=p+q order by 1;
	rc c%rowtype;

	c2 cursor for select a, b from table2 order by 1;
	rc2 c2%rowtype;
BEGIN
	open c2;
	fetch c2 into rc;
	raise notice 'value % %', rc.a, rc.b;
	close c2;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();

-- 7. exception testing
--   7.1 no cursor or table in current namespace.
CREATE or replace FUNCTION row_type_func() RETURNS text AS $$
DECLARE
	c cursor (p int) for select a, b from table2 where a=p order by 1;
	rc nof%rowtype;
BEGIN
	open c(2);
	fetch c into rc;
	raise notice 'value % %', rc.a, rc.b;
	close c;
    RETURN 'OK';
END;
$$ LANGUAGE default_plsql;
select row_type_func();
-- 8. memory leak?
-- manual tested

-- 9 rowtype as parameter
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

-- 10 rowtype as return
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

-- 11 rowtype from view
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

-- 12 rowtype with namespace
drop schema if exists sch1_20230531 cascade;
create schema sch1_20230531;
create table sch1_20230531.tbl1_20230531(c1 int, c2 text);

CREATE PROCEDURE p_test_mxm_20230531(p1 sch1_20230531.tbl1_20230531%rowtype)
LANGUAGE default_plsql
AS $$ 
DECLARE
  r1 sch1_20230531.tbl1_20230531%rowtype;
BEGIN
  raise notice 'r1: %, p1: %', r1, p1;
  r1 := p1;
  p1.c1 := 1000;
  r1.c1 := 1;
  raise notice 'r1: %, p1: %', r1, p1;
END; 
$$;

do $$
declare
  r sch1_20230531.tbl1_20230531%rowtype;
begin
  r.c2 = 'abc';
  call p_test_mxm_20230531(r);
end;
$$;
drop procedure p_test_mxm_20230531;

set search_path = sch1_20230531;
CREATE PROCEDURE p_test_mxm_20230531(p1 tbl1_20230531%rowtype)
LANGUAGE default_plsql
AS $$ 
DECLARE
  r1 sch1_20230531.tbl1_20230531%rowtype;
BEGIN
  raise notice 'r1: %, p1: %', r1, p1;
  r1 := p1;
  p1.c1 := 1000;
  r1.c1 := 1;
  raise notice 'r1: %, p1: %', r1, p1;
END; 
$$;

do $$
declare
  r sch1_20230531.tbl1_20230531%rowtype;
begin
  r.c2 = 'abc';
  call p_test_mxm_20230531(r);
end;
$$;

reset search_path;
drop procedure sch1_20230531.p_test_mxm_20230531;
drop table sch1_20230531.tbl1_20230531;
drop schema sch1_20230531;
