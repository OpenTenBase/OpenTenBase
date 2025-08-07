/*
 * Basic test cases for plpgsql package.
 * 1. syntaxer
 *  1.1 create package
 *   1.1.1 with public
 *   1.1.2 with private
 *   1.1.3 with private and public
 *   1.1.4 all empty
 *   1.1.5 end pkg name
 *   1.1.6 with/without $foo$
 * 2. package execution
 *  2.1 execute package function
 *  2.2 call package function from other
 *  2.3 with public/private
 *  2.4 with initial part
 *  2.5 anonymous block
 * 3. package cache validate
 * 4. function end with name
 */

-- 1. syntaxer
--  1.1 create package
--   1.1.1 with public
create database dyn_pkg sql mode opentenbase_ora;
\c dyn_pkg

-- dynamic link package test
set package_link_method='dynamic';

drop package pkghobby;
create package pkghobby as
	p_a int default 10;
    procedure ds();
end;
/

create package body pkghobby as
	b int;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', pa;
	end;
end;
/

drop package pkghobby;
--   1.1.2 with private
--   1.1.3 with private and public
-- tested above
--   1.1.4 all empty
-- tested in create_package
--   1.1.5 end pkg name
create package pkghobby as
	p_a int default 10;
    procedure ds();
end pkghobby;
/
create package body pkghobby as
	b int;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', pa;
	end;
end pkghobby;
/
drop package pkghobby;
--   1.1.6 with/without $foo$
-- tested above and previous create package.

-- 2. package execution
--  2.1 execute package function
create package pkghobby as
	p_a int default 10;
    procedure ds();
end pkghobby;
/

create package body pkghobby as
	b int;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', pa;
	end;
end pkghobby;
/
call pkghobby.ds(); --error

create or replace package body pkghobby as
	b int;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', p_a;
	end;
end pkghobby;
/
call pkghobby.ds();
call pkghobby.ds();
call pkghobby.ds();

create or replace package body pkghobby as
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', p_a;
	end;
end pkghobby;
/
call pkghobby.ds();
call pkghobby.ds();
call pkghobby.ds();

create or replace package body pkghobby as
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		p_a = p_a + 1;
		raise notice '%', p_a;
	end;
end pkghobby;
/
call pkghobby.ds();
call pkghobby.ds();
call pkghobby.ds();

--
do
$$
begin
	pkghobby.p_a = pkghobby.p_a + 1;
	raise notice '%', pkghobby.p_a;
end;
$$;

do
$$
begin
	pkghobby.p_a = pkghobby.p_a + 1;
	raise notice '%', pkghobby.p_a;
end;
$$;
drop package pkghobby;

--  2.2 call package function from other
create package pkghobby as
	p_a int default 10;
    procedure ds();
end pkghobby;
/

create or replace package body pkghobby as
	b int;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', p_a;
	end;
end pkghobby;
/

create or replace procedure outer_func() as
$$
	declare
	  aa text;
	begin
		pkghobby.p_a = pkghobby.p_a + 1;
		raise notice '%', pkghobby.p_a;
	end;
$$;

call outer_func();
call outer_func();
call outer_func();
call pkghobby.ds();
drop package pkghobby;

--  2.3 with public/private
-- tested above
--  2.4 with initial part
create package pkghobby as
	p_a int default 10;
    procedure ds();
end pkghobby;
/

create or replace package body pkghobby as
	b int;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', p_a;
	end;
begin
	p_a = 100;
end pkghobby;
/

create or replace procedure outer_func() as
$$
	declare
	  aa text;
	begin
		pkghobby.p_a = pkghobby.p_a + 1;
		raise notice '%', pkghobby.p_a;
	end;
$$;

\x
select pkgheadersrc, pkgfuncspec, pkgbodysrc, pkgprivvarsrc, pkgprocdefsrc, pkginitsrc from pg_package where pkgname = 'PKGHOBBY';
\x

call outer_func();
call outer_func();
call outer_func();
call pkghobby.ds();
drop package pkghobby;

--  2.5 anonymous block
-- tested above

 -- 3. package cache validate
create package pkghobby as
	p_a int default 10;
    procedure ds();
end pkghobby;
/

create or replace package body pkghobby as
	b int;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', p_a;
	end;
begin
	p_a = 100;
end pkghobby;
/

create or replace procedure outer_func() as
$$
	declare
	  aa text;
	begin
		pkghobby.p_a = pkghobby.p_a + 1;
		raise notice '%', pkghobby.p_a;
	end;
$$;

\x
select pkgheadersrc, pkgfuncspec, pkgbodysrc, pkgprivvarsrc, pkgprocdefsrc, pkginitsrc from pg_package where pkgname = 'PKGHOBBY';
\x

call outer_func();
call outer_func();
create or replace package body pkghobby as
	b int;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', p_a;
	end;
begin
	p_a = 100;
end pkghobby;
/
call outer_func();
call outer_func();
create or replace package body pkghobby as
	b int;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', p_a;
	end;
begin
	p_a = 100;
end pkghobby;
/
call outer_func();
call outer_func();
drop package pkghobby;

-- 4. function end with name
create or replace procedure outer_func() as
      aa text;
    begin
        raise notice '%', 1;
    end;
/
call outer_func();
create or replace procedure outer_func() as
      aa text;
    begin
        raise notice '%', 1;
    end outer_func;
/
call outer_func();

/*
 * More test case of packages
 * 1. mix procedure and variables.
 * 2. cursor and type
 * 3. alter package
 * 4. other types used in package or outer
 *  4.1 row/text/record
 *  4.2 priv/pub name are same
 * 5. dyn-loaded/dep-loaded (other test case file)
 * 6. opentenbase_ora case
 *  6.1 pragma/package example
 *  6.2 more complex case?
 * 7. free tests:
 *  7.1 remove necessary vars - found, etc
 *  7.2 pkg_a.var in pkg_a
 *  7.3 many packages
 */

-- 1. mix procedure and variables.
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  procedure ds();
  ds_a int default 11;
  procedure ds2();
  ds_b int default 12;
  procedure ds3();
  ds_c int default 13;
end;
/
 
create or replace package body pkghobby as
  b int;
  procedure ds2();
  procedure ds() as
    aa text;
  begin
    raise notice '%', ds_a;
    ds_a = ds_a+1;
  end;
  procedure ds2() as
    bb text;
  begin
    raise notice '%', ds_b;
    ds_b = ds_b+1;
  end;
  procedure ds3() as
    cc text;
  begin
    raise notice '%', ds_c;
    ds_c = ds_c+1;
  end;
end;
/

call pkghobby.ds();
call pkghobby.ds2();
call pkghobby.ds3();
call pkghobby.ds();
call pkghobby.ds2();
call pkghobby.ds3();

-- 2. cursor and type
create table ctab(a int, b int);
insert into ctab values(generate_series(1, 20), generate_series(1, 20));

drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  cursor c1 return ctab%rowtype;
  procedure ds();
end;
/
 
create or replace package body pkghobby as
  b int;
  cursor c1 return ctab%rowtype is select * from ctab order by 1;
  procedure ds2();
  procedure ds() as
    aa text;
  begin
    for r in c1 loop
      raise notice '% from %', r.a, c1;
    end loop;
  end;
end;
/

call pkghobby.ds();
call pkghobby.ds();

-- fetch with state
-- package dropped also drop opened cursor
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  cursor c2 return ctab%rowtype;
  procedure ds();
end;
/
create or replace package body pkghobby as
  b int;
  cursor c2 return ctab%rowtype is select * from ctab order by 1;
  procedure ds2();
  procedure ds() as
    aa text;
    rd record;
  begin
    raise notice '%',c2%isopen;
    if not c2%isopen then
      open c2;
    end if;
    fetch c2 into rd;
    raise notice '%',rd;
  end;
end;
/
call pkghobby.ds();
call pkghobby.ds(); -- should move forward
call pkghobby.ds(); -- should move forward

begin
  raise notice 'isopen=%', pkghobby.c2%isopen;
  raise notice 'rowcount=%', pkghobby.c2%rowcount;
end;
/

declare
  a int;
begin
  raise notice 'isopen=%', pkghobby.c2%isopen;
  raise notice 'rowcount=%', pkghobby.c2%rowcount;
end;
/

declare
  r record;
begin
  raise notice 'rowcount=% found=%', pkghobby.c2%rowcount, pkghobby.c2%found;
  fetch pkghobby.c2 into r;
  raise notice 'rowcount=% found=% r=%', pkghobby.c2%rowcount, pkghobby.c2%found, r;
end;
/

-- cursor should append with package name
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  cursor c3 return ctab%rowtype;
  procedure ds();
end;
/
create or replace package body pkghobby as
  b int;
  cursor c3 return ctab%rowtype is select * from ctab order by 1;
  procedure ds2();
  procedure ds() as
    aa text;
    rd record;
    begin
      fetch c3 into rd;
      raise notice '%',rd;
    end;
  begin
    open c3;
end;
/
call pkghobby.ds();
call pkghobby.ds(); -- should move forward
call pkghobby.ds(); -- should move forward

-- package dropped also drop opened cursor
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  cursor c4 return ctab%rowtype;
  procedure ds();
end;
/
create or replace package body pkghobby as
  b int;
  cursor c4 return ctab%rowtype is select * from ctab order by 1;
  procedure ds2();
  procedure ds() as
    aa text;
    rd record;
    begin
      fetch c4 into rd;
      raise notice '%',rd;
    end;
  begin
    open c4;
end;
/
call pkghobby.ds(); -- should ok
call pkghobby.ds();
call pkghobby.ds();

-- reopen cursor?
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  cursor c5 return ctab%rowtype;
  procedure ds();
end;
/
create or replace package body pkghobby as
  b int;
  cursor c5 return ctab%rowtype is select * from ctab where a = 1 order by 1;
  procedure ds2();
  procedure ds() as
    aa text;
    rd record;
    begin
      fetch c5 into rd;
      raise notice '%',rd;
    end;
  begin
    open c5;
end;
/
call pkghobby.ds();
call pkghobby.ds(); -- no rows

begin
  close pkghobby.c5;
end;
/
begin
  open pkghobby.c5;
end;
/
call pkghobby.ds();
call pkghobby.ds();

-- support type in package
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  type v_ar is varray(8) of int;
  procedure ds();
end;
/
 
create or replace package body pkghobby as
  b int;
  procedure ds() as
	my_array v_ar;
  begin
	my_array ='{1,2,3}';
	for i in 1..my_array.count loop
		raise notice '%',my_array[i];
	end loop;
  end;
end;
/

call pkghobby.ds();
call pkghobby.ds();

declare
  my_array pkghobby.v_ar;
begin
	my_array ='{1,2,3}';
	for i in 1..my_array.count loop
		raise notice '%',my_array[i];
	end loop;
end;
/

create or replace function test_pt(my_array pkghobby.v_ar) is
aa int;
begin
	my_array ='{1,2,3}';
	for i in 1..my_array.count loop
		raise notice '%',my_array[i];
	end loop;
end;
/

select test_pt('{1234}');

-- access private
drop package pkghobby;
create package pkghobby as
	p_a int default 10;
    procedure ds();
end pkghobby;
/
create package body pkghobby as
	b int default 20;
    procedure ds2();
    procedure ds() as
	  aa text;
	begin
		raise notice '%', p_a + b;
	end;
end pkghobby;
/
call pkghobby.ds();
call pkghobby.ds();
drop package pkghobby;

-- 3. alter package
create package pkghobby as
	p_a int default 10;
    procedure ds();
end pkghobby;
/
create package body pkghobby as
  b int default 20;
  procedure ds2();
  procedure ds() as
    aa text;
  begin
    p_a = p_a + 2;
    raise notice '%', p_a;
  end;
end pkghobby;
/
call pkghobby.ds();
call pkghobby.ds();

alter package pkghobby compile;
call pkghobby.ds();
call pkghobby.ds();
alter package pkghobby compile debug;
alter package pkghobby compile debug reuse settings;
drop package pkghobby;

-- 4. other types used in package or outer
--  4.1 row/text/record
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  p_b text default 10;
  p_r record;
  p_rt ctab%rowtype;
  p_rtc ctab%rowtype;
  procedure ds(f int);
  procedure prt();
end pkghobby;
/
create package body pkghobby as
  b int default 20;
  procedure ds2();
  procedure ds(f int) as
    aa text;
  begin
    p_a = p_a + b;
    p_b = p_a + b;
    select * into p_r from ctab where a = f;
    select * into p_rt from ctab where a = f;
    p_rtc.a = f;

    raise notice 'p_a(int) %', p_a;
    raise notice 'p_b(text) %', p_b;
    raise notice 'p_r(record) %', p_r;
    raise notice 'p_rt(rowtype) %', p_rt;
    raise notice 'p_rtc.a(rowtypec) %', p_rtc.a;
  end;
  procedure prt() as
    aa text;
  begin
    raise notice 'p_a(int) %', p_a;
    raise notice 'p_b(text) %', p_b;
    raise notice 'p_r(record) %', p_r;
    raise notice 'p_rt(rowtype) %', p_rt;
    raise notice 'p_rtc.a(rowtypec) %', p_rtc.a;
  end;
end pkghobby;
/
-- change inner package and output
call pkghobby.ds(1);
call pkghobby.ds(2);
call pkghobby.ds(3);

call pkghobby.prt();

-- change outer page and output
drop procedure outer_func;
create or replace procedure outer_func(f int) as
begin
  pkghobby.p_a = pkghobby.p_a + 20;
  pkghobby.p_b = pkghobby.p_a + 20;
  select * into pkghobby.p_r from ctab where a = f;
  select * into pkghobby.p_rt from ctab where a = f;
  pkghobby.p_rtc.a = f;
end;
/

call outer_func(11);
call pkghobby.prt();
call outer_func(12);
call pkghobby.prt();
call outer_func(13);

call pkghobby.prt();
\c

-- dynamic link package test
set package_link_method='dynamic';

drop procedure outer_func;
create or replace procedure outer_func(f int) as
begin
  pkghobby.p_a = pkghobby.p_a + 20;
  pkghobby.p_b = pkghobby.p_a + 20;
  select * into pkghobby.p_r from ctab where a = f;
  select * into pkghobby.p_rt from ctab where a = f;
  pkghobby.p_rtc.a = f;
end;
/

call outer_func(11);
call outer_func(12);
call outer_func(13);

call pkghobby.prt();

-- should error when accessing package pub-var without package name qualified
drop procedure outer_func;
create or replace procedure outer_func(f int) as
begin
  pkghobby.p_a = pkghobby.p_a + 20;
  pkghobby.p_b = pkghobby.p_a + 20;
  select * into pkghobby.p_r from ctab where a = f;
  select * into pkghobby.p_rt from ctab where a = f;
  pkghobby.p_rtc.a = f;

  raise notice 'p_a(int) %', p_a; -- should be error
end;
/

call outer_func(11);

--  4.2 priv/pub name are same
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  procedure ds(f int);
end pkghobby;
/
create package body pkghobby as
  p_a int default 20; -- should error like in opentenbase_ora
  procedure ds2();
  procedure ds(f int) as
    aa text;
  begin
    p_a = p_a + f;

    raise notice 'p_a(int) %', p_a;
  end;
end pkghobby;
/

call pkghobby.ds(1);

-- 5. dyn-loaded/dep-loaded (other test case file)
-- pl_dyn_packages files
-- 6. opentenbase_ora case
--  6.1 pragma/package example
-- alter package
alter package a.pkghobby compile body     plsql_optimize_level=  2    plsql_code_type=  interpreted    plsql_debug=  false    plscope_settings=  'identifiers:none' reuse settings;
--  6.2 more complex case?
create or replace package emp_mgmt as
  function hire (last_name varchar2, job_id varchar2, manager_id number, salary number, commission_pct number, department_id number) return number;
  function create_dept(department_id number, location_id number) return number;
  procedure remove_emp(employee_id number);
  procedure remove_dept(department_id number);
  procedure increase_sal(employee_id number, salary_incr number);
  procedure increase_comm(employee_id number, comm_incr number);
  -- no_comm exception;
  -- no_sal exception;
end emp_mgmt;
/

create or replace package body emp_mgmt as
  tot_emps number;
  tot_depts number;
  function hire  (last_name varchar2, job_id varchar2, manager_id number, salary number, commission_pct number, department_id number)  return number
    is new_empno number;
  begin
    select employees_seq.nextval into new_empno from dual;
	insert into employees values (new_empno, 'first', 'last','first.last@opentenbase_ora.com', '(123)123-1234','18-jun-02','it_prog',90000000,00, 100,110);
	tot_emps := tot_emps + 1;
	return(new_empno);
  end;

  function create_dept(department_id number, location_id number)  return number
    is new_deptno number;
  begin
    select departments_seq.nextval  into new_deptno  from dual;
	insert into departments  values (new_deptno, 'department name', 100, 1700); tot_depts := tot_depts + 1; return(new_deptno);
  end;

  procedure remove_emp (employee_id number) is
  begin
	delete from employees where employees.employee_id = remove_emp.employee_id;
	tot_emps := tot_emps - 1;
  end;

  procedure remove_dept(department_id number) is
  begin
    delete from departments where departments.department_id = remove_dept.department_id;
	tot_depts := tot_depts - 1;
	select count(*) into tot_emps from employees;
  end;

  procedure increase_sal(employee_id number, salary_incr number) is
    curr_sal number;
  begin
    select salary into curr_sal from employees where employees.employee_id = increase_sal.employee_id;
	if curr_sal is null  then
	  raise exception 'no_sal';
	else
	  update employees  set salary = salary + salary_incr  where employee_id = employee_id;
	end if;
  end;

  procedure increase_comm(employee_id number, comm_incr number) is
    curr_comm number;
  begin
    select commission_pct into curr_comm from employees where employees.employee_id = increase_comm.employee_id;
	if curr_comm is null  then
	  raise exception 'no_comm';
	else
	  update employees  set commission_pct = commission_pct + comm_incr;
	end if;
  end;
end emp_mgmt;
/
-- 7. free tests:
--  7.1 remove necessary vars - found, etc
-- manual tested
--  7.2 pkg_a.var in pkg_a
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  procedure ds();
end pkghobby;
/
create package body pkghobby as
  b int default 20;
  procedure ds2();
  procedure ds() as
    aa text;
  begin
    raise notice '%', pkghobby.p_a + b;
  end;
end pkghobby;
/
call pkghobby.ds();
-- use package in 2,3 ident
drop package pkghobby;
create package pkghobby as
  p_a int default 10;
  procedure ds();
end pkghobby;
/
create package body pkghobby as
  b int default 20;
  procedure ds2();
  procedure ds() as
    aa text;
  begin
    raise notice '%', pkghobby.p_a;
  end;
end pkghobby;
/

-- schema.pkg
begin
  public.pkghobby.p_a = public.pkghobby.p_a + 1;
end;
/
call pkghobby.ds();
begin
  public.pkghobby.p_a = public.pkghobby.p_a + 1;
end;
/
call pkghobby.ds();

begin
  public.pkghobby.p_a = public.pkghobby.p_a + 1;
end;
/
call pkghobby.ds();

--  7.3 many packages
drop package opg1;
create package opg1 as
  a1 int default 11;
  b1 int default 11;
  procedure ds();
  procedure ds2();
end;
/
create package body opg1 as
  b int default 20;
  procedure ds() as
    aa text;
  begin
    raise notice '%', a1;
  end;
  procedure ds2() as
    aa text;
  begin
    raise notice '%', b1;
  end;
end;
/

drop package opg2;
create package opg2 as
  a1 int default 12;
  b1 int default 12;
  procedure ds();
  procedure ds2();
end;
/
create package body opg2 as
  b int default 20;
  procedure ds() as
    aa text;
  begin
	a1 = a1 + opg1.a1;
    raise notice '%', a1 + opg1.a1;
  end;
  procedure ds2() as
    aa text;
  begin
	b1 = b1 + opg1.b1;
    raise notice '%', b1 + opg1.b1;
  end;
end;
/

drop package opg3;
create package opg3 as
  a1 int default 13;
  b1 int default 13;
  procedure ds();
  procedure ds2();
end;
/
create package body opg3 as
  b int default 20;
  procedure ds() as
    aa text;
  begin
    raise notice '%', a1;
  end;
  procedure ds2() as
    aa text;
  begin
    raise notice '%', b1;
  end;
end;
/

drop package opg4;
create package opg4 as
  a1 int default 13;
  b1 int default 13;
  procedure ds();
  procedure ds2();
end;
/
create package body opg4 as
  b int default 20;
  procedure ds() as
    aa text;
  begin
    raise notice '%', a1;
  end;
  procedure ds2() as
    aa text;
  begin
    raise notice '%', b1;
  end;
end;
/

/*
 * anon-block call opg4, opg3 and opg2: opg4 = opg3 + opg2
 */
do
$$
begin
  opg4.a1 = opg2.a1 + opg3.a1;
  opg4.b1 = opg2.b1 + opg3.b1;

  call opg4.ds();
  call opg4.ds2();

  -- change opg2
  call opg2.ds();
  call opg2.ds2();

  opg4.a1 = opg2.a1 + opg3.a1;
  opg4.b1 = opg2.b1 + opg3.b1;

  call opg4.ds();
  call opg4.ds2();
end;
$$;

\c

-- dynamic link package test
set package_link_method='dynamic';

begin
  opg4.a1 = opg2.a1 + opg3.a1;
  opg4.b1 = opg2.b1 + opg3.b1;
end;
/

begin
  call opg4.ds();
  call opg4.ds2();
end;
/

begin
  -- change opg2
  call opg2.ds();
  call opg2.ds2();
end;
/

begin
  opg4.a1 = opg2.a1 + opg3.a1;
  opg4.b1 = opg2.b1 + opg3.b1;
end;
/

begin
  call opg4.ds();
  call opg4.ds2();
end;
/

call opg2.ds();
call opg2.ds2();

/*
 * Bugs for P0 case.
 */
-- function called in package function
create or replace package npkg is
procedure p; -- no-parameters in procedure in package
function f return int;

-- overload function/procedure in package
procedure pn(a int, b int);
procedure pn(a text);
function fn(a int) return int;
function fn(a text) return int;

a int;
procedure add_a;
procedure print_a;
end;
/

create or replace package body npkg is
procedure p is
begin
  raise notice 'proc p';
end;

function f return int is
begin
  raise notice 'func f';
  return 1;
end;

procedure pn(a int, b int) is
begin
  raise notice 'pn(a int, b int) --> % %', a, b;
end;

procedure pn(a text) is
begin
  raise notice 'pn(a text) --> %', a;
  call pn(1, 2);
end;

function fn(a int) return int is
begin
  raise notice 'fn(a int) --> %', a;
  return 1;
end;

function fn(a text) return int is
begin
  raise notice 'fn(a text) --> %', a;
  return 1;
end;

procedure add_a is
begin
 a = a+1;
end;

procedure print_a is
begin
  raise notice 'a = %', a;
end;

begin
 a = 1;
end;
/

call npkg.pn(1, 2);
call npkg.pn('test');
select npkg.fn(1);

-- package var changed instantly
begin
  npkg.add_a;
  npkg.print_a;
  npkg.add_a;
  npkg.print_a;
  npkg.add_a;
  npkg.print_a;
  npkg.add_a;
  npkg.print_a;
end;
/

-- ATHID in package
create or replace package au authid definer is
procedure p;
end;
/
drop package au;

-- empty package?
create package empkg is
end;
/

create package body empkg is
procedure a is
begin
null;
end;
end;
/
drop package empkg;

-- only type package
create or replace package ptyp is
  type a is ref cursor;
end;
/

-- case-sensitive
declare
  a ptyp.a;
begin
  raise notice '%',A%isOPen;
end;
/

declare
procedure f;
begin
end;
/
