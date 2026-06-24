---  1020421696092367411
\c regression_ora
drop extension if exists opentenbase_ora_package_function;
create schema dbms_output;
create schema s1020421696092367411;
set search_path to pubilc,s1020421696092367411;

create or replace function dbms_output.serveroutput(a text) return int is
begin
  return 1;
end;
/

create or replace function dbms_output.put_line(a text) return int is
begin
  raise notice '%', a;
  return 1;
end;
/

create or replace function dbms_output.put_line(a bigint) return int is
begin
  raise notice '%', a;
  return 1;
end;
/

create table a(id int, mc varchar2(10));
insert into a values(1, 'OpenTenBase');
insert into a values(2, 'OpenTenBase');
insert into a values(3, 'OpenTenBase');

create table b(id int, mc varchar2(10));

declare
	v_data a%rowtype;
begin
	perform dbms_output.serveroutput('t');
	select * into v_data from a WHERE id=4;
	-- update a set mc='abc' WHERE id=4;
	-- delete from a where id=4;
	--insert into b select * from a where id=4;

	if sql%isopen then
		perform dbms_output.put_line('Openging');
	else
		perform dbms_output.put_line('closing');
	end if;

	if sql%found then
		perform dbms_output.put_line('valid data');
	else
		perform dbms_output.put_line('Sorry');
	end if;
	
	if sql%notfound then
		perform dbms_output.put_line('Also Sorry');
	else
		perform dbms_output.put_line('Haha');
	end if;

	perform dbms_output.put_line(sql%rowcount);
	exception
		when no_data_found then
			perform dbms_output.put_line('Sorry No data');
		when too_many_rows then
			perform dbms_output.put_line('Too Many rows');
end;
/

declare
	v_data a%rowtype;
begin
	perform dbms_output.serveroutput('t');
	--select * into v_data from a WHERE id=4;
	update a set mc='abc' WHERE id=4;
	-- delete from a where id=4;
	--insert into b select * from a where id=4;

	if sql%isopen then
		perform dbms_output.put_line('Openging');
	else
		perform dbms_output.put_line('closing');
	end if;

	if sql%found then
		perform dbms_output.put_line('valid data');
	else
		perform dbms_output.put_line('Sorry');
	end if;
	
	if sql%notfound then
		perform dbms_output.put_line('Also Sorry');
	else
		perform dbms_output.put_line('Haha');
	end if;

	perform dbms_output.put_line(sql%rowcount);
	exception
		when no_data_found then
			perform dbms_output.put_line('Sorry No data');
		when too_many_rows then
			perform dbms_output.put_line('Too Many rows');
end;
/

declare
	v_data a%rowtype;
begin
	perform dbms_output.serveroutput('t');
	--select * into v_data from a WHERE id=4;
	-- update a set mc='abc' WHERE id=4;
	delete from a where id=4;
	--insert into b select * from a where id=4;

	if sql%isopen then
		perform dbms_output.put_line('Openging');
	else
		perform dbms_output.put_line('closing');
	end if;

	if sql%found then
		perform dbms_output.put_line('valid data');
	else
		perform dbms_output.put_line('Sorry');
	end if;
	
	if sql%notfound then
		perform dbms_output.put_line('Also Sorry');
	else
		perform dbms_output.put_line('Haha');
	end if;

	perform dbms_output.put_line(sql%rowcount);
	exception
		when no_data_found then
			perform dbms_output.put_line('Sorry No data');
		when too_many_rows then
			perform dbms_output.put_line('Too Many rows');
end;
/

declare
	v_data a%rowtype;
begin
	perform dbms_output.serveroutput('t');
	--select * into v_data from a WHERE id=4;
	-- update a set mc='abc' WHERE id=4;
	-- delete from a where id=4;
	insert into b select * from a where id=4;

	if sql%isopen then
		perform dbms_output.put_line('Openging');
	else
		perform dbms_output.put_line('closing');
	end if;

	if sql%found then
		perform dbms_output.put_line('valid data');
	else
		perform dbms_output.put_line('Sorry');
	end if;
	
	if sql%notfound then
		perform dbms_output.put_line('Also Sorry');
	else
		perform dbms_output.put_line('Haha');
	end if;

	perform dbms_output.put_line(sql%rowcount);
	exception
		when no_data_found then
			perform dbms_output.put_line('Sorry No data');
		when too_many_rows then
			perform dbms_output.put_line('Too Many rows');
end;
/
reset search_path;
drop schema dbms_output cascade;
drop schema s1020421696092367411 cascade;

-- loop cursor with commit/rollbak
CREATE or REPLACE PROCEDURE p_econtext32() AS
DECLARE
	rec RECORD;
BEGIN
	FOR rec IN SELECT t.i as a FROM generate_series(1,10) t(i)
	LOOP
		IF rec.a % 2 = 1 THEN
			commit;
		ELSE
			rollback;
		END IF;
	END LOOP;
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
END;
/
call p_econtext32();
drop procedure if exists p_econtext32;

-- rollback cannot drop the portal of the loop statement
declare
	rec record;
begin
	for rec in select * from generate_series(1,2)
	loop
    	raise notice '%', rec;
        rollback;
	end loop;
exception when others then
	raise notice '%', sqlerrm;
end;
/

-- reset _SPI_errstack when MessageContext is reset
drop package plsql_52180_20240527_pkg2;
create or replace package plsql_52180_20240527_pkg2
is
mergeExcep EXCEPTION;
procedure mergeOp(opIdx number);
procedure bootstrap(loops number);
end plsql_52180_20240527_pkg2;
/

create or replace package body plsql_52180_20240527_pkg2
is
procedure mergeOp(opIdx number)
is
  idx number := 0;
begin
   if mod(opIdx,1) = 0 then
     RAISE mergeExcep;
   end if;
end mergeOp;

procedure bootstrap(loops number)
is
begin
   FOR opIdx IN 1..loops
   LOOP
   BEGIN
 		mergeOp(opIdx);
     	commit;
    EXCEPTION when others then
        commit;
    END;
  END LOOP;
end bootstrap;
end plsql_52180_20240527_pkg2;
/

begin;
begin
plsql_52180_20240527_pkg2.bootstrap(3);
end;
/

begin
plsql_52180_20240527_pkg2.bootstrap(3);
end;
/
rollback;
drop package plsql_52180_20240527_pkg2;

-- cursor opened by for stmt should be closed when for stmt end
create table table_2024_06_22_t1(id int primary key, num number);

begin
 for x in 1..10
 loop
  insert into table_2024_06_22_t1 values(x,x);
 end loop;
end;
/

begin;
declare 
  myError exception;
  myError2 exception;
  ans varchar2(200);

  procedure at1 
  is
    myError exception;
    myError2 exception;
    cursor vCur is select * from table_2024_06_22_t1 order by id;
    vRec  table_2024_06_22_t1%rowtype;
  BEGIN
    for vRec in vCur
    loop
      savepoint sp1;
      update table_2024_06_22_t1 set num = num + vRec.id where id = vRec.id;
      if vRec.id = 5 then
        rollback to sp1;
      end if;
      if vRec.id = 9 then
        raise myError;
      end if;
    end loop;
    commit;
  exception
    when others then
      commit;
  end at1;
begin
  BEGIN
    at1;
  exception
    when others then null;
  end;
  BEGIN
    at1;
  exception
    when others then null;
  end;
  BEGIN
    at1;
  exception
    when others then null;
  end;
end;
/
rollback;
drop table table_2024_06_22_t1;
