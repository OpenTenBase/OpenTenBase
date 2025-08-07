\c regression_ora
create EXTENSION if not exists opentenbase_ora_package_function;

create table tbl_guid(a int, b raw(16));
insert into tbl_guid values(1, sys_guid());
insert into tbl_guid values(2, sys_guid());

begin
for i in 1..10 loop
insert into tbl_guid values(i, sys_guid());
end loop;
end;
/

select count(distinct(b)), count(*) from tbl_guid;

create or replace function print_guid(guid raw, b raw) return int is
begin
dbms_output.serveroutput(true);
-- perform dbms_output.put_line('print guid:' || guid || ' - ' || b);
dbms_output.put_line('print guid');
return 1;
end;
/
create or replace function print_guid1(guid raw) return int is
begin
dbms_output.serveroutput(true);
-- perform dbms_output.put_line('print guid:' || guid);
dbms_output.put_line('print guid1');
return 1;
end;
/
