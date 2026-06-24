--
-- Test constraint default null and not null.
--

drop table if exists constraint_null_test;
drop table if exists constraint_empty_test;

--test null

create table constraint_null_test(f0 int, f1 int not null default null, f2 char(1) , f3 varchar(16), f4 text);
create table constraint_null_test(f0 int, f1 int, f2 char(1) not null default null, f3 varchar(16), f4 text);
create table constraint_null_test(f0 int, f1 int, f2 char(1), f3 varchar(16) not null default null, f4 text);
create table constraint_null_test(f0 int, f1 int, f2 char(1), f3 varchar(16), f4 text not null default null);


create table constraint_null_test(f0 int, f1 int not null default 1, f2 char(1) not null default '1', f3 varchar(16) not null default '123', f4 text not null default '123');
alter table constraint_null_test alter column f1 set default null;
alter table constraint_null_test alter column f2 set default null;
alter table constraint_null_test alter column f3 set default null;
alter table constraint_null_test alter column f4 set default null;

alter table constraint_null_test alter column f1 drop not null;
alter table constraint_null_test alter column f2 drop not null;
alter table constraint_null_test alter column f3 drop not null;
alter table constraint_null_test alter column f4 drop not null;

alter table constraint_null_test alter column f1 set default null;  -- default is empty
alter table constraint_null_test alter column f2 set default null;
alter table constraint_null_test alter column f3 set default null;
alter table constraint_null_test alter column f4 set default null;  -- default is empty

alter table constraint_null_test alter column f1 set not null;
alter table constraint_null_test alter column f2 set not null;
alter table constraint_null_test alter column f3 set not null;
alter table constraint_null_test alter column f4 set not null;

alter table constraint_null_test add column f5 varchar(5) not null default null;

drop table constraint_null_test;

