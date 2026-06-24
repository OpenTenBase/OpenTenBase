--
-- Test constraint default null and not null.
--
\c regression_ora
drop table if exists constraint_null_test;
drop table if exists constraint_empty_test;

--test null

create table constraint_null_test(f0 int, f1 int not null default null, f2 char(1) , f3 varchar(16), f4 text);
drop table constraint_null_test;
create table constraint_null_test(f0 int, f1 int, f2 char(1) not null default null, f3 varchar(16), f4 text);
drop table constraint_null_test;
create table constraint_null_test(f0 int, f1 int, f2 char(1), f3 varchar(16) not null default null, f4 text);
drop table constraint_null_test;
create table constraint_null_test(f0 int, f1 int, f2 char(1), f3 varchar(16), f4 text not null default null);
insert into constraint_null_test values(1,1,'1','1');
drop table constraint_null_test;


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

--test ''

create table constraint_empty_test(f0 int, f1 int not null default '', f2 char(1) , f3 varchar(16), f4 text);
drop table if exists constraint_empty_test;
create table constraint_empty_test(f0 int, f1 int, f2 char(1) not null default '', f3 varchar(16), f4 text);
drop table if exists constraint_empty_test;
create table constraint_empty_test(f0 int, f1 int, f2 char(1), f3 varchar(16) not null default '', f4 text);
drop table if exists constraint_empty_test;
create table constraint_empty_test(f0 int, f1 int, f2 char(1), f3 varchar(16), f4 text not null default '');

drop table if exists constraint_empty_test;
create table constraint_empty_test(f0 int, f1 int not null default 1, f2 char(1) not null default '1', f3 varchar(16) not null default '123', f4 text not null default '123');
alter table constraint_empty_test alter column f1 set default '';
alter table constraint_empty_test alter column f2 set default '';
alter table constraint_empty_test alter column f3 set default '';
alter table constraint_empty_test alter column f4 set default '';

alter table constraint_empty_test alter column f1 drop not null;
alter table constraint_empty_test alter column f2 drop not null;
alter table constraint_empty_test alter column f3 drop not null;
alter table constraint_empty_test alter column f4 drop not null;

alter table constraint_empty_test alter column f1 set default '';  -- default is empty
alter table constraint_empty_test alter column f2 set default '';
alter table constraint_empty_test alter column f3 set default '';
alter table constraint_empty_test alter column f4 set default '';  -- default is empty

alter table constraint_empty_test alter column f1 set not null;
alter table constraint_empty_test alter column f2 set not null;
alter table constraint_empty_test alter column f3 set not null;
alter table constraint_empty_test alter column f4 set not null;

alter table constraint_null_test add column f5 varchar(5) not null default '';
drop table constraint_empty_test;
