-- int data type --
create table list_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by list(citycode)
(
	partition list_pt_p1 values (1001, 1002),
	partition list_pt_p2 values (1003, 1004)
);

insert into list_pt values(1, 11, 'name5', 30, 1001);
insert into list_pt values(1, 13, 'name1', 30, 1002);
insert into list_pt values(1, 13, 'name1', 30, 1001);
insert into list_pt values(1, 14, 'name2', 31, 1003);
insert into list_pt values(1, 13, 'name3', 32, 1001);
insert into list_pt values(1, 16, 'name4', 33, 1003);
insert into list_pt values(1, 11, 'name5', 30, 1002);
insert into list_pt values(1, 13, 'name1', 30, 1004);
insert into list_pt values(1, 13, 'name1', 30, 1004);
insert into list_pt values(1, 14, 'name2', 31, 1003);

select count(*) from list_pt;
select count(*) from list_pt_p1;
select count(*) from list_pt_p2;

delete from list_pt;
select count(*) from list_pt;

drop table list_pt;

-- varchar data type --
create table list_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by list(name)
(
	partition list_pt_p1 values ('1001', '1002'),
	partition list_pt_p2 values ('1003', '1004')
);

insert into list_pt values(1, 11, '1001', 30, 40);
insert into list_pt values(1, 13, '1002', 30, 52);
insert into list_pt values(1, 13, '1001', 30, 36);
insert into list_pt values(1, 14, '1003', 31, 36);
insert into list_pt values(1, 13, '1001', 32, 32);
insert into list_pt values(1, 16, '1003', 33, 45);
insert into list_pt values(1, 11, '1002', 30, 23);
insert into list_pt values(1, 13, '1004', 30, 89);
insert into list_pt values(1, 13, '1004', 30, 10);
insert into list_pt values(1, 14, '1003', 31, 73);

select count(*) from list_pt;
select count(*) from list_pt_p1;
select count(*) from list_pt_p2;

delete from list_pt;
select count(*) from list_pt;

drop table list_pt;

-- overlapping data range --
create table list_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by list(citycode)
(
	partition list_pt_p1 values (1001, 1002),
	partition list_pt_p2 values (1002, 1004)
);

-- delete/truncate from child partition table --
create table list_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by list(citycode)
(
	partition list_pt_p1 values (1001, 1002),
	partition list_pt_p2 values (1003, 1004)
);

insert into list_pt values(1, 11, 'name5', 30, 1001);
insert into list_pt values(1, 13, 'name1', 30, 1002);
insert into list_pt values(1, 13, 'name1', 30, 1001);
insert into list_pt values(1, 14, 'name2', 31, 1003);
insert into list_pt values(1, 13, 'name3', 32, 1001);
insert into list_pt values(1, 16, 'name4', 33, 1003);
insert into list_pt values(1, 11, 'name5', 30, 1002);
insert into list_pt values(1, 13, 'name1', 30, 1004);
insert into list_pt values(1, 13, 'name1', 30, 1004);
insert into list_pt values(1, 14, 'name2', 31, 1003);

-- delete --
delete from list_pt_p1;
select count(*) from list_pt_p1;
select count(*) from list_pt;

delete from list_pt_p2;
select count(*) from list_pt_p2;
select count(*) from list_pt;

insert into list_pt values(1, 11, 'name5', 30, 1001);
insert into list_pt values(1, 13, 'name1', 30, 1002);
insert into list_pt values(1, 14, 'name2', 31, 1003);
insert into list_pt values(1, 13, 'name1', 30, 1004);

-- truncate --
truncate table list_pt_p1;
select count(*) from list_pt_p1;
select count(*) from list_pt;

truncate table list_pt_p2;
select count(*) from list_pt_p2;
select count(*) from list_pt;

drop table list_pt;

-- update child partition table in case some data will be moved to other table --
create table list_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by list(citycode)
(
	partition list_pt_p1 values (1001, 1002),
	partition list_pt_p2 values (1003, 1004)
);

insert into list_pt values(1, 11, 'name5', 30, 1001);
insert into list_pt values(1, 13, 'name1', 30, 1002);
insert into list_pt values(1, 14, 'name2', 31, 1003);
insert into list_pt values(1, 13, 'name1', 30, 1004);

-- move data in table list_pt_p1 to table list_pt_p2 --
update list_pt set age = 1004 where age = 1001 or age = 1002;

select count(*) from list_pt_p1;
select count(*) from list_pt_p2;

drop table list_pt;

-- add/drop partition --
create table list_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by list(citycode)
(
	partition list_pt_p1 values (1001, 1002),
	partition list_pt_p2 values (1003, 1004)
);

insert into list_pt values(1, 11, 'name5', 30, 1001);
insert into list_pt values(1, 13, 'name1', 30, 1002);
insert into list_pt values(1, 14, 'name2', 31, 1003);
insert into list_pt values(1, 13, 'name1', 30, 1004);

-- add partition --
create table list_pt_p3 partition of list_pt for values in (1005, 1006);

insert into list_pt values(1, 13, 'name1', 80, 1006);
insert into list_pt values(1, 14, 'name2', 99, 1005);

select count(*) from list_pt;
select count(*) from list_pt_p3;

-- drop child partition table --
drop table list_pt_p1;

select count(*) from list_pt;

-- DDL --
alter table list_pt add tall int;
insert into list_pt values(1, 13, 'name3', 20, 1003, 22);
insert into list_pt values(1, 16, 'name4', 39, 1005, 23);
select count(tall) from list_pt;

alter table list_pt_p2 add wrg int;

drop table list_pt;


































