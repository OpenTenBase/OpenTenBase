-- int data type --
create table range_pt_int
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_int_p1 values less than (10),
	partition range_pt_int_p2 values less than (20),
	partition range_pt_int_p3 values less than (40),
	partition range_pt_int_p4 values less than (80),
	partition range_pt_int_p5 values less than (100)
);

insert into range_pt_int values(1, 11, 'name5', 0, 30);
insert into range_pt_int values(1, 13, 'name1', 9, 30);
insert into range_pt_int values(1, 13, 'name1', 10, 30);
insert into range_pt_int values(1, 14, 'name2', 19, 31);
insert into range_pt_int values(1, 13, 'name3', 20, 32);
insert into range_pt_int values(1, 16, 'name4', 39, 33);
insert into range_pt_int values(1, 11, 'name5', 40, 30);
insert into range_pt_int values(1, 13, 'name1', 79, 30);
insert into range_pt_int values(1, 13, 'name1', 80, 30);
insert into range_pt_int values(1, 14, 'name2', 99, 31);

select count(*) from range_pt_int;
select count(*) from range_pt_int_p1;
select count(*) from range_pt_int_p2;
select count(*) from range_pt_int_p3;
select count(*) from range_pt_int_p4;
select count(*) from range_pt_int_p5;

delete from range_pt_int;
select count(*) from range_pt_int;

drop table range_pt_int;

-- create child partition table in alter clause --
create table range_pt_int
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age);
ALTER TABLE range_pt_int ADD PARTITION range_pt_int_p0 values less than (10);
ALTER TABLE range_pt_int ADD PARTITION range_pt_int_p2 values less than (11);
ALTER TABLE range_pt_int ADD PARTITION range_pt_int_p3 values less than (12);
ALTER TABLE range_pt_int ADD PARTITION range_pt_int_p4 values less than (10);

drop table range_pt_int;

create table range_pt_int
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_int_p1 values less than (10)
);
ALTER TABLE range_pt_int ADD PARTITION range_pt_int_p0 values less than (10);
ALTER TABLE range_pt_int ADD PARTITION range_pt_int_p2 values less than (11);
ALTER TABLE range_pt_int ADD PARTITION range_pt_int_p3 values less than (12);
ALTER TABLE range_pt_int ADD PARTITION range_pt_int_p4 values less than (10);

insert into range_pt_int values(1, 11, 'name5', 0, 9);
insert into range_pt_int values(1, 13, 'name1', 9, 10);
insert into range_pt_int values(1, 13, 'name3', 10, 11);
insert into range_pt_int values(1, 13, 'name3', 10, 12);

select count(*) from range_pt_int;
select count(*) from range_pt_int_p1;
select count(*) from range_pt_int_p2;
select count(*) from range_pt_int_p3;

drop table range_pt_int;

-- varchar data type --
create table range_pt_varchar
(id int, class int, name varchar(20), age int, citycode int)
partition by range(name)
(
	partition range_pt_varchar_p1 values less than ('name2'),
	partition range_pt_varchar_p2 values less than ('name3'),
	partition range_pt_varchar_p3 values less than ('name4'),
	partition range_pt_varchar_p4 values less than ('name5'),
	partition range_pt_varchar_p5 values less than ('name6')
);

insert into range_pt_varchar values(1, 11, 'name5', 0, 30);
insert into range_pt_varchar values(1, 13, 'name1', 9, 30);
insert into range_pt_varchar values(1, 13, 'name1', 10, 30);
insert into range_pt_varchar values(1, 14, 'name2', 19, 31);
insert into range_pt_varchar values(1, 13, 'name3', 20, 32);
insert into range_pt_varchar values(1, 16, 'name4', 39, 33);
insert into range_pt_varchar values(1, 11, 'name5', 40, 30);
insert into range_pt_varchar values(1, 13, 'name1', 79, 30);
insert into range_pt_varchar values(1, 13, 'name1', 80, 30);
insert into range_pt_varchar values(1, 14, 'name2', 99, 31);

select count(*) from range_pt_varchar;
select count(*) from range_pt_varchar_p1;
select count(*) from range_pt_varchar_p2;
select count(*) from range_pt_varchar_p3;
select count(*) from range_pt_varchar_p4;
select count(*) from range_pt_varchar_p5;

delete from range_pt_varchar;
select count(*) from range_pt_varchar;

drop table range_pt_varchar;

-- create child partition table in alter clause --
create table range_pt_varchar
(id int, class int, name varchar(20), age int, citycode int)
partition by range(name);
ALTER TABLE range_pt_varchar ADD PARTITION range_pt_varchar_p0 values less than ('name1');
ALTER TABLE range_pt_varchar ADD PARTITION range_pt_varchar_p2 values less than ('name2');
ALTER TABLE range_pt_varchar ADD PARTITION range_pt_varchar_p3 values less than ('name3');
ALTER TABLE range_pt_varchar ADD PARTITION range_pt_varchar_p4 values less than ('name2');

drop table range_pt_varchar;
create table range_pt_varchar
(id int, class int, name varchar(20), age int, citycode int)
partition by range(name)
(
	partition range_pt_varchar_p1 values less than ('name1')
);
ALTER TABLE range_pt_varchar ADD PARTITION range_pt_varchar_p0 values less than ('name1');
ALTER TABLE range_pt_varchar ADD PARTITION range_pt_varchar_p2 values less than ('name2');
ALTER TABLE range_pt_varchar ADD PARTITION range_pt_varchar_p3 values less than ('name3');
ALTER TABLE range_pt_varchar ADD PARTITION range_pt_varchar_p4 values less than ('name2');

insert into range_pt_varchar values(1, 11, 'name1', 0, 9);
insert into range_pt_varchar values(1, 13, 'name2', 9, 10);
insert into range_pt_varchar values(1, 13, 'name1', 10, 12);

select count(*) from range_pt_varchar;
select count(*) from range_pt_varchar_p1;
select count(*) from range_pt_varchar_p2;
select count(*) from range_pt_varchar_p3;

drop table range_pt_varchar;

-- overlapping data range --
create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_p1 values less than (10),
	partition range_pt_p2 values less than (20),
	partition range_pt_p3 values less than (15)
);

-- delete/truncate from child partition table --
create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_p1 values less than (10),
	partition range_pt_p2 values less than (20),
	partition range_pt_p3 values less than (40)
);

insert into range_pt values(1, 11, 'name5', 0, 30);
insert into range_pt values(1, 13, 'name1', 9, 30);
insert into range_pt values(1, 13, 'name1', 10, 30);
insert into range_pt values(1, 14, 'name2', 19, 31);
insert into range_pt values(1, 13, 'name3', 20, 32);
insert into range_pt values(1, 16, 'name4', 39, 33);

-- delete --
delete from range_pt_p1;
select count(*) from range_pt_p1;
select count(*) from range_pt;

delete from range_pt_p2;
select count(*) from range_pt_p2;
select count(*) from range_pt;

delete from range_pt_p3;
select count(*) from range_pt_p3;
select count(*) from range_pt;

insert into range_pt values(1, 11, 'name5', 0, 30);
insert into range_pt values(1, 13, 'name1', 9, 30);
insert into range_pt values(1, 13, 'name1', 10, 30);
insert into range_pt values(1, 14, 'name2', 19, 31);
insert into range_pt values(1, 13, 'name3', 20, 32);
insert into range_pt values(1, 16, 'name4', 39, 33);

-- truncate --
truncate table range_pt_p1;
select count(*) from range_pt_p1;
select count(*) from range_pt;

truncate table range_pt_p2;
select count(*) from range_pt_p2;
select count(*) from range_pt;

truncate table range_pt_p3;
select count(*) from range_pt_p3;
select count(*) from range_pt;

drop table range_pt;

-- update child partition table in case some data will be moved to other table --
create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_p1 values less than (10),
	partition range_pt_p2 values less than (20),
	partition range_pt_p3 values less than (40)
);

insert into range_pt values(1, 11, 'name5', 0, 30);
insert into range_pt values(1, 13, 'name1', 9, 30);
insert into range_pt values(1, 13, 'name1', 10, 30);
insert into range_pt values(1, 14, 'name2', 19, 31);
insert into range_pt values(1, 13, 'name3', 20, 32);
insert into range_pt values(1, 16, 'name4', 39, 33);

-- move data in table range_pt_p1 and range_pt_p2 to table range_pt_p3 --
update range_pt set age = 35 where age < 20;

select count(*) from range_pt_p1;
select count(*) from range_pt_p2;
select count(*) from range_pt_p3;

drop table range_pt;

-- add/drop partition with consecutive value range --
create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_p1 values less than (10),
	partition range_pt_p2 values less than (20),
	partition range_pt_p3 values less than (40)
);

ALTER TABLE range_pt ADD PARTITION range_pt_p4 values less than (60);
ALTER TABLE range_pt ADD PARTITION range_pt_p5 values less than (80);

insert into range_pt values(1, 11, 'name5', 0, 30);
insert into range_pt values(1, 13, 'name1', 9, 30);
insert into range_pt values(1, 13, 'name1', 10, 30);
insert into range_pt values(1, 14, 'name2', 19, 31);
insert into range_pt values(1, 13, 'name3', 20, 32);
insert into range_pt values(1, 16, 'name4', 39, 33);
insert into range_pt values(1, 13, 'name3', 20, 45);
insert into range_pt values(1, 16, 'name4', 39, 79);

drop table range_pt_p4;
drop table range_pt_p5;

-- add consecutive value range --
create table range_pt_p4 partition of range_pt for values from (80) to (100);

insert into range_pt values(1, 13, 'name1', 80, 30);
insert into range_pt values(1, 14, 'name2', 99, 31);

select count(*) from range_pt;
select count(*) from range_pt_p4;

-- drop child partition table --
drop table range_pt_p1;

select count(*) from range_pt;

-- DDL --
ALTER TABLE range_pt ADD PARTITION range_pt_p6 values less than (200);
alter table range_pt add tall int;
insert into range_pt values(1, 13, 'name3', 20, 32, 22);
insert into range_pt values(1, 16, 'name4', 39, 33, 23);
insert into range_pt values(1, 16, 'name4', 39, 33, 199);
select count(tall) from range_pt;

alter table range_pt_p2 add wrg int;

drop table range_pt;

-- add extra data --
create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age);

ALTER TABLE range_pt ADD PARTITION range_pt_p1 values less than (1, 2);
ALTER TABLE range_pt ADD PARTITION range_pt_p2 values less than ('1');
ALTER TABLE range_pt ADD PARTITION range_pt_p3 values less than ('s');

drop table range_pt;

create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age, name);

ALTER TABLE range_pt ADD PARTITION range_pt_p1 values less than (1, 2);
ALTER TABLE range_pt ADD PARTITION range_pt_p2 values less than (1, '1');
ALTER TABLE range_pt ADD PARTITION range_pt_p3 values less than ('s', 2);
ALTER TABLE range_pt ADD PARTITION range_pt_p4 values less than ('3', 's');

drop table range_pt;
