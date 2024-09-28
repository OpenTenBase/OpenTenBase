-- exchange range --
create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_p1 values less than (10),
	partition range_pt_p2 values less than (20),
	partition range_pt_p3 values less than (40)
);
create table range_pt_ex (id int, class int, name varchar(20), age int, citycode int);

insert into range_pt values(1, 13, 'name1', 0, 30);
insert into range_pt values(1, 13, 'name1', 10, 30);
insert into range_pt values(1, 14, 'name2', 19, 31);
insert into range_pt values(1, 13, 'name1', 29, 30);
insert into range_pt_ex values(1, 13, 'name1', 69, 30);
insert into range_pt_ex values(1, 13, 'name1', 49, 30);

ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p1 WITH TABLE range_pt_ex;

select count(*) from range_pt;
select count(*) from range_pt_ex;
select count(*) from range_pt_p2;
select count(*) from range_pt_p3;
select count(*) from range_pt_p1;

drop table range_pt;
drop table range_pt_ex;

-- exchange list --
create table list_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by list(citycode)
(
	partition list_pt_p1 values (1001, 1002),
	partition list_pt_p2 values (1003, 1004)
);
create table list_pt_ex (id int, class int, name varchar(20), age int, citycode int);

insert into list_pt values(1, 11, 'name5', 30, 1001);
insert into list_pt values(1, 13, 'name1', 30, 1002);
insert into list_pt values(1, 13, 'name1', 30, 1004);
insert into list_pt values(1, 14, 'name2', 31, 1003);
insert into list_pt_ex values(1, 14, 'name2', 31, 1008);

ALTER TABLE list_pt EXCHANGE PARTITION list_pt_p1 WITH TABLE list_pt_ex;

select count(*) from list_pt;
select count(*) from list_pt_ex;
select count(*) from list_pt_p2;
select count(*) from list_pt_p1;

drop table list_pt;
drop table list_pt_ex;

-- table does not exist --
create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_p1 values less than (10),
	partition range_pt_p2 values less than (20),
	partition range_pt_p3 values less than (40)
);
create table range_pt_ex (id int, class int, name varchar(20), age int, citycode int);
create table list_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by list(citycode)
(
	partition list_pt_p1 values (1001, 1002),
	partition list_pt_p2 values (1003, 1004)
);
create table list_pt_ex (id int, class int, name varchar(20), age int, citycode int);

ALTER TABLE list_pt EXCHANGE PARTITION range_pt_p1 WITH TABLE range_pt_ex;
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p4 WITH TABLE range_pt_ex;
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p1 WITH TABLE list_pt_ex;

-- 1. child_rel must be partition of parent_rel and not be partition of ex_rel. --
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_ex WITH TABLE range_pt_ex;
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p2 WITH TABLE range_pt;

-- 2. ex_rel must be not partition of parent_rel nad child_rel. --
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p2 WITH TABLE range_pt_p3;
ALTER TABLE range_pt EXCHANGE PARTITION range_pt WITH TABLE range_pt_p3;

-- 3. parent_rel must be not partition of ex_rel nad child_rel. --
ALTER TABLE range_pt_p2 EXCHANGE PARTITION range_pt WITH TABLE range_pt_ex;
ALTER TABLE range_pt_p2 EXCHANGE PARTITION range_pt_p1 WITH TABLE range_pt;

-- 4. ex_rel must be non-partition table. --
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p1 WITH TABLE list_pt;

ALTER TABLE range_pt EXCHANGE PARTITION list_pt WITH TABLE range_pt_ex;
ALTER TABLE range_pt EXCHANGE PARTITION list_pt_p1 WITH TABLE range_pt_ex;
ALTER TABLE range_pt EXCHANGE PARTITION list_pt_ex WITH TABLE range_pt_ex;

-- list_pt_p1 is not partition table, so it should be pass --
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_ex WITH TABLE list_pt_p1;

drop table range_pt;
drop table range_pt_ex;
drop table list_pt;
drop table list_pt_ex;

-- nested partition table --
create table range_pt_p1
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_p1_p1 values less than (10),
	partition range_pt_p1_p2 values less than (20),
	partition range_pt_p1_p3 values less than (40)
);
create table range_pt_p2
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age);
create table range_pt_p2_p1 partition of range_pt_p2 for values from (40) to (60);
create table range_pt_p2_p2 partition of range_pt_p2 for values from (60) to (80);

create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age);

create table range_pt_ex (id int, class int, name varchar(20), age int, citycode int);

ALTER TABLE range_pt ATTACH PARTITION range_pt_p1 FOR VALUES FROM (MINVALUE) TO (40);
ALTER TABLE range_pt ATTACH PARTITION range_pt_p2 FOR VALUES FROM (40) TO (80);

insert into range_pt values(1, 13, 'name1', 39, 30);
insert into range_pt_ex values(1, 13, 'name1', 69, 30);
insert into range_pt_ex values(1, 13, 'name1', 49, 30);

ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p1_p3 WITH TABLE range_pt_ex;

select count(*) from range_pt;
select count(*) from range_pt_ex;
select count(*) from range_pt_p1_p3;

ALTER TABLE range_pt EXCHANGE PARTITION range_pt_ex WITH TABLE range_pt_p1_p3;

select count(*) from range_pt;
select count(*) from range_pt_p1_p3;
select count(*) from range_pt_ex;

drop table range_pt;
drop table range_pt_ex;

-- exchange partition table without same structure --
create table range_pt
(id int, class int, name varchar(20), age int, citycode int)
partition by range(age)
(
	partition range_pt_p1 values less than (10),
	partition range_pt_p2 values less than (20),
	partition range_pt_p3 values less than (40)
);
create table range_pt_ex_int (age int, citycode int);
create table range_pt_ex_char (id int, class int, name varchar(20), age char, citycode int);
create table range_pt_ex_varchar_30 (id int, class int, name varchar(30), age char, citycode int);
create table range_pt_ex_name (id int, class int, name varchar(20), age_other char, citycode int);

ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p1 WITH TABLE range_pt_ex_int;
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p1 WITH TABLE range_pt_ex_char;
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p1 WITH TABLE range_pt_ex_varchar_30;
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p1 WITH TABLE range_pt_ex_name;

drop table range_pt_ex_int;
drop table range_pt_ex_char;
drop table range_pt_ex_varchar_30;
drop table range_pt_ex_name;

-- exchange partition table with same structure but wrong order --
create table range_pt_ex (class int, citycode int, id int, age int, name varchar(20));
ALTER TABLE range_pt EXCHANGE PARTITION range_pt_p1 WITH TABLE range_pt_ex;

insert into range_pt_ex values(1, 13, 'name1', 2, 30);
insert into range_pt_ex values(1, 13, 'name1', 9, 30);
insert into range_pt_p1 values(1, 13, 29, 30, 'name1');

select count(*) from range_pt_p1;
select count(*) from range_pt;
select count(*) from range_pt_ex;

ALTER TABLE range_pt EXCHANGE PARTITION range_pt_ex WITH TABLE range_pt_p1;

drop table range_pt;
drop table range_pt_ex;
