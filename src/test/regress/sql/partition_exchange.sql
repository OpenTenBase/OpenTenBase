-- avoid seqscan affection --
SET SESSION enable_seqscan = false;

-- test relmap: check if the filenode of relmap in pg_class is the same before and after --
CREATE OR REPLACE FUNCTION test_relmap(t TEXT, t1 TEXT, t2 TEXT, including_indexes BOOLEAN DEFAULT TRUE)
RETURNS TEXT
AS $$
DECLARE
	filenode_t1_old OID;
	filenode_t2_old OID;
	filenode_t1_new OID;
	filenode_t2_new OID;
	mysql TEXT;
BEGIN
	BEGIN
		SELECT relfilenode INTO filenode_t1_old from pg_class where relname = t1;
		SELECT relfilenode INTO filenode_t2_old from pg_class where relname = t2;

		mysql := 'ALTER TABLE '
			|| t
			|| ' EXCHANGE PARTITION '
			|| t1
			|| ' WITH TABLE '
			|| t2;
		IF including_indexes THEN
			mysql := mysql || ' including indexes';
		END IF;
		mysql := mysql || ';';

		EXECUTE mysql;

		SELECT relfilenode INTO filenode_t1_new from pg_class where relname = t1;
		SELECT relfilenode INTO filenode_t2_new from pg_class where relname = t2;
		IF filenode_t1_old = filenode_t2_new THEN
			RETURN 'equal';
		ELSE
			RETURN 'non-equal';
		END IF;
	COMMIT;
	END;
EXCEPTION WHEN OTHERS THEN
	RETURN 'error';
END;
$$ LANGUAGE plpgsql;

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

SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');

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

SELECT test_relmap('list_pt', 'list_pt_p1', 'list_pt_ex');

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

SELECT test_relmap('list_pt', 'range_pt_p1', 'range_pt_ex');
SELECT test_relmap('range_pt', 'range_pt_p4', 'range_pt_ex');
SELECT test_relmap('range_pt', 'range_pt_p1', 'list_pt_ex');

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

SELECT test_relmap('range_pt', 'range_pt_p1_p3', 'range_pt_ex');

select count(*) from range_pt;
select count(*) from range_pt_ex;
select count(*) from range_pt_p1_p3;

SELECT test_relmap('range_pt', 'range_pt_ex', 'range_pt_p1_p3');

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

SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex_int');
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex_char');
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex_varchar_30');
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex_name');

drop table range_pt_ex_int;
drop table range_pt_ex_char;
drop table range_pt_ex_varchar_30;
drop table range_pt_ex_name;

-- exchange partition table with same structure but wrong order (it should be error) --
create table range_pt_ex (class int, citycode int, id int, age int, name varchar(20));
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');

drop table range_pt;
drop table range_pt_ex;

-- if there are some different constraints bewteen child table and ex table, --
-- the exchange statement should be error --

-- 1. check constraint --
create table range_pt
(id int, age int)
partition by range(age)
(
	partition range_pt_p1 values less than (10)
);
create table range_pt_ex (id int, age int);

-- one have check constraint, but the another does not --
alter table range_pt_p1 add constraint ck check(id > 1);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');
-- different value --
alter table range_pt_ex add constraint ck check(id > 2);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');
-- different constraint's name --
alter table range_pt_ex drop constraint ck;
alter table range_pt_ex add constraint ckck check(id > 1);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');
-- different column --
alter table range_pt_ex drop constraint ckck;
alter table range_pt_ex add constraint ck check(age > 1);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');
-- correct --
alter table range_pt_ex drop constraint ck;
alter table range_pt_ex add constraint ck check(id > 1);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');

alter table range_pt_p1 drop constraint ck;
alter table range_pt_ex drop constraint ck;

-- 2. unique key constraint --
-- one have unique key constraint, but the another does not --
alter table range_pt_p1 add constraint uk unique (id);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');
-- correct --
alter table range_pt_ex add constraint ukuk unique (id);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');

-- 3. foreign key constraint --
create table t (id int not null primary key, age int);
-- one have foreign key constraint, but the another does not --
alter table range_pt_ex add constraint fk
FOREIGN KEY (id)
REFERENCES t (id);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');
-- different action --
alter table range_pt_p1 add constraint fk
FOREIGN KEY (id)
REFERENCES t (id)
ON DELETE SET NULL ON UPDATE CASCADE;
alter table range_pt_ex drop constraint fk;
alter table range_pt_ex add constraint fk
FOREIGN KEY (id)
REFERENCES t (id)
ON DELETE SET NULL ON UPDATE NO ACTION;
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');
-- correct --
alter table range_pt_ex drop constraint fk;
alter table range_pt_ex add constraint fk
FOREIGN KEY (id)
REFERENCES t (id)
ON DELETE SET NULL ON UPDATE CASCADE;
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');

alter table range_pt_ex drop constraint fk;
alter table range_pt_p1 drop constraint fk;
drop table t;

-- 4. primary key constraint --
-- one have primary key constraint, but the another does not --
alter table range_pt_p1 add constraint pk primary key(id);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');
-- correct --
alter table range_pt_ex add constraint pkpk primary key(id);
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex');

alter table range_pt_ex drop constraint pkpk;
alter table range_pt_p1 drop constraint pk;

drop table range_pt;
drop table range_pt_ex;

-- if exchanging data including index, the index must be reindexed --
create table range_pt
(id int, age int)
partition by range(age)
(
	partition range_pt_p1 values less than (10)
);
create table range_pt_ex (id int, age int);
create index key_ex on range_pt_ex (age);
create index key_p1 on range_pt_p1 (age);

insert into range_pt_p1 values(1, 1);
insert into range_pt_p1 values(2, 2);
insert into range_pt_p1 values(3, 3);
insert into range_pt_ex values(4, 4);

-- exchange data without index --
SELECT test_relmap('range_pt', 'range_pt_p1', 'range_pt_ex', false);
-- it should be 3 when correct, but 1 --
select count(*) from range_pt_ex where age >= 0;

drop table range_pt;
drop table range_pt_ex;

DROP FUNCTION IF EXISTS test_relmap;
