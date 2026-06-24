\c regression_ora

set enable_seqscan to off;

--case1, normal table
drop table if exists const_test;
create table const_test (id int,a int);
insert into const_test values(1,2);
alter table const_test add constraint aaaa check(id < 10);
--detail:  failing row contains (11, 2).
insert into const_test values(11,2);
alter table const_test disable novalidate constraint aaaa;
insert into const_test values(11,2);
--error:  check constraint "aaaa" is violated by some row
alter table const_test disable validate constraint aaaa;
delete from const_test where id >= 10;
-- success
alter table const_test disable validate constraint aaaa;
--error:  no insert/update/delete on table with constraint "aaaa" disabled and validated
insert into const_test values(11,2);
--error:  no insert/update/delete on table with constraint "aaaa" disabled and validated
update const_test set a = a +1;
--error:  no insert/update/delete on table with constraint "aaaa" disabled and validated
delete from const_test;
alter table const_test enable novalidate constraint aaaa;
--detail:  failing row contains (11, 2).
insert into const_test values(11,2);
alter table const_test enable validate constraint aaaa;
--detail:  failing row contains (11, 2).
insert into const_test values(11,2);

-- index , normal table
drop table const_test;
create table const_test (id int,a int);
insert into const_test values(1,2);
alter table const_test add constraint aaaa unique(id);
--detail:  key (id)=(1) already exists.
insert into const_test values(1,3);
alter table const_test disable novalidate constraint aaaa;
insert into const_test values(1,3);
--seq scan
explain (costs off, nodes off) select * from const_test where id = 10;
--detail:  key (id)=(1) is duplicated.
alter table const_test disable validate constraint aaaa;
--detail:  key (id)=(1) is duplicated.
alter table const_test enable novalidate constraint aaaa;
--detail:  key (id)=(1) is duplicated
alter table const_test enable validate constraint aaaa;
delete from const_test where a = 3;
alter table const_test disable validate constraint aaaa;
--seq scan
explain (costs off, nodes off) select * from const_test where id = 10;
alter table const_test enable novalidate constraint aaaa;
--index scan
explain (costs off, nodes off) select * from const_test where id = 10;
alter table const_test enable validate constraint aaaa;
--detail:  key (id)=(1) already exists.
insert into const_test values(1,3);
--index scan
explain (costs off, nodes off) select * from const_test where id = 10;

-- partition table
drop table const_test cascade;
create table const_test (
id integer,
join_date date,
a int
) partition by range (join_date);
create table const_test_201211 partition of const_test
for values from ('2012-11-01') to ('2012-12-01');
create table const_test_201212 partition of const_test
for values from ('2012-12-01') to ('2013-01-01');
insert into const_test values(1,'2012-11-10'::date);
alter table const_test add constraint aaaa check(id < 10);
--detail:  failing row contains (11, 2012-11-10 00:00:00, null).
insert into const_test values(11,'2012-11-10'::date);
alter table const_test disable novalidate constraint aaaa;
insert into const_test values(11,'2012-11-10'::date);
--ERROR:  check constraint "AAAA" is violated by some row
alter table const_test disable validate constraint aaaa;
delete from const_test where id >= 10;
-- success
alter table const_test disable validate constraint aaaa;
--error:  no insert/update/delete on table with constraint "aaaa" disabled and validated
insert into const_test values(11,'2012-11-10'::date);
--error:  no insert/update/delete on table with constraint "aaaa" disabled and validated
update const_test set a = a +1;
--error:  no insert/update/delete on table with constraint "aaaa" disabled and validated
delete from const_test;

alter table const_test enable novalidate constraint aaaa;
--detail:  failing row contains (11, 2).
insert into const_test values(11,'2012-11-10'::date);
alter table const_test enable validate constraint aaaa;
--detail:  failing row contains (11, 2).
insert into const_test values(11,'2012-11-10'::date);

--inherits
drop table const_test cascade;
create table const_test (id integer,join_date date,a int);
create table const_test_201211 (
check ( join_date >= date '2012-11-01' and join_date < date '2012-12-01' )
) inherits (const_test);
create table const_test_201212 (
check ( join_date >= date '2012-12-01' and join_date < date '2013-01-01' )
) inherits (const_test);
insert into const_test values(11,'2012-11-10'::date);
alter table const_test add constraint aaaa unique(id);
--detail:  key (id)=(11) already exists.
insert into const_test values(11,'2012-12-10'::date);
alter table const_test disable novalidate constraint aaaa;
insert into const_test values(11,'2012-12-10'::date,3);
-- seqscan
explain (costs off, nodes off) select * from const_test where id = 10;
--detail:  key (id)=(11) is duplicated.
alter table const_test disable validate constraint aaaa;
--detail:  key (id)=(11) is duplicated.
alter table const_test enable novalidate constraint aaaa;
--detail:  key (id)=(11) is duplicated.
alter table const_test enable validate constraint aaaa;
delete from const_test where a = 3;
alter table const_test disable validate constraint aaaa;
--error:  no insert/update/delete on table with constraint "aaaa" disabled and validated
insert into const_test values(11,'2012-12-10'::date,3);
--error:  no insert/update/delete on table with constraint "aaaa" disabled and validated
delete from const_test where a = 3;
-- seqscan
explain (costs off, nodes off) select * from const_test where id = 10;
alter table const_test enable novalidate constraint aaaa;
--detail:  key (id)=(11) already exists.
insert into const_test values(11,'2012-12-10'::date,3);
-- index scan
explain (costs off, nodes off) select * from const_test where id = 10;
alter table const_test enable validate constraint aaaa;
--detail:  key (id)=(11) already exists.
insert into const_test values(11,'2012-12-10'::date);
-- index scan
explain (costs off, nodes off) select * from const_test where id = 10;
drop table const_test cascade;
reset enable_seqscan;

-- foreign key
drop table if exists A_0905_2;
drop table if exists A_0905_1;
create table A_0905_1(col1 char(13) primary key, col2 char(14) unique) distribute by replication;
create table A_0905_2(col1 char(13) references A_0905_1(col1), col2 char(13), col3 char(13)) distribute by shard(col1);
insert into A_0905_2 values(1, 2, 3);
alter table A_0905_2 disable constraint A_0905_2_COL1_FKEY;
insert into A_0905_2 values(1, 2, 3);
alter table A_0905_2 enable constraint A_0905_2_COL1_FKEY;
delete from A_0905_2;
alter table A_0905_2 enable constraint A_0905_2_COL1_FKEY;
insert into A_0905_2 values(1, 2, 3);
drop table A_0905_2;
drop table A_0905_1;

-- foreign key for normal table
drop table if exists tb_product;
drop table if exists tb_category;
create table tb_category(id int primary key, name varchar2(50)) distribute by shard(id);
create table tb_product(id int, price number(4,1), cid int) distribute by shard(id);
--add foreign key
alter table tb_product add constraint product_fk foreign key (cid) references tb_category (id);
insert into tb_category values(1, 'abc');
insert into tb_category values(11, 'abc');
insert into tb_product values(1, 110.5, '1');
insert into tb_product values(1, 110.5, '11');
insert into tb_product values(11,10,'11');
delete from tb_category where id=1;
--disable constraint
alter table tb_product disable constraint product_fk;
insert into tb_product values(10,null,3);
select * from tb_product order by 1,2,3;
--Enable validate constraint
alter table tb_product Enable validate constraint product_fk;
delete from tb_product where cid=3;
alter table tb_product Enable validate constraint product_fk;
select * from tb_product order by 1,2,3;
insert into tb_product values(1,1,1);
--error
insert into tb_product values(13,null,3);
select * from tb_product order by 1,2,3;
drop table if exists tb_product;
drop table tb_category;

-- Primary key does not allow duplicates, even with 'novalidate'.
drop table if exists abcd;
create table abcd(
    id int,
    char1 VARCHAR2(64),
    char2 VARCHAR2(64), 
    char3 VARCHAR2(64),
    int1 int,
    int2 int,
    int3 int
);
insert into abcd(id,char1) values(10,'c10');
----add primary key 
ALTER TABLE abcd ADD CONSTRAINT "const1_PK" PRIMARY KEY (id);
delete from abcd where id = 10;
ALTER TABLE abcd ADD CONSTRAINT "const1_PK" PRIMARY KEY (id);
alter table abcd disable constraint "const1_PK";
insert into abcd(id,char1) values(10,'c10');
insert into abcd(id,char1) values(10,'c10');
select * from abcd where id = 10;
delete from abcd where id = 10;
select * from abcd where id = 10;
insert into abcd(id,char1) values(10,'c10');
insert into abcd(id,char1) values(10,'c10');
alter table abcd enable constraint "const1_PK";
-- bug, Primary key does not allow duplicates, even with 'novalidate'.
alter table abcd enable novalidate constraint "const1_PK";
select * from abcd where id = 10;
insert into abcd(id,char1) values(10,'c10');
insert into abcd(id,char1) values(20,'c10');
insert into abcd(id,char1) values(20,'c10');
delete from abcd where id = 10;
select * from abcd where id = 10;
alter table abcd enable constraint "const1_PK";
insert into abcd(id,char1) values(10,'c10');
insert into abcd(id,char1) values(10,'c10');
select * from abcd where id = 10;
drop table abcd;