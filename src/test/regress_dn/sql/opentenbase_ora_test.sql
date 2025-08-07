-- Bugs:
--  ID83145215 cover by shard, also for modulo
\c regression_ora
drop table if exists list_tb;
create table list_tb(id int,name varchar2(10),age int) partition by list(name) distribute by shard(id);
CREATE TABLE p_list_201901 PARTITION OF list_tb FOR VALUES in ('201901') AS p_list_201901;
CREATE TABLE p_list_201902 PARTITION OF list_tb FOR VALUES in ('201902');
CREATE TABLE p_list_201903 PARTITION OF list_tb FOR VALUES in ('201903');
\d+ list_tb
insert into list_tb values(1, '201901', 12);
insert into list_tb values(2, '201902', 13);
insert into list_tb values(3, '201903', 14);
alter table list_tb merge partitions (p_list_201901, p_list_201902) into partition p_list_201901_201902;
select * from p_list_201901_201902 order by 1;

-- hash
drop table if exists list_tb;
create table list_tb(id int, ts timestamp, name varchar2(10),age int) partition by list(name) distribute by hash(id);
CREATE TABLE p_list_201901 PARTITION OF list_tb FOR VALUES in ('201901') AS p_list_201901;
CREATE TABLE p_list_201902 PARTITION OF list_tb FOR VALUES in ('201902');
CREATE TABLE p_list_201903 PARTITION OF list_tb FOR VALUES in ('201903');
\d+ list_tb
insert into list_tb values(1, '2020-01-01', '201901', 12);
insert into list_tb values(2, '2020-01-01', '201902', 13);
insert into list_tb values(3, '2020-01-01', '201903', 14);
alter table list_tb merge partitions (p_list_201901, p_list_201902) into partition p_list_201901_201902;
\d+ list_tb
select * from p_list_201901_201902 order by 1;

-- replication
drop table if exists list_tb;
create table list_tb(id int, ts timestamp, name varchar2(10),age int) partition by list(name) distribute by replication;
CREATE TABLE p_list_201901 PARTITION OF list_tb FOR VALUES in ('201901') AS p_list_201901;
CREATE TABLE p_list_201902 PARTITION OF list_tb FOR VALUES in ('201902');
CREATE TABLE p_list_201903 PARTITION OF list_tb FOR VALUES in ('201903');
\d+ list_tb
insert into list_tb values(1, '2020-01-01', '201901', 12);
insert into list_tb values(2, '2020-01-01', '201902', 13);
insert into list_tb values(3, '2020-01-01', '201903', 14);
alter table list_tb merge partitions (p_list_201901, p_list_201902) into partition p_list_201901_201902;
\d+ list_tb
select * from p_list_201901_201902 order by 1;

-- rr
drop table if exists list_tb;
create table list_tb(id int, ts timestamp, name varchar2(10),age int) partition by list(name) distribute by roundrobin;
CREATE TABLE p_list_201901 PARTITION OF list_tb FOR VALUES in ('201901') AS p_list_201901;
CREATE TABLE p_list_201902 PARTITION OF list_tb FOR VALUES in ('201902');
CREATE TABLE p_list_201903 PARTITION OF list_tb FOR VALUES in ('201903');
\d+ list_tb
insert into list_tb values(1, '2020-01-01', '201901', 12);
insert into list_tb values(2, '2020-01-01', '201902', 13);
insert into list_tb values(3, '2020-01-01', '201903', 14);
alter table list_tb merge partitions (p_list_201901, p_list_201902) into partition p_list_201901_201902;
\d+ list_tb
select * from p_list_201901_201902 order by 1;

drop table if exists list_tb;
create table list_tb(id int,name varchar2(10),age int) partition by list(name) distribute by shard(id);
CREATE TABLE p_list_201901 PARTITION OF list_tb FOR VALUES in ('201901') AS p_list_201901;
CREATE TABLE p_list_def PARTITION OF list_tb default;
insert into list_tb values(1, '201901', 12);
insert into list_tb values(2, '201902', 13);
insert into list_tb values(3, '201903', 14);
insert into list_tb values(4, '201904', 14);
CREATE TABLE p_list_201902_3 PARTITION OF list_tb FOR VALUES in ('201902', '201903');
select * from p_list_201902_3 order by id;
select * from p_list_def order by id;

drop table if exists list_tb;
create table list_tb(id int,name varchar2(10),age int) partition by list(name) distribute by modulo(id);
CREATE TABLE p_list_201901 PARTITION OF list_tb FOR VALUES in ('201901') AS p_list_201901;
CREATE TABLE p_list_201902 PARTITION OF list_tb FOR VALUES in ('201902');
CREATE TABLE p_list_201903 PARTITION OF list_tb FOR VALUES in ('201903');
\d+ list_tb
insert into list_tb values(1, '201901', 12);
insert into list_tb values(2, '201902', 13);
insert into list_tb values(3, '201903', 14);
alter table list_tb merge partitions (p_list_201901, p_list_201902) into partition p_list_201901_201902;

drop table if exists list_tb;
create table list_tb(id int,name varchar2(10),age int) partition by list(name) distribute by modulo(id);
CREATE TABLE p_list_201901 PARTITION OF list_tb FOR VALUES in ('201901') AS p_list_201901;
CREATE TABLE p_list_def PARTITION OF list_tb default;
insert into list_tb values(1, '201901', 12);
insert into list_tb values(2, '201902', 13);
insert into list_tb values(3, '201903', 14);
insert into list_tb values(4, '201904', 14);
CREATE TABLE p_list_201902_3 PARTITION OF list_tb FOR VALUES in ('201902', '201903');
select * from p_list_201902_3 order by id;
select * from p_list_def order by id;


/* Tuples in default partition should be inserted into newly added partition */
-- add partition
--  without index
--  with index
--  OID/ROWID keep as same
-- list, range, interval parition test

-- add partition
--  without index & OID/ROWID keep as same
drop table if exists order_list;
create table order_list(id bigserial not null,userid integer,product text,area text, createdate date) partition by list( area );
create table order_list_gd partition of order_list(id primary key,userid,product,area,createdate) for values in ('guangdong');
create table order_list_bj partition of order_list(id primary key,userid,product,area,createdate) for values in ('beijing');
create table order_list_default partition of order_list default;
insert into order_list values(1,1,'abc','guangdong','2017-01-01');
insert into order_list values(2,2,'abc','beijing','2017-01-01');
insert into order_list values(3,3,'abc','shenzhen','2017-01-01');
select rowid, * from order_list order by id;
create table order_list_sz partition of order_list(id primary key,userid,product,area,createdate) for values in ('shenzhen');

select rowid, * from order_list order by id;
select rowid, * from order_list_default order by id;
select rowid, * from order_list_sz order by id;

--  with index
drop table if exists order_list;
create table order_list(id bigserial not null,userid integer,product text,area text, createdate date) partition by list( area );
create table order_list_gd partition of order_list(id primary key,userid,product,area,createdate) for values in ('guangdong');
create table order_list_bj partition of order_list(id primary key,userid,product,area,createdate) for values in ('beijing');
create table order_list_default partition of order_list default;
create index order_list_area on order_list(area);
insert into order_list values(1,1,'abc','guangdong','2017-01-01');
insert into order_list values(2,2,'abc','beijing','2017-01-01');
insert into order_list values(3,3,'abc','shenzhen','2017-01-01');
create table order_list_sz partition of order_list(id primary key,userid,product,area,createdate) for values in ('shenzhen');

set enable_seqscan to off;
set enable_bitmapscan to off;
explain(costs off) select * from order_list_sz where area = 'shenzhen';
select * from order_list_sz where area = 'shenzhen';
select * from order_list where area = 'shenzhen';
select * from order_list_default where area = 'shenzhen';

-- list, range, hash, interval parition test
-- list partition is tested above
-- range, rowid
create table test_range(id int, mydate int) 
  partition by range ( mydate );
create table test_range_201801 partition of test_range(id primary key) 
  for values from (10) to (30);
create table test_range_201802 partition of test_range(id primary key) 
  for values from (50) to (60);
create table test_range_default partition of test_range default;
insert into test_range values(1, 20);
insert into test_range values(2, 21);
insert into test_range values(3, 51);
insert into test_range values(4, 52);
insert into test_range values(5, 91);
insert into test_range values(6, 92);
insert into test_range values(7, 102);
insert into test_range values(8, 102);
select rowid, * from test_range order by id;
create table test_range_201890 partition of test_range(id primary key) 
  for values from (90) to (100);
  
select rowid, * from test_range order by id;
select rowid, * from test_range_201890 order by id;
select rowid, * from test_range_default order by id;

-- index scan
explain (costs off) select * from test_range where id = 5;
select * from test_range where id = 5;
select * from test_range_default where id = 5;
select * from test_range_201890 where id = 5;
drop table test_range;

-- interval
create table int_drop(f1 bigint, f2 int, f3 integer) partition by range (f3) begin (1) step (50) partitions (2);
insert into int_drop(f3) values(20);
insert into int_drop(f3) values(20);
insert into int_drop(f3) values(90);
insert into int_drop(f3) values(90);
select rowid, * from int_drop; -- should unique
drop table int_drop;

-- oids
drop table if exists order_list;
create table order_list(id bigserial not null,userid integer,product text,area text, createdate date) partition by list( area )with(oids);
create table order_list_gd partition of order_list(id primary key,userid,product,area,createdate) for values in ('guangdong');
create table order_list_bj partition of order_list(id primary key,userid,product,area,createdate) for values in ('beijing');
create table order_list_default partition of order_list default;
insert into order_list values(1,1,'abc','guangdong','2017-01-01');
insert into order_list values(2,2,'abc','beijing','2017-01-01');
insert into order_list values(3,3,'abc','shenzhen','2017-01-01');
create table order_list_sz partition of order_list(id primary key,userid,product,area,createdate) for values in ('shenzhen');

select * from order_list order by 1;
select * from order_list_default order by 1;
select * from order_list_sz order by 1;

select oid is not null c from order_list;
drop table order_list;

--attach
drop table if exists order_list;
drop table if exists order_list_gd;
create table order_list(id bigserial not null,userid integer,product text,area text, createdate text) partition by list( area );
create table order_list_bj partition of order_list(id primary key,userid,product,area,createdate) for values in ('beijing');
create table order_list_default partition of order_list default;
insert into order_list values(1,1,'abc','guangdong','2017-01-01');
insert into order_list values(2,2,'abc','beijing','2017-01-01');
insert into order_list values(3,3,'abc','shenzhen','2017-01-01');
select * from order_list order by 1;

create table order_list_gd(id bigserial not null,userid integer,product text,area text, createdate text);
alter table order_list attach partition order_list_gd for values in ('guangdong');
select * from order_list order by 1;
select * from order_list_default order by 1;
select * from order_list_gd order by 1;
drop table order_list;
drop table order_list_gd;

drop table list_parted;
create table list_parted (      
     a text,                     
     b int                       
 ) partition by list (lower(a)); 
 create table part_xx_yy partition of list_parted for values in ('xx', 'yy', 'zz') partition by list (a);
create table part_xx_yy_p1 partition of part_xx_yy for values in ('xx');                  
create table part_xx_yy_defpart partition of part_xx_yy default;                          
create table part_default partition of list_parted default partition by range(b);         

insert into list_parted values('xx', 1);
insert into list_parted values('yy', 1);
insert into list_parted values('zz', 1);
select * from part_xx_yy_defpart;
create table part_xx_yy_p2_zz partition of part_xx_yy for values in ('zz');

select * from part_xx_yy_defpart;
select * from part_xx_yy_p2_zz;
drop table list_parted;

-- range/timestamp
drop table if exists order_range;
create table order_range(id bigserial not null,userid integer,product text, createdate timestamp not null) partition by range ( createdate );
create table order_range_201701 partition of order_range(id primary key,userid,product, createdate) for values from ('2017-01-01') to ('2017-02-01');
create table order_range_201702 partition of order_range(id primary key,userid,product, createdate) for values from ('2017-02-01') to ('2017-03-01');
create table order_range_default partition of order_range default;
insert into order_range values(1,1,'abc','2017-01-03');                                   
insert into order_range values(2,2,'abc','2017-02-03');                                   
insert into order_range values(3,3,'abc','2017-04-03');
select * from order_range_default;
create table order_range_201704 partition of order_range(id primary key,userid,product, createdate) for values from ('2017-04-01') to ('2017-05-01');
select * from order_range_default;
select * from order_range_201704;
drop table order_range;

reset all;

-- test update pull up
drop table if exists test1;
drop table if exists test2;
drop table if exists test3;
drop table if exists test4;
create table test1 (
    c11 integer,
    c12 integer,
    c13 integer,
    c14 integer,
    c15 integer
);
create table test2 (
    c21 integer,
    c22 integer,
    c23 integer,
    c24 integer,
    c25 integer
);

create table test3 (
    c31 bigint,
    c32 bigint,
    c33 bigint,
    c34 bigint,
    c35 bigint
);

create table test4 (
    c41 char(10),
    c42 char(10),
    c43 char(10),
    c44 char(10),
    c45 char(10)
);
insert into test1
select  i,i,i,i,i
from generate_series(1, 10) as i;

insert into test2
select  i,i,i+1,i,i
from generate_series(1, 10) as i;

insert into test3
select  i,i,i+1,i,i
from generate_series(1, 10) as i;

insert into test4
select  i,i,i+1,i,i
from generate_series(1, 10) as i;
explain (costs off) update test1 set c13=(select test2.c23 from test2 where test1.c12=test2.c22);

update test1 set c13=(select test2.c23 from test2 where test1.c12=test2.c22);

select * from test1 order by c11;

delete from test1;

insert into test1
select  i,i,i,i,i
from generate_series(1, 10) as i;

explain (costs off) update test1 set c13=(select test3.c33 from test3 where test1.c12=test3.c32);

update test1 set c13=(select test3.c33 from test3 where test1.c12=test3.c32);
drop table test1;
drop table test2;
drop table test3;
drop table test4;

-- opentenbase_ora compatible
CREATE TABLE tbl_gb18030_opentenbase_ora(f1 varchar(3));
CREATE TABLE tbl_gb18030_opentenbase_ora(f1 varchar(3));
INSERT INTO tbl_gb18030_opentenbase_ora (f1) VALUES ('邓东宝');
INSERT INTO tbl_gb18030_opentenbase_ora (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_gb18030_opentenbase_ora (f1) VALUES ('朱镕非');
INSERT INTO tbl_gb18030_opentenbase_ora (f1) VALUES ('王家坝');
INSERT INTO tbl_gb18030_opentenbase_ora (f1) VALUES ('王一位');
INSERT INTO tbl_gb18030_opentenbase_ora (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_gb18030_opentenbase_ora (f1) VALUES ('<80>𣘗𧄧');
-- out of bound error
INSERT INTO tbl_gb18030_opentenbase_ora (f1) VALUES ('王家坝2');
INSERT INTO tbl_gb18030_opentenbase_ora (f1) VALUES ('<80>𣘗𧄧2');
DROP TABLE tbl_gb18030_opentenbase_ora;

--
-- utf8
--
CREATE DATABASE db_utf8 template template0_ora encoding = UTF8;
\c db_utf8;
CREATE TABLE tbl_utf8(f1 varchar(3));
INSERT INTO tbl_utf8 (f1) VALUES ('邓东宝');
INSERT INTO tbl_utf8 (f1) VALUES ('李尔王');
DROP TABLE tbl_utf8;

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
