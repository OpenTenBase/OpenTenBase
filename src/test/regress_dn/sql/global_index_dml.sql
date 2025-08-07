-- =====================================================================
-- Title     : global index ddl and dml
-- Author    : azurezhao
-- Date      : 2022-08-22
-- Level     : P0
-- Tag	     :
-- Status    : Ready
-- AllocateEnv   : ENV_SERIAL
-- =====================================================================

drop table if exists gindex_insert;
create table gindex_insert(a int,b int,c varchar(10),d varchar) distribute by shard(a);
create unique index on gindex_insert(a);
create global unique index on gindex_insert(b);
create global unique index on gindex_insert(d);
create global index on gindex_insert(c);

insert into gindex_insert values(0,0,'zero','0');
insert into gindex_insert values(1,1,'one','1'),(2,2,'two','2');
insert into gindex_insert select generate_series(3,10),generate_series(3,10),'na',generate_series(3,10);

insert into gindex_insert values(10000,NULL,'zero','011');
insert into gindex_insert values(10002,NULL,'zero','012');

insert into gindex_insert values(11,11,'11',null);
insert into gindex_insert values(12,12,'12',null);
insert into gindex_insert values(13,13,'13','');
insert into gindex_insert values(14,14,'14','');
explain (costs off, nodes off) select * from gindex_insert where b=14;
explain (costs off, nodes off) select * from gindex_insert where c='14';
insert into gindex_insert values((select count(1)+10085 from gindex_insert),null,null,null);
insert into gindex_insert values((select count(1)+10084 from gindex_insert),null,null,null);
insert into gindex_insert values((select count(1)+10086 from gindex_insert),(select count(1)+10086 from gindex_insert),(select count(1)+10086 from gindex_insert),(select count(1)+10086 from gindex_insert));

-- todo insert into gindex_insert values(20001,nullif('A','A'),null,null);

--todo insert into gindex_insert values (1,1,'one') on conflict(a) do UPDATE SET c = gindex_insert.c+1;
--todo insert into gindex_insert values (1,1,'one') on conflict(a) do NOTHING;


/* update */
explain (costs off, nodes off) update gindex_insert set d= '1023232' where d = '1';
explain (costs off, nodes off) update gindex_insert set c= '100' where c = 'na';
explain (costs off, nodes off) update gindex_insert set c= '100' where d = '011';
explain (costs off, nodes off) update gindex_insert set d= '100' where c = '10104';

update gindex_insert set d= '1023232' where d = '1';
update gindex_insert set c= '100' where c = 'na';
update gindex_insert set c= '100' where d = '011';
update gindex_insert set d= '100' where c = '10104';

update gindex_insert set b='10000',c= '10000',d='10000' where c = '10104';
--tod update gindex_insert set b=(select count(1)+10086 from gindex_insert),c= '10000',d=now()::varchar where c = '10000';
update gindex_insert set b=null,c= null,d=null where c = '10000';

delete from gindex_insert where d != '1023232' or d is null;
COPY gindex_insert (a, b, c, d) TO stdout;
COPY gindex_insert (d) TO stdout;
COPY gindex_insert FROM stdin; -- fail
20003	20003	20003	20003
\.

delete from gindex_insert where d = '2';
delete from gindex_insert where c = 'one';
delete from gindex_insert where c = '100';
delete from gindex_insert where b in (select count(1) from gindex_insert); --fail

merge into gindex_insert t1 using gindex_insert t2 on(t1.a=t2.a) when matched then update set t1.b=t2.b; --fail

truncate gindex_insert;
vacuum gindex_insert;
vacuum full gindex_insert;
vacuum analyze gindex_insert;

drop table gindex_insert;

drop table if exists gindex_alter;
create table gindex_alter(a int,b int,c int) distribute by shard(a);
create index on gindex_alter(a);
create global index on gindex_alter(b);

--rename table
insert into gindex_alter select generate_series(1,100),generate_series(1,100),generate_series(1,100);
alter table gindex_alter rename to gindex_alter1;
insert into gindex_alter1 select generate_series(1,100),generate_series(1,100),generate_series(1,100);
update gindex_alter1 set b = 10,c = 20;
delete from gindex_alter1;
alter table gindex_alter1 rename to gindex_alter;
insert into gindex_alter select generate_series(1,100),generate_series(1,100),generate_series(1,100);

--rename column
alter table gindex_alter rename column b to d;
insert into gindex_alter select generate_series(1,100),generate_series(1,100),generate_series(1,100);
update gindex_alter set d = d+10,c = 20;
delete from gindex_alter;

--rename index
insert into gindex_alter select generate_series(1,100),generate_series(1,100),generate_series(1,100);
insert into gindex_alter select generate_series(1,100),generate_series(1,100),generate_series(1,100);
update gindex_alter set d = d+10,c = 20;
delete from gindex_alter;

--drop column
alter table gindex_alter drop COLUMN d;
select * from gindex_alter;

--add column
alter table gindex_alter add d varchar;
create global index on gindex_alter(d);
insert into gindex_alter select generate_series(1,100),generate_series(1,100),generate_series(1,100);

--alter type
alter table gindex_alter alter column d TYPE int; --fail
alter table gindex_alter alter column c TYPE varchar; --fail

alter table gindex_alter alter column c set NOT NULL;
alter table gindex_alter alter column c set default 999;
insert into gindex_alter(a,d) select generate_series(1,100),generate_series(1,100) limit 10;

create global index on gindex_alter(c);
drop sequence if exists gindex_seq;
create sequence gindex_seq;
alter table gindex_alter alter column c set default nextval('gindex_seq');
insert into gindex_alter(a,d) select generate_series(100,200),generate_series(1,100) limit 10;
select *  from gindex_alter where a >100 order by c;
delete from gindex_alter;

--truncate table
truncate gindex_alter_c_idx_idt;
truncate gindex_alter;

--drop table 
drop table gindex_alter;
select count(1) from gindex_alter_c_idx; --failed

--vacuum
drop table if exists gindex_vacuum;
create table gindex_vacuum(a int,b int,c int) distribute by shard(a);
create global index on gindex_vacuum(c);
create global index on gindex_vacuum(b);

insert into gindex_vacuum(a,b) select generate_series(100,200),generate_series(1,100);

vacuum gindex_vacuum_b_idx ;
vacuum gindex_vacuum;
vacuum FULL gindex_vacuum_b_idx ;
vacuum FULL gindex_vacuum; --failed

vacuum analyze gindex_vacuum_b_idx ;
vacuum analyze gindex_vacuum;

insert into gindex_vacuum select generate_series(1,100),generate_series(1,100),generate_series(1,100);
insert into gindex_vacuum values(11111,132423,12132342);
select count(1) from gindex_vacuum;
select * from gindex_vacuum where c = 10;
select * from gindex_vacuum where b = 11;
--todo [gindex-update] failed to find old tid we want to update
update gindex_vacuum set b = 10000;
update gindex_vacuum set c = b + 10,b = 5;
select count(1) from gindex_vacuum where b = 5;
delete from gindex_vacuum;
drop table gindex_vacuum;

--cluster
drop table if exists gindex_cluster;
create table gindex_cluster(a int,b int,c int) distribute by shard(a);
create index on gindex_cluster(a);
create global index gindex_cluster_c_idx on gindex_cluster(c);
create global index gindex_cluster_b_idx on gindex_cluster(b);
cluster gindex_cluster using gindex_cluster_a_idx;
drop table gindex_cluster;
