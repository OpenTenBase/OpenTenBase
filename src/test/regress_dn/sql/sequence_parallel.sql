\c regression_ora

-- force to parallel
set parallel_tuple_cost to 0;
set parallel_setup_cost to 0;
set min_parallel_table_scan_size to 0;
set max_parallel_workers_per_gather to 2;



-- create source table
drop table if exists base1;
create table base1(id int primary key, name varchar2(20));
insert into base1 select n,'col_'||n from generate_series(1,1000) n;

-- create sequence
CREATE SEQUENCE seq;

-- enable sequence parallel
set enable_parallel_sequence to on;

-- select test
explain (costs off) select seq.nextval, name from base1;

-- reset sequence
select setval('seq', 1, false);

-- Insert test
create table t1(id int primary key, name varchar2(20));
explain (costs off) insert into t1 select seq.nextval, name from base1;
insert into t1 select seq.nextval, name from base1;
select count(distinct id) from t1;

-- update test
set enable_parallel_update to on;
explain (costs off) update t1 set id = seq.nextval where id <= 10;

-- rowid test
create table t2(id int primary key, name varchar2(20));
set enable_parallel_sequence to off;
explain (costs off) insert into t2 select * from base1;
set enable_parallel_sequence to on;
explain (costs off) insert into t2 select * from base1;
insert into t2 select * from base1;

drop table base1;
drop sequence seq;
drop table t1;
drop table t2;
reset parallel_tuple_cost;
reset parallel_setup_cost;
reset min_parallel_table_scan_size;
reset max_parallel_workers_per_gather;