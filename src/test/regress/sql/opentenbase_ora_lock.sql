\c regression_ora

set enable_fast_query_shipping=true;

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists p1;
drop table if exists c1;

-- create some tables
create table t1(val int, val2 int);
create table t2(val int, val2 int);
create table t3(val int, val2 int);

create table p1(a int, b int);
create table c1(d int, e int) inherits (p1);

-- insert some rows in them
insert into t1 values(1,11),(2,11);
insert into t2 values(3,11),(4,11);
insert into t3 values(5,11),(6,11);

insert into p1 values(55,66),(77,88);
insert into c1 values(111,222,333,444),(123,345,567,789);

-- create a view too
create view v2 as select * from t1 for update wait 10;
drop view v2;

-- test a few queries with row marks
select * from t1 order by 1 for update of t1 wait 10;

WITH q1 AS (SELECT * from t1 order by 1 FOR UPDATE) SELECT * FROM q1 FOR UPDATE WAIT 10;

-- confirm that in various join scenarios for update gets to the remote query
-- single table case
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 for update of t1 wait 10;

-- two table case, not supported
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 where t1.val = t2.val for update wait 10;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 for update wait 10;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 for share wait 10;

-- three table case, not supported
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2, t3 for update of t1,t3 wait 10;

-- check a few subquery cases, not supported
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 where val in (select val from t2 for update of t2 wait 10) for update;

-- make sure FOR UPDATE takes prioriy over FOR SHARE when mentioned for the same table
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 for share of t1 wait 5 for update of t1 wait 10;

-- make sure NOWAIT is used in remote query even if it is not mentioned with FOR UPDATE clause
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 for share of t1 for share of t1 nowait for update of t1 wait 10;

-- confirm that in various join scenarios for update gets to the remote query
-- single table case
select * from t1 for update of t1 wait 10;

-- two table case, not supported
select * from t1, t2 where t1.val = t2.val for update wait 10;

create table mytab1(val int, val2 int, val3 int);
-- create index,rule,trgger & view on one of the tables
CREATE UNIQUE INDEX test_idx ON mytab1 (val);

-- insert some rows
insert into mytab1 values(1,11,1122),(2,11,3344);

-- prepare a transaction that holds a ACCESS EXCLUSIVE (ROW SHARE) lock on a table
begin;
declare c1 cursor for select * from mytab1 for update;
fetch 1 from c1;
prepare transaction 'tbl_mytab1_locked';

set statement_timeout to 2000;

-- do a SELECT FOR UPDATE WAIT on it (Should fail)
begin;
    declare c1 cursor for select * from mytab1 for update wait 1;
    fetch 1 from c1;
end;

-- do a SELECT FOR UPDATE WAIT on it (Should fail)
begin;
    declare c1 cursor for select * from mytab1 for update wait 3;
    fetch 1 from c1;
end;

-- behave like NOWAIT
select * from mytab1 for update wait 0;

select * from mytab1 for update nowait;

-- priority
explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1  for update for update wait 1;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1  for update for update wait 0;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1  for update wait 0 for update wait 1;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1  for update wait 1 for update wait 20000;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1  for update wait 1 for update nowait;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1  for update wait 1 for update skip locked;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1  for update wait 0 for update nowait;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1  for update wait 0 for update skip locked;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1 for update for update nowait;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1 for update for update skip locked;

explain (costs off, num_nodes off, nodes off, verbose on)
select * from mytab1 for update nowait for update skip locked;

reset statement_timeout;

SET lock_timeout = 2000;
select * from mytab1 for update wait 2000 for update wait 20000;
reset lock_timeout;

commit prepared 'tbl_mytab1_locked';
drop table if exists t1 cascade;
drop table if exists t2 cascade;
drop table if exists t3 cascade;
drop table if exists p1 cascade;
drop table if exists c1 cascade;
drop table if exists mytab1 cascade;
reset enable_fast_query_shipping;