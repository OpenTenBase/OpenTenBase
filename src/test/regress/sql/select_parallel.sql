--
-- PARALLEL
--

create or replace function parallel_restricted(int) returns int as
  $$begin return $1; end$$ language plpgsql parallel restricted;

-- Serializable isolation would disable parallel query, so explicitly use an
-- arbitrary other level.
begin isolation level repeatable read;

-- encourage use of parallel plans
set parallel_setup_cost=0;
set parallel_tuple_cost=0;
set min_parallel_table_scan_size=0;
set max_parallel_workers_per_gather=4;

-- OPENTENBASE: increase to encourage use of parallel
analyze a_star; analyze b_star; analyze c_star; analyze d_star; analyze e_star; analyze f_star;
update pg_class set relpages=relpages*100, reltuples=reltuples*100 where relname like '%_star';

-- Parallel Append with partial-subplans
explain (costs off)
  select round(avg(aa)), sum(aa) from a_star;
select round(avg(aa)), sum(aa) from a_star a1;

-- Parallel Append with both partial and non-partial subplans
alter table c_star set (parallel_workers = 0);
alter table d_star set (parallel_workers = 0);
explain (costs off)
  select round(avg(aa)), sum(aa) from a_star;
select round(avg(aa)), sum(aa) from a_star a2;

-- Parallel Append with only non-partial subplans
alter table a_star set (parallel_workers = 0);
alter table b_star set (parallel_workers = 0);
alter table e_star set (parallel_workers = 0);
alter table f_star set (parallel_workers = 0);
explain (costs off)
  select round(avg(aa)), sum(aa) from a_star;
select round(avg(aa)), sum(aa) from a_star a3;

-- Disable Parallel Append
alter table a_star reset (parallel_workers);
alter table b_star reset (parallel_workers);
alter table c_star reset (parallel_workers);
alter table d_star reset (parallel_workers);
alter table e_star reset (parallel_workers);
alter table f_star reset (parallel_workers);
set enable_parallel_append to off;
explain (costs off)
  select round(avg(aa)), sum(aa) from a_star;
select round(avg(aa)), sum(aa) from a_star a4;
reset enable_parallel_append;

-- OPENTENBASE: reset
update pg_class set relpages=relpages/2, reltuples=reltuples/2 where relname like '%_star';

-- Parallel Append that runs serially
create or replace function foobar() returns setof text as
$$ select 'foo'::varchar union all select 'bar'::varchar $$
language sql stable;
select foobar() order by 1;

-- test with leader participation disabled
set parallel_leader_participation = off;
explain (costs off)
  select count(*) from tenk1 where stringu1 = 'GRAAAA';
select count(*) from tenk1 where stringu1 = 'GRAAAA';

-- test with leader participation disabled, but no workers available (so
-- the leader will have to run the plan despite the setting)
set max_parallel_workers = 0;
explain (costs off)
  select count(*) from tenk1 where stringu1 = 'GRAAAA';
select count(*) from tenk1 where stringu1 = 'GRAAAA';

reset max_parallel_workers;
reset parallel_leader_participation;

-- test that parallel_restricted function doesn't run in worker
alter table tenk1 set (parallel_workers = 4);
explain (verbose, costs off)
select parallel_restricted(unique1) from tenk1
  where stringu1 = 'GRAAAA' order by 1;

-- test parallel plan when group by expression is in target list.
explain (costs off)
	select length(stringu1) from tenk1 group by length(stringu1);
select length(stringu1) from tenk1 group by length(stringu1);

explain (costs off)
	select stringu1, count(*) from tenk1 group by stringu1 order by stringu1;

-- test that parallel plan for aggregates is not selected when
-- target list contains parallel restricted clause.
set enable_indexonlyscan to off;
explain (costs off)
	select  sum(parallel_restricted(unique1)) from tenk1
	group by(parallel_restricted(unique1));
reset enable_indexonlyscan;

-- test prepared statement
prepare tenk1_count(integer) As select  count((unique1)) from tenk1 where hundred > $1;
explain (costs off) execute tenk1_count(1);
execute tenk1_count(1);
deallocate tenk1_count;

-- test parallel plans for queries containing un-correlated subplans.
alter table tenk2 set (parallel_workers = 0);
explain (costs off)
	select count(*) from tenk1 where (two, four) not in
	(select hundred, thousand from tenk2 where thousand > 100);
select count(*) from tenk1 where (two, four) not in
	(select hundred, thousand from tenk2 where thousand > 100);
-- this is not parallel-safe due to use of random() within SubLink's testexpr:
explain (costs off)
	select * from tenk1 where (unique1 + random())::integer not in
	(select ten from tenk2);
alter table tenk2 reset (parallel_workers);

-- test parallel plan for a query containing initplan.
set enable_indexscan = off;
set enable_indexonlyscan = off;
set enable_bitmapscan = off;
alter table tenk2 set (parallel_workers = 2);

explain (costs off)
	select count(*) from tenk1
        where tenk1.unique1 = (Select max(tenk2.unique1) from tenk2);
select count(*) from tenk1
    where tenk1.unique1 = (Select max(tenk2.unique1) from tenk2);

reset enable_indexscan;
reset enable_indexonlyscan;
reset enable_bitmapscan;
alter table tenk2 reset (parallel_workers);

-- test parallel index scans.
set enable_seqscan to off;
set enable_bitmapscan to off;

explain (costs off)
	select  count((unique1)) from tenk1 where hundred > 1;
select  count((unique1)) from tenk1 where hundred > 1;

-- test parallel index-only scans.
explain (costs off)
	select  count(*) from tenk1 where thousand > 95;
select  count(*) from tenk1 where thousand > 95;

-- test rescan cases too
set enable_material = false;

explain (costs off)
select * from
  (select count(unique1) from tenk1 where hundred > 10) ss
  right join (values (1),(2),(3)) v(x) on true;
select * from
  (select count(unique1) from tenk1 where hundred > 10) ss
  right join (values (1),(2),(3)) v(x) on true;

explain (costs off)
select * from
  (select count(*) from tenk1 where thousand > 99) ss
  right join (values (1),(2),(3)) v(x) on true;
select * from
  (select count(*) from tenk1 where thousand > 99) ss
  right join (values (1),(2),(3)) v(x) on true;

-- test rescans for a Limit node with a parallel node beneath it.
reset enable_seqscan;
set enable_indexonlyscan to off;
set enable_indexscan to off;
alter table tenk1 set (parallel_workers = 0);
alter table tenk2 set (parallel_workers = 1);
explain (costs off)
select count(*) from tenk1
  left join (select tenk2.unique1 from tenk2 order by 1 limit 1000) ss
  on tenk1.unique1 < ss.unique1 + 1
  where tenk1.unique1 < 2;
select count(*) from tenk1
  left join (select tenk2.unique1 from tenk2 order by 1 limit 1000) ss
  on tenk1.unique1 < ss.unique1 + 1
  where tenk1.unique1 < 2;
--reset the value of workers for each table as it was before this test.
alter table tenk1 set (parallel_workers = 4);
alter table tenk2 reset (parallel_workers);

reset enable_material;
reset enable_bitmapscan;
reset enable_indexonlyscan;
reset enable_indexscan;

-- test parallel bitmap heap scan.
set enable_seqscan to off;
set enable_indexscan to off;
set enable_hashjoin to off;
set enable_mergejoin to off;
set enable_material to off;
-- test prefetching, if the platform allows it
--DO $$
--BEGIN
-- SET effective_io_concurrency = 50;
--EXCEPTION WHEN invalid_parameter_value THEN
--END $$;
set work_mem='64kB';  --set small work mem to force lossy pages
explain (costs off)
	select count(*) from tenk1, tenk2 where tenk1.hundred > 1 and tenk2.thousand=0;
select count(*) from tenk1, tenk2 where tenk1.hundred > 1 and tenk2.thousand=0;

create table bmscantest (a int, t text);
insert into bmscantest select r, 'fooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo' FROM generate_series(1,100000) r;
create index i_bmtest ON bmscantest(a);
select count(*) from bmscantest where a>1;

-- test accumulation of stats for parallel nodes
reset enable_seqscan;
reset enable_material;
alter table tenk2 set (parallel_workers = 0);
--explain (analyze, timing off, summary off, costs off)
--   select count(*) from tenk1, tenk2 where tenk1.hundred > 1
--       and tenk2.thousand=0;
alter table tenk2 reset (parallel_workers);

reset work_mem;
create function explain_parallel_sort_stats() returns setof text
language plpgsql as
$$
declare ln text;
begin
    for ln in
        explain (analyze, timing off, summary off, costs off)
          select * from
          (select ten from tenk1 where ten < 100 order by ten) ss
          right join (values (1),(2),(3)) v(x) on true
    loop
        ln := regexp_replace(ln, 'Memory: \S*',  'Memory: xxx');
        return next ln;
    end loop;
end;
$$;
--select * from explain_parallel_sort_stats();

reset enable_indexscan;
reset enable_hashjoin;
reset enable_mergejoin;
reset enable_material;
reset effective_io_concurrency;
drop table bmscantest;
drop function explain_parallel_sort_stats();

-- test parallel merge join path.
set enable_hashjoin to off;
set enable_nestloop to off;
set enable_indexonlyscan to off;

select  count(*) from tenk1, tenk2 where tenk1.unique1 = tenk2.unique1;

reset enable_hashjoin;
reset enable_nestloop;
reset enable_indexonlyscan;

-- test gather merge
set enable_hashagg = false;

explain (costs off)
   select count(*) from tenk1 group by twenty;

select count(*) from tenk1 group by twenty;

--test expressions in targetlist are pushed down for gather merge
create or replace function simple_func(var1 integer) returns integer
as $$
begin
        return var1 + 10;
end;
$$ language plpgsql PARALLEL SAFE;

explain (costs off, verbose)
    select ten, simple_func(ten) from tenk1 where ten < 100 order by ten;

drop function simple_func(integer);

-- test gather merge with parallel leader participation disabled
set parallel_leader_participation = off;

explain (costs off)
   select count(*) from tenk1 group by twenty;

select count(*) from tenk1 group by twenty;

reset parallel_leader_participation;

--test rescan behavior of gather merge
set enable_material = false;

explain (costs off)
select * from
  (select string4, count(unique2)
   from tenk1 group by string4 order by string4) ss
  right join (values (1),(2),(3)) v(x) on true;

select * from
  (select string4, count(unique2)
   from tenk1 group by string4 order by string4) ss
  right join (values (1),(2),(3)) v(x) on true order by 1, 2, 3;

reset enable_material;

reset enable_hashagg;

-- check parallelized int8 aggregate (bug #14897)
set enable_indexonlyscan to off;
explain (costs off)
select avg(unique1::int8) from tenk1;

select avg(unique1::int8) from tenk1;
reset enable_indexonlyscan;

-- gather merge test with a LIMIT
explain (costs off)
  select fivethous from tenk1 order by fivethous limit 4;

select fivethous from tenk1 order by fivethous limit 4;

-- gather merge test with 0 worker
set max_parallel_workers = 0;
explain (costs off)
   select string4 from tenk1 order by string4 limit 5;
select string4 from tenk1 order by string4 limit 5;

-- gather merge test with 0 workers, with parallel leader
-- participation disabled (the leader will have to run the plan
-- despite the setting)
set parallel_leader_participation = off;
explain (costs off)
   select string4 from tenk1 order by string4 limit 5;
select string4 from tenk1 order by string4 limit 5;

reset parallel_leader_participation;
reset max_parallel_workers;

SAVEPOINT settings;
SET LOCAL force_parallel_mode = 1;
explain (costs off)
  select stringu1::int2 from tenk1 where unique1 = 1;
ROLLBACK TO SAVEPOINT settings;

-- exercise record typmod remapping between backends
CREATE OR REPLACE FUNCTION make_record(n int)
  RETURNS RECORD LANGUAGE plpgsql PARALLEL SAFE AS
$$
BEGIN
  RETURN CASE n
           WHEN 1 THEN ROW(1)
           WHEN 2 THEN ROW(1, 2)
           WHEN 3 THEN ROW(1, 2, 3)
           WHEN 4 THEN ROW(1, 2, 3, 4)
           ELSE ROW(1, 2, 3, 4, 5)
         END;
END;
$$;
SAVEPOINT settings;
SET LOCAL force_parallel_mode = 1;
SELECT make_record(x) FROM (SELECT generate_series(1, 5) x) ss ORDER BY x;
ROLLBACK TO SAVEPOINT settings;
DROP function make_record(n int);

-- test the sanity of parallel query after the active role is dropped.
drop role if exists regress_parallel_worker;
create role regress_parallel_worker;
set role regress_parallel_worker;
reset session authorization;
drop role regress_parallel_worker;
set force_parallel_mode = 1;
select count(*) from tenk1;
reset force_parallel_mode;
reset role;

-- to increase the parallel query test coverage
SAVEPOINT settings;
SET LOCAL force_parallel_mode = 1;
EXPLAIN (analyze, timing off, summary off, costs off) SELECT * FROM tenk1;
ROLLBACK TO SAVEPOINT settings;

-- provoke error in worker
SAVEPOINT settings;
SET LOCAL force_parallel_mode = 1;
select stringu1::int2 from tenk1 where unique1 = 1;
ROLLBACK TO SAVEPOINT settings;

-- test interaction with set-returning functions
SAVEPOINT settings;

-- multiple subqueries under a single Gather node
-- must set parallel_setup_cost > 0 to discourage multiple Gather nodes
SET LOCAL parallel_setup_cost = 10;
EXPLAIN (COSTS OFF)
SELECT unique1 FROM tenk1 WHERE fivethous = tenthous + 1
UNION ALL
SELECT unique1 FROM tenk1 WHERE fivethous = tenthous + 1;
ROLLBACK TO SAVEPOINT settings;

-- can't use multiple subqueries under a single Gather node due to initPlans
EXPLAIN (COSTS OFF)
SELECT unique1 FROM tenk1 WHERE fivethous =
	(SELECT unique1 FROM tenk1 WHERE fivethous = 1 LIMIT 1)
UNION ALL
SELECT unique1 FROM tenk1 WHERE fivethous =
	(SELECT unique2 FROM tenk1 WHERE fivethous = 1 LIMIT 1)
ORDER BY 1;

-- test interaction with SRFs
SELECT * FROM information_schema.foreign_data_wrapper_options
ORDER BY 1, 2, 3;

-- test interation between subquery and partial_paths
SET LOCAL min_parallel_table_scan_size TO 0;
CREATE VIEW tenk1_vw_sec WITH (security_barrier) AS SELECT * FROM tenk1;
EXPLAIN (COSTS OFF)
SELECT 1 FROM tenk1_vw_sec WHERE EXISTS (SELECT 1 WHERE unique1 = 0);

-- OPENTENBASE
ROLLBACK TO SAVEPOINT settings;

explain (costs off)
	select count(stringu1) as num, (CASE WHEN length(stringu1) > 5 THEN 'LONG' ELSE 'SHORT' END) as islong
		from tenk1 group by islong order by num;
select count(stringu1) as num, (CASE WHEN length(stringu1) > 5 THEN 'LONG' ELSE 'SHORT' END) as islong
	from tenk1 group by islong order by num;

-- make sure identifier was set in workers
CREATE TABLE t_worker_identifier (a int);
INSERT INTO t_worker_identifier values(1);
EXPLAIN (costs off) SELECT xc_node_id != 0 FROM t_worker_identifier;
SELECT xc_node_id != 0 FROM t_worker_identifier;
drop table t_worker_identifier;

rollback;

-- Fixing the issue of redundant data returned in concurrent append scenarios with UNION ALL
DROP TABLE if exists rqg_table6_copy_p cascade;
CREATE TABLE rqg_table6_copy_p (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)    PARTITION BY hash( c0) distribute by replication;
create TABLE rqg_table6_copy_p_p2 partition of rqg_table6_copy_p for values with(modulus 4,remainder 2);
DROP TABLE if exists rqg_table1_copy cascade;
CREATE TABLE rqg_table1_copy (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)   ;
INSERT INTO rqg_table1_copy VALUES  (5, 0, 'iqjbwqidqeqffetnvalawyljwrjjoqmyuue', NULL, '2023-07-09', '2000-07-07', '2032-08-21 16:08:36.040014', '1995-01-08 17:12:47.021500', 0.830612182617188, 0.420501708984375) ;
set parallel_tuple_cost to 0.001;
set parallel_setup_cost to 1;
explain (costs off)
select 1 as t, c0 from rqg_table6_copy_p union all select 2 as t,c0 from rqg_table1_copy where c0>2;
select 1 as t, c0 from rqg_table6_copy_p union all select 2 as t,c0 from rqg_table1_copy where c0>2;
reset parallel_tuple_cost;
reset parallel_setup_cost;
DROP TABLE rqg_table6_copy_p cascade;
DROP TABLE rqg_table1_copy cascade;
