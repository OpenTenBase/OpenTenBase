--
-- parallel update tests
--

drop table if exists updatetest;
CREATE TABLE updatetest (
    unique1 int4,
    a       int4,
    b       int4,
    c       int4,
    d       int4,
    e       int4
);

create index u_index on updatetest using BTREE(unique1);
create index a_index on updatetest using BTREE(a);
create index b_index on updatetest using BTREE(b);
create index c_index on updatetest using BTREE(c);
create index d_index on updatetest using BTREE(d);

INSERT INTO updatetest(unique1, a, b, c, d, e)
SELECT n,
       n % 10,
       n % 100,
       n % 1000,
       n + 100,
       n + 200
FROM generate_series(1,10000) AS n;

drop table if exists paral_test1;
CREATE TABLE paral_test1 (
    c1  int4,
    c2  int4
);

INSERT INTO paral_test1(c1, c2)
SELECT n % 10,
       n % 100
FROM generate_series(1,100) AS n;

drop table if exists paral_test2;
CREATE TABLE paral_test2 (
    x1  int4,
    x2  int4
);

INSERT INTO paral_test2(x1, x2)
SELECT n % 10 + 3,
       n % 100 + 5
FROM generate_series(1,100) AS n;


set enable_parallel_update = on;
set parallel_tuple_cost = 0;
set parallel_setup_cost = 0;
set min_parallel_table_scan_size = 0;
set enable_indexscan = off;
SET enable_bitmapscan = OFF;


set max_parallel_workers_per_gather=4;

-- parallel plan test
explain (costs off) update updatetest set a = a + 1;
explain (costs off) update updatetest set b = b + 1 where b > 50;
explain (costs off) update updatetest set c = c + 1;
explain (costs off) update updatetest set d = d + 1 where d > 10;

-- update
update updatetest set a = a + 1 ;
update updatetest set b = b + 1 where b > 5;
update updatetest set c = c + 1;
update updatetest set d = d + 1 where d > 10;

-- heap update check
select * from updatetest order by unique1 limit 10;

-- force index scan
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;

-- index update check
select * from updatetest where unique1 = 20;
select * from updatetest where a = 5 order by unique1 limit 10;
select * from updatetest where b = 90 order by unique1 limit 10;

-- complex update
SET enable_seqscan to default;
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_mergejoin = OFF;

explain (costs off) update updatetest set d = 99999 from paral_test1 where unique1 = c1;
explain (costs off) update updatetest L set d = L.e - (select sum(c1) from paral_test1, paral_test2 where c2 = x2 );
explain (costs off) update updatetest L set d = L.e - (select min(c1) from paral_test1, paral_test2 where c2 = x2 and c = paral_test2.x2);
explain (costs off) update updatetest L set d = L.e - (select min(c1) from paral_test1, paral_test2 where c2 = x1 and c = paral_test2.x2) where L.b = (select c2 from paral_test1 where L.a = c2);

update updatetest set d = 99999 from paral_test1 where unique1 = c1;
select * from updatetest where d = 99999 order by unique1;

update updatetest L set d = L.e - (select sum(c1) from paral_test1, paral_test2 where c2 = x2 );
select * from updatetest order by unique1 limit 10;

update updatetest L set d = L.e - (select min(c1) from paral_test1, paral_test2 where c2 = x2 and c = paral_test2.x2);
select * from updatetest order by unique1 limit 10;

update updatetest L set d = L.e - (select min(c1) from paral_test1, paral_test2 where c2 = x1 and c = paral_test2.x2) where L.b = (select c2 from paral_test1 where L.a = c2);
select * from updatetest order by unique1 limit 10;

drop table updatetest;
drop table paral_test1;
drop table paral_test2;


