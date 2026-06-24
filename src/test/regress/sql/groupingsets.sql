--
-- grouping sets
--

-- test data sources

create temp view gstest1(a,b,v)
  as values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),
            (2,3,15),
            (3,3,16),(3,4,17),
            (4,1,18),(4,1,19);

create temp table gstest2 (a integer, b integer, c integer, d integer,
                           e integer, f integer, g integer, h integer);
copy gstest2 from stdin;
1	1	1	1	1	1	1	1
1	1	1	1	1	1	1	2
1	1	1	1	1	1	2	2
1	1	1	1	1	2	2	2
1	1	1	1	2	2	2	2
1	1	1	2	2	2	2	2
1	1	2	2	2	2	2	2
1	2	2	2	2	2	2	2
2	2	2	2	2	2	2	2
\.

create temp table gstest3 (a integer, b integer, c integer, d integer);
copy gstest3 from stdin;
1	1	1	1
2	2	2	2
\.
alter table gstest3 add primary key (a);

create temp table gstest4(id integer, v integer,
                          normal_col bit(4), unsortable_col xid);
insert into gstest4
values (1,1,b'0000','1'), (2,2,b'0001','1'),
       (3,4,b'0010','2'), (4,8,b'0011','2'),
       (5,16,b'0000','2'), (6,32,b'0001','2'),
       (7,64,b'0010','1'), (8,128,b'0011','1');

create temp table gstest5(id integer, v integer,
                          unhashable_col tid, unsortable_col xid);
insert into gstest5
values (1,1,'(0,0)','1'), (2,2,'(0,1)','1'),
       (3,4,'(0,2)','2'), (4,8,'(0,3)','2'),
       (5,16,'(0,0)','2'), (6,32,'(0,1)','2'),
       (7,64,'(0,2)','1'), (8,128,'(0,3)','1');

create temp table gstest_empty (a integer, b integer, v integer);

create function gstest_data(v integer, out a integer, out b integer)
  returns setof record
  as $f$
    begin
      return query select v, i from generate_series(1,3) i;
    end;
  $f$ language plpgsql;

-- basic functionality

set enable_hashagg = false;  -- test hashing explicitly later

-- simple rollup with multiple plain aggregates, with and without ordering
-- (and with ordering differing from grouping)

select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by rollup (a,b);
select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by rollup (a,b) order by a,b;
select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by rollup (a,b) order by b desc, a;
select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by rollup (a,b) order by coalesce(a,0)+coalesce(b,0),a,b;

-- further tests for grouping cte optimize

set enable_hashagg = false;

select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a)
  from gstest1 t1, gstest2 t2
 group by grouping sets ((t1.a, t2.b), ()) order by 1, 2, 3, 4, 5;

set enable_hashagg = true;

select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a)
  from gstest1 t1, gstest2 t2
 group by grouping sets ((t1.a, t2.b), ()) order by 1, 2, 3, 4, 5;

set enable_hashagg = false;

-- various types of ordered aggs
select a, b, grouping(a,b),
       array_agg(v order by v),
       string_agg(v::text, ':' order by v desc),
       percentile_disc(0.5) within group (order by v),
       rank(1,2,12) within group (order by a,b,v)
  from gstest1 group by rollup (a,b) order by a,b;

-- test usage of grouped columns in direct args of aggs
select grouping(a), a, array_agg(b),
       rank(a) within group (order by b nulls first),
       rank(a) within group (order by b nulls last)
  from (values (1,1),(1,4),(1,5),(3,1),(3,2)) v(a,b)
 group by rollup (a) order by a;

-- nesting with window functions
select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
  from gstest2 group by rollup (a,b) order by rsum, a, b;

-- nesting with grouping sets
select sum(c) from gstest2
  group by grouping sets((), grouping sets((), grouping sets(())))
  order by 1 desc;
select sum(c) from gstest2
  group by grouping sets((), grouping sets((), grouping sets(((a, b)))))
  order by 1 desc;
select sum(c) from gstest2
  group by grouping sets(grouping sets(rollup(c), grouping sets(cube(c))))
  order by 1 desc;
select sum(c) from gstest2
  group by grouping sets(a, grouping sets(a, cube(b)))
  order by 1 desc;
select sum(c) from gstest2
  group by grouping sets(grouping sets((a, (b))))
  order by 1 desc;
select sum(c) from gstest2
  group by grouping sets(grouping sets((a, b)))
  order by 1 desc;
select sum(c) from gstest2
  group by grouping sets(grouping sets(a, grouping sets(a), a))
  order by 1 desc;
select sum(c) from gstest2
  group by grouping sets(grouping sets(a, grouping sets(a, grouping sets(a), ((a)), a, grouping sets(a), (a)), a))
  order by 1 desc;
select sum(c) from gstest2
  group by grouping sets((a,(a,b)), grouping sets((a,(a,b)),a))
  order by 1 desc;

-- empty input: first is 0 rows, second 1, third 3 etc.
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),a);
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),());
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),(),(),());
select sum(v), count(*) from gstest_empty group by grouping sets ((),(),());

-- empty input with joins tests some important code paths
select t1.a, t2.b, sum(t1.v), count(*) from gstest_empty t1, gstest_empty t2
 group by grouping sets ((t1.a,t2.b),());

-- simple joins, var resolution, GROUPING on join vars
select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a)
  from gstest1 t1, gstest2 t2
 group by grouping sets ((t1.a, t2.b), ()) order by 4;

select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a)
  from gstest1 t1 join gstest2 t2 on (t1.a=t2.a)
 group by grouping sets ((t1.a, t2.b), ()) order by 4;

select a, b, grouping(a, b), sum(t1.v), max(t2.c)
  from gstest1 t1 join gstest2 t2 using (a,b)
 group by grouping sets ((a, b), ()) order by 1,2;

-- check that functionally dependent cols are not nulled
select a, d, grouping(a,b,c)
  from gstest3
 group by grouping sets ((a,b), (a,c)) order by 1,2,3;

-- check that distinct grouping columns are kept separate
-- even if they are equal()
explain (costs off)
select g as alias1, g as alias2
  from generate_series(1,3) g
 group by alias1, rollup(alias2);

select g as alias1, g as alias2
  from generate_series(1,3) g
 group by alias1, rollup(alias2);

-- check that pulled-up subquery outputs still go to null when appropriate
select four, x
  from (select four, ten, 'foo'::text as x from tenk1) as t
  group by grouping sets (four, x)
  having x = 'foo';

select four, x || 'x'
  from (select four, ten, 'foo'::text as x from tenk1) as t
  group by grouping sets (four, x)
  order by four;

select (x+y)*1, sum(z)
 from (select 1 as x, 2 as y, 3 as z) s
 group by grouping sets (x+y, x);

select x, not x as not_x, q2 from
  (select *, q1 = 1 as x from int8_tbl i1) as t
  group by grouping sets(x, q2)
  order by x, q2;

-- simple rescan tests

select a, b, sum(v.x)
  from (values (1),(2)) v(x), gstest_data(v.x)
 group by rollup (a,b);

select *
  from (values (1),(2)) v(x),
       lateral (select a, b, sum(v.x) from gstest_data(v.x) group by rollup (a,b)) s;

-- min max optimization should still work with GROUP BY ()
explain (costs off)
  select min(unique1) from tenk1 GROUP BY ();

-- Views with GROUPING SET queries
CREATE VIEW gstest_view AS select a, b, grouping(a,b), sum(c), count(*), max(c)
  from gstest2 group by rollup ((a,b,c),(c,d));

select pg_get_viewdef('gstest_view'::regclass, true);

-- Nested queries with 3 or more levels of nesting
select(select (select grouping(a,b) from (values (1)) v2(c)) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY ROLLUP(e,f);
select(select (select grouping(e,f) from (values (1)) v2(c)) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY ROLLUP(e,f);
select(select (select grouping(c) from (values (1)) v2(c) GROUP BY c) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY ROLLUP(e,f);

-- Combinations of operations
select a, b, c, d from gstest2 group by rollup(a,b),grouping sets(c,d) order by 1,2,3,4;
select a, b from (values (1,2),(2,3)) v(a,b) group by a,b, grouping sets(a);

-- Tests for chained aggregates
select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by grouping sets ((a,b),(a+1,b+1),(a+2,b+2)) order by 3,6;
select(select (select grouping(a,b) from (values (1)) v2(c)) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY ROLLUP((e+1),(f+1));
select(select (select grouping(a,b) from (values (1)) v2(c)) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY CUBE((e+1),(f+1)) ORDER BY (e+1),(f+1);
select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
  from gstest2 group by cube (a,b) order by rsum, a, b;
select a, b, sum(c) from (values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),(2,3,15),(3,3,16),(3,4,17),(4,1,18),(4,1,19)) v(a,b,c) group by rollup (a,b);
select a, b, sum(v.x)
  from (values (1),(2)) v(x), gstest_data(v.x)
 group by cube (a,b) order by a,b;


-- Agg level check. This query should error out.
select (select grouping(a,b) from gstest2) from gstest2 group by a,b;

--Nested queries
select a, b, sum(c), count(*) from gstest2 group by grouping sets (rollup(a,b),a) order by 1,2,3,4;

-- HAVING queries
select ten, sum(distinct four) from onek a
group by grouping sets((ten,four),(ten))
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- Tests around pushdown of HAVING clauses, partially testing against previous bugs
select a,count(*) from gstest2 group by rollup(a) order by a;
select a,count(*) from gstest2 group by rollup(a) having a is distinct from 1 order by a;
explain (costs off)
  select a,count(*) from gstest2 group by rollup(a) having a is distinct from 1 order by a;

select v.c, (select count(*) from gstest2 group by () having v.c)
  from (values (false),(true)) v(c) order by v.c;
explain (costs off)
  select v.c, (select count(*) from gstest2 group by () having v.c)
    from (values (false),(true)) v(c) order by v.c;

-- HAVING with GROUPING queries
select ten, grouping(ten) from onek
group by grouping sets(ten) having grouping(ten) >= 0
order by 2,1;
select ten, grouping(ten) from onek
group by grouping sets(ten, four) having grouping(ten) > 0
order by 2,1;
select ten, grouping(ten) from onek
group by rollup(ten) having grouping(ten) > 0
order by 2,1;
select ten, grouping(ten) from onek
group by cube(ten) having grouping(ten) > 0
order by 2,1;
select ten, grouping(ten) from onek
group by (ten) having grouping(ten) >= 0
order by 2,1;

-- FILTER queries
select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
group by rollup(ten);

-- More rescan tests
select * from (values (1),(2)) v(a) left join lateral (select v.a, four, ten, count(*) from onek group by cube(four,ten)) s on true order by v.a,four,ten;
select array(select row(v.a,s1.*) from (select two,four, count(*) from onek group by cube(two,four) order by two,four) s1) from (values (1),(2)) v(a);

-- Grouping on text columns
select sum(ten) from onek group by two, rollup(four::text) order by 1;
select sum(ten) from onek group by rollup(four::text), two order by 1;

-- hashing support

set enable_hashagg = true;

-- failure cases

select count(*) from gstest5 group by rollup(unhashable_col,unsortable_col);
select array_agg(v order by v) from gstest4 group by grouping sets ((id,unsortable_col),(id));

-- simple cases

select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by grouping sets ((a),(b)) order by 3,1,2;
explain (costs off) select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by grouping sets ((a),(b)) order by 3,1,2;

select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by cube(a,b) order by 3,1,2;
explain (costs off) select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by cube(a,b) order by 3,1,2;

-- shouldn't try and hash
explain (costs off)
  select a, b, grouping(a,b), array_agg(v order by v)
    from gstest1 group by cube(a,b);

-- unsortable cases
select unsortable_col, count(*)
  from gstest4 group by grouping sets ((unsortable_col),(unsortable_col))
  order by unsortable_col::text;

-- mixed hashable/normal cases
select normal_col, unsortable_col,
       grouping(normal_col, unsortable_col),
       count(*), sum(v)
  from gstest4 group by grouping sets ((normal_col),(unsortable_col))
 order by 3, 5;
explain (costs off)
  select normal_col, unsortable_col,
         grouping(normal_col, unsortable_col),
         count(*), sum(v)
    from gstest4 group by grouping sets ((normal_col),(unsortable_col))
   order by 3,5;

select normal_col, unsortable_col,
       grouping(normal_col, unsortable_col),
       count(*), sum(v)
  from gstest4 group by grouping sets ((v,normal_col),(v,unsortable_col))
 order by 3,5;
explain (costs off)
  select normal_col, unsortable_col,
         grouping(normal_col, unsortable_col),
         count(*), sum(v)
    from gstest4 group by grouping sets ((v,normal_col),(v,unsortable_col))
   order by 3,5;

-- empty input: first is 0 rows, second 1, third 3 etc.
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),a);
explain (costs off)
  select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),a);
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),());
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),(),(),());
explain (costs off)
  select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),(),(),());
select sum(v), count(*) from gstest_empty group by grouping sets ((),(),());
explain (costs off)
  select sum(v), count(*) from gstest_empty group by grouping sets ((),(),());

-- check that functionally dependent cols are not nulled
select a, d, grouping(a,b,c)
  from gstest3
 group by grouping sets ((a,b), (a,c)) order by 1,2,3;
explain (costs off)
  select a, d, grouping(a,b,c)
    from gstest3
   group by grouping sets ((a,b), (a,c));

-- simple rescan tests

select a, b, sum(v.x)
  from (values (1),(2)) v(x), gstest_data(v.x)
 group by grouping sets (a,b)
 order by 1, 2, 3;
explain (costs off)
  select a, b, sum(v.x)
    from (values (1),(2)) v(x), gstest_data(v.x)
   group by grouping sets (a,b)
   order by 3, 1, 2;
select *
  from (values (1),(2)) v(x),
       lateral (select a, b, sum(v.x) from gstest_data(v.x) group by grouping sets (a,b)) s;
explain (costs off)
  select *
    from (values (1),(2)) v(x),
         lateral (select a, b, sum(v.x) from gstest_data(v.x) group by grouping sets (a,b)) s;

-- Tests for chained aggregates
select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by grouping sets ((a,b),(a+1,b+1),(a+2,b+2)) order by 3,6;
explain (costs off)
  select a, b, grouping(a,b), sum(v), count(*), max(v)
    from gstest1 group by grouping sets ((a,b),(a+1,b+1),(a+2,b+2)) order by 3,6;
select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
  from gstest2 group by cube (a,b) order by rsum, a, b;
explain (costs off)
  select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
    from gstest2 group by cube (a,b) order by rsum, a, b;
select a, b, sum(v.x)
  from (values (1),(2)) v(x), gstest_data(v.x)
 group by cube (a,b) order by a,b;
explain (costs off)
  select a, b, sum(v.x)
    from (values (1),(2)) v(x), gstest_data(v.x)
   group by cube (a,b) order by a,b;

-- More rescan tests
select * from (values (1),(2)) v(a) left join lateral (select v.a, four, ten, count(*) from onek group by cube(four,ten)) s on true order by v.a,four,ten;
select array(select row(v.a,s1.*) from (select two,four, count(*) from onek group by cube(two,four) order by two,four) s1) from (values (1),(2)) v(a);

-- Rescan logic changes when there are no empty grouping sets, so test
-- that too:
select * from (values (1),(2)) v(a) left join lateral (select v.a, four, ten, count(*) from onek group by grouping sets(four,ten)) s on true order by v.a,four,ten;
select array(select row(v.a,s1.*) from (select two,four, count(*) from onek group by grouping sets(two,four) order by two,four) s1) from (values (1),(2)) v(a);

-- test the knapsack

set enable_indexscan = false;
set work_mem = '64kB';
explain (costs off)
  select unique1,
         count(two), count(four), count(ten),
         count(hundred), count(thousand), count(twothousand),
         count(*)
    from tenk1 group by grouping sets (unique1,twothousand,thousand,hundred,ten,four,two);
explain (costs off)
  select unique1,
         count(two), count(four), count(ten),
         count(hundred), count(thousand), count(twothousand),
         count(*)
    from tenk1 group by grouping sets (unique1,hundred,ten,four,two);

set work_mem = '384kB';
explain (costs off)
  select unique1,
         count(two), count(four), count(ten),
         count(hundred), count(thousand), count(twothousand),
         count(*)
    from tenk1 group by grouping sets (unique1,twothousand,thousand,hundred,ten,four,two);

-- check collation-sensitive matching between grouping expressions
-- (similar to a check for aggregates, but there are additional code
-- paths for GROUPING, so check again here)

select v||'a', case grouping(v||'a') when 1 then 1 else 0 end, count(*)
  from unnest(array[1,1], array['a','b']) u(i,v)
 group by rollup(i, v||'a') order by 1,3;
select v||'a', case when grouping(v||'a') = 1 then 1 else 0 end, count(*)
  from unnest(array[1,1], array['a','b']) u(i,v)
 group by rollup(i, v||'a') order by 1,3;

-- Bug #16784
CREATE TABLE bug_16784(i INT, j INT);
ANALYZE bug_16784;
ALTER TABLE bug_16784 SET (autovacuum_enabled = 'false');
UPDATE pg_class SET reltuples = 10 WHERE relname='bug_16784';

INSERT INTO bug_16784 SELECT g/10, g FROM generate_series(1,40) g;

SET work_mem='64kB';

explain (costs off) select * from
  (values (1),(2)) v(a),
  lateral (select v.a, i, j, count(*) from
             bug_16784 group by cube(i,j)) s
  order by v.a, i, j;
select * from
  (values (1),(2)) v(a),
  lateral (select a, i, j, count(*) from
             bug_16784 group by cube(i,j)) s
  order by v.a, i, j;

--
-- Compare results between plans using sorting and plans using hash
-- aggregation. Force spilling in both cases by setting work_mem low
-- and altering the statistics.
--

create table gs_data_1 as
select g%1000 as g1000, g%100 as g100, g%10 as g10, g
   from generate_series(0,1999) g;

analyze gs_data_1;
alter table gs_data_1 set (autovacuum_enabled = 'false');
update pg_class set reltuples = 10 where relname='gs_data_1';

SET work_mem='64kB';

-- Produce results with sorting.

set enable_hashagg = false;
set jit_above_cost = 0;

explain (costs off)
select g100, g10, sum(g::numeric), count(*), max(g::text)
from gs_data_1 group by cube (g1000, g100,g10);

create table gs_group_1 as
select g100, g10, sum(g::numeric), count(*), max(g::text)
from gs_data_1 group by cube (g1000, g100,g10);

-- Produce results with hash aggregation.

set enable_hashagg = true;
set enable_sort = false;

explain (costs off)
select g100, g10, sum(g::numeric), count(*), max(g::text)
from gs_data_1 group by cube (g1000, g100,g10);

create table gs_hash_1 as
select g100, g10, sum(g::numeric), count(*), max(g::text)
from gs_data_1 group by cube (g1000, g100,g10);

set enable_sort = true;
set work_mem to default;

-- Compare results

(select * from gs_hash_1 except select * from gs_group_1)
  union all
(select * from gs_group_1 except select * from gs_hash_1);

drop table gs_group_1;
drop table gs_hash_1;

-- Check plan with group_optimizer
-- normal output target
explain (costs off, verbose) select g1000, g100 from gs_data_1 group by rollup(g1000, g100);
-- output with grouping func + expr
explain (costs off, verbose) select g1000, g100, grouping(g1000, g100) * 2 from gs_data_1 group by rollup(g1000, g100);
-- output with agg
explain (costs off, verbose) select g1000, g100, sum(g100), avg(g10) from gs_data_1 group by rollup(g1000, g100);
-- output with agg + grouping func + expr
explain (costs off, verbose) select g1000, g100, sum(g100), avg(g10), grouping(g1000, g100) * 2 from gs_data_1 group by rollup(g1000, g100);

-- OPENTENBASE
-- mixed hashable/sortable cases
select unhashable_col, unsortable_col,
       grouping(unhashable_col, unsortable_col),
       count(*), sum(v)
  from gstest5 group by grouping sets ((unhashable_col),(unsortable_col))
 order by 3, 5;
explain (costs off)
  select unhashable_col, unsortable_col,
         grouping(unhashable_col, unsortable_col),
         count(*), sum(v)
    from gstest5 group by grouping sets ((unhashable_col),(unsortable_col))
   order by 3,5;

select unhashable_col, unsortable_col,
       grouping(unhashable_col, unsortable_col),
       count(*), sum(v)
  from gstest5 group by grouping sets ((v,unhashable_col),(v,unsortable_col))
 order by 3,5;
explain (costs off)
  select unhashable_col, unsortable_col,
         grouping(unhashable_col, unsortable_col),
         count(*), sum(v)
    from gstest5 group by grouping sets ((v,unhashable_col),(v,unsortable_col))
   order by 3,5;


-- grouping func with share ctescan
create table grouping_cte (c1 int, t2c2 numeric, c2 numeric, t2c1 int, c3 numeric);
insert into grouping_cte values(10, 505.5, 505.5, 10, 48562.84);
explain (costs off) select grouping(c1,c2,t2c1,t2c2, c3), c1, c2, t2c2, t2c1,c3 from grouping_cte group by cube ((c1, t2c2, c2, t2c1), c3, sin(1)) order by 1,2,3;
select grouping(c1,c2,t2c1,t2c2, c3), c1, c2, t2c2, t2c1,c3 from grouping_cte group by cube ((c1, t2c2, c2, t2c1), c3, sin(1)) order by 1,2,3;
set parallel_tuple_cost to 0;
set parallel_setup_cost to 0;
set min_parallel_table_scan_size to 0;
explain (costs off) select grouping(c1,c2,t2c1,t2c2, c3), c1, c2, t2c2, t2c1,c3 from grouping_cte group by cube ((c1, t2c2, c2, t2c1), c3, sin(1)) order by 1,2,3;
select grouping(c1,c2,t2c1,t2c2, c3), c1, c2, t2c2, t2c1,c3 from grouping_cte group by cube ((c1, t2c2, c2, t2c1), c3, sin(1)) order by 1,2,3;
reset parallel_tuple_cost;
reset parallel_setup_cost;
reset min_parallel_table_scan_size;
drop table grouping_cte;


drop view gstest1;
drop view gstest_view;
drop table gstest2;
drop table gstest3;
drop table gstest4;
drop table gstest5;
drop table gstest_empty;
drop function gstest_data;
drop table bug_16784;
drop table gs_data_1;

reset enable_hashagg;
select 'test gs' from test_gs_empty group by cube(a);
select 'test gs' from test_gs_empty group by rollup(a);
select 'test gs' from test_gs_empty group by (a);
select 'test gs' from test_gs_empty group by ();
-- opentenbase_ora return 0 rows, opentenbase return 1 rows
select 'test gs with agg', count(*) from test_gs_empty group by cube(a);
-- opentenbase_ora return 0 rows, opentenbase return 1 rows
select 'test gs with agg', count(*) from test_gs_empty group by rollup(a);
select 'test gs with agg', count(*) from test_gs_empty group by (a);
select 'test gs with agg', count(*) from test_gs_empty group by ();

insert into test_gs_empty values(1, 1);
insert into test_gs_empty values(2, 2);
insert into test_gs_empty values(3, 3);
select a from test_gs_empty group by cube(a) order by 1;
select a from test_gs_empty group by rollup(a) order by 1;
select a from test_gs_empty group by (a) order by 1;
select 'test' from test_gs_empty group by () order by 1;
-- opentenbase_ora return 0 rows, opentenbase return 1 rows
select a, count(*) from test_gs_empty group by cube(a) order by 1;
-- opentenbase_ora return 0 rows, opentenbase return 1 rows
select a, count(*) from test_gs_empty group by rollup(a) order by 1;
select a, count(*) from test_gs_empty group by (a) order by 1;
select 'test', count(*) from test_gs_empty group by () order by 1;

drop table test_gs_empty;

drop table if exists gscte1, gscte2;
create table gscte1(c0 int, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int);
create table gscte2(c0 int, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int)distribute by replication;

explain (costs off)
select c2, count(c1) from gscte1 t1 where c5 in (select avg(c5) from gscte2 group by rollup(c3,c4)) group by rollup(c2, c4);
drop table if exists gscte1, gscte2;
