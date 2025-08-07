--
-- SUBSELECT
--

SELECT 1 AS one WHERE 1 IN (SELECT 1);

SELECT 1 AS zero WHERE 1 NOT IN (SELECT 1);

SELECT 1 AS zero WHERE 1 IN (SELECT 2);

-- Check grammar's handling of extra parens in assorted contexts

SELECT * FROM (SELECT 1 AS x) ss;
SELECT * FROM ((SELECT 1 AS x)) ss;

(SELECT 2) UNION SELECT 2;
((SELECT 2)) UNION SELECT 2;

SELECT ((SELECT 2) UNION SELECT 2);
SELECT (((SELECT 2)) UNION SELECT 2);

SELECT (SELECT ARRAY[1,2,3])[1];
SELECT ((SELECT ARRAY[1,2,3]))[2];
SELECT (((SELECT ARRAY[1,2,3])))[3];

-- Set up some simple test tables

CREATE TABLE SUBSELECT_TBL (
  f1 integer,
  f2 integer,
  f3 float
);

INSERT INTO SUBSELECT_TBL VALUES (1, 2, 3);
INSERT INTO SUBSELECT_TBL VALUES (2, 3, 4);
INSERT INTO SUBSELECT_TBL VALUES (3, 4, 5);
INSERT INTO SUBSELECT_TBL VALUES (1, 1, 1);
INSERT INTO SUBSELECT_TBL VALUES (2, 2, 2);
INSERT INTO SUBSELECT_TBL VALUES (3, 3, 3);
INSERT INTO SUBSELECT_TBL VALUES (6, 7, 8);
INSERT INTO SUBSELECT_TBL VALUES (8, 9, NULL);

SELECT '' AS eight, * FROM SUBSELECT_TBL ORDER BY f1, f2, f3;

-- Uncorrelated subselects

SELECT '' AS two, f1 AS "Constant Select" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT 1) ORDER BY 2;

SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL) 
  ORDER BY 2;

SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE
    f2 IN (SELECT f1 FROM SUBSELECT_TBL)) 
    ORDER BY 2;

SELECT '' AS three, f1, f2
  FROM SUBSELECT_TBL
  WHERE (f1, f2) NOT IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                         WHERE f3 IS NOT NULL) 
                         ORDER BY f1, f2;

-- Correlated subselects

SELECT '' AS six, f1 AS "Correlated Field", f2 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE f1 = upper.f1) 
  ORDER BY f1, f2;

SELECT '' AS six, f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN
    (SELECT f2 FROM SUBSELECT_TBL WHERE CAST(upper.f2 AS float) = f3)
    ORDER BY 2, 3;

SELECT '' AS six, f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f3 IN (SELECT upper.f1 + f2 FROM SUBSELECT_TBL
               WHERE f2 = CAST(f3 AS integer)) 
               ORDER BY 2, 3;

SELECT '' AS five, f1 AS "Correlated Field"
  FROM SUBSELECT_TBL
  WHERE (f1, f2) IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                     WHERE f3 IS NOT NULL) 
                     ORDER BY 2;

--
-- Use some existing tables in the regression test
--

SELECT '' AS eight, ss.f1 AS "Correlated Field", ss.f3 AS "Second Field"
  FROM SUBSELECT_TBL ss
  WHERE f1 NOT IN (SELECT f1+1 FROM INT4_TBL
                   WHERE f1 != ss.f1 AND f1 < 2147483647) 
                   ORDER BY 2, 3;

--
-- Tests for CTE inlining behavior
--

-- Basic subquery that can be inlined
explain (verbose, costs off)
with x as (select * from (select f1 from subselect_tbl) ss)
select * from x where f1 = 1;

-- Explicitly request materialization
explain (verbose, costs off)
with x as materialized (select * from (select f1 from subselect_tbl) ss)
select * from x where f1 = 1;

-- Stable functions are safe to inline
explain (verbose, costs off)
with x as (select * from (select f1, now() from subselect_tbl) ss)
select * from x where f1 = 1;

-- Volatile functions prevent inlining
explain (verbose, costs off)
with x as (select * from (select f1, random() from subselect_tbl) ss)
select * from x where f1 = 1;

-- SELECT FOR UPDATE cannot be inlined
explain (verbose, costs off)
with x as (select * from (select f1 from subselect_tbl for update) ss)
select * from x where f1 = 1;

-- Multiply-referenced CTEs are inlined only when requested
explain (verbose, costs off)
with x as (select * from (select f1, now() as n from subselect_tbl) ss)
select * from x, x x2 where x.n = x2.n;

explain (verbose, costs off)
with x as not materialized (select * from (select f1, now() as n from subselect_tbl) ss)
select * from x, x x2 where x.n = x2.n;

-- Multiply-referenced CTEs can't be inlined if they contain outer self-refs
explain (verbose, costs off)
with recursive x(a) as
  ((values ('a'), ('b'))
   union all
   (with z as not materialized (select * from x)
    select z.a || z1.a as a from z cross join z as z1
    where length(z.a || z1.a) < 5))
select * from x;

with recursive x(a) as
  ((values ('a'), ('b'))
   union all
   (with z as not materialized (select * from x)
    select z.a || z1.a as a from z cross join z as z1
    where length(z.a || z1.a) < 5))
select * from x;

explain (verbose, costs off)
with recursive x(a) as
  ((values ('a'), ('b'))
   union all
   (with z as not materialized (select * from x)
    select z.a || z.a as a from z
    where length(z.a || z.a) < 5))
select * from x;

with recursive x(a) as
  ((values ('a'), ('b'))
   union all
   (with z as not materialized (select * from x)
    select z.a || z.a as a from z
    where length(z.a || z.a) < 5))
select * from x;

-- Check handling of outer references
explain (verbose, costs off)
with x as (select * from int4_tbl)
select * from (with y as (select * from x) select * from y) ss;

explain (verbose, costs off)
with x as materialized (select * from int4_tbl)
select * from (with y as (select * from x) select * from y) ss;

-- Ensure that we inline the currect CTE when there are
-- multiple CTEs with the same name
explain (verbose, costs off)
with x as (select 1 as y)
select * from (with x as (select 2 as y) select * from x) ss;

-- Row marks are not pushed into CTEs
explain (verbose, costs off)
with x as (select * from subselect_tbl)
select * from x for update;


select q1, float8(count(*)) / (select count(*) from int8_tbl)
from int8_tbl group by q1 order by q1;

-- Unspecified-type literals in output columns should resolve as text

SELECT *, pg_typeof(f1) FROM
  (SELECT 'foo' AS f1 FROM generate_series(1,3)) ss ORDER BY 1;

-- ... unless there's context to suggest differently

explain verbose select '42' union all select '43';
explain verbose select '42' union all select 43;

-- check materialization of an initplan reference (bug #14524)
explain (verbose, costs off)
select 1 = all (select (select 1));
select 1 = all (select (select 1));

--
-- Check EXISTS simplification with LIMIT
--
explain (costs off)
select * from int4_tbl o where exists
  (select 1 from int4_tbl i where i.f1=o.f1 limit null);
explain (costs off, nodes off)
select * from int4_tbl o where not exists
  (select 1 from int4_tbl i where i.f1=o.f1 limit 1);
explain (costs off, nodes off)
select * from int4_tbl o where exists
  (select 1 from int4_tbl i where i.f1=o.f1 limit 0);

--
-- Test cases to catch unpleasant interactions between IN-join processing
-- and subquery pullup.
--

select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;
select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;

--
-- Test cases to check for overenthusiastic optimization of
-- "IN (SELECT DISTINCT ...)" and related cases.  Per example from
-- Luca Pireddu and Michael Fuhr.
--

CREATE TEMP TABLE foo (id integer);
CREATE TEMP TABLE bar (id1 integer, id2 integer);

INSERT INTO foo VALUES (1);

INSERT INTO bar VALUES (1, 1);
INSERT INTO bar VALUES (2, 2);
INSERT INTO bar VALUES (3, 1);

-- These cases require an extra level of distinct-ing above subquery s
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT id1, id2 FROM bar) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id1,id2 FROM bar GROUP BY id1,id2) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id1, id2 FROM bar UNION
                      SELECT id1, id2 FROM bar) AS s);

-- These cases do not
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT ON (id2) id1, id2 FROM bar) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM bar GROUP BY id2) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM bar UNION
                      SELECT id2 FROM bar) AS s);

--
-- Test case to catch problems with multiply nested sub-SELECTs not getting
-- recalculated properly.  Per bug report from Didier Moens.
--

CREATE TABLE orderstest (
    approver_ref integer,
    po_ref integer,
    ordercanceled boolean
);

INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 5, false);
INSERT INTO orderstest VALUES (66, 6, false);
INSERT INTO orderstest VALUES (66, 7, false);
INSERT INTO orderstest VALUES (66, 1, true);
INSERT INTO orderstest VALUES (66, 8, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (77, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);

CREATE VIEW orders_view AS
SELECT *,
(SELECT CASE
   WHEN ord.approver_ref=1 THEN '---' ELSE 'Approved'
 END) AS "Approved",
(SELECT CASE
 WHEN ord.ordercanceled
 THEN 'Canceled'
 ELSE
  (SELECT CASE
		WHEN ord.po_ref=1
		THEN
		 (SELECT CASE
				WHEN ord.approver_ref=1
				THEN '---'
				ELSE 'Approved'
			END)
		ELSE 'PO'
	END)
END) AS "Status",
(CASE
 WHEN ord.ordercanceled
 THEN 'Canceled'
 ELSE
  (CASE
		WHEN ord.po_ref=1
		THEN
		 (CASE
				WHEN ord.approver_ref=1
				THEN '---'
				ELSE 'Approved'
			END)
		ELSE 'PO'
	END)
END) AS "Status_OK"
FROM orderstest ord;

SELECT * FROM orders_view 
ORDER BY approver_ref, po_ref, ordercanceled;

DROP TABLE orderstest cascade;

--
-- Test cases to catch situations where rule rewriter fails to propagate
-- hasSubLinks flag correctly.  Per example from Kyle Bateman.
--

create temp table parts (
    partnum     text,
    cost        float8
);

create temp table shipped (
    ttype       char(2),
    ordnum      int4,
    partnum     text,
    value       float8
);

create temp view shipped_view as
    select * from shipped where ttype = 'wt';

create rule shipped_view_insert as on insert to shipped_view do instead
    insert into shipped values('wt', new.ordnum, new.partnum, new.value);

insert into parts (partnum, cost) values (1, 1234.56);

insert into shipped_view (ordnum, partnum, value)
    values (0, 1, (select cost from parts where partnum = '1'));

select * from shipped_view;

create rule shipped_view_update as on update to shipped_view do instead
    update shipped set partnum = new.partnum, value = new.value
        where ttype = new.ttype and ordnum = new.ordnum;

update shipped_view set value = 11
    from int4_tbl a join int4_tbl b
      on (a.f1 = (select f1 from int4_tbl c where c.f1=b.f1))
    where ordnum = a.f1;

select * from shipped_view;

select f1, ss1 as relabel from
    (select *, (select sum(f1) from int4_tbl b where f1 >= a.f1) as ss1
     from int4_tbl a) ss 
     ORDER BY f1, relabel;

explain(verbose, costs off)
select * from
(values
  (3 not in (select * from (values (1), (2)) ss1)),
  (false)
) ss;

select * from
(values
  (3 not in (select * from (values (1), (2)) ss1)),
  (false)
) ss;

--
-- Test cases involving PARAM_EXEC parameters and min/max index optimizations.
-- Per bug report from David Sanchez i Gregori.
--

select * from (
  select max(unique1) from tenk1 as a
  where exists (select 1 from tenk1 as b where b.thousand = a.unique2)
) ss;

select * from (
  select min(unique1) from tenk1 as a
  where not exists (select 1 from tenk1 as b where b.unique2 = 10000)
) ss;

--
-- Test that an IN implemented using a UniquePath does unique-ification
-- with the right semantics, as per bug #4113.  (Unfortunately we have
-- no simple way to ensure that this test case actually chooses that type
-- of plan, but it does in releases 7.4-8.3.  Note that an ordering difference
-- here might mean that some other plan type is being used, rendering the test
-- pointless.)
--

create temp table numeric_table (num_col numeric);
insert into numeric_table values (1), (1.000000000000000000001), (2), (3);

create temp table float_table (float_col float8);
insert into float_table values (1), (2), (3);

select * from float_table
  where float_col in (select num_col from numeric_table) 
  ORDER BY float_col;

select * from numeric_table
  where num_col in (select float_col from float_table) 
  ORDER BY num_col;

--
-- Test case for bug #4290: bogus calculation of subplan param sets
--

create temp table ta (id int primary key, val int);

insert into ta values(1,1);
insert into ta values(2,2);

create temp table tb (id int primary key, aval int);

insert into tb values(1,1);
insert into tb values(2,1);
insert into tb values(3,2);
insert into tb values(4,2);

create temp table tc (id int primary key, aid int);

insert into tc values(1,1);
insert into tc values(2,2);

select
  ( select min(tb.id) from tb
    where tb.aval = (select ta.val from ta where ta.id = tc.aid) ) as min_tb_id
from tc 
ORDER BY min_tb_id;

--
-- Test case for 8.3 "failed to locate grouping columns" bug
--

create temp table t1 (f1 numeric(14,0), f2 varchar(30));

select * from
  (select distinct f1, f2, (select f2 from t1 x where x.f1 = up.f1) as fs
   from t1 up) ss
group by f1,f2,fs;

--
-- Test case for bug #5514 (mishandling of whole-row Vars in subselects)
--

create temp table table_a(id integer);
insert into table_a values (42);

create temp view view_a as select * from table_a;

select view_a from view_a;
select (select view_a) from view_a;
select (select (select view_a)) from view_a;
select (select (a.*)::text) from view_a a;

--
-- Check that whole-row Vars reading the result of a subselect don't include
-- any junk columns therein
--

select q from (select max(f1) from int4_tbl group by f1 order by f1) q;
with q as (select max(f1) from int4_tbl group by f1 order by f1)
  select q from q;

--
-- Test case for sublinks pushed down into subselects via join alias expansion
--

select
  (select sq1) as qq1
from
  (select exists(select 1 from int4_tbl where f1 = q2) as sq1, 42 as dummy
   from int8_tbl) sq0
  join
  int4_tbl i4 on dummy = i4.f1;

--
-- Test case for subselect within UPDATE of INSERT...ON CONFLICT DO UPDATE
--
create temp table upsert(key int4 primary key, val text) distribute by replication;
insert into upsert values(1, 'val') on conflict (key) do update set val = 'not seen';
insert into upsert values(1, 'val') on conflict (key) do update set val = 'seen with subselect ' || (select f1 from int4_tbl where f1 != 0 limit 1)::text;

select * from upsert;

with aa as (select 'int4_tbl' u from int4_tbl limit 1)
insert into upsert values (1, 'x'), (999, 'y')
on conflict (key) do update set val = (select u from aa)
returning *;

--
-- Test case for cross-type partial matching in hashed subplan (bug #7597)
--

create temp table outer_7597 (f1 int4, f2 int4);
insert into outer_7597 values (0, 0);
insert into outer_7597 values (1, 0);
insert into outer_7597 values (0, null);
insert into outer_7597 values (1, null);

create temp table inner_7597(c1 int8, c2 int8);
insert into inner_7597 values(0, null);

select * from outer_7597 where (f1, f2) not in (select * from inner_7597) order by 1;

--
-- Test case for premature memory release during hashing of subplan output
--

select '1'::text in (select '1'::name union all select '1'::name);

--
-- Test case for planner bug with nested EXISTS handling
--
select a.thousand from tenk1 a, tenk1 b
where a.thousand = b.thousand
  and exists ( select 1 from tenk1 c where b.hundred = c.hundred
                   and not exists ( select 1 from tenk1 d
                                    where a.thousand = d.thousand ) );

--
-- Check that nested sub-selects are not pulled up if they contain volatiles
--
explain (verbose, costs off)
  select x, x from
    (select (select now()) as x from (values(1),(2)) v(y)) ss;
explain (verbose, costs off)
  select x, x from
    (select (select random()) as x from (values(1),(2)) v(y)) ss;
explain (verbose, costs off)
  select x, x from
    (select (select now() where y=y) as x from (values(1),(2)) v(y)) ss;
explain (verbose, costs off)
  select x, x from
    (select (select random() where y=y) as x from (values(1),(2)) v(y)) ss;

--
-- Check we behave sanely in corner case of empty SELECT list (bug #8648)
--
create temp table nocolumns();
select exists(select * from nocolumns);

--
-- Check sane behavior with nested IN SubLinks
--
set enable_indexonlyscan to off;
explain (verbose, costs off)
select * from int4_tbl where
  (case when f1 in (select unique1 from tenk1 a) then f1 else null end) in
  (select ten from tenk1 b);
select * from int4_tbl where
  (case when f1 in (select unique1 from tenk1 a) then f1 else null end) in
  (select ten from tenk1 b);
reset enable_indexonlyscan;

--
-- Check for incorrect optimization when IN subquery contains a SRF
--
explain (verbose, costs off)
select * from int4_tbl o where (f1, f1) in
  (select f1, generate_series(1,2) / 10 g from int4_tbl i group by f1);
select * from int4_tbl o where (f1, f1) in
  (select f1, generate_series(1,2) / 10 g from int4_tbl i group by f1);

--
-- check for over-optimization of whole-row Var referencing an Append plan
--
select (select q from
         (select 1,2,3 where f1 > 0
          union all
          select 4,5,6.0 where f1 <= 0
         ) q )
from int4_tbl order by 1;

--
-- Check that volatile quals aren't pushed down past a DISTINCT:
-- nextval() should not be called more than the nominal number of times
--
create temp sequence ts1;

select * from
  (select distinct ten from tenk1) ss
  where ten < 10 + nextval('ts1')
  order by 1;

select nextval('ts1');

SELECT setseed(0);

-- DROP TABLE IF EXISTS asd ;

CREATE TABLE IF NOT EXISTS asd  AS
SELECT clientid::numeric(20),
 (clientid / 20 )::integer::numeric(20) as userid,
 cts + ((random()* 3600 *24 )||'sec')::interval as cts,
 (ARRAY['A','B','C','D','E','F'])[(random()*5+1)::integer] as state,
 0 as dim,
 ((ARRAY['Cat','Dog','Duck'])[(clientid / 10  )% 3  +1 ]) ::text as app_name,
 ((ARRAY['A','B'])[(clientid / 10  )% 2  +1 ]) ::text as platform
 FROM generate_series('2016-01-01'::timestamp,'2016-10-01'::timestamp,interval '15 day') cts , generate_series( 1000,2000,10) clientid , generate_series(1,6) t
;

SELECT dates::timestamp as dates ,B.platform,B.app_name, B.clientid, B.userid,
	B.state as state
FROM ( VALUES
('2016.08.30. 08:52:43') ,('2016.08.29. 04:57:12') ,('2016.08.26. 08:15:05') ,
('2016.08.24. 11:49:51') ,('2016.08.22. 08:45:29') ,('2016.08.21. 04:53:47') ,('2016.08.20. 08:44:03')
) AS D (dates)
JOIN
( SELECT DISTINCT clientid FROM asd
	WHERE userid=74 ) C ON True
INNER JOIN LATERAL (
	SELECT DISTINCT ON (clientid,app_name,platform,state,dim) x.*
	FROM asd x
	INNER JOIN (SELECT p.clientid,p.app_name,p.platform , p.state, p.dim ,
	     MAX(p.cts) AS selected_cts
		FROM asd p
		where cts<D.dates::timestamp and state in
		('A','B')
	GROUP BY p.clientid,p.app_name,p.platform,p.state,p.dim) y
	ON y.clientid = x.clientid
	AND y.selected_cts = x.cts
	AND y.platform = x.platform
	AND y.app_name=x.app_name
	AND y.state=x.state
	AND y.dim = x.dim
	and x.clientid = C.clientid
) B ON True
ORDER BY dates desc, state, B.platform,B.app_name, B.clientid, B.userid;

DROP TABLE asd;
SELECT setseed(0);
--
-- Check that volatile quals aren't pushed down past a set-returning function;
-- while a nonvolatile qual can be, if it doesn't reference the SRF.
--
create function tattle(x int, y int) returns bool
volatile language plpgsql as $$
begin
  raise notice 'x = %, y = %', x, y;
  return x > y;
end$$;

explain (verbose, costs off)
select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, 8);

select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, 8);

-- if we pretend it's stable, we get different results:
alter function tattle(x int, y int) stable;

explain (verbose, costs off)
select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, 8);

select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, 8);

-- although even a stable qual should not be pushed down if it references SRF
explain (verbose, costs off)
select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, u);

select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, u);

drop function tattle(x int, y int);

-- test subquery pathkey
CREATE TABLE catalog_sales (
    cs_sold_date_sk integer,
    cs_item_sk integer NOT NULL,
    cs_order_number integer NOT NULL
);
CREATE TABLE catalog_returns (
    cr_returned_date_sk integer,
    cr_item_sk integer NOT NULL,
    cr_order_number integer NOT NULL
);
CREATE TABLE date_dim (
    d_date_sk integer NOT NULL,
    d_year integer
);
with cs as
(
    select d_year AS cs_sold_year, cs_item_sk
    from catalog_sales
        left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
        join date_dim on cs_sold_date_sk = d_date_sk
    order by d_year, cs_item_sk
)
select 1
from date_dim
    join cs on (cs_sold_year=d_year and cs_item_sk=cs_item_sk);
drop table catalog_sales, catalog_returns, date_dim;

--
-- Test that LIMIT can be pushed to SORT through a subquery that just projects
-- columns.  We check for that having happened by looking to see if EXPLAIN
-- ANALYZE shows that a top-N sort was used.  We must suppress or filter away
-- all the non-invariant parts of the EXPLAIN ANALYZE output.
--
create table sq_limit (pk int primary key, c1 int, c2 int) distribute by replication;
insert into sq_limit values
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3),
    (4, 4, 4),
    (5, 1, 1),
    (6, 2, 2),
    (7, 3, 3),
    (8, 4, 4);

create function explain_sq_limit() returns setof text language plpgsql as
$$
declare ln text;
begin
    for ln in
        explain (analyze, summary off, timing off, costs off)
        select * from (select pk,c2 from sq_limit order by c1,pk) as x limit 3
    loop
        ln := regexp_replace(ln, 'Memory: \S*',  'Memory: xxx');
        return next ln;
    end loop;
end;
$$;

/* Unfortunately, we use quicksort but not top-N sort. Should be optimized later.
select * from explain_sq_limit();
*/

select * from (select pk,c2 from sq_limit order by c1,pk) as x limit 3;

drop function explain_sq_limit();

drop table sq_limit;

--
-- Ensure that backward scan direction isn't propagated into
-- expression subqueries (bug #15336)
--

begin;

declare c1 scroll cursor for
 select * from generate_series(1,4) i
  where i <> all (values (2),(3));

move forward all in c1;
fetch backward all in c1;

commit;

-- OPENTENBASE
-- not in optimization
create table notin_t1 (id1 int, num1 int not null);
create table notin_t2 (id2 int, num2 int not null);
explain(costs off) select num1 from notin_t1 where num1 not in (select num2 from notin_t2);
drop table notin_t1;
drop table notin_t2;

create table tbl_a(a int,b int);
create table tbl_b(a int,b int);
insert into tbl_a select generate_series(1,10),1 ;
insert into tbl_b select generate_series(2,11),1 ;

-- check targetlist subquery scenario.
set enable_nestloop to true;
set enable_hashjoin to false;
set enable_mergejoin to false;
explain (costs off) select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;

set enable_nestloop to false;
set enable_hashjoin to true;
set enable_mergejoin to false;
explain (costs off) select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;

set enable_nestloop to false;
set enable_hashjoin to false;
set enable_mergejoin to true;
explain (costs off) select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;

-- check non-scalar scenario.
insert into tbl_b values(2,2);

set enable_nestloop to true;
set enable_hashjoin to false;
set enable_mergejoin to false;
explain (costs off) select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;

set enable_nestloop to false;
set enable_hashjoin to true;
set enable_mergejoin to false;
explain (costs off) select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;

set enable_nestloop to false;
set enable_hashjoin to false;
set enable_mergejoin to true;
explain (costs off) select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;

explain (costs off) select a.a,(select b.a from tbl_b b where b.a = a.a and b.a = 5) q from tbl_a a order by 1,2;
select a.a,(select b.a from tbl_b b where b.a = a.a and b.a = 5) q from tbl_a a order by 1,2;

-- check distinct scenario.
set enable_nestloop to true;
set enable_hashjoin to false;
set enable_mergejoin to false;
explain (costs  off) select a.a,(select distinct b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
select a.a,(select distinct b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;

set enable_nestloop to false;
set enable_hashjoin to true;
set enable_mergejoin to false;
explain (costs off) select a.a,(select distinct b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
select a.a,(select distinct b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;

set enable_nestloop to false;
set enable_hashjoin to false;
set enable_mergejoin to true;
explain (costs  off) select a.a,(select distinct b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
select a.a,(select distinct b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;

set enable_nestloop to true;
set enable_hashjoin to true;
set enable_mergejoin to true;

drop table tbl_a;
drop table tbl_b;

create table ss_test1
(   c11 integer,
    c12 integer,
    c13 integer,
    c14 integer,
    c15 integer
);

create table ss_test2
(   c21 integer,
    c22 integer,
    c23 integer,
    c24 integer,
    c25 integer
);

explain (COSTS OFF)
select * from ss_test1
where c11=1 or exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22);

explain (COSTS OFF)
select * from ss_test1
where c11=1 or ss_test1.c12 IN (select ss_test2.c23 from ss_test2);


explain (COSTS OFF)
select * from ss_test1
where c11=1 or (c12=2 and exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22));


explain (COSTS OFF)
select * from ss_test1
where (c11=1 and exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22))
   or (c12=2 and exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22));

explain (COSTS OFF)
select * from ss_test1
where (c11=1 and ss_test1.c12 IN (select ss_test2.c23 from ss_test2))
   or (c12=2 and exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22));

explain (COSTS OFF)
select * from ss_test1
where c11=1 or not exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22);

explain (COSTS OFF)
select * from ss_test1
where c11=1 or ss_test1.c12 not IN (select ss_test2.c23 from ss_test2);


explain (COSTS OFF)
select * from ss_test1
where c11=1 or (c12=2 and not exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22));


explain (COSTS OFF)
select * from ss_test1
where (c11=1 and not exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22))
   or (c12=2 and exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22));

explain (COSTS OFF)
select * from ss_test1
where (c11=1 and ss_test1.c12 not IN (select ss_test2.c23 from ss_test2))
   or (c12=2 and exists (select ss_test2.c23, ss_test2.c24
                       from ss_test2 where ss_test1.c12=ss_test2.c22));

explain (COSTS OFF)
select c11 from ss_test1
      where case
            when ss_test1.c11=1 THEN 1
            when ss_test1.c11=2 THEN 2
            when exists (select ss_test2.c23, ss_test2.c24
                         from ss_test2 where ss_test1.c12=ss_test2.c22) THEN ss_test1.c11
            END = 1;

explain (COSTS OFF)
select c11 from ss_test1
      where case
            when ss_test1.c11=1 THEN 1
            when ss_test1.c11=2 THEN 2
            when ss_test1.c12 IN (select ss_test2.c22
                         from ss_test2) THEN ss_test1.c11
            END = 1;

explain (COSTS OFF)
select c11 from ss_test1
      where case
            when ss_test1.c11=1 THEN 1
            when ss_test1.c11=2 THEN 2
            when ss_test1.c12 NOT IN (select ss_test2.c22
                         from ss_test2) THEN ss_test1.c11
            END = 1;

explain (COSTS OFF)
select c11 from ss_test1
      where case
            when ss_test1.c11=1 THEN 1
            when ss_test1.c11=2 THEN 2
            when exists (select ss_test2.c23, ss_test2.c24
                         from ss_test2 where ss_test1.c12=ss_test2.c22) AND c12= 3 THEN ss_test1.c11
            END = 1;

explain (COSTS OFF)
select c11 from ss_test1
      where case
            when ss_test1.c11=1 THEN 1
            when ss_test1.c11=2 THEN 2
            when ss_test1.c12 NOT IN (select ss_test2.c22
                         from ss_test2) and c12=3 THEN ss_test1.c11
            END = 1;

explain (COSTS OFF)
select * from ss_test1
where c11=1 or exists (select ss_test2.c23, ss_test2.c24
                                      from ss_test2 where ss_test1.c12=ss_test2.c22)
                       or c11 in (select c21 from ss_test2
                                       where ss_test1.c12=ss_test2.c22);
explain (COSTS OFF)
select c11 from ss_test1
      where case
            when ss_test1.c11=1 THEN 1
            when ss_test1.c11=2 THEN 2
            when c11=1 or exists (select ss_test2.c23, ss_test2.c24
                                      from ss_test2 where ss_test1.c12=ss_test2.c22)
                       or c11 in (select c21 from ss_test2
                                       where ss_test1.c12=ss_test2.c22) THEN ss_test1.c11
            END = 1;

explain (COSTS OFF)
select count(1) from ss_test1 ss1 left join ss_test1 ss2
      on case
           when exists (select 1 from ss_test2
                         where ss1.c12=ss_test2.c22
                           and ss2.c13=ss_test2.c23) THEN 1
           when ss2.c11=2 THEN 2
           when ss1.c11=1 THEN ss1.c11
           END = 1;

explain (COSTS OFF)
select count(1) from ss_test1 ss1 join ss_test1 ss2
      on case
           when exists (select 1 from ss_test2
                         where ss1.c12=ss_test2.c22
                           and ss2.c13=ss_test2.c23) THEN 1
           when ss2.c11=2 THEN 2
           when ss1.c11=1 THEN ss1.c11
           END = 1;

explain (COSTS OFF)
select count(1) from ss_test1 ss1 left join ss_test1 ss2
      on ss1.c14=ss2.c14 or exists (select 1 from ss_test2
                                    where ss1.c12=ss_test2.c22
                                      and ss2.c13=ss_test2.c23);

explain (COSTS OFF)
select count(1) from ss_test1 ss1 join ss_test1 ss2
      on ss1.c14=ss2.c14 or exists (select 1 from ss_test2
                                    where ss1.c12=ss_test2.c22
                                      and ss2.c13=ss_test2.c23);

drop table ss_test1;
drop table ss_test2;

create table ss_tt1
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 char(20)
)
DISTRIBUTE BY SHARD (c1);

create table tt2
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 char(20)
)
DISTRIBUTE BY SHARD (c1);

create table tt3
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 char(20)
)
DISTRIBUTE BY SHARD (c1);

insert into ss_tt1
select  i,i,i,'1'
from generate_series(1, 10) as i;

insert into tt2
select  i,i,i,'1'
from generate_series(1, 10) as i;

insert into tt3
select  i,i,i,'1'
from generate_series(1, 10) as i;

explain (costs off)
update ss_tt1 set c2 = 3
where exists (with aa as 
                (select tt3.c2
                 from tt3 
                 left join tt2 on tt2.c4 = tt3.c4
                 where tt2.c1=1 )
              select * from aa where c3=aa.c2 );

update ss_tt1 set c2 = 3
where exists (with aa as 
                (select tt3.c2
                 from tt3 
                 left join tt2 on tt2.c4 = tt3.c4
                 where tt2.c1=1 )
              select * from aa where c3=aa.c2 );

explain (costs off) 
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count
from ss_tt1;

explain (costs off) 
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                c1
from ss_tt1;

explain (costs off) 
select from (select c1 from ss_tt1) as sub1 group by c1
union select all from (select c1 from ss_tt1);

explain (costs off)
select c2 from ss_tt1 union select c2 from tt2 order by c2 limit 10;

explain (costs off)
select c2 from ss_tt1 union select c2 from tt2 order by c2 limit 10;

explain (costs off)
select c2 from ss_tt1 union select c2 from tt2 limit 10;

explain (costs off)
select c2 from ss_tt1 union select c2 from tt2 order by c2;

explain (costs off)
select c2 from ss_tt1 union select c2 from tt2 order by c2 limit 10 offset 100;

explain (costs off)
select c2 from ss_tt1 union select c2 from tt2 limit 10 offset 100;

explain (costs off)
select c2 from (select c2 from ss_tt1
                union
                select c2 from tt2) as a
order by c2 limit 10 offset 100;

explain (costs off)
select c2 from (select c2 from ss_tt1
                union
                select c2 from tt2) as a
order by c2 limit 10;

explain (costs off)
select c2 from (select c2 from ss_tt1
                union
                select c2 from tt2) as a
order by c2;

explain (costs off)
select c2 from (select c2 from ss_tt1
                union
                select c2 from tt2) as a
limit 10;

explain (costs off)
select c1, c2 from (select c2, c1 from ss_tt1
                union
                select c2, c1 from tt2) as a
order by c2, c1 limit 10;

explain (costs off)
select c1, c2 from (select c2, c1 from ss_tt1
                union
                select c2, c1 from tt2) as a
order by c1, c2 limit 10;

explain (costs off)
select c1, c2 from (select c2, c1 from ss_tt1
                union
                select tt2.c2, tt2.c1 from tt2, ss_tt1 where tt2.c1=ss_tt1.c1) as a
order by c1, c2 limit 10;

explain (costs off)
select c1, c2 from ((select c2, c1 from ss_tt1 order by c1, c2 limit 10)
                union
                    (select c2, c1 from tt2 order by c1, c2 limit 10)) as a
order by c1, c2 limit 10;

explain (costs off)
select c1, c2, c3 from (select c2, c1, c3 from ss_tt1
                union
                select c2, c1, c3 from tt2) as a
order by c3, c2 limit 10;


explain (costs off)
select c1, c2, c3 from (select c2, c1, c3 from ss_tt1
                union
                select tt2.c2, tt2.c1, ss_tt1.c3 from tt2, ss_tt1 where tt2.c1=ss_tt1.c1) as a
order by c1, c2 limit 10;


explain (costs off)
select c3, c2, c1 from (select c2, c1, c3 from ss_tt1
                union
                select tt2.c2, tt2.c1, ss_tt1.c3 from tt2, ss_tt1 where tt2.c1=ss_tt1.c1) as a
order by c2, c1 limit 10;

explain (costs off) 
select c1 from (select c1 from ss_tt1
                union
                select c1 from tt2
                order by c1 limit 20000) a
order by c1 limit 5;

explain (costs off)
select c1 from ss_tt1
where exists(select 1 from tt2 where ss_tt1.c1 = tt2.c1
             union
             select 1 from tt3 where ss_tt1.c1 = tt3.c1);

explain (costs off)
select c1 from ss_tt1 where exists(select 1 from tt2 where ss_tt1.c1 = tt2.c1)
                      or exists(select 1 from tt3 where ss_tt1.c1 = tt3.c1);


explain (costs off)
select c1 from ss_tt1 where exists(select 1 from tt2 where ss_tt1.c1 = tt2.c1
                                                    and exists(select * from tt3 where ss_tt1.c2=tt3.c2)
                                union
                                select 1 from tt3 where ss_tt1.c1 = tt3.c1);

explain (costs off)
select c1 from ss_tt1 where exists(select 1 from tt2 where ss_tt1.c1 = tt2.c1
                                                    and exists(select * from tt3 where ss_tt1.c2=tt3.c2))
                      or exists (select 1 from tt3 where ss_tt1.c1 = tt3.c1);


explain (costs off)
select c1 from ss_tt1 where exists(select 1 from tt2 where ss_tt1.c1 = tt2.c1
                                union
                                select 1 from tt3 where ss_tt1.c1 = tt3.c1
                                union
                                select 1 from tt3 where ss_tt1.c1 = tt3.c1);


explain (costs off)
select c1 from ss_tt1 where exists(select 1 from tt2 where ss_tt1.c1 = tt2.c1)
                      or exists(select 1 from tt3 where ss_tt1.c1 = tt3.c1)
                      or exists(select 1 from tt3 where ss_tt1.c1 = tt3.c1);

explain (costs off)
select c1 from ss_tt1
where exists(select 1 from tt2 where ss_tt1.c1 = tt2.c1
             union
             select 1 from tt3 where ss_tt1.c1 = tt3.c1
             limit 0);

explain (costs off)
select c1 from ss_tt1
where exists(select 1 from tt2 where ss_tt1.c1 = tt2.c1
             union
             select 1 from tt3 where ss_tt1.c1 = tt3.c1
             limit 1);

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c3) as count,
                c1
from ss_tt1;

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c3) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c3 group by tt2.c3) as count,
                c1
from ss_tt1;

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c1 group by tt2.c3) as count,
                c1
from ss_tt1;

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c3 group by tt2.c3) as count,
                c1
from ss_tt1;

explain (costs off)
select distinct ss_tt1.c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c3 group by tt2.c3) as count,
                ss_tt1.c1
from ss_tt1 order by ss_tt1.c1, ss_tt1.c2;

explain (costs off)
with a as (select c1, c2, count(*) count from ss_tt1 group by c1,c2)
select distinct a.count, (select count(*)
                     from tt2
                     where tt2.c2=a.c1) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=a.count group by tt2.c3) as count,
                a.c1
from a order by a.c1, a.count;

explain (costs off)
with a as (select c1, count(*) count from ss_tt1 group by c1)
select distinct a.count, (select count(*)
                     from tt2
                     where tt2.c2=b.c1) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=a.count group by tt2.c3) as count,
                a.c1
from a, ss_tt1 b order by a.c1, a.count;

explain (costs off)
with a as (select c1, count(*) count from ss_tt1 group by c1)
select distinct a.count, (select count(*)
                     from tt2
                     where tt2.c2=b.c1) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=a.count group by tt2.c3) as count,
                b.c1
from a, ss_tt1 b
group by b.c1, a.count
order by b.c1, a.count;

explain (costs off)
select distinct ss_tt1.c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=b.c3 group by tt2.c3) as count,
                ss_tt1.c1
from ss_tt1, tt2 b
where ss_tt1.c3 = b.c3
order by ss_tt1.c1, ss_tt1.c2;

explain (costs off)
select distinct ss_tt1.c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c1 group by tt2.c3) as count,
                ss_tt1.c1
from ss_tt1, tt2 b
where ss_tt1.c3 = b.c3
group by ss_tt1.c1, ss_tt1.c2
order by ss_tt1.c1, ss_tt1.c2;

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c3) as count,
                c1
from ss_tt1 order by c2 limit 10 offset 20;

set optimize_distinct_subquery = 2;

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c3) as count,
                c1
from ss_tt1;

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c3) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c3 group by tt2.c3) as count,
                c1
from ss_tt1;

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c1 group by tt2.c3) as count,
                c1
from ss_tt1;

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c3 group by tt2.c3) as count,
                c1
from ss_tt1;

explain (costs off)
select distinct ss_tt1.c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c3 group by tt2.c3) as count,
                ss_tt1.c1
from ss_tt1 order by ss_tt1.c1, ss_tt1.c2;

explain (costs off)
with a as (select c1, c2, count(*) count from ss_tt1 group by c1,c2)
select distinct a.count, (select count(*)
                     from tt2
                     where tt2.c2=a.c1) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=a.count group by tt2.c3) as count,
                a.c1
from a order by a.c1, a.count;

explain (costs off)
with a as (select c1, count(*) count from ss_tt1 group by c1)
select distinct a.count, (select count(*)
                     from tt2
                     where tt2.c2=b.c1) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=a.count group by tt2.c3) as count,
                a.c1
from a, ss_tt1 b order by a.c1, a.count;

explain (costs off)
with a as (select c1, count(*) count from ss_tt1 group by c1)
select distinct a.count, (select count(*)
                     from tt2
                     where tt2.c2=b.c1) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=a.count group by tt2.c3) as count,
                b.c1
from a, ss_tt1 b
group by b.c1, a.count
order by b.c1, a.count;

explain (costs off)
select distinct ss_tt1.c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=b.c3 group by tt2.c3) as count,
                ss_tt1.c1
from ss_tt1, tt2 b
where ss_tt1.c3 = b.c3
order by ss_tt1.c1, ss_tt1.c2;

explain (costs off)
select distinct ss_tt1.c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c2) as count,
                     (select tt2.c3
                     from tt2
                     where tt2.c2=ss_tt1.c1 group by tt2.c3) as count,
                ss_tt1.c1
from ss_tt1, tt2 b
where ss_tt1.c3 = b.c3
group by ss_tt1.c1, ss_tt1.c2
order by ss_tt1.c1, ss_tt1.c2;

explain (costs off)
select distinct((select tt2.c1 from tt2
                 where tt2.c2 = ss_tt1.c2), c3)
from ss_tt1;

explain (costs off)
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=ss_tt1.c3) as count,
                c1
from ss_tt1 order by c2 limit 10 offset 20;

explain (costs off)
select distinct to_char(c2), c2, 
                (select tt2.c1 from tt2
                 where tt2.c2=ss_tt1.c2
                 group by tt2.c1)
from ss_tt1;

set optimize_distinct_subquery = 1;

drop table ss_tt1;
drop table tt2;
drop table tt3;

create table ss_tt1
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

create table tt2
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

create table tt3
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

explain (costs off, verbose)
select c1,  (select c1
                 from tt2
                 where ss_tt1.c2=tt2.c2)
from ss_tt1;

explain (costs off, verbose)
select c1, CASE
         WHEN c1 = 1 
           THEN 3
         ELSE  (select c1
                 from tt2
                 where ss_tt1.c2=tt2.c2)
       END AS count
from ss_tt1;

explain (costs off, verbose)
select c1, CASE
         WHEN c1 = 1 
           THEN 4
         WHEN c1 = 2
           THEN (select c1
                 from tt2
                 where ss_tt1.c2=tt2.c2 limit 100)
         ELSE  (select c1
                 from tt2
                 where ss_tt1.c2=tt2.c2 limit 100)
       END AS count
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 =1 then 1
                WHEN C1 IN (select c1 from tt3) AND c1 = 2 then 2
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select ss_tt1.c1, case when tt3.c1 is not null AND ss_tt1.c1=1 then 1
                    when ss_tt1.c1 = tt3.c1 AND ss_tt1.c1=2 then 1
                    ELSE 3 END
from ss_tt1 left outer join tt3 on ss_tt1.c1 = tt3.c1;


explain (costs off, verbose)
select c1, CASE WHEN C1 IN (select c1 from tt3) then 2
                ELSE 3 END
from ss_tt1;


explain (costs off, verbose)
select c1, CASE WHEN exists (select c1 from tt3 where tt3.c1=ss_tt1.c2) then 2
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                         AND c1 IN (select c3 from tt3) AND c1=1 then 2
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                 AND c1 IN (select c3 from tt3 limit 10) AND c1=1 then 2
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                 AND c1 IN (select c3 from tt3 limit 10) AND c1=1
                THEN (select c3 from tt3 where ss_tt1.c1=tt3.c1)
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                 AND c1 IN (select c3 from tt3 limit 10) AND c1=1
                THEN (select c3 from tt3 where ss_tt1.c1=tt3.c1 limit 1)
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3) 
                 AND c1 IN (select c3 from tt3 limit 10)
                 AND c1=1
                THEN (select c3 from tt3 where ss_tt1.c1=tt3.c1 limit 1)
                ELSE (select c3 from tt3 where ss_tt1.c1=tt3.c1 limit 5) END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                 AND c1 IN (select c3 from tt3 limit 10)
                 AND c1=1
                THEN (select c3 from tt3 where ss_tt1.c1=tt3.c1)
                ELSE (select c3 from tt2 where ss_tt1.c1=tt2.c1) END
from ss_tt1;

 explain (costs off, verbose)
select c1, CASE WHEN c1 =1 then 1
                WHEN exists (select c1 from tt3 where tt3.c1 = ss_tt1.c1) AND c1 = 2 then 2
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                 AND c1 IN (select c3 from tt3)
                 AND c1=1
                THEN (select c3 from tt3 where ss_tt1.c1=tt3.c1)
                ELSE (select c3 from tt2 where ss_tt1.c1=tt2.c1) END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                THEN  (select c3 from tt3 where ss_tt1.c1=tt3.c1)
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                 AND exists (select c1 from tt3 where tt3.c1 = ss_tt1.c1)
                 AND c1 = 2 then 2
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                 AND c1 IN (select c3 from tt3) AND c1 = 2 then 2
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                 AND c1 IN (select c3 from tt3) AND c1 = 2 then 2
                ELSE 3 END,
           CASE WHEN c1 IN (select c3 from tt3)
            AND c1 IN (select c3 from tt3) AND c1 = 2 then 2
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN c1 IN (select c3 from tt3)
                 AND c1 IN (select c3 from tt3)
                 AND c1 = 2 then 2
                ELSE 3 END,
           CASE WHEN c1 IN (select c3 from tt3)
                 AND c1 IN (select c3 from tt3) AND c1 = 2 then 2
                ELSE 3 END
from ss_tt1;

explain (costs off, verbose)
select c1, CASE WHEN Max(c1) IN (select c3 from tt3)
                THEN 1
                ELSE 3 END
from ss_tt1
group by c1;

explain (costs off)
select c4
from ss_tt1 t1
where (c4 is not null OR
       exists (select c2 from tt2 t7 where t7.c1=t1.c1))
 and c1=(select max(c1) from ss_tt1 t2 where t2.c3=t1.c3);

select case when exists (select 1 from tt2
                         where ss_tt1.c2 is not null)
       then 2 else 3 end
from ss_tt1;

explain (costs off, verbose)
select case when exists (select 1 from tt2
                         where ss_tt1.c2 is not null
                           and tt2.c1 = ss_tt1.c1)
       then 2 else 3 end
from ss_tt1;


explain (costs off, verbose)
select 1
from ss_tt1
where  exists (select 1 from tt2
                         where ss_tt1.c2 is not null
                           and tt2.c1 = ss_tt1.c1) or c1=1;

explain (costs off, verbose)
select case when ss_tt1.c1 in (select tt2.c1 from tt2
                         where ss_tt1.c2 is not null
                           and tt2.c2 is not null)
       then 2 else 3 end
from ss_tt1;

explain (costs off, verbose)
select case when c3 = 1
       then (select c2 from tt2 where tt2.c3 = ss_tt1.c3)
       else (select c2::bigint from tt2 where tt2.c3 = ss_tt1.c3)
       end
from ss_tt1;

explain (costs off, verbose)
select case when c3 = 1
       then (select c2::bigint from tt2 where tt2.c3 = ss_tt1.c3)
       else (select c2 from tt2 where tt2.c3 = ss_tt1.c3)
       end
from ss_tt1;

explain (costs off, verbose)
select case when c3 = 1
       then (select c2::bigint from tt2 where tt2.c3 = ss_tt1.c3)
       else (select c2::bigint from tt2 where tt2.c3 = ss_tt1.c3)
       end
from ss_tt1;

explain (costs off)
select c1 from ss_tt1
where (ss_tt1.c2, ss_tt1.c3) = (select c2, c3 from tt2 where ss_tt1.c4 = tt2.c4);

set distinct_pushdown_factor = 1;

explain (costs off)  select distinct ss_tt1.c1 from ss_tt1, tt2 where ss_tt1.c1 = tt2.c1;
explain (costs off)  select distinct ss_tt1.c1, ss_tt1.c2 from ss_tt1, tt2 where ss_tt1.c2 = tt2.c2;
explain (costs off)  select distinct ss_tt1.c1, tt2.c2 from ss_tt1, tt2 where ss_tt1.c2 = tt2.c2;
explain (costs off)  select distinct ss_tt1.c1, tt2.c2 from ss_tt1, tt2 where ss_tt1.c1 = tt2.c1 and ss_tt1.c2 = tt2.c2;

explain (costs off)
select c1 from ss_tt1
where c2 = (select c2 from tt2 where ss_tt1.c2 is null);

explain (costs off)
select c1 from ss_tt1
where c2 = (select c2 from tt2 where ss_tt1.c2 is null and tt2.c3 = ss_tt1.c3);

explain (costs off)
select distinct ss_tt1.c1
from ss_tt1, tt2, tt3
where ss_tt1.c1 = tt2.c1
  and tt2.c2 = tt3.c2;

explain (costs off)
select distinct ss_tt1.c1
from ss_tt1, tt2, tt3 a, tt3 b
where ss_tt1.c1 = tt2.c1
  and tt2.c2 = a.c2
  and b.c2 = a.c2;

explain (costs off)
select c2 from ss_tt1
where c3 in (select max(c3) from tt2);

set distinct_pushdown_factor = 0.2;

explain (costs off)
select c1 from ss_tt1 where c1 = (select c1 from tt2 where ss_tt1.c2=tt2.c2) and ss_tt1.c3=0;

explain (costs off)
select c1, (select c1 from ss_tt1
            where ss_tt1.c2 = tt2.c1)
from tt2
group by c1
having c1 not in (select c1 from tt3);

explain (costs off)
select case when c1 not in (select t1.c1
                        from ss_tt1 t1
                         left join tt2 t2
                        on t1.c1=t2.c1)
              or c2 =1
            then 1
            else 2
            end
from ss_tt1 t1 ;

explain (costs off)
select case when c1 in (select t1.c1
                        from ss_tt1 t1
                         left join tt2 t2
                        on t1.c1=t2.c1)
              or c2 =1
            then 1
            else 2
            end
from ss_tt1 t1 ;

explain (costs off)
select c3 from (select distinct ss_tt1.c3, (select tt2.c2
                                         from tt2
                                         where tt2.c3 = ss_tt1.c3)
                from ss_tt1
                group by ss_tt1.c3)
where c3 is not null;

drop table ss_tt1;
drop table tt2;
drop table tt3;


create table ss_tt1
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

create table tt2
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

insert into ss_tt1 values(1,1,1,1);
insert into ss_tt1 values(2,2,2,2);

insert into tt2 values(1,1,1,1);
insert into tt2 values(2,2,2,2);
insert into tt2 values(3,1,3,1);
insert into tt2 values(4,3,4,3);

explain (costs off, verbose)
select 1 from ss_tt1
where c1 = (select c2 from tt2 where ss_tt1.c2 = tt2.c2);

explain (costs off, verbose)
select 1 from ss_tt1
where c1 = (select c3 from tt2 where ss_tt1.c2 = tt2.c2);

explain (costs off, verbose)
select 1 from ss_tt1
where c1 = (select c2 from tt2 where ss_tt1.c3 = tt2.c3);

select 1 from ss_tt1
where c1 = (select c2 from tt2 where ss_tt1.c2 = tt2.c2)
order by ss_tt1.c1, ss_tt1.c2, ss_tt1.c3, ss_tt1.c4;

select 1 from ss_tt1
where c1 = (select c3 from tt2 where ss_tt1.c2 = tt2.c2)
order by ss_tt1.c1, ss_tt1.c2, ss_tt1.c3, ss_tt1.c4;

select 1 from ss_tt1
where c1 = (select c2 from tt2 where ss_tt1.c3 = tt2.c3)
order by ss_tt1.c1, ss_tt1.c2, ss_tt1.c3, ss_tt1.c4;

select 1 from ss_tt1,tt2
where ss_tt1.c1=tt2.c2 and ss_tt1.c2 = tt2.c2
order by ss_tt1.c1, ss_tt1.c2, ss_tt1.c3, ss_tt1.c4;

select 1 from ss_tt1,tt2
where ss_tt1.c1=tt2.c3 and ss_tt1.c2 = tt2.c2
order by ss_tt1.c1, ss_tt1.c2, ss_tt1.c3, ss_tt1.c4;

explain (costs off)
select c1,  (select c2 from tt2 where ss_tt1.c4 = tt2.c4)
from ss_tt1;

explain (costs off)
select c1,  (select c2 from tt2 where ss_tt1.c4 = tt2.c4),
            (select c3 from tt2 where ss_tt1.c4 = tt2.c4),
       c4
from ss_tt1;

select  (select c2 from tt2 where ss_tt1.c2 = tt2.c2) from ss_tt1
order by ss_tt1.c1, ss_tt1.c2, ss_tt1.c3, ss_tt1.c4;

select (select c2 from tt2 where ss_tt1.c3 = tt2.c3) from ss_tt1
order by ss_tt1.c1, ss_tt1.c2, ss_tt1.c3, ss_tt1.c4;

explain(costs off)
select c1, (select c1 from ss_tt1 where ss_tt1.c2=tt2.c2 limit 1)
from tt2;

select c1, (select c1 from ss_tt1 where ss_tt1.c2=tt2.c2 limit 1)
from tt2 order by tt2.c1, tt2.c2, tt2.c3, tt2.c4;

explain(costs off)
select c1, (select c1 from ss_tt1 
            where ss_tt1.c2=tt2.c2
            order by c1 limit 1)
from tt2;

select c1, (select c1 from ss_tt1 
            where ss_tt1.c2=tt2.c2
            order by c1 limit 1)
from tt2 order by tt2.c1, tt2.c2, tt2.c3, tt2.c4;

explain (costs off)
select * from ss_tt1 where c1 in (select distinct c1 from tt2);

explain (costs off)
select * from ss_tt1 where c1 not in (select distinct c1 from tt2);

explain(costs off) select * from ss_tt1
where c1 in (select distinct c1 from tt2 where ss_tt1.c2=tt2.c2);

explain(costs off) select * from ss_tt1
where c1 not in (select distinct c1 from tt2 where ss_tt1.c2=tt2.c2);

explain(costs off)
select * from ss_tt1
where ss_tt1.c1 not in (select 10
                     from tt2
                     where c1=1);

drop table ss_tt1;
drop table tt2;

create table ss_tt1 (c1 int, c2 int not null, c3 int not null);
create table tt2 (c1 int, c2 int not null, c3 int not null);

explain (costs off) select 1 from ss_tt1 where c1 not in (select c1 from tt2);
explain (costs off) select 1 from ss_tt1 where c2 not in (select c2 from tt2);
explain (costs off) select 1 from ss_tt1 where (c1,c2) not in (select c1,c2 from tt2);
explain (costs off) select 1 from ss_tt1 where (c2,c3) not in (select c2,c3 from tt2);
explain (costs off) select 1 from ss_tt1 where (c2,c3) not in (select c1,c3 from tt2);
explain (costs off) select 1 from ss_tt1 where c2 not in (select c2 from tt2 union select NULL from tt2);
explain (costs off) select 1 from ss_tt1 where c2 not in (select c2 from tt2 union all select NULL from tt2);
explain (costs off) select 1 from ss_tt1 where c2 not in (select c2 from tt2 intersect select NULL from tt2);
explain (costs off) select 1 from ss_tt1 where c2 not in (select c2 from tt2 except select NULL from tt2);

explain (costs off)
select a.c1 from
ss_tt1 a left join ss_tt1 b on a.c2=b.c3
      inner join tt2 on tt2.c3=a.c3
      where tt2.c2 not in (select c2 from ss_tt1 c);

explain (costs off)
select a.c1 from
ss_tt1 a left join ss_tt1 b on a.c2=b.c3
      inner join tt2 on tt2.c3=a.c3
      where tt2.c1 not in (select c2 from ss_tt1 c where c.c2=tt2.c2);

explain (costs off)
select a.c1 from
ss_tt1 a left join ss_tt1 b on a.c2=b.c3
      left join tt2 on tt2.c3=a.c3
      where tt2.c2 not in (select c2 from ss_tt1 c);

explain (costs off)
select a.c1 from
ss_tt1 a left join (ss_tt1 b
      inner join tt2 on tt2.c3=b.c3) on a.c2=b.c3
      where tt2.c2 not in (select c2 from ss_tt1 c where c.c2=tt2.c2);

explain (costs off)
select a.c1 from
      (ss_tt1 b
      inner join tt2 on tt2.c3=b.c3) left join ss_tt1 a  on a.c2=b.c3
      where tt2.c2 not in (select c2 from ss_tt1 c where c.c2=tt2.c2);

explain (costs off)
with cte1 as (select a.c1 from
      (ss_tt1 b inner join tt2 on tt2.c3=b.c3) left join ss_tt1 a  on a.c2=b.c3
      where tt2.c2 not in (select c2 from ss_tt1 c where c.c2=tt2.c2) limit 10)
select * from cte1 left join ss_tt1 d on 1=1;

explain (costs off)
select (select c2 from tt2 where c2 not in (select c2 from ss_tt1))
from ss_tt1;

explain (costs off) select 1 from ss_tt1
where c2 not in (select c2 from tt2
                 where tt2.c3 not in (select c3 from ss_tt1));

explain (costs off)
select (select a.c1 from tt2 a, tt2 b
        where (a.c1 = ss_tt1.c1 or b.c1 = ss_tt1.c1)
          and a.c2 = b.c2)
from ss_tt1;

explain (costs off)
select (select a.c1 from tt2 a, tt2 b
        where (a.c1 = ss_tt1.c1 or b.c1 = ss_tt1.c1)
          and a.c2 = b.c2
          and a.c3 = ss_tt1.c3)
from ss_tt1;

explain (costs off)
select (select a.c1 from tt2 a
        where exists(select * from ss_tt1 b where b.c2=a.c2)
                   and a.c3=ss_tt1.c3)
from ss_tt1;

drop table ss_tt1;
drop table tt2;

create table ss_tt1
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

create table tt2
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

insert into ss_tt1 values(1,1,1,1);
insert into ss_tt1 values(2,2,2,2);

insert into tt2 values(1,1,1,1);
insert into tt2 values(2,2,2,2);
insert into tt2 values(3,1,3,1);
insert into tt2 values(4,3,4,3);

analyze ss_tt1;
analyze tt2;

explain (costs off)
select * from ss_tt1 where c1 in (select distinct c1 from tt2);

explain (costs off)
select * from ss_tt1 where c1 not in (select distinct c1 from tt2);

explain (costs off)
select 1 from ss_tt1 where (c3,c4) not in (select c1,c3 from tt2);

explain (costs off)
select c1 from tt2 where (c1,c2) = (select c1,c2 from ss_tt1 where c3 = ss_tt1.c3) order by c1;

select c1 from tt2 where (c1,c2) = (select c1,c2 from ss_tt1 where c3 = ss_tt1.c3) order by c1;

drop table ss_tt1;
drop table tt2;


-- OPENTENBASE
CREATE SCHEMA chqin;
SET search_path TO chqin,public;
CREATE TABLE rqg_table1 (
c0 int,
c1 int not null,
c2 text,
c3 text not null,
c4 date,
c5 date not null,
c6 timestamp,
c7 timestamp not null,
c8 numeric,
c9 numeric not null)   ;
alter table rqg_table1 alter column c0 drop not null;
INSERT INTO rqg_table1 VALUES  (6, 2, 'bar', 'aj', '2003-06-15', '2031-03-24 06:39:53', '2010-12-23', '2001-04-11 23:56:54', 1.23456789123457e-09, -1209401344) ,  (5, 1, 'bar', 'foo', '1993-02-19', '2021-04-25', NULL, '1998-11-28 11:48:15', -1601372160, -1.23456789123457e+39) ,  (7, 5, 'bar', 'bar', '2000-11-03', '2008-05-15', '1995-09-19 09:11:43.039133', '2015-11-16 00:25:00.022454', 63832064, -0.123456789123457) ,  (4, 7, 'jopzkvyuglfxkqrl', 'foo', '2008-06-15', '2027-04-14', '2017-01-27 23:57:57.036861', '2001-03-13 20:04:28.024907', 0.123456789123457, -1.23456789123457e-09) ,  (NULL, 9, 'foo', 'foo', '1979-12-05 01:54:07', '2026-12-02', '2014-10-08 07:43:03.030307', '2001-05-25 01:16:36.059321', 1.23456789123457e-09, -1.23456789123457e+44) ,  (8, 4, 'opz', 'foo', '1977-05-05', '2024-02-23', NULL, '2012-10-10 22:35:27', -8.47605597368798e+18, -0.123456789123457) ,  (3, 0, 'bar', 'bar', '1977-05-17', '1984-09-02', '2029-05-12 05:22:17.002455', '2018-07-16 08:57:18.018046', 1252720640, 0.120880126953125) ,  (NULL, 6, 'foo', 'pzkvyu', '2002-07-05 12:01:07', '2013-11-02 03:51:54', '1994-01-28 06:58:27.061488', '1994-11-13 08:17:52.044224', 6.56765563158974e+18, -1.23456789123457e+25) ,  (2, 2, 'bar', 'zkvyu', '2025-09-17', '1991-10-08', '2006-10-25 15:05:05', '1999-12-19 11:49:37.010816', 1.23456789123457e-09, 1.23456789123457e+25) ,  (6, 8, 'foo', 'kvyuglf', '1982-01-23 12:04:08', '1975-03-16 11:35:44', '1999-01-21 11:31:18', '1995-10-24 21:27:26', 1.23456789123457e+39, 1.23456789123457e+25) ;
CREATE TABLE rqg_table2 (
c0 int,
c1 int not null,
c2 text,
c3 text not null,
c4 date,
c5 date not null,
c6 timestamp,
c7 timestamp not null,
c8 numeric,
c9 numeric not null)   distribute by replication;
alter table rqg_table2 alter column c0 drop not null;
INSERT INTO rqg_table2 VALUES  (NULL, 4, 'v', 'foo', '1975-11-12', '1977-11-10 20:07:35', '2013-12-21 17:46:30.045303', '2003-07-21 15:15:20.054059', -1.23456789123457e+25, -1.23456789123457e-09) ,  (3, 8, 'bar', 'foo', '1973-06-19 10:58:07', '1978-02-28 14:15:54', NULL, '1993-11-26 17:24:15.030869', -8.27728271484375e+80, 8.69166580584835e+18) ,  (NULL, 6, 'yuglfxkqrlzmikahzqtakkxbhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikj', 'uglfxkq', '1981-08-02 07:41:27', '2009-11-08', '2011-06-22', '2008-02-18', 1.23456789123457e+30, -240451584) ,  (3, 9, 'glfxkqrlz', 'bar', '2032-03-26 12:45:30', '1980-01-20', '2031-04-20', '2025-04-23', -2.84591674733147e+125, -1.23456789123457e+44) ,  (NULL, 1, 'lfxkqr', 'foo', '2024-09-11 14:18:59', '2002-12-25 04:51:48', '2007-01-18 09:35:49.006747', '1980-04-20 02:21:47.016900', 1554513920, 1.23456789123457e-09) ,  (8, 8, 'fxkqrlzmi', 'bar', '2019-05-25 03:44:38', '1976-02-01', '2035-08-05 05:08:52.013768', '1995-08-17 00:58:23.049842', -1.23456789123457e-09, -1.87631219475323e+18) ,  (2, 6, 'xkqrlzmikahzqtakkxbhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyh', 'bar', '2014-09-02', '2012-06-01 00:55:06', NULL, '2023-10-19 22:59:41', 1.23456789123457e+39, -1.23456789123457e-09) ,  (2, 2, 'bar', 'bar', '1982-11-05', '2005-09-18 07:02:10', '1975-01-02', '1984-05-10 00:07:38.005899', 0.123456789123457, -1.23456789123457e+39) ,  (6, 2, 'foo', 'kqrlzmikah', '2024-05-17', '1999-08-18 08:37:41', NULL, '2025-11-04', 0.29620361328125, 0.47149658203125) ,  (8, 4, 'bar', 'qrlzmik', '2030-08-21 19:18:34', '2019-12-17', '1978-05-26 04:53:14.048213', '2019-11-08 02:17:46.037957', -1646133248, -1.23456789123457e+30) ;
CREATE TABLE rqg_table3 (
c0 int,
c1 int not null,
c2 text,
c3 text not null,
c4 date,
c5 date not null,
c6 timestamp,
c7 timestamp not null,
c8 numeric,
c9 numeric not null)    PARTITION BY hash( c0) ;
create TABLE rqg_table3_p0 partition of rqg_table3 for values with(modulus 2,remainder 0);
create TABLE rqg_table3_p1 partition of rqg_table3 for values with(modulus 2,remainder 1);
alter table rqg_table3 alter column c0 drop not null;
INSERT INTO rqg_table3 VALUES  (NULL, 2, 'rlzmikahzq', 'foo', '2009-04-22 16:17:34', '2014-09-17 11:45:27', '1998-03-27', '1981-04-27', 1.23456789123457e+39, -1.23456789123457e+25) ,  (NULL, 3, 'lzm', 'bar', '1973-10-05 23:49:14', '1997-11-12 03:01:22', '2003-11-21 15:24:57.015663', '1990-01-10 03:14:17', -4.52880859374999e+79, -1356398592) ,  (6, 9, 'foo', 'bar', '2019-03-26 10:37:19', '2003-02-06 02:18:03', '2031-04-20', '2018-02-06 09:29:56', 0.892349243164062, -1.23456789123457e+39) ,  (NULL, 4, 'zmikahzqtakkxbhtxfskqlbwbuzmfpswekjuxnyheqdhroa', 'bar', '2019-05-27', '2010-05-21 15:29:17', '1985-05-16 09:05:15.015996', '2008-07-10', -0.123456789123457, -6.01669311483605e+125) ,  (0, 4, 'foo', 'mikahzqtakkxbhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefw', '1971-11-01', '2012-10-13 02:04:22', '2033-12-13 15:09:21.064741', '2021-07-13', 1.23456789123457e+25, 1.23456789123457e-09) ,  (9, 5, 'bar', 'ikahzqtak', '1984-07-22 05:33:50', '2013-04-07', '1987-08-12 02:12:01', '1975-05-19', -4.74594116158397e+125, -1.23456789123457e+30) ,  (1, 2, 'kahzqtakkxbhtxfs', 'foo', '2013-05-11', '1982-07-05', '2010-06-26 16:49:39.050733', '2009-02-01 22:28:23.055368', -1.23456789123457e+39, 5.1324147253421e+18) ,  (9, 8, 'ahzqtakkxbhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqunfsoztuyarrzilebjixpwtvbvlsrcfgtffkbkav', 'hzqtakkx', '1977-07-14 19:27:06', '2006-07-18', '1982-09-06 04:36:07.061768', '1993-04-14 21:44:44.053814', -1.23456789123457e-09, 0.518081665039062) ,  (9, 7, 'bar', 'foo', '2016-06-06', '2031-05-06 22:12:00', '2003-04-28', '2005-07-19 19:21:07.026637', 1.23456789123457e+39, -1.23456789123457e-09) ,  (9, 2, 'bar', 'zqtakkxbhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwh', '1979-05-22 03:45:11', '2034-09-24', '1991-10-19 15:00:12', '1972-09-04 09:27:20.061268', 1.23456789123457e+25, -0.123456789123457) ;
CREATE TABLE rqg_table4 (
c0 int,
c1 int not null,
c2 text,
c3 text not null,
c4 date,
c5 date not null,
c6 timestamp,
c7 timestamp not null,
c8 numeric,
c9 numeric not null)    PARTITION BY hash( c2) distribute by replication;
create TABLE rqg_table4_p0 partition of rqg_table4 for values with(modulus 2,remainder 0);
create TABLE rqg_table4_p1 partition of rqg_table4 for values with(modulus 2,remainder 1);
alter table rqg_table4 alter column c0 drop not null;
INSERT INTO rqg_table4 VALUES  (NULL, 4, 'bar', 'qtakkxbhtxfskql', '1987-05-22', '1995-06-24', '2027-05-19 23:11:32', '1971-01-03 05:26:48', 0.791397094726562, 1.23456789123457e-09) ,  (NULL, 4, 'takkxbhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqunfsoztuyarrzilebjixpwtvbvlsrcfgtf', 'akkxbhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqunfsoztuyarrzilebjixpwtv', '2008-11-20 15:32:02', '1989-05-12', '2004-01-16 18:37:13', '1992-04-16 17:53:53.012534', 0.123456789123457, 0.123456789123457) ,  (3, 4, 'bar', 'bar', '2010-03-17', '1981-06-06 18:40:26', '1989-07-09', '2020-02-12 15:34:14.045295', -0.123456789123457, 0.628570556640625) ,  (5, 8, 'kkxbhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqunfsoztuyarrzileb', 'bar', '2016-06-03 21:55:58', '2002-11-09', '2018-04-28 10:13:03', '2008-08-07 18:07:04.019452', -1.23456789123457e-09, 0.123456789123457) ,  (1, 3, 'kxbhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsm', 'bar', '2023-02-07', '1971-08-08', '2017-03-19', '2023-03-23 09:45:01', 0.123456789123457, -3.99047851502405e+125) ,  (0, 5, 'foo', 'bar', '1986-06-24 16:42:29', '1979-06-26 13:59:17', '1976-09-12 12:18:44.001232', '2032-02-04', 1.23456789123457e+44, -0.123456789123457) ,  (NULL, 4, 'foo', 'xbhtx', '1987-07-22', '2017-03-11', '1984-05-28 10:59:52', '2014-12-03', 767819776, 0.123456789123457) ,  (NULL, 6, 'bhtxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqunfsoztuyarrzilebjixpwtvbvlsrcfgtffkbkavsvzrtxq', 'htxfskqlbwbuzmfpswekjuxnyheqdhroavymvovdla', '1982-11-05 16:12:50', '2016-06-10 22:41:35', '2023-02-06', '1986-09-26 02:53:43', -1.23456789123457e+44, 0.123456789123457) ,  (NULL, 5, 'foo', 'bar', '2017-09-18', '1977-12-01', '1999-04-26 20:25:39.000019', '2016-02-28 10:06:40.004809', 629342208, -9.32952880859375e+80) ,  (4, 2, 'bar', 'bar', '1975-12-13', '2024-04-13', '2014-06-26 19:41:15', '2007-06-05 23:57:31.018232', 0.312225341796875, 0.123456789123457) ;
CREATE TABLE rqg_table5 (
c0 int,
c1 int not null,
c2 text,
c3 text not null,
c4 date,
c5 date not null,
c6 timestamp,
c7 timestamp not null,
c8 numeric,
c9 numeric not null)    PARTITION BY hash( c7) ;
create TABLE rqg_table5_p0 partition of rqg_table5 for values with(modulus 4,remainder 0);
create TABLE rqg_table5_p1 partition of rqg_table5 for values with(modulus 4,remainder 1);
create TABLE rqg_table5_p2 partition of rqg_table5 for values with(modulus 4,remainder 2);
create TABLE rqg_table5_p3 partition of rqg_table5 for values with(modulus 4,remainder 3);
alter table rqg_table5 alter column c0 drop not null;
INSERT INTO rqg_table5 VALUES  (3, 1, 'txfskql', 'bar', '1972-04-24', '1991-05-27 19:13:13', '1993-10-07', '2028-03-05', 271187968, 0.123456789123457) ,  (2, 9, 'xfskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqu', 'bar', '2004-08-16', '2001-04-07', '2004-07-02 02:53:55.005631', '2028-01-17 05:45:31.031383', -0.123456789123457, -0.123456789123457) ,  (3, 6, 'bar', 'fskqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqunfsoztuyarrzilebjixpwtvbvlsrcfgtffkbkavsvzrtxqtwzrnft', '1996-07-22', '2035-09-03 23:23:34', '2031-09-28 14:29:45.046084', '2010-03-03 12:16:57.060560', 1.23456789123457e+30, 1.23456789123457e-09) ,  (3, 1, 'skqlbwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqunfsoztuyarrzilebjixpw', 'bar', '1992-07-09 15:34:03', '1989-09-22 15:36:04', NULL, '1990-03-13 13:57:03.022468', 0.123456789123457, -1.23456789123457e+30) ,  (6, 3, 'kql', 'qlbwbuzm', '1987-02-15', '1988-09-22', '2021-10-16 09:25:28', '1977-02-26 17:14:23', 849543168, 5.25992288979203e+18) ,  (0, 0, 'foo', 'lbw', '2015-08-19', '1990-05-12 12:53:50', '1985-07-14', '2006-06-08 06:30:05.053345', -1.23456789123457e+43, -1295646720) ,  (5, 5, 'foo', 'foo', '1987-05-24 14:16:18', '2003-07-08', '2019-08-01 05:07:06', '2010-09-04 15:30:49.050978', 0.123456789123457, 0.123456789123457) ,  (8, 3, 'bar', 'bwbuzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttz', '2018-04-20', '1992-09-04 19:10:16', NULL, '2031-08-01', -9.12887573233476e+125, 1.23456789123457e+39) ,  (9, 6, 'foo', 'foo', '2033-03-15 14:58:07', '1989-12-03', NULL, '2008-09-12', 0.123456789123457, 0.123456789123457) ,  (7, 4, 'foo', 'bar', '1988-12-03 01:25:10', '1986-10-28', '2031-07-13 20:34:19.018103', '1999-05-21 18:27:26.001589', 1.23456789123457e+25, 1.23456789123457e+25) ;
CREATE TABLE rqg_table6 (
c0 int,
c1 int not null,
c2 text,
c3 text not null,
c4 date,
c5 date not null,
c6 timestamp,
c7 timestamp not null,
c8 numeric,
c9 numeric not null)    PARTITION BY hash( c2) distribute by replication;
create TABLE rqg_table6_p0 partition of rqg_table6 for values with(modulus 4,remainder 0);
create TABLE rqg_table6_p1 partition of rqg_table6 for values with(modulus 4,remainder 1);
create TABLE rqg_table6_p2 partition of rqg_table6 for values with(modulus 4,remainder 2);
create TABLE rqg_table6_p3 partition of rqg_table6 for values with(modulus 4,remainder 3);
alter table rqg_table6 alter column c0 drop not null;
INSERT INTO rqg_table6 VALUES  (8, 8, 'wbuz', 'buz', '2001-05-25', '1975-05-27', '1981-02-15 19:05:13.034805', '2007-04-06', -8.3721923828125e+80, 1.23456789123457e+39) ,  (8, 3, 'foo', 'uzmfpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdv', '1995-03-12', '2012-05-08', '1972-07-12 22:54:03', '2031-08-11 03:50:22.007270', 178978816, 1.23456789123457e+30) ,  (5, 1, 'bar', 'z', '1973-10-23', '2012-08-10', '2022-06-26 19:21:18.004742', '1980-12-05', -5.63934326128269e+125, 1.23456789123457e+39) ,  (NULL, 4, 'mfpswekj', 'fpswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtk', '2000-07-19', '1977-08-16 00:45:58', '2032-01-03 05:44:04.056770', '2032-11-17 01:40:53.004601', 1.23456789123457e-09, -0.123456789123457) ,  (NULL, 3, 'pswekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqunfs', 'swekj', '1990-02-10 05:33:40', '1999-03-10', '2003-02-12 15:04:27.019492', '1990-02-05', -6.27266985599697e+18, -1.23456789123457e+43) ,  (6, 6, 'bar', 'bar', '1991-01-21 18:14:00', '2002-01-17 01:57:20', '2012-03-16 22:13:52', '2015-11-01 18:37:15', -1.23456789123457e+43, 1.23456789123457e+43) ,  (7, 5, 'foo', 'wekjuxnyheqdhroavymvovdlacqthtmzwwffsbbynzttzigopxsmfmlbclirpxyyhtkqvzxfdcikjrilkzwmwhcbmbutrdvlcklpgzchsefwxirwqunfsoztuyarrzilebjixpwtvbvlsr', '1997-04-01 21:33:02', '1974-12-02', '1991-08-10', '1994-06-28 21:14:52.016636', 1.23456789123457e-09, 1.23456789123457e-09) ,  (2, 6, 'bar', 'bar', '1979-07-09', '1983-05-22', '1995-05-26 23:28:21', '2027-12-13 03:08:21.014750', 1109721088, 1.23456789123457e+30) ,  (NULL, 7, 'ekjuxny', 'foo', '1982-11-17', '2014-07-05 07:36:59', '2024-02-13 13:57:58.056596', '2000-01-13', 1.23456789123457e+43, 0.123456789123457) ,  (NULL, 1, 'bar', 'kjux', '2010-06-06 06:22:08', '1997-06-21', '1989-07-19 18:00:36.001478', '2008-01-21 18:23:24.024546', 0.123456789123457, -1.23456789123457e-09) ;

set max_parallel_workers_per_gather=2;
set parallel_tuple_cost to 0;
set parallel_setup_cost to 0;
set min_parallel_table_scan_size to 0;

explain (costs off)
select t2.c11,( select avg(c1) from (select c1 from ( SELECT a2.c0, a1.c1, a1.c3, a1.c2, a2.c4, a1.c5, a1.c6, a1.c7, a2.c8, a2.c9 FROM ( rqg_table1 a1 INNER JOIN ( SELECT a1.c0, a1.c1, a1.c3, a1.c2, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table3 a1 WHERE t1.c0 = 5 AND a1.c3 < t1.c3 GROUP BY a1.c0, a1.c1, a1.c3, a1.c2, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 HAVING NOT ( a1.c4 IS NOT NULL ) ) a2 ON ( NOT ( a1.c0 IN ( a2.c1, a2.c1 ) ) AND ( a1.c0 = a2.c1 ) ) ) LEFT JOIN rqg_table6 a3 ON ( ( a3.c1 BETWEEN 0 AND 0 + 10 AND ( a1.c0 = a3.c0 ) ) AND ( a3.c5 IS NULL ) ) WHERE NOT ( a1.c4 IN ( a2.c4, a2.c5 ) ) AND a3.c9 IN ( 0.12345678912345678912345678912345678912345678, 0.12345678912345678912345678912345678912345678,-12345678912345678912345678.0000123456789123456789123456789123456789123456789 ) GROUP BY a2.c0, a1.c1, a1.c3, a1.c2, a2.c4, a1.c5, a1.c6, a1.c7, a2.c8, a2.c9 HAVING ( ( ( ( ( ( a2.c9 IN ( -0.123456789123456789123456789123456789123456789, -1234567891234567891234567890000.123456789123456789123456789123456789123456789,-0.12345678912345678912345678912345678912345678 ) ) OR ( a1.c1 = a2.c0 ) ) OR ( a1.c1 = a1.c1 ) ) OR ( a1.c3 IS NOT NULL ) ) AND ( a2.c4 NOT BETWEEN '2008-11-14 17:50:36' AND TO_DATE( '2008-11-14 17:50:36','YYYY-MM-DD HH24:MI:SS') + 1 ) ) AND ( a1.c1 = a1.c1 ) ) AND ( a1.c6 >= '1991-12-06 07:11:34.021363' ) ) order by c1) ) from rqg_table5 t1 left join (select c1 c11 from rqg_table5) t2 on t1.c1=t2.c11;
select t2.c11,( select avg(c1) from (select c1 from ( SELECT a2.c0, a1.c1, a1.c3, a1.c2, a2.c4, a1.c5, a1.c6, a1.c7, a2.c8, a2.c9 FROM ( rqg_table1 a1 INNER JOIN ( SELECT a1.c0, a1.c1, a1.c3, a1.c2, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table3 a1 WHERE t1.c0 = 5 AND a1.c3 < t1.c3 GROUP BY a1.c0, a1.c1, a1.c3, a1.c2, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 HAVING NOT ( a1.c4 IS NOT NULL ) ) a2 ON ( NOT ( a1.c0 IN ( a2.c1, a2.c1 ) ) AND ( a1.c0 = a2.c1 ) ) ) LEFT JOIN rqg_table6 a3 ON ( ( a3.c1 BETWEEN 0 AND 0 + 10 AND ( a1.c0 = a3.c0 ) ) AND ( a3.c5 IS NULL ) ) WHERE NOT ( a1.c4 IN ( a2.c4, a2.c5 ) ) AND a3.c9 IN ( 0.12345678912345678912345678912345678912345678, 0.12345678912345678912345678912345678912345678,-12345678912345678912345678.0000123456789123456789123456789123456789123456789 ) GROUP BY a2.c0, a1.c1, a1.c3, a1.c2, a2.c4, a1.c5, a1.c6, a1.c7, a2.c8, a2.c9 HAVING ( ( ( ( ( ( a2.c9 IN ( -0.123456789123456789123456789123456789123456789, -1234567891234567891234567890000.123456789123456789123456789123456789123456789,-0.12345678912345678912345678912345678912345678 ) ) OR ( a1.c1 = a2.c0 ) ) OR ( a1.c1 = a1.c1 ) ) OR ( a1.c3 IS NOT NULL ) ) AND ( a2.c4 NOT BETWEEN '2008-11-14 17:50:36' AND TO_DATE( '2008-11-14 17:50:36','YYYY-MM-DD HH24:MI:SS') + 1 ) ) AND ( a1.c1 = a1.c1 ) ) AND ( a1.c6 >= '1991-12-06 07:11:34.021363' ) ) order by c1) ) from rqg_table5 t1 left join (select c1 c11 from rqg_table5) t2 on t1.c1=t2.c11 order by 1;

explain (costs off)
select case when c1 not in ( select c1 from ( SELECT DISTINCT a2.c0, a1.c1, a1.c3, a1.c2, a2.c4, a1.c5, a1.c6, a1.c7, a2.c8, a1.c9 FROM rqg_table5 a1 RIGHT JOIN ( SELECT DISTINCT a1.c0, a1.c1, a2.c3, a2.c2, a2.c4, a1.c5, a2.c6, a2.c7, a2.c8, a1.c9 FROM ( rqg_table1 a1 INNER JOIN rqg_table6 a2 ON ( ( NOT ( a1.c0 = 8 ) AND ( a1.c0 = a2.c0 +1 ) ) OR ( a1.c3 >= a2.c2 ) AND ( a1.c0 = a2.c0 ) ) ) INNER JOIN rqg_table6 a3 ON ( NOT ( a3.c0 = a1.c1 ) AND ( a1.c0 = a3.c0 +1 ) ) WHERE a2.c1 = 7 AND ( t1.c9 NOT IN ( -12345678912345678912345678912345678912345678, 1234567891234567891234567890000000000000,-1234567891234567891234567890000.123456789123456789123456789123456789123456789 ) AND ( t1.c0 = 8 AND t1.c2 LIKE 'C%' ) AND ( a3.c0 NOT IN ( 5, 9, 1, 8 ) ) ) AND ( a2.c8 IS NULL ) ) a2 ON ( NOT ( a1.c0 BETWEEN 0 AND 0 + 10 ) AND ( a1.c0 = a2.c0 +1 ) ) WHERE NOT ( a1.c4 IN ( '2000-04-18', a2.c5, a2.c4 ) ) AND a2.c1 = a2.c0 GROUP BY a2.c0, a1.c1, a1.c3, a1.c2, a2.c4, a1.c5, a1.c6, a1.c7, a2.c8, a1.c9 HAVING ( NOT ( a2.c0 = 1 ) ) AND ( a1.c5 IS NOT NULL ) ) ) and c3 in (select c3 from rqg_table3 ) or exists (select * from rqg_table6 tt where tt.c0=t1.c0 ) then 1 else 2 end from rqg_table4 t1;
select case when c1 not in ( select c1 from ( SELECT DISTINCT a2.c0, a1.c1, a1.c3, a1.c2, a2.c4, a1.c5, a1.c6, a1.c7, a2.c8, a1.c9 FROM rqg_table5 a1 RIGHT JOIN ( SELECT DISTINCT a1.c0, a1.c1, a2.c3, a2.c2, a2.c4, a1.c5, a2.c6, a2.c7, a2.c8, a1.c9 FROM ( rqg_table1 a1 INNER JOIN rqg_table6 a2 ON ( ( NOT ( a1.c0 = 8 ) AND ( a1.c0 = a2.c0 +1 ) ) OR ( a1.c3 >= a2.c2 ) AND ( a1.c0 = a2.c0 ) ) ) INNER JOIN rqg_table6 a3 ON ( NOT ( a3.c0 = a1.c1 ) AND ( a1.c0 = a3.c0 +1 ) ) WHERE a2.c1 = 7 AND ( t1.c9 NOT IN ( -12345678912345678912345678912345678912345678, 1234567891234567891234567890000000000000,-1234567891234567891234567890000.123456789123456789123456789123456789123456789 ) AND ( t1.c0 = 8 AND t1.c2 LIKE 'C%' ) AND ( a3.c0 NOT IN ( 5, 9, 1, 8 ) ) ) AND ( a2.c8 IS NULL ) ) a2 ON ( NOT ( a1.c0 BETWEEN 0 AND 0 + 10 ) AND ( a1.c0 = a2.c0 +1 ) ) WHERE NOT ( a1.c4 IN ( '2000-04-18', a2.c5, a2.c4 ) ) AND a2.c1 = a2.c0 GROUP BY a2.c0, a1.c1, a1.c3, a1.c2, a2.c4, a1.c5, a1.c6, a1.c7, a2.c8, a1.c9 HAVING ( NOT ( a2.c0 = 1 ) ) AND ( a1.c5 IS NOT NULL ) ) ) and c3 in (select c3 from rqg_table3 ) or exists (select * from rqg_table6 tt where tt.c0=t1.c0 ) then 1 else 2 end from rqg_table4 t1 order by 1;

explain (costs off)
select t2.c11,( select avg(c1) from (select c1 from ( SELECT DISTINCT a2.c0, a1.c1, a1.c3, a1.c2, a1.c4, a2.c5, a2.c6, a1.c7, a2.c8, a1.c9 FROM ( ( SELECT DISTINCT a2.c0, a1.c1, a1.c3, a1.c2, a1.c4, a1.c5, a1.c6, a1.c7, a2.c8, a1.c9 FROM ( rqg_table4 a1 RIGHT JOIN rqg_table1 a2 ON ( NOT ( a1.c1 IS NOT NULL ) AND ( a1.c0 = a2.c1 ) ) ) RIGHT JOIN rqg_table6 a3 ON ( ( ( ( NOT ( a3.c1 = 9 ) AND ( a1.c0 = a3.c1 ) ) AND ( a1.c5 = '1971-12-07' ) ) OR ( a3.c1 = a3.c1 ) AND ( a1.c0 = a3.c0 ) ) AND ( a3.c6 != '2029-07-14 16:15:10.057656' ) ) WHERE t1.c3 LIKE 'C%' GROUP BY a2.c0, a1.c1, a1.c3, a1.c2, a1.c4, a1.c5, a1.c6, a1.c7, a2.c8, a1.c9 HAVING NOT ( a2.c0 BETWEEN 0 AND 0 + 10 ) ) a1 RIGHT JOIN rqg_table6 a2 ON ( NOT ( a2.c6 IN ( a1.c6, a1.c6 ) ) AND ( a1.c0 = a2.c1 ) ) ) INNER JOIN rqg_table1 a3 ON ( ( NOT ( a1.c0 = 9 ) AND ( a1.c0 = a3.c1 ) ) AND ( a3.c2 != a1.c2 ) ) WHERE a2.c1 NOT IN ( a3.c0, 8, a1.c0, 7, 4, 6, 6 ) AND a3.c3 not LIKE 'N9%' GROUP BY a2.c0, a1.c1, a1.c3, a1.c2, a1.c4, a2.c5, a2.c6, a1.c7, a2.c8, a1.c9 HAVING a1.c4 <= '1995-11-08 01:18:53' ) order by c1) ) from rqg_table4 t1 left join (select c1 c11 from rqg_table4) t2 on t1.c1=t2.c11;
select t2.c11,( select avg(c1) from (select c1 from ( SELECT DISTINCT a2.c0, a1.c1, a1.c3, a1.c2, a1.c4, a2.c5, a2.c6, a1.c7, a2.c8, a1.c9 FROM ( ( SELECT DISTINCT a2.c0, a1.c1, a1.c3, a1.c2, a1.c4, a1.c5, a1.c6, a1.c7, a2.c8, a1.c9 FROM ( rqg_table4 a1 RIGHT JOIN rqg_table1 a2 ON ( NOT ( a1.c1 IS NOT NULL ) AND ( a1.c0 = a2.c1 ) ) ) RIGHT JOIN rqg_table6 a3 ON ( ( ( ( NOT ( a3.c1 = 9 ) AND ( a1.c0 = a3.c1 ) ) AND ( a1.c5 = '1971-12-07' ) ) OR ( a3.c1 = a3.c1 ) AND ( a1.c0 = a3.c0 ) ) AND ( a3.c6 != '2029-07-14 16:15:10.057656' ) ) WHERE t1.c3 LIKE 'C%' GROUP BY a2.c0, a1.c1, a1.c3, a1.c2, a1.c4, a1.c5, a1.c6, a1.c7, a2.c8, a1.c9 HAVING NOT ( a2.c0 BETWEEN 0 AND 0 + 10 ) ) a1 RIGHT JOIN rqg_table6 a2 ON ( NOT ( a2.c6 IN ( a1.c6, a1.c6 ) ) AND ( a1.c0 = a2.c1 ) ) ) INNER JOIN rqg_table1 a3 ON ( ( NOT ( a1.c0 = 9 ) AND ( a1.c0 = a3.c1 ) ) AND ( a3.c2 != a1.c2 ) ) WHERE a2.c1 NOT IN ( a3.c0, 8, a1.c0, 7, 4, 6, 6 ) AND a3.c3 not LIKE 'N9%' GROUP BY a2.c0, a1.c1, a1.c3, a1.c2, a1.c4, a2.c5, a2.c6, a1.c7, a2.c8, a1.c9 HAVING a1.c4 <= '1995-11-08 01:18:53' ) order by c1) ) from rqg_table4 t1 left join (select c1 c11 from rqg_table4) t2 on t1.c1=t2.c11 order by 1;

explain (costs off)
select * from rqg_table1 t WHERE ( t.c1 = 4 AND ( ( t.c3 IN ( SELECT c2 FROM ( SELECT a1.c0, a2.c1, a2.c3, a2.c2, a1.c4, a2.c5, a1.c6, a2.c7, a1.c8, a1.c9 FROM rqg_table5 a1 LEFT JOIN rqg_table5 a2 ON ( a2.c3 <= a2.c2 AND ( a1.c0 = a2.c0 ) ) WHERE ( a1.c8 >= 12345678912345678912345678.0000123456789123456789123456789123456789123456789 AND a1.c1 BETWEEN 1 AND 1 + 10 ) OR ( a2.c4 <= '1993-10-09 00:12:19' ) AND a1.c1 IN ( 0, 6, 7 ) ) ) AND t.c3 != t.c3 ) OR ( t.c0 = t.c1 ) AND t.c2 IN ( t.c3,'dcrnvwxbpsbpmte', t.c2, t.c2 ) ) AND ( ( 1 + 2 ) != t.c1 ) ) OR ( t.c1 = 6 ) AND t.c1 IN ( t.c1, 5, 3, t.c1, t.c0 ) and (c0,c1) not in ( select c0,c1 from rqg_table5 where (c0,c1) not in ( select c0,c1 from rqg_table4 where (c0,c1) not in ( select c0,c1 from rqg_table6 where (c0,c1) not in ( select c0,c1 from rqg_table4 where (c0,c1) not in ( select c0,c1 from rqg_table3 where (c0,c1) not in ( select c0,c1 from rqg_table6 ) ) ) )));
select * from rqg_table1 t WHERE ( t.c1 = 4 AND ( ( t.c3 IN ( SELECT c2 FROM ( SELECT a1.c0, a2.c1, a2.c3, a2.c2, a1.c4, a2.c5, a1.c6, a2.c7, a1.c8, a1.c9 FROM rqg_table5 a1 LEFT JOIN rqg_table5 a2 ON ( a2.c3 <= a2.c2 AND ( a1.c0 = a2.c0 ) ) WHERE ( a1.c8 >= 12345678912345678912345678.0000123456789123456789123456789123456789123456789 AND a1.c1 BETWEEN 1 AND 1 + 10 ) OR ( a2.c4 <= '1993-10-09 00:12:19' ) AND a1.c1 IN ( 0, 6, 7 ) ) ) AND t.c3 != t.c3 ) OR ( t.c0 = t.c1 ) AND t.c2 IN ( t.c3,'dcrnvwxbpsbpmte', t.c2, t.c2 ) ) AND ( ( 1 + 2 ) != t.c1 ) ) OR ( t.c1 = 6 ) AND t.c1 IN ( t.c1, 5, 3, t.c1, t.c0 ) and (c0,c1) not in ( select c0,c1 from rqg_table5 where (c0,c1) not in ( select c0,c1 from rqg_table4 where (c0,c1) not in ( select c0,c1 from rqg_table6 where (c0,c1) not in ( select c0,c1 from rqg_table4 where (c0,c1) not in ( select c0,c1 from rqg_table3 where (c0,c1) not in ( select c0,c1 from rqg_table6 ) ) ) )));

reset search_path;
DROP SCHEMA chqin CASCADE;

create table if not exists ss_tt1 (
c0 INTEGER,
c1 BYTEA default '',
c5 INT4 default -1,
c12 FLOAT4 default 0.1,
c18 DATE default '2022-02-16 14:25:25',
c22 INTERVAL default interval '1 hour')
partition by hash(c22) distribute by shard(c5,c12,c22);

drop table ss_tt1;

create table ss_tt1(c1 VARCHAR default '', c2 INET default '148.85.65.189/25');
create table tt2(c1 VARCHAR default '', c2 INET default '148.85.65.189/25');

explain (costs off,nodes off)
select distinct t1.c1 from ss_tt1 t1 inner join tt2 t2 on t1.c1=t2.c1;

explain (costs off,nodes off)
select distinct t1.c2 from ss_tt1 t1 inner join tt2 t2 on t1.c2=t2.c2;

drop table ss_tt1;
drop table tt2;

create table col_subquery_distinct_pd_select_replication_1
(c0 INTEGER,
 c18 DATE default '2022-02-16 14:25:25',
 c23 TIMETZ default '14:36:04+08');

create table col_subquery_distinct_pd_select_multi_shard_1
(c0 INTEGER,
 c18 DATE default '2022-02-16 14:25:25',
 c23 TIMETZ default '14:36:04+08') distribute by shard(c0);

create table col_subquery_distinct_pd_select_par_range_1
(c0 INTEGER,
 c18 DATE default '2022-02-16 14:25:25',
 c20 TIMESTAMP default now(),
 c23 TIMETZ default '14:36:04+08')
partition by range (c20)
distribute by shard(c18);

create table col_subquery_distinct_pd_select_par_range_1_part_0
partition of col_subquery_distinct_pd_select_par_range_1
for values from (minvalue) to ('2022-04-18 00:00:00');

create table col_subquery_distinct_pd_select_par_range_1_part_1
partition of col_subquery_distinct_pd_select_par_range_1
for values from ('2022-04-18 00:00:00') to ('2022-06-18 00:00:00');

create table col_subquery_distinct_pd_select_par_range_1_part_2
partition of col_subquery_distinct_pd_select_par_range_1
for values from ('2022-06-18 00:00:00') to ('2022-08-18 00:00:00');

create table col_subquery_distinct_pd_select_par_range_1_part_3
partition of col_subquery_distinct_pd_select_par_range_1
for values from ('2022-10-18 00:00:00') to ('2022-12-18 00:00:00');

explain (costs off)
select count(1) from(select distinct t1.c18,t2.c23
                     from col_subquery_distinct_pd_select_par_range_1 t1
                     join col_subquery_distinct_pd_select_replication_1 t2
                     on t1.c23=t2.c23 and t1.c18=t2.c18);

explain (costs off)
select count(1) from(select distinct t1.c18,t2.c23
                     from col_subquery_distinct_pd_select_replication_1 t1
                     join col_subquery_distinct_pd_select_multi_shard_1 t2
                     on t1.c23=t2.c23 and t1.c18=t2.c18);

drop table col_subquery_distinct_pd_select_replication_1;
drop table col_subquery_distinct_pd_select_multi_shard_1;
drop table col_subquery_distinct_pd_select_par_range_1;

explain (verbose, costs off)select * from SUBSELECT_TBL parent where (select sum(f3 * 2)/parent.f3 from SUBSELECT_TBL son where parent.f2 = son.f2) > 1 order by 1,2,3;
select * from SUBSELECT_TBL parent where (select sum(f3 * 2)/parent.f3 from SUBSELECT_TBL son where parent.f2 = son.f2) > 1 order by 1,2,3;

create table t_fn_glo1_gl(
  fee_id numeric(12,0) not null,
  fee_type numeric(3,0) not null,
  fee_status numeric(2,0) not null,
  biz_posting_id numeric(20,0),
  source_batch_id varchar(50),
  posted char(1)
);

create table t_fn_basic_voucher(
  basic_id numeric(12,0) not null,
  is_posted varchar(1),
  gl_fee_organ varchar(40) not null,
  gl_process_organ varchar(40) not null,
  biz_posting_id varchar(20),
  source_batch_id varchar(50)
);

set enable_pullup_subquery TO on;
explain (costs off, nodes off)
update t_fn_basic_voucher a 
set (biz_posting_id, source_batch_id, is_posted) =
    (select b.biz_posting_id, b.source_batch_id, b.posted from t_fn_glo1_gl b where 
      b.source_batch_id = '121212' and b.posted = 'y' and b.fee_id=a.basic_id) 
where a.is_posted = 'n'
and exists(select 1 from t_fn_glo1_gl c where c.source_batch_id = '121212' and c.posted = 'y'and c.fee_id=a.basic_id);
reset enable_pullup_subquery;
drop table t_fn_glo1_gl, t_fn_basic_voucher;

create table tt1_20231229
(
c1 integer not null,
c2 integer not null,
c3 timestamp,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

create table tt2
(
c1 integer not null,
c2 integer not null,
c3 timestamp,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

set optimize_distinct_subquery = 2;

explain (costs off)
select distinct to_char(c3, 'YYYY-YY-DD'), c3, 
                (select tt2.c1 from tt2
                 where tt2.c2=tt1_20231229.c2
                 group by tt2.c1)
from tt1_20231229;

explain (costs off)
select distinct to_char(c3, 'YYYY-YY-DD'), c2,
                to_char(c2, 'FM999'),
                (select tt2.c1 from tt2
                 where tt2.c2=tt1_20231229.c2
                 group by tt2.c1)
from tt1_20231229;

explain (costs off)
select distinct to_char(c3, 'YYYY-YY-DD'), c2,
                (select tt2.c1 from tt2
                 where tt2.c2=tt1_20231229.c2
                 group by tt2.c1), c3
from tt1_20231229;

explain (costs off)
select distinct NOW()::TIMESTAMP, c2,
                (select tt2.c1 from tt2
                 where tt2.c2=tt1_20231229.c2
                 group by tt2.c1), c3
from tt1_20231229;

explain (costs off)
select distinct to_char(sysdate, 'YYYY-YY-DD'), c3, 
                (select tt2.c1 from tt2
                 where tt2.c2=tt1_20231229.c2
                 group by tt2.c1)
from tt1_20231229;

set optimize_distinct_subquery = 1;

explain (costs off)
select distinct to_char(c3, 'YYYY-YY-DD'), c3, 
                (select tt2.c1 from tt2
                 where tt2.c2=tt1_20231229.c2
                 group by tt2.c1)
from tt1_20231229;

explain (costs off)
select distinct to_char(c3, 'YYYY-YY-DD'), c2,
                to_char(c2, 'FM999'),
                (select tt2.c1 from tt2
                 where tt2.c2=tt1_20231229.c2
                 group by tt2.c1)
from tt1_20231229;

explain (costs off)
select distinct to_char(c3, 'YYYY-YY-DD'), c2,
                (select tt2.c1 from tt2
                 where tt2.c2=tt1_20231229.c2
                 group by tt2.c1), c3
from tt1_20231229;

explain (costs off)
select distinct to_char(sysdate, 'YYYY-YY-DD'), c3, 
                (select tt2.c1 from tt2
                 where tt2.c2=tt1_20231229.c2
                 group by tt2.c1)
from tt1_20231229;

drop table tt1_20231229;
drop table tt2;

create table tt1_20240102
(
c1 integer not null,
c2 integer not null,
c3 timestamp,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

create table tt2_20240102
(
c1 integer not null,
c2 integer not null,
c3 timestamp,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

explain (costs off)
update tt1_20240102
set c2 = (select tt2_20240102.c2 from tt2_20240102 where tt2_20240102.c4 = tt1_20240102.c4)
where exists(select 1 from tt2_20240102 where tt2_20240102.c3 = tt1_20240102.c3);

explain (costs off)
select c2, (select tt2_20240102.c2 from tt2_20240102 where tt2_20240102.c4 = tt1_20240102.c4)
from tt1_20240102
where exists(select 1 from tt2_20240102 where tt2_20240102.c3 = tt1_20240102.c3);

drop table tt1_20240102;
drop table tt2_20240102;
create table tt1(c3 INT8 default 0, c4 INT2 default -1, c5 INT4 default -1, c15 varchar default '00:05:04', c19 TIME default '14:25:03');
create table tt2(c3 INT8 default 0,c4 INT2 default -1, c5 INT4 default -1, c15 varchar default '00:05:04',c19 TIME default '14:25:03');

explain (costs off)
update col_subquery_or_shard_1 tt1 set (c4,c19)=(select c3+c4,c19+interval '1 sec' from tt2 t2 where tt1.c15=t2.c15);

explain (costs off)
select (select c3+c4 from tt2 t2 where tt1.c15=t2.c15),
       (select c19+interval '1 sec' from tt2 t2 where tt1.c15=t2.c15)
from col_subquery_or_shard_1 tt1;

explain (costs off)
with sub_tep1 as(
    select distinct c3,c4 from tt1 t1
), sub_cte2 as(
   select distinct c3,count(1) over(partition by c4 order by c3) from sub_tep1
)
select t1.c19 from tt2 t1 where (t1.c3,t1.c4) not in (select * from sub_tep1 tt1 where tt1.c3=t1.c5);

drop table tt1;
drop table tt2;

create table tt1
(
c1 integer not null,
c2 integer not null,
c3 timestamp,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

create table tt2
(
c1 integer not null,
c2 integer not null,
c3 timestamp,
c4 integer
)
DISTRIBUTE BY SHARD (c1);

explain (costs off)
update tt1
set c2 = (select tt2.c2 from tt2 where tt2.c4 = tt1.c4)
where exists(select 1 from tt2 where tt2.c3 = tt1.c3);

explain (costs off)
select c2, (select tt2.c2 from tt2 where tt2.c4 = tt1.c4)
from tt1
where exists(select 1 from tt2 where tt2.c3 = tt1.c3);

explain (costs off)
select c2, (select tt2.c2 from tt2 where tt2.c4 = tt1.c4)
from tt1
where c2 in (select (select tt2.c2 from tt2
                     where tt2.c4 = tt1.c4)
             from tt2 where tt2.c3 = tt1.c3);

drop table tt1;
drop table tt2;

Create table t1_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t2_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t3_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t4_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t5_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t6_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t7_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t8_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t9_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t10_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t11_21240206(c1 int, c2 int, c3 int, c4 int);
Create table t12_21240206(c1 int, c2 int, c3 int, c4 int);

create view v1_21240206 as
Select t3_21240206.c1, t3_21240206.c2, t3_21240206.c3, t3_21240206.c4,
       (select t2_21240206.c1 from t1_21240206, t2_21240206
        where t1_21240206.c2 = t2_21240206.c2
          and t1_21240206.c3 = t3_21240206.c3) as apply
from t3_21240206,t4_21240206 where t3_21240206.c1 = t4_21240206.c1;

create view v2_21240206 as
Select t7_21240206.c1, t7_21240206.c2, t7_21240206.c3, t7_21240206.c4,
       (select t6_21240206.c1 from t5_21240206, t6_21240206
        where t5_21240206.c2 = t6_21240206.c2
          and t5_21240206.c3 = t7_21240206.c3) as apply
from t7_21240206,t8_21240206 where t7_21240206.c1 = t8_21240206.c1;

create view v3_21240206 as
Select t11_21240206.c1, t11_21240206.c2, t11_21240206.c3, t11_21240206.c4,
       (select t10_21240206.c1 from t9_21240206, t10_21240206
        where t9_21240206.c2 = t10_21240206.c2
          and t9_21240206.c3 = t11_21240206.c3) as apply
from t11_21240206,t12_21240206 where t11_21240206.c1 = t12_21240206.c1;

explain (costs off)
select x2.c1 from v1_21240206 x2
where x2.c2 in (select x.c2 from v1_21240206 x, v1_21240206 x3
                where x.c4 = x3.c4 and x3.c1 = 5);

explain (costs off)
select x2.c1 from v1_21240206 x2
where x2.c2 in (select x.c2 from v2_21240206 x, v3_21240206 x3
                where x.c4 = x3.c4 and x3.c1 = 5);

explain (costs off)
select x.c2 from v2_21240206 x, v3_21240206 x3
where x.c4 = x3.c4 and x3.c1 = 5;

drop view  v1_21240206;
drop view  v2_21240206;
drop view  v3_21240206;
drop table t1_21240206;
drop table t2_21240206;
drop table t3_21240206;
drop table t4_21240206;
drop table t5_21240206;
drop table t6_21240206;
drop table t7_21240206;
drop table t8_21240206;
drop table t9_21240206;
drop table t10_21240206;
drop table t11_21240206;
drop table t12_21240206;

create table unionlimit1(a int);
create table unionlimit2(a numeric);
-- should show: Sort Key: unionlimit1.a
-- should not show: Sort Key: unionlimit1.a USING <
explain (costs off) select a from unionlimit1 union select a from unionlimit2 order by a limit 10;
drop table unionlimit1;
drop table unionlimit2;

CREATE TABLE FEE (
FEE_C1 NUMERIC(10, 0) NOT NULL,
FEE_C2 CHARACTER(1),
FEE_C3 NUMERIC(10,0) NOT NULL
);


CREATE TABLE TT (
TT_C1 NUMERIC(2,0) NOT NULL,
TT_C2 CHARACTER VARYING(90) NOT NULL
);

CREATE TABLE TP (
T_C1 CHARACTER VARYING(100),
T_C2 CHARACTER(1) NOT NULL
);


select
count(*)
from
(
select *
from
(
select
(case when F.TT_C2='aa' then null
      else(select distinct T.T_C1
           from FEE FEE,
                TT TT,
                TP T
            where FEE.FEE_C2 = T.T_C2
              and FEE.FEE_C1=TT.TT_C1
              and TT.TT_C2 = F.TT_C2)
      end)period
from
( select A.*
  from (select FIT.TT_C2
        from TT FIT)A
  union select B.*
         from (select FIT.TT_C2
               from TT FIT) B) F) T);

drop table FEE;
drop table TT;
drop table TP;

drop table if exists t1_pk_20240506_1 cascade;
CREATE TABLE t1_pk_20240506_1(id int, name varchar(20), age int, constraint t1_pk_20240506_1_01 primary KEY(id));
INSERT INTO t1_pk_20240506_1 values(1, 'a', 10);
INSERT INTO t1_pk_20240506_1 values(2, 'b', 20);
INSERT INTO t1_pk_20240506_1 values(3, 'c', 30);

drop table t1_pk_20240506;
create table t1_pk_20240506 as select * from t1_pk_20240506_1;

explain (verbose, costs off, nodes off)select * from t1_pk_20240506 where id=(select id from (select id from t1_pk_20240506 order by id) limit 1) ;
select * from t1_pk_20240506 where id=(select id from (select id from t1_pk_20240506 order by id) limit 1) ;
drop table if exists t1_pk_20240506_1 cascade;
drop table t1_pk_20240506;

CREATE TABLE sub_test (
    empno int NOT NULL PRIMARY KEY,
    ename VARCHAR(10),
    job VARCHAR(9),
    mgr int,
    hiredate DATE,
    sal numeric(7,2),
    comm numeric(7,2),
    deptno int
) ;

SELECT de.empno
FROM sub_test de
WHERE  EXISTS (
    SELECT 1
    FROM sub_test sm
    WHERE de.empno IN (
        SELECT empno
        FROM sub_test
        WHERE mgr = sm.empno
    )
);

DROP TABLE sub_test;

create table assert_non_nestloop1(c0 integer, c5 INT4 default -1, c12 FLOAT4 default 0.1, c22 INTERVAL default interval '1 hour', c24 CIDR default '198.24.10.0/24')  distribute by shard(c5,c12,c22);
create table assert_non_nestloop2(c22 INTERVAL default interval '1 hour', c23 int default 0) distribute by shard(c22);
create table assert_non_nestloop3(c3 INT8 default 0, c18 DATE default '2022-02-16 14:25:25', c20 TIMESTAMP default now(), c22 INTERVAL default interval '1 hour') distribute by shard(c18,c20,c3);
create index assert_non_nestloop3_idx on assert_non_nestloop3(c22);
create table assert_non_nestloop4(c0 INTEGER,c15 varchar default '00:05:04', c18 DATE default '2022-02-16 14:25:25') distribute by replication;

insert INTO assert_non_nestloop1 select i, int2(i%127), ((i%200)*2.35)::float4, (date '2022-02-18' + (i%365) * interval '10 hour')::time without time zone,  concat(concat_ws('.',(i+100)%200, i%100,i%100,(i+100)%200),'/25')::inet from generate_series(1, 10000) i;

insert INTO assert_non_nestloop2 select (date '2022-02-18' + (i%365) * interval '10 hour')::time without time zone, i from generate_series(1,12000) i;

insert INTO assert_non_nestloop3 select int8(i%550),date('2022-02-17 19:12:40+08'::timestamp + (i%500+1)*interval '1 day'),(date('2022-02-18')+(i%500) * interval '1 day'+(i%24) * interval '1 minute'+(i%24) * interval '1 second')::timestamp, (date '2022-02-18' + (i%365) * interval '10 hour')::time without time zone from generate_series (1, 10200) i;

insert INTO assert_non_nestloop4 select i, 'reltime'||i%22, date('2022-02-17 19:12:40+08'::timestamp + (i%500+1)*interval '1 day') from generate_series(40, 13000) i;

-- test distinct cause assert failed --
select c15,c18,(select t1.c24 from assert_non_nestloop1 t1 join (select distinct c22 from assert_non_nestloop2 where c22 in (select c22 from assert_non_nestloop3)) t2 on t1.c22=t2.c22 where t1.c0=t3.c0) from assert_non_nestloop4 t3 where c18 between '2022-05-18' and '2022-06-18' order by 1,2,3 limit 5 offset 337;
drop table assert_non_nestloop1;
drop table assert_non_nestloop2;
drop table assert_non_nestloop3;
drop table assert_non_nestloop4;

\c regression_ora
CREATE TABLE T_CLIENT_TERMINAL (
    BUSI_DATE CHARACTER(8),
    CLIENT_ID CHARACTER VARYING(20),
    BRANCH_NO CHARACTER VARYING(6),
    FUND_ACCOUNT CHARACTER VARYING(20),
    STOCK_ACCOUNT CHARACTER VARYING(20),
    IP CHARACTER VARYING(39),
    MAC CHARACTER VARYING(128),
    MOBILE CHARACTER VARYING(20),
    HDD CHARACTER VARYING(128),
    TERMINAL_SOURCE CHARACTER(1),
    IP_N NUMERIC,
    ENTRUST_WAY CHARACTER VARYING(4)
)
WITH (checksum='on')
DISTRIBUTE BY SHARD (BUSI_DATE) ;

CREATE TABLE T_IP (
    BEGIN_IP CHARACTER VARYING(40),
    END_IP CHARACTER VARYING(40),
    BEGIN_IP_N NUMERIC,
    END_IP_N NUMERIC,
    OCEAN CHARACTER VARYING(40),
    COUNTRY CHARACTER VARYING(200),
    PROVINCE CHARACTER VARYING(100),
    CITY CHARACTER VARYING(100),
    AREA CHARACTER VARYING(100),
    AREA_CODE CHARACTER VARYING(20),
    COUNTRY_EN CHARACTER VARYING(100),
    COUNTRY_SHORT CHARACTER VARYING(20),
    COL14 NUMERIC,
    COL15 NUMERIC,
    IP_SOURCE CHARACTER VARYING(200),
    ADDRESS CHARACTER VARYING(100)
)
WITH (checksum='on')
DISTRIBUTE BY REPLICATION;

CREATE TEMPORARY TABLE TMP_WFD99760_NEW (
    STOCK_ACCOUNT CHARACTER VARYING(30),
    BEGIN_DATE CHARACTER(8),
    END_DATE CHARACTER(8),
    BRANCH_NO CHARACTER VARYING(6),
    CLIENT_ID CHARACTER VARYING(20)
)
WITH (checksum='on')
DISTRIBUTE BY SHARD (STOCK_ACCOUNT) ;

explain (costs off)
SELECT T2.BUSI_DATE,
      T2.BRANCH_NO,
      T2.CLIENT_ID,
      T2.STOCK_ACCOUNT,
      COUNT(DISTINCT T2.IP_EXCEP_REGION) IP_NUM,
      (CASE
        WHEN COUNT(DISTINCT T2.IP_EXCEP_REGION) > 1 THEN
         T2.BUSI_DATE || ':' ||RTRIM(STRING_AGG( T2.IP , ','  ORDER BY T2.BUSI_DATE, T2.BRANCH_NO, T2.CLIENT_ID, T2.STOCK_ACCOUNT, T2.IP), ',')
      END) AS IP_EXCEP,
      (CASE
        WHEN COUNT(DISTINCT T2.IP_EXCEP_REGION) > 1 THEN
         T2.BUSI_DATE
      END) AS IP_EXCEP_REGION
FROM (SELECT DISTINCT T.BUSI_DATE,
                       T.BRANCH_NO,
                       T.CLIENT_ID,
                       T.STOCK_ACCOUNT,
                       T.IP,
                       T1.ADDRESS  IP_EXCEP_REGION 
         FROM T_CLIENT_TERMINAL T
         LEFT JOIN T_IP T1
           ON T.IP_N BETWEEN T1.BEGIN_IP_N AND
              T1.END_IP_N
        INNER JOIN TMP_WFD99760_NEW T0
           ON T.BRANCH_NO = T0.BRANCH_NO
          AND T.CLIENT_ID = T0.CLIENT_ID
          AND T.STOCK_ACCOUNT = T0.STOCK_ACCOUNT
          AND T.BUSI_DATE BETWEEN T0.BEGIN_DATE AND
              T0.END_DATE
        WHERE T.TERMINAL_SOURCE = '1') T2
GROUP BY T2.BUSI_DATE,
         T2.BRANCH_NO,
         T2.CLIENT_ID,
         T2.STOCK_ACCOUNT;

drop table TMP_WFD99760_NEW;
drop table T_IP;
drop table T_CLIENT_TERMINAL;
\c regression
