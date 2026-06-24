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
explain select a.a,(select b.a from tbl_b b where b.a = a.a) q from tbl_a a order by 1,2;
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

create table test1
(   c11 integer,
    c12 integer,
    c13 integer,
    c14 integer,
    c15 integer
);

create table test2
(   c21 integer,
    c22 integer,
    c23 integer,
    c24 integer,
    c25 integer
);

explain (COSTS OFF)
select * from test1
where c11=1 or exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22);

explain (COSTS OFF)
select * from test1
where c11=1 or test1.c12 IN (select test2.c23 from test2);


explain (COSTS OFF)
select * from test1
where c11=1 or (c12=2 and exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22));


explain (COSTS OFF)
select * from test1
where (c11=1 and exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22))
   or (c12=2 and exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22));

explain (COSTS OFF)
select * from test1
where (c11=1 and test1.c12 IN (select test2.c23 from test2))
   or (c12=2 and exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22));

explain (COSTS OFF)
select * from test1
where c11=1 or not exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22);

explain (COSTS OFF)
select * from test1
where c11=1 or test1.c12 not IN (select test2.c23 from test2);


explain (COSTS OFF)
select * from test1
where c11=1 or (c12=2 and not exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22));


explain (COSTS OFF)
select * from test1
where (c11=1 and not exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22))
   or (c12=2 and exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22));

explain (COSTS OFF)
select * from test1
where (c11=1 and test1.c12 not IN (select test2.c23 from test2))
   or (c12=2 and exists (select test2.c23, test2.c24
                       from test2 where test1.c12=test2.c22));

explain (COSTS OFF)
select c11 from test1
      where case
            when test1.c11=1 THEN 1
            when test1.c11=2 THEN 2
            when exists (select test2.c23, test2.c24
                         from test2 where test1.c12=test2.c22) THEN test1.c11
            END = 1;

explain (COSTS OFF)
select c11 from test1
      where case
            when test1.c11=1 THEN 1
            when test1.c11=2 THEN 2
            when test1.c12 IN (select test2.c22
                         from test2) THEN test1.c11
            END = 1;

explain (COSTS OFF)
select c11 from test1
      where case
            when test1.c11=1 THEN 1
            when test1.c11=2 THEN 2
            when test1.c12 NOT IN (select test2.c22
                         from test2) THEN test1.c11
            END = 1;

explain (COSTS OFF)
select c11 from test1
      where case
            when test1.c11=1 THEN 1
            when test1.c11=2 THEN 2
            when exists (select test2.c23, test2.c24
                         from test2 where test1.c12=test2.c22) AND c12= 3 THEN test1.c11
            END = 1;

explain (COSTS OFF)
select c11 from test1
      where case
            when test1.c11=1 THEN 1
            when test1.c11=2 THEN 2
            when test1.c12 NOT IN (select test2.c22
                         from test2) and c12=3 THEN test1.c11
            END = 1;

explain (COSTS OFF)
select * from test1
where c11=1 or exists (select test2.c23, test2.c24
                                      from test2 where test1.c12=test2.c22)
                       or c11 in (select c21 from test2
                                       where test1.c12=test2.c22);
explain (COSTS OFF)
select c11 from test1
      where case
            when test1.c11=1 THEN 1
            when test1.c11=2 THEN 2
            when c11=1 or exists (select test2.c23, test2.c24
                                      from test2 where test1.c12=test2.c22)
                       or c11 in (select c21 from test2
                                       where test1.c12=test2.c22) THEN test1.c11
            END = 1;

drop table test1;
drop table test2;

create table tt1_20231229
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

insert into tt1_20231229
select  i,i,i,'1'
from generate_series(1, 10) as i;

insert into tt2
select  i,i,i,'1'
from generate_series(1, 10) as i;

insert into tt3
select  i,i,i,'1'
from generate_series(1, 10) as i;

explain (costs off)
update tt1_20231229 set c2 = 3
where exists (with aa as 
                (select tt3.c2
                 from tt3 
                 left join tt2 on tt2.c4 = tt3.c4
                 where tt2.c1=1 )
              select * from aa where c3=aa.c2 );

update tt1_20231229 set c2 = 3
where exists (with aa as 
                (select tt3.c2
                 from tt3 
                 left join tt2 on tt2.c4 = tt3.c4
                 where tt2.c1=1 )
              select * from aa where c3=aa.c2 );

explain (costs off) 
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=tt1_20231229.c2) as count
from tt1_20231229;

explain (costs off) 
select distinct c2, (select count(*)
                     from tt2
                     where tt2.c2=tt1_20231229.c2) as count,
                c1
from tt1_20231229;

drop table tt1_20231229;
drop table tt2;
drop table tt3;

explain (verbose, costs off)select * from SUBSELECT_TBL parent where (select sum(f3 * 2)/parent.f3 from SUBSELECT_TBL son where parent.f2 = son.f2) > 1;
select * from SUBSELECT_TBL parent where (select sum(f3 * 2)/parent.f3 from SUBSELECT_TBL son where parent.f2 = son.f2) > 1 order by 1;

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
drop table subselect_tbl;

--bugfix:JOINT_LEFT_SEMI and JOIN_LEFT_SCALAR cause pg_hint_plan to core
\c regression_ora

CREATE TABLE t_dest_tab1_20221122 AS
SELECT * FROM t_source_tab_20221122 WHERE 1=2;

CREATE TABLE t_dest_tab2_20221122 AS
SELECT * FROM t_source_tab_20221122 WHERE 1=2;

CREATE TABLE t_dest_tab3_20221122 AS
SELECT * FROM t_source_tab_20221122 WHERE 1=2;


drop table if exists t_source_tab_20221122;
drop table if exists t_dest_tab1_20221122;
drop table if exists t_dest_tab2_20221122;
drop table if exists t_dest_tab3_20221122;

--TestPoint:2.left semi join
create table t1_20220116(id integer,key integer) ;
insert into t1_20220116 select t as id,t as key from generate_series(1,100) as t; 
create table t2_20220116(id integer,key integer) ;
insert into t2_20220116 select t as id,t as key from generate_series(1,100) as t; 
analyze t1_20220116;
analyze t2_20220116;
/*+
  NoHashJoin(a b)
*/ 
explain select (select a.id from t2_20220116 a where a.id=b.id limit 1) id,b.key from t1_20220116 b;

--TestPoint:3.left scalar join
/*+
  NoHashJoin(a b)
*/
explain select (select a.id from t2_20220116 a where a.id=b.id) id,b.key from t1_20220116 b;

drop table if exists t1_20220116;
drop table if exists t2_20220116;

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

create table update_agg_sublink1(c1 int, c2 int, c3 int, c4 int);
create table update_agg_sublink2(c1 int, c2 int, c3 int, c4 int);
create table update_agg_sublink3(c1 int, c2 int, c3 int, c4 int);

set enable_pullup_expr_agg_update to off;
set enable_pullup_expr_agg_update_noqual to off;

explain (costs off) update update_agg_sublink1 set c1 =
			(select sum(c1) from update_agg_sublink2) where c2 = 1;
explain (costs off) update update_agg_sublink1 s1 set c1 =
			(select sum(c1) from update_agg_sublink2 s2 where s1.c3 = s2.c3) where c2 = 1;
explain (costs off) update update_agg_sublink1 s1 set c1 =
			(select sum(c1) from update_agg_sublink2 s2 where s1.c3 = s2.c3 and s1.c4 = s2.c4) where c2 = 1;
explain (costs off) update update_agg_sublink1 s1 set c1 =
			(select sum(c1) from (select * from update_agg_sublink2 union all select * from update_agg_sublink3) st2 where s1.c3 = st2.c3 and s1.c4 = st2.c4) where c2 = 1;


set enable_pullup_expr_agg_update to on;
set enable_pullup_expr_agg_update_noqual to off;

explain (costs off) update update_agg_sublink1 set c1 =
			(select sum(c1) from update_agg_sublink2) where c2 = 1;
explain (costs off) update update_agg_sublink1 s1 set c1 =
			(select sum(c1) from update_agg_sublink2 s2 where s1.c3 = s2.c3) where c2 = 1;
explain (costs off) update update_agg_sublink1 s1 set c1 =
			(select sum(c1) from update_agg_sublink2 s2 where s1.c3 = s2.c3 and s1.c4 = s2.c4) where c2 = 1;
explain (costs off) update update_agg_sublink1 s1 set c1 =
			(select sum(c1) from (select * from update_agg_sublink2 union all select * from update_agg_sublink3) st2 where s1.c3 = st2.c3 and s1.c4 = st2.c4) where c2 = 1;


set enable_pullup_expr_agg_update to on;
set enable_pullup_expr_agg_update_noqual to on;

explain (costs off) update update_agg_sublink1 set c1 =
			(select sum(c1) from update_agg_sublink2) where c2 = 1;
explain (costs off) update update_agg_sublink1 s1 set c1 =
			(select sum(c1) from update_agg_sublink2 s2 where s1.c3 = s2.c3) where c2 = 1;
explain (costs off) update update_agg_sublink1 s1 set c1 =
			(select sum(c1) from update_agg_sublink2 s2 where s1.c3 = s2.c3 and s1.c4 = s2.c4) where c2 = 1;
explain (costs off) update update_agg_sublink1 s1 set c1 =
			(select sum(c1) from (select * from update_agg_sublink2 union all select * from update_agg_sublink3) st2 where s1.c3 = st2.c3 and s1.c4 = st2.c4) where c2 = 1;

reset enable_pullup_expr_agg_update;
reset enable_pullup_expr_agg_update_noqual;
drop table update_agg_sublink1;
drop table update_agg_sublink2;
drop table update_agg_sublink3;

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
