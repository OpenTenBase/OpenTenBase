--
-- JOIN
-- Test JOIN clauses
--

CREATE TABLE J1_TBL (
  i integer,
  j integer,
  t text
);

CREATE TABLE J2_TBL (
  i integer,
  k integer
);


INSERT INTO J1_TBL VALUES (1, 4, 'one');
INSERT INTO J1_TBL VALUES (2, 3, 'two');
INSERT INTO J1_TBL VALUES (3, 2, 'three');
INSERT INTO J1_TBL VALUES (4, 1, 'four');
INSERT INTO J1_TBL VALUES (5, 0, 'five');
INSERT INTO J1_TBL VALUES (6, 6, 'six');
INSERT INTO J1_TBL VALUES (7, 7, 'seven');
INSERT INTO J1_TBL VALUES (8, 8, 'eight');
INSERT INTO J1_TBL VALUES (0, NULL, 'zero');
INSERT INTO J1_TBL VALUES (NULL, NULL, 'null');
INSERT INTO J1_TBL VALUES (NULL, 0, 'zero');

INSERT INTO J2_TBL VALUES (1, -1);
INSERT INTO J2_TBL VALUES (2, 2);
INSERT INTO J2_TBL VALUES (3, -3);
INSERT INTO J2_TBL VALUES (2, 4);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (0, NULL);
INSERT INTO J2_TBL VALUES (NULL, NULL);
INSERT INTO J2_TBL VALUES (NULL, 0);

analyze J1_TBL;
analyze J2_TBL;

--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--

SELECT '' AS "xxx", *
  FROM J1_TBL AS tx 
  ORDER BY i, j, t;

SELECT '' AS "xxx", *
  FROM J1_TBL tx 
  ORDER BY i, j, t;

SELECT '' AS "xxx", *
  FROM J1_TBL AS t1 (a, b, c) 
  ORDER BY a, b, c;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c)
  ORDER BY a, b, c;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e) 
  ORDER BY a, b, c, d, e;

SELECT '' AS "xxx", t1.a, t2.e
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e)
  WHERE t1.a = t2.d
  ORDER BY a, e;


--
-- CROSS JOIN
-- Qualifications are not allowed on cross joins,
-- which degenerate into a standard unqualified inner join.
--

SELECT '' AS "xxx", *
  FROM J1_TBL CROSS JOIN J2_TBL
  ORDER BY J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k;

-- ambiguous column
SELECT '' AS "xxx", i, k, t
  FROM J1_TBL CROSS JOIN J2_TBL;

-- resolve previous ambiguity by specifying the table name
SELECT '' AS "xxx", t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2
  ORDER BY i, k, t;

SELECT '' AS "xxx", ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx (ii, jj, tt, ii2, kk)
    ORDER BY ii, tt, kk;

SELECT '' AS "xxx", tx.ii, tx.jj, tx.kk
  FROM (J1_TBL t1 (a, b, c) CROSS JOIN J2_TBL t2 (d, e))
    AS tx (ii, jj, tt, ii2, kk)
    ORDER BY ii, jj, kk;

SELECT '' AS "xxx", *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b
  ORDER BY J1_TBL.i,J1_TBL.j,J1_TBL.t,a.i,a.k,b.i,b.k;


--
--
-- Inner joins (equi-joins)
--
--

--
-- Inner joins (equi-joins) with USING clause
-- The USING syntax changes the shape of the resulting table
-- by including a column in the USING clause only once in the result.
--

-- Inner equi-join on specified column
SELECT '' AS "xxx", *
  FROM J1_TBL INNER JOIN J2_TBL USING (i)
  ORDER BY i, j, k, t;

-- Same as above, slightly different syntax
SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL USING (i)
  ORDER BY i, j, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, d) USING (a)
  ORDER BY a, d;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, b) USING (b)
  ORDER BY b, t1.a;


--
-- NATURAL JOIN
-- Inner equi-join on all columns with the same name
--

SELECT '' AS "xxx", *
  FROM J1_TBL NATURAL JOIN J2_TBL
  ORDER BY i, j, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (a, d)
  ORDER BY a, b, c, d;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (d, a)
  ORDER BY a, b, c, d;

-- mismatch number of columns
-- currently, Postgres will fill in with underlying names
SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b) NATURAL JOIN J2_TBL t2 (a)
  ORDER BY a, b, t, k;


--
-- Inner joins (equi-joins)
--

SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i)
  ORDER BY J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k;

SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k)
  ORDER BY J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k;


--
-- Non-equi-joins
--

SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k)
  ORDER BY J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k;


--
-- Outer joins
-- Note that OUTER is a noise word
--

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, j, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i)
  ORDER BY i, j, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1);

--
-- semijoin selectivity for <>
--
explain (costs off)
select * from int4_tbl i4, tenk1 a
where exists(select * from tenk1 b
             where a.twothousand = b.twothousand and a.fivethous <> b.fivethous)
      and i4.f1 = a.tenthous;


--
-- More complicated constructs
--

--
-- Multiway full join
--

CREATE TABLE join_20231120_t1 (name TEXT, n INTEGER);
CREATE TABLE join_20231120_t2 (name TEXT, n INTEGER);
CREATE TABLE join_20231120_t3 (name TEXT, n INTEGER);

INSERT INTO join_20231120_t1 VALUES ( 'bb', 11 );
INSERT INTO join_20231120_t2 VALUES ( 'bb', 12 );
INSERT INTO join_20231120_t2 VALUES ( 'cc', 22 );
INSERT INTO join_20231120_t2 VALUES ( 'ee', 42 );
INSERT INTO join_20231120_t3 VALUES ( 'bb', 13 );
INSERT INTO join_20231120_t3 VALUES ( 'cc', 23 );
INSERT INTO join_20231120_t3 VALUES ( 'dd', 33 );

SELECT * FROM join_20231120_t1 FULL JOIN join_20231120_t2 USING (name) FULL JOIN join_20231120_t3 USING (name)
ORDER BY name,join_20231120_t1.n, join_20231120_t2.n, join_20231120_t3.n;

--
-- Test interactions of join syntax and subqueries
--

-- Basic cases (we expect planner to pull up the subquery here)
SELECT * FROM
(SELECT * FROM join_20231120_t2) as s2
INNER JOIN
(SELECT * FROM join_20231120_t3) s3
USING (name)
ORDER BY name, s2.n, s3.n;

SELECT * FROM
(SELECT * FROM join_20231120_t2) as s2
LEFT JOIN
(SELECT * FROM join_20231120_t3) s3
USING (name)
ORDER BY name, s2.n, s3.n;

SELECT * FROM
(SELECT * FROM join_20231120_t2) as s2
FULL JOIN
(SELECT * FROM join_20231120_t3) s3
USING (name)
ORDER BY name, s2.n, s3.n;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM join_20231120_t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM join_20231120_t3) s3
ORDER BY name, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM join_20231120_t2) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM join_20231120_t3) s3
ORDER BY name, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM join_20231120_t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM join_20231120_t3) s3
ORDER BY name, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM join_20231120_t1) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM join_20231120_t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM join_20231120_t3) s3
ORDER BY name, s1_n, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM join_20231120_t1) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM join_20231120_t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM join_20231120_t3) s3
ORDER BY name, s1_n, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s1_n FROM join_20231120_t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM join_20231120_t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM join_20231120_t3) as s3
  ) ss2
  ORDER BY name, s1_n, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s1_n FROM join_20231120_t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM join_20231120_t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM join_20231120_t3) as s3
  ) ss2
  ORDER BY name, s1_n, s2_n, s3_n;

-- Constants as join keys can also be problematic
SELECT * FROM
  (SELECT name, n as s1_n FROM join_20231120_t1) as s1
FULL JOIN
  (SELECT name, 2 as s2_n FROM join_20231120_t2) as s2
ON (s1_n = s2_n) ORDER BY 1,2,3,4;


-- Test for propagation of nullability constraints into sub-joins

create temp table x (x1 int, x2 int);
insert into x values (1,11);
insert into x values (2,22);
insert into x values (3,null);
insert into x values (4,44);
insert into x values (5,null);

create temp table y (y1 int, y2 int);
insert into y values (1,111);
insert into y values (2,222);
insert into y values (3,333);
insert into y values (4,null);

select * from x ORDER BY x1;
select * from y ORDER BY y1;

select * from x left join y on (x1 = y1 and x2 is not null) ORDER BY x1, x2, y1, y2;
select * from x left join y on (x1 = y1 and y2 is not null) ORDER BY x1, x2, y1, y2;

select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and x2 is not null) ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and y2 is not null) ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and xx2 is not null) ORDER BY x1, x2, y1, y2;
-- these should NOT give the same answers as above
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (x2 is not null)
ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (y2 is not null)
ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (xx2 is not null)
ORDER BY x1, x2, y1, y2;

--
-- regression test: check for bug with propagation of implied equality
-- to outside an IN
--
select count(*) from tenk1 a where unique1 in
  (select unique1 from tenk1 b join tenk1 c using (unique1)
   where b.unique2 = 42);

--
-- regression test: check for failure to generate a plan with multiple
-- degenerate IN clauses
--
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);

-- try that with GEQO too
begin;
set geqo = on;
set geqo_threshold = 2;
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);
rollback;

--
-- regression test: be sure we cope with proven-dummy append rels
--
explain (costs off)
select aa, bb, unique1, unique1
  from tenk1 right join b on aa = unique1
  where bb < bb and bb is null;

select aa, bb, unique1, unique1
  from tenk1 right join b on aa = unique1
  where bb < bb and bb is null;

--
-- regression test: check handling of empty-FROM subquery underneath outer join
--
explain (costs off)
select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

--
-- regression test: check a case where join_clause_is_movable_into() gives
-- an imprecise result, causing an assertion failure
--
select count(*)
from
  (select t3.tenthous as x1, coalesce(t1.stringu1, t2.stringu1) as x2
   from tenk1 t1
   left join tenk1 t2 on t1.unique1 = t2.unique1
   join tenk1 t3 on t1.unique2 = t3.unique2) ss,
  tenk1 t4,
  tenk1 t5
where t4.thousand = t5.unique1 and ss.x1 = t4.tenthous and ss.x2 = t5.stringu1;

--
-- regression test: check a case where we formerly missed including an EC
-- enforcement clause because it was expected to be handled at scan level
--
explain (costs off)
select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;


--
-- Clean up
--

DROP TABLE join_20231120_t1;
DROP TABLE join_20231120_t2;
DROP TABLE join_20231120_t3;

DROP TABLE J1_TBL;
DROP TABLE J2_TBL;

-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.

CREATE TEMP TABLE t1 (a int, b int);
CREATE TEMP TABLE t2 (a int, b int);
CREATE TEMP TABLE t3 (x int, y int);

INSERT INTO t1 VALUES (5, 10);
INSERT INTO t1 VALUES (15, 20);
INSERT INTO t1 VALUES (100, 100);
INSERT INTO t1 VALUES (200, 1000);
INSERT INTO t2 VALUES (200, 2000);
INSERT INTO t3 VALUES (5, 20);
INSERT INTO t3 VALUES (6, 7);
INSERT INTO t3 VALUES (7, 8);
INSERT INTO t3 VALUES (500, 100);

DELETE FROM t3 USING t1 table1 WHERE t3.x = table1.a;
SELECT * FROM t3 ORDER By x, y;
DELETE FROM t3 USING t1 JOIN t2 USING (a) WHERE t3.x > t1.a;
SELECT * FROM t3 ORDER By x, y;
DELETE FROM t3 USING t3 t3_other WHERE t3.x = t3_other.x AND t3.y = t3_other.y;
SELECT * FROM t3 ORDER By x, y;

-- Test join against inheritance tree

create temp table t2a () inherits (t2);

insert into t2a values (200, 2001);

select * from t1 left join t2 on (t1.a = t2.a) order by 1,2,3,4;

-- Test matching of column name with wrong alias

select t1.x from t1 join t3 on (t1.a = t3.x);

--
-- regression test for 8.1 merge right join bug
--

CREATE TEMP TABLE tt1 ( tt1_id int4, joincol int4 );
INSERT INTO tt1 VALUES (1, 11);
INSERT INTO tt1 VALUES (2, NULL);

CREATE TEMP TABLE tt2 ( tt2_id int4, joincol int4 );
INSERT INTO tt2 VALUES (21, 11);
INSERT INTO tt2 VALUES (22, 11);

set enable_hashjoin to off;
set enable_nestloop to off;

-- these should give the same results

select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol 
      ORDER BY tt1_id, tt2_id; 

select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol 
      ORDER BY tt1_id, tt2_id; 

reset enable_hashjoin;
reset enable_nestloop;

--
-- regression test for bug #13908 (hash join with skew tuples & nbatch increase)
--

set work_mem to '64kB';
set enable_mergejoin to off;
set enable_indexonlyscan to off;

explain (costs off)
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;

reset work_mem;
reset enable_mergejoin;
reset enable_indexonlyscan;

--
-- regression test for 8.2 bug with improper re-ordering of left joins
--

create temp table tt3(f1 int, f2 text);
insert into tt3 select x, repeat('xyzzy', 100) from generate_series(1,10000) x;
create index tt3i on tt3(f1);
analyze tt3;

create temp table tt4(f1 int);
insert into tt4 values (0),(1),(9999);
analyze tt4;

SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE c.f1 IS NULL
) AS d ON (a.f1 = d.f1)
WHERE d.f1 IS NULL ORDER BY f1;

--
-- regression test for proper handling of outer joins within antijoins
--

create temp table tt4x(c1 int, c2 int, c3 int);

explain (costs off)
select * from tt4x t1
where not exists (
  select 1 from tt4x t2
    left join tt4x t3 on t2.c3 = t3.c1
    left join ( select t5.c1 as c1
                from tt4x t4 left join tt4x t5 on t4.c2 = t5.c1
              ) a1 on t3.c2 = a1.c1
  where t1.c1 = t2.c2
);

--
-- regression test for problems of the sort depicted in bug #3494
--

create temp table tt5(f1 int, f2 int);
create temp table tt6(f1 int, f2 int);

insert into tt5 values(1, 10);
insert into tt5 values(1, 11);

insert into tt6 values(1, 9);
insert into tt6 values(1, 2);
insert into tt6 values(2, 9);

select * from tt5,tt6 where tt5.f1 = tt6.f1 and tt5.f1 = tt5.f2 - tt6.f2 
      ORDER BY tt5.f1, tt5.f2, tt6.f1, tt6.f2;

--
-- regression test for problems of the sort depicted in bug #3588
--

create temp table xx (pkxx int);
create temp table yy (pkyy int, pkxx int);

insert into xx values (1);
insert into xx values (2);
insert into xx values (3);

insert into yy values (101, 1);
insert into yy values (201, 2);
insert into yy values (301, NULL);

select yy.pkyy as yy_pkyy, yy.pkxx as yy_pkxx, yya.pkyy as yya_pkyy,
       xxa.pkxx as xxa_pkxx, xxb.pkxx as xxb_pkxx
from yy
     left join (SELECT * FROM yy where pkyy = 101) as yya ON yy.pkyy = yya.pkyy
     left join xx xxa on yya.pkxx = xxa.pkxx
     left join xx xxb on coalesce (xxa.pkxx, 1) = xxb.pkxx 
     ORDER BY yy_pkyy, yy_pkxx, yya_pkyy, xxa_pkxx, xxb_pkxx;

--
-- regression test for improper pushing of constants across outer-join clauses
-- (as seen in early 8.2.x releases)
--

create temp table zt1 (f1 int primary key);
create temp table zt2 (f2 int primary key);
create temp table zt3 (f3 int primary key);
insert into zt1 values(53);
insert into zt2 values(53);

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53 
ORDER BY f1, f2, f3;

create temp view zv1 as select *,'dummy'::text AS junk from zt1;

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53 
ORDER BY f1, f2, f3;

--
-- regression test for improper extraction of OR indexqual conditions
-- (as seen in early 8.3.x releases)
--

select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);

--
-- test proper positioning of one-time quals in EXISTS (8.4devel bug)
--
prepare foo(bool) as
  select count(*) from tenk1 a left join tenk1 b
    on (a.unique2 = b.unique1 and exists
        (select 1 from tenk1 c where c.thousand = b.unique2 and $1));
execute foo(true);
execute foo(false);

--
-- test for sane behavior with noncanonical merge clauses, per bug #4926
--

begin;

set enable_mergejoin = 1;
set enable_hashjoin = 0;
set enable_nestloop = 0;

create temp table a (i integer);
create temp table b (x integer, y integer);

select * from a left join b on i = x and i = y and x = i;

rollback;

--
-- test handling of merge clauses using record_ops
--
begin;

create type mycomptype as (id int, v bigint);

create temp table tidv (idv mycomptype);
create index on tidv (idv);

explain (costs off)
select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

set enable_mergejoin = 0;

explain (costs off)
select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

rollback;

--
-- test NULL behavior of whole-row Vars, per bug #5025
--
select t1.q2, count(t2.*)
from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl offset 0) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.*)
from int8_tbl t1 left join
  (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2
  on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--
-- test incorrect failure to NULL pulled-up subexpressions
--
begin;

create temp table a (
     code char not null,
     constraint a_pk primary key (code)
);
create temp table b (
     a char not null,
     num integer not null,
     constraint b_pk primary key (a, num)
);
create temp table c (
     name char not null,
     a char,
     constraint c_pk primary key (name)
);

insert into a (code) values ('p');
insert into a (code) values ('q');
insert into b (a, num) values ('p', 1);
insert into b (a, num) values ('p', 2);
insert into c (name, a) values ('A', 'p');
insert into c (name, a) values ('B', 'q');
insert into c (name, a) values ('C', null);

select c.name, ss.code, ss.b_cnt, ss.const
from c left join
  (select a.code, coalesce(b_grp.cnt, 0) as b_cnt, -1 as const
   from a left join
     (select count(1) as cnt, b.a from b group by b.a) as b_grp
     on a.code = b_grp.a
  ) as ss
  on (c.a = ss.code)
order by c.name;

rollback;

--
-- test incorrect handling of placeholders that only appear in targetlists,
-- per bug #6154
--
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

-- test the path using join aliases, too
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

--
-- test case where a PlaceHolderVar is used as a nestloop parameter
--

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

--
-- nested nestloops can require nested PlaceHolderVars
--

create temp table nt1 (
  id int primary key,
  a1 boolean,
  a2 boolean
);
create temp table nt2 (
  id int primary key,
  nt1_id int,
  b1 boolean,
  b2 boolean
);
create temp table nt3 (
  id int primary key,
  nt2_id int,
  c1 boolean
);

insert into nt1 values (1,true,true);
insert into nt1 values (2,true,false);
insert into nt1 values (3,false,false);
insert into nt2 values (1,1,true,true);
insert into nt2 values (2,2,true,false);
insert into nt2 values (3,3,false,false);
insert into nt3 values (1,1,true);
insert into nt3 values (2,2,false);
insert into nt3 values (3,3,true);

explain(num_nodes off, nodes off, costs off) 
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--
-- test case where a PlaceHolderVar is propagated into a subquery
--

explain (num_nodes off, nodes off, costs off)
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

--
-- test the corner cases FULL JOIN ON TRUE and FULL JOIN ON FALSE
--
select * from int4_tbl a full join int4_tbl b on true order by 1,2;
select * from int4_tbl a full join int4_tbl b on false order by 1,2;

--
-- test for ability to use a cartesian join when necessary
--

explain (num_nodes off, nodes off, costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  int4(sin(1)) q1,
  int4(sin(0)) q2
where q1 = thousand or q2 = thousand;

explain (num_nodes off, nodes off, costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  int4(sin(1)) q1,
  int4(sin(0)) q2
where thousand = (q1 + q2);

--
-- test ability to generate a suitable plan for a star-schema query
--

explain (costs off)
select * from
  tenk1, int8_tbl a, int8_tbl b
where thousand = a.q1 and tenthous = b.q1 and a.q2 = 1 and b.q2 = 2;

--
-- test a corner case in which we shouldn't apply the star-schema optimization
--

explain (costs off, nodes off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- variant that isn't quite a star-schema case

select ss1.d1 from
  tenk1 as t1
  inner join tenk1 as t2
  on t1.tenthous = t2.ten
  inner join
    int8_tbl as i8
    left join int4_tbl as i4
      inner join (select 64::information_schema.cardinal_number as d1
                  from tenk1 t3,
                       lateral (select abs(t3.unique1) + random()) ss0(x)
                  where t3.fivethous < 0) as ss1
      on i4.f1 = ss1.d1
    on i8.q1 = i4.f1
  on t1.tenthous = ss1.d1
where t1.unique1 < i4.f1;

--
-- test extraction of restriction OR clauses from join OR clause
-- (we used to only do this for indexable clauses)
--

explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.hundred = 4);
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.ten = 4);
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or
  ((a.unique2 = 3 or a.unique2 = 7) and b.hundred = 4);

--
-- test placement of movable quals in a parameterized join tree
--

explain (num_nodes off, nodes off, costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten = t3.ten
where t1.unique1 = 1;

explain (num_nodes off, nodes off, costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
where t1.unique1 = 1;

set enable_indexonlyscan to off;
explain (num_nodes off, nodes off, costs off)
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

explain (num_nodes off, nodes off, costs off)
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;
reset enable_indexonlyscan;

explain (num_nodes off, nodes off, costs off)
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

explain (costs off)
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--
-- test handling of potential equivalence clauses above outer joins
--

explain (num_nodes off, nodes off, costs off)
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

set enable_indexonlyscan to off;
set enable_indexscan to off;
explain (num_nodes off, nodes off, costs off)
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;
reset enable_indexonlyscan;
reset enable_indexscan;
--
-- another case with equivalence clauses above outer joins (bug #8591)
--

explain (costs off)
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

--
-- check handling of join aliases when flattening multiple levels of subquery
--
set enable_indexonlyscan to off;
set enable_mergejoin to off;

explain (verbose, costs off)
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key) order by 1;

reset enable_mergejoin;
reset enable_indexonlyscan;
--
-- test successful handling of nested outer joins with degenerate join quals
--
set enable_nestloop to on;
set enable_hashjoin to off;
set enable_mergejoin to off;

explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1) order by 1;

set enable_fast_query_shipping to off;

explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1) order by 1;

explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1) order by 1;

reset enable_fast_query_shipping;

explain (verbose, costs off)
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1 order by 4;

reset enable_nestloop;
reset enable_hashjoin;
reset enable_mergejoin;
--
-- test for appropriate join order in the presence of lateral references
--

explain (verbose, costs off, nodes off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 order by 2 desc limit 1) as ss
where t1.f1 = ss.f1;

explain (verbose, costs off, nodes off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 order by 2 desc limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

explain (verbose, costs off, nodes off)
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--
-- check a case in which a PlaceHolderVar forces join order
--

explain (verbose, costs off, nodes off)
select ss2.* from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

select ss2.* from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

--
-- test successful handling of full join underneath left join (bug #14105)
--

explain (costs off)
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--
-- test ability to push constants through outer join clauses
--

explain (num_nodes off, nodes off, costs off)
  select * from int4_tbl a left join tenk1 b on f1 = unique2 where f1 = 0;

explain (num_nodes off, nodes off, costs off)
  select * from tenk1 a full join tenk1 b using(unique2) where unique2 = 42;

--
-- test that quals attached to an outer join have correct semantics,
-- specifically that they don't re-use expressions computed below the join;
-- we force a mergejoin so that coalesce(b.q1, 1) appears as a join input
--

set enable_hashjoin to off;
set enable_nestloop to off;

explain (verbose, costs off)
  select a.q2, b.q1
    from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
    where coalesce(b.q1, 1) > 0;
select a.q2, b.q1
  from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
  where coalesce(b.q1, 1) > 0;

reset enable_hashjoin;
reset enable_nestloop;

--
-- test join removal
--

begin;

CREATE TEMP TABLE a (id int PRIMARY KEY, b_id int);
CREATE TEMP TABLE b (id int PRIMARY KEY, c_id int);
CREATE TEMP TABLE c (id int PRIMARY KEY);
CREATE TEMP TABLE d (a int, b int);
INSERT INTO a VALUES (0, 0), (1, NULL);
INSERT INTO b VALUES (0, 0), (1, NULL);
INSERT INTO c VALUES (0), (1);
INSERT INTO d VALUES (1,3), (2,2), (3,1);

-- all three cases should be optimizable into a simple seqscan
explain (verbose false, costs false, nodes false) SELECT a.* FROM a LEFT JOIN b ON a.b_id = b.id;
explain (verbose false, costs false, nodes false) SELECT b.* FROM b LEFT JOIN c ON b.c_id = c.id;
explain (verbose false, costs false, nodes false)
  SELECT a.* FROM a LEFT JOIN (b left join c on b.c_id = c.id)
  ON (a.b_id = b.id);

-- check optimization of outer join within another special join
explain (verbose false, costs false, nodes false)
select id from a where id in (
	select b.id from b left join c on b.id = c.id
);

-- check that join removal works for a left join when joining a subquery
-- that is guaranteed to be unique by its GROUP BY clause
explain (costs off)
select d.* from d left join (select * from b group by b.id, b.c_id) s
  on d.a = s.id and d.b = s.c_id;

-- similarly, but keying off a DISTINCT clause
explain (costs off)
select d.* from d left join (select distinct * from b) s
  on d.a = s.id and d.b = s.c_id;

-- join removal is not possible when the GROUP BY contains a column that is
-- not in the join condition.  (Note: as of 9.6, we notice that b.id is a
-- primary key and so drop b.c_id from the GROUP BY of the resulting plan;
-- but this happens too late for join removal in the outer plan level.)
explain (costs off)
select d.* from d left join (select * from b group by b.id, b.c_id) s
  on d.a = s.id;

-- similarly, but keying off a DISTINCT clause
explain (costs off)
select d.* from d left join (select distinct * from b) s
  on d.a = s.id;

-- check join removal works when uniqueness of the join condition is enforced
-- by a UNION
explain (costs off)
select d.* from d left join (select id from a union select id from b) s
  on d.a = s.id;

-- check join removal with a cross-type comparison operator
-- commenting out queries on replicated tables
-- as they can go either on datanode_1 or datanode_2
--explain (costs off)
--select i8.* from int8_tbl i8 left join (select f1 from int4_tbl group by f1) i4
  --on i8.q1 = i4.f1;

rollback;

create temp table parent (k int primary key, pd int);
create temp table child (k int unique, cd int);
insert into parent values (1, 10), (2, 20), (3, 30);
insert into child values (1, 100), (4, 400);

-- this case is optimizable
select p.* from parent p left join child c on (p.k = c.k) order by 1,2;
explain (verbose false, costs false, nodes false)
  select p.* from parent p left join child c on (p.k = c.k) order by 1,2;

-- this case is not
select p.*, linked from parent p
  left join (select c.*, true as linked from child c) as ss
  on (p.k = ss.k) order by p.k;
explain (verbose false, costs false, nodes false)
  select p.*, linked from parent p
    left join (select c.*, true as linked from child c) as ss
    on (p.k = ss.k) order by p.k;

-- check for a 9.0rc1 bug: join removal breaks pseudoconstant qual handling
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;
explain (verbose false, costs false, nodes false)
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;

select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;
explain (verbose false, costs false, nodes false)
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;

-- bug 5255: this is not optimizable by join removal
begin;

CREATE TEMP TABLE a (id int PRIMARY KEY);
CREATE TEMP TABLE b (id int PRIMARY KEY, a_id int);
INSERT INTO a VALUES (0), (1);
INSERT INTO b VALUES (0, 0), (1, NULL);

SELECT * FROM b LEFT JOIN a ON (b.a_id = a.id) WHERE (a.id IS NULL OR a.id > 0);
SELECT b.* FROM b LEFT JOIN a ON (b.a_id = a.id) WHERE (a.id IS NULL OR a.id > 0);

rollback;

-- another join removal bug: this is not optimizable, either
begin;

create temp table innertab (id int8 primary key, dat1 int8);
insert into innertab values(123, 42);

SELECT * FROM
    (SELECT 1 AS x) ss1
  LEFT JOIN
    (SELECT q1, q2, COALESCE(dat1, q1) AS y
     FROM int8_tbl LEFT JOIN innertab ON q2 = id) ss2
  ON true order by 1, 2, 3, 4;

rollback;

-- another join removal bug: we must clean up correctly when removing a PHV
begin;

create temp table uniquetbl (f1 text unique);

explain (costs off)
select t1.* from
  uniquetbl as t1
  left join (select *, '***'::text as d1 from uniquetbl) t2
  on t1.f1 = t2.f1
  left join uniquetbl t3
  on t2.d1 = t3.f1;

explain (costs off)
select t0.*
from
 text_tbl t0
 left join
   (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
           t1.stringu2
     from tenk1 t1
     join int4_tbl i4 ON i4.f1 = t1.unique2
     left join uniquetbl u1 ON u1.f1 = t1.string4) ss
  on t0.f1 = ss.case1
where ss.stringu2 !~* ss.case1;

select t0.*
from
 text_tbl t0
 left join
   (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
           t1.stringu2
     from tenk1 t1
     join int4_tbl i4 ON i4.f1 = t1.unique2
     left join uniquetbl u1 ON u1.f1 = t1.string4) ss
  on t0.f1 = ss.case1
where ss.stringu2 !~* ss.case1;

rollback;

-- bug #8444: we've historically allowed duplicate aliases within aliased JOINs

select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = f1; -- error
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = y.f1; -- error
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y(ff)) j on q1 = f1; -- ok

--
-- Test hints given on incorrect column references are useful
--

select t1.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t1" suggestion
select t2.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t2" suggestion
select uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, suggest both at once

--
-- Take care to reference the correct RTE
--

select atts.relid::regclass, s.* from pg_stats s join
    pg_attribute a on s.attname = a.attname and s.tablename =
    a.attrelid::regclass::text join (select unnest(indkey) attnum,
    indexrelid from pg_index i) atts on atts.attnum = a.attnum where
    schemaname != 'pg_catalog';

--
-- Test LATERAL
--

select unique2, x.*
from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
explain (costs off)
  select unique2, x.*
  from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
select unique2, x.*
from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
explain (costs off)
  select unique2, x.*
  from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
explain (costs off)
  select unique2, x.*
  from int4_tbl x cross join lateral (select unique2 from tenk1 where f1 = unique1) ss;
select unique2, x.*
from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true order by 2;
--explain (costs off)
  --select unique2, x.*
  --from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;

-- check scoping of lateral versus parent references
-- the first of these should return int8_tbl.q2, the second int8_tbl.q1
select *, (select r from (select q1 as q2) x, (select q2 as r) y) from int8_tbl order by 1,2,3;
select *, (select r from (select q1 as q2) x, lateral (select q2 as r) y) from int8_tbl order by 1,2,3;

-- lateral with function in FROM
select count(*) from tenk1 a, lateral generate_series(1,two) g;
explain (costs off)
  select count(*) from tenk1 a, lateral generate_series(1,two) g;
explain (costs off)
  select count(*) from tenk1 a cross join lateral generate_series(1,two) g;
-- don't need the explicit LATERAL keyword for functions
explain (costs off)
  select count(*) from tenk1 a, generate_series(1,two) g;

-- lateral with UNION ALL subselect
explain (num_nodes off, nodes off, costs off)
  select * from generate_series(100,200) g,
    lateral (select * from int8_tbl a where g = q1 union all
             select * from int8_tbl b where g = q2) ss;
select * from generate_series(100,200) g,
  lateral (select * from int8_tbl a where g = q1 union all
           select * from int8_tbl b where g = q2) ss;

-- lateral with VALUES
set enable_indexonlyscan to off;
explain (num_nodes off, nodes off, costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;

-- lateral with VALUES, no flattening possible
explain (num_nodes off, nodes off, costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;
reset enable_indexonlyscan;

-- lateral injecting a strange outer join condition
set enable_hashjoin to off;
set enable_mergejoin to off;

explain (num_nodes off, nodes off, costs off)
  select * from int8_tbl a,
    int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
      on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;
select * from int8_tbl a,
  int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
    on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;

reset enable_hashjoin;
reset enable_mergejoin;

-- lateral reference to a join alias variable
select * from (select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (select x) ss2(y);
select * from (select f1 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (values(x)) ss2(y) order by 1;
select * from ((select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1) j,
  lateral (select x) ss2(y);

-- lateral references requiring pullup
select * from (values(1)) x(lb),
  lateral generate_series(lb,4) x4;
select * from (select f1/1000000000 from int4_tbl) x(lb),
  lateral generate_series(lb,4) x4 order by 1,2;
select * from (values(1)) x(lb),
  lateral (values(lb)) y(lbcopy);
select * from (values(1)) x(lb),
  lateral (select lb from int4_tbl) y(lbcopy);
select * from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (values(x.q1,y.q1,y.q2)) v(xq1,yq1,yq2);
select * from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
select x.* from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
select v.* from
  (int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy)
  order by vx, vy;
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy)
  order by vx, vy;
create temp table dual();
insert into dual default values;
analyze dual;
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 from dual union all select x.q2,y.q2 from dual) v(vx,vy)
  order by vx, vy;

explain (verbose, num_nodes off, nodes off, costs off)
select * from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
select * from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
--explain (verbose, costs off)
--select * from
  --int8_tbl a left join
  --lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;
select * from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;

-- lateral can result in join conditions appearing below their
-- real semantic level
set enable_nestloop to on;
set enable_hashjoin to off;
set enable_mergejoin to off;

explain (num_nodes off, nodes off, verbose, costs off)
select * from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
select * from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true order by 1;

reset enable_nestloop;
reset enable_hashjoin;
reset enable_mergejoin;

explain (num_nodes off, nodes off, verbose, costs off)
select * from int4_tbl i left join
  lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true;
select * from int4_tbl i left join
  lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true order by 1;
explain (num_nodes off, nodes off, verbose, costs off)
select * from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;
select * from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss order by 1,2,3,4;

-- lateral reference in a PlaceHolderVar evaluated at join level
explain (num_nodes off, nodes off, verbose, costs off)
select * from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;
select * from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;

-- case requiring nested PlaceHolderVars
explain (num_nodes off, nodes off, verbose, costs off)
select * from
  int8_tbl c left join (
    int8_tbl a left join (select q1, coalesce(q2,42) as x from int8_tbl b) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select ss2.y offset 0) ss3;

-- case that breaks the old ph_may_need optimization
explain (num_nodes off, nodes off, verbose, costs off)
select c.*,a.*,ss1.q1,ss2.q1,ss3.* from
  int8_tbl c left join (
    int8_tbl a left join
      (select q1, coalesce(q2,f1) as x from int8_tbl b, int4_tbl b2
       where q1 < f1) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select * from int4_tbl i where ss2.y > f1) ss3;

-- check processing of postponed quals (bug #9041)
explain (num_nodes off, nodes off, verbose, costs off)
select * from
  (select 1 as x offset 0) x cross join (select 2 as y offset 0) y
  left join lateral (
    select * from (select 3 as z offset 0) z where z.z = x.x
  ) zz on zz.z = y.y;

-- check handling of nested appendrels inside LATERAL
select * from
  ((select 2 as v) union all (select 3 as v)) as q1
  cross join lateral
  ((select * from
      ((select 4 as v) union all (select 5 as v)) as q3)
   union all
   (select q1.v)
  ) as q2;

-- check we don't try to do a unique-ified semijoin with LATERAL
explain (verbose, costs off, nodes off)
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;

-- check proper extParam/allParam handling (this isn't exactly a LATERAL issue,
-- but we can make the test case much more compact with LATERAL)
explain (verbose, costs off)
select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where q1 = any (select q2 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0;

select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where q1 = any (select q2 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0 order by 3;

-- test some error cases where LATERAL should have been used but wasn't
select f1,g from int4_tbl a, (select f1 as g) ss;
select f1,g from int4_tbl a, (select a.f1 as g) ss;
select f1,g from int4_tbl a cross join (select f1 as g) ss;
select f1,g from int4_tbl a cross join (select a.f1 as g) ss;
-- SQL:2008 says the left table is in scope but illegal to access here
select f1,g from int4_tbl a right join lateral generate_series(0, a.f1) g on true;
select f1,g from int4_tbl a full join lateral generate_series(0, a.f1) g on true;
-- check we complain about ambiguous table references
select * from
  int8_tbl x cross join (int4_tbl x cross join lateral (select x.f1) ss);
-- LATERAL can be used to put an aggregate into the FROM clause of its query
select 1 from tenk1 a, lateral (select max(a.unique1) from int4_tbl b) ss;

-- check behavior of LATERAL in UPDATE/DELETE

create temp table xx1 as select f1 as x1, -f1 as x2 from int4_tbl;

-- error, can't do this:
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = x1) ss;
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = xx1.x1) ss;
-- can't do it even with LATERAL:
update xx1 set x2 = f1 from lateral (select * from int4_tbl where f1 = x1) ss;
-- we might in future allow something like this, but for now it's an error:
update xx1 set x2 = f1 from xx1, lateral (select * from int4_tbl where f1 = x1) ss;

-- also errors:
delete from xx1 using (select * from int4_tbl where f1 = x1) ss;
delete from xx1 using (select * from int4_tbl where f1 = xx1.x1) ss;
delete from xx1 using lateral (select * from int4_tbl where f1 = x1) ss;

-- demonstrate problem with extrememly slow join
CREATE TABLE testr (a int, b int) DISTRIBUTE BY REPLICATION;
INSERT INTO testr SELECT generate_series(1, 10000), generate_series(5001, 15000);
CREATE TABLE testh (a int, b int);
INSERT INTO testh SELECT generate_series(1, 10000), generate_series(8001, 18000);
set enable_mergejoin TO false;
set enable_hashjoin TO false;
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM testr WHERE NOT EXISTS (SELECT * FROM testh WHERE testr.b = testh.b);
SELECT count(*) FROM testr WHERE NOT EXISTS (SELECT * FROM testh WHERE testr.b = testh.b);

--
-- test LATERAL reference propagation down a multi-level inheritance hierarchy
-- produced for a multi-level partitioned table hierarchy.
--
create table pt1 (a int, b int, c varchar) partition by range(a);
create table pt1p1 partition of pt1 for values from (0) to (100) partition by range(b);
create table pt1p2 partition of pt1 for values from (100) to (200);
create table pt1p1p1 partition of pt1p1 for values from (0) to (100);
insert into pt1 values (1, 1, 'x'), (101, 101, 'y');
create table ut1 (a int, b int, c varchar);
insert into ut1 values (101, 101, 'y'), (2, 2, 'z');
explain (verbose, costs off)
select t1.b, ss.phv from ut1 t1 left join lateral
              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
                     from pt1 t2 join ut1 t3 on t2.a = t3.b) ss
              on t1.a = ss.t2a order by t1.a;
select t1.b, ss.phv from ut1 t1 left join lateral
              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
                     from pt1 t2 join ut1 t3 on t2.a = t3.b) ss
              on t1.a = ss.t2a order by t1.a;

drop table pt1;
drop table ut1;
--
-- test that foreign key join estimation performs sanely for outer joins
--

begin;

create table fkest (a int, b int, c int unique, primary key(a,b));
create table fkest1 (a int, b int, primary key(a,b));

insert into fkest select x/10, x%10, x from generate_series(1,2000) x;
insert into fkest1 select x/10, x%10 from generate_series(1,2000) x;

alter table fkest1
  add constraint fkest1_a_b_fkey foreign key (a,b) references fkest;

analyze fkest;
analyze fkest1;

explain (costs off)
select *
from fkest f
  left join fkest1 f1 on f.a = f1.a and f.b = f1.b
  left join fkest1 f2 on f.a = f2.a and f.b = f2.b
  left join fkest1 f3 on f.a = f3.a and f.b = f3.b
where f.c = 1;

rollback;

--
-- test planner's ability to mark joins as unique
--

create table j1 (id int primary key);
create table j2 (id int primary key);
create table j3 (id int);

insert into j1 values(1),(2),(3);
insert into j2 values(1),(2),(3);
insert into j3 values(1),(1);

analyze j1;
analyze j2;
analyze j3;

-- ensure join is properly marked as unique
explain (verbose, costs off)
select * from j1 inner join j2 on j1.id = j2.id;

-- ensure join is not unique when not an equi-join
explain (verbose, costs off)
select * from j1 inner join j2 on j1.id > j2.id;

-- ensure non-unique rel is not chosen as inner
explain (verbose, costs off)
select * from j1 inner join j3 on j1.id = j3.id;

-- ensure left join is marked as unique
explain (verbose, costs off)
select * from j1 left join j2 on j1.id = j2.id;

-- ensure right join is marked as unique
explain (verbose, costs off)
select * from j1 right join j2 on j1.id = j2.id;

-- ensure full join is marked as unique
explain (verbose, costs off)
select * from j1 full join j2 on j1.id = j2.id;

-- a clauseless (cross) join can't be unique
explain (verbose, costs off)
select * from j1 cross join j2;

-- ensure a natural join is marked as unique
explain (verbose, costs off)
select * from j1 natural join j2;

-- ensure a distinct clause allows the inner to become unique
explain (verbose, costs off)
select * from j1
inner join (select distinct id from j3) j3 on j1.id = j3.id;

-- ensure group by clause allows the inner to become unique
explain (verbose, costs off)
select * from j1
inner join (select id from j3 group by id) j3 on j1.id = j3.id;

drop table j1;
drop table j2;
drop table j3;

-- test more complex permutations of unique joins

create table j1 (id1 int, id2 int, primary key(id1,id2));
create table j2 (id1 int, id2 int, primary key(id1,id2));
create table j3 (id1 int, id2 int, primary key(id1,id2));

insert into j1 values(1,1),(1,2);
insert into j2 values(1,1);
insert into j3 values(1,1);

analyze j1;
analyze j2;
analyze j3;

-- ensure there's no unique join when not all columns which are part of the
-- unique index are seen in the join clause
explain (verbose, costs off)
select * from j1
inner join j2 on j1.id1 = j2.id1;

-- ensure proper unique detection with multiple join quals
explain (verbose, costs off)
select * from j1
inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2;

-- ensure we don't detect the join to be unique when quals are not part of the
-- join condition
explain (verbose, costs off)
select * from j1
inner join j2 on j1.id1 = j2.id1 where j1.id2 = 1;

-- as above, but for left joins.
explain (verbose, costs off)
select * from j1
left join j2 on j1.id1 = j2.id1 where j1.id2 = 1;

-- validate logic in merge joins which skips mark and restore.
-- it should only do this if all quals which were used to detect the unique
-- are present as join quals, and not plain quals.
set enable_nestloop to 0;
set enable_hashjoin to 0;
set enable_sort to 0;

-- create an index that will be preferred over the PK to perform the join
create index j1_id1_idx on j1 (id1) where id1 % 1000 = 1;

explain (costs off) select * from j1 j1
inner join j1 j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1;

select * from j1 j1
inner join j1 j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1;

reset enable_nestloop;
reset enable_hashjoin;
reset enable_sort;

drop table j1;
drop table j2;
drop table j3;

-- check that semijoin inner is not seen as unique for a portion of the outerrel
set enable_indexonlyscan to off;
explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from tenk1 t3
              where t3.thousand = t1.unique1 and t3.tenthous = t2.hundred)
      and t1.unique1 < 1;
reset enable_indexonlyscan;

-- ... unless it actually is unique
create table j3 as select unique1, tenthous from onek;
vacuum analyze j3;
create unique index on j3(unique1, tenthous);

explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from j3
              where j3.unique1 = t1.unique1 and j3.tenthous = t2.hundred)
      and t1.unique1 < 1;

drop table j3;

--
-- exercises for the hash join code
--

-- something wrong with squeue + subtransaction, just reset
\c

begin;

set local min_parallel_table_scan_size = 0;
set local parallel_setup_cost = 0;

-- Extract bucket and batch counts from an explain analyze plan.  In
-- general we can't make assertions about how many batches (or
-- buckets) will be required because it can vary, but we can in some
-- special cases and we can check for growth.
create or replace function find_hash(node json)
returns json language plpgsql
as
$$
declare
  x json;
  child json;
begin
  if node->>'Node Type' = 'Hash' then
    return node;
  else
    for child in select json_array_elements(node->'Plans')
    loop
      x := find_hash(child);
      if x is not null then
        return x;
      end if;
    end loop;
    return null;
  end if;
end;
$$;
create or replace function hash_join_batches(query text)
returns table (original int, final int) language plpgsql
as
$$
declare
  whole_plan json;
  hash_node json;
begin
  for whole_plan in
    execute 'explain (analyze, format ''json'') ' || query
  loop
    hash_node := find_hash(json_extract_path(whole_plan, '0', 'Plan'));
    original := hash_node->>'Original Hash Batches';
    final := hash_node->>'Hash Batches';
    return next;
  end loop;
end;
$$;

-- Make a simple relation with well distributed keys and correctly
-- estimated size.
create table simple as
  select generate_series(1, 20000) AS id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
alter table simple set (parallel_workers = 2);
analyze simple;

-- Make a relation whose size we will under-estimate.  We want stats
-- to say 1000 rows, but actually there are 20,000 rows.
create table bigger_than_it_looks as
  select generate_series(1, 20000) as id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
alter table bigger_than_it_looks set (autovacuum_enabled = 'false');
alter table bigger_than_it_looks set (parallel_workers = 2);
analyze bigger_than_it_looks;
update pg_class set reltuples = 1000 where relname = 'bigger_than_it_looks';

-- Make a relation whose size we underestimate and that also has a
-- kind of skew that breaks our batching scheme.  We want stats to say
-- 2 rows, but actually there are 20,000 rows with the same key.
create table extremely_skewed (id int, t text);
alter table extremely_skewed set (autovacuum_enabled = 'false');
alter table extremely_skewed set (parallel_workers = 2);
analyze extremely_skewed;
insert into extremely_skewed
  select 42 as id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  from generate_series(1, 20000);
DO $$
DECLARE
    v_size bigint;
BEGIN
    SELECT pg_relation_size('extremely_skewed') / 8192 INTO v_size;
    UPDATE pg_class
    SET reltuples = 2, relpages = v_size
    WHERE relname = 'extremely_skewed';
END;$$;

-- Make a relation with a couple of enormous tuples.
create table wide as select generate_series(1, 2) as id, rpad('', 320000, 'x') as t;
alter table wide set (parallel_workers = 2);

-- something wrong with savepoint, move all create table before it
create table foo as select generate_series(1, 3) as id, 'xxxxx'::text as t;
alter table foo set (parallel_workers = 0);
create table bar as select generate_series(1, 10000) as id, 'xxxxx'::text as t;
alter table bar set (parallel_workers = 2);

-- The "optimal" case: the hash table fits in memory; we plan for 1
-- batch, we stick to that number, and peak memory usage stays within
-- our work_mem budget

-- non-parallel
savepoint settings;
set local max_parallel_workers_per_gather = 0;
set local work_mem = '4MB';
explain (costs off)
  select count(*) from simple r join simple s using (id);
select count(*) from simple r join simple s using (id);
--select original > 1 as initially_multibatch, final > original as increased_batches
--  from hash_join_batches(
--$$
--  select count(*) from simple r join simple s using (id);
--$$);
rollback to settings;

-- parallel with parallel-oblivious hash join
savepoint settings;
set local max_parallel_workers_per_gather = 2;
set local work_mem = '4MB';
set local enable_parallel_hash = off;
explain (costs off)
  select count(*) from simple r join simple s using (id);
select count(*) from simple r join simple s using (id);
--select original > 1 as initially_multibatch, final > original as increased_batches
--  from hash_join_batches(
--$$
--  select count(*) from simple r join simple s using (id);
--$$);
rollback to settings;

-- parallel with parallel-aware hash join
savepoint settings;
set local max_parallel_workers_per_gather = 2;
set local work_mem = '4MB';
set local enable_parallel_hash = on;
explain (costs off)
  select count(*) from simple r join simple s using (id);
select count(*) from simple r join simple s using (id);
--select original > 1 as initially_multibatch, final > original as increased_batches
--  from hash_join_batches(
--$$
--  select count(*) from simple r join simple s using (id);
--$$);
rollback to settings;

-- The "good" case: batches required, but we plan the right number; we
-- plan for some number of batches, and we stick to that number, and
-- peak memory usage says within our work_mem budget

-- non-parallel
savepoint settings;
set local max_parallel_workers_per_gather = 0;
set local work_mem = '128kB';
explain (costs off)
  select count(*) from simple r join simple s using (id);
select count(*) from simple r join simple s using (id);
--select original > 1 as initially_multibatch, final > original as increased_batches
--  from hash_join_batches(
--$$
--  select count(*) from simple r join simple s using (id);
--$$);
rollback to settings;

-- parallel with parallel-oblivious hash join
savepoint settings;
set local max_parallel_workers_per_gather = 2;
set local work_mem = '128kB';
set local enable_parallel_hash = off;
explain (costs off)
  select count(*) from simple r join simple s using (id);
select count(*) from simple r join simple s using (id);
--select original > 1 as initially_multibatch, final > original as increased_batches
--  from hash_join_batches(
--$$
--  select count(*) from simple r join simple s using (id);
--$$);
rollback to settings;

-- parallel with parallel-aware hash join
savepoint settings;
set local max_parallel_workers_per_gather = 2;
set local work_mem = '192kB';
set local enable_parallel_hash = on;
explain (costs off)
  select count(*) from simple r join simple s using (id);
select count(*) from simple r join simple s using (id);
--select original > 1 as initially_multibatch, final > original as increased_batches
--  from hash_join_batches(
--$$
--  select count(*) from simple r join simple s using (id);
--$$);
rollback to settings;

-- The "bad" case: during execution we need to increase number of
-- batches; in this case we plan for 1 batch, and increase at least a
-- couple of times, and peak memory usage stays within our work_mem
-- budget

-- non-parallel
savepoint settings;
set local max_parallel_workers_per_gather = 0;
set local work_mem = '128kB';
explain (costs off)
  select count(*) FROM simple r JOIN bigger_than_it_looks s USING (id);
select count(*) FROM simple r JOIN bigger_than_it_looks s USING (id);
--select original > 1 as initially_multibatch, final > original as increased_batches
--  from hash_join_batches(
--$$
--  select count(*) FROM simple r JOIN bigger_than_it_looks s USING (id);
--$$);
rollback to settings;

-- parallel with parallel-oblivious hash join
savepoint settings;
set local max_parallel_workers_per_gather = 2;
set local work_mem = '128kB';
set local enable_parallel_hash = off;
explain (costs off)
  select count(*) from simple r join bigger_than_it_looks s using (id);
select count(*) from simple r join bigger_than_it_looks s using (id);
--select original > 1 as initially_multibatch, final > original as increased_batches
--  from hash_join_batches(
--$$
--  select count(*) from simple r join bigger_than_it_looks s using (id);
--$$);
rollback to settings;

-- parallel with parallel-aware hash join
savepoint settings;
set local max_parallel_workers_per_gather = 1;
set local work_mem = '192kB';
set local enable_parallel_hash = on;
explain (costs off)
  select count(*) from simple r join bigger_than_it_looks s using (id);
select count(*) from simple r join bigger_than_it_looks s using (id);
--select original > 1 as initially_multibatch, final > original as increased_batches
--  from hash_join_batches(
--$$
--  select count(*) from simple r join bigger_than_it_looks s using (id);
--$$);
rollback to settings;

-- The "ugly" case: increasing the number of batches during execution
-- doesn't help, so stop trying to fit in work_mem and hope for the
-- best; in this case we plan for 1 batch, increases just once and
-- then stop increasing because that didn't help at all, so we blow
-- right through the work_mem budget and hope for the best...

-- non-parallel
savepoint settings;
set local max_parallel_workers_per_gather = 0;
set local work_mem = '128kB';
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);
select count(*) from simple r join extremely_skewed s using (id);
--select * from hash_join_batches(
--$$
--  select count(*) from simple r join extremely_skewed s using (id);
--$$);
rollback to settings;

-- parallel with parallel-oblivious hash join
savepoint settings;
set local max_parallel_workers_per_gather = 2;
set local work_mem = '128kB';
set local enable_parallel_hash = off;
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);
select count(*) from simple r join extremely_skewed s using (id);
--select * from hash_join_batches(
--$$
--  select count(*) from simple r join extremely_skewed s using (id);
--$$);
rollback to settings;

-- parallel with parallel-aware hash join
savepoint settings;
set local max_parallel_workers_per_gather = 1;
set local work_mem = '128kB';
set local enable_parallel_hash = on;
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);
select count(*) from simple r join extremely_skewed s using (id);
--select * from hash_join_batches(
--$$
--  select count(*) from simple r join extremely_skewed s using (id);
--$$);
rollback to settings;

-- A couple of other hash join tests unrelated to work_mem management.

-- Check that EXPLAIN ANALYZE has data even if the leader doesn't participate
savepoint settings;
set local max_parallel_workers_per_gather = 2;
set local work_mem = '4MB';
set local parallel_leader_participation = off;
--select * from hash_join_batches(
--$$
--  select count(*) from simple r join simple s using (id);
--$$);
rollback to settings;

-- Exercise rescans.  We'll turn off parallel_leader_participation so
-- that we can check that instrumentation comes back correctly.

-- multi-batch with rescan, parallel-oblivious
savepoint settings;
set enable_parallel_hash = off;
set parallel_leader_participation = off;
set min_parallel_table_scan_size = 0;
set parallel_setup_cost = 0;
set parallel_tuple_cost = 0;
set max_parallel_workers_per_gather = 2;
set enable_material = off;
set enable_mergejoin = off;
set work_mem = '64kB';
explain (costs off)
  select count(*) from foo
    left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
    on foo.id < ss.id + 1 and foo.id > ss.id - 1;
select count(*) from foo
  left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
  on foo.id < ss.id + 1 and foo.id > ss.id - 1;
--select final > 1 as multibatch
--  from hash_join_batches(
--$$
--  select count(*) from foo
--    left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
--    on foo.id < ss.id + 1 and foo.id > ss.id - 1;
--$$);
rollback to settings;

-- single-batch with rescan, parallel-oblivious
savepoint settings;
set enable_parallel_hash = off;
set parallel_leader_participation = off;
set min_parallel_table_scan_size = 0;
set parallel_setup_cost = 0;
set parallel_tuple_cost = 0;
set max_parallel_workers_per_gather = 2;
set enable_material = off;
set enable_mergejoin = off;
set work_mem = '4MB';
explain (costs off)
  select count(*) from foo
    left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
    on foo.id < ss.id + 1 and foo.id > ss.id - 1;
select count(*) from foo
  left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
  on foo.id < ss.id + 1 and foo.id > ss.id - 1;
--select final > 1 as multibatch
--  from hash_join_batches(
--$$
--  select count(*) from foo
--    left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
--    on foo.id < ss.id + 1 and foo.id > ss.id - 1;
--$$);
rollback to settings;

-- multi-batch with rescan, parallel-aware
savepoint settings;
set enable_parallel_hash = on;
set parallel_leader_participation = off;
set min_parallel_table_scan_size = 0;
set parallel_setup_cost = 0;
set parallel_tuple_cost = 0;
set max_parallel_workers_per_gather = 2;
set enable_material = off;
set enable_mergejoin = off;
set work_mem = '64kB';
explain (costs off)
  select count(*) from foo
    left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
    on foo.id < ss.id + 1 and foo.id > ss.id - 1;
select count(*) from foo
  left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
  on foo.id < ss.id + 1 and foo.id > ss.id - 1;
--select final > 1 as multibatch
--  from hash_join_batches(
--$$
--  select count(*) from foo
--    left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
--    on foo.id < ss.id + 1 and foo.id > ss.id - 1;
--$$);
rollback to settings;

-- single-batch with rescan, parallel-aware
savepoint settings;
set enable_parallel_hash = on;
set parallel_leader_participation = off;
set min_parallel_table_scan_size = 0;
set parallel_setup_cost = 0;
set parallel_tuple_cost = 0;
set max_parallel_workers_per_gather = 2;
set enable_material = off;
set enable_mergejoin = off;
set work_mem = '4MB';
explain (costs off)
  select count(*) from foo
    left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
    on foo.id < ss.id + 1 and foo.id > ss.id - 1;
select count(*) from foo
  left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
  on foo.id < ss.id + 1 and foo.id > ss.id - 1;
--select final > 1 as multibatch
--  from hash_join_batches(
--$$
--  select count(*) from foo
--    left join (select b1.id, b1.t from bar b1 join bar b2 using (id)) ss
--    on foo.id < ss.id + 1 and foo.id > ss.id - 1;
--$$);
rollback to settings;

-- A full outer join where every record is matched.

-- non-parallel
savepoint settings;
set local max_parallel_workers_per_gather = 0;
explain (costs off)
     select  count(*) from simple r full outer join simple s using (id);
select  count(*) from simple r full outer join simple s using (id);
rollback to settings;

-- parallelism not possible with parallel-oblivious outer hash join
savepoint settings;
set local max_parallel_workers_per_gather = 2;
explain (costs off)
     select  count(*) from simple r full outer join simple s using (id);
select  count(*) from simple r full outer join simple s using (id);
rollback to settings;

-- An full outer join where every record is not matched.

-- non-parallel
savepoint settings;
set local max_parallel_workers_per_gather = 0;
explain (costs off)
     select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);
select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);
rollback to settings;

-- parallelism not possible with parallel-oblivious outer hash join
savepoint settings;
set local max_parallel_workers_per_gather = 2;
explain (costs off)
     select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);
select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);
rollback to settings;

-- exercise special code paths for huge tuples (note use of non-strict
-- expression and left join required to get the detoasted tuple into
-- the hash table)

-- parallel with parallel-aware hash join (hits ExecParallelHashLoadTuple and
-- sts_puttuple oversized tuple cases because it's multi-batch)
savepoint settings;
set max_parallel_workers_per_gather = 2;
set enable_parallel_hash = on;
set work_mem = '128kB';
explain (costs off)
  select length(max(s.t))
  from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);
select length(max(s.t))
from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);
--select final > 1 as multibatch
--  from hash_join_batches(
--$$
--  select length(max(s.t))
--  from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);
--$$);
rollback to settings;

rollback;

--
-- Test nestloop path suppression if the selectivity could be under estimated
--
create table nestloop_suppression1(a int, b int, c int, d varchar(20));
create table nestloop_suppression2(a int, b int, c int, d varchar(20));
create table nestloop_suppression3(a int, b int);

insert into nestloop_suppression1 select i, i+1, i+2, 'char'||i from generate_series(1,10000) i;
insert into nestloop_suppression2 select i, i+1, i+2, 'char'||i from generate_series(1,10000) i;
insert into nestloop_suppression3 select i, i+1 from generate_series(1,100) i;
create index idx_nestloop_suppression1_b on nestloop_suppression1(b);
analyze nestloop_suppression1;
analyze nestloop_suppression2;
analyze nestloop_suppression3;

set enable_hashjoin = false;
set enable_indexonlyscan to off;
set enable_indexscan to off;
explain (costs off) select t3.b from nestloop_suppression1 t1, nestloop_suppression2 t2, nestloop_suppression3 t3 
	where t1.b=2 and t1.c=3 and t1.d like 'char%' and t1.a=t2.a and t3.b>t2.a;
set enable_nestloop_suppression = true;
explain (costs off) select t3.b from nestloop_suppression1 t1, nestloop_suppression2 t2, nestloop_suppression3 t3 
	where t1.b=2 and t1.c=3 and t1.d like 'char%' and t1.a=t2.a and t3.b>t2.a;

drop table nestloop_suppression1;
drop table nestloop_suppression2;
drop table nestloop_suppression3;

create table tb1(c1 int, c2 int);
create table tb2(c1 int, c2 int) PARTITION BY list(c2);
create table tb2_list1 partition of tb2 for values in (100);

explain select 1 from tb1
left outer join (select c1 from tb2
                  where tb2.c2 = 100
                    and tb2.c2 > 200) as tb3
on tb1.c1 = tb3.c1;

explain select 1 from tb1
left outer join (select c1 from tb2
                  where tb2.c1 = 100
                    and tb2.c1 > 200) as tb3
on tb1.c1 = tb3.c1;

explain select 1 from tb1
left outer join (select c1 from tb2
                  where tb2.c1 = 100
                    and tb2.c1 = 200) as tb3
on tb1.c1 = tb3.c1;

drop table tb1;
drop table tb2;

reset enable_nestloop_suppression;
reset enable_indexscan;
reset enable_indexonlyscan;
reset enable_hashjoin;

CREATE TABLE rqg_table1 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)   distribute by shard(c0);
alter table rqg_table1 alter column c0 drop not null;
CREATE INDEX idx_rqg_table1_c1 ON rqg_table1(c1);
CREATE INDEX idx_rqg_table1_c3 ON rqg_table1(c3);
CREATE INDEX idx_rqg_table1_c5 ON rqg_table1(c5);
CREATE INDEX idx_rqg_table1_c7 ON rqg_table1(c7);
CREATE INDEX idx_rqg_table1_c9 ON rqg_table1(c9);
INSERT INTO rqg_table1 VALUES  (6, NULL, 'foo', 'foo', '1982-01-16 18:52:19', '2034-10-22 23:11:42', '1972-03-04 21:15:53.048712', '2013-11-26 11:04:23.043793', 0.6064453125, -0.123456789123457) ,  (0, 2, 'foo', 'foo', '2000-03-06 21:12:58', '2029-06-13 09:19:17', '2003-03-20', '2035-12-20 04:50:51', -1.23456789123457e+44, 1.23456789123457e+30) ,  (9, NULL, 'xmlbo', 'foo', '2010-05-11 12:38:22', '1985-08-25 14:58:27', '1973-03-22 01:32:46.047991', NULL, -1.23456789123457e+39, -0.123456789123457) ,  (2, 4, 'mlbopuyyo', NULL, '2025-06-11 03:05:26', '1991-09-20 02:41:55', '2032-03-28 12:47:16.063544', NULL, -96731136, 0.437469482421875) ,  (1, NULL, 'bar', 'bar', '1992-07-11', '1999-10-20 00:19:14', NULL, '2023-03-02 16:05:08.004696', 1.23456789123457e+39, -1.23456789123457e+43) ,  (NULL, NULL, 'bar', NULL, '1978-04-04', '1985-12-15 06:07:11', '1979-02-01 23:53:51.062078', '2021-12-11 23:07:45', -1.23456789123457e+43, -5.93236660915379e+18) ,  (9, NULL, 'foo', 'bar', '1987-09-19', '1978-02-21', '2034-11-23 16:52:42.014876', '2025-11-23 20:18:48.019448', -1.23456789123457e+44, -1336606720) ,  (5, 0, 'foo', 'foo', '2023-09-21 09:32:29', '2031-01-21 12:15:28', '1976-04-23 08:32:12.001141', '2015-08-22 16:08:05.021997', 1.23456789123457e+30, 6.5081787109375e+80) ,  (NULL, NULL, 'lbopuyyoixibkpkyoivmfshvcgmpzvdnxnxfpapiydzdpcixmfofwarsvxubcruomsozdfldaicsgheitlvdhdpuydxwpojdsnxliabrqjepfmbmgemihpfthigw', 'bar', '1994-06-21', '1976-03-24', '1972-05-25', '1975-11-11 07:05:24.001543', -0.123456789123457, 1.23456789123457e+39) ,  (7, 7, 'bopuyy', 'bar', '1988-02-06', '1998-03-02', '2010-11-17 14:52:51.063678', '2015-01-23 11:33:37', 1.23456789123457e-09, NULL) ;
CREATE TABLE rqg_table6 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)   distribute by replication;
alter table rqg_table6 alter column c0 drop not null;
CREATE INDEX idx_rqg_table6_c1 ON rqg_table6(c1);
CREATE INDEX idx_rqg_table6_c3 ON rqg_table6(c3);
CREATE INDEX idx_rqg_table6_c5 ON rqg_table6(c5);
CREATE INDEX idx_rqg_table6_c7 ON rqg_table6(c7);
CREATE INDEX idx_rqg_table6_c9 ON rqg_table6(c9);
INSERT INTO rqg_table6 VALUES  (5, 0, 'mpzvdnx', NULL, '1985-12-26 00:38:24', '1994-02-07 00:27:30', '2003-08-07 22:25:05.049399', '1997-06-16 20:39:49.034445', -0.123456789123457, 1.23456789123457e+43) ,  (5, 1, NULL, 'foo', '1983-10-08', '2023-04-01', '1979-10-17', '1987-06-09 12:04:29.064057', 0.57427978515625, 1.23456789123457e+44) ,  (1, NULL, 'bar', 'pzvdnxnxf', '2002-11-11 01:11:12', '2013-01-10', '2001-09-22 04:16:14', '2017-08-22 11:20:36', -0.123456789123457, -5.1138373768792e+18) ,  (5, 4, 'zvdnxnxfpapiydzdpcixmfofw', 'vdnxnxfpa', '1979-02-10', '1990-02-01', '1998-07-06 05:11:49.060007', '2033-05-13 12:42:41', -1.23456789123457e+44, -4.979248046875e+80) ,  (6, 4, NULL, 'dnxnxfpapiydzdpcixmfofwarsvxubcruomsoz', '2004-02-13 20:18:29', '2023-09-03', '2029-04-15', '2002-12-06 01:52:57.010271', -1.23456789123457e+44, -1.23456789123457e+30) ,  (7, 6, 'bar', 'bar', '2033-08-24', '2018-02-07 05:41:10', '2029-06-15 19:55:56.049270', '2033-02-23', NULL, -1.23456789123457e+30) ,  (4, 9, 'nxnxfpapi', 'foo', '2031-04-08', '1978-08-22', '2035-06-18 14:44:17', '2020-10-20 22:57:11', -4.81750487763001e+124, 3.86383056640625e+80) ,  (4, 3, NULL, NULL, '2033-02-19 06:06:12', '2032-09-06', '1973-05-19 19:22:56.048458', '1977-10-08', -2.54028319566528e+124, -1.3995361328125e+80) ,  (9, 5, 'foo', 'bar', '2019-12-14 23:02:18', '1972-12-12 14:32:22', '2001-03-28 21:13:39.037798', '2025-07-04', 1.23456789123457e-09, -6.1541748046875e+80) ,  (8, 4, 'xnxfpapiydzdpcixmfofwarsvxubcruomsozdfldaicsgheitlvdhdpuydxwpojdsnxliabrqjepfmbmgemihpfthigwckkovxupsbqpekxcpuatmrftuuivlmuqiuurqojgh', 'nxfp', '1978-10-22 01:40:49', '2008-04-04', NULL, NULL, 1.23456789123457e+30, 1.23456789123457e+44) ;

set enable_seqscan = off;
set enable_indexscan = off;

drop table if exists rqg_table4;
CREATE TABLE rqg_table4 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)   distribute by shard(c3);
alter table rqg_table4 alter column c0 drop not null;
CREATE INDEX idx_rqg_table4_c1 ON rqg_table4(c1);
CREATE INDEX idx_rqg_table4_c3 ON rqg_table4(c3);
CREATE INDEX idx_rqg_table4_c5 ON rqg_table4(c5);
CREATE INDEX idx_rqg_table4_c7 ON rqg_table4(c7);
CREATE INDEX idx_rqg_table4_c9 ON rqg_table4(c9);
INSERT INTO rqg_table4 VALUES  (NULL, 4, 'foo', NULL, '1985-11-25 21:57:41', '2012-01-19 17:02:33', '2034-10-03 20:24:02.061413', '1987-05-26', 0.881423950195312, 1.23456789123457e+30) ,  (1, 9, 'bar', NULL, '1977-10-19', '1979-06-09', NULL, '1974-10-28 19:32:45', 1.23456789123457e+30, -1.23456789123457e+44) ,  (2, NULL, 'foo', 'bar', '2016-05-27 20:59:31', '1980-02-21', '2019-11-12 18:44:33', '2022-08-14 13:08:06.024802', 1.23456789123457e+43, 0.123456789123457) ,  (4, 7, 'lixijtqhzvfmfgsouvzcntxxugyvdrqhhrssjbpdflvbvarlgsuwmolgymptcbfdjyds', 'ixijtqhzvfmfgsouvzcntxxugyvdrqhhrssjbpdflvbvarlgsuwmolgymptcbfdj', '1983-12-23 17:09:01', '2012-08-25', '2025-01-14 06:40:38.039286', '2026-07-11', -1.23456789123457e-09, 1.23456789123457e+44) ,  (2, 2, NULL, NULL, '1991-01-23 01:21:39', '2017-02-20 15:39:23', '2007-08-06 14:40:11', '2000-02-23 16:25:21', -1.23456789123457e+25, -1.23456789123457e+43) ,  (8, 2, 'xijtqhzvfmfgsouvzcntxxugyvdrqhhrssjbpdflvbvarlgsuwmolgymptcbfdjydsrqmhapcflxfyagbalbcbjokujrifbolqjyymlkogflnpdwipokiluwgjdbltqivlmrloeaqlwuwhoukbmcqm', 'bar', '1987-03-26 05:21:59', '2008-12-17', '2024-05-06 07:37:53.048135', '2022-09-17 22:51:12.064065', -1.23456789123457e+25, 2.40936279296875e+80) ,  (9, 8, NULL, 'bar', '1972-09-09 17:41:09', '2023-11-14', '2023-10-22 22:59:21', '1979-07-10 00:13:19.020138', 1.23456789123457e+25, 0.123456789123457) ,  (NULL, NULL, NULL, 'bar', '2007-03-27 22:04:23', '1992-01-24 12:02:35', '2027-04-13', '1981-03-22 01:48:33.016070', 0.88189697265625, -3.86324405535375e+18) ,  (0, NULL, 'bar', NULL, '2028-04-01 04:31:07', '1998-07-05 16:08:05', '1981-04-25', '2023-01-06 12:42:41.034613', 1.23456789123457e+25, 0.123456789123457) ,  (0, 5, 'bar', 'i', '1999-09-28', '1983-02-01 12:18:37', '1990-05-26 18:04:19.009021', '1993-10-27 19:25:34', 0.123456789123457, 0.5126953125) ;
drop table if exists rqg_table6;
CREATE TABLE rqg_table6 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)   distribute by replication;
alter table rqg_table6 alter column c0 drop not null;
CREATE INDEX idx_rqg_table6_c1 ON rqg_table6(c1);
CREATE INDEX idx_rqg_table6_c3 ON rqg_table6(c3);
CREATE INDEX idx_rqg_table6_c5 ON rqg_table6(c5);
CREATE INDEX idx_rqg_table6_c7 ON rqg_table6(c7);
CREATE INDEX idx_rqg_table6_c9 ON rqg_table6(c9);
INSERT INTO rqg_table6 VALUES  (8, 1, 'v', 'bar', '1973-12-09', '1974-10-15', '1985-01-26 06:26:28', '2024-09-27', 1.23456789123457e+43, 1.23456789123457e+44) ,  (NULL, 7, NULL, 'fmfgsouvz', '1977-03-06 13:22:11', '2024-07-04 15:41:53', '1992-11-22 00:48:55.031345', '2028-07-16 15:28:17.008752', NULL, NULL) ,  (2, 2, 'bar', 'mfgsouvzcntxxugyvdrqhhrssjbpdflvbvarlgsuwmolgymptcbfdjydsrqmhapcflxfyagbalbcbjokujrifbolqjyymlkogflnpdwipokiluwgjdbltqivlmrloeaqlwuwhou', '1996-09-03', '1990-02-09 00:37:47', '2022-08-25 11:04:36.008903', '1976-04-22 05:48:26.034826', -8.13547125186809e+18, 1.23456789123457e+25) ,  (NULL, NULL, 'fgsouv', 'gsouvzcntxxugyvdrqhhrssjbpdflvbvarlgsuwmolgymptcbfdjydsrqmhapcflxfyagbalbcbjokujrifbolqjyymlkogflnpdwipokiluwgjdbltqivlmrloeaqlwuwhou', '1975-08-19 00:51:09', '2003-09-25', NULL, '2026-12-02 10:34:34.019304', -2.73593677362758e+18, -1.23456789123457e+44) ,  (2, NULL, 'souvzcn', 'ouvzcntxxugyvdrqhhrssjbpdflvbvarlgsuwmolgymptcbf', '1976-01-09', '1999-08-05 08:49:36', '2007-07-02 23:16:01.039734', '1986-03-13', 1.23456789123457e+43, 1.23456789123457e-09) ,  (0, 5, 'foo', 'uvzcn', '1989-11-12 14:45:26', '2003-05-06 05:01:19', '2004-12-26 22:26:18.029048', '2024-11-02 14:36:48', -5.31959558485469e+18, 0.123456789123457) ,  (NULL, 2, 'bar', 'bar', '2021-05-14', '2016-04-17', '1989-08-07', '1983-05-26', -1.23456789123457e+30, -1.23456789123457e-09) ,  (9, NULL, 'bar', 'foo', '1998-08-19', '1979-11-20', '2026-08-08 09:21:42.026149', '2034-12-04 13:35:11.053542', 1913454592, 1.23456789123457e+39) ,  (NULL, 1, 'foo', 'vzcntxxugyvdrqhhrssjbpdflvbvarlgsuwmolgymptcbfdjydsrqmhapcflxfyagbalbcbjokujrifbolqjyymlkogflnpdwipokiluwgjdbltqivlmrloeaqlwuwhoukbmcqmwykqyjxebsajruafshdqzafcnlcbozucrhxbbawwonsaivqjosudqanoqsgqar', '1978-08-16', '1999-09-17 10:43:06', '1989-10-19', '1982-02-10 11:55:58.006975', -574750720, NULL) ,  (3, NULL, 'z', 'bar', '2013-03-15', '1978-02-01', '1989-06-23 01:37:07.024905', '1977-01-03 01:32:21.021484', -1.23456789123457e+25, NULL) ;
drop table if exists rqg_table12;
CREATE TABLE rqg_table12 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)    PARTITION BY hash( c9) distribute by replication;
create TABLE rqg_table12_p0 partition of rqg_table12 for values with(modulus 4,remainder 0);
create TABLE rqg_table12_p1 partition of rqg_table12 for values with(modulus 4,remainder 1);
create TABLE rqg_table12_p2 partition of rqg_table12 for values with(modulus 4,remainder 2);
create TABLE rqg_table12_p3 partition of rqg_table12 for values with(modulus 4,remainder 3);
alter table rqg_table12 alter column c0 drop not null;
CREATE INDEX idx_rqg_table12_c1 ON rqg_table12(c1);
CREATE INDEX idx_rqg_table12_c3 ON rqg_table12(c3);
CREATE INDEX idx_rqg_table12_c5 ON rqg_table12(c5);
CREATE INDEX idx_rqg_table12_c7 ON rqg_table12(c7);
CREATE INDEX idx_rqg_table12_c9 ON rqg_table12(c9);
INSERT INTO rqg_table12 VALUES  (NULL, 5, 'bar', 'bar', '1973-05-23 03:39:38', '2031-09-04', '2028-06-21 18:17:15', '2003-08-01', -0.123456789123457, 6.07957802197346e+18) ,  (5, 5, NULL, 'bar', '2028-12-21', '2002-07-05 20:34:19', '1973-02-18 01:02:35.024329', '1982-07-03', -1.23456789123457e+43, 1.23456789123457e-09) ,  (7, 9, NULL, 'foo', '1985-10-28 13:53:24', '2002-10-02 21:54:27', '2002-03-28 17:12:07.021140', '1972-07-15 21:21:35.060278', -6.37062314789228e+18, 0.123456789123457) ,  (8, 6, 'bfdjydsrqmhapcflxfyagbalbcbjokujrifbolqjyymlkogflnpdwipokiluwgjdbltqivlmrloeaqlwuwhou', 'bar', '2017-07-28 16:26:42', '1997-11-25 02:25:53', '2003-06-26 00:04:10.005657', '2002-06-12 11:52:59.006766', 1.23456789123457e+30, 1.23456789123457e+39) ,  (5, 6, 'bar', NULL, '2001-07-04', '1973-08-13 17:51:42', '2020-01-08 11:46:37.056358', '2005-02-11', -1.23456789123457e+44, 0.123456789123457) ,  (NULL, 6, NULL, NULL, '2019-06-11 00:28:47', '1997-08-06', '1985-04-13 04:49:00.047978', '1980-05-04 07:33:03.060762', 1.23456789123457e-09, 8.81410742071748e+18) ,  (2, 5, 'foo', 'fdjydsrq', '2004-12-17 12:59:58', '2026-06-20', '2024-11-15 19:08:15.020034', '1993-04-04 22:29:05.030482', 1.23456789123457e+39, 0.50250244140625) ,  (7, 7, 'djydsrqmhapcflxfyagbalbcbjokujrifbolqjyymlkogflnpdwipokiluwgjdbltqivlmrloeaqlwuwhoukbmcqmwykqyjxebsajruafshdqzafcnlcbozucrhxbbawwonsaivqjosudqanoqsgqarxfyxlglkhrefqckcqkejcqci', 'jydsrq', '1984-09-13', '2033-03-26', '2023-04-15 13:18:45.009238', NULL, -7.82021927795216e+18, -7.13145000994118e+18) ,  (2, NULL, 'yds', NULL, '1993-07-07 04:26:19', '1985-12-08 17:38:19', '1994-08-16 15:41:13.043276', '1981-08-18', 1.23456789123457e+39, 1.23456789123457e+44) ,  (0, 9, 'dsrqmhapcflxfyagbalbcbjokujrifbolqjyymlkogflnpdwipokiluwgjdbltqivlmrloeaqlwuwhoukbmcqmwykqyjxebsajruafshdqzafcnlcbozucrhxbbawwonsaivqjosudqa', 's', '2029-02-13', '2006-12-08 16:20:22', '1982-04-16', '2014-02-14 15:24:57', -1.23456789123457e+39, 0.07537841796875) ;
analyze rqg_table4;
analyze rqg_table6;
analyze rqg_table12;
explain (costs off)
select 
    1
from rqg_table12 t1 where c0 in (select c1 from rqg_table4 t4 where not exists(select 1 from rqg_table6 t2 where t2.c0 = t4.c0 AND t4.c0 = 7  ) ) and c1=2;
drop table rqg_table4, rqg_table6, rqg_table12;

set enable_hashjoin=on;
set enable_mergejoin=off;

drop function if exists get_random_date(start_date date) cascade;
create function get_random_date(start_date date) RETURNS date
as
$$
begin
 if start_date is not null then
 RETURN start_date;
 else
 RETURN date_trunc('day', now() - interval '1 day' * (random() * 365))::date;
 end if;
end;
$$
language plpgsql stable;

drop table if exists jt1;
drop table if exists jt2;
create table jt1(c0 int, c1 int, c2 int, c3 date);
create table jt2(c0 int, c1 int, c2 int, c3 date);
explain (costs off,verbose) select * from jt1 left join  jt2 s on s.c3=get_random_date('2020-02-10') and jt1.c2=s.c2;

explain (costs off,verbose) select * from jt1 left join (select * from jt2 where c3=get_random_date('2020-02-10')) s on jt1.c2=s.c2;

explain (costs off,verbose) select * from (select * from jt1 where c3=get_random_date('2020-02-10')) jt1 left join jt2 s on jt1.c2=s.c2;

explain (costs off,verbose) select * from jt1 left join  jt2 s on jt1.c3=get_random_date('2020-02-10') and jt1.c2=s.c2;

explain (costs off,verbose) select * from jt1 left join  jt2 s on s.c3=get_random_date('2020-02-10') and jt1.c2=s.c2;

explain (costs off,verbose) select c1 from jt1 where c2 in (select c2 from jt2 where c3=get_random_date('2022-01-20'));

explain (costs off,verbose) select c1 from jt1 where c2 in (select c2 from jt1 where c3=get_random_date('2022-01-20'));

drop table if exists jt1;
drop table if exists jt2;

drop function if exists get_random_date(start_date date) cascade;

drop function if exists pullupf(x int);
create function pullupf(x int) returns int language plpgsql AS
$$
begin
    return x;
end $$ not pushdown;

drop table if exists ft1;
drop table if exists ft2;
drop table if exists ft3;
create table ft1(c0 int, c1 int, c2 int, c3 int);
create table ft2(c0 int, c1 int, c2 int, c3 int);
create table ft3(c0 int, c1 int, c2 int, c3 int);

explain (costs off)
select * from ft1 left join (select ft2.* from ft2 join (select * from ft3 where c2 = pullupf(c2)) fmm on ft2.c1=fmm.c1) fm on ft1.c2 = fm.c2;

drop function if exists pullupfs(x int);
create function pullupfs(x int) returns int language plpgsql AS
$$
begin
    return x;
end $$ not pushdown stable;

set enable_hashagg to off;
explain (costs off)
select count(1) from ft1 t1 where t1.c1=10 and exists (select c2 from ft2 t2 where t1.c3=t2.c3 and t2.c1=pullupfs(1));
reset enable_hashagg;
drop function if exists pullupf(x int);
drop function if exists pullupfs(x int);
drop table if exists ft1;
drop table if exists ft2;
drop table if exists ft3;
