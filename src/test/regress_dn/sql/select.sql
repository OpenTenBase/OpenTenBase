--
-- SELECT
--


-- btree index
-- awk '{if($1<10){print;}else{next;}}' onek.data | sort +0n -1
--
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY stringu1 using <;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1d -2 +0nr -1
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using <, unique1 using >;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1dr -2 +0n -1
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using >, unique1 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0nr -1 +1d -2
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >, string4 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0n -1 +1dr -2
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using <, string4 using >;

--
-- test partial btree indexes
--
-- As of 7.2, planner probably won't pick an indexscan without stats,
-- so ANALYZE first.  Also, we want to prevent it from picking a bitmapscan
-- followed by sort, because that could hide index ordering problems.
--
ANALYZE onek2;

SET enable_seqscan TO off;
SET enable_bitmapscan TO off;
SET enable_sort TO off;

--
-- awk '{if($1<10){print $0;}else{next;}}' onek.data | sort +0n -1
--
SELECT onek2.* FROM onek2 WHERE onek2.unique1 < 10 ORDER BY unique1;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
SELECT onek2.unique1, onek2.stringu1 FROM onek2
    WHERE onek2.unique1 < 20
    ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
SELECT onek2.unique1, onek2.stringu1 FROM onek2
   WHERE onek2.unique1 > 980 
   ORDER BY unique1 using <;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;


SELECT two, stringu1, ten, string4
   INTO TABLE tmp
   FROM onek;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=2){print $4,$5;}else{print;}}' - stud_emp.data
--
-- SELECT name, age FROM person*; ??? check if different
SELECT p.name, p.age FROM person* p 
    ORDER BY p.name, p.age;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=1){print $4,$5;}else{print;}}' - stud_emp.data |
-- sort +1nr -2
--
SELECT p.name, p.age FROM person* p ORDER BY age using >, name;

--
-- Test some cases involving whole-row Var referencing a subquery
--
select foo from (select 1) as foo;
select foo from (select null) as foo;
select foo from (select 'xyzzy',1,null) as foo;

--
-- Test VALUES lists
--
select * from onek, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE onek.unique1 = v.i and onek.stringu1 = v.j 
    ORDER BY unique1;

-- a more complex case
-- looks like we're coding lisp :-)
select * from onek,
  (values ((select i from
    (values(10000), (2), (389), (1000), (2000), ((select 10029))) as foo(i)
    order by i asc limit 1))) bar (i)
  where onek.unique1 = bar.i 
  ORDER BY unique1;

-- try VALUES in a subquery
select * from onek
    where (unique1,ten) in (values (1,1), (20,0), (99,9), (17,99))
    order by unique1;

-- VALUES is also legal as a standalone query or a set-operation member
VALUES (1,2), (3,4+4), (7,77.7);

VALUES (1,2), (3,4+4), (7,77.7)
UNION ALL
SELECT 2+2, 57
UNION ALL
TABLE int8_tbl 
ORDER BY column1,column2;

--
-- Test ORDER BY options
--

CREATE TEMP TABLE foo (f1 int);

INSERT INTO foo VALUES (42),(3),(10),(7),(null),(null),(1);

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 ASC;	-- same thing
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

-- check if indexscans do the right things
CREATE INDEX fooi ON foo (f1);
SET enable_sort = false;

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

DROP INDEX fooi;
CREATE INDEX fooi ON foo (f1 DESC);

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

DROP INDEX fooi;
CREATE INDEX fooi ON foo (f1 DESC NULLS LAST);

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--
-- Test planning of some cases with partial indexes
--

-- partial index is usable
explain (costs off)
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- get the plan for the following query
explain (costs off, timing off, summary off)
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- actually run the query with an analyze to use the partial index
explain (costs off, analyze on, timing off, summary off)
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- partial index predicate implies clause, so no need for retest
explain (costs off)
select * from onek2 where unique2 = 11 and stringu1 < 'B';
select * from onek2 where unique2 = 11 and stringu1 < 'B';
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
-- but if it's an update target, must retest anyway
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
-- partial index is not applicable
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
-- partial index implies clause, but bitmap scan must recheck predicate anyway
SET enable_indexscan TO off;
SET enable_bitmapscan TO off;
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
RESET enable_indexscan;
RESET enable_bitmapscan;
-- check multi-index cases too
explain (costs off)
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
explain (costs off)
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;

--
-- Test some corner cases that have been known to confuse the planner
--

-- ORDER BY on a constant doesn't really need any sorting
SELECT 1 AS x ORDER BY x;

-- But ORDER BY on a set-valued expression does
create function sillysrf(int) returns setof int as
  'values (1),(10),(2),($1)' language sql immutable;

select sillysrf(42) order by 1;
select sillysrf(-1) order by 1;

drop function sillysrf(int);

-- X = X isn't a no-op, it's effectively X IS NOT NULL assuming = is strict
-- (see bug #5084)
select * from (values (2),(null),(1)) v(k) where k = k order by k;
select * from (values (2),(null),(1)) v(k) where k = k order by k desc;

-- Test partitioned tables with no partitions, which should be handled the
-- same as the non-inheritance case when expanding its RTE.
create table list_parted_tbl (a int,b int) partition by list (a);
create table list_parted_tbl1 partition of list_parted_tbl
  for values in (1) partition by list(b);
explain (costs off) select * from list_parted_tbl;
drop table list_parted_tbl;

create table tt1 (
    c11 integer,
    c12 integer,
    c13 integer,
    c14 integer,
    c15 integer
);

insert into tt1
select  i,i,i,i,i
from generate_series(1, 50) as i;

explain
select c12
from tt1
order by c12
for update
limit 10;

select c12
from tt1
order by c12
for update
limit 10;

drop table tt1;

create table tt1
(
c1 int,
c2 int,
c3 int
);

insert into tt1 values(1,1,1);
insert into tt1 values(1,2,1);

select distinct max(c3) over(partition by c2) from tt1 order by 1;

drop table tt1;

explain (costs off, verbose) select distinct(1,'a');
select distinct(1,'a');

set enable_hashagg = off;

create table tt1(site text, pt_d text, cnty_code text) DISTRIBUTE BY SHARD (pt_d,cnty_code);
create table tt2(site text, pt_d text, cnty_code text) DISTRIBUTE BY SHARD (pt_d,cnty_code);

explain (costs off, verbose)
select t3.site
from (select site, pt_d  from tt1 group by 1,2) as t3
left outer join
     (select pt_d, site  from tt2 group by 1,2) as t4
on t3.site=t4.site and t3.pt_d=t4.pt_d
group by 1;

drop table tt1;
drop table tt2;

set enable_hashagg = on;

CREATE TABLE tt1 (
c0 int not null,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date
);

CREATE TABLE tt2 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date);

INSERT INTO tt1 VALUES  (9, 2, NULL, 'bar', '1993-04-07', '2022-04-09');
INSERT INTO tt1 VALUES  (8, 2, NULL, 'bar', '1993-04-07', '2022-04-09');

SELECT DISTINCT a1.c0
FROM ( tt1 a1 
       LEFT JOIN tt2 a2
       ON ( NOT ( a2.c5 >= a1.c4 ) AND ( a1.c0 = a2.c1 ) ) )
     RIGHT JOIN tt1 a3
     ON ( ( NOT ( a3.c1 = 9 )
      AND ( a1.c0 = a3.c1 ) )
      AND ( a3.c2 LIKE 'C%' ) )
WHERE a1.c0 IS NULL;

drop table tt1;
drop table tt2;

\c regression_ora
-- test 'select unique xxx from table'
create table t_unique(f1 varchar(10),f2 int);
insert into t_unique values('test1',1);
insert into t_unique values('test2',1);
insert into t_unique values('test1',1);
insert into t_unique values('test3',1);
insert into t_unique values('test2',1);
select unique f1 from t_unique order by 1;
select count(unique f1) from t_unique;
drop table t_unique;

create table test_unique1(id int, c0 int, c1 number , c2 varchar(100), c3 varchar2(100));
insert into test_unique1(id, c0, c1, c2, c3) values(1,123, 123.123, 'abc', '123');
insert into test_unique1(id, c0, c1, c2, c3) values(2,234, 234.234, 'bcd', '234');
insert into test_unique1(id, c0, c1, c2, c3) values(3,345, 345.345, 'cde', '345');
insert into test_unique1(id, c0, c1, c2, c3) values(4,456, 234.234, 'def', '456');
insert into test_unique1(id, c0, c1, c2, c3) values(5,567, 567.567, 'efg', '567');
insert into test_unique1(id, c0, c1, c2, c3) values(6,567, 567.567, 'fgh', '567');
insert into test_unique1(id, c0, c1, c2, c3) values(7,678, 678.678, 'ghi', '789');
insert into test_unique1(id, c0, c1, c2, c3) values(8,678, 678.678, '', '789');
insert into test_unique1(id, c0, c1, c2, c3) values(9,678, 678678.8123, '', '8423');
insert into test_unique1(id, c0, c1, c2) values(10,789, 789.789, 'hij');
insert into test_unique1(id, c0, c1, c2) values(11,890, 123.123, 'ijk');
insert into test_unique1(id, c0, c1, c2) values(12,901, 901.901, 'abc');
create table test_unique2(id int, c0 int, c1 number , c2 varchar(100), c3 varchar2(100));
insert into test_unique2(id, c0, c1, c2, c3) values(1,123, 123.123, 'abc', '123');
insert into test_unique2(id, c0, c1, c2, c3) values(2,234, 234.234, 'bcd', '234');
insert into test_unique2(id, c0, c1, c2, c3) values(3,345, 345.345, 'cde', '345');
insert into test_unique2(id, c0, c1, c2, c3) values(4,456, 234.234, 'def', '456');
insert into test_unique2(id, c0, c1, c2, c3) values(5,567, 567.567, 'efg', '567');
insert into test_unique2(id, c0, c1, c2, c3) values(6,567, 567.567, 'fgh', '567');
insert into test_unique2(id, c0, c1, c2, c3) values(7,678, 678.678, 'ghi', '789');
insert into test_unique2(id, c0, c1, c2, c3) values(8,678, 678.678, '', '789');
insert into test_unique2(id, c0, c1, c2, c3) values(9,678, 678678.8123, '', '8423');
insert into test_unique2(id, c0, c1, c2) values(10,789, 789.789, 'hij');
insert into test_unique2(id, c0, c1, c2) values(11,890, 123.123, 'ijk');
insert into test_unique2(id, c0, c1, c2) values(12,901, 901.901, 'abc');
select * from test_unique2 where c3 in (select unique c3 from test_unique1) order by id;
select unique sin(c3) from (select unique c3 from test_unique1) order by 1;
drop table test_unique1;
drop table test_unique2;


-- Test partitioned tables with no partitions, which should be handled the
-- same as the non-inheritance case when expanding its RTE.
create table list_parted_tbl (a int,b int) partition by list (a);
create table list_parted_tbl1 partition of list_parted_tbl
  for values in (1) partition by list(b);
explain (costs off) select * from list_parted_tbl;
drop table list_parted_tbl;

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

explain (costs off)
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

CREATE TABLE rqg_table10 (
c0 int,
c1 int,
pk int)    PARTITION BY hash(pk) ;
create TABLE rqg_table10_p0 partition of rqg_table10 for values with(modulus 4,remainder 0);
create TABLE rqg_table10_p1 partition of rqg_table10 for values with(modulus 4,remainder 1);
create TABLE rqg_table10_p2 partition of rqg_table10 for values with(modulus 4,remainder 2);
create TABLE rqg_table10_p3 partition of rqg_table10 for values with(modulus 4,remainder 3);

select t1.c0, t1.c1
from rqg_table10 t1
left outer join rqg_table10 t2
on t1.c1 = t2.c1
group by t1.c0, t1.c1;

DROP TABLE rqg_table10;

-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
create table rel1_for_update_with_join(c1 int, c3 int);
create table rel2_for_update_with_join(c1 int);
insert into rel1_for_update_with_join values (50, 60);
insert into rel2_for_update_with_join values (50);

SELECT t1.c1, t2.c1 FROM rel1_for_update_with_join t1 JOIN rel2_for_update_with_join t2
    ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 0 LIMIT 10 FOR UPDATE OF t1;

-- test deparsing rowmarked relations as subqueries
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM rel1_for_update_with_join WHERE c1 = 50) t1
    INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM rel1_for_update_with_join WHERE c1 between 50 and 60) t2
    FULL JOIN (SELECT c1 FROM rel2_for_update_with_join WHERE c1 between 50 and 60) t3
    ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE)
    ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;

drop table rel1_for_update_with_join;
drop table rel2_for_update_with_join;

create table t1_function(c1 int);

insert into t1_function
select 1
from generate_series(1, 100) as i;

SELECT pg_catalog.regr_sxx(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.regr_syy(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.regr_sxy(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.regr_avgx(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.regr_avgy(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.regr_r2(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.regr_slope(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.regr_intercept(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.covar_pop(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.covar_samp(5, 3.14) as c1 from t1_function;
SELECT pg_catalog.corr(5, 3.14) as c1 from t1_function;

drop table t1_function;
