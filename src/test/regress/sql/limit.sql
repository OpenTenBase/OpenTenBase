--
-- LIMIT
-- Check the LIMIT/OFFSET feature of SELECT
--

SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 50
		ORDER BY unique1 LIMIT 2;
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60
		ORDER BY unique1 LIMIT 5;
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60 AND unique1 < 63
		ORDER BY unique1 LIMIT 5;
SELECT ''::text AS three, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 100
		ORDER BY unique1 LIMIT 3 OFFSET 20;
SELECT ''::text AS zero, unique1, unique2, stringu1
		FROM onek WHERE unique1 < 50
		ORDER BY unique1 DESC LIMIT 8 OFFSET 99;
SELECT ''::text AS eleven, unique1, unique2, stringu1
		FROM onek WHERE unique1 < 50
		ORDER BY unique1 DESC LIMIT 20 OFFSET 39;
SELECT ''::text AS ten, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 OFFSET 990;
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 OFFSET 990 LIMIT 5;
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 LIMIT 5 OFFSET 900;

-- Test null limit and offset.  The planner would discard a simple null
-- constant, so to ensure executor is exercised, do this:
select * from int8_tbl order by 1,2 limit (case when random() < 0.5 then null::bigint end);
select * from int8_tbl order by 1,2 offset (case when random() < 0.5 then null::bigint end);

-- Test assorted cases involving backwards fetch from a LIMIT plan node
begin;

declare c1 scroll cursor for select * from int8_tbl order by 1,2 limit 10;
fetch all in c1;
fetch 1 in c1;
fetch backward 1 in c1;
fetch backward all in c1;
fetch backward 1 in c1;
fetch all in c1;

declare c2 scroll cursor for select * from int8_tbl order by 1,2 limit 3;
fetch all in c2;
fetch 1 in c2;
fetch backward 1 in c2;
fetch backward all in c2;
fetch backward 1 in c2;
fetch all in c2;

declare c3 scroll cursor for select * from int8_tbl order by 1,2 offset 3;
fetch all in c3;
fetch 1 in c3;
fetch backward 1 in c3;
fetch backward all in c3;
fetch backward 1 in c3;
fetch all in c3;

declare c4 scroll cursor for select * from int8_tbl order by 1,2 offset 10;
fetch all in c4;
fetch 1 in c4;
fetch backward 1 in c4;
fetch backward all in c4;
fetch backward 1 in c4;
fetch all in c4;

declare c5 SCROLL cursor for select * from int8_tbl order by q1 fetch first 2 rows with ties;
fetch all in c5;
fetch 1 in c5;
fetch backward 1 in c5;
fetch backward 1 in c5;
fetch all in c5;
fetch backward all in c5;
fetch all in c5;
fetch backward all in c5;

rollback;

-- Stress test for variable LIMIT in conjunction with bounded-heap sorting

SELECT
  (SELECT n
     FROM (VALUES (1)) AS x,
          (SELECT n FROM generate_series(1,10) AS n
             ORDER BY n LIMIT 1 OFFSET s-1) AS y) AS z
  FROM generate_series(1,10) AS s;

--
-- Test behavior of volatile and set-returning functions in conjunction
-- with ORDER BY and LIMIT.
--

create temp sequence testseq;

explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10;

select t.unique1, t.unique2 from (select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10) as t;

select currval('testseq');

explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10;

select t.unique1, t.unique2 from (select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10) as t;

select currval('testseq');

explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by unique2 limit 7;

select unique1, unique2, generate_series(1,10)
  from tenk1 order by unique2 limit 7;

explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by tenthous limit 7;

select unique1, unique2, generate_series(1,10)
  from tenk1 order by tenthous limit 7;

-- use of random() is to keep planner from folding the expressions together
explain (verbose, costs off)
select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2;

select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2;

explain (verbose, costs off)
select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2
order by s2 desc;

select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2
order by s2 desc;

-- test for failure to set all aggregates' aggtranstype
explain (verbose, costs off)
select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

-- limit all with offset
create table limit_all_t (id int, num int);
insert into limit_all_t values (generate_series(1,5),generate_series(1,5));
explain (costs off) select * from limit_all_t order by 1,2 limit all offset 1;
select * from limit_all_t order by 1,2 limit all offset 1;
drop table limit_all_t;

-- limit + offset > INT64_MAX
DROP TABLE IF EXISTS limit_offset_over_t cascade;
CREATE TABLE limit_offset_over_t(c0 TEXT) WITH (autovacuum_vacuum_threshold=784680650, autovacuum_vacuum_cost_limit=4969, autovacuum_freeze_min_age=426750238, autovacuum_analyze_threshold=1579189975);
SELECT DISTINCT ON (FALSE) (t2.c0 COLLATE "C") FROM ONLY limit_offset_over_t t2 WHERE ((t2.c0 COLLATE "C") COLLATE "C") BETWEEN SYMMETRIC ((((((t2.c0 COLLATE "C"))::VARCHAR COLLATE "C") COLLATE "C") COLLATE "C")) AND ((t2.c0 COLLATE "C") COLLATE "C") LIMIT 1879268089421534688 OFFSET 8009103420926229377;
DROP TABLE limit_offset_over_t cascade;
--
-- FETCH FIRST
-- Check the WITH TIES clause
--

SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 2 ROW WITH TIES;

SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 1 ROW WITH TIES;

SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 2 ROW ONLY;
-- should fail
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 50
		FETCH FIRST 2 ROW WITH TIES;

-- test ruleutils
begin;
CREATE VIEW limit_thousand_v_1 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST 5 ROWS WITH TIES OFFSET 10;
\d+ limit_thousand_v_1
CREATE VIEW limit_thousand_v_2 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand OFFSET 10 FETCH FIRST 5 ROWS ONLY;
\d+ limit_thousand_v_2
rollback;
CREATE VIEW limit_thousand_v_3 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST NULL ROWS WITH TIES;		-- fails
begin;
CREATE VIEW limit_thousand_v_3 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST (NULL+1) ROWS WITH TIES;
\d+ limit_thousand_v_3
CREATE VIEW limit_thousand_v_4 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST NULL ROWS ONLY;
\d+ limit_thousand_v_4
rollback;
-- leave these views
