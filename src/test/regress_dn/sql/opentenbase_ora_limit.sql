------------------------------------------
-- Test cases for opentenbase_ora row_limiting_clause.
--
-- 1. offset without fetch 
-- 2. offset with fetch 
--   2.1 fetch with only
--     2.1.1 fetch without percent
--     2.1.2 fetch with percent
--   2.2 fetch with with ties
--     2.2.1 fetch without percent
--     2.2.2 fetch with percent
-- 3. other cases
------------------------------------------

\c regression_ora

-- Data initialization.
drop table if exists t_limit;
drop table if exists t_limit2;
create table t_limit (q1 int, q2 int);

INSERT INTO t_limit VALUES (1, 1);
INSERT INTO t_limit VALUES (1, 1);
INSERT INTO t_limit VALUES (1, 1);
INSERT INTO t_limit VALUES (2, 2);
INSERT INTO t_limit VALUES (2, 2);
INSERT INTO t_limit VALUES (2, 2);
INSERT INTO t_limit VALUES (3, 3);
INSERT INTO t_limit VALUES (3, 3);
INSERT INTO t_limit VALUES (3, 3);
create table t_limit2 as select * from t_limit;

-- 1. offset without fetch 
select * from t_limit order by q1 offset 0 rows;
select * from t_limit order by q1 offset null rows;
select * from t_limit order by q1 offset 1 rows;
select * from t_limit order by q1 offset -1 rows;
select * from t_limit order by q1 offset 1.4 rows;
select * from t_limit order by q1 offset 1.6 rows;
select * from t_limit order by q1 offset to_number(1.6) rows;
--   1.1.1 fetch with only
select * from t_limit order by q1 fetch first 0 rows only;
select * from t_limit order by q1 fetch first null rows only;
select * from t_limit order by q1 fetch first 1 rows only;
select * from t_limit order by q1 fetch first -1 rows only;
select * from t_limit order by q1 fetch first 1.4 rows only;
select * from t_limit order by q1 fetch first 1.6 rows only;
select * from t_limit order by q1 fetch first to_number(1.6)  rows only;
select * from t_limit order by q1 fetch next 2 rows only;
select * from t_limit order by q1 fetch next -2 rows only;
select * from t_limit order by q1 fetch next 2.4 rows only;
select * from t_limit order by q1 fetch next 2.6 rows only;
select * from t_limit order by q1 fetch next to_number(2.6)  rows only;
--   1.1.1 fetch with only
select * from t_limit order by q1 offset 1 rows fetch first 0 rows only;
select * from t_limit order by q1 offset 1 rows fetch first null rows only;
select * from t_limit order by q1 offset 1 rows fetch first 1 rows only;
select * from t_limit order by q1 offset 1 rows fetch first -1 rows only;
select * from t_limit order by q1 offset 1 rows fetch first 1.4 rows only;
select * from t_limit order by q1 offset 1 rows fetch first 1.6 rows only;
select * from t_limit order by q1 offset 1 rows fetch first to_number(1.6)  rows only;
select * from t_limit order by q1 offset 1 rows fetch next 2 rows only;
select * from t_limit order by q1 offset 1 rows fetch next -2 rows only;
select * from t_limit order by q1 offset 1 rows fetch next 2.4 rows only;
select * from t_limit order by q1 offset 1 rows fetch next 2.6 rows only;
select * from t_limit order by q1 offset 1 rows fetch next to_number(2.6)  rows only;
--     1.1.2 fetch with percent
select * from t_limit order by q1 fetch first 0 percent rows only;
select * from t_limit order by q1 fetch first null percent rows only;
select * from t_limit order by q1 fetch first 1 percent rows only;
select * from t_limit order by q1 fetch first -1 percent rows only;
select * from t_limit order by q1 fetch first 1.4 percent rows only;
select * from t_limit order by q1 fetch first to_number(1.6) percent  rows only;
select * from t_limit order by q1 fetch first 1000 percent  rows only;
select * from t_limit order by q1 fetch first 90 percent  rows only;
select * from t_limit order by q1 fetch first 24.5 percent  rows only;
select * from t_limit order by q1 fetch first 22 percent  rows only;
select * from t_limit order by q1 fetch first 30 percent  rows only;
select * from t_limit order by q1 fetch first 32 percent  rows only;
select * from t_limit order by q1 fetch first 24.5 percent  rows only;
select * from t_limit order by q1 fetch first 23 percent  rows only;
select * from t_limit order by q1 fetch first 19 percent  rows only;

-- 2. offset with fetch
--   2.1.1 fetch with only
select * from t_limit order by q1 offset 1 rows fetch first 0 rows only;
select * from t_limit order by q1 offset 1 rows fetch first null rows only;
select * from t_limit order by q1 offset null rows fetch first 3 rows only;
select * from t_limit order by q1 offset 1 rows fetch first 1 rows only;
select * from t_limit order by q1 offset 1 rows fetch first -1 rows only;
select * from t_limit order by q1 offset 1 rows fetch first 1.4 rows only;
select * from t_limit order by q1 offset 1 rows fetch first 1.6 rows only;
select * from t_limit order by q1 offset 1 rows fetch first to_number(1.6)  rows only;
select * from t_limit order by q1 offset 1 rows fetch next 2 rows only;
select * from t_limit order by q1 offset 1 rows fetch next -2 rows only;
select * from t_limit order by q1 offset 1 rows fetch next 2.4 rows only;
select * from t_limit order by q1 offset 1 rows fetch next 2.6 rows only;
select * from t_limit order by q1 offset 1 rows fetch next to_number(2.6)  rows only;
--     2.1.2 fetch with percent
select * from t_limit order by q1 offset 1 rows fetch first 0 percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first null percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 1 percent rows only;
select * from t_limit order by q1 offset 1 rows fetch first -1 percent rows only;
select * from t_limit order by q1 offset 1 rows fetch first 1.4 percent rows only;
select * from t_limit order by q1 offset 1 rows fetch first to_number(1.6) percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 1000 percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 90 percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 24.5 percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 22 percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 30 percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 32 percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 24.5 percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 23 percent  rows only;
select * from t_limit order by q1 offset 1 rows fetch first 19 percent  rows only;

--   2.2 fetch with with ties
--   2.2.1 fetch with with ties
select * from t_limit order by q1 offset 3 rows fetch first 0 rows with ties;
-- Validating the boundaries
select * from t_limit order by q1 offset 3 rows fetch first 6 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 7 rows with ties;

select * from t_limit order by q1 offset 3 rows fetch first null rows with ties;
select * from t_limit order by q1 offset null rows fetch first 3 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 1 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first -1 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 1.4 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 1.6 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first to_number(1.6)  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch next 2 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch next -2 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch next 2.4 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch next 2.6 rows with ties;
select * from t_limit order by q1 offset 3 rows fetch next to_number(2.6)  rows with ties;
--     2.2.2 fetch with percent
select * from t_limit order by q1 offset 3 rows fetch first 0 percent  rows with ties;
-- Validating the boundaries
select * from t_limit order by q1 offset 3 rows fetch first 6 percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 7 percent  rows with ties;

select * from t_limit order by q1 offset 3 rows fetch first null percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 1 percent rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first -1 percent rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 1.4 percent rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first to_number(1.6) percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 1000 percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 90 percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 24.5 percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 22 percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 30 percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 32 percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 24.5 percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 23 percent  rows with ties;
select * from t_limit order by q1 offset 3 rows fetch first 19 percent  rows with ties;
--WITH TIES: number of rows is optional and defaults to one
select * from t_limit order by q1 offset 3 rows fetch first rows with ties;

-- 3. other cases
--     3.1.2 fetch in plpgsql
DO $$
DECLARE
   a INT := 3;
   v_t_limit t_limit%ROWTYPE;
   c5 CURSOR FOR
      SELECT *
      FROM (SELECT *
            FROM t_limit
            ORDER BY q1
            OFFSET a ROWS
            FETCH FIRST 1.4 ROWS ONLY) subquery;
BEGIN
   OPEN c5;
   LOOP
      FETCH c5 INTO v_t_limit;
      EXIT WHEN NOT FOUND;
      RAISE NOTICE '%, %', v_t_limit.q1, v_t_limit.q2;
   END LOOP;
   CLOSE c5;
END;
$$;

DO $$
DECLARE
   a INT := 3;
   v_t_limit t_limit%ROWTYPE;
   c5 CURSOR FOR
      SELECT *
      FROM (SELECT *
            FROM t_limit
            ORDER BY q1
            OFFSET a ROWS
            FETCH FIRST 1.4 ROWS WITH TIES) subquery;
BEGIN
   OPEN c5;
   LOOP
      FETCH c5 INTO v_t_limit;
      EXIT WHEN NOT FOUND;
      RAISE NOTICE '%, %', v_t_limit.q1, v_t_limit.q2;
   END LOOP;
   CLOSE c5;
END;
$$;

-- clean
drop table t_limit;
drop table t_limit2;

-- other cases
drop table if exists t_test_rowid_20220916;
create table t_test_rowid_20220916(id int,name varchar(20));
insert into t_test_rowid_20220916 values(1,'1111');
insert into t_test_rowid_20220916 values(2,'2222');
insert into t_test_rowid_20220916 values(3,'3333');
drop table t_test_rowid_20220916;

-- test float percent
drop table if exists t1;
CREATE TABLE t1(c1 int, c2 int) distribute by shard(c2);
insert into t1 select generate_series(1,1000), 1;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 0.1 PERCENT ROW ONLY;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 0.2 PERCENT ROW ONLY;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 0.3 PERCENT ROW ONLY;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 0.4 PERCENT ROW ONLY;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 0.5 PERCENT ROW ONLY;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 0.6 PERCENT ROW ONLY;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 0.7 PERCENT ROW ONLY;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 0.8 PERCENT ROW ONLY;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 0.9 PERCENT ROW ONLY;
SELECT * FROM t1 ORDER BY c1 FETCH FIRST 1 PERCENT ROW ONLY;
-- fqs
SELECT * FROM t1 where c2 = 1 ORDER BY c1 FETCH FIRST 0.1 PERCENT ROW ONLY;
drop table t1;
