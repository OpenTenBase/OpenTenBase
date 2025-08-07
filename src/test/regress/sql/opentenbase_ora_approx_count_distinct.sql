---------------------------------------------
-- Test cases for opentenbase_ora to functions.
---------------------------------------------

\c regression_ora

-- test approx_count_distinct
CREATE TABLE t1 ( c1 int, c2 varchar(10), c3 int);

insert into t1 values(1, 'a', 1);
insert into t1 values(1, 'a', 1);
insert into t1 values(1, 'a', 1);
insert into t1 values(2, 'b', 2);
insert into t1 values(3, 'c', 2);
insert into t1 values(3, 'c', 2);

select COUNT(c1), APPROX_COUNT_DISTINCT(c1), COUNT(c2), APPROX_COUNT_DISTINCT(c2)  from t1 order by 1,2,3,4;
select COUNT(c1), APPROX_COUNT_DISTINCT(c1), COUNT(c2), APPROX_COUNT_DISTINCT(c2), c3  from t1 group by c3 order by 1,2,3,4;

drop table t1;
