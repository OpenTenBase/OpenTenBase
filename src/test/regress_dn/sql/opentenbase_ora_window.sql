--
-- WINDOW FUNCTIONS
--

--Test Cases for opentenbase_ora Compatible Window Functions
--test cases for function ratio_to_report
\c regression_ora
create extension opentenbase_ora_package_function;
CREATE TEMPORARY TABLE empsalary (
    depname varchar,
    empno bigint,
    salary int,
    enroll_date date
);

INSERT INTO empsalary VALUES
('develop', 10, 5200, '2007-08-01'),
('sales', 1, 5000, '2006-10-01'),
('personnel', 5, 3500, '2007-12-10'),
('sales', 4, 4800, '2007-08-08'),
('personnel', 2, 3900, '2006-12-23'),
('develop', 7, 4200, '2008-01-01'),
('develop', 9, 4500, '2008-01-01'),
('sales', 3, 4800, '2007-08-01'),
('develop', 8, 6000, '2006-10-01'),
('develop', 11, 5200, '2007-08-15');
select depname,ratio_to_report(null) over() from empsalary order by 1;
select depname,empno,salary,ratio_to_report(null) over(partition by depname order by salary) from empsalary order by 1;

select depname,ratio_to_report(100) over() from empsalary order by 1;
select depname,empno,salary,ratio_to_report(100) over(partition by depname order by salary) from empsalary order by 1;

select depname,ratio_to_report('100') over() from empsalary order by 1;
select depname,empno,salary,ratio_to_report('100') over(partition by depname order by salary) from empsalary order by 1;

select depname,salary,sum(salary) over(), ratio_to_report(salary) over() from empsalary order by 1;
select depname,empno,salary,sum(salary) over(partition by depname), ratio_to_report(salary) over(partition by depname order by salary) from empsalary order by 1;

select depname,salary,empno,ratio_to_report(salary) over(), sum(salary) over(), ratio_to_report(empno) over(), sum(empno) over() from empsalary order by 1;
select depname,empno,salary,ratio_to_report(salary) over(partition by depname order by salary), sum(salary) over(partition by depname), ratio_to_report(salary) over(), sum(salary) over() from empsalary order by 1;


DROP VIEW v_window;

CREATE TEMP VIEW v_window AS
	SELECT i, min(i) over (order by i range between '1 day' preceding and '10 days' following) as min_i
  FROM generate_series(now(), now()+'100 days'::interval, '1 hour') i;

SELECT pg_get_viewdef('v_window');

-- RANGE offset PRECEDING/FOLLOWING tests

SELECT sum(unique1) over (order by four range between 2::int8 preceding and 1::int2 preceding),
	unique1, four
FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (order by four desc range between 2::int8 preceding and 1::int2 preceding),
	unique1, four
FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (partition by four order by unique1 range between 5::int8 preceding and 6::int2 following),
	unique1, four
FROM tenk1 WHERE unique1 < 10;

select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;

select sum(salary) over (order by enroll_date desc range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;

select sum(salary) over (order by enroll_date desc range between '1 year'::interval following and '1 year'::interval following),
	salary, enroll_date from empsalary;

select first_value(salary) over(order by salary range between 1000 preceding and 1000 following),
	lead(salary) over(order by salary range between 1000 preceding and 1000 following),
	nth_value(salary, 1) over(order by salary range between 1000 preceding and 1000 following),
	salary from empsalary;

select last_value(salary) over(order by salary range between 1000 preceding and 1000 following),
	lag(salary) over(order by salary range between 1000 preceding and 1000 following),
	salary from empsalary;

-- RANGE offset PRECEDING/FOLLOWING with null values
select x, y,
       first_value(y) over w,
       last_value(y) over w
from
  (select x, x as y from generate_series(1,5) as x
   union all select null, 42
   union all select null, 43) ss
window w as
  (order by x asc nulls first range between 2 preceding and 2 following);

select x, y,
       first_value(y) over w,
       last_value(y) over w
from
  (select x, x as y from generate_series(1,5) as x
   union all select null, 42
   union all select null, 43) ss
window w as
  (order by x asc nulls last range between 2 preceding and 2 following);

select x, y,
       first_value(y) over w,
       last_value(y) over w
from
  (select x, x as y from generate_series(1,5) as x
   union all select null, 42
   union all select null, 43) ss
window w as
  (order by x desc nulls first range between 2 preceding and 2 following);

select x, y,
       first_value(y) over w,
       last_value(y) over w
from
  (select x, x as y from generate_series(1,5) as x
   union all select null, 42
   union all select null, 43) ss
window w as
  (order by x desc nulls last range between 2 preceding and 2 following);

-- Check overflow behavior for various integer sizes

select x, last_value(x) over (order by x::smallint range between current row and 2147450884 following)
from generate_series(32764, 32766) x;

select x, last_value(x) over (order by x::smallint desc range between current row and 2147450885 following)
from generate_series(-32766, -32764) x;

select x, last_value(x) over (order by x range between current row and 4 following)
from generate_series(2147483644, 2147483646) x;

select x, last_value(x) over (order by x desc range between current row and 5 following)
from generate_series(-2147483646, -2147483644) x;

select x, last_value(x) over (order by x range between current row and 4 following)
from generate_series(9223372036854775804, 9223372036854775806) x;

select x, last_value(x) over (order by x desc range between current row and 5 following)
from generate_series(-9223372036854775806, -9223372036854775804) x;

-- Test in_range for other numeric datatypes

create temp table numerics(
    id int,
    f_float4 float4,
    f_float8 float8,
    f_numeric numeric
);

insert into numerics values
(0, '-infinity', '-infinity', '-1000'),  -- numeric type lacks infinities
(1, -3, -3, -3),
(2, -1, -1, -1),
(3, 0, 0, 0),
(4, 1.1, 1.1, 1.1),
(5, 1.12, 1.12, 1.12),
(6, 2, 2, 2),
(7, 100, 100, 100),
(8, 'infinity', 'infinity', '1000'),
(9, 'NaN', 'NaN', 'NaN');

select id, f_float4, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_float4 range between
             1 preceding and 1 following);
select id, f_float4, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_float4 range between
             1 preceding and 1.1::float4 following);
select id, f_float4, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_float4 range between
             'inf' preceding and 'inf' following);
select id, f_float4, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_float4 range between
             1.1 preceding and 'NaN' following);  -- error, NaN disallowed

select id, f_float8, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_float8 range between
             1 preceding and 1 following);
select id, f_float8, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_float8 range between
             1 preceding and 1.1::float8 following);
select id, f_float8, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_float8 range between
             'inf' preceding and 'inf' following);
select id, f_float8, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_float8 range between
             1.1 preceding and 'NaN' following);  -- error, NaN disallowed

select id, f_numeric, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_numeric range between
             1 preceding and 1 following);
select id, f_numeric, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_numeric range between
             1 preceding and 1.1::numeric following);
select id, f_numeric, first_value(id) over w, last_value(id) over w
from numerics
window w as (order by f_numeric range between
             1.1 preceding and 'NaN' following);  -- error, NaN disallowed

-- Test in_range for other datetime datatypes

create temp table datetimes(
    id int,
    f_time time,
    f_timetz timetz,
    f_interval interval,
    f_timestamptz pg_catalog.timestamptz,
    f_timestamp pg_catalog.timestamp
);

insert into datetimes values
(1, '11:00', '11:00 BST', '1 year', '2000-10-19 10:23:54+01', '2000-10-19 10:23:54'),
(2, '12:00', '12:00 BST', '2 years', '2001-10-19 10:23:54+01', '2001-10-19 10:23:54'),
(3, '13:00', '13:00 BST', '3 years', '2001-10-19 10:23:54+01', '2001-10-19 10:23:54'),
(4, '14:00', '14:00 BST', '4 years', '2002-10-19 10:23:54+01', '2002-10-19 10:23:54'),
(5, '15:00', '15:00 BST', '5 years', '2003-10-19 10:23:54+01', '2003-10-19 10:23:54'),
(6, '15:00', '15:00 BST', '5 years', '2004-10-19 10:23:54+01', '2004-10-19 10:23:54'),
(7, '17:00', '17:00 BST', '7 years', '2005-10-19 10:23:54+01', '2005-10-19 10:23:54'),
(8, '18:00', '18:00 BST', '8 years', '2006-10-19 10:23:54+01', '2006-10-19 10:23:54'),
(9, '19:00', '19:00 BST', '9 years', '2007-10-19 10:23:54+01', '2007-10-19 10:23:54'),
(10, '20:00', '20:00 BST', '10 years', '2008-10-19 10:23:54+01', '2008-10-19 10:23:54');

select id, f_time, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_time range between
             '70 min'::interval preceding and '2 hours'::interval following);

select id, f_time, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_time desc range between
             '70 min' preceding and '2 hours' following);

select id, f_timetz, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_timetz range between
             '70 min'::interval preceding and '2 hours'::interval following);

select id, f_timetz, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_timetz desc range between
             '70 min' preceding and '2 hours' following);

select id, f_interval, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_interval range between
             '1 year'::interval preceding and '1 year'::interval following);

select id, f_interval, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_interval desc range between
             '1 year' preceding and '1 year' following);

select id, f_timestamptz, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_timestamptz range between
             '1 year'::interval preceding and '1 year'::interval following);

select id, f_timestamptz, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_timestamptz desc range between
             '1 year' preceding and '1 year' following);

select id, f_timestamp, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_timestamp range between
             '1 year'::interval preceding and '1 year'::interval following);

select id, f_timestamp, first_value(id) over w, last_value(id) over w
from datetimes
window w as (order by f_timestamp desc range between
             '1 year' preceding and '1 year' following);

-- Show differences in offset interpretation between ROWS, RANGE, and GROUPS
WITH cte (x) AS (
        SELECT * FROM generate_series(1, 35, 2)
)
SELECT x, (sum(x) over w)
FROM cte
WINDOW w AS (ORDER BY x rows between 1 preceding and 1 following);

WITH cte (x) AS (
        SELECT * FROM generate_series(1, 35, 2)
)
SELECT x, (sum(x) over w)
FROM cte
WINDOW w AS (ORDER BY x range between 1 preceding and 1 following);

WITH cte (x) AS (
        select 1 union all select 1 union all select 1 union all
        SELECT * FROM generate_series(5, 49, 2)
)
SELECT x, (sum(x) over w)
FROM cte
WINDOW w AS (ORDER BY x rows between 1 preceding and 1 following);

WITH cte (x) AS (
        select 1 union all select 1 union all select 1 union all
        SELECT * FROM generate_series(5, 49, 2)
)
SELECT x, (sum(x) over w)
FROM cte
WINDOW w AS (ORDER BY x range between 1 preceding and 1 following);

create temp table ora_datetimes(
    id int,
    f_time time,
    f_timetz timetz,
    f_interval interval,
    f_timestamptz timestamptz,
    f_timestamp timestamp
);

insert into ora_datetimes values
(1, '11:00', '11:00 BST', '1 year', '2000-10-19 10:23:54+01', '2000-10-19 10:23:54'),
(2, '12:00', '12:00 BST', '2 years', '2001-10-19 10:23:54+01', '2001-10-19 10:23:54'),
(3, '13:00', '13:00 BST', '3 years', '2001-10-19 10:23:54+01', '2001-10-19 10:23:54'),
(4, '14:00', '14:00 BST', '4 years', '2002-10-19 10:23:54+01', '2002-10-19 10:23:54'),
(5, '15:00', '15:00 BST', '5 years', '2003-10-19 10:23:54+01', '2003-10-19 10:23:54'),
(6, '15:00', '15:00 BST', '5 years', '2004-10-19 10:23:54+01', '2004-10-19 10:23:54'),
(7, '17:00', '17:00 BST', '7 years', '2005-10-19 10:23:54+01', '2005-10-19 10:23:54'),
(8, '18:00', '18:00 BST', '8 years', '2006-10-19 10:23:54+01', '2006-10-19 10:23:54'),
(9, '19:00', '19:00 BST', '9 years', '2007-10-19 10:23:54+01', '2007-10-19 10:23:54'),
(10, '20:00', '20:00 BST', '10 years', '2008-10-19 10:23:54+01', '2008-10-19 10:23:54');

select vsize(F_TIMESTAMPTZ), vsize(F_TIMESTAMP) from ora_datetimes limit 1;

select id, f_timestamptz, first_value(id) over w, last_value(id) over w
from ora_datetimes
window w as (order by f_timestamptz range between
             '1 year'::interval preceding and '1 year'::interval following);

select id, f_timestamptz, first_value(id) over w, last_value(id) over w
from ora_datetimes
window w as (order by f_timestamptz desc range between
             '1 year' preceding and '1 year' following);

select id, f_timestamp, first_value(id) over w, last_value(id) over w
from ora_datetimes
window w as (order by f_timestamp range between
             '1 year'::interval preceding and '1 year'::interval following);

select id, f_timestamp, first_value(id) over w, last_value(id) over w
from ora_datetimes
window w as (order by f_timestamp desc range between
             '1 year' preceding and '1 year' following);

DROP  table sales;
CREATE TABLE sales (
    sale_id NUMBER PRIMARY KEY,
    sale_date DATE NOT NULL,
    product_id NUMBER NOT NULL,
    sub_product_id NUMBER NOT NULL,
    quantity NUMBER NOT NULL
);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (1, TO_DATE('2022-01-01 02:34:01', 'YYYY-MM-DD HH24:MI:SS'), 101, 1011, 10);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (2, TO_DATE('2022-01-02 07:19:05', 'YYYY-MM-DD HH24:MI:SS'), 102, 1021, 5);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (3, TO_DATE('2022-01-03 13:24:06', 'YYYY-MM-DD HH24:MI:SS'), 101, 1011, 15);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (4, TO_DATE('2022-01-04 19:08:10', 'YYYY-MM-DD HH24:MI:SS'), 103, 1031, 20);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (5, TO_DATE('2022-01-05 21:08:12', 'YYYY-MM-DD HH24:MI:SS'), 102, 1021, 10);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (6, TO_DATE('2022-01-06 12:24:16', 'YYYY-MM-DD HH24:MI:SS'), 101, 1012, 25);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (7, TO_DATE('2022-01-07 00:14:18', 'YYYY-MM-DD HH24:MI:SS'), 103, 1031, 30);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (8, TO_DATE('2022-01-08 16:21:22', 'YYYY-MM-DD HH24:MI:SS'), 102, 1021, 15);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (9, TO_DATE('2022-01-09 04:59:25', 'YYYY-MM-DD HH24:MI:SS'), 101, 1011, 30);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (10, TO_DATE('2022-01-10 12:00:49', 'YYYY-MM-DD HH24:MI:SS'), 103, 1031, 35);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (11, TO_DATE('2022-01-11 13:13:37', 'YYYY-MM-DD HH24:MI:SS'), 102, 1021, 20);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (12, TO_DATE('2022-01-12 05:24:32', 'YYYY-MM-DD HH24:MI:SS'), 101, 1012, 35);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (13, TO_DATE('2022-01-13 12:00:55', 'YYYY-MM-DD HH24:MI:SS'), 103, 1031, 40);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (14, TO_DATE('2022-01-14 08:50:56', 'YYYY-MM-DD HH24:MI:SS'), 102, 1022, 25);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (15, TO_DATE('2022-01-15 08:16:00', 'YYYY-MM-DD HH24:MI:SS'), 101, 1011, 40);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (16, TO_DATE('2022-01-16 19:10:49', 'YYYY-MM-DD HH24:MI:SS'), 103, 1031, 45);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (17, TO_DATE('2022-01-17 22:07:37', 'YYYY-MM-DD HH24:MI:SS'), 102, 1022, 30);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (18, TO_DATE('2022-01-18 23:11:55', 'YYYY-MM-DD HH24:MI:SS'), 101, 1012, 45);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (19, TO_DATE('2022-01-19 11:48:56', 'YYYY-MM-DD HH24:MI:SS'), 103, 1031, 50);
INSERT INTO sales (sale_id, sale_date, product_id, sub_product_id, quantity) VALUES (20, TO_DATE('2022-01-20 18:32:11', 'YYYY-MM-DD HH24:MI:SS'), 102, 1022, 35);
