-- Regression tests for MySQL compatibility functions

-- Test IFNULL
SELECT IFNULL(NULL, 'default') = 'default' AS ifnull_test1;
SELECT IFNULL('value', 'default') = 'value' AS ifnull_test2;
SELECT IFNULL(NULL, NULL) IS NULL AS ifnull_test3;
SELECT IFNULL(0, 1) = 0 AS ifnull_test4;

-- Test CONCAT_WS
SELECT CONCAT_WS(',', 'a', 'b', 'c') = 'a,b,c' AS concat_ws_test1;
SELECT CONCAT_WS(',', 'a', NULL, 'c') = 'a,c' AS concat_ws_test2;
SELECT CONCAT_WS('-', 'hello', 'world') = 'hello-world' AS concat_ws_test3;
SELECT CONCAT_WS(NULL, 'a', 'b') IS NULL AS concat_ws_test4;

-- Test FIND_IN_SET
SELECT FIND_IN_SET('b', 'a,b,c,d') = 2 AS find_in_set_test1;
SELECT FIND_IN_SET('x', 'a,b,c,d') = 0 AS find_in_set_test2;
SELECT FIND_IN_SET(NULL, 'a,b,c') = 0 AS find_in_set_test3;

-- Test ELT
SELECT ELT(1, 'a', 'b', 'c') = 'a' AS elt_test1;
SELECT ELT(3, 'a', 'b', 'c') = 'c' AS elt_test2;
SELECT ELT(0, 'a', 'b') IS NULL AS elt_test3;
SELECT ELT(4, 'a', 'b') IS NULL AS elt_test4;

-- Test FIELD
SELECT FIELD('b', 'a', 'b', 'c') = 2 AS field_test1;
SELECT FIELD('x', 'a', 'b', 'c') = 0 AS field_test2;
SELECT FIELD(NULL, 'a', 'b') = 0 AS field_test3;

-- Test INSERT function
SELECT "INSERT"('hello', 2, 3, 'abc') = 'habc' AS insert_test1;
SELECT "INSERT"('hello', 10, 1, 'x') = 'hello' AS insert_test2;

-- Test DATE_FORMAT
SELECT DATE_FORMAT('2026-07-05 12:30:45'::timestamp, '%Y-%m-%d') = '2026-07-05' AS date_format_test1;
SELECT DATE_FORMAT('2026-07-05 12:30:45'::timestamp, '%Y') = '2026' AS date_format_test2;

-- Test DATEDIFF
SELECT DATEDIFF('2026-07-10'::date, '2026-07-01'::date) = 9 AS datediff_test1;
SELECT DATEDIFF('2026-07-01'::date, '2026-07-10'::date) = -9 AS datediff_test2;

-- Test LAST_DAY
SELECT LAST_DAY('2026-02-01'::date) = '2026-02-28'::date AS last_day_feb;
SELECT LAST_DAY('2026-01-15'::date) = '2026-01-31'::date AS last_day_jan;
SELECT LAST_DAY('2024-02-15'::date) = '2024-02-29'::date AS last_day_leap;

-- Test TRUNCATE
SELECT TRUNCATE(1.2345, 2) = 1.23 AS truncate_test1;
SELECT TRUNCATE(1.2399, 2) = 1.23 AS truncate_test2;
SELECT TRUNCATE(1234.5678, 0) = 1234 AS truncate_test3;

-- Test IF
SELECT "IF"(true, 'yes', 'no') = 'yes' AS if_test1;
SELECT "IF"(false, 'yes', 'no') = 'no' AS if_test2;
SELECT "IF"(NULL, 'yes', 'no') = 'no' AS if_test3;
SELECT "IF"(1 = 1, 100, 0) = 100 AS if_test4;

-- Test GROUP_CONCAT
CREATE TEMP TABLE test_gc (grp int, val text);
INSERT INTO test_gc VALUES (1, 'a'), (1, 'b'), (1, 'c'), (2, 'x'), (2, 'y');
SELECT grp, GROUP_CONCAT(val ORDER BY val) FROM test_gc GROUP BY grp ORDER BY grp;
DROP TABLE test_gc;

-- Test TIMESTAMPDIFF
SELECT TIMESTAMPDIFF('DAY', '2026-07-01'::timestamp, '2026-07-05'::timestamp) = 4 AS tsdiff_day;
SELECT TIMESTAMPDIFF('SECOND', '2026-07-01 00:00:00'::timestamp, '2026-07-01 00:01:00'::timestamp) = 60 AS tsdiff_sec;

-- Test STR_TO_DATE
SELECT STR_TO_DATE('2026-07-05', '%Y-%m-%d')::date = '2026-07-05'::date AS str_to_date_test;
