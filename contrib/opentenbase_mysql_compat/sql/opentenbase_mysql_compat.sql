-- =============================================================
-- Regression tests for opentenbase_mysql_compat
-- =============================================================

CREATE EXTENSION opentenbase_mysql_compat;

-- -------------------------------------------------------------
-- CONCAT
-- -------------------------------------------------------------
SELECT concat('Open', 'Ten', 'Base');
SELECT concat('value = ', 42);
SELECT concat('price = ', 1.50::numeric);
SELECT concat('a', NULL, 'b');
SELECT concat();

-- -------------------------------------------------------------
-- CONCAT_WS
-- -------------------------------------------------------------
SELECT concat_ws(',', 'First', 'Second', 'Third');
SELECT concat_ws('-', 'a', NULL, 'b');
SELECT concat_ws(NULL, 'a', 'b');
SELECT concat_ws(',', 'First', 2, 3.5::numeric);

-- -------------------------------------------------------------
-- FIND_IN_SET
-- -------------------------------------------------------------
SELECT find_in_set('b', 'a,b,c,d');
SELECT find_in_set('c', 'a,b,c,d');
SELECT find_in_set('z', 'a,b,c,d');
SELECT find_in_set('B', 'a,b,c');
SELECT find_in_set(NULL, 'a,b');
SELECT find_in_set('a', NULL);
SELECT find_in_set('', '');
SELECT find_in_set('', 'a,b');
SELECT find_in_set('a,b', 'a,b,c');

-- -------------------------------------------------------------
-- ELT
-- -------------------------------------------------------------
SELECT elt(1, 'ej', 'Heja', 'hej', 'foo');
SELECT elt(4, 'ej', 'Heja', 'hej', 'foo');
SELECT elt(0, 'ej', 'Heja', 'hej', 'foo');
SELECT elt(5, 'ej', 'Heja', 'hej', 'foo');
SELECT elt(NULL, 'a', 'b');
SELECT elt(2, 'a', NULL);

-- -------------------------------------------------------------
-- FIELD
-- -------------------------------------------------------------
SELECT field('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
SELECT field('Gg', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
SELECT field(NULL, 'a', 'b');
SELECT field('bb', 'aa', 'BB');

-- -------------------------------------------------------------
-- INSERT
-- -------------------------------------------------------------
SELECT "insert"('Quadratic', 3, 4, 'What');
SELECT "insert"('Quadratic', -1, 4, 'What');
SELECT "insert"('Quadratic', 3, 100, 'What');
SELECT mysql_insert('Quadratic', 3, 4, 'What');
SELECT "insert"('数据库管理系统', 3, 2, 'DB');
SELECT "insert"(NULL, 1, 1, 'x');

-- -------------------------------------------------------------
-- SUBSTRING_INDEX
-- -------------------------------------------------------------
SELECT substring_index('www.mysql.com', '.', 2);
SELECT substring_index('www.mysql.com', '.', -2);
SELECT substring_index('www.mysql.com', '.', 0);
SELECT substring_index('www.mysql.com', ':', 1);
SELECT substring_index('a-b-c-d', '-', -1);
SELECT substring_index('www.mysql.com', '', 2);
SELECT substring_index(NULL, '.', 2);

-- -------------------------------------------------------------
-- LOCATE / INSTR
-- -------------------------------------------------------------
SELECT locate('bar', 'foobarbar');
SELECT locate('xbar', 'foobar');
SELECT locate('bar', 'foobarbar', 5);
SELECT locate('BAR', 'foobar');
SELECT locate('', 'foobar');
SELECT locate('b', 'foobar', 0);
SELECT instr('foobarbar', 'bar');
SELECT instr('数据库系统', '库');
SELECT locate('bar', NULL);

-- -------------------------------------------------------------
-- REVERSE
-- -------------------------------------------------------------
SELECT reverse('abc');
SELECT reverse('数据库');
SELECT reverse('');

-- -------------------------------------------------------------
-- SPACE
-- -------------------------------------------------------------
SELECT space(5) = '     ';
SELECT space(0) = '';
SELECT space(-3) = '';

-- -------------------------------------------------------------
-- UNHEX
-- -------------------------------------------------------------
SELECT unhex('4D7953514C');
SELECT unhex('4d7953514c');
SELECT unhex('abc');
SELECT unhex('zz');
SELECT unhex(NULL);

-- -------------------------------------------------------------
-- BIN / OCT
-- -------------------------------------------------------------
SELECT bin(5);
SELECT bin(0);
SELECT bin(-1);
SELECT oct(8);
SELECT oct(0);
SELECT oct(12);

-- -------------------------------------------------------------
-- DATE_FORMAT
-- -------------------------------------------------------------
SELECT date_format('2009-10-04 22:23:00'::timestamp, '%W %M %Y');
SELECT date_format('2007-10-04 22:23:00'::timestamp, '%H:%i:%s');
SELECT date_format('1900-10-04 22:23:00'::timestamp, '%D %y %a %d %m %b %j');
SELECT date_format('1997-10-04 22:23:00'::timestamp, '%H %k %I %r %T %S %w');
SELECT date_format('1999-01-01'::timestamp, '%X %V');
SELECT date_format('2006-06-00'::timestamp::date::timestamp, '%d');
SELECT date_format('2006-06-13'::date, '%D');
SELECT date_format('2026-01-15 10:30:45.123456'::timestamp, '%f');
SELECT date_format('2024-02-29'::timestamp, '%Y-%m-%d %j %U %u %V %v %X %x');
SELECT date_format('2016-01-03'::timestamp, '%X %V %x %v');
SELECT date_format('2021-01-01'::timestamp, '%x %v');
SELECT date_format('2009-10-04 22:23:00'::timestamp, '100% %%!');
SELECT date_format('2009-10-04 22:23:00'::timestamp, '%p %l');
SELECT date_format(NULL, '%Y');
SELECT date_format('2009-10-04 22:23:00'::timestamp, NULL);

-- -------------------------------------------------------------
-- STR_TO_DATE
-- -------------------------------------------------------------
SELECT str_to_date('01,5,2013', '%d,%m,%Y');
SELECT str_to_date('May 1, 2013', '%M %d,%Y');
SELECT str_to_date('a09:30:17', 'a%h:%i:%s');
SELECT str_to_date('09:30:17 PM', '%h:%i:%s %p');
SELECT str_to_date('09:30:17 AM', '%h:%i:%s %p');
SELECT str_to_date('abc','abc');
SELECT str_to_date('09:30:17 PM', '%T');
SELECT str_to_date('2013 12 17', '%Y %j');
SELECT str_to_date('11:12:13.456', '%H:%i:%s.%f');
SELECT str_to_date(NULL, '%Y');
SELECT str_to_date('04/31/2004', '%m/%d/%Y');

-- -------------------------------------------------------------
-- DATEDIFF
-- -------------------------------------------------------------
SELECT datediff('2007-12-31 23:59:59'::timestamp, '2007-12-30'::timestamp);
SELECT datediff('2010-11-30'::date, '2010-01-31'::date);
SELECT datediff('2010-01-31'::date, '2010-11-30'::date);
SELECT datediff('2020-01-02 01:00:00'::timestamp, '2020-01-01 23:00:00'::timestamp);
SELECT datediff(NULL, '2020-01-01'::timestamp);

-- -------------------------------------------------------------
-- TIMESTAMPDIFF
-- -------------------------------------------------------------
SELECT timestampdiff('MONTH', '2003-02-01'::timestamp, '2003-05-01'::timestamp);
SELECT timestampdiff('YEAR', '2002-05-01'::timestamp, '2001-01-01'::timestamp);
SELECT timestampdiff('MINUTE', '2003-02-01'::timestamp, '2003-05-01 12:05:55'::timestamp);
SELECT timestampdiff('DAY', '2020-01-01 23:00:00'::timestamp, '2020-01-02 01:00:00'::timestamp);
SELECT timestampdiff('WEEK', '2020-01-01'::timestamp, '2020-01-22'::timestamp);
SELECT timestampdiff('MONTH', '2022-01-31'::timestamp, '2022-02-28'::timestamp);
SELECT timestampdiff('MONTH', '2022-01-28'::timestamp, '2022-02-28'::timestamp);
SELECT timestampdiff('QUARTER', '2022-01-01'::timestamp, '2022-12-31'::timestamp);
SELECT timestampdiff('MICROSECOND', '2020-01-01 00:00:00'::timestamp, '2020-01-01 00:00:01'::timestamp);
SELECT timestampdiff('HOUR', '2020-01-01 00:00:00'::timestamp, '2020-01-02 03:00:00'::timestamp);
SELECT timestampdiff('SECOND', '2020-01-01 00:00:00'::timestamp, '2020-01-01 00:01:00'::timestamp);
SELECT timestampdiff('YEAR2', '2020-01-01'::timestamp, '2021-01-01'::timestamp);

-- -------------------------------------------------------------
-- TIMESTAMPADD
-- -------------------------------------------------------------
SELECT timestampadd('MINUTE', 1, '2003-01-02'::timestamp);
SELECT timestampadd('WEEK', 1, '2003-01-02'::timestamp);
SELECT timestampadd('QUARTER', 1, '2003-11-01'::timestamp);
SELECT timestampadd('YEAR', -1, '2003-01-02'::timestamp);
SELECT timestampadd('MONTH', 1, '2022-01-31'::timestamp);
SELECT timestampadd('MONTH', 1, '2024-01-31'::timestamp);
SELECT timestampadd('DAY', 31, '2020-01-01 23:59:59'::timestamp);
SELECT timestampadd('MICROSECOND', 500000, '2020-01-01 00:00:00'::timestamp);
SELECT timestampadd('SECOND', 1, '1999-12-31 23:59:59'::timestamp);
SELECT timestampadd('FORTNIGHT', 1, '2003-01-02'::timestamp);

-- -------------------------------------------------------------
-- LAST_DAY
-- -------------------------------------------------------------
SELECT last_day('2003-02-05'::timestamp);
SELECT last_day('2004-02-05'::timestamp);
SELECT last_day('2003-12-31'::date);
SELECT last_day(NULL);

-- -------------------------------------------------------------
-- QUARTER / DAY functions
-- -------------------------------------------------------------
SELECT quarter('2008-04-01'::timestamp);
SELECT quarter('2008-01-01'::date);
SELECT dayofweek('2007-02-03'::timestamp);
SELECT dayofweek('2026-10-14'::timestamp);
SELECT weekday('2026-10-14'::timestamp);
SELECT dayofmonth('2026-10-14'::timestamp);
SELECT dayofyear('2026-02-28'::timestamp);
SELECT dayofyear('2024-12-31'::timestamp);
SELECT weekofyear('2008-02-20'::timestamp);
SELECT weekofyear('2008-12-31'::timestamp);

-- -------------------------------------------------------------
-- FORMAT
-- -------------------------------------------------------------
SELECT format(12332.123456, 4);
SELECT format(12332.1, 4);
SELECT format(12332.123456, 0);
SELECT format(-12332.123456, 4);
SELECT format(12332.2::double precision, 2);
SELECT format(12332, 1);
SELECT format(NULL, 2);

-- -------------------------------------------------------------
-- TRUNCATE
-- -------------------------------------------------------------
SELECT "truncate"(1.223, 1);
SELECT "truncate"(1.999, 1);
SELECT "truncate"(1.999, 0);
SELECT "truncate"(-1.999, 1);
SELECT "truncate"(122, -2);
SELECT "truncate"(-122, -2);
SELECT mysql_truncate(122.45, -1);
SELECT "truncate"(1.999::double precision, 1);

-- -------------------------------------------------------------
-- IF / IFNULL / ISNULL
-- -------------------------------------------------------------
SELECT if(1 > 0, 'true', 'false');
SELECT if(1 > 2, 'true', 'false');
SELECT if(1 > 2, NULL, 'fallback');
SELECT if(NULL, 'a', 'b');
SELECT if(true, 1, 2) + 10;
SELECT ifnull(NULL, 10);
SELECT ifnull('real', 'fallback');
SELECT ifnull(NULL, NULL) IS NULL;
SELECT isnull(NULL);
SELECT isnull(5);
SELECT isnull(''::text);

-- -------------------------------------------------------------
-- GROUP_CONCAT
-- -------------------------------------------------------------
CREATE TABLE gc_test (grp int, val text);
INSERT INTO gc_test VALUES
    (1, 'a'), (1, 'b'), (1, 'c'),
    (2, 'x'), (2, NULL),
    (3, NULL), (3, NULL);

SELECT grp, group_concat(val ORDER BY val) FROM gc_test GROUP BY grp ORDER BY grp;
SELECT grp, group_concat(val, ';' ORDER BY val) FROM gc_test GROUP BY grp ORDER BY grp;
SELECT group_concat(DISTINCT val ORDER BY val) FROM gc_test;
SELECT grp, group_concat(val ORDER BY val DESC) FROM gc_test WHERE grp = 1 GROUP BY grp;

CREATE TABLE gc_num (grp int, val int);
INSERT INTO gc_num VALUES (1, 10), (1, 20), (2, 30);
SELECT grp, group_concat(val ORDER BY val) FROM gc_num GROUP BY grp ORDER BY grp;

SELECT group_concat(NULL::text) FROM gc_test;
SELECT group_concat('only'::text);

DROP TABLE gc_test;
DROP TABLE gc_num;

-- -------------------------------------------------------------
-- search_path usage (extension functions shadowing built-ins)
-- -------------------------------------------------------------
CREATE SCHEMA mysql_compat;
ALTER EXTENSION opentenbase_mysql_compat SET SCHEMA mysql_compat;
SET search_path = mysql_compat, pg_catalog, public;
SELECT concat('a', NULL, 'b');
SELECT concat('a', 1, 2);
RESET search_path;
DROP SCHEMA mysql_compat CASCADE;
