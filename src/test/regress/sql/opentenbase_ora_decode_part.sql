---------------------------------------------------------------------------------------------
-- A part of opentenbase_ora.sql and opentenbase_ora_other_syntax.sql, test decode functions related cases.
---------------------------------------------------------------------------------------------
\c regression_ora

----------------------------------------
-- A part of opentenbase_ora.sql
----------------------------------------
-- Test VSIZE
DROP TABLE IF EXISTS tbl_emp;
CREATE TABLE tbl_emp (
empno    NUMBER(4) CONSTRAINT pk_emp PRIMARY KEY,
ename    VARCHAR2(10),
job      VARCHAR2(9),
mgr      NUMBER(4),
hiredate DATE,
sal      NUMBER(7,2),
comm     NUMBER(7,2),
deptno   NUMBER(2)
);

SELECT VSIZE(NULL);
SELECT ename, VSIZE(ename) FROM tbl_emp ORDER BY 1;
SELECT VSIZE('opentenbase'::text);
SELECT VSIZE('opentenbase'::cstring);
SELECT VSIZE('opentenbase'::varchar(20));
SELECT VSIZE('opentenbase'::char(20));

DROP TABLE tbl_emp;

-- Test DUMP
SELECT DUMP('Yellow dog'::text) ~ E'^Typ=25 Len=(\\d+): \\d+(,\\d+)*$' AS t;
SELECT DUMP('Yellow dog'::text, 10) ~ E'^Typ=25 Len=(\\d+): \\d+(,\\d+)*$' AS t;
SELECT DUMP('Yellow dog'::text, 17) ~ E'^Typ=25 Len=(\\d+): .(,.)*$' AS t;
SELECT DUMP(10::int2) ~ E'^Typ=21 Len=2: \\d+(,\\d+){1}$' AS t;
SELECT DUMP(10::int4) ~ E'^Typ=23 Len=4: \\d+(,\\d+){3}$' AS t;
SELECT DUMP(10::int8) ~ E'^Typ=20 Len=8: \\d+(,\\d+){7}$' AS t;
SELECT DUMP(10.23::float4) ~ E'^Typ=700 Len=4: \\d+(,\\d+){3}$' AS t;
SELECT DUMP(10.23::float8) ~ E'^Typ=701 Len=8: \\d+(,\\d+){7}$' AS t;
SELECT DUMP(10.23::numeric) ~ E'^Typ=1700 Len=(\\d+): \\d+(,\\d+)*$' AS t;
-- 等date类型和转换函数合入后，再恢复这个用例，进行测试
-- SELECT DUMP('2008-10-10'::date) ~ E'^Typ=4726 Len=8: \\d+(,\\d+){7}$' AS t;
-- SELECT DUMP('2008-10-10'::timestamp) ~ E'^Typ=1114 Len=8: \\d+(,\\d+){7}$' AS t;
-- SELECT DUMP('2009-10-10'::timestamp) ~ E'^Typ=1114 Len=8: \\d+(,\\d+){7}$' AS t;
SELECT DUMP('Yellow dog'::text, 1010) ~ E'^Typ=25 Len=(\\d+) CharacterSet=(\\w+): \\d+(,\\d+)*$' AS t;
SELECT DUMP('Yellow dog'::text, 1017) ~ E'^Typ=25 Len=(\\d+) CharacterSet=(\\w+): .(,.)*$' AS t;
-- SELECT DUMP('2009-10-10'::timestamp, 10, 1, 5) ~ E'^Typ=1114 Len=8: \\d+(,\\d+){4}$' AS t;
-- SELECT DUMP('2009-10-10'::timestamp, 10, 0, 0) ~ E'^Typ=1114 Len=8: \\d+(,\\d+){7}$' AS t;
-- SELECT DUMP('2009-10-10'::timestamp, 16, 1, 5) ~ E'^Typ=1114 Len=8: [0-9a-fA-F]+(,[0-9a-fA-F]+){4}$' AS t;
-- SELECT DUMP('2009-10-10'::timestamp, 8, 1, 5) ~ E'^Typ=1114 Len=8: [0-7]+(,[0-7]+){4}$' AS t;

-- Test DECODE
select DECODE(1, 1, 100, 2, 200);
select DECODE(2, 1, 100, 2, 200);
select DECODE(3, 1, 100, 2, 200);
select DECODE(3, 1, 100, 2, 200, 300);
select DECODE(NULL, 1, 100, NULL, 200, 300);
select DECODE('1'::text, '1', 100, '2', 200);
select DECODE(2, 1, 'ABC', 2, 'DEF');

-- convert expr and all search params to the type of first search.

-- For type 'bpchar'
select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar);
select DECODE('c'::bpchar, 'a'::bpchar,'postgres'::bpchar);
select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'default value'::bpchar);
select DECODE('c', 'a'::bpchar,'postgres'::bpchar,'default value'::bpchar);

select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar);
select DECODE('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar);
select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar,'default value'::bpchar);
select DECODE('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar,'default value'::bpchar);

select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar);
select DECODE('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar);
select DECODE('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar,'default value'::bpchar);
select DECODE('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar,'default value'::bpchar);

select DECODE(NULL, 'a'::bpchar, 'postgres'::bpchar, NULL,'database'::bpchar);
select DECODE(NULL, 'a'::bpchar, 'postgres'::bpchar, 'b'::bpchar,'database'::bpchar);
select DECODE(NULL, 'a'::bpchar, 'postgres'::bpchar, NULL,'database'::bpchar,'default value'::bpchar);
select DECODE(NULL, 'a'::bpchar, 'postgres'::bpchar, 'b'::bpchar,'database'::bpchar,'default value'::bpchar);

-- For type 'bigint'
select DECODE(2147483651::bigint, 2147483650::bigint,2147483650::bigint);
select DECODE(2147483653::bigint, 2147483651::bigint,2147483650::bigint);
select DECODE(2147483653::bigint, 2147483651::bigint,2147483650::bigint,9999999999::bigint);
select DECODE(2147483653::bigint, 2147483651::bigint,2147483650::bigint,9999999999::bigint);

select DECODE(2147483651::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint);
select DECODE(2147483654::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint);
select DECODE(2147483651::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint,9999999999::bigint);
select DECODE(2147483654::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint,9999999999::bigint);

select DECODE(2147483651::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint);
select DECODE(2147483654::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint);
select DECODE(2147483651::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint,9999999999::bigint);
select DECODE(2147483654::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint,9999999999::bigint);

select DECODE(NULL, 2147483651::bigint, 2147483650::bigint, NULL,2147483651::bigint);
select DECODE(NULL, 2147483651::bigint, 2147483650::bigint, 2147483652::bigint,2147483651::bigint);
select DECODE(NULL, 2147483651::bigint, 2147483650::bigint, NULL,2147483651::bigint,9999999999::bigint);
select DECODE(NULL, 2147483651::bigint, 2147483650::bigint, 2147483652::bigint,2147483651::bigint,9999999999::bigint);

-- For type 'numeric'
select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4));
select DECODE(12.003::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4));
select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),999999.9999::numeric(10,4));
select DECODE(12.003::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),999999.9999::numeric(10,4));

select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4));
select DECODE(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4));
select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4),999999.9999::numeric(10,4));
select DECODE(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4),999999.9999::numeric(10,4));

select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4));
select DECODE(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4));
select DECODE(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4),999999.9999::numeric(10,4));
select DECODE(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4),999999.9999::numeric(10,4));

select DECODE(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), NULL,214748.3651::numeric(10,4));
select DECODE(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), 12.002::numeric(5,3),214748.3651::numeric(10,4));
select DECODE(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), NULL,214748.3651::numeric(10,4),999999.9999::numeric(10,4));
select DECODE(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), 12.002::numeric(5,3),214748.3651::numeric(10,4),999999.9999::numeric(10,4));

--For type 'date'
select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date);
select DECODE('2020-01-03'::date, '2020-01-01'::date,'2012-12-20'::date);
select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2012-12-21'::date);
select DECODE('2020-01-03'::date, '2020-01-01'::date,'2012-12-20'::date,'2012-12-21'::date);

select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date);
select DECODE('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date);
select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date,'2012-12-31'::date);
select DECODE('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date,'2012-12-31'::date);

select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date);
select DECODE('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date);
select DECODE('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date,'2013-01-01'::date);
select DECODE('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date,'2013-01-01'::date);

select DECODE(NULL, '2020-01-01'::date, '2012-12-20'::date, NULL,'2012-12-21'::date);
select DECODE(NULL, '2020-01-01'::date, '2012-12-20'::date, '2020-01-02'::date,'2012-12-21'::date);
select DECODE(NULL, '2020-01-01'::date, '2012-12-20'::date, NULL,'2012-12-21'::date,'2012-12-31'::date);
select DECODE(NULL, '2020-01-01'::date, '2012-12-20'::date, '2020-01-02'::date,'2012-12-21'::date,'2012-12-31'::date);

-- For type 'time'
select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time);
select DECODE('01:00:03'::time, '01:00:01'::time,'09:00:00'::time);
select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'00:00:00'::time);
select DECODE('01:00:03'::time, '01:00:01'::time,'09:00:00'::time,'00:00:00'::time);

select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time);
select DECODE('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time);
select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time,'00:00:00'::time);
select DECODE('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:01'::time,'12:00:00'::time,'00:00:00'::time);

select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time);
select DECODE('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time);
select DECODE('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time,'00:00:00'::time);
select DECODE('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time,'00:00:00'::time);

select DECODE(NULL, '01:00:01'::time, '09:00:00'::time, NULL,'12:00:00'::time);
select DECODE(NULL, '01:00:01'::time, '09:00:00'::time, '01:00:02'::time,'12:00:00'::time);
select DECODE(NULL, '01:00:01'::time, '09:00:00'::time, NULL,'12:00:00'::time,'00:00:00'::time);
select DECODE(NULL, '01:00:01'::time, '09:00:00'::time, '01:00:02'::time,'12:00:00'::time,'00:00:00'::time);

-- For type 'timestamp'
select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp);
select DECODE('2020-01-03 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp);
select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select DECODE('2020-01-03 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp);
select DECODE('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp);
select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select DECODE('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp);
select DECODE('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp);
select DECODE('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select DECODE('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

select DECODE(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, NULL,'2012-12-20 12:00:00'::timestamp);
select DECODE(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, '2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp);
select DECODE(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, NULL,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select DECODE(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, '2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

-- For type 'timestamptz'
select DECODE('2020-01-01 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz);
select DECODE('2020-01-03 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz);
select DECODE('2020-01-01 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz,'2012-12-20 00:00:00+08'::timestamptz);
select DECODE('2020-01-03 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz,'2012-12-20 00:00:00+08'::timestamptz);

select DECODE('2020-01-01 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz,'2020-01-02 01:00:01+08'::timestamptz,'2012-12-20 12:00:00+08'::timestamptz);
select DECODE('2020-01-04 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz,'2020-01-02 01:00:01+08'::timestamptz,'2012-12-20 12:00:00+08'::timestamptz);
select DECODE('2020-01-01 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz,'2020-01-02 01:00:01+08'::timestamptz,'2012-12-20 12:00:00+08'::timestamptz,'2012-12-20 00:00:00+08'::timestamptz);
select DECODE('2020-01-04 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz,'2020-01-02 01:00:01+08'::timestamptz,'2012-12-20 12:00:00+08'::timestamptz,'2012-12-20 00:00:00+08'::timestamptz);

select DECODE('2020-01-01 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz,'2020-01-02 01:00:01+08'::timestamptz,'2012-12-20 12:00:00+08'::timestamptz, '2020-01-03 01:00:01+08'::timestamptz, '2012-12-20 15:00:00+08'::timestamptz);
select DECODE('2020-01-04 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz,'2020-01-02 01:00:01+08'::timestamptz,'2012-12-20 12:00:00+08'::timestamptz, '2020-01-03 01:00:01+08'::timestamptz, '2012-12-20 15:00:00+08'::timestamptz);
select DECODE('2020-01-01 01:00:01+08'::timestamptz, '2020-01-01 01:00:01+08'::timestamptz,'2012-12-20 09:00:00+08'::timestamptz,'2020-01-02 01:00:01+08'::timestamptz,'2012-12-20 12:00:00+08'::timestamptz, '2020-01-03 01:00:01+08'::timestamptz, '2012-12-20 15:00:00+08'::timestamptz,'2012-12-20 00:00:00+08'::timestamptz);
select DECODE(4, 1,'2012-12-20 09:00:00+08'::timestamptz,2,'2012-12-20 12:00:00+08'::timestamptz, 3, '2012-12-20 15:00:00+08'::timestamptz,'2012-12-20 00:00:00+08'::timestamptz);

select DECODE(NULL, '2020-01-01 01:00:01+08'::timestamptz, '2012-12-20 09:00:00+08'::timestamptz, NULL,'2012-12-20 12:00:00+08'::timestamptz);
select DECODE(NULL, '2020-01-01 01:00:01+08'::timestamptz, '2012-12-20 09:00:00+08'::timestamptz, '2020-01-02 01:00:01+08'::timestamptz,'2012-12-20 12:00:00+08'::timestamptz);
select DECODE(NULL, '2020-01-01 01:00:01+08'::timestamptz, '2012-12-20 09:00:00+08'::timestamptz, NULL,'2012-12-20 12:00:00+08'::timestamptz,'2012-12-20 00:00:00+08'::timestamptz);
select DECODE(NULL, '2020-01-01 01:00:01+08'::timestamptz, '2012-12-20 09:00:00+08'::timestamptz, '2020-01-02 01:00:01+08'::timestamptz,'2012-12-20 12:00:00+08'::timestamptz,'2012-12-20 00:00:00+08'::timestamptz);

select DECODE(1, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(2, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(19, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(20, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(21, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(22, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(23, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select DECODE(20, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');
select DECODE(21, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');
select DECODE(22, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');
select DECODE(23, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');

select DECODE(1.0::numeric,2::bigint,3::bigint,10::numeric);
select DECODE(1.0::numeric,2::bigint,3::bigint,4::bigint,5::bigint,1::bigint,10::bigint);
select DECODE(1.0::numeric,2::bigint,'3'::text,1::bigint,'10'::text);
select DECODE(1::bigint,2::integer,3::integer,10.0::double precision);
select DECODE(1::bigint,2::bigint,3::bigint,10.0::double precision);
select DECODE(1::bigint,2::integer,3::integer,1::integer,10.0::integer);
select DECODE(1::bigint,2::integer,3::integer,10::integer);
select DECODE(1.0::numeric,2::integer,'3'::text,10::numeric);
SELECT DECODE(null::text, 1, 'a', null, 'b', 'default');
SELECT DECODE(1, null::text, 'a', 2, 'b', 1, 'c', 'default');
WITH v_tmp AS
         (SELECT NULL col1, 'x' col2 FROM dual)
SELECT DECODE(tan(null),col1, 'a', 'default')
FROM v_tmp;
WITH v_tmp AS
         (SELECT NULL col1, 'x' col2 FROM dual)
SELECT DECODE(col1,'null', 'a', 'b')
FROM v_tmp;
WITH v_tmp AS
         (SELECT NULL col1, 'x' col2 FROM dual)
SELECT DECODE(null,col1, 'a', 'default')
FROM v_tmp;
WITH v_tmp AS
         (SELECT NULL col1, 'x' col2 FROM dual)
SELECT DECODE(1, col1,'a', 'default')
FROM v_tmp;

-- pre-process constant expression.
drop table if exists t2;
create table t2(f1 int,f2 int);
insert into t2 values(1,0);
select DECODE(f2,0,0,12/f2*100) from t2;
select DECODE(f2,0,0,12/0*100) from t2;
select DECODE(f2,0,0,12/f2*100,0) from t2;
select DECODE(f2,0,0,12/0*100,0) from t2;
drop table if exists t2;

-- complicate case
drop table if exists t_student;
create table t_student(id int, name varchar2(20), age int, sex int);
insert into t_student values(1, '张三', 15, 1);
insert into t_student values(2, '李四', 20, 3);
insert into t_student values(3, '王五', 19, 2);
insert into t_student values(4, '李六', 30, 1);
select DECODE(count(id),0,1,max(to_number(id)+1)) new_id from t_student order by id;
select count(*) from t_student order by 1;    --should pass
select count(*) from t_student order by 2;    --should error
select count(*) from t_student order by id;   --should pass
select count(*) from t_student order by id1;  --should error
insert into t_student select * from t_student where id < 3;
select distinct count(1) over(partition by name) from t_student order by 1;
select distinct count(1) over(partition by name) from t_student order by 1 desc;
drop table t_student;

----------------------------------------
-- A part of racle_other_syntax.sql
----------------------------------------
---------------------
-- DECODE functions
---------------------
select DECODE(0,0,0,0/0) from dual;

select DECODE(1,0,0,1,1,0/0.0) from dual;

select DECODE(1,0,0/0,1,1,0/0.0) from dual;

select DECODE(0,0,0/0,1,1,0/0.0) from dual;

select DECODE(4,0,0,1,1,0/0.0) from dual;

---------------------
-- ORA_HASH functions
---------------------
SELECT ORA_HASH(1111::int2, 4294967295) FROM DUAL;
SELECT ORA_HASH(1111::int2, 100, 97) FROM DUAL;
SELECT ORA_HASH(1111::int2) = ORA_HASH(1111::int2, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH(1111::int4, 4294967295) FROM DUAL;
SELECT ORA_HASH(1111::int4, 100, 97) FROM DUAL;
SELECT ORA_HASH(1111::int4) = ORA_HASH(1111::int4, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH(1111::int8, 4294967295) FROM DUAL;
SELECT ORA_HASH(1111::int8, 100, 97) FROM DUAL;
SELECT ORA_HASH(1111::int8) = ORA_HASH(1111::int8, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH(1111::oid, 4294967295) FROM DUAL;
SELECT ORA_HASH(1111::oid, 100, 97) FROM DUAL;
SELECT ORA_HASH(1111::oid) = ORA_HASH(1111::oid, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH(11.11::float4, 4294967295) FROM DUAL;
SELECT ORA_HASH(11.11::float4, 100, 97) FROM DUAL;
SELECT ORA_HASH(11.11::float4) = ORA_HASH(11.11::float4, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH(11e11::float8, 4294967295) FROM DUAL;
SELECT ORA_HASH(11e11::float8, 100, 97) FROM DUAL;
SELECT ORA_HASH(11e11::float8) = ORA_HASH(11e11::float8, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH('2021-05-24 21:19:52.402748+08', 4294967295) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08', 100, 97) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08') = ORA_HASH('2021-05-24 21:19:52.402748+08', 4294967295, 0) FROM DUAL;

SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::varchar, 4294967295) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::varchar, 100, 97) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::varchar) = ORA_HASH('2021-05-24 21:19:52.402748+08'::varchar, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH('2021-05-24 21:19:52.402748'::timestamp, 4294967295) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748'::timestamp, 100, 97) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748'::timestamp) = ORA_HASH('2021-05-24 21:19:52.402748'::timestamp, 4294967295, 0) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::timestamptz, 4294967295) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::timestamptz, 100, 97) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::timestamptz) = ORA_HASH('2021-05-24 21:19:52.402748+08'::timestamptz, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::timetz, 4294967295) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::timetz, 100, 97) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::timetz) = ORA_HASH('2021-05-24 21:19:52.402748+08'::timetz, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::time, 4294967295) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::time, 100, 97) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52.402748+08'::time) = ORA_HASH('2021-05-24 21:19:52.402748+08'::time, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH('2021-05-24 21:19:52'::date, 4294967295) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52'::date, 100, 97) FROM DUAL;
SELECT ORA_HASH('2021-05-24 21:19:52'::date) = ORA_HASH('2021-05-24 21:19:52'::date, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH(interval '49 days', 4294967295) FROM DUAL;
SELECT ORA_HASH(interval '49 days', 100, 97) FROM DUAL;
SELECT ORA_HASH(interval '49 days', 4294967295, 0) = ORA_HASH(interval '49 days') FROM DUAL;

SELECT ORA_HASH(ascii('W'), 4294967295) FROM DUAL;
SELECT ORA_HASH(ascii('W'), 100, 97) FROM DUAL;
SELECT ORA_HASH(ascii('W')) = ORA_HASH(ascii('W'), 4294967295, 0) FROM DUAL;

SELECT ORA_HASH(true, 100, 97) FROM DUAL;
SELECT ORA_HASH(false, 100, 97) FROM DUAL;
SELECT ORA_HASH(false, 4294967295) FROM DUAL;
SELECT ORA_HASH(false) = ORA_HASH(false, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH('{"a":1, "b":2, "c":3, "d":4}'::jsonb, 100, 97) FROM DUAL;
SELECT ORA_HASH('{"a":1, "b":2, "c":3, "d":4}'::jsonb, 4294967295) FROM DUAL;
SELECT ORA_HASH('{"a":1, "b":2, "c":3, "d":4}'::jsonb) = ORA_HASH('{"a":1, "b":2, "c":3, "d":4}'::jsonb, 4294967295, 0) FROM DUAL;

SELECT ORA_HASH(123456789.98765::numeric, 100, 97) FROM DUAL;
SELECT ORA_HASH(123456789.98765::numeric, 4294967295) FROM DUAL;
SELECT ORA_HASH(123456789.98765::numeric) = ORA_HASH(123456789.98765::numeric, 4294967295, 0) FROM DUAL;

CREATE TABLE tr_ora_hash(f1 int);
INSERT INTO tr_ora_hash VALUES(1), (2),(3), (4);
DROP TABLE tr_ora_hash;

-- do not throw error while pre-processing constant expression.
DROP TABLE IF EXISTS CASE_TBL;
CREATE TABLE CASE_TBL (
                          i integer,
                          f double precision
) DISTRIBUTE BY REPLICATION;
INSERT INTO CASE_TBL VALUES (1, 10.1);
INSERT INTO CASE_TBL VALUES (2, 20.2);
INSERT INTO CASE_TBL VALUES (3, -30.3);
INSERT INTO CASE_TBL VALUES (4, NULL);
SELECT CASE WHEN i > 100 THEN 1/0 ELSE 0 END FROM case_tbl;
SELECT 1/0 FROM CASE_TBL;
TRUNCATE CASE_TBL;
SELECT 1/0 FROM CASE_TBL;
DROP TABLE CASE_TBL;

\c regression
DROP TABLE IF EXISTS CASE_TBL;
CREATE TABLE CASE_TBL (
                          i integer,
                          f double precision
) DISTRIBUTE BY REPLICATION;
INSERT INTO CASE_TBL VALUES (1, 10.1);
INSERT INTO CASE_TBL VALUES (2, 20.2);
INSERT INTO CASE_TBL VALUES (3, -30.3);
INSERT INTO CASE_TBL VALUES (4, NULL);
SELECT CASE WHEN i > 100 THEN 1/0 ELSE 0 END FROM case_tbl;
SELECT 1/0 FROM CASE_TBL;
TRUNCATE CASE_TBL;
SELECT 1/0 FROM CASE_TBL;
DROP TABLE CASE_TBL;
