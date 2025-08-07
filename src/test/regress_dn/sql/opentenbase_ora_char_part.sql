------------------------------------------------------------
-- A part of opentenbase_ora.sql, test char functions related cases.
------------------------------------------------------------
\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;

DROP DATABASE IF EXISTS regression_sort;
CREATE DATABASE regression_sort WITH TEMPLATE = template0_ora ENCODING='SQL_ASCII' LC_COLLATE='C' LC_CTYPE='C';

\c regression_ora
-- test varchar2
    -- ERROR (typmod >= 1)
    CREATE TABLE foo (a VARCHAR2(0));
    -- ERROR (number of typmods = 1)
    CREATE TABLE foo (a VARCHAR2(10, 1));
    -- OK
    CREATE TABLE foo (a VARCHAR(5000));
    -- cleanup
    DROP TABLE foo;

    -- OK
    CREATE TABLE foo (a VARCHAR2(5));
    CREATE INDEX ON foo(a);

    --
    -- test that no value longer than maxlen is allowed
    --
    -- ERROR (length > 5)
    INSERT INTO foo VALUES ('abcdef');

    -- ERROR (length > 5);
    -- VARCHAR2 does not truncate blank spaces on implicit coercion
    INSERT INTO foo VALUES ('abcde  ');

    -- OK
    INSERT INTO foo VALUES ('abcde');
    -- OK
    INSERT INTO foo VALUES ('abcdef'::VARCHAR2(5));
    -- OK
    INSERT INTO foo VALUES ('abcde  '::VARCHAR2(5));
    --OK
    INSERT INTO foo VALUES ('abc'::VARCHAR2(5));

    --
    -- test whitespace semantics on comparison
    --
    -- equal
    SELECT 'abcde   '::VARCHAR2(10) = 'abcde   '::VARCHAR2(10);
    -- not equal
    SELECT 'abcde  '::VARCHAR2(10) = 'abcde   '::VARCHAR2(10);

    -- cleanup
    DROP TABLE foo;

    -- varchar2 specified unit is a character or byte (default is byte)
    create table test_varchar2_1(c1 varchar(5), c2 varchar2(5), c3 varchar2(5 char), c4 varchar2(5 byte));
    create table test_varchar2_2(c1 varchar(5), c2 varchar2(5), c3 varchar2(5 CHAR), c4 varchar2(5 BYTE));
    create table test_varchar2_3(c1 varchar(5), c2 varchar2(5), c3 varchar2(5 CHARACTER), c4 varchar2(5 byte));
    create table test_varchar2_4(c1 varchar(5), c2 varchar2(5), c3 varchar2(5 character), c4 varchar2(5 BYTE));

    insert into test_varchar2_1 values('中国中国中', '中', '中国中国中', '中');   -- should OK (c3 maxlen is 5 char)
    insert into test_varchar2_2 values('中国中国中', '中国中国中', '中国中国中', '中');   -- should failed (c2 byte longer than maxlen 5)
    insert into test_varchar2_3 values('中国中国中', '中', '中国中国中', '中国中国中');   -- should failed (c4 byte longer than maxlen 5)
    insert into test_varchar2_4 values('中国中国中', '中', '中国中国中', 'abcde');    -- should OK (c3 maxlen is 5 char, c2/c4 maxlen is 5 byte)

    drop table test_varchar2_1, test_varchar2_2, test_varchar2_3, test_varchar2_4;

-- test nvarchar2
    --
    -- test type modifier related rules
    --
    -- ERROR (typmod >= 1)
    CREATE TABLE bar (a NVARCHAR2(0));
    -- ERROR (number of typmods = 1)
    CREATE TABLE bar (a NVARCHAR2(10, 1));

    -- OK
    CREATE TABLE bar (a VARCHAR(5000));
    CREATE INDEX ON bar(a);

    -- cleanup
    DROP TABLE bar;

    -- OK
    CREATE TABLE bar (a NVARCHAR2(5));

    --
    -- test that no value longer than maxlen is allowed
    --
    -- ERROR (length > 5)
    INSERT INTO bar VALUES ('abcdef');

    -- ERROR (length > 5);
    -- NVARCHAR2 does not truncate blank spaces on implicit coercion
    INSERT INTO bar VALUES ('abcde  ');
    -- OK
    INSERT INTO bar VALUES ('abcde');
    -- OK
    INSERT INTO bar VALUES ('abcdef'::NVARCHAR2(5));
    -- OK
    INSERT INTO bar VALUES ('abcde  '::NVARCHAR2(5));
    --OK
    INSERT INTO bar VALUES ('abc'::NVARCHAR2(5));

    --
    -- test whitespace semantics on comparison
    --
    -- equal
    SELECT 'abcde   '::NVARCHAR2(10) = 'abcde   '::NVARCHAR2(10);
    -- not equal
    SELECT 'abcde  '::NVARCHAR2(10) = 'abcde   '::NVARCHAR2(10);

    -- cleanup
    DROP TABLE bar;

-- test cast string to numeric
drop table if exists test_t;
create table test_t(id varchar2(20));

insert into test_t values('1'); 
insert into test_t values(23);
insert into test_t values(24);
insert into test_t values(25);

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

insert into test_t values('xxx');


select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

drop table if exists test_t;

select 'xxx' || 1 as r;
select 1 || 'xxxx' as r;

drop table if exists test_t;
create table test_t (n int);
insert into test_t values(1);
insert into test_t values(2);
insert into test_t values(3);
insert into test_t values(4);
insert into test_t values(5);
insert into test_t values(6);
insert into test_t values(7);
insert into test_t values(8);
insert into test_t values(9);

select r from (SELECT '7' as r from dual UNION ALL SELECT n+1 as r FROM test_t WHERE n < 7 ) order by r;
drop table if exists test_t;

-- test nlssort
\c regression_sort
DROP TABLE IF EXISTS test_sort;
CREATE TABLE test_sort (name TEXT);
INSERT INTO test_sort VALUES ('red'), ('brown'), ('yellow'), ('Purple');
INSERT INTO test_sort VALUES ('guangdong'), ('shenzhen'), ('Tencent'), ('OpenTenBase');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'en_US.utf8');
SELECT * FROM test_sort ORDER BY NLSSORT(name, '');
SELECT set_nls_sort('invalid');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort(' ');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('en_US.utf8');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
INSERT INTO test_sort VALUES(NULL);
SELECT * FROM test_sort ORDER BY NLSSORT(name);

SELECT set_nls_sort('nls_sort = russian');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('nls_sortr = pt_PT.iso885915@euro');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('nls_sortr = en_US.iso885915');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sort =   ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sort =  zh_CN.gb18030 ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sort =  wa_BE.iso885915@euro ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'NLS_SORT =  tt_RU.utf8@iqtelif ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sortR =  tt_RU.utf8@iqtelif ');
SELECT * FROM test_sort ORDER BY NLSSORT(name,'NLS_SORT = SCHINESE_PINYIN_M');
SELECT * FROM test_sort ORDER BY NLSSORT(name,'NLS_SORT = SCHINESE_STROKE_M');
SELECT * FROM test_sort ORDER BY NLSSORT(name,'NLS_SORT = SCHINESE_RADICAL_M');
DROP TABLE test_sort;

\c regression_ora
SET client_encoding to default;

-- test instr
select instr('Tech on the net', 'e') = 2;
select instr('Tech on the net', 'e', 1, 1) = 2;
select instr('Tech on the net', 'e', 1, 2) = 11;
select instr('Tech on the net', 'e', 1, 3) = 14;
select instr('Tech on the net', 'e', -3, 2) = 2;
select instr('abc', NULL) IS NULL;
select instr('abc', '') IS NULL;
select instr('', 'a') IS NULL;
select 1 = instr('abc', 'a');
select 3 = instr('abc', 'c');
select 0 = instr('abc', 'z');
select 1 = instr('abcabcabc', 'abca', 1);
select 4 = instr('abcabcabc', 'abca', 2);
select 0 = instr('abcabcabc', 'abca', 7);
select 0 = instr('abcabcabc', 'abca', 9);
select 4 = instr('abcabcabc', 'abca', -1);
select 1 = instr('abcabcabc', 'abca', -8);
select 1 = instr('abcabcabc', 'abca', -9);
select 0 = instr('abcabcabc', 'abca', -10);
select 1 = instr('abcabcabc', 'abca', 1, 1);
select 4 = instr('abcabcabc', 'abca', 1, 2);
select 0 = instr('abcabcabc', 'abca', 1, 3);
select 2 = instr('张三','三') from dual;
select 0 = instr('张三',' ') from dual;
select instr('张三','') is NULL from dual;
select instr('','三') is NULL from dual;

-- test substr
select substr('This is a test', 6, 2) = 'is';
select substr('This is a test', 6) =  'is a test';
select substr('TechOnTheNet', 1, 4) =  'Tech';
select substr('TechOnTheNet', -3, 3) =  'Net';
select substr('TechOnTheNet', -6, 3) =  'The';
select substr('TechOnTheNet', -8, 2) =  'On';
select substr('TechOnTheNet', -8, 0) =  '';
select substr('TechOnTheNet', -8, -1) =  '';
select substr(1234567,3.6::smallint)='4567';
select substr(1234567,3.6::int)='4567';
select substr(1234567,3.6::bigint)='4567';
select substr(1234567,3.6::numeric)='34567';
select substr(1234567,-1)='7';
select substr(1234567,3.6::smallint,2.6)='45';
select substr(1234567,3.6::smallint,2.6::smallint)='456';
select substr(1234567,3.6::smallint,2.6::int)='456';
select substr(1234567,3.6::smallint,2.6::bigint)='456';
select substr(1234567,3.6::smallint,2.6::numeric)='45';
select substr(1234567,3.6::int,2.6::smallint)='456';
select substr(1234567,3.6::int,2.6::int)='456';
select substr(1234567,3.6::int,2.6::bigint)='456';
select substr(1234567,3.6::int,2.6::numeric)='45';
select substr(1234567,3.6::bigint,2.6::smallint)='456';
select substr(1234567,3.6::bigint,2.6::int)='456';
select substr(1234567,3.6::bigint,2.6::bigint)='456';
select substr(1234567,3.6::bigint,2.6::numeric)='45';
select substr(1234567,3.6::numeric,2.6::smallint)='345';
select substr(1234567,3.6::numeric,2.6::int)='345';
select substr(1234567,3.6::numeric,2.6::bigint)='345';
select substr(1234567,3.6::numeric,2.6::numeric)='34';
select substr('abcdef'::varchar,3.6::smallint)='def';
select substr('abcdef'::varchar,3.6::int)='def';
select substr('abcdef'::varchar,3.6::bigint)='def';
select substr('abcdef'::varchar,3.6::numeric)='cdef';
select substr('abcdef'::varchar,3.5::int,3.5::int)='def';
select substr('abcdef'::varchar,3.5::numeric,3.5::numeric)='cde';
select substr('abcdef'::varchar,3.5::numeric,3.5::int)='cdef';

-- test lengthb
select length('opentenbase_ora'),lengthB('opentenbase_ora') from dual;

-- test strposb
select strposb('abc', '') from dual;
select strposb('abc', 'a') from dual;
select strposb('abc', 'c') from dual;
select strposb('abc', 'z') from dual;
select strposb('abcabcabc', 'abca') from dual;

--test regexp_replace function
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,2) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,'1') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,to_number(2)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,2.1::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,to_char(1)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,to_char(1)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,power(1,1)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',4) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',5) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1000000) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@','5') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',5.5::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',to_char(5)) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',power(1,1)::int) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,'i') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,'c') output  from dual;
SELECT regexp_replace('abc1
def2', '[[:digit:]].d','@',1,1,'n') output  from dual;
SELECT regexp_replace('abc1
def2', '[[:digit:]].d','#',1,1,'xic') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]] d','@',1,1,'x') output  from dual;
select regexp_replace('abcxxx#%
adfbc','^a','@',1,2,'n') from dual;
select regexp_replace('abcxxx#%
adfbc','^a','@',1,2,'c') from dual;
SELECT regexp_replace('', '', '1', 1, 0)
regexp_replace FROM DUAL;
SELECT opentenbase_ora.regexp_replace('', '', '', 1, 0) regexp_replace FROM DUAL;
select opentenbase_ora.REGEXP_REPLACE('i,aaaaa,bbi,ccc', ',', '') from dual;
select opentenbase_ora.REGEXP_REPLACE('i,aaaaa,bbi,ccc', ',', null) from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,'') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,null) output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]','@',1,'') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@', null, 1) output  from dual;
select opentenbase_ora.regexp_replace('abcxxx', 'xx') from dual;
select opentenbase_ora.regexp_replace('abcxxx', '') from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]','@', '') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@', null) output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@','', 1,'x') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@',3, null,'x') output  from dual;
SELECT opentenbase_ora.regexp_replace('abc1def2', '[[:digit:]]D','@',3, '','x') output  from dual;

-- fix customer issue:877165291
\c regression
select regexp_replace('aaaaaaaaa', '[^0-9]',',') from dual;
select regexp_replace('aaaaaaaaa', '[^0-9]','') from dual;
select regexp_replace('aaaaaaaaa', null,',') from dual;
select regexp_replace('aaaaaaaaa', null, null) from dual;
select regexp_replace(null, null, null) from dual;
select regexp_replace(null, 'a', null) from dual;
select regexp_replace(null, 'a', 'a') from dual;

\c regression_ora
select regexp_replace('aaaaaaaaa', '[^0-9]',',') from dual;
select regexp_replace('aaaaaaaaa', '[^0-9]','') from dual;
select regexp_replace('aaaaaaaaa', null,',') from dual;
select regexp_replace('aaaaaaaaa', null, null) from dual;
select regexp_replace(null, null, null) from dual;
select regexp_replace(null, 'a', null) from dual;
select regexp_replace(null, 'a', 'a') from dual;

--test regexp_count function
select regexp_count('abcdfbc','Bc',1,'i') from dual;
select regexp_count('abcdfBc','Bc',1,'c') from dual;
select regexp_count('ab
cdfbc','b.c',1,'n') from dual;
select regexp_count('ab
cdfbc','b.c',1,'i') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'m') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'i') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'n') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'x') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'c') from dual;
select regexp_count('abcvvbcvvb c','b c',1,'x') from dual;
select regexp_count('abcvvbcvvb c','b c',1,'n') from dual;
select regexp_count('abcvvbcvvBC','bc',1,'ic') from dual;
select regexp_count('abcvvbcvvBC','bc',1,'ci') from dual;
select regexp_count('abcvvbcvvBC','b c',1,'ix') from dual;
select regexp_count('abcvvb
cvvB
C','b.c',1,'in') from dual;
select regexp_count('abcvvb cvvB C','b c') from dual;
select regexp_count('abacvvb
cvvB C','b.c') from dual;
select regexp_count('abc
abc','bc?') from dual;
select regexp_count('abcvvbcvvbc','bc',2.9::int,'c') from dual;
select regexp_count('abcvvbcvvbc','bc',exp(2)::int,'c') from dual;
select regexp_count('abcvvbcvvbc','bc','1','c') from dual;
select regexp_count('abcvvbcvvbc','bc',-1,'c') from dual;
select regexp_count('abcvvbcvvbc','bc',1000000,'c') from dual;
select regexp_count('12345','123',1) from dual;
select regexp_count('abcvvbcvvbc','bc',2.1::int,'c') from dual;
select regexp_count(null,'',1,'i') from dual;
select regexp_count('','',1,'i') from dual;

--test regexp_substr function
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,2::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1::bigint,'1') output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,to_number(2)::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,2.1::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,to_char(1)::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1,power(2,1)::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',4::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',5::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',1000000) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]','5'::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',5.5::bigint) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',to_char(5)::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]',power(5,1)::int) output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]D',1,1,'i') output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]]D',1,1,'c') output  from dual;
SELECT regexp_substr('abc1
def2', '[[:digit:]].d',1,1,'n') output  from dual;
SELECT regexp_substr('abc1
def2', '[[:digit:]].d',1,1,'i') output  from dual;
SELECT regexp_substr('abc1def2', '[[:digit:]] d',1,1,'x') output  from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'m') from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'n') from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'i') from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'x') from dual;
select regexp_substr('abcxxx#%
adfbc','^a',1,2,'c') from dual;
SELECT REGEXP_SUBSTR('123', '12', 1, 1)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 4)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 1)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 9::bigint)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 0)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 4.5::bigint) FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', to_char(4)::int)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i',power(2,2)::bigint)  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890', '(123)(4(56)(78))', 1, 1, 'i', '4')  FROM DUAL;
SELECT REGEXP_SUBSTR('1234567890abcdefg', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)(a)(b)', 1, 1, 'i', 10::bigint)  FROM DUAL;
SELECT REGEXP_SUBSTR('', '', 1, 1, '0') FROM DUAL;
SELECT REGEXP_SUBSTR(null, null, 1, 1, '0') FROM DUAL;

\c regression_ora
--test chr function
select chr(67)||chr(0)||chr(65)||chr(0)||chr(84) "dog" from dual;
select chr(0),length(chr(0)) from dual;

--test nchr function
select nchr(256);
select nchr(257);
select nchr(258);
select nchr(420);
select nchr(-1);
select nchr(10241234);
select nchr(1024123);
select nchr('1234');
select nchr(to_char(97)::int);
select nchr(0),length(nchr(0)) from dual;

-- test number like text
select 9999 like '9%' from dual;
select 9999 like '09%' from dual;
select 9999 ilike '9%' from dual;
select 9999 ilike '09%' from dual;
select 9999 not like '9%' from dual;
select 9999 not like '09%' from dual;
select 9999 not ilike '9%' from dual;
select 9999 not ilike '09%' from dual;

create table t_nlssort_regress (f1 integer,f2 varchar2(10),f3 varchar(255));
insert into t_nlssort_regress values(1,'深圳','深圳abcdefghijklmnopqrstuvwxyz');
insert into t_nlssort_regress values(2,'中国','中国ABCDEFGHIJKLMNOPQRSTUVWXYZ');
insert into t_nlssort_regress values(3,'PG','PG_ABCDEFGHIJKLMNOPQRSTUVWXYZ');
insert into t_nlssort_regress values(4,'OpenTenBase','OpenTenBase_abcdefghijklmnopqrstuvwxyz');
insert into t_nlssort_regress values(5,'PG中国','PG中国_abcdefghijklmnopqrstuvwxyz');

--test function NLS_UPPER
SELECT nls_upper(f3) FROM t_nlssort_regress order by f1;
SELECT nls_upper(f3,'NLS_SORT = zh_CN.gb2312') FROM t_nlssort_regress order by f1;
SELECT nls_upper(f3,'NLS_SORT = zh_CN.UTF8') FROM t_nlssort_regress order by f1;
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = SCHINESE_PINYIN_M') ;
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = zh_CN.UTF8') ;
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = zh_CN.gb2312') ;
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = zh_CN.gb18030') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = SCHINESE_PINYIN_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = SCHINESE_STROKE_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = SCHINESE_RADICAL_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = zh_CN.UTF8') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = zh_CN.gb2312') ;
SELECT count(1) FROM t_nlssort_regress group by nls_upper(f3,'NLS_SORT = zh_CN.gb18030') ;
insert into t_nlssort_regress values (6,nls_upper('aBCCCaaaa'));
select * from t_nlssort_regress order by f1;

--test function NLS_LOWER
SELECT nls_lower(f3) FROM t_nlssort_regress order by f1;
SELECT nls_lower(f3,'zh_CN.gb2312') FROM t_nlssort_regress order by f1;;
SELECT nls_lower(f3,'zh_CN.UTF8') FROM t_nlssort_regress order by f1;
SELECT * FROM t_nlssort_regress order by nls_lower(f3,'NLS_SORT = SCHINESE_PINYIN_M') ;
SELECT * FROM t_nlssort_regress order by nls_lower(f3,'NLS_SORT = zh_CN.UTF8') ;
SELECT * FROM t_nlssort_regress order by nls_lower(f3,'NLS_SORT = zh_CN.gb2312') ;
SELECT * FROM t_nlssort_regress order by nls_lower(f3,'NLS_SORT = zh_CN.gb18030') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = SCHINESE_PINYIN_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = SCHINESE_STROKE_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = SCHINESE_RADICAL_M') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = zh_CN.UTF8') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = zh_CN.gb2312') ;
SELECT count(1) FROM t_nlssort_regress group by nls_lower(f3,'NLS_SORT = zh_CN.gb18030') ;
insert into t_nlssort_regress values (7,nls_lower('aBCCCaaaa'));
select * from t_nlssort_regress order by f1;

--get correct encoding
SELECT * FROM t_nlssort_regress order by nls_upper(f3,'NLS_SORT = zh_CN.18030') ;
drop table t_nlssort_regress;

--
select substring(ltrim(to_char(sysdate,'yyyy')), 1,2) FROM dual;
select to_char('1234', 'xxxxx') from dual;
select to_char('1234.23', 'xxxxx') from dual;
select to_char(NULL, 'xxxxx') from dual;
select to_char('1234.23', NULL) from dual;
select to_char(NULL, NULL) from dual;

-- test char(n)
DROP TABLE IF EXISTS CHAR_TBL;
CREATE TABLE CHAR_TBL(key int, f1 char(4));
INSERT INTO CHAR_TBL (key, f1) VALUES (1,'a');
INSERT INTO CHAR_TBL (key, f1) VALUES (2, 'ab');
INSERT INTO CHAR_TBL (key, f1) VALUES (3, 'abcd');
INSERT INTO CHAR_TBL (key, f1) VALUES (4, 'abcde');
INSERT INTO CHAR_TBL (key, f1) VALUES (5, 'abcd    ');
SELECT key, f1, length(f1) FROM CHAR_TBL ORDER BY f1;

DROP TABLE IF EXISTS VARCHAR_TBL;
CREATE TABLE VARCHAR_TBL(key int, f1 varchar(4));
INSERT INTO VARCHAR_TBL (key, f1) VALUES (1, 'a');
INSERT INTO VARCHAR_TBL (key, f1) VALUES (2, 'ab');
INSERT INTO VARCHAR_TBL (key, f1) VALUES (3, 'abcd');
INSERT INTO VARCHAR_TBL (key, f1) VALUES (4, 'abcde');
INSERT INTO VARCHAR_TBL (key, f1) VALUES (5, 'abcd    ');
SELECT key, f1, length(f1) FROM VARCHAR_TBL ORDER BY f1;

-- cast char(n) to text
SELECT CAST(f1 AS text) AS c_text, length(CAST(f1 AS text)) len FROM CHAR_TBL ORDER BY f1;
-- cast char(n) to varchar
SELECT CAST(f1 AS varchar(4)) c_varchar, length(CAST(f1 AS varchar(4))) len FROM CHAR_TBL ORDER BY f1;
DROP TABLE CHAR_TBL;
DROP TABLE VARCHAR_TBL;

SELECT CAST('characters' AS char(20)) || ' and text' AS "Concat char to unknown type";
SELECT CAST('text' AS text) || CAST(' and characters' AS char(20)) AS "Concat text to char";
SELECT '|' || lpad(cast('X123bcd' as char(8)), 10) || '|' from dual;


select to_char(to_date('2023-07-18', 'yyyy-mm-dd'), 'iw') from dual;
select to_char(to_date('2023-01-01', 'yyyy-mm-dd'), 'iw') from dual;
select to_char(to_date('2023-12-31', 'yyyy-mm-dd'), 'iw') from dual;
select to_char(to_date('2023-10-19', 'yyyy-mm-dd'), 'ww') from dual;
select to_char(to_date('2023-01-01', 'yyyy-mm-dd'), 'ww') from dual;
select to_char(to_date('2023-12-31', 'yyyy-mm-dd'), 'ww') from dual;
select to_char(to_date('2023-06-18', 'yyyy-mm-dd'), 'w') from dual;
select to_char(to_date('2023-01-01', 'yyyy-mm-dd'), 'w') from dual;
select to_char(to_date('2023-12-31', 'yyyy-mm-dd'), 'w') from dual;

-- save pg function for upgrade case in opentenbase_ora mode issue: ID884752731
\c regression
-- arg 3
SELECT regexp_replace('hello world', 'o', '0');
-- Output: 'hell0 world'
SELECT regexp_replace('123-456-789', '\d+', 'X');
-- Output: 'X-456-789'

-- arg 4
SELECT regexp_replace('hello world', 'o', '', 'g');
-- Output: 'hell wrld'
SELECT regexp_replace('hello world', 'o', 'O', 'g');
-- Output: 'hellO wOrld'
select regexp_replace('aaaaaaaaa', '[^0-9]',',') from dual;
select regexp_replace('aaaaaaaaa', '[^0-9]','') from dual;
select regexp_replace('aaaaaaaaa', null,',') from dual;
select regexp_replace('aaaaaaaaa', null, null) from dual;
select regexp_replace(null, null, null) from dual;
select regexp_replace(null, 'a', null) from dual;
select regexp_replace(null, 'a', 'a') from dual;

\c regression_ora
-- arg 3
SELECT regexp_replace('hello world', 'o', '0') from dual;
-- Output: 'hell0 w0rld'
SELECT regexp_replace('123-456-789', '\d+', 'X') from dual;
-- Output: 'X-X-X'

-- arg 4
SELECT regexp_replace('hello world', 'o', '', 'g') from dual;
-- ERROR: invalid number
SELECT regexp_replace('hello world', 'o', 'O', 'g') from dual;
-- ERROR: invalid number
select regexp_replace('abcd(def)','\(|\)','\1','g') from dual;
-- ERROR: invalid number

-- const test
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','1a') from dual;
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','gggg') from dual;
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','ggggiii') from dual;
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','ggggiii12') from dual;
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','1g') from dual;

select regexp_replace('1g3fd6f8dyf', '[0-9]',',','1') from dual;
select regexp_replace('1g3fd6f8dyf', null,',',1) from dual;
select regexp_replace('1g3fd6f8dyf', null, null,'5') from dual;
select regexp_replace('1g3fd6f8dyf', null, null,5) from dual;

-- postion err
select regexp_replace('1g3fd6f8dyf', '[0-9]',',','-1') from dual;
select regexp_replace('1g3fd6f8dyf', null,',',-1) from dual;
select regexp_replace('1g3fd6f8dyf', null, null,'0') from dual;
select regexp_replace('1g3fd6f8dyf', null, null,0) from dual;

select regexp_replace('1g3fd6f8dyf', '[0-9]',',','100000') from dual;
select regexp_replace('1g3fd6f8dyf', null,',',100000) from dual;
select regexp_replace('1g3fd6f8dyf', null, null,'100000') from dual;
select regexp_replace('1g3fd6f8dyf', null, null,100000) from dual;

--------------------------------------------------------------------------
-- A part of opentenbase_ora_other_syntax.sql, test char functions related cases.
--------------------------------------------------------------------------
-- null equals '' for opentenbase's functions
\c regression_ora
select translate('123345567789', '#0123456789', '#') from dual;
select translate('123345567789', '#0123456789', '#') from dual where translate('123345567789', '#0123456789', '#') is null;
select translate('123345567789', '#0123456789', '#') from dual where translate('123345567789', '#0123456789', '#') = '';
select translate('123345567789','#0123456789', '#'), nvl(translate('123345567789','#0123456789', '#'), 'f') from dual;

select replace('123', '123', '') from dual;
select 'abcdef' as xxxxxxxx from dual where replace('123', '123', '') is null;
select 'abcdef' as xxxxxxxx from dual where replace('123', '123', '') = '';
select replace('123', '123', '') || 'abcdef' as xxxxxx from dual;
select length(replace('123', '123', '') || 'abcdef') as xxxxxx from dual;
select replace('123', '123', ''), nvl(replace('123', '123', ''), 'f') from dual;

select case when trim('  ') is null then 1 else 0 end from dual;
select trim('  '), nvl(trim('  '), 'f') from dual;
select trim('  ') || 'abcdef' as xxxxxx from dual where trim('  ') is null;
select trim('  ') || 'abcdef' as xxxxxx from dual where trim('  ') = '';
