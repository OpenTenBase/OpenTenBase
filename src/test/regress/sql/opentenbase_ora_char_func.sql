---------------------------------------------
-- Test cases for opentenbase_ora character function.
---------------------------------------------

\c regression_ora

create schema if not exists opentenbase_ora_char_func authorization current_user;
set search_path = opentenbase_ora_char_func;

--------------------------
-- Test ascii function
--------------------------
select ascii('abc') from dual;
select ascii('bcd') from dual;
select ascii('cde') from dual;
select ascii('def') from dual;

select ascii(0) from dual;
select ascii(1) from dual;
select ascii(2) from dual;
select ascii(-1) from dual;

select ascii(0.0) from dual;
select ascii(0.1) from dual;
select ascii(.23) from dual;
select ascii(2.3) from dual;

drop table if exists tbl_ascii_test;
create table tbl_ascii_test(id int, c20 long raw, c21 raw(100), c22 ROWID, c23 varchar2(50), c24 integer);
insert into tbl_ascii_test values (1, 'ABC', 'ABC', '', 'ABC', 0);
select ascii(c21),ascii(c22), ascii(c23), ascii(c24) from tbl_ascii_test;
drop table tbl_ascii_test;

drop table if exists t_ascii;
create table t_ascii(f1 int, f2 varchar2, f3 int, f4 numeric);
insert into t_ascii values(1, 'abc', 0, 0.0);
insert into t_ascii values(2, 'bcd', 1, 0.1);
insert into t_ascii values(3, 'cde', 2, .23);
insert into t_ascii values(4, 'def', -1, 2.3);


drop table t_ascii;

--------------------------
-- Test replace function
--------------------------
select replace('JACK and JUE', 'J', 'BL') "Changes" from dual;
select replace('JACK and JUE', 'J', 23) "Changes" from dual;
select replace('JACK and JUE', 'J') "Changes" from dual;

select replace(replace('$1'||rtrim(trim(leading '=' from '=='||lpad(chr(12540), 20, remainder(1123E13, ascii(122E13)))||'=='), '=')||'$2', '$1', '$$'), '$2', '$$') from dual;
select ascii(replace(rtrim(trim('a' from 'a'||lpad(null||'abcde'||'你好'||-20||3*6||'',remainder('1228', '100'))||'a'), '-'), ' ','')) as ascii from dual;

drop table if exists t_replace;
create table t_replace(f1 int, f2 varchar2);
insert into t_replace values(1, 'JACK');
insert into t_replace values(2, 'JUE');
insert into t_replace values(3, 'JACK and JUE');
insert into t_replace values(4, 'JACK or JUE');

select f1, f2, replace(f2, 'J', 'BL') from t_replace order by f1;
select f1, f2, replace(f2, 'J', 23) from t_replace order by f1;
select f1, f2, replace(f2, 'J') from t_replace order by f1;

drop table t_replace;

drop table if exists replace_t;
create table replace_t(id int, c1 varchar2(100), c2 char(100), c3 nchar(100), c4 nvarchar2(100), c5 clob, c6 nclob, c7 number, c8 smallint, c9 date, c10 timestamp);
insert into replace_t values(1, 'abc', 'eqw', chr(20)||'da', chr(30)||'==', '而我却', chr(465)||'饿我去的', 465.89, -100, '2021-07-06 10:15:12', to_timestamp('2021-07-06 14:38:48.680297', 'yyyy-mm-dd HH24:mi:ss.ff'));
insert into replace_t values(2, '中国', '大苏打', chr(125)||'ddsaa', chr(120)||'==', 'daewq', chr(879)||'大苏打ew', 1.89, 128,'2020-08-06 10:15:12', to_timestamp('2021-07-06 14:38:48.680297', 'yyyy-mm-dd HH24:mi:ss.ff'));
insert into replace_t values(3, '四届', 'eqw', chr(51082)||'打算', chr(654)||'==', '打算而我却', chr(135)||'而我打算eqw', 0.89, 0, '2020-08-06 10:15:12', to_timestamp('2021-07-08 14:38:48', 'yyyy-mm-dd HH24:mi:ss'));
insert into replace_t values(4, 'abc', '而且我给v的', chr(20)||'da', chr(445)||'==', '和梵蒂冈', chr(135)||'aeq4556大', -895.89, 11, '2021-08-06 10:15:12', to_timestamp('2021-08-08 14:38:48', 'yyyy-mm-dd HH24:mi:ss'));
insert into replace_t values(5, 'abewqe', 'eqw', chr(34)||'恶趣味', chr(102)||'==', '日期', chr(798)||'321', -7895.89, 22, '2020-09-06 10:15:12', to_timestamp('2021-07-09 14:38:48', 'yyyy-mm-dd HH24:mi:ss'));
insert into replace_t values(6, '打算', 'dasdas', chr(38)||'大师的人情味', chr(128)||'==', '发', chr(4565)||'', 7851.89, -56, '2020-10-06 10:15:12', to_timestamp('2021-10-08 14:38:48', 'yyyy-mm-dd HH24:mi:ss'));
-- 插入空值
insert into replace_t values(7, '', 'dasdas','', chr(128)||'==', '发', chr(4565)||'', '', 127, '', '');
insert into replace_t values(9, '', 'dasdas', '', '', '', chr(4565)||'', 7895.89, '', '', to_timestamp('2021-10-08 14:38:48', 'yyyy-mm-dd HH24:mi:ss'));

-- 查询
select id, c1, replace(c1, 'a', '替换') replace from replace_t order by id;
select id, c1, c2, c3, replace(c1, c2, c3) as replace from replace_t order by id;
select id, c1, c2, c3, replace(c2, c2, c3) as replace from replace_t order by id;
select id, c1, c2, c3, replace(c8, c8, c7) as replace from replace_t order by id;
select id, c9 ,replace(c9, '-', '/') as replace from replace_t order by id;
select id, c10 ,replace(c10, '-', '/') as replace from replace_t order by id;
-- where 语句
select id, c1 from replace_t where replace(c1, 'a', '替换')=c1 order by id;
select id, c1, c2 from replace_t where replace(c1, c2, '替换')=c1 order by id;
select id, c1, c2, c3 from replace_t where replace(c1, c2, c3)=c3 order by id;

drop table if exists replace_t;

--------------------------
-- Test instr function
--------------------------
select instr('CORPORATE FLOOR', 'OR', 3, 2) "Instring" from dual;
select instr('CORPORATE FLOOR', 'OR', -1, 1) "Instring" from dual;
select instr('CORPORATE FLOOR', 'OR', -1) "Instring" from dual;
select instr('CORPORATE FLOOR', 'OR') "Instring" from dual;
select instr('CORPORATE FLOOR', 'OR', 0) "Instring" from dual;
select instr('CORPORATE FLOOR', 'OR', 0.1) "Instring" from dual;

select instr('This is a test', 6, 2) from dual;
select instr('This is a test', 6) from dual;

drop table if exists t_instr;
create table t_instr(f1 int, f2 varchar2, f3 varchar2);
insert into t_instr values(1, 'CORPORATE FLOOR', 'OR');
insert into t_instr values(2, 'A CORPORATE FLOOR', 'OR');
insert into t_instr values(3, 'AB CORPORATE FLOOR', 'OR');
insert into t_instr values(4, 'ABC CORPORATE FLOOR', 'OR');

select f1, f2, instr(f2, f3, 3, 2) from t_instr order by f1;
select f1, f2, instr(f2, f3, -1, 1) from t_instr order by f1;
select f1, f2, instr(f2, f3, -1) from t_instr order by f1;
select f1, f2, instr(f2, f3) from t_instr order by f1;
select f1, f2, instr(f2, f3, 0) from t_instr order by f1;
select f1, f2, instr(f2, f3, 0.1) from t_instr order by f1;

drop table t_instr;

--------------------------
-- Test substr function
--------------------------
select substr('ABCDEFG', 3, 4) "Substring" from dual;
select substr('ABCDEFG', -5, 4) "Substring" from dual;
select substr('1234567890', 1) "Substring with bytes" from dual;
select substr('1234567890', 0) "Substring with bytes" from dual;
select substr('1234567890', 2) "Substring with bytes" from dual;
select substr(1234567890, 1) "Substring with bytes" from dual;
select substr(1234567890, 2) "Substring with bytes" from dual;
select substr(1234567890, 0) "Substring with bytes" from dual;
select substr(1234567890, -1) "Substring with bytes" from dual;
select substr(1234567890, 5, 3) "Substring with bytes" from dual;
select substr(1234567890, -5, 3) "Substring with bytes" from dual;
select substr(123.456, 2, 3) "Substring with bytes" from dual;
select substr(123.456, 2) "Substring with bytes" from dual;

select substr( '今天天气很不错', 3) from dual;
select substr( '今天天气很不错', 3, 2) from dual;

drop table if exists t_substr;
create table t_substr(f1 int, f2 varchar2, f3 numeric);
insert into t_substr values(1, 'ABCDEFG', 123.456);
insert into t_substr values(2, '1234567890', 1234567890);
insert into t_substr values(3, '今天天气很不错', 98765.4321);
insert into t_substr values(4, '9876543210', 9876543210);

select f1, f2, substr(f2, 3, 4), f3, substr(f3, 3, 4) from t_substr order by f1;
select f1, f2, substr(f2, -5, 4), f3, substr(f3, -5, 4) from t_substr order by f1;
select f1, f2, substr(f2, 1), f3, substr(f3, 1) from t_substr order by f1;
select f1, f2, substr(f2, 0), f3, substr(f3, 0) from t_substr order by f1;
select f1, f2, substr(f2, 2), f3, substr(f3, 2) from t_substr order by f1;
select f1, f2, substr(f2, -1), f3, substr(f3, -1) from t_substr order by f1;
select f1, f2, substr(f2, 5, 3), f3, substr(f3, 5, 3) from t_substr order by f1;
select f1, f2, substr(f2, -5, 3), f3, substr(f3, -5, 3) from t_substr order by f1;
select f1, f2, substr(f2, 2, 3), f3, substr(f3, 2, 3) from t_substr order by f1;

drop table t_substr;

--------------------------
-- Test substrb function
--------------------------
select substrb('ABCDEFG', 5, 4.2) "Substring with bytes" from dual;
select substrb('ABCDEFG', 1.2) "Substring with bytes" from dual;
select substrb('ABCDEFG', 1) "Substring with bytes" from dual;

drop table if exists t_substrb;
create table t_substrb(f1 int, f2 nvarchar2);
insert into t_substrb values(1, 'ABCDEFG');
insert into t_substrb values(2, '1234567890');
insert into t_substrb values(3, '今天天气很不错');
insert into t_substrb values(4, '9876543210');

select f1, f2, substrb(f2, 5, 4.2) from t_substrb order by f1;
select f1, f2, substrb(f2, 1.2) from t_substrb order by f1;
select f1, f2, substrb(f2, 1) from t_substrb order by f1;

drop table t_substrb;

-------------------------------------
-- Test length && lengthb function
-------------------------------------
select lengthb('中国') from dual;
select lengthb('123') from dual;
select lengthb(to_date('2018-01-17 15:56:30','yyyy-mm-dd hh24:mi:ss')) from dual;
select length('中国') from dual;
select length(123) from dual;
select length(123.3456) from dual;
select length(to_date('2018-01-17 15:56:30','yyyy-mm-dd hh24:mi:ss')) from dual;

drop table if exists t_length;
create table t_length(f1 int, f2 nvarchar2, f3 numeric);
insert into t_length values(1, '123', 123);
insert into t_length values(2, '中国', 123.3456);

select f1, f2, length(f2), lengthb(f2), f3, length(f3) from t_length order by f1;

drop table t_length;

--------------------------
-- Test nchr function
--------------------------
select nchr(123) from dual;
select nchr(123.3) from dual;
select nchr(123.6) from dual;

drop table if exists t_nchr;
create table t_nchr(f1 int, f2 numeric);
insert into t_nchr values(1, 123);
insert into t_nchr values(2, 123.3);
insert into t_nchr values(3, 123.6);

select f1, f2, nchr(f2) from t_nchr order by f1;

drop table t_nchr;

------------------------------
-- Test empty_blob function
------------------------------
drop table if exists t_blob;
create table t_blob(b blob, c clob);
insert into t_blob values(empty_blob(), '2');
select length(b), length(c) from t_blob;
drop table t_blob;

--------------------------
-- Test strposb function
--------------------------
select strposb('abc123abc', '123') from dual;

drop table if exists t_strposb;
create table t_strposb(f1 int, f2 varchar2, f3 varchar2, f4 varchar2);
insert into t_strposb values(1, 'abc123abc', '123', 'abc');
insert into t_strposb values(2, 'abcabc123', '123', 'abc');
insert into t_strposb values(3, '123abcabc', '123', 'abc');

select f1, f2, f3, strposb(f2, f3) from t_strposb order by f1;
select f1, f2, f4, strposb(f2, f4) from t_strposb order by f1;

drop table t_strposb;

--------------------------
-- Test nlssort function
--------------------------
drop table if exists test_nlssort;
create table test_nlssort (name varchar2(15));
insert into test_nlssort values ('Gaardiner');
insert into test_nlssort values ('Gaberd');
insert into test_nlssort values ('Gaasten');
select * from test_nlssort order by nlssort(name, 'NLS_SORT = C');
select * from test_nlssort order by name;
drop table test_nlssort;

-----------------------------
-- Test nls_lower function
-----------------------------
select nls_lower('NOKTASINDA', 'NLS_SORT = danish') "Lowercase" from dual;
select nls_lower('NOKTASINDA', 'NLS_SORT = SCHINESE_PINYIN_M') "Lowercase" from dual;
select nls_lower('NOKTASINDA', 'NLS_SORT = SCHINESE_STROKE_M') "Lowercase" from dual;
select nls_lower('NOKTASINDA', 'NLS_SORT = SCHINESE_RADICAL_M') "Lowercase" from dual;

drop table if exists t_nls_lower;
create table t_nls_lower(f1 int, f2 varchar2);
insert into t_nls_lower values(1, 'NOKTASINDA');
insert into t_nls_lower values(2, 'ABCDEFG');
insert into t_nls_lower values(3, 'ABCDefg');
insert into t_nls_lower values(4, 'abcdefg');

select f1, f2, nls_lower(f2), nls_lower(f2, 'NLS_SORT = danish'), nls_lower(f2, 'NLS_SORT = SCHINESE_PINYIN_M'), nls_lower(f2, 'NLS_SORT = SCHINESE_STROKE_M'), nls_lower(f2, 'NLS_SORT = SCHINESE_RADICAL_M') from t_nls_lower order by f1;

drop table t_nls_lower;

-----------------------------
-- Test nls_upper function
-----------------------------
select nls_upper('große') "Uppercase" from dual;
select nls_upper('große', 'NLS_SORT = German') "Uppercase" from dual;

drop table if exists t_nls_upper;
create table t_nls_upper(f1 int, f2 varchar2);
insert into t_nls_upper values(1, 'NOKTASINDA');
insert into t_nls_upper values(2, 'ABCDEFG');
insert into t_nls_upper values(3, 'ABCDefg');
insert into t_nls_upper values(4, 'abcdefg');
insert into t_nls_upper values(5, 'große');

select f1, f2, nls_upper(f2), nls_upper(f2, 'NLS_SORT = German'), nls_upper(f2, 'NLS_SORT = danish'), nls_upper(f2, 'NLS_SORT = SCHINESE_PINYIN_M'), nls_upper(f2, 'NLS_SORT = SCHINESE_STROKE_M'), nls_upper(f2, 'NLS_SORT = SCHINESE_RADICAL_M') from t_nls_upper order by f1;

drop table t_nls_upper;

-----------------------------
-- Test nls_initcap function
-----------------------------
select nls_initcap('ijsland') "InitCap" from dual;
select nls_initcap('ijsland', 'NLS_SORT = Dutch') "InitCap" from dual;

drop table if exists t_nls_initcap;
create table t_nls_initcap(f1 int, f2 varchar2);
insert into t_nls_initcap values(1, 'ijsland');
insert into t_nls_initcap values(2, 'ABCDEFG');
insert into t_nls_initcap values(3, 'abcdefg');
insert into t_nls_initcap values(4, 'it is a dog');

select f1, f2, nls_initcap(f2), nls_initcap(f2, 'NLS_SORT = Dutch') from t_nls_initcap order by f1;

drop table t_nls_initcap;

-----------------------------
-- Test lower function
-----------------------------
select lower(123) from dual;
select lower('') from dual;
select lower(null) from dual;

-----------------------------
-- Test upper function
-----------------------------
select upper(123) from dual;
select upper('') from dual;
select upper(null) from dual;

-------------------------------
-- Test regexp_count function
-------------------------------
select regexp_count('123123123123123', '(12)3', 1, 'i') from dual;
select regexp_count('abcdefgABcdefg', '(ab)c', 1, 'i') from dual;
select regexp_count('abcdefgABcdefg', '(ab)c', 1, 'c') from dual;
select regexp_count('abcdefgABcdefg', '(ab)c') from dual;
select regexp_count('ABC123', '[A-Z]'), regexp_count('A1B2C3', '[A-Z]') from dual;
select regexp_count('ABC123', '[A-Z][0-9]'), regexp_count('A1B2C3', '[A-Z][0-9]') from dual;
select regexp_count('ABC123', '[A-Z][0-9]'), regexp_count('A1B2C3', '[A-Z][0-9]') from dual;

--各种数据类型
drop table if exists t_regexp_count;
create table t_regexp_count(f1 int, f2 varchar2, f3 varchar2);
insert into t_regexp_count values(1, '123123123123123', '(12)3');
insert into t_regexp_count values(2, 'abcdefgABcdefg', '(ab)c');
insert into t_regexp_count values(3, 'ABC123', '[A-Z]');
insert into t_regexp_count values(4, 'ABC123', '[A-Z][0-9]');
insert into t_regexp_count values(5, 'A1B2C3', '[A-Z]');
insert into t_regexp_count values(6, 'A1B2C3', '[A-Z][0-9]');

select f1, f2, f3, regexp_count(f2, f3), regexp_count(f2, f3, 1, 'i'), regexp_count(f2, f3, 1, 'c') from t_regexp_count order by f1;

drop table t_regexp_count;

-------------------------------
-- Test regexp_substr function
-------------------------------
select regexp_substr('500 opentenbase_ora Parkway, Redwood Shores, CA', ',[^,]+,') from dual;
select regexp_substr('http://www.example.com/products', 'http://([[:alnum:]]+\.?){3,4}/?') from dual;
select regexp_substr('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 1) from dual;
select regexp_substr('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 4) from dual;

with strings as (
  select 'ABC123' str from dual union all
  select 'A1B2C3' str from dual union all
  select '123ABC' str from dual union all
  select '1A2B3C' str from dual
)
  select regexp_substr(str, '[0-9]') First_Occurrence_of_Number,
         regexp_substr(str, '[0-9].*') Num_Followed_by_String,
         regexp_substr(str, '[A-Z][0-9]') Letter_Followed_by_String
  from strings;

with strings as (
  select 'LHRJFK/010315/JOHNDOE' str from dual union all
  select 'CDGLAX/050515/JANEDOE' str from dual union all
  select 'LAXCDG/220515/JOHNDOE' str from dual union all
  select 'SFOJFK/010615/JANEDOE' str from dual
)
  select regexp_substr(str, '[A-Z]{6}') String_of_6_characters,
         regexp_substr(str, '[0-9]+') First_Matching_Numbers,
         regexp_substr(str, '[A-Z].*$') Letter_by_other_characters,
         regexp_substr(str, '/[A-Z].*$') Slash_letter_and_characters
  from strings;

drop table if exists t_regexp_substr;
create table t_regexp_substr(f1 int, f2 varchar2, f3 varchar2);
insert into t_regexp_substr values(1, '500 opentenbase_ora Parkway, Redwood Shores, CA', ',[^,]+,');
insert into t_regexp_substr values(2, 'http://www.example.com/products', 'http://([[:alnum:]]+\.?){3,4}/?');
insert into t_regexp_substr values(3, '1234567890', '(123)(4(56)(78))');

insert into t_regexp_substr values(4, 'ABC123', '[0-9]');
insert into t_regexp_substr values(5, 'A1B2C3', '[0-9]');
insert into t_regexp_substr values(6, '123ABC', '[0-9]');
insert into t_regexp_substr values(7, '1A2B3C', '[0-9]');

insert into t_regexp_substr values(8, 'LHRJFK/010315/JOHNDOE', '[0-9]');
insert into t_regexp_substr values(9, 'CDGLAX/050515/JANEDOE', '[0-9]');
insert into t_regexp_substr values(10, 'LAXCDG/220515/JOHNDOE', '[0-9]');
insert into t_regexp_substr values(11, 'SFOJFK/010615/JANEDOE', '[0-9]');

select f1, f2, f3, regexp_substr(f2, f3), regexp_substr(f2, f3, 1, 1, 'i', 1), regexp_substr(f2, f3, 1, 1, 'i', 4) from t_regexp_substr order by f1;
select f1, f2, f3, regexp_substr(f2, '[0-9]'), regexp_substr(f2, '[0-9].*'), regexp_substr(f2, '[A-Z][0-9]') from t_regexp_substr order by f1;
select f1, f2, f3, regexp_substr(f2, '[A-Z]{6}'), regexp_substr(f2, '[0-9]+'), regexp_substr(f2, '[A-Z].*$'), regexp_substr(f2, '/[A-Z].*$') from t_regexp_substr order by f1;

drop table t_regexp_substr;
select 1 as res from dual where regexp_substr('123ABC', '[A-Z][0-9]') is null;

-------------------------------
-- Test regexp_replace function
-------------------------------
with strings as (
  select 'abc123' s from dual union all
  select '123abc' s from dual union all
  select 'a1b2c3' s from dual
)
  select s "STRING", regexp_replace(s, '[0-9]', '') "MODIFIED_STRING"
  from strings;

with strings as (
  select 'abc123' s from dual union all
  select '123abc' s from dual union all
  select 'a1b2c3' s from dual
)
  select s "STRING", regexp_replace(s, '[0-9]', '', 1, 1) "MODIFIED_STRING"
  from strings;

with strings as (
  select 'abc123' s from dual union all
  select '123abc' s from dual union all
  select 'a1b2c3' s from dual
)
  select s "STRING", regexp_replace(s, '[0-9]', '', 1, 2) "MODIFIED_STRING"
  from strings;

drop table if exists t_regexp_replace;
create table t_regexp_replace(f1 int, f2 varchar2, f3 varchar2, f4 varchar2);
insert into t_regexp_replace values(1, 'abc123', '[0-9]', '');
insert into t_regexp_replace values(2, '123abc', '[0-9]', '');
insert into t_regexp_replace values(3, 'a1b2c3', '[0-9]', '');

select f1, f2, f3, f4, regexp_replace(f2, f3, f4), regexp_replace(f2, f3, f4, 1, 1), regexp_replace(f2, f3, f4, 1, 2) from t_regexp_replace order by f1;

drop table t_regexp_replace;

with strings as (
  select 'AddressLine1' s from dual union all
  select 'ZipCode'      s from dual union all
  select 'Country'      s from dual
)
  select s "STRING", lower(regexp_replace(s, '([A-Z0-9])', '_\1', 2)) "MODIFIED_STRING"
  from strings;

drop table if exists t_regexp_replace;
create table t_regexp_replace(f1 int, f2 varchar2, f3 varchar2, f4 varchar2);
insert into t_regexp_replace values(1, 'AddressLine1', '([A-Z0-9])', '_\1');
insert into t_regexp_replace values(2, 'ZipCode',      '([A-Z0-9])', '_\1');
insert into t_regexp_replace values(3, 'Country',      '([A-Z0-9])', '_\1');


select f1, f2, f3, f4, lower(regexp_replace(f2, f3, f4, 2)) from t_regexp_replace order by f1;

drop table t_regexp_replace;

with strings as (
  select 'Hello  World'       s from dual union all
  select 'Hello        World' s from dual union all
  select 'Hello,   World  !'  s from dual
)
  select s "STRING", regexp_replace(s, ' {2,}', ' ') "MODIFIED_STRING"
  from strings;

with date_strings as (
  select '2015-01-01' d from dual union all
  select '2000-12-31' d from dual union all
  select '900-01-01'  d from dual
)
  select d "STRING",
         regexp_replace(d, '([[:digit:]]+)-([[:digit:]]{2})-([[:digit:]]{2})', '\3.\2.\1') "MODIFIED_STRING"
  from date_strings;

select regexp_replace('500   opentenbase_ora     Parkway,    Redwood  Shores, CA', '( ){2,}', ' ') from dual;
select length(regexp_replace('500   opentenbase_ora     Parkway,    Redwood  Shores, CA', '( ){2,}', ' ')) from dual;

drop table if exists t_regexp_replace;
create table t_regexp_replace(f1 int, f2 varchar2, f3 varchar2, f4 varchar2);
insert into t_regexp_replace values(1, 'Hello  World',       ' {2,}', ' ');
insert into t_regexp_replace values(2, 'Hello        World', ' {2,}', ' ');
insert into t_regexp_replace values(3, 'Hello,   World  !',  ' {2,}', ' ');
insert into t_regexp_replace values(4, '2015-01-01', '([[:digit:]]+)-([[:digit:]]{2})-([[:digit:]]{2})', '\3.\2.\1');
insert into t_regexp_replace values(5, '2000-12-31', '([[:digit:]]+)-([[:digit:]]{2})-([[:digit:]]{2})', '\3.\2.\1');
insert into t_regexp_replace values(6, '900-01-01',  '([[:digit:]]+)-([[:digit:]]{2})-([[:digit:]]{2})', '\3.\2.\1');
insert into t_regexp_replace values(7, '500   opentenbase_ora     Parkway,    Redwood  Shores, CA', '( ){2,}', ' ');

select f1, f2, f3, f4, regexp_replace(f2, f3, f4), length(regexp_replace(f2, f3, f4)) from t_regexp_replace order by f1;

drop table t_regexp_replace;

with strings as (
  select 'NEW YORK' s from dual union all
  select 'New York' s from dual union all
  select 'new york' s from dual
)
  select s "STRING",
        regexp_replace(s, '[a-z]', '1', 1, 0, 'i') "CASE_INSENSITIVE",
        regexp_replace(s, '[a-z]', '1', 1, 0, 'c') "CASE_SENSITIVE",
        regexp_replace(s, '[a-zA-Z]', '1', 1, 0, 'c') "CASE_SENSITIVE_MATCHING"
  from  strings;


drop table if exists t_regexp_replace;
create table t_regexp_replace(f1 int, f2 varchar2, f3 varchar2, f4 varchar2);
insert into t_regexp_replace values(1, 'NEW YORK', '[a-z]', '1');
insert into t_regexp_replace values(2, 'New York', '[a-z]', '1');
insert into t_regexp_replace values(3, 'new york', '[a-z]', '1');

select f1, f2, f3, f4, regexp_replace(f2, f3, f4, 1, 0, 'i'), regexp_replace(f2, f3, f4, 1, 0, 'c'), regexp_replace(f2, '[a-zA-Z]', '1', 1, 0, 'c') from t_regexp_replace order by f1;

drop table t_regexp_replace;

--------------------------
-- Test trim function
--------------------------
\c regression
-- usage1
SELECT trim('    aaa  bbb  ccc     ') ltrim FROM dual;
SELECT ltrim('    aaa  bbb  ccc     ') ltrim FROM dual;
SELECT rtrim('    aaa  bbb  ccc     ') ltrim FROM dual;
-- usage2
SELECT trim(leading 'd' from 'dfssa') FROM dual;
SELECT trim(both '1' from '123sfd111') FROM dual;
SELECT trim(trailing '2' from '213dsq12') FROM dual;
-- usage3
SELECT trim('    aaa  bbb  ccc     ') ltrim FROM dual;
SELECT ltrim('    aaa  bbb  ccc     ') ltrim FROM dual;
SELECT rtrim('    aaa  bbb  ccc     ') ltrim FROM dual;
select ltrim('10900111000991110224323','109') from dual;
SELECT rtrim('aaaaminb','main') FROM dual;
SELECT rtrim('aaaaminb','mainb') FROM dual;
SELECT ltrim('ccbcminb','cb') FROM dual;
SELECT rtrim('abcdssscb','adscb') FROM dual;
SELECT rtrim('abcdssscb','badsc') FROM dual;
-- usage4
SELECT trim(leading 'df' from 'dfssa') FROM dual;
SELECT trim(both '13' from '123sfd111') FROM dual;
SELECT trim(trailing '23' from '213dsq12') FROM dual;
\c regression_ora
-- usage5
SELECT trim(leading 'df' from 'dfssa') FROM dual;
SELECT trim(both '13' from '123sfd111') FROM dual;
SELECT trim(trailing '23' from '213dsq12') FROM dual;
-- trim(' ') is null
select 'abcd' col_name from dual where trim(' ') is null;
select nvl(trim(both '' from ' '),'-1') from dual;

-- Trim parameter length detection, suitable for different codes
select trim(both '啊' from '啊中国啊') "test_trim" from dual;
select trim(leading '啊' from '啊中国啊') "test_trim" from dual;
select trim(trailing '啊' from '啊中国啊') "test_trim" from dual;
select trim(both '报错' from '啊中国啊') "test_trim" from dual;
select trim(leading '报错' from '啊中国啊') "test_trim" from dual;
select trim(trailing '报错' from '啊中国啊') "test_trim" from dual;
SELECT trim(leading 'df' from 'dfssa') FROM dual;
SELECT trim(both '13' from '123sfd111') FROM dual;
SELECT trim(trailing '23' from '213dsq12') FROM dual;

-- test INSERB
select INSTRB('abc', 'bc')||INSTRB('abc', '')||'avc' from dual;
select 1||INSTRB('abc', '')||'avc' from dual;
select INSTRB('abc', 'bc')||INSTRB('abc', '')||INSTRB('abc', 'bc') from dual;
SELECT nullif(trim(SUBSTR(lpad('abcde',INSTRB('32.8,63.5',',', 1, 1), '2'), 2)) , 'bcde') FROM DUAL;
select nullif(INSTRB('TechOnTheNet', 'On','0001E1', '00001'),0) from dual;
SELECT nullif(INSTRB('32.8,63.5',',', 1, 1), 2+2+1)  FROM dual;
select INSTRB('Tech on the net', 'e') = 2;
select INSTRB('Tech on the net', 'e', 1, 1) = 2;
select INSTRB('Tech on the net', 'e', 1, 2) = 11;
select INSTRB('Tech on the net', 'e', 1, 3) = 14;
select INSTRB('Tech on the net', 'e', -3, 2) = 2;
select INSTRB('abc', NULL) IS NULL;
select INSTRB('abc', '') IS NULL;
select INSTRB('', 'a') IS NULL;
select 1 = INSTRB('abc', 'a');
select 3 = INSTRB('abc', 'c');
select 0 = INSTRB('abc', 'z');
select 1 = INSTRB('abcabcabc', 'abca', 1);
select 4 = INSTRB('abcabcabc', 'abca', 2);
select 0 = INSTRB('abcabcabc', 'abca', 7);
select 0 = INSTRB('abcabcabc', 'abca', 9);
select 4 = INSTRB('abcabcabc', 'abca', -1);
select 1 = INSTRB('abcabcabc', 'abca', -8);
select 1 = INSTRB('abcabcabc', 'abca', -9);
select 0 = INSTRB('abcabcabc', 'abca', -10);
select 1 = INSTRB('abcabcabc', 'abca', 1, 1);
select 4 = INSTRB('abcabcabc', 'abca', 1, 2);
select 0 = INSTRB('abcabcabc', 'abca', 1, 3);
select 2 = INSTRB('张三','三') from dual;
select 0 = INSTRB('张三',' ') from dual;
select INSTRB('张三','') is NULL from dual;
select INSTRB('','三') is NULL from dual;
select INSTRB('Tech on the net', 'e', '1', '1') = 2;
select INSTRB('Tech on the net', 'e', 1.12, 2.65) = 11;
select INSTRB('Tech on the net', 'e', 1.12, 3.12) = 14;
select INSTRB('Tech on the net', 'e', '1', '4a') from dual;
SELECT ltrim(trim(both 'l' from lpad(SUBSTR('32.8,63.5',1,INSTRB('32.8,63.5',',', 1, 1)+1), 123, 'lpad')), 'lpad') trim FROM DUAL;

-------------------------------------------------------------------------------
--  FIX 【【5.21.6】【分布式流水线】【SQL】流水线chr（556）不符合预期】
--  https://tapd.woa.com/OpenTenBase_C/bugtrace/bugs/view?bug_id=1020385652118469715.
--------------------------------------------------------------------------------
\c regression_ora
select ascii(chr(replace(rtrim(trim(' ' from instr(INITCAP(lpad(concat('010-','88888888')||'转23'||chr(789), 100, '||')||rpad(chr(434)||'中国', 10, avg(123))||chr(556)), '转',  trunc(2.5), 1))), '',''))) replace from dual;

select dump(chr(0), 16) from dual;
select chr(-1) from dual;
select chr(0.1) from dual;
select substrb('hello', -5.5), substrb('hello', -5), substrb('hello', 0), substrb('hello', 1), substrb('hello', 5), substrb('hello', 5.5) from dual;
select dump(substrb('hello', -1234567890987654321), 16), dump(substrb('hello', -6), 16), dump(substrb('hello', 6), 16), dump(substrb('hello', 1234567890987654321), 16) from dual;
select dump(substrb('hello', 1, -1234567890987654321), 16), dump(substrb('hello', 1, -0.1), 16), dump(substrb('hello', 1, 0), 16), dump(substrb('hello', 1, 0.1), 16) from dual;
select substrb('hello', 1, 1.1), substrb('hello', 1, 1234567890987654321) from dual;

-------------------------------------------------------------------------------
-- 【【OpenTenBase内核】【5.21.6.1】substr 函数在某些场景下结果与 opentenbase_ora 不一致】
-- https://tapd.woa.com/OpenTenBase_C/bugtrace/bugs/view?bug_id=1020385652120676821
-------------------------------------------------------------------------------
select 1 from dual where substr('123', 4) is null;
select 1 from dual where substr('123', 1, 0) is null;
select 1 from dual where substr('123', 1, -1) is null;

-------------------------------------------------------------------------------
--【【memcheck】【heap-buffer-overflow】【td_regress】pg_utf_mblen orcl_lpad】
-- https://tapd.woa.com/OpenTenBase_C/bugtrace/bugs/view?bug_id=1020385652122033179
-------------------------------------------------------------------------------
select lpad(chr(12540), 20, chr(4822)) from dual;

-------------------------------------------------------------------------------
--【CI】【5.21.7.0 sqlsmith】core函数: shortest
-- https://tapd.woa.com/20385652/bugtrace/bugs/view/1020385652122498845
-- core by regexp_substr('test', 'test', -1733464470)
-------------------------------------------------------------------------------
select regexp_substr('test', 'test', -1733464470) from dual;
select regexp_substr('test', 'test', 2561502826) from dual;

\c postgres
select regexp_substr('test', 'test', -1733464470);
