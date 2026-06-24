------------------------------------------------------------
-- A part of opentenbase_ora.sql, test null functions related cases.
------------------------------------------------------------
\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;

\c regression_ora

-- Test nanvl
SELECT nanvl(12345, 1), nanvl('NaN', 1) FROM DUAL;
SELECT nanvl(12345::float4, 1), nanvl('NaN'::float4, 1) FROM DUAL;
SELECT nanvl(12345::float8, 1), nanvl('NaN'::float8, 1) FROM DUAL;
SELECT nanvl(12345::numeric, 1), nanvl('NaN'::numeric, 1) FROM DUAL;
SELECT nanvl(12345, '1'::varchar), nanvl('NaN', 1::varchar) FROM DUAL;
SELECT nanvl(12345::float4, '1'::varchar), nanvl('NaN'::float4, '1'::varchar) FROM DUAL;
SELECT nanvl(12345::float8, '1'::varchar), nanvl('NaN'::float8, '1'::varchar) FROM DUAL;
SELECT nanvl(12345::numeric, '1'::varchar), nanvl('NaN'::numeric, '1'::varchar) FROM DUAL;
SELECT nanvl(12345, '1'::char), nanvl('NaN', 1::char) FROM DUAL;
SELECT nanvl(12345::float4, '1'::char), nanvl('NaN'::float4, '1'::char) FROM DUAL;
SELECT nanvl(12345::float8, '1'::char), nanvl('NaN'::float8, '1'::char) FROM DUAL;
SELECT nanvl(12345::numeric, '1'::char), nanvl('NaN'::numeric, '1'::char) FROM DUAL;

-- Test nvl
SELECT nvl('A'::text, 'B') FROM DUAL;
SELECT nvl(NULL::text, 'B') FROM DUAL;
SELECT nvl(NULL::text, NULL) FROM DUAL;
SELECT nvl(1, 2) FROM DUAL;
SELECT nvl(NULL, 2) FROM DUAL;

-- Test nvl2
SELECT nvl2('A'::text, 'B', 'C') FROM DUAL;
SELECT nvl2(NULL::text, 'B', 'C') FROM DUAL;
SELECT nvl2('A'::text, NULL, 'C') FROM DUAL;
SELECT nvl2(NULL::text, 'B', NULL) FROM DUAL;
SELECT nvl2(1, 2, 3) FROM DUAL;
SELECT nvl2(NULL, 2, 3) FROM DUAL;
SELECT nvl2(' ', 1, 2) FROM DUAL;
SELECT nvl2('', 1, 2) FROM DUAL;

-- Test lnnvl
SELECT lnnvl(true) FROM DUAL;
SELECT lnnvl(false) FROM DUAL;
SELECT lnnvl(NULL) FROM DUAL;

-- Test NULL
SELECT dump('') FROM DUAL;
SELECT nvl('',3) FROM DUAL;
SELECT nvl2('','1','2') FROM DUAL ;
SELECT 1 FROM DUAL WHERE '' is NULL;
SELECT NULL||'opentenbase' FROM DUAL;
SELECT NULL+1 FROM DUAL;

DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null(f1 int,f2 varchar2(10));
INSERT INTO t_null VALUES(1,NULL);
INSERT INTO t_null VALUES(2,'');
SELECT * FROM t_null WHERE f2 is NULL;
SELECT * FROM t_null WHERE f2 ='' ;

INSERT INTO t_null VALUES(3,'1');
INSERT INTO t_null VALUES(4,'2');
SELECT * FROM t_null ORDER BY f2;
DROP TABLE t_null;

-- TODO: enable this case after opentenbase_ora datetime functions are merged
-- 1020421696880130617
-- Test nvl with diff length type
-- DROP TABLE IF EXISTS t_fix_nvl_ret_cut_20221201;
-- CREATE TABLE t_fix_nvl_ret_cut_20221201(id int,name varchar(6),name2 timestamp);
-- INSERT INTO t_fix_nvl_ret_cut_20221201 VALUES (1, '202210', 'Sun Dec 04 23:48:56.654772 2022');
-- INSERT INTO t_fix_nvl_ret_cut_20221201(id, name2) VALUES (2, 'Sun Dec 04 23:48:56.656310 2022');
-- INSERT INTO t_fix_nvl_ret_cut_20221201(id, name2) VALUES (3, 'Sun Dec 04 23:48:56.657340 2022');
-- INSERT INTO t_fix_nvl_ret_cut_20221201 VALUES (4, '202012', 'Sun Dec 04 23:48:56.658634 2022');
-- SELECT * FROM t_fix_nvl_ret_cut_20221201 ORDER BY id;
-- SELECT nvl(name, to_char(name2, 'yyyy:mm:dd hh24:mi:ss')) FROM t_fix_nvl_ret_cut_20221201 ORDER BY id;
-- DROP TABLE t_fix_nvl_ret_cut_20221201;

DROP TABLE IF EXISTS t_test_nvl_with_diff_type;
CREATE TABLE t_test_nvl_with_diff_type(f1 int, f2 char(1), f3 nchar(1), f4 varchar(1),
                                f5 varchar2(1), f6 nvarchar2(1), f7 bpchar(1),
                                f8 text, f9 clob, f10 name,
                                f11 pg_dependencies, f12 pg_ndistinct, f13 pg_node_tree,
                                f14 information_schema.sql_identifier,
                                f15 information_schema.character_data,
                                f16 information_schema.yes_or_no);
INSERT INTO t_test_nvl_with_diff_type VALUES (1);
SELECT nvl(f1, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f2, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f3, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f4, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f5, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f6, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f7, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f8, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f9, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f10, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f11, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f12, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f13, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f14, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f15, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
SELECT nvl(f16, 111111111111111111111111111111111111) FROM t_test_nvl_with_diff_type;
DROP TABLE t_test_nvl_with_diff_type;
-- Fix bugs of NVL for numeric type
drop table if exists ttest_20230609;
create table ttest_20230609(c1 number(4,3),c2 number(5,2), c3 number(5,3), c4 number(5,4), c5 number(22,9));
insert into ttest_20230609 values(1.999, 100.99, 10.999,1.9999, '');

-- number(a,b) number(c,d)
--  a>c, b<d
select nvl(c2, c1) from ttest_20230609;
--  a>c, b>d
select nvl(c4, c1) from ttest_20230609;

--  a<c, b>d
select nvl(c1, c2) from ttest_20230609;
--  a<c, b<d
select nvl(c1, c4) from ttest_20230609;

--  a=c, b<d
select nvl(c2, c3) from ttest_20230609;
--  a=c, b>d
select nvl(c3, c2) from ttest_20230609;

--  a<c, b=d
select nvl(c1, c3) from ttest_20230609;
--  a>c, b=d
select nvl(c3, c1) from ttest_20230609;

--  a=c, b=d
select nvl(c1, c1) from ttest_20230609;

-- null and number
select nvl(c1, c5) from ttest_20230609;
select nvl(c5, c1) from ttest_20230609;

select nvl(c2, c5) from ttest_20230609;
select nvl(c5, c2) from ttest_20230609;

select nvl(c3, c5) from ttest_20230609;
select nvl(c5, c3) from ttest_20230609;

select nvl(c4, c5) from ttest_20230609;
select nvl(c5, c4) from ttest_20230609;

-- const
select nvl(1.23456789012345678::float4, c1) from ttest_20230609;
select nvl(1.23456789012345678::float8, c1) from ttest_20230609;

select nvl(c1, 1.23456789012345678::float4) from ttest_20230609;
select nvl(c2, 1.23456789012345678::float8) from ttest_20230609;

select nvl(1.23456789012345678::number, c1) from ttest_20230609;
select nvl(1.23456789012345678::number, c1) from ttest_20230609;

select nvl(c1, 1.23456789012345678::number) from ttest_20230609;
select nvl(c2, 1.23456789012345678::number) from ttest_20230609;

select nvl(1.1999::number(5,4), 1.99::number(10,2));

-- func expr
create or replace function ftest_20230612(a int) return number
as
r1 number(3,2);
r2 number(6,5);
r3 number(9,8);
begin
    r1 = 1.99;
    r2 = 1.99999;
    r3 = 1.99999999;
    if a = 1::int then
        return r1;
    elseif a = 2::int then
        return r2;
    else
        return r3;
    end if;
end;
/

select nvl(ftest_20230612(1), 1.9::number(2,1)) from dual;
select nvl(ftest_20230612(1), 1.9::number(10,1)) from dual;

select nvl(ftest_20230612(2), 1.999::number(4,3)) from dual;
select nvl(ftest_20230612(2), 1.9999::number(10,4)) from dual;

select nvl(ftest_20230612(3), 1.9::number(2,1)) from dual;
select nvl(ftest_20230612(3), 1.999999::number(20,6)) from dual;

select nvl(ftest_20230612(1), ftest_20230612(2)) from dual;
select nvl(ftest_20230612(2), ftest_20230612(1)) from dual;

select nvl(ftest_20230612(1), ftest_20230612(3)) from dual;
select nvl(ftest_20230612(3), ftest_20230612(1)) from dual;

select nvl(ftest_20230612(3), ftest_20230612(2)) from dual;
select nvl(ftest_20230612(2), ftest_20230612(3)) from dual;

-- case when
prepare s as
select nvl(
    case when $1 = 1 then 1.999::number(4,3)
         when $1 = 2 then 100.99::number(5,2)
         when $1 = 3 then 10.999::number(5,3)
         when $1 = 4 then 1.9999::number(5,4)
         else null::number(9,3) end,
    case when $2 = 1 then 1.999::number(4,3)
         when $2 = 2 then 100.99::number(5,2)
         when $2 = 3 then 10.999::number(5,3)
         when $2 = 4 then 1.9999::number(5,4)
         else null::number(9,2) end) from dual;

execute s(1,2);
execute s(1,3);
execute s(1,4);
execute s(1,5);

execute s(2,1);
execute s(2,3);
execute s(2,4);
execute s(2,5);

execute s(3,1);
execute s(3,2);
execute s(3,4);
execute s(3,5);

execute s(4,1);
execute s(4,2);
execute s(4,3);
execute s(4,5);

execute s(5,1);
execute s(5,2);
execute s(5,3);
execute s(5,4);

deallocate s;

-- decode
create or replace function ftest_20230612_1(a int) return int
as
begin
    return a;
end;
/

prepare s as
select nvl(decode(ftest_20230612_1($1::int), 1, 1.999::number(4,3),
                        2, 100.99::number(5,2),
                        3, 10.999::number(5,3),
                        4, 1.9999::number(5,4),
                        null::number(10,2)),
           decode(ftest_20230612_1($2::int), 1, 1.999::number(4,3),
                        2, 100.99::number(5,2),
                        3, 10.999::number(5,3),
                        4, 1.9999::number(5,4),
                        null::number(10,2))) from dual;

execute s(1,2);
execute s(1,3);
execute s(1,4);
execute s(1,5);

execute s(2,1);
execute s(2,3);
execute s(2,4);
execute s(2,5);

execute s(3,1);
execute s(3,2);
execute s(3,4);
execute s(3,5);

execute s(4,1);
execute s(4,2);
execute s(4,3);
execute s(4,5);

execute s(5,1);
execute s(5,2);
execute s(5,3);
execute s(5,4);

deallocate s;

-- subquery
select nvl((select 1.999::number(4,3)), (select 100.99::number(5,2))) from dual;
select nvl((select 10.999::number(5,3)), (select 100.99::number(5,2))) from dual;
select nvl((select 1.9999::number(5,4)), (select 10.999::number(5,3))) from dual;

select nvl((select cast (1.9999 as number(5,4)) from dual), (select cast (10.999 as number(5,3)) from dual)) from dual;

drop function ftest_20230612;
drop function ftest_20230612_1;
drop table ttest_20230609;
