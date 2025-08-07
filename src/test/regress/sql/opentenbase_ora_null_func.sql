\c regression_ora
-- Test lnnvl
SELECT CASE WHEN LNNVL(1=0) THEN 0 ELSE 1 END "1=0"
     , CASE WHEN LNNVL(1=1) THEN 0 ELSE 1 END "1=1"
     , LNNVL(1=0) "1=0"
     , LNNVL(1=1) "1=0"
FROM DUAL;

-- Test nanvl
SELECT NANVL(binary_double_nan, 0), NANVL(binary_float_nan, 0) FROM DUAL;
SELECT NANVL(0/0, 1) FROM DUAL;

DROP TABLE IF EXISTS float_point_demo;
CREATE TABLE float_point_demo (id int, dec_num NUMBER(10, 2), bin_double BINARY_DOUBLE, bin_float BINARY_FLOAT);
INSERT INTO float_point_demo VALUES (1, 1234.56, 1234.56, 1234.56);
INSERT INTO float_point_demo VALUES (2, 0, 'NaN', 'NaN');
SELECT bin_float, NANVL(bin_float, 0) FROM float_point_demo ORDER BY id;
SELECT bin_float, NANVL(bin_float, '1111') FROM float_point_demo ORDER BY id;
SELECT bin_double, NANVL(bin_double, '0') FROM float_point_demo ORDER BY id;
SELECT bin_double, NANVL(bin_double, 0) FROM float_point_demo ORDER BY id;
DROP TABLE float_point_demo;
-- begin 22123691
select nanvl('0', '-1') from dual;
select nanvl('-0', '-1') from dual;
select nanvl('-0', '-0') from dual;
select nanvl('-0', '0') from dual;
select nanvl('-0', '+0') from dual;
select nanvl('-0', '1') from dual;
select nanvl('0.0', '1') from dual;
select nanvl('-0.0', '1') from dual;
select nanvl('0.00', '1') from dual;
select nanvl('-0.00', '1') from dual;
select nanvl('-0.00000001', '1') from dual;

select atan2('0', '-1') from dual;
select atan2('-0', '-1') from dual;
select atan2('0.0', '-1') from dual;
select atan2('-0.0', '-1') from dual;
select atan2('0.00000', '-1') from dual;
select atan2('-0.00000', '-1') from dual;

select lnnvl(1 > 2) from dual;
select lnnvl(null) from dual;			-- failed
select lnnvl('') from dual;			-- failed
select 1 from dual where lnnvl(null);		-- failed
select 1 from dual where lnnvl('');		-- failed
select case when lnnvl(null) then 1 else 2 end as lnnvl from dual;	-- failed
select case when lnnvl('') then 1 else 2 end as lnnvl from dual;	-- failed
select lnnvl('1' || 'a') from dual; -- failed;
select lnnvl('1' + 2) from dual; -- failed;
select lnnvl(1 + 2) from dual; -- failed;

select coalesce(null, 2) from dual;
select coalesce('', '2') from dual; 
select coalesce('', null, '2') from dual;
select coalesce('', 2) from dual;		-- failed
select coalesce(null, '', 2) from dual; -- failed
select coalesce('', '2', 2) from dual;	-- failed
select coalesce('', null, 2) from dual;	-- failed

-- end 22123691


-- Test nvl
SELECT NVL('', '123') FROM DUAL;
SELECT NVL(NULL, '123') FROM DUAL;
SELECT NVL('not null', '123') FROM DUAL;

SELECT NVL(123.456, '123') FROM DUAL;

-- Test nvl2
-- SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';
SELECT NVL2(sysdate, '2022-07-22', '2022-08-22') FROM DUAL;
SELECT NVL2(null, '2022-07-22', '2022-08-22') FROM DUAL;
SELECT NVL2(systimestamp, '2022-07-22 13:22:33.666 +8:00', '2022-08-22 13:22:33.666 +8:00') FROM DUAL;
SELECT NVL2(null, '2022-07-22 13:22:33.666 +8:00', '2022-08-22 13:22:33.666 +8:00') FROM DUAL;

drop table if exists employees cascade;
create table employees(last_name varchar2(100), salary int, commission_pct number);
insert into employees values('Baer', 12223, 2.5);
insert into employees values('Baida', 4523, 0.5);
insert into employees values('Banda', 10000, null);
insert into employees values('Bates', 23516, 1.5);
insert into employees values('Bell', 12350, 0.5);
insert into employees values('Bernstein', 45555, '');
insert into employees values('Bissot', 2451, 1.8);
insert into employees values('Bloom', 7095, 1);
insert into employees values('Bull', 8051, 2);
SELECT last_name, salary,commission_pct,
       NVL2(commission_pct, salary + (salary * commission_pct), salary) income
  FROM employees
  WHERE last_name like 'B%'
  ORDER BY last_name;
drop table if exists employees cascade;

-- Test lnnvl demo
DROP TABLE IF EXISTS tbl_lnnvl_demo;
CREATE TABLE tbl_lnnvl_demo(a int, b int);
INSERT INTO tbl_lnnvl_demo VALUES (2);
SELECT * FROM tbl_lnnvl_demo;

SELECT * FROM tbl_lnnvl_demo WHERE lnnvl(a = 1);
SELECT * FROM tbl_lnnvl_demo WHERE lnnvl(a = 2);
SELECT * FROM tbl_lnnvl_demo WHERE lnnvl(a IS NULL);
SELECT * FROM tbl_lnnvl_demo WHERE lnnvl(b = 1);
SELECT * FROM tbl_lnnvl_demo WHERE lnnvl(b IS NULL);
SELECT * FROM tbl_lnnvl_demo WHERE lnnvl(a = b);

SELECT a = 1 AS condition, lnnvl(a = 1) FROM tbl_lnnvl_demo;
SELECT a = 2 AS condition, lnnvl(a = 2) FROM tbl_lnnvl_demo;
SELECT a IS NULL AS condition, lnnvl(a IS NULL) FROM tbl_lnnvl_demo;
SELECT b = 1 AS condition, lnnvl(b = 1) FROM tbl_lnnvl_demo;
SELECT b IS NULL AS condition, lnnvl(b IS NULL) FROM tbl_lnnvl_demo;
SELECT a = b AS condition, lnnvl(a = b) FROM tbl_lnnvl_demo;
DROP TABLE tbl_lnnvl_demo;

-- Test nanvl demo
DROP TABLE IF EXISTS tbl_nanvl_demo;
CREATE TABLE tbl_nanvl_demo(a int, b float4, c float8);
INSERT INTO tbl_nanvl_demo VALUES (1, 'NaN', 'NaN');
SELECT * FROM tbl_nanvl_demo;
SELECT a, b, nanvl(b, a) FROM tbl_nanvl_demo;
SELECT a, c, nanvl(c, a) FROM tbl_nanvl_demo;
DROP TABLE tbl_nanvl_demo;

-- Test nvl demo
DROP TABLE IF EXISTS tbl_nvl_demo;
CREATE TABLE tbl_nvl_demo(id int, num numeric, str varchar2);
INSERT INTO tbl_nvl_demo VALUES (1, 100, NULL);
INSERT INTO tbl_nvl_demo VALUES (2, NULL, 'str is not NULL');
SELECT * FROM tbl_nvl_demo ORDER BY id;
SELECT num, nvl(num, 0) FROM tbl_nvl_demo ORDER BY id;
SELECT str, nvl(str, 'str is NULL') FROM tbl_nvl_demo ORDER BY id;
DROP TABLE tbl_nvl_demo;

-- Test nvl2 demo
DROP TABLE IF EXISTS tbl_nvl2_demo;
CREATE TABLE tbl_nvl2_demo(id int, num1 numeric, num2 numeric, num3 numeric, str1 varchar2, str2 varchar2, str3 varchar2);
INSERT INTO tbl_nvl2_demo VALUES (1, 101, 102, 103, 'str1 is not NULL', 'str1 is not NULL, this is str2', 'str1 is not NULL, this is str3');
INSERT INTO tbl_nvl2_demo VALUES (2, NULL, 201, 203, NULL, 'str1 is NULL, this is str2', 'str1 is NULL, this is str3');
SELECT * FROM tbl_nvl2_demo ORDER BY id;
SELECT num1, num2, num3, nvl2(num1, num2, num3) FROM tbl_nvl2_demo ORDER BY id;
SELECT str1, str2, str3, nvl2(str1, str2, str3) FROM tbl_nvl2_demo ORDER BY id;
DROP TABLE tbl_nvl2_demo;

-- 1020421696880130617
-- Test nvl with diff length type
DROP TABLE IF EXISTS tbl_test_nvl_with_diff_type;
CREATE TABLE tbl_test_nvl_with_diff_type(f1 int, f2 char(1), f3 nchar(1), f4 varchar(1),
                                f5 varchar2(1), f6 nvarchar2(1), f7 bpchar(1),
                                f8 text, f9 clob, f10 name,
                                f11 pg_dependencies, f12 pg_ndistinct, f13 pg_node_tree,
                                f14 information_schema.sql_identifier,
                                f15 information_schema.character_data,
                                f16 information_schema.yes_or_no);
INSERT INTO tbl_test_nvl_with_diff_type VALUES (1);
SELECT nvl(f1, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f2, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f3, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f4, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f5, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f6, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f7, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f8, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f9, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f10, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f11, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f12, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f13, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f14, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f15, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
SELECT nvl(f16, 111111111111111111111111111111111111) FROM tbl_test_nvl_with_diff_type;
DROP TABLE tbl_test_nvl_with_diff_type;
