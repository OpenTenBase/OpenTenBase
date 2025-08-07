-- check that CTAS inserts rows into correct table
CREATE TABLE ctas_t1 AS SELECT 1;
CREATE TEMP TABLE ctas_t1 AS SELECT 2;
SELECT * FROM ctas_t1;
SELECT * FROM public.ctas_t1;
SELECT * FROM pg_temp.ctas_t1;
-- this drops the temp table
DROP TABLE ctas_t1;
-- also drop the public table
DROP TABLE ctas_t1;

-- same tests with special table name
CREATE TABLE "ctas t1" AS SELECT 1;
CREATE TEMP TABLE "ctas t1" AS SELECT 2;
SELECT * FROM "ctas t1";
SELECT * FROM public."ctas t1";
SELECT * FROM pg_temp."ctas t1";
DROP TABLE "ctas t1";
DROP TABLE "ctas t1";

CREATE SCHEMA "ctas schema";
SET search_path TO "ctas schema";
CREATE TABLE "ctas t1" AS SELECT 1;
CREATE TEMP TABLE "ctas t1" AS SELECT 2;
SELECT * FROM "ctas t1";
SELECT * FROM public."ctas t1";
SELECT * FROM "ctas schema"."ctas t1";
SELECT * FROM pg_temp."ctas t1";
DROP TABLE "ctas t1";
DROP TABLE "ctas t1";

-- check CTAS in a function
CREATE TABLE ctas_t1 (f1 int, f2 timestamp without time zone);
INSERT INTO ctas_t1 values(1, '2021-06-02');
CREATE OR REPLACE FUNCTION ctas_p(arg varchar(10)) RETURNS void AS
$$
begin
    raise notice '%',arg;
    CREATE TABLE ctas_t2 AS SELECT * FROM ctas_t1 WHERE f2 > arg::date;
end;
$$
language plpgsql;
SELECT ctas_p('2021-06-01');
SELECT * FROM ctas_t2;
DROP TABLE ctas_t2;
DROP TABLE ctas_t1;

-- test create table column table as... when ISOLATION LEVEL REPEATABLE READ
create table col_test_trans_par_range_20230104_1(names varchar default 'JAMES', age smallint default 18, school varchar default '北京大学', city char(10) default '深圳', address text default '南山', job varchar default '程序员', sal numeric(7,2) default -1, comm  numeric(7,2) default -99, DEPTNO numeric(2) default -1, HIREDATE DATE default '2022-08-06', runtime interval default '1 hour', sleeptime time default '23:59:59', bigobject varchar(8000) default '大对象存储',id int default 10) partition by range(HIREDATE) distribute by shard(id);
create index on col_test_trans_par_range_20230104_1(HIREDATE);

create table col_test_trans_par_range_20230104_1_part_1 partition of col_test_trans_par_range_20230104_1 for values from (minvalue) to ('2023-01-06 17:00:00');
create table col_test_trans_par_range_20230104_1_part_2 partition of col_test_trans_par_range_20230104_1 for values from ('2023-01-06 17:00:00') to ('2023-08-06 17:00:00');
create table col_test_trans_par_range_20230104_1_part_3 partition of col_test_trans_par_range_20230104_1 for values from ('2023-08-06 17:00:00') to ('2024-04-06 17:00:00') ;
create table col_test_trans_par_range_20230104_1_part_4 partition of col_test_trans_par_range_20230104_1 for values from ('2024-04-06 17:00:00') to ('2024-11-06 17:00:00');
create table col_test_trans_par_range_20230104_1_part_5 partition of col_test_trans_par_range_20230104_1 default;


drop table if exists col_test_trans_par_range_20230104_1 cascade;

CREATE TABLE rp(a int);
START TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	create table concurrence_1 as select * from rp;
END;
DROP TABLE rp;
