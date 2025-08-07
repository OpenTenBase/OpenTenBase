-- test store and select non case insensitive opentenbase_ora identifier with double quotation marks
\c regression_ora
-- create, alter, drop database
CREATE DATABASE "DB_UPPER";
ALTER DATABASE DB_UPPER RENAME TO "D_UPPER";
DROP DATABASE D_UPPER;
-- create, alter, drop schema
CREATE SCHEMA "SCHEMA_UPPER";
ALTER SCHEMA SCHEMA_UPPER RENAME TO "S_UPPER";
DROP SCHEMA S_UPPER;
-- create, insert, delete, update, select, alter, drop table in opentenbase_ora way
CREATE TABLE "T_UPPER"("COL1_UPPER" INT, COL2_UPPER TEXT);
INSERT INTO T_UPPER VALUES(1, 'Jack'),(2, 'Tom'),(3, 'Jim');
DELETE FROM "T_UPPER" WHERE COL1_UPPER = 1;
SELECT "COL1_UPPER", COL2_UPPER FROM T_UPPER;
UPDATE T_UPPER SET COL1_UPPER="COL1_UPPER" + 1;
SELECT "COL1_UPPER", COL2_UPPER FROM "T_UPPER" ORDER BY COL1_UPPER;
SELECT "COL1_UPPER", COL2_UPPER FROM "T_UPPER" ORDER BY "COL1_UPPER", "COL2_UPPER";
ALTER TABLE "T_UPPER" ADD COLUMN "COL3_UPPER" INT;
ALTER TABLE T_UPPER ADD COLUMN COL4_UPPER INT;
DROP TABLE T_UPPER;
-- create, alter, drop index in opentenbase_ora way
CREATE TABLE T_UPPER(COL1_UPPER INT, COL2_UPPER TEXT);
CREATE INDEX "INDEX_UPPER" ON T_UPPER(COL1_UPPER);
ALTER INDEX "INDEX_UPPER" RENAME TO "I_UPPER";
DROP INDEX "I_UPPER";
DROP TABLE T_UPPER;
-- create, select, drop view in opentenbase_ora way
CREATE TABLE T_UPPER(COL1_UPPER INT, COL2_UPPER TEXT);
INSERT INTO T_UPPER VALUES(1, 'Jack'),(2, 'Tom'),(3, 'Jim');
CREATE VIEW "VIEW_UPPER" AS SELECT COL1_UPPER, "COL2_UPPER" FROM T_UPPER;
SELECT COL1_UPPER, COL2_UPPER FROM VIEW_UPPER ORDER BY 1;
ALTER VIEW "VIEW_UPPER" RENAME TO "V_UPPER";
DROP VIEW V_UPPER;
DROP TABLE T_UPPER;
-- create, select, alter, drop function in opentenbase_ora way
CREATE OR REPLACE FUNCTION "FUNC_UPPER"(NAME TEXT)
RETURN TEXT
IS
DECLARE
RESULT TEXT:=NAME;
BEGIN
RETURN RESULT;
END;
/
SELECT FUNC_UPPER('FUNCTION');
SELECT "FUNC_UPPER"('FUNCTION');
ALTER FUNCTION FUNC_UPPER RENAME TO "F_UPPER";
CREATE OR REPLACE FUNCTION "OUTF_UPPER"(NAME TEXT)
RETURN TEXT
IS
DECLARE
RESULT TEXT;
BEGIN
SELECT F_UPPER(NAME) INTO RESULT;
RETURN RESULT;
END;
/
SELECT "OUTF_UPPER"('OUT FUNCTION');
DROP FUNCTION OUTF_UPPER;
DROP FUNCTION F_UPPER;
-- call some opentenbase_ora comptiale system function
SELECT "TO_CHAR"('1') from dual;
SELECT TO_CHAR('1') from dual;
SELECT "LENGTHB"('ABC') from dual;
SELECT LENGTHB('ABC') from dual;
-- create, call, alter, drop procedure in opentenbase_ora way
CREATE OR REPLACE PROCEDURE "PROC_UPPER"(NAME TEXT)
IS
BEGIN
raise notice '%', NAME;
END;
/
CALL PROC_UPPER('PROCEDURE');
CALL "PROC_UPPER"('PROCEDURE');
ALTER PROCEDURE "PROC_UPPER" RENAME TO "P_UPPER";
CREATE OR REPLACE PROCEDURE "OUTP_UPPER"(NAME TEXT)
IS
BEGIN
CALL P_UPPER(NAME);
END;
/
CALL OUTP_UPPER('OUT PROCEDURE');
DROP PROCEDURE OUTP_UPPER;
DROP PROCEDURE P_UPPER;
-- create, alter, use, drop domain
CREATE DOMAIN "DOM_UPPER" AS  VARCHAR NOT NULL CHECK (value !~ '\s');
ALTER DOMAIN DOM_UPPER RENAME TO "D_UPPER";
CREATE TABLE T_UPPER(ID INT, F_NAME D_UPPER, L_NAME "D_UPPER");
INSERT INTO T_UPPER VALUES(1, 'Zhang', 'San');
INSERT INTO T_UPPER VALUES(1, NULL, 'San');
INSERT INTO T_UPPER VALUES(1, 'Zhang', NULL);
DROP TABLE T_UPPER;
DROP DOMAIN D_UPPER;
-- create, alter, use, drop type
CREATE TYPE "TY_UPPER" AS  ("X" INT, "Y" INT);
CREATE TABLE T_UPPER(ID INT, L_HOME TY_UPPER, L_COMPANY "TY_UPPER");
INSERT INTO T_UPPER VALUES(1, (124, 53), (77, 89));
SELECT * FROM T_UPPER;
DROP TABLE T_UPPER;
DROP TYPE TY_UPPER;

-- create, alter, drop role
CREATE ROLE "USER_UPPER" WITH LOGIN;
ALTER ROLE USER_UPPER RENAME TO "U_UPPER";
DROP ROLE U_UPPER;
-- create, select, update, alter, drop sequence
CREATE SEQUENCE "S_UPPER";
SELECT NEXTVAL('S_UPPER');
SELECT CURRVAL('S_UPPER');
SELECT LASTVAL();
SELECT SETVAL('S_UPPER', 1);
DROP SEQUENCE S_UPPER;
-- create, alter, select, drop package
CREATE OR REPLACE PACKAGE "P_UPPER" IS
  PROCEDURE "PR_UPPER";
END "P_UPPER";
/
CREATE OR REPLACE PACKAGE BODY P_UPPER IS
  PROCEDURE "PR_UPPER" IS
  BEGIN
    RAISE NOTICE 'Call the procedure';
END;
END;
/
CALL P_UPPER.PR_UPPER();
DROP PACKAGE P_UPPER;
-- identifier contain lower or special character
CREATE TABLE "T_t"(ID INT);
DROP TABLE IF EXISTS "T_t";
CREATE TABLE "t_t"(ID INT);
DROP TABLE IF EXISTS "t_t";
CREATE TABLE "T没有大写"(ID INT);
DROP TABLE IF EXISTS "T没有大写";
CREATE TABLE "全部汉字"(ID INT);
DROP TABLE IF EXISTS "全部汉字";
-- ignore the COLLATE
CREATE DATABASE "D_UPPER" WITH TEMPLATE template0 ENCODING "UTF8" LC_COLLATE "C" LC_CTYPE "POSIX";
DROP DATABASE D_UPPER;
CREATE TABLE "T_UPPER"(NAME TEXT COLLATE "C", TITLE TEXT COLLATE "POSIX");
CREATE INDEX "I_UPPER" ON T_UPPER (NAME COLLATE "POSIX");
DROP TABLE T_UPPER;
SET client_min_messages TO 'WARNING'; 
CREATE or REPLACE PROCEDURE TEST_COLL_P() IS
  V1 TEXT COLLATE "C" := '严'; --变量带COLLATE
BEGIN
  CREATE TABLE "T_UPPER"(NAME TEXT COLLATE "C", TITLE TEXT COLLATE "POSIX");
  RAISE NOTICE '%',V1;
END;
/
CALL TEST_COLL_P();
DROP TABLE T_UPPER;
DROP PROCEDURE TEST_COLL_P;
RESET client_min_messages;
CREATE TYPE "TYP_UPPER" AS RANGE (subtype=text, collation="C");
DROP TYPE TYP_UPPER;
-- ignore the xml functions
CREATE TABLE "XMLT_UPPER"(id serial, data xml);
INSERT INTO XMLT_UPPER(data) VALUES('<ROWS>
<ROW id="1">
  <COUNTRY_ID>AU</COUNTRY_ID>
  <COUNTRY_NAME>Australia</COUNTRY_NAME>
  <REGION_ID>3</REGION_ID>
</ROW>
<ROW id="2">
  <COUNTRY_ID>CN</COUNTRY_ID>
  <COUNTRY_NAME>China</COUNTRY_NAME>
  <REGION_ID>3</REGION_ID>
</ROW>
<ROW id="3">
  <COUNTRY_ID>HK</COUNTRY_ID>
  <COUNTRY_NAME>HongKong</COUNTRY_NAME>
  <REGION_ID>3</REGION_ID>
</ROW>
<ROW id="4">
  <COUNTRY_ID>IN</COUNTRY_ID>
  <COUNTRY_NAME>India</COUNTRY_NAME>
  <REGION_ID>3</REGION_ID>
</ROW>
<ROW id="5">
  <COUNTRY_ID>JP</COUNTRY_ID>
  <COUNTRY_NAME>Japan</COUNTRY_NAME>
  <REGION_ID>3</REGION_ID><PREMIER_NAME>Sinzo Abe</PREMIER_NAME>
</ROW>
<ROW id="6">
  <COUNTRY_ID>SG</COUNTRY_ID>
  <COUNTRY_NAME>Singapore</COUNTRY_NAME>
  <REGION_ID>3</REGION_ID><SIZE unit="km">791</SIZE>
</ROW>
</ROWS>');
SELECT xmltable.* FROM XMLT_UPPER, LATERAL xmltable('/ROWS/ROW[COUNTRY_NAME="Japan" or COUNTRY_NAME="India"]' PASSING data COLUMNS "COUNTRY_NAME" text, "REGION_ID" int) order by id;
DROP TABLE XMLT_UPPER;
SELECT xmlelement(name "FOO");
SELECT xmlelement(name "FOO", xmlattributes('xyz' as "BAR"));
SELECT xmlforest('abc' AS "FOO", 123 AS "BAR");
SELECT xmlpi(name "PHP", 'echo "hello world";');
-- ignore the database link, the case is in opentenbase_sql
-- ouch, there are some side effects.
CREATE TABLE "T_UPPER"(ID INT);
SELECT count(*) FROM pg_class WHERE relname='T_UPPER';
CREATE TABLE t_upper(ID INT);
DROP TABLE t_upper;
-- truncate the too long identifier
create table t_upper_20231127202311272023112720231127202311272023112720231127(c_upper_20231127202311272023112720231127202311272023112720231127 int);
alter table t_upper_20231127202311272023112720231127202311272023112720231127 add constraint colum_upper_unique_20231127202311272023112720231127202311272023112720231127 unique(c_upper_20231127202311272023112720231127202311272023112720231127);
create index c_upper_index_20231127202311272023112720231127202311272023112720231127 on t_upper_20231127202311272023112720231127202311272023112720231127 (c_upper_20231127202311272023112720231127202311272023112720231127);
\d T_UPPER_2023112720231127202311272023112720231127202311272023112
select c_upper_20231127202311272023112720231127202311272023112720231127 from t_upper_20231127202311272023112720231127202311272023112720231127 group by c_upper_20231127202311272023112720231127202311272023112720231127 order by c_upper_20231127202311272023112720231127202311272023112720231127;
create view v_upper_20231127202311272023112720231127202311272023112720231127 as select c_upper_20231127202311272023112720231127202311272023112720231127 from t_upper_20231127202311272023112720231127202311272023112720231127;
select c_upper_20231127202311272023112720231127202311272023112720231127 from v_upper_20231127202311272023112720231127202311272023112720231127;
\d V_UPPER_2023112720231127202311272023112720231127202311272023112
drop view v_upper_20231127202311272023112720231127202311272023112720231127;
drop table t_upper_20231127202311272023112720231127202311272023112720231127;
-- truncate the too long identifier in double quotes
create table "t_upper_20231127202311272023112720231127202311272023112720231127"("c_upper_20231127202311272023112720231127202311272023112720231127" int);
alter table "t_upper_20231127202311272023112720231127202311272023112720231127" add constraint "colum_upper_unique_20231127202311272023112720231127202311272023112720231127" unique("c_upper_20231127202311272023112720231127202311272023112720231127");
create index "c_upper_index_20231127202311272023112720231127202311272023112720231127" on "t_upper_20231127202311272023112720231127202311272023112720231127" ("c_upper_20231127202311272023112720231127202311272023112720231127");
\d "t_upper_2023112720231127202311272023112720231127202311272023112"
select "c_upper_20231127202311272023112720231127202311272023112720231127" from "t_upper_20231127202311272023112720231127202311272023112720231127" group by "c_upper_20231127202311272023112720231127202311272023112720231127" order by "c_upper_20231127202311272023112720231127202311272023112720231127";
create view "v_upper_20231127202311272023112720231127202311272023112720231127" as select "c_upper_20231127202311272023112720231127202311272023112720231127" from "t_upper_20231127202311272023112720231127202311272023112720231127";
select "c_upper_20231127202311272023112720231127202311272023112720231127" from "v_upper_20231127202311272023112720231127202311272023112720231127";
\d "v_upper_2023112720231127202311272023112720231127202311272023112"
drop view "v_upper_20231127202311272023112720231127202311272023112720231127";
drop table "t_upper_20231127202311272023112720231127202311272023112720231127";

-- col_description function
create table t_col_desc_20240116(id int);
comment on column t_col_desc_20240116.id is 'sadadsadasdsa';
select pg_catalog.col_description('t_col_desc_20240116'::regclass, 1);
\d+ t_col_desc_20240116
drop table t_col_desc_20240116;
-- regclass function
select 'pg_class'::regclass;
select 'PG_CLASS'::regclass;
select '"pg_class"'::regclass;
select '"PG_CLASS"'::regclass;
-- regrole function
create role "r_regrole_20240116";
select 'r_regrole_20240116'::regrole;
select 'R_REGROLE_20240116'::regrole;
drop role "r_regrole_20240116";
-- format function
SELECT format('INSERT INTO %I VALUES(%L)', 'path', 'C:\Program Files');
SELECT format('INSERT INTO %I VALUES(%L)', 'PATH', 'C:\Program Files');
-- print strict params
create or replace function f_print_p_20240116() returns void as $$
#print_strict_params off
declare
begin
raise notice '1';
end$$ language plpgsql;
drop function f_print_p_20240116;
create or replace function f_print_p_20240116() returns void as $$
#print_strict_params on
declare
begin
raise notice '1';
end$$ language oraplsql;
drop function f_print_p_20240116;
-- test collation and create range type with collation and subopclass
CREATE COLLATION col_range_20140117 (provider = libc, locale = 'en_US.utf8');
comment on collation col_range_20140117 is 'test collation';
\dO+ col_range_20140117
create type typ_rang_20240117 as range(subtype=text, collation=col_range_20140117, subtype_opclass=text_ops);
drop type typ_rang_20240117;
drop collation col_range_20140117;
-- use others exception in plpgsql procedure in opentenbase_ora mode
DO
$$
BEGIN
SELECT * FROM nonexist_table_20240118 ORDER BY deptno;
EXCEPTION WHEN others THEN
RAISE NOTICE '%', sqlstate;
RAISE NOTICE '%', sqlerrm;
END;
$$
LANGUAGE plpgsql;
-- set search_path
create schema s_search_path_20240117;
set search_path = s_search_path_20240117;
show search_path;
create table t_search_path_20240117(id int);
create schema "s_search_path_20240118";
set search_path = "s_search_path_20240118";
show search_path;
create table t_search_path_20240117(id int);
insert into t_search_path_20240117 values (1);
set search_path to 's_search_path_20240118', s_search_path_20240117;
show search_path;
select * from t_search_path_20240117;
reset search_path;
DO
$$
BEGIN
create schema s_search_path_20240124;
set search_path to s_search_path_20240124, "s_search_path_20240118";
create table t_search_path_20240117(id int);
insert into t_search_path_20240117 values (1);
END;
$$
LANGUAGE default_plsql;
create or replace function f_search_path_20240124()
return void as
$$
BEGIN
create schema s_search_path_20240124_1;
set search_path to s_search_path_20240124_1, "s_search_path_20240118";
create table t_search_path_20240117(id int);
insert into t_search_path_20240117 values (1);
END;
$$
LANGUAGE default_plsql;
select f_search_path_20240124();
drop table t_search_path_20240117;
create or replace procedure p_search_path_20240124()
as
$$
BEGIN
create schema s_search_path_20240124_2;
set search_path to s_search_path_20240124_2, "s_search_path_20240118";
create table t_search_path_20240117(id int);
insert into t_search_path_20240117 values (1);
END;
$$
LANGUAGE default_plsql;
call p_search_path_20240124();
drop table t_search_path_20240117;
reset search_path;
drop schema "s_search_path_20240118" cascade;
drop schema s_search_path_20240117 cascade;
drop schema s_search_path_20240124 cascade;
drop schema s_search_path_20240124_1 cascade;
drop schema s_search_path_20240124_2 cascade;
reset search_path;
-- set role
create role "r_gucrole_20240118" with login superuser;
create role r_gucrole_20240118 with login superuser;
set role to r_gucrole_20240118;
show role;
set role to "r_gucrole_20240118";
show role;
set role to 'r_gucrole_20240118';
show role;
set role to 'R_GUCROLE_20240118';
show role;
reset role;
drop role "r_gucrole_20240118";
drop role r_gucrole_20240118;

-- test select tableoid from user table for TAPD: 886039481
create table r_test_sysattr_20240228(id int) with (oids);
select ctid, oid, xmin, cmin, xmax, xmax, tableoid, xc_node_id, shardid, xmax_gts, xmin_gts, gtid from r_test_sysattr_20240228;
select ctid as oid, tableoid is null as tableoid from r_test_sysattr_20240228;
select t1.xmin, t2.xmax from r_test_sysattr_20240228 t1, r_test_sysattr_20240228 t2 where t1.xmax=t2.xmin;
insert into r_test_sysattr_20240228 values (1999);
select id as tableoid from r_test_sysattr_20240228;
drop table r_test_sysattr_20240228;

-- test set role for TAPD: https://tapd.woa.com/OpenTenBase_C/bugtrace/bugs/view?bug_id=1020385652126364313&jump_count=1
create user "u_upcase_20240625" with createrole createdb login;
CREATE DATABASE "d_upcase_20240625" sql mode opentenbase_ora;
\c "d_upcase_20240625"
create extension if not exists opentenbase_ora_package_function;
set role "u_upcase_20240625";
drop user if exists u2_upcase_20240625 ;
create user u2_upcase_20240625 with nosuperuser createrole createdb login;
drop user if exists u2_upcase_20240625 ;
set SESSION AUTHORIZATION "u_upcase_20240625";
drop user if exists u3_upcase_20240625 ;
create user u3_upcase_20240625 with nosuperuser createrole createdb login;
drop user if exists u3_upcase_20240625 ;
\c postgres
drop DATABASE "d_upcase_20240625" with (force);
drop user if exists "u_upcase_20240625";
