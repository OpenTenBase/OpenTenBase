drop database if exists db_dbms_metadata_test;
create database db_dbms_metadata_test with sql_mode=opentenbase_ora;
\c db_dbms_metadata_test
CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;

create user usr_dbms_metadata_test with superuser;
SET SESSION AUTHORIZATION usr_dbms_metadata_test;

-- Tests for package DBMS_METADATA
create table mtbl(a int);
\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', '');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', '', 'public');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'
\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', '', 'PGXZM_OSS');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

-- create type/table/function
create type public.my_test_type as (cf1 text, cf2 text);
create table public.my_test_table1(col1 integer, col2 text, col3 integer);
create table public.my_test_table2(col_1 integer, col_2 text, col_3 integer);
create global temp table public.my_test_gtt(a int, b text);
create unlogged table public.my_test_unlog(a int, b text);
CREATE OR REPLACE FUNCTION my_test_func_add(aa int, bb int) RETURNS int AS $$
DECLARE
	cnt int;
BEGIN
	cnt = aa + bb;
	raise notice 'count=%', cnt;
	return cnt;
END;
$$ LANGUAGE default_plsql;


CREATE OR REPLACE FUNCTION my_test_func_add(aa text, bb text) RETURNS text AS $$
DECLARE
	res text;
BEGIN
	res =  aa || bb;
	raise notice 'count=%', res;
	return res;
END;
$$ LANGUAGE default_plsql;

-- create schema type/table/function
CREATE SCHEMA IF NOT EXISTS pgxzm_oss;
set search_path=pgxzm_oss;
select current_schema();
create type pgxzm_oss.my_test_type as (cf1 int, cf2 int);
create table pgxzm_oss.my_test_table1(col1 integer, col2 text, col3 integer);
create table pgxzm_oss.my_test_table2(col_1 integer, col_2 text, col_3 integer);
CREATE OR REPLACE FUNCTION my_test_func_add(aa int, bb int) RETURNS int AS $$
DECLARE
	cnt int;
BEGIN
	cnt = aa + bb;
	raise notice 'count=%', cnt;
	return cnt;
END;
$$ LANGUAGE default_plsql;


CREATE OR REPLACE FUNCTION my_test_func_add(aa text, bb text) RETURNS text AS $$
DECLARE
	res text;
BEGIN
	res =  aa || bb;
	raise notice 'count=%', res;
	return res;
END;
$$ LANGUAGE default_plsql;


-----------
select typname, n.nspname from pg_type t, pg_namespace n where typname like 'MY_TEST%' and t.typnamespace=n.oid order by typnamespace, typname, t.oid;
select relname, n.nspname from pg_class c, pg_namespace n where relname like 'MY_TEST%' and c.relnamespace=n.oid order by relnamespace, relname, c.oid;
select proname, n.nspname from pg_proc p, pg_namespace n where proname like 'MY_TEST%' and p.pronamespace=n.oid order by pronamespace, proname, p.oid;

--test 
select current_schema();

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TYPE', 'MY_TEST_TYPE');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TYPE', 'MY_TEST_TYPE', 'public');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', 'MY_TEST_TABLE1');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', '');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', 'MY_TEST_TABLE1', 'public');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', '', 'public');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', 'MY_TEST_GTT', 'public');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', 'MY_TEST_UNLOG', 'public');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('FUNCTION', 'MY_TEST_FUNC_ADD');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('FUNCTION', '');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('FUNCTION', 'MY_TEST_FUNC_ADD', 'public');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('FUNCTION', '', 'public');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

set search_path = default;
select current_schema();
\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', 'MY_TEST_TABLE1');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'


\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', '');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', 'MY_TEST_TABLE1', 'public');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', '', 'PGXZM_OSS');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', 'MY_TEST_TABLE1', 'PGXZM_OSS');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('TABLE', '', 'PGXZM_OSS');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('FUNCTION', 'MY_TEST_FUNC_ADD');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('FUNCTION', '');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('FUNCTION', 'MY_TEST_FUNC_ADD', 'PGXZM_OSS');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'

\o dbms_metadata.tmp
select dbms_metadata.get_ddl('FUNCTION', '', 'PGXZM_OSS');
\o
\! cat dbms_metadata.tmp | sed '/-- Dumped by pg_dump version 10.0 @ *.*/d'


--clean all object
set search_path = default;
drop FUNCTION my_test_func_add(aa int, bb int);
drop FUNCTION my_test_func_add(aa text, bb text);
drop table my_test_table2;
drop table my_test_table1;
drop type my_test_type;
drop schema pgxzm_oss CASCADE;
drop table mtbl;
reset SESSION AUTHORIZATION;
drop user usr_dbms_metadata_test;
