\c opentenbase_ora_package_function_regression_ora
\set ECHO none
set client_min_messages TO error;
CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;
set client_min_messages TO default;

-- =========================================================================
-- DBA_ALL_TABLES
-- ALL_ALL_TABLES
-- USER_ALL_TABLES
-- =========================================================================

SELECT * FROM DBA_TABLES limit 0;
SELECT * FROM ALL_TABLES limit 0;
SELECT * FROM USER_TABLES limit 0;

create table v_all_tables_test_1(id int);
create table v_all_tables_test_2(id int);
insert into v_all_tables_test_2 select n from generate_series(1,100) n;
analyze v_all_tables_test_2;

create temporary table v_all_tables_test_3(id int);
insert into v_all_tables_test_3 select n from generate_series(1,100) n;
analyze v_all_tables_test_3;

SELECT table_name, duration is null, LAST_ANALYZED is null FROM ALL_TABLES WHERE TABLE_NAME ilike 'v_all_tables_test%' order by 1;
SELECT table_name, duration is null, LAST_ANALYZED is null FROM DBA_TABLES WHERE TABLE_NAME ilike 'v_all_tables_test%' order by 1;
SELECT table_name, duration is null, LAST_ANALYZED is null FROM USER_TABLES WHERE TABLE_NAME ilike 'v_all_tables_test%' order by 1;

drop table v_all_tables_test_3;
drop table v_all_tables_test_2;
drop table v_all_tables_test_1;
