\c opentenbase_ora_package_function_regression_ora
\set ECHO none
set client_min_messages TO error;
CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;
set client_min_messages TO default;
\set ECHO all

--
-- test empty_clob() function
--

select empty_clob() is null;
drop table if exists empty_clob_table;
create table empty_clob_table (id int, col clob not null);
insert into empty_clob_table values(1, null::clob);
insert into empty_clob_table values(2, ''::clob);
insert into empty_clob_table values(3, empty_clob());
select * from empty_clob_table;
drop table empty_clob_table;
select empty_clob() is null;
create table empty_clob_table (id int, col clob not null);
insert into empty_clob_table values(1, null::clob);
insert into empty_clob_table values(2, ''::clob);
insert into empty_clob_table values(3, empty_clob());
select * from empty_clob_table;
drop table empty_clob_table;

-- test nclob
create table nct(a int, b nclob);
insert into nct values(1, 'nclob value');
select * from nct;
drop table nct;

select to_nclob(1);
select to_nclob('2');
select to_nclob('2'::nclob);
select to_nclob(2.1);

-- test nclob array
create table nct(v int, b nclob[]);
insert into nct values(1, ARRAY [ 'text1', 'test2' ] );
insert into nct values(1, '{ "test3", "test4" }');
select * from nct;
drop table nct;
