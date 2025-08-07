--
-- CREATE FUNCTION
--
-- Assorted tests using SQL-language functions
--

-- All objects made in this test are in temp_func_test schema

CREATE USER regress_unpriv_user;

CREATE SCHEMA temp_func_test;
GRANT ALL ON SCHEMA temp_func_test TO public;

SET search_path TO temp_func_test, public;

--
-- Make sanity checks on the pg_proc entries created by CREATE FUNCTION
--

--
-- ARGUMENT and RETURN TYPES
--
CREATE FUNCTION functest_A_1(text, date) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 = ''abcd'' AND $2 > ''2001-01-01''';
CREATE FUNCTION functest_A_2(text[]) RETURNS int LANGUAGE 'sql'
       AS 'SELECT $1[0]::int';
CREATE FUNCTION functest_A_3() RETURNS bool LANGUAGE 'sql'
       AS 'SELECT false';
SELECT proname, prorettype::regtype, proargtypes::regtype[] FROM pg_proc
       WHERE oid in ('functest_A_1'::regproc,
                     'functest_A_2'::regproc,
                     'functest_A_3'::regproc) ORDER BY proname;

--
-- IMMUTABLE | STABLE | VOLATILE
--
CREATE FUNCTION functest_B_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 0';
CREATE FUNCTION functest_B_2(int) RETURNS bool LANGUAGE 'sql'
       IMMUTABLE AS 'SELECT $1 > 0';
CREATE FUNCTION functest_B_3(int) RETURNS bool LANGUAGE 'sql'
       STABLE AS 'SELECT $1 = 0';
CREATE FUNCTION functest_B_4(int) RETURNS bool LANGUAGE 'sql'
       VOLATILE AS 'SELECT $1 < 0';
SELECT proname, provolatile FROM pg_proc
       WHERE oid in ('functest_B_1'::regproc,
                     'functest_B_2'::regproc,
                     'functest_B_3'::regproc,
		     'functest_B_4'::regproc) ORDER BY proname;

ALTER FUNCTION functest_B_2(int) VOLATILE;
ALTER FUNCTION functest_B_3(int) COST 100;	-- unrelated change, no effect
SELECT proname, provolatile FROM pg_proc
       WHERE oid in ('functest_B_1'::regproc,
                     'functest_B_2'::regproc,
                     'functest_B_3'::regproc,
		     'functest_B_4'::regproc) ORDER BY proname;

--
-- SECURITY DEFINER | INVOKER
--
CREATE FUNCTION functest_C_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 0';
CREATE FUNCTION functest_C_2(int) RETURNS bool LANGUAGE 'sql'
       SECURITY DEFINER AS 'SELECT $1 = 0';
CREATE FUNCTION functest_C_3(int) RETURNS bool LANGUAGE 'sql'
       SECURITY INVOKER AS 'SELECT $1 < 0';
SELECT proname, prosecdef FROM pg_proc
       WHERE oid in ('functest_C_1'::regproc,
                     'functest_C_2'::regproc,
                     'functest_C_3'::regproc) ORDER BY proname;

ALTER FUNCTION functest_C_1(int) IMMUTABLE;	-- unrelated change, no effect
ALTER FUNCTION functest_C_2(int) SECURITY INVOKER;
ALTER FUNCTION functest_C_3(int) SECURITY DEFINER;
SELECT proname, prosecdef FROM pg_proc
       WHERE oid in ('functest_C_1'::regproc,
                     'functest_C_2'::regproc,
                     'functest_C_3'::regproc) ORDER BY proname;

--
-- LEAKPROOF
--
CREATE FUNCTION functest_E_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 100';
CREATE FUNCTION functest_E_2(int) RETURNS bool LANGUAGE 'sql'
       LEAKPROOF AS 'SELECT $1 > 100';
SELECT proname, proleakproof FROM pg_proc
       WHERE oid in ('functest_E_1'::regproc,
                     'functest_E_2'::regproc) ORDER BY proname;

ALTER FUNCTION functest_E_1(int) LEAKPROOF;
ALTER FUNCTION functest_E_2(int) STABLE;	-- unrelated change, no effect
SELECT proname, proleakproof FROM pg_proc
       WHERE oid in ('functest_E_1'::regproc,
                     'functest_E_2'::regproc) ORDER BY proname;

ALTER FUNCTION functest_E_2(int) NOT LEAKPROOF;	-- remove leakproog attribute
SELECT proname, proleakproof FROM pg_proc
       WHERE oid in ('functest_E_1'::regproc,
                     'functest_E_2'::regproc) ORDER BY proname;

-- it takes superuser privilege to turn on leakproof, but not for turn off
ALTER FUNCTION functest_E_1(int) OWNER TO regress_unpriv_user;
ALTER FUNCTION functest_E_2(int) OWNER TO regress_unpriv_user;

SET SESSION AUTHORIZATION regress_unpriv_user;
SET search_path TO temp_func_test, public;
ALTER FUNCTION functest_E_1(int) NOT LEAKPROOF;
ALTER FUNCTION functest_E_2(int) LEAKPROOF;

CREATE FUNCTION functest_E_3(int) RETURNS bool LANGUAGE 'sql'
       LEAKPROOF AS 'SELECT $1 < 200';	-- failed

RESET SESSION AUTHORIZATION;

--
-- CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
--
CREATE FUNCTION functest_F_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 50';
CREATE FUNCTION functest_F_2(int) RETURNS bool LANGUAGE 'sql'
       CALLED ON NULL INPUT AS 'SELECT $1 = 50';
CREATE FUNCTION functest_F_3(int) RETURNS bool LANGUAGE 'sql'
       RETURNS NULL ON NULL INPUT AS 'SELECT $1 < 50';
CREATE FUNCTION functest_F_4(int) RETURNS bool LANGUAGE 'sql'
       STRICT AS 'SELECT $1 = 50';
SELECT proname, proisstrict FROM pg_proc
       WHERE oid in ('functest_F_1'::regproc,
                     'functest_F_2'::regproc,
                     'functest_F_3'::regproc,
                     'functest_F_4'::regproc) ORDER BY proname;

ALTER FUNCTION functest_F_1(int) IMMUTABLE;	-- unrelated change, no effect
ALTER FUNCTION functest_F_2(int) STRICT;
ALTER FUNCTION functest_F_3(int) CALLED ON NULL INPUT;
SELECT proname, proisstrict FROM pg_proc
       WHERE oid in ('functest_F_1'::regproc,
                     'functest_F_2'::regproc,
                     'functest_F_3'::regproc,
                     'functest_F_4'::regproc) ORDER BY proname;


-- pg_get_functiondef tests

SELECT pg_get_functiondef('functest_A_1'::regproc);
SELECT pg_get_functiondef('functest_B_3'::regproc);
SELECT pg_get_functiondef('functest_C_3'::regproc);
SELECT pg_get_functiondef('functest_F_2'::regproc);


-- information_schema tests

CREATE FUNCTION functest_IS_1(a int, b int default 1, c text default 'foo')
    RETURNS int
    LANGUAGE SQL
    AS 'SELECT $1 + $2';

CREATE FUNCTION functest_IS_2(out a int, b int default 1)
    RETURNS int
    LANGUAGE SQL
    AS 'SELECT $1';

CREATE FUNCTION functest_IS_3(a int default 1, out b int)
    RETURNS int
    LANGUAGE SQL
    AS 'SELECT $1';

SELECT routine_name, ordinal_position, parameter_name, parameter_default
    FROM information_schema.parameters JOIN information_schema.routines USING (specific_schema, specific_name)
    WHERE routine_schema = 'temp_func_test' AND routine_name ~ '^functest_is_'
    ORDER BY 1, 2;

DROP FUNCTION functest_IS_1(int, int, text), functest_IS_2(int), functest_IS_3(int);

-- overload
CREATE FUNCTION functest_B_2(bigint) RETURNS bool LANGUAGE 'sql'
       IMMUTABLE AS 'SELECT $1 > 0';

DROP FUNCTION functest_b_1;
DROP FUNCTION functest_b_1;  -- error, not found
DROP FUNCTION functest_b_2;  -- error, ambiguous


-- CREATE OR REPLACE tests

CREATE FUNCTION functest1(a int) RETURNS int LANGUAGE SQL AS 'SELECT $1';
CREATE OR REPLACE FUNCTION functest1(a int) RETURNS int LANGUAGE SQL WINDOW AS 'SELECT $1';
CREATE OR REPLACE PROCEDURE functest1(a int) LANGUAGE SQL AS 'SELECT $1';
DROP FUNCTION functest1(a int);


-- Check behavior of VOID-returning SQL functions

CREATE FUNCTION voidtest1(a int) RETURNS VOID LANGUAGE SQL AS
$$ SELECT a + 1 $$;
SELECT voidtest1(42);

CREATE FUNCTION voidtest2(a int, b int) RETURNS VOID LANGUAGE SQL AS
$$ SELECT voidtest1(a + b) $$;
SELECT voidtest2(11,22);

-- currently, we can inline voidtest2 but not voidtest1
EXPLAIN (verbose, costs off) SELECT voidtest2(11,22);

CREATE TABLE sometable(f1 int);

CREATE FUNCTION voidtest3(a int) RETURNS VOID LANGUAGE SQL AS
$$ INSERT INTO sometable VALUES(a + 1) $$;
SELECT voidtest3(17);

CREATE FUNCTION voidtest4(a int) RETURNS VOID LANGUAGE SQL AS
$$ INSERT INTO sometable VALUES(a - 1) RETURNING f1 $$;
SELECT voidtest4(39);

TABLE sometable;

CREATE FUNCTION voidtest5(a int) RETURNS SETOF VOID LANGUAGE SQL AS
$$ SELECT generate_series(1, a) $$ STABLE;
SELECT * FROM voidtest5(3);

-- Cleanup
\set VERBOSITY terse \\ -- suppress cascade details
DROP SCHEMA temp_func_test CASCADE;
\set VERBOSITY default
DROP USER regress_unpriv_user;
RESET search_path;

---------------------------------------------------------------------------------
--Optimize the execution efficiency of simple sql function calls in targetlist.
--TestPoint:1. simple select statement in sql function
\c regression_ora
set enable_inline_target_function to on;

drop table tab_tab1_20230705;
drop table tab_tab2_20230705;
drop function func_f1_20230705;

create  table tab_tab1_20230705(id int, index int);
insert into tab_tab1_20230705 values (generate_series(1, 5), generate_series(1, 5));

create table tab_tab2_20230705(id int);
insert into tab_tab2_20230705 values (generate_series(1, 5));

create function func_f1_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql immutable;

select func_f1_20230705(id) from tab_tab2_20230705 order by func_f1_20230705;

drop function func_f1_20230705;

create function func_f1_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql stable;

select func_f1_20230705(id) from tab_tab2_20230705 order by func_f1_20230705;

--not support volatile or default;
drop function func_f1_20230705;

create function func_f1_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql volatile;

select func_f1_20230705(id) from tab_tab2_20230705 order by func_f1_20230705;

drop function func_f1_20230705;

create function func_f1_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql;

select func_f1_20230705(id) from tab_tab2_20230705 order by func_f1_20230705;


--TestPoint:2.aggregate function in sql function
drop function func_f1_20230705;
drop table tab_tab1_20230705;
drop table tab_tab2_20230705;

create table tab_tab1_20230705(id1 int, id2 int, id3 int);
insert into tab_tab1_20230705 values(generate_series(1,5), generate_series(1,5), generate_series(1,5));

create table tab_tab2_20230705(id2 int, id3 int);
insert into tab_tab2_20230705 values(generate_series(1,5), generate_series(1, 5));

create function func_f1_20230705(id22 int, id33 int) returns numeric as $$
 select nvl(sum(round(id1, 2)), 0) from tab_tab1_20230705 where  tab_tab1_20230705.id2 = id22 and tab_tab1_20230705.id3=id33;
$$ language sql stable;

select func_f1_20230705(id2, id3) from tab_tab2_20230705 order by func_f1_20230705;

--TestPoint:3.join in sql function;
drop table tab_tab1_20230705;
drop table tab_tab2_20230705;
drop function func_f1_20230705;

create table tab_tab1_20230705(id1 int, id2 int, id3 int);
insert into tab_tab1_20230705 values(generate_series(1,5), generate_series(1,5), generate_series(1,5));

create table tab_tab2_20230705(id2 int, id3 int);
insert into tab_tab2_20230705 values(generate_series(1,5), generate_series(1, 5));

create function func_f1_20230705(var int) returns integer as $$
 select tab_tab1_20230705.id1 from tab_tab1_20230705,tab_tab2_20230705 where tab_tab1_20230705.id2= tab_tab2_20230705.id2 and tab_tab2_20230705.id3 = var;
$$ language sql stable;

select func_f1_20230705(id2) from tab_tab2_20230705 order by func_f1_20230705;

--TestPoint:7. test $;
drop table tab_tab1_20230705;
drop table tab_tab2_20230705;
drop function func_f1_20230705;
drop type type_test_comp_20230705;

create  table tab_tab1_20230705(id int, id1 int, id2 int, index int);
insert into tab_tab1_20230705 values (generate_series(1, 5), generate_series(1, 5), generate_series(1, 5),generate_series(1, 5));

create table tab_tab2_20230705(id int, id1 int);
insert into tab_tab2_20230705 values (generate_series(1, 5), generate_series(1,5));

create function func_f1_20230705(int, int) returns int as $$
select index from tab_tab1_20230705 where id = $1 and id1 = $2 and id2 = $1 ;
$$ language sql immutable;

select func_f1_20230705(id, id1) from tab_tab2_20230705 order by func_f1_20230705;

drop table tab_tab1_20230705;
drop table tab_tab2_20230705;
drop function func_f1_20230705;

--TestPoint:8. test multi-function call;

create  table tab_tab1_20230705(id int, index int);
insert into tab_tab1_20230705 values (generate_series(1, 5), generate_series(1, 5));

create table tab_tab2_20230705(id int);
insert into tab_tab2_20230705 values (generate_series(1, 5));

create function func_f1_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql immutable;

create function func_f2_20230705(var int) returns int as $$
select index + 1 from tab_tab1_20230705 where id = var;
$$ language sql stable;

select func_f1_20230705(id), func_f2_20230705(id), func_f1_20230705(id) from tab_tab2_20230705 order by func_f1_20230705;

--TestPoint:9. test pkg.func
create or replace package pkg_pkg1_20230705
  is
  function func1(var int) return int;
end;
/

create or replace package body pkg_pkg1_20230705
is
$p$
function func1(var int) return int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql immutable ;
$p$ 
/

ALTER FUNCTION pkg_pkg1_20230705.func1(int) SECURITY INVOKER;
select pkg_pkg1_20230705.func1(id) from tab_tab2_20230705 order by func1;
drop package pkg_pkg1_20230705;

--TestPoint:10.return set
create function func_func3_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var or id = var + 1;
$$ language sql immutable security invoker;

select func_func3_20230705(id) from tab_tab2_20230705 order by 1;

--TestPoint:11. func1, func2...
drop function func_func3_20230705;

create function func_func3_20230705(var int) return int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql immutable security invoker;

create function func_func4_20230705(var int) return int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql immutable security invoker;

explain (costs off) select func_func3_20230705(id),func_func4_20230705(id) from tab_tab2_20230705 order by 1,2;

--TestPoint:12.func1(func2)
explain (costs off) select func_func3_20230705(func_func4_20230705(id)) from tab_tab2_20230705 order by 1;

--TestPoint:13.explain
explain (costs off) select func_func3_20230705(id) from tab_tab2_20230705 order by func_func3_20230705;

--TestPoint:14.group by,order by
select func_func3_20230705(id) from tab_tab2_20230705 group by func_func3_20230705 order by func_func3_20230705;
select func_func3_20230705(id) from tab_tab2_20230705 group by 1 order by 1;

--TestPoint:15.out parameter (Not optimize)
drop function func_func3_20230705;

create function func_func3_20230705( out var int) return int as $$
select index from tab_tab1_20230705 where id = 1;
$$ language sql immutable security invoker;

explain (costs off) select func_func3_20230705(id) from tab_tab2_20230705 group by 1 order by 1;

--TestPoint:17.cte (Not optimize)
explain (costs off) WITH my_cte_20230705 AS (
select id +1 from tab_tab1_20230705
)
SELECT *
FROM my_cte_20230705 order by 1;
/

--TestPoint:18. test expr
select (func_f1_20230705(id)+1)/2 func from tab_tab2_20230705 order by func;
explain (costs off) select (func_f1_20230705(id)+1)/2 func from tab_tab2_20230705 order by func;

select (func_f1_20230705(id)+1)/2 func, (func_f1_20230705(id + 1) + func_f2_20230705(id)) * (func_f1_20230705(id + 1) + func_f2_20230705(id)) func2 from tab_tab2_20230705 order by func, func2; 
explain (costs off) select (func_f1_20230705(id)+1)/2 func, (func_f1_20230705(id + 1) + func_f2_20230705(id)) * (func_f1_20230705(id + 1) + func_f2_20230705(id)) func2 from tab_tab2_20230705 order by func, func2; 

select (func_f1_20230705(id)+1)/2 func, (func_f1_20230705(id + 1) + func_f2_20230705(id)) * (func_f1_20230705(id + 1) + func_f2_20230705(id)) func2, func_f1_20230705(func_f2_20230705(id))*func_f1_20230705(func_f2_20230705(id)) func3 from tab_tab2_20230705 order by func, func2, func3; 
explain (costs off) select (func_f1_20230705(id)+1)/2 func, (func_f1_20230705(id + 1) + func_f2_20230705(id)) * (func_f1_20230705(id + 1) + func_f2_20230705(id)) func2, func_f1_20230705(func_f2_20230705(id))*func_f1_20230705(func_f2_20230705(id)) func3 from tab_tab2_20230705 order by func, func2, func3; 

drop function func_f1_20230705;
drop function func_f2_20230705;
drop function func_func3_20230705;
drop function func_func4_20230705;

drop table tab_tab1_20230705;
drop table tab_tab2_20230705;

--TestPoint:19.case when
create  table tab_tab1_20230705(id int, index int);
insert into tab_tab1_20230705 values (generate_series(1, 5), generate_series(1, 5));

create table tab_tab2_20230705(id int);
insert into tab_tab2_20230705 values (generate_series(1, 5));

create function func_f1_20230705(var int) returns bool as $$
select index>0 from tab_tab1_20230705 where id = var;
$$ language sql immutable;

explain (costs off) select case when(func_f1_20230705(id)) then 1 end from tab_tab2_20230705;

--TestPoint:20.subquery in range table
explain (costs off) select * from (select func_f1_20230705(id) from tab_tab2_20230705);
explain (costs off) select * from (select case when(func_f1_20230705(id)) then 1 end from tab_tab2_20230705);

drop function func_f1_20230705;
drop table tab_tab2_20230705;
drop table tab_tab1_20230705;

--TestPoint:21. Fix the problem that there are resjunk columns in the select result set that are not optimized
DROP TABLE if exists rqg_table1_20230818 cascade;
CREATE TABLE rqg_table1_20230818 (
c0 int,
c1 int default null,
c2 varchar2(200),
c3 varchar2(200) default null,
c4 date,
c5 date default null,
c6 timestamp,
c7 timestamp default null,
c8 number,
c9 number default null)   ;
alter table rqg_table1_20230818 alter column c0 drop not null;
INSERT INTO rqg_table1_20230818 VALUES  (NULL, NULL, 'wefi', 'efiefgiow', '2027-03-09 08:58:16', '2026-10-03 09:43:53', '1997-02-19 14:14:33.022419', '1982-09-25 16:13:19.028159', -1.23456789123457e+30, 0.123456789123457) ,  (NULL, NULL, 'fiefgiowidpscybxdjowjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxgyyxggjwknlrtxeaywyrecbgxyqvymfhtvilxtkzldzbzvphejanvyseyvhzwnbhmaigvybdqsx', 'foo', '2031-10-21 14:17:04', '1984-07-28 04:51:40', '1988-12-18', NULL, 6.32840190138568e+18, -1.23456789123457e+30) ,  (NULL, NULL, 'foo', 'iefgiowidpscybxdjowjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxgyyxggjwkn', '2004-11-02', '2003-12-07 14:51:42', '1996-09-05 11:35:27.019267', '1983-06-28 15:39:10', 0.123456789123457, 1.23456789123457e+39) ,  (NULL, NULL, 'foo', 'efgiowidpscybxdjowjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxgyyxggjwknlrtxeaywyrecbgxyqvymfhtvilxtkzldzbzvphejanvyseyvhzwnbhmaigvybdqsxzvhvnswnbawthd', '2018-12-06', '2010-08-22 09:36:40', '1993-04-07 21:46:35.017665', '1989-09-05 18:25:56', 1147928576, 1.23456789123457e-09) ,  (NULL, NULL, 'fgiowidpscybxdjowjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxgyyxggjwknlrtxeaywyrecbgxyqvymfhtvilxtkzld', 'bar', '2011-07-26 18:15:58', '2033-05-26', '2035-08-06 17:03:17.038644', '2009-05-06 19:15:53.021350', 0.123456789123457, 1.23456789123457e+25) ,  (NULL, NULL, 'giowidp', 'iowidpscybxdjowjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxgyyxggjwknlrtxeaywyrecbgxyqvymfhtv', '2030-04-04 01:06:57', '2005-02-04', '1974-09-16 12:02:11.034339', '2027-07-21 13:09:40.062233', -694353920, 1.23456789123457e+30) ,  (NULL, NULL, 'owidpscybxdjowjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxgyyxggjwknlrtxeaywyrecbgxyqvymfhtvilxtkzld', 'w', '2019-09-19 16:07:02', '2009-08-12 14:37:06', '1972-02-15 13:06:22.054404', '1992-08-17', 1.23456789123457e+30, 1.23456789123457e+25) ,  (NULL, NULL, 'idpscybxdjowjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxgyyxggjwknlrtxeaywyrecbgxyqvymfhtvilxtkzldzbzvphejanvyseyvhzwnbhmaigvybdqsxzvhvnswnbawthdankofmwkvdkcbjtoim', 'dpscybxd', '1979-06-23 16:46:37', '2018-12-09', '2032-10-05 05:03:24.001243', '2021-12-17 08:13:20.059316', 1.23456789123457e+44, 0.627197265625) ,  (NULL, NULL, 'bar', 'pscybxd', '1977-05-12', '2021-09-14 06:26:57', '1977-07-18 04:31:35', '1995-10-23 14:51:18', 1.23456789123457e-09, 1.23456789123457e+43) ,  (NULL, NULL, 'scy', 'cybxd', '2021-03-08 06:07:14', '2024-10-16 07:49:29', NULL, '2009-11-18 07:44:23.029878', -1.23456789123457e+39, 0.123456789123457) ;
DROP TABLE if exists rqg_table2_20230818 cascade;
CREATE TABLE rqg_table2_20230818 (
c0 int,
c1 int default null,
c2 varchar2(200),
c3 varchar2(200) default null,
c4 date,
c5 date default null,
c6 timestamp,
c7 timestamp default null,
c8 number,
c9 number default null)   distribute by replication;
alter table rqg_table2_20230818 alter column c0 drop not null;
INSERT INTO rqg_table2_20230818 VALUES  (NULL, NULL, 'foo', 'ybxdjowjnr', '2011-02-20 02:03:03', '2029-11-07 19:52:01', '1996-12-12 15:48:15.035886', '1989-12-07 03:43:11', -1.23456789123457e-09, -1.23456789123457e+39) ,  (NULL, NULL, 'bxdjowjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxg', 'xd', '2007-02-07', '2007-03-22', '2020-08-05 18:22:53.032564', '1991-08-25 14:07:50.022546', 1.23456789123457e+25, -1.23456789123457e+43) ,  (NULL, NULL, 'foo', 'djowjnrztz', '2014-06-24 11:43:37', '1973-07-04 22:06:52', '2009-02-27 20:20:33.044304', '1982-02-14 03:52:36.053808', 1.23456789123457e+44, -1.23456789123457e-09) ,  (NULL, NULL, 'foo', 'bar', '2018-05-11', '1980-09-05 03:02:56', '2013-03-06 00:20:53', '1996-01-10 19:05:16.061765', 1.23456789123457e-09, -1.23456789123457e+30) ,  (NULL, NULL, 'bar', 'bar', '1984-10-20', '2010-05-17', '1978-01-21 17:23:58.062912', '2035-08-16 18:39:07', 1.23456789123457e+30, 0.123456789123457) ,  (NULL, NULL, 'jowjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxgyyxggjwknlrtxeaywyrecbgxyqvymfhtvilxtkzldzbzvphejanvyseyvhzwnbhmaigvybdqsxzvhvnswnb', 'owjnr', '1977-10-15', '1977-03-14 12:17:56', '2026-09-22 11:30:55', '1971-07-16 09:24:50.019257', 1.23456789123457e+39, 1.23456789123457e+43) ,  (NULL, NULL, 'foo', 'bar', '2020-12-19 16:15:20', '1985-05-05 05:31:05', '2029-10-01 10:52:59.034402', '2023-09-28 12:38:57', 6.12179926848006e+18, 0.123456789123457) ,  (NULL, NULL, 'wjnrztzxgzesqbzfyqffhoowltnboetsvjxsojplnmfcvrtccgfuslcuktxgyyxggjwk', 'foo', '2007-08-17', '1992-11-27', '2015-03-03 15:35:50', '1998-03-12 10:47:29.034583', -1.23456789123457e+39, -1.23456789123457e+43) ,  (NULL, NULL, 'foo', 'bar', '2011-10-04 05:12:22', '2029-09-20', '2035-04-14 14:48:27.050640', '1990-06-16 08:45:41.033703', -1.23456789123457e+43, 1999437824) ,  (NULL, NULL, 'bar', 'foo', '1985-01-15', '1994-10-12 22:44:16', '2003-11-23 07:18:14.032409', NULL, -1.23456789123457e+25, 0.123456789123457) ;

CREATE OR REPLACE FUNCTION F_INCLINE_TARGET_20230816 (v1 rqg_table1_20230818.c1%type) RETURNS rqg_table1_20230818.c1%type AS $$ SELECT c0 FROM  ( SELECT  a1.c0,  a1.c1  ,a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table1_20230818 AS a1 WHERE (   ( a1.c8 > -2.82287596684479e+124 ) AND a1.c9 IS NOT NULL ))  where c0<=v1  ORDER BY c1 limit 1; $$ LANGUAGE SQL IMMUTABLE ;
explain (costs off) SELECT F_INCLINE_TARGET_20230816(F_INCLINE_TARGET_20230816(F_INCLINE_TARGET_20230816(c1))) from ( SELECT a1.c0, a1.c1, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table2_20230818 AS a1 WHERE a1.c0 IS  NULL AND ( a1.c4 IN ( a1.c4, '2018-12-27 12:19:46', a1.c5, '1979-06-26', a1.c5, '2026-10-08 02:10:04', a1.c4, '1977-04-15', a1.c4, a1.c5 ) AND a1.c7 <= any ( a1.c7, a1.c6 ) ) AND ( a1.c0  is  null ) GROUP BY a1.c0, a1.c1, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 HAVING ( ( ( a1.c4 IS NOT NULL ) OR ( a1.c1 = 8 ) ) OR ( a1.c0 > 6408 ) ) OR ( a1.c3 IS not NULL ) ) ;
drop function F_INCLINE_TARGET_20230816;
drop table rqg_table1_20230818;
drop table rqg_table2_20230818;

--TestPoint:22. Fix Limit core.
create table t_inline_target_20230818(id int,f1 varchar2(20));
insert into t_inline_target_20230818 select t,t from generate_series(1,10)t;
create table t2_inline_target_20230818(id int,f1 varchar2(20));
insert into t2_inline_target_20230818 select t,t from generate_series(1,10)t;

drop function F6_INCLINE_TARGET_20230816;
drop table t2_inline_target_20230818;
drop table t_inline_target_20230818;

--TestPoint:24. Fix limit 0 result wrong

--limit
create table t_inline_target_20230818(id int,f1 varchar2(20)) with (oids);
insert into t_inline_target_20230818 select t,t-1 from generate_series(1,10)t;
insert into t_inline_target_20230818 select null,null from generate_series(1,10)t;

create table t2_inline_target_20230818(id int,f1 varchar2(20)) with (oids);
insert into t2_inline_target_20230818 select t,t-1 from generate_series(1,10)t;
insert into t2_inline_target_20230818 select t,t-1 from generate_series(1,10)t;
insert into t2_inline_target_20230818 select null,null from generate_series(1,10)t;

CREATE OR REPLACE FUNCTION F6_INCLINE_TARGET_20230816 (v1 int) RETURNS int AS $$ SELECT distinct id from t_inline_target_20230818 t where f1>v1 order by 1 limit  v1%3 ; $$ LANGUAGE SQL IMMUTABLE ;

select distinct *, F6_INCLINE_TARGET_20230816(id) from t2_inline_target_20230818  order by 1,2 ;
drop function F6_INCLINE_TARGET_20230816;
drop table t_inline_target_20230818;
drop table t2_inline_target_20230818;

--TestPoint:25. test Limit not null;

create  table tab_tab1_20230705(id int, index int);
insert into tab_tab1_20230705 values (generate_series(1, 5), generate_series(1, 5));

create table tab_tab2_20230705(id int);
insert into tab_tab2_20230705 values (generate_series(1, 5));

drop function func_f1_20230705;

create function func_f1_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var limit 1+2*3;
$$ language sql immutable;

explain (costs off) select func_f1_20230705(id) from tab_tab2_20230705 order by func_f1_20230705;

drop function func_f1_20230705;

create function func_f1_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var limit 0;
$$ language sql immutable;

select func_f1_20230705(id) from tab_tab2_20230705 order by func_f1_20230705;

--TestPoint:26. test Limit is null;
drop function func_f1_20230705;

create function func_f1_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql immutable;

explain (costs off) select func_f1_20230705(id) from tab_tab2_20230705 order by func_f1_20230705;

drop function func_f1_20230705;
drop table tab_tab1_20230705;
drop table tab_tab2_20230705;

--TestPoint:27. subquery has function;
create  table tab_tab1_20230705(id int, index int);
insert into tab_tab1_20230705 values (generate_series(1, 5), generate_series(1, 5));

create table tab_tab2_20230705(id int);
insert into tab_tab2_20230705 values (generate_series(1, 5));

create function func_f1_20230705(var int) returns int as $$
select index from tab_tab1_20230705 where id = var;
$$ language sql immutable;


select (select func_f1_20230705(id) from tab_tab2_20230705 order by func_f1_20230705 limit 1) from dual;

drop function func_f1_20230705;
drop table tab_tab1_20230705;
drop table tab_tab2_20230705;

-- 
-- Fix the problem that the inline_target_function parameter is not inlined.
-- 
--TestPoint:1.TAPD Tests.
create table tab_tb1_20230919(id int, id1 int, id2 int);
create table tab_tb2_20230919(id int, id1 int, id2 int);

create or replace function f_f1_20230919() returns numeric as
$$select NVL(SUM(round(tab_tb1_20230919.id*tab_tb2_20230919.id,2)),0) from tab_tb1_20230919, tab_tb2_20230919
 where tab_tb1_20230919.id=tab_tb2_20230919.id
 and tab_tb1_20230919.id1=tab_tb2_20230919.id1;$$
language sql stable;

select * from 
(select case when 1 = 2 then
round(1,2)
else
round(100 - f_f1_20230919()) end);

explain (costs off) select * from 
(select case when 1 = 2 then
round(1,2)
else
round(100 - f_f1_20230919()) end);

drop function f_f1_20230919;
drop table tab_tb1_20230919;
drop table tab_tb2_20230919;

--TestPoint:2.round test.
create table tab_tab1_20230919(id int, id1 int, id2 int);
create table tab_tab3_20230919(id int, id1 int, id2 int);
insert into tab_tab1_20230919 values(1,1,1);
insert into tab_tab3_20230919 values(1,1,1);

create or replace function f_f3_20230919(var int) returns float8 as
$$select id::float8 from tab_tab3_20230919 where id1=var;$$
language sql stable;

select round(f_f3_20230919(id)) from tab_tab1_20230919 order by 1;
explain (costs off) select round(f_f3_20230919(id)) from tab_tab1_20230919;

select round((f_f3_20230919(id)+1)*100) from tab_tab1_20230919 order by 1;
explain (costs off) select round((f_f3_20230919(id)+1)*100) from tab_tab1_20230919;

select round((f_f3_20230919(id)+1)*100) +round((f_f3_20230919(id)+1)*100) from tab_tab1_20230919 order by 1;
explain (costs off) select round((f_f3_20230919(id)+1)*100) + round((f_f3_20230919(id)+1)*100) from tab_tab1_20230919;


--TestPoint:3.func(func()) test.
create or replace function f_f4_20230919(var int) returns float8 as
$$select id::float8 from tab_tab3_20230919 where id1=var;$$
language sql volatile;

  --f_f4_20230919 can't inline,f_f3_20230919 can inline
select f_f4_20230919(f_f3_20230919(id)) from tab_tab1_20230919 order by 1;
explain select f_f4_20230919(f_f3_20230919(id)) from tab_tab1_20230919 order by 1;

drop function f_f4_20230919;
drop function f_f3_20230919;
drop table tab_tab3_20230919;
drop table tab_tab1_20230919;

drop extension if exists opentenbase_type_object;
reset enable_inline_target_function;
---------------------------------------------------------------------------------
