CREATE TEMP TABLE x (
	a serial,
	b int,
	c text not null default 'stuff',
	d text,
	e text
) WITH OIDS;

CREATE FUNCTION fn_x_before () RETURNS TRIGGER AS '
  BEGIN
		NEW.e := ''before trigger fired''::text;
		return NEW;
	END;
' LANGUAGE plpgsql;

CREATE FUNCTION fn_x_after () RETURNS TRIGGER AS '
  BEGIN
		UPDATE x set e=''after trigger fired'' where c=''stuff'';
		return NULL;
	END;
' LANGUAGE plpgsql;

CREATE TRIGGER trg_x_after AFTER INSERT ON x
FOR EACH ROW EXECUTE PROCEDURE fn_x_after();

CREATE TRIGGER trg_x_before BEFORE INSERT ON x
FOR EACH ROW EXECUTE PROCEDURE fn_x_before();

COPY x (a, b, c, d, e) from stdin;
9999	\N	\\N	\NN	\N
10000	21	31	41	51
\.

COPY x (b, d) from stdin;
1	test_1
\.

COPY x (b, d) from stdin;
2	test_2
3	test_3
4	test_4
5	test_5
\.

COPY x (a, b, c, d, e) from stdin;
10001	22	32	42	52
10002	23	33	43	53
10003	24	34	44	54
10004	25	35	45	55
10005	26	36	46	56
\.

-- non-existent column in column list: should fail
COPY x (xyz) from stdin;

-- too many columns in column list: should fail
COPY x (a, b, c, d, e, d, c) from stdin;

-- missing data: should fail
COPY x from stdin;

\.
COPY x from stdin;
2000	230	23	23
\.
COPY x from stdin;
2001	231	\N	\N
\.

-- extra data: should fail
COPY x from stdin;
2002	232	40	50	60	70	80
\.

-- various COPY options: delimiters, oids, NULL string, encoding
COPY x (b, c, d, e) from stdin with oids delimiter ',' null 'x';
500000,x,45,80,90
500001,x,\x,\\x,\\\x
500002,x,\,,\\\,,\\
\.

COPY x from stdin WITH DELIMITER AS ';' NULL AS '';
3000;;c;;
\.

COPY x from stdin WITH DELIMITER AS ':' NULL AS E'\\X' ENCODING 'sql_ascii';
4000:\X:C:\X:\X
4001:1:empty::
4002:2:null:\X:\X
4003:3:Backslash:\\:\\
4004:4:BackslashX:\\X:\\X
4005:5:N:\N:\N
4006:6:BackslashN:\\N:\\N
4007:7:XX:\XX:\XX
4008:8:Delimiter:\::\:
\.

-- check results of copy in
SELECT * FROM x ORDER BY a, b;

-- COPY w/ oids on a table w/o oids should fail
CREATE TABLE no_oids (
	a	int,
	b	int
) WITHOUT OIDS;

INSERT INTO no_oids (a, b) VALUES (5, 10);
INSERT INTO no_oids (a, b) VALUES (20, 30);

-- should fail
COPY no_oids FROM stdin WITH OIDS;
COPY no_oids TO stdout WITH OIDS;

-- check copy out
COPY x TO stdout;
COPY x (c, e) TO stdout;
COPY x (b, e) TO stdout WITH NULL 'I''m null';

CREATE TEMP TABLE y (
	col1 text,
	col2 text
);

INSERT INTO y VALUES ('Jackson, Sam', E'\\h');
INSERT INTO y VALUES ('It is "perfect".',E'\t');
INSERT INTO y VALUES ('', NULL);

COPY y TO stdout WITH CSV;
COPY y TO stdout WITH CSV QUOTE '''' DELIMITER '|';
COPY y TO stdout WITH CSV FORCE QUOTE col2 ESCAPE E'\\' ENCODING 'sql_ascii';
COPY y TO stdout WITH CSV FORCE QUOTE *;

-- Repeat above tests with new 9.0 option syntax

COPY y TO stdout (FORMAT CSV);
COPY y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE *);

\copy y TO stdout (FORMAT CSV)
\copy y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE *)

--test that we read consecutive LFs properly

CREATE TEMP TABLE testnl (a int, b text, c int);

COPY testnl FROM stdin CSV;
1,"a field with two LFs

inside",2
\.

-- test end of copy marker
CREATE TEMP TABLE testeoc (a text);

COPY testeoc FROM stdin CSV;
a\.
\.b
c\.d
"\."
\.

COPY testeoc TO stdout CSV;

-- test handling of nonstandard null marker that violates escaping rules

CREATE TEMP TABLE testnull(a int, b text);
INSERT INTO testnull VALUES (1, E'\\0'), (NULL, NULL);

COPY testnull TO stdout WITH NULL AS E'\\0';

COPY testnull FROM stdin WITH NULL AS E'\\0';
42	\\0
\0	\0
\.

SELECT * FROM testnull ORDER BY 1,2;

BEGIN;
CREATE TABLE vistest (LIKE testeoc);
COPY vistest FROM stdin CSV;
a0
b
\.
COMMIT;
SELECT * FROM vistest order by 1;
BEGIN;
TRUNCATE vistest;
COPY vistest FROM stdin CSV;
a1
b
\.
SELECT * FROM vistest order by 1;
SAVEPOINT s1;
TRUNCATE vistest;
COPY vistest FROM stdin CSV;
d1
e
\.
SELECT * FROM vistest order by 1;
COMMIT;
SELECT * FROM vistest order by 1;

BEGIN;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
a2
b
\.
SELECT * FROM vistest order by 1;
SAVEPOINT s1;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
d2
e
\.
SELECT * FROM vistest order by 1;
COMMIT;
SELECT * FROM vistest order by 1;

BEGIN;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
x
y
\.
SELECT * FROM vistest order by 1;
COMMIT;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
p
g
\.
BEGIN;
TRUNCATE vistest;
SAVEPOINT s1;
COPY vistest FROM stdin CSV FREEZE;
m
k
\.
COMMIT;
BEGIN;
INSERT INTO vistest VALUES ('z');
SAVEPOINT s1;
TRUNCATE vistest;
ROLLBACK TO SAVEPOINT s1;
COPY vistest FROM stdin CSV FREEZE;
d3
e
\.
COMMIT;
CREATE FUNCTION truncate_in_subxact() RETURNS VOID AS
$$
BEGIN
	TRUNCATE vistest;
EXCEPTION
  WHEN OTHERS THEN
	INSERT INTO vistest VALUES ('subxact failure');
END;
$$ language plpgsql;
BEGIN;
INSERT INTO vistest VALUES ('z');
SELECT truncate_in_subxact();
COPY vistest FROM stdin CSV FREEZE;
d4
e
\.
SELECT * FROM vistest order by 1;
COMMIT;
SELECT * FROM vistest order by 1;
-- Test FORCE_NOT_NULL and FORCE_NULL options
CREATE TEMP TABLE forcetest (
    a INT NOT NULL,
    b TEXT NOT NULL,
    c TEXT,
    d TEXT,
    e TEXT
);
\pset null NULL
-- should succeed with no effect ("b" remains an empty string, "c" remains NULL)
BEGIN;
COPY forcetest (a, b, c) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL(b), FORCE_NULL(c));
1,,""
\.
COMMIT;
SELECT b, c FROM forcetest WHERE a = 1;
-- should succeed, FORCE_NULL and FORCE_NOT_NULL can be both specified
BEGIN;
COPY forcetest (a, b, c, d) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL(c,d), FORCE_NULL(c,d));
2,'a',,""
\.
COMMIT;
SELECT c, d FROM forcetest WHERE a = 2;
-- should fail with not-null constraint violation
BEGIN;
COPY forcetest (a, b, c) FROM STDIN WITH (FORMAT csv, FORCE_NULL(b), FORCE_NOT_NULL(c));
3,,""
\.
ROLLBACK;
-- should fail with "not referenced by COPY" error
BEGIN;
COPY forcetest (d, e) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL(b));
ROLLBACK;
-- should fail with "not referenced by COPY" error
BEGIN;
COPY forcetest (d, e) FROM STDIN WITH (FORMAT csv, FORCE_NULL(b));
ROLLBACK;
\pset null ''

-- test case with whole-row Var in a check constraint
create table check_con_tbl (f1 int);
create function check_con_function(check_con_tbl) returns bool as $$
begin
  raise notice 'input = %', row_to_json($1);
  return $1.f1 > 0;
end $$ language plpgsql immutable;
alter table check_con_tbl add check (check_con_function(check_con_tbl.*));
\d+ check_con_tbl
copy check_con_tbl from stdin;
1
\N
\.
copy check_con_tbl from stdin;
0
\.
select * from check_con_tbl;

-- test with RLS enabled.
CREATE ROLE regress_rls_copy_user;
CREATE ROLE regress_rls_copy_user_colperms;
CREATE TABLE rls_t1 (a int, b int, c int);

COPY rls_t1 (a, b, c) from stdin;
1	4	1
2	3	2
3	2	3
4	1	4
\.

CREATE POLICY p1 ON rls_t1 FOR SELECT USING (a % 2 = 0);
ALTER TABLE rls_t1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE rls_t1 FORCE ROW LEVEL SECURITY;

GRANT SELECT ON TABLE rls_t1 TO regress_rls_copy_user;
GRANT SELECT (a, b) ON TABLE rls_t1 TO regress_rls_copy_user_colperms;

-- all columns
COPY rls_t1 TO stdout;
COPY rls_t1 (a, b, c) TO stdout;

-- subset of columns
COPY rls_t1 (a) TO stdout;
COPY rls_t1 (a, b) TO stdout;

-- column reordering
COPY rls_t1 (b, a) TO stdout;

SET SESSION AUTHORIZATION regress_rls_copy_user;

-- all columns
COPY rls_t1 TO stdout;
COPY rls_t1 (a, b, c) TO stdout;

-- subset of columns
COPY rls_t1 (a) TO stdout;
COPY rls_t1 (a, b) TO stdout;

-- column reordering
COPY rls_t1 (b, a) TO stdout;

RESET SESSION AUTHORIZATION;

SET SESSION AUTHORIZATION regress_rls_copy_user_colperms;

-- attempt all columns (should fail)
COPY rls_t1 TO stdout;
COPY rls_t1 (a, b, c) TO stdout;

-- try to copy column with no privileges (should fail)
COPY rls_t1 (c) TO stdout;

-- subset of columns (should succeed)
COPY rls_t1 (a) TO stdout;
COPY rls_t1 (a, b) TO stdout;

RESET SESSION AUTHORIZATION;

-- test with INSTEAD OF INSERT trigger on a view
CREATE TABLE instead_of_insert_tbl(id serial, name text);
CREATE VIEW instead_of_insert_tbl_view AS SELECT ''::text AS str;

COPY instead_of_insert_tbl_view FROM stdin; -- fail
test1
\.

CREATE FUNCTION fun_instead_of_insert_tbl() RETURNS trigger AS $$
BEGIN
  INSERT INTO instead_of_insert_tbl (name) VALUES (NEW.str);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trig_instead_of_insert_tbl_view
  INSTEAD OF INSERT ON instead_of_insert_tbl_view
  FOR EACH ROW EXECUTE PROCEDURE fun_instead_of_insert_tbl();

COPY instead_of_insert_tbl_view FROM stdin;
test1
\.

SELECT * FROM instead_of_insert_tbl;

\c regression_ora
set transform_insert_to_copy TO on;
drop TABLE if exists ctv_data_20220713 cascade;
CREATE TABLE ctv_data_20220713 (v, h, c, i, d) AS
VALUES
   ('v1','h2','foo', 3, '2015-04-01'::date),
   ('v2','h1','bar', 3, '2015-01-02'),
   ('v1','h0','baz', NULL, '2015-07-12'),
   ('v0','h4','qux', 4, '2015-07-15'),
   ('v0','h4','dbl', -3, '2014-12-15'),
   ('v0',NULL,'qux', 5, '2014-07-15'),
   ('v1','h2','quux',7, '2015-04-04');

select * from ctv_data_20220713 order by 1,2;

drop TABLE if exists ctv_data_20220713 cascade;

create table t_copy_type_convert(v varchar(3)) distribute by shard(v);
copy t_copy_type_convert(v position(1:6) trim($1)) from stdin encoding 'UTF8';
qqqqqq
\.
copy t_copy_type_convert(v position(1:6) cast(nullif(trim($1),'') as varchar)) from stdin encoding 'UTF8';
qqqqqq
\.
drop table t_copy_type_convert;

\c regression
reset transform_insert_to_copy;

set transform_insert_to_copy to on;
drop table if exists t4;
create table t4(f1 int,f2 varchar,f3 int);
--expected error
insert into t4(f1,f2,f3) values(1,'xxxxx'),(2,'xxxxx');
insert into t4(f1,f2) values(1,'xxxxx'),(2,'xxxxx');
insert into t4 values(1,'xxxxx',1),(2,'xxxxx',2);
-- Additional support for partial column insert to copy
insert into t4 values(1,'xxxxx'),(2,'xxxxx');
select * from t4 order by 1,2,3;
drop table t4;

drop table if exists rules_src;
drop table if exists rules_log;
create table rules_src(f1 int, f2 int);
create table rules_log(f1 int, f2 int, tag text);
insert into rules_src values(1,2), (11,12);
-- No error is reported when the execution is successful.
create rule r1 as on update to rules_src do also
insert into rules_log values(old.*, 'old'), (new.*, 'new');
update rules_src set f2 = f2 + 1;
-- No error 'ERROR:  INSERT has more target columns than expressions'
insert into rules_log values(1,2), (11, to_number('14', '999'));
select * from rules_log order by 1,2,3;
drop table rules_src cascade;
drop table rules_log cascade;

-- The cast conversion supports the insert to copy statement.
SET datestyle = iso, ymd;
drop table if exists t_varchar;
create table t_varchar(a varchar(1), b varchar(30)) distribute by shard(a);
insert into t_varchar values('123'::varchar(1), '2023-01-02 11:31:22'::date);
insert into t_varchar values('123'::varchar(1), '2023-01-02 11:31:22'::date),('123'::varchar(1), '2023-01-02 11:31:22'::date);
insert into t_varchar values('456'::varchar(1), NULL::date);
insert into t_varchar values('456'::varchar(1), NULL::date),('456'::varchar(1), NULL::date);
select *, b is null from t_varchar order by 1,2;
drop table t_varchar;

-- prepare insert
drop table if exists t_insert_success;
CREATE TABLE t_insert_success (
    col1 integer,
    col2 integer,
    col3 char(3),
    col4 int,
    col5 date
) distribute by shard(col1);
delete from t_insert_success;
prepare p1_3(int)  as insert into t_insert_success  values(1,2,'a',1,'2011-01-01'),(2,2,2,3,'2020-11-01'),(3,3,3,3,'2022-11-01') ;
execute p1_3(1);
select * from t_insert_success order by 1,2,3,4,5;
select count(*) from t_insert_success;
drop table if exists t_insert_success;

-- plpgsql support tocopy
drop table if exists bug_test_pro_multi_values_shard_1 cascade;
create table bug_test_pro_multi_values_shard_1(f1 int default 1,f2 int default 1, f3 float default 0.9, f4 date default '2023-01-12', f5 timestamp default '2023-01-12 15:50:00.424278', f6 time default '15:50:00', f7 interval default '1 day', f8 varchar default '你好', f9 varchar(8000) default repeat('深圳', 10), f10 text default '南山', f11 serial);
drop procedure if exists bug_test_pro_multi_values_shard_1_insert();
CREATE OR REPLACE PROCEDURE bug_test_pro_multi_values_shard_1_insert() AS
$$
BEGIN
    delete from bug_test_pro_multi_values_shard_1;
    insert into bug_test_pro_multi_values_shard_1 values(3,3),(4,4);
    insert into bug_test_pro_multi_values_shard_1 values(5,5,default),(6,6,default),(default,default,default);
    insert into bug_test_pro_multi_values_shard_1 values(7,7,default),(8,8,default),(default,default,default);
    insert into bug_test_pro_multi_values_shard_1 values(default,default,default,default,default,default,default),(default,default,default,default,default,default,default),(default,default,default,default,default,default,default);
    exception
     when others then
		raise notice '%',SQLERRM;
end;
$$
language plpgsql;
truncate table bug_test_pro_multi_values_shard_1;
call bug_test_pro_multi_values_shard_1_insert();
select * from bug_test_pro_multi_values_shard_1 order by f11;
drop table bug_test_pro_multi_values_shard_1 cascade;
reset transform_insert_to_copy;
-- clean up
DROP TABLE forcetest;
DROP TABLE vistest;
DROP FUNCTION truncate_in_subxact();
DROP TABLE x, y;
DROP TABLE rls_t1 CASCADE;
DROP ROLE regress_rls_copy_user;
DROP ROLE regress_rls_copy_user_colperms;
DROP FUNCTION fn_x_before();
DROP FUNCTION fn_x_after();
DROP TABLE instead_of_insert_tbl;
DROP VIEW instead_of_insert_tbl_view;
DROP FUNCTION fun_instead_of_insert_tbl();

\c regression_ora
create table t_copy_type_convert(v varchar(3)) distribute by shard(v);
copy t_copy_type_convert(v position(1:6) trim($1)) from stdin encoding 'UTF8';
qqqqqq
\.
copy t_copy_type_convert(v position(1:6) cast(nullif(trim($1),'') as varchar)) from stdin encoding 'UTF8';
qqqqqq
\.
drop table t_copy_type_convert;

-- test copy position
drop table if exists t_copy;
create table t_copy(v varchar(3)) distribute by shard(v);
-- ok
copy t_copy(v position(1:3)) from stdin encoding 'UTF8';
qqqqqq
\.
select * from t_copy;
drop table t_copy;

-- copy with fixed
create table test_cp(f1 smallint, f2 integer, f3 bigint, f4 decimal, f5 numeric, f6 numeric(10), f7 numeric(10,5), f8 real ,f9 double precision, f10 money, f11 varchar(10), f12 char(15), f13 text, f14 timestamp without time zone, f15 boolean, f16 varchar(15));

copy test_cp from stdin delimiter ',';
1,2,3,4,5,12345.6789,12345.6789,12345.6789,12345.6789,12345.67,abc,ccccc,dddddd,2023-05-01 10:10:10,true,eeeeeeee
11,21,31,41,51,12345.6789,12345.6789,12345.6789,12345.6789,12345.67,abcabcabc,ccccccccccccccc,dddddd,2023-05-01 10:10:10,true,eeeeeeee
12,22,32,42,51,12345.6789,12345.6789,12345.6789,12345.6789,12345.67,dfedfedfe,ccc,dddddd,2023-05-01 10:10:10,true,eeeeeeee
13,23,33,43,51,12345.6789,12345.6789,12345.6789,12345.6789,12345.67,qwerqwerq,ccc,dddddd,2023-05-01 10:10:10,true,eeeeeeee
\.

copy test_cp to stdout csv formatter(f1(10));

copy test_cp to stdout binary formatter(f1(10));

copy test_cp to stdout fixed;

copy test_cp to stdout fixed formatter(f1(10));

copy test_cp to stdout fixed formatter(f1(5), f2(5), f3(5), f4(5), f5(6), f6(6), f7(6), f8(7), f9(7));


copy test_cp to stdout fixed formatter(f1(5), f2(5), f3(5), f4(5), f5(6), f6(6), f7(6), f8(7), f9(7), f13(2), f14(10), f15(1));

copy test_cp to stdout fixed formatter(f1(5), f2(5), f3(5), f4(5), f5(6), f6(6), f7(6), f8(7), f9(7), f10(8), f13(2), f14(10), f15(1));

copy test_cp to stdout fixed formatter with truncate (f1(5), f2(5), f3(5), f4(5), f5(6), f6(6), f7(6), f8(7), f9(7), f10(8), f13(2), f14(10), f15(1));

copy test_cp to stdout fixed formatter(f1(5), f2(5), f3(5), f4(5), f5(6), f6(6), f7 with truncate(6), f8(9), f9 with truncate(7), f10 with truncate(8), f13 with truncate(2), f14 with truncate(10), f15(2));

copy test_cp to stdout fixed formatter(f1(5), f2(5), f3(5), f4(5), f5(6), f6(6), f7 with truncate(6), f8 with truncate(7), f9 with truncate(7), f10 with truncate(8), f13 with truncate(2), f14 with truncate(10), f15(8)) header;

select * from test_cp order by 1,2,3,4;

copy test_cp to stdout fixed formatter(f1(5), f2(5), f3(5), f4(5), f5(6), f6(6), f7(15), f8(9), f9(10), f10(15), f13(12), f14(25), f15(8)) header;

copy (select f1, f11, f12, f14 from test_cp) to stdout fixed formatter(f1(5), f14(25)) header;
copy test_cp(f1, f11, f12, f14) from stdin fixed formatter(f1(5), f14(20)) header;
f1   f11       f12            f14
00011abcabcabc ccccccccccccccc02023-05-01 10:10:10
00001abc       ccccc          02023-05-01 10:10:10
00012dfedfedfe ccc            02023-05-01 10:10:10
00013qwerqwerq ccc            02023-05-01 10:10:10
\.
select * from test_cp order by 1,2,3,4;

copy (select f1, f8, f11, f12, f14 from test_cp) to stdout fixed formatter(f1(5), f8(10), f11(20), f14(25)) header;
copy test_cp(f1, f8, f11, f12, f14) from stdin fixed formatter(f1(5), f8(10), f11(20), f14(20)) header;
f1   f8        f11                 f12            f14
0001100012345.7abcabcabc           ccccccccccccccc02023-05-01 10:10:10
0001300012345.7qwerqwerq           ccc            02023-05-01 10:10:10
0001100000000\Nabcabcabc           ccccccccccccccc02023-05-01 10:10:10
0001300000000\Nqwerqwerq           ccc            02023-05-01 10:10:10
0000100012345.7abc                 ccccc          02023-05-01 10:10:10
0001200012345.7dfedfedfe           ccc            02023-05-01 10:10:10
0000100000000\Nabc                 ccccc          02023-05-01 10:10:10
0001200000000\Ndfedfedfe           ccc            02023-05-01 10:10:10
\.
select * from test_cp order by 1,2,3,4;

truncate test_cp;

copy test_cp(f1, f8, f11, f12, f14) from stdin fixed formatter(f1(5), f8(10), f11(20), f14(20), fff(10)) header;

copy test_cp(f1, f8, f11, f12, f14) from stdin fixed formatter(f1(5), f8(10), f11(20), f14(20), fff(10)) header IGNORE_EXTRA_DATA;
f1   f8        f11                 f12            f14                 fff
0001100012345.7abcabcabc           ccccccccccccccc02023-05-01 10:10:10aaa0      
0001300012345.7qwerqwerq           ccc            02023-05-01 10:10:10bbb0      
0001100000000\Nabcabcabc           ccccccccccccccc02023-05-01 10:10:10ccc       
\.
select * from test_cp order by 1,2,3,4;

truncate test_cp;
copy test_cp(f1, f8, f11, f12, f14) from stdin fixed formatter(f1(5), f8(10), f11(20), f14(20), fff(10)) header IGNORE_EXTRA_DATA;
f1   f8        f11                 f12            f14                 fff       
0001100012345.7abcabcabc           ccccccccccccccc02023-05-01 10:10:10aaa0      
0001300012345.7qwerqwerq           ccc            02023-05-01 10:10:10bbb0      
0001100000000\Nabcabcabc           ccccccccccccccc
\.
copy test_cp(f1, f8, f11, f12, f14) from stdin fixed formatter(f1(5), f8(10), f11(20), f14(20), fff(10)) header IGNORE_EXTRA_DATA FILL_MISSING_FIELDS;
f1   f8        f11                 f12            f14                 fff
0001100012345.7abcabcabc           ccccccccccccccc02023-05-01 10:10:10aaa0      
0001300012345.7qwerqwerq           ccc            02023-05-01 10:10:10bbb0      
0001100000000\Nabcabcabc           ccccccccccccccc
\.
select * from test_cp order by 1,2,3,4;

drop table t_copy_type_convert;

-----------------------------------------------
-- 定长字段：语法、基本功能
-----------------------------------------------
-- 先创建所需表
drop table if exists data_20230612_31 cascade ;
drop table if exists test_20230612_31 cascade ;
create table data_20230612_31(id int, f1 char(10), f2 varchar(10), f3 numeric(10,5));
create table test_20230612_31(id int, f1 char(10), f2 varchar(10), f3 numeric(10,5));

-- 语法不支持formatter在括号里面，所以下面语句报错
insert into data_20230612_31 values (1, 'f1', 'f2', 1);
copy data_20230612_31 to stdout with (FORMAT fixed, formatter(id(3)));
copy data_20230612_31 to stdout( delimiter '$',encoding utf8, format csv);
copy data_20230612_31 to stdout( delimiter '$',encoding utf8, format fixed, formatter(id(3), f1(10), f2(10), f3 with truncate (10)));

-- 在定长模式下，需要指定输出列的长度，默认采用列定义长度
truncate  table data_20230612_31;
insert into data_20230612_31 values (1, 'f1', 'f2', 1);
insert into data_20230612_31 values (2, 'f1');
copy data_20230612_31 to stdout;
copy data_20230612_31 to stdout fixed formatter(id(3)) delimiter '$';
copy data_20230612_31 to stdout fixed formatter(id(3), f1(10), f2(10), f3 with truncate (10)) delimiter '$';
copy data_20230612_31 to stdout with fixed formatter(id(3), f1(10), f2(10), f3 with truncate (10)) delimiter '$';

-- 在定长模式下，需要指定输出列的长度，默认采用列定义长度----导出
truncate  table data_20230612_31;
insert into data_20230612_31 values (1, 'f1', 'f2', 1);
copy data_20230612_31 to stdout;
copy data_20230612_31 to stdout with fixed formatter(id(3)) ;
truncate  table data_20230612_31;
insert into data_20230612_31 values (2, 'f1');
copy data_20230612_31 to stdout;
copy data_20230612_31 to stdout with fixed formatter(id(3)) ;

-- 定长字段，不指定长度，长度超长：不带with cut会报错，带with cut后按照定义的长度导入导出
truncate  table data_20230612_31;
insert into data_20230612_31 values (2, 'hij', 'hijklmn', 22222);
copy data_20230612_31 to stdout;
copy data_20230612_31 to stdout fixed formatter(id(3));
copy data_20230612_31 to stdout fixed formatter(id(3), f1(10), f2(10), f3 with truncate (10)) delimiter '$';
copy data_20230612_31 to stdout fixed formatter(id(3), f3 with truncate (10)) delimiter '$';

drop table if exists data_20230612_31 cascade ;
drop table if exists test_20230612_31 cascade ;

create table emptystr(c1 int, c2 text, c3 text);
set enable_lightweight_ora_syntax TO off;
COPY emptystr (c1, c2, c3) from stdin (format 'csv',delimiter ',', null '\N');
1,,aa
2,ab,bb
3,,cc
\.
select * from emptystr where c2 is null;

truncate emptystr;
set enable_lightweight_ora_syntax TO on;
COPY emptystr (c1, c2, c3) from stdin (format 'csv',delimiter ',', null '\N');
1,,aa
2,ab,bb
3,,cc
\.
select * from emptystr where c2 is null;

reset enable_lightweight_ora_syntax;
drop table emptystr;

create table emptystr_20250106(c1 int, c2 text, c3 text) distribute by replication;;
COPY emptystr_20250106 (c1, c2, c3) from stdin fixed formatter(c1(10), c2(10), c3(10));
1         aa        aaaaaaaaaa
2         ab        bb
3                   ccccccccccccc
\.
select * from emptystr_20250106;
drop table emptystr_20250106;

create table emptystr_20250106(c1 int, c2 text, c3 text) with(oids) distribute by replication;;
COPY emptystr_20250106 (c1, c2, c3) from stdin fixed oids formatter(oid(2), c1(8), c2(10), c3(10));
1 1       aa        aaaaaaaaaa
2 2       ab        bb
3 3                 ccccccccccccc
\.
\o copy_to_tmp.data
COPY emptystr_20250106 to stdout oids fixed formatter(c1(10), c2(10), c3(10)) DELIMITER '|';
\o
drop table emptystr_20250106;

create table tbquote(c1 int, c2 text, c3 text);
copy tbquote from stdin (format 'csv', delimiter '^C', encoding utf8, quote E'&', escape '\');
1^Cc^Ct1
2^Cd^C&t2
3^Ce^Ct3
4^Cf^Ct4
\.

copy tbquote from stdin (format 'csv', delimiter '^C', encoding utf8, quote E'\b', escape '\');
1^Cc^Ct1
2^Cd^C&t2
3^Ce^Ct3
4^Cf^Ct4
\.

select * from tbquote;
drop table tbquote;

-- reset conn
\c regression
-- cannot copy to table with foreign key or insertTrigger that could not be pushed down
set enable_datanode_row_triggers to on;
drop table if exists t_copy_tri_from_20220622_tb1;
create table t_copy_tri_from_20220622_tb1
(
f1 int,
id integer default 10 not null,
id1 int,
id2 date
);
drop sequence if exists s_copy_tri_20220622;
create sequence s_copy_tri_20220622;
CREATE or replace function tri_copy_tri_20220622_1()
returns trigger
as $$
begin
    IF new.f1 IS NULL THEN
     new.f1=NEXTVAL('s_copy_tri_20220622') ;
    END IF;
    return new;
END;
$$ language plpgsql;
drop trigger if exists  tri_copy_tri_20220622_1 on t_copy_tri_from_20220622_tb1;
CREATE TRIGGER tri_copy_tri_20220622_1
BEFORE INSERT ON t_copy_tri_from_20220622_tb1
FOR EACH ROW
execute procedure tri_copy_tri_20220622_1();
copy t_copy_tri_from_20220622_tb1(id,id1,id2) from stdin (format 'csv',delimiter ',', null '\N');;
123,123,2022-01-01
\.
select * from t_copy_tri_from_20220622_tb1 order by 1,2,3;
drop trigger tri_copy_tri_20220622_1 on t_copy_tri_from_20220622_tb1;
drop function tri_copy_tri_20220622_1;
drop sequence s_copy_tri_20220622;
drop table t_copy_tri_from_20220622_tb1;
reset enable_datanode_row_triggers;

set transform_insert_to_copy TO on;

CREATE TABLE test_insert_copy_20240322 (
    col1 integer
) distribute by shard(col1);

prepare p1_20240322 as insert into test_insert_copy_20240322 values(1),(2),(3); -- Fix: value list all consts, only insert a NULL row.
alter table test_insert_copy_20240322 alter column col1 set default NULL; -- after DDL, RevalidateCachedQuery will re-analysis and re-planning.
execute p1_20240322;
select distinct * from test_insert_copy_20240322 order by col1;


prepare p2_20240322 as insert into test_insert_copy_20240322 values(1),(2),($1); -- value list not all consts
alter table test_insert_copy_20240322 alter column col1 set default NULL;
execute p2_20240322(3);
select distinct * from test_insert_copy_20240322 order by col1;

deallocate p1_20240322;
deallocate p2_20240322;
drop table test_insert_copy_20240322;

reset transform_insert_to_copy;
