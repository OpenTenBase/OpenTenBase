/*
	Test Specification
	The file is mainly used to test constant type of opentenbase_ora.
	Including:
    1. q quoted string
	2. treat '' as null
*/

-- need to use opentenbase_ora mode to test these test case.
\c regression_ora

-- 1. q quoted string
-- 1.1 test q quoted string, should pass
select q'<what's your name?>' from dual;
select q'(what's your name?)' from dual;
select q'[what's your name?]' from dual;
select q'{what's your name?}' from dual;
select q'~what's your name?~' from dual;
select q'`what's your name?`' from dual;
select q'@what's your name?@' from dual;
select q'#what's your name?#' from dual;
select q'$what's your name?$' from dual;
select q'%what's your name?%' from dual;
select q'^what's your name?^' from dual;
select q'&what's your name?&' from dual;
select q'*what's your name?*' from dual;
select q'-what's your name?-' from dual;
select q'_what's your name?_' from dual;
select q'+what's your name?+' from dual;
select q'=what's your name?=' from dual;
select q'|what's your name?|' from dual;
select q'\what's your name?\' from dual;
select q'"what's your name?"' from dual;
select q':what's your name?:' from dual;
select q';what's your name?;' from dual;
select q'?what's your name??' from dual;
select q'/what's your name?/' from dual;
select q',what's your name?,' from dual;
select q'.what's your name?.' from dual;
select q''what's your name?'' from dual;
select q'!what's your name?!' from dual;
select q'Qwhat's your name?Q' from dual;
select q'awhat's your name?a' from dual;
select q'3what's your name?3' from dual;

select q'[~!@#$%^&*()__+<>?:"{{{}|}}]' from dual;
select q'[ï Ÿ1234 ï]' from dual;

select q'[what's your name?['my name is opentenbase_ora!]' from dual;
select q'(what's your name?('my name is opentenbase_ora!)' from dual;
select q'<what's your name?<'my name is opentenbase_ora!>' from dual;
select q'{what's your name?{'my name is opentenbase_ora!}' from dual;

select q'<>' from dual;
select q'()' from dual;
select q'[]' from dual;
select q'{}' from dual;
select q'~~' from dual;
select q'``' from dual;
select q'@@' from dual;
select q'##' from dual;
select q'$$' from dual;
select q'%%' from dual;
select q'^^' from dual;
select q'&&' from dual;
select q'**' from dual;
select q'--' from dual;
select q'__' from dual;
select q'++' from dual;
select q'==' from dual;
select q'||' from dual;
select q'\\' from dual;
select q'""' from dual;
select q'::' from dual;
select q';;' from dual;
select q'??' from dual;
select q'//' from dual;
select q',,' from dual;
select q'..' from dual;
select q'''' from dual;
select q'!!' from dual;
select q'QQ' from dual;
select q'aa' from dual;
select q'33' from dual;

SELECT q'['first line';
'next line';
'third line';]' AS "Three lines";

select q']what's your name?]' from dual;
select q'>what's your name?>' from dual;
select q'}what's your name?}' from dual;
select q')what's your name?)' from dual;

-- 1.2 noascii char or unsupported char as delimeter, should fail
select q'ï Ÿ1234 ï' from dual;
select q' Ÿ1234 ' from dual;

-- 1.3 q' string in dml
create table qtest(id int, qval varchar2(100));
insert into qtest values (1,q'[what's your name?]');
insert into qtest values (2,q'<q'<>');
insert into qtest values (3,q'{q'{}}');
delete from qtest where qval = q'<q'<>';
update qtest set qval = q'*q'***' where id=3;
select * from qtest;
drop table qtest;

-- 2.1 is null / is not null
drop table if exists null_test;
create table null_test (c1 char(10), c2 varchar(20), c3 varchar2(10), c4 number(2,0));
insert into null_test values('20230301','','',1);
insert into null_test values('','20230301','',2);
insert into null_test values('','','20230301',3);
insert into null_test values('20230301','20230301','20230301',4);
select * from null_test where c1 is null or c2 is null or c3 is null order by 1,2,3,4;
select * from null_test where c1 is not null and c2 is not null and c3 is not null;

-- 2.2 order by
select * from null_test order by 1, 2, 3, 4;

-- 2.3 '' as opertor l/r values
select '' + 1, '' - 1, '' * 1, '' / 1 from dual;
select * from null_test where '' >= 'a' or '' <= 'a';
select * from null_test where '' > 'a' or '' < 'a' or '' = 'a' or '' != 'a' or '' <> 'a';
select * from null_test where c3 >= 'a' or c3 <= 'a';
select * from null_test where c3 > 'a' or c3 < 'a' or c3 = 'a' or c3 != 'a';
drop table null_test;

-- 2.5 '' as partition key
drop table if exists list_partition;
drop type if exists week;
drop type if exists p_gender;

create type p_gender as enum ('male', 'female', '');
create type week as enum ('1','2','3','4','5','6','7');
create table list_partition(
id int,
name varchar,
gender p_gender,
birth_week week
)partition by list (gender);

create index list_index on list_partition(id);
create table part_men partition of list_partition for values in('male');
create table part_women partition of list_partition for values in('female');
create table part_null partition of list_partition for values in('') ;

insert into list_partition values
(1, 'zhang', 'male','1'),
(2, 'zhan', 'female','1'),
(3,'wang', 'male','3'),
(4,'li', '', '6'),
(5,'tian', 'female','4'),
(6,'su', '','7'),
(7,'liu', 'female','2'),
(8,'qin', '','3');

select * from part_men order by id;
select * from part_women order by id;
select * from part_null order by id;

drop table part_men;
drop table part_women;
drop table part_null;
drop table list_partition;
drop type week;
drop type p_gender;

--2.6 copy treat '' as null
drop table if exists t1;
create table t1(f1 number(2,0), f2 varchar(10), f3 varchar2(10), f4 text);
insert into t1 values(1, '', '', '');
insert into t1 values(2, null, null, null);
copy t1 from stdin;
3			
4	\N	\N	\N
\.
select length(f2), length(f3), length(f4), * from t1 order by f1;
drop table t1;

-- q quoto used in PLSQL
DO
$$
DECLARE
	a TEXT := q'#This is test#';
	b VARCHAR(20) := q'$It'a joke$';
	c TEXT;
BEGIN
	RAISE NOTICE '%', q'[It's an example]';
	RAISE NOTICE '%', a;
	RAISE NOTICE '%', b;
	c := a || b;
	RAISE NOTICE '%', c;
END;
$$;

CREATE OR REPLACE FUNCTION foo(a VARCHAR) RETURNS VARCHAR
AS
$$
BEGIN
	RETURN a || ' END!';
END;
$$ LANGUAGE default_plsql;

SELECT foo('abc');
SELECT foo(q'#abc#');
SELECT foo(q'#a'b'c#');

\c regression
