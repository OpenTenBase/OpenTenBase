------------------------------------------
-- Test cases for opentenbase_ora character type.
------------------------------------------

\c regression_ora

--------------------------
-- test char
--------------------------
create table char_table(c1 char(5), c2 char(5 char), c3 char(5 byte));

insert into char_table values('abc', 'abc', 'abc');
select * from char_table;
select c1, length(c1), c2, length(c2), c3, length(c3) from char_table;
select c1, lengthb(c1), c2, lengthb(c2), c3, lengthb(c3) from char_table;
select c1 || '1' as new_c1, c2 || '2' as new_c2, c3 || '3' as new_c3 from char_table;

-- ERROR: value too long
insert into char_table values('abcdefg', 'abc', 'abc');
insert into char_table values('abc', 'abcdefg', 'abc');
insert into char_table values('abc', 'abc', 'abcdefg');

drop table char_table;

-- char max length
create table char_table_length_max(c1 char(10485760));
insert into char_table_length_max values('abc');
select length(c1), lengthb(c1) from char_table_length_max;
drop table char_table_length_max;

-- ERROR: char length exceed
create table char_table_length_exceed(c1 char(10485761));

-- char default length is 1
create table char_table_default_length(c1 char);
insert into char_table_default_length values('a');
select c1, length(c1) from char_table_default_length;
insert into char_table_default_length values('ab');
drop table char_table_default_length;

--------------------------
-- test nchar
--------------------------
create table nchar_table(c1 char(5), c2 nchar(5));

insert into nchar_table values('abc', 'abc');
select * from nchar_table;
select c1, length(c1), c2, length(c2) from nchar_table;
select c1, lengthb(c1), c2, lengthb(c2) from nchar_table;
select c1 || '1' as new_c1, c2 || '2' as new_c2 from nchar_table;

-- ERROR: value too long
insert into nchar_table values('abc', 'abcdefg');

drop table nchar_table;

-- nchar max length
create table nchar_table_length_max(c1 nchar(10485760));
insert into nchar_table_length_max values('abc');
select length(c1), lengthb(c1) from nchar_table_length_max;
drop table nchar_table_length_max;

-- ERROR: nchar length exceed
create table nchar_table_length_exceed(c1 nchar(10485761));

-- nchar default length is 1
create table nchar_table_default_length(c1 nchar);
insert into nchar_table_default_length values('a');
select c1, length(c1) from nchar_table_default_length;
insert into nchar_table_default_length values('ab');
drop table nchar_table_default_length;

--------------------------
-- test varchar
--------------------------
create table varchar_table(c1 char(5), c2 varchar(5));

insert into varchar_table values('abc', 'abc');
select * from varchar_table;
select c1, length(c1), c2, length(c2) from varchar_table;
select c1, lengthb(c1), c2, lengthb(c2) from varchar_table;
select c1 || '1' as new_c1, c2 || '2' as new_c2 from varchar_table;

-- ERROR: value too long
insert into varchar_table values('abc', 'abcdefg');

drop table varchar_table;

-- varchar max length
create table varchar_table_length_max(c1 varchar(10485760));
insert into varchar_table_length_max values('abc');
select c1, length(c1), lengthb(c1) from varchar_table_length_max;
drop table varchar_table_length_max;

-- ERROR: varchar length exceed
create table varchar_table_length_exceed(c1 varchar(10485761));

-- varchar default length is 10485760
create table varchar_table_default_length(c1 varchar);
insert into varchar_table_default_length values('a');
select c1, length(c1) from varchar_table_default_length;
insert into varchar_table_default_length values('abcdefg');
select c1, length(c1) from varchar_table_default_length order by 1;
drop table varchar_table_default_length;

--------------------------
-- test varchar2
--------------------------
create table varchar2_table(c1 char(5), c2 varchar2(5));

insert into varchar2_table values('abc', 'abc');
select * from varchar2_table;
select c1, length(c1), c2, length(c2) from varchar2_table;
select c1, lengthb(c1), c2, lengthb(c2) from varchar2_table;
select c1 || '1' as new_c1, c2 || '2' as new_c2 from varchar2_table;

-- ERROR: value too long
insert into varchar2_table values('abc', 'abcdefg');

drop table varchar2_table;

-- varchar2 max length
create table varchar2_table_length_max(c1 varchar2(10485760));
insert into varchar2_table_length_max values('abc');
select c1, length(c1), lengthb(c1) from varchar2_table_length_max;
drop table varchar2_table_length_max;

-- ERROR: varchar2 length exceed
create table varchar2_table_length_exceed(c1 varchar2(10485761));

-- varchar2 default length is 10485760
create table varchar2_table_default_length(c1 varchar2);
insert into varchar2_table_default_length values('a');
select c1, length(c1) from varchar2_table_default_length;
insert into varchar2_table_default_length values('abcdefg');
select c1, length(c1) from varchar2_table_default_length order by 1;
drop table varchar2_table_default_length;

--------------------------
-- test nvarchar2
--------------------------
create table nvarchar2_table(c1 char(5), c2 nvarchar2(5));

insert into nvarchar2_table values('abc', 'abc');
select * from nvarchar2_table;
select c1, length(c1), c2, length(c2) from nvarchar2_table;
select c1, lengthb(c1), c2, lengthb(c2) from nvarchar2_table;
select c1 || '1' as new_c1, c2 || '2' as new_c2 from nvarchar2_table;

-- ERROR: value too long
insert into nvarchar2_table values('abc', 'abcdefg');

drop table nvarchar2_table;

-- nvarchar2 max length
create table nvarchar2_table_length_max(c1 nvarchar2(10485760));
insert into nvarchar2_table_length_max values('abc');
select c1, length(c1), lengthb(c1) from nvarchar2_table_length_max;
drop table nvarchar2_table_length_max;

-- ERROR: nvarchar2 length exceed
create table nvarchar2_table_length_exceed(c1 nvarchar2(10485761));

-- nvarchar2 default length is 10485760
create table nvarchar2_table_default_length(c1 nvarchar2);
insert into nvarchar2_table_default_length values('a');
select c1, length(c1) from nvarchar2_table_default_length;
insert into nvarchar2_table_default_length values('abcdefg');
select c1, length(c1) from nvarchar2_table_default_length order by 1;
drop table nvarchar2_table_default_length;
