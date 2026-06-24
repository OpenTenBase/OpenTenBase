
\c regression_ora

-- test regexp_count(numeric, numeric)
select regexp_count(123423423466722341234, 123) from dual;
select regexp_count('123423423466722341234', '123') from dual;

select regexp_count(123423423466722341234, 123, 3) from dual;
select regexp_count('123423423466722341234', '123', 3) from dual;

select regexp_count(123423423466722341234, 123, 3, 'x') from dual;
select regexp_count('123423423466722341234', '123', 3, 'x') from dual;

-- test regexp_count(text, numeric)
select regexp_count('12341234', 34) from dual;
select regexp_count('12341234', 34, 3) from dual;
select regexp_count('12341234', 34, 4) from dual;
select regexp_count('12341234', 34, 1, 'x') from dual;

-- test regexp_count(numeric, text)
select regexp_count(12341234, '34') from dual;
select regexp_count(12341234, '34', 3) from dual;
select regexp_count(12341234, '34', 4) from dual;
select regexp_count(12341234, '34', 1, 'x') from dual;

create table regexp_tbl (a number, b text, c numeric, d text);
insert into regexp_tbl values(1234512378123, '1234512378123', 123, '123');
insert into regexp_tbl values(1237812378123, '1237812378123', 78, '78');
insert into regexp_tbl values(12256223234123, '12256223234123', 23, '23');

select regexp_count(a, c) from regexp_tbl;
select regexp_count(a, c, 3) from regexp_tbl;
select regexp_count(a, c, 3, 'x') from regexp_tbl;

select regexp_count(a, d) from regexp_tbl;
select regexp_count(a, d, 3) from regexp_tbl;
select regexp_count(a, d, 3, 'x') from regexp_tbl;

select regexp_count(b, c) from regexp_tbl;
select regexp_count(b, c, 3) from regexp_tbl;
select regexp_count(b, c, 3, 'x') from regexp_tbl;

drop table regexp_tbl;

-- BEGIN: tapd 119561653 
drop table if exists regexp_count_tb1_toast ;
create table regexp_count_tb1_toast(id int,id1 text);
insert into regexp_count_tb1_toast values(3,'adbadrXXbcdfgdiec,ncdn');
insert into regexp_count_tb1_toast values(1,'adbadrabmmcdfBBBbb,ncdn'||repeat('bx-',300000));
insert into regexp_count_tb1_toast values(2,'cdx');
select id,regexp_count(id1,'b',1) from regexp_count_tb1_toast order by 1;
drop table if exists regexp_count_tb1_toast ;
-- END: 119561653
