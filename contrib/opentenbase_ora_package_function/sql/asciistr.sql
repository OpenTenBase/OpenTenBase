\c opentenbase_ora_package_function_regression_ora
set client_encoding = utf8;
set client_min_messages = 'warning';
create extension if not exists opentenbase_ora_package_function;
-- test asciistr common case
select asciistr('嗨！JIM') from dual;
select asciistr('环境治理第1要务！') from dual;
select asciistr('魑魅魍魉钆xyz😀123') from dual;
select asciistr('魑👦xyz😀123') from dual;
select asciistr('') from dual;
select asciistr(NULL) from dual;
-- test asciistr with system function
select asciistr(repeat('魑👦xyz😀123', 2)) from dual;
select asciistr(chr(72)) from dual;
select asciistr(to_char('')) from dual;
-- test asciistr with table colum
create table r_asciistr_20230207(id int, name text);
insert into r_asciistr_20230207 values (1, '汉字123abc！');
insert into r_asciistr_20230207 values (2, 'emoj👦xyz😀123？');
insert into r_asciistr_20230207 values (3, 'Just English.!?');
insert into r_asciistr_20230207 values (4, '');
insert into r_asciistr_20230207 values (5, NULL);
select asciistr(name) from r_asciistr_20230207 order by id;
-- clean up
drop table r_asciistr_20230207;
reset client_encoding;
reset client_min_messages;
