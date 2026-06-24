\c regression_ora

\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;
--
-- blob compare function
--
select compare('麓轹麈痨喙q耜腹额钐邡'::blob, '麓轹麈耜腹额钐邡痨喙q噤耜腹额钐邡维醑点'::blob, 7, 1, 1) from dual;
select compare('麓轹麈痨喙q'::blob, '麓轹麈痨喙q噤耜腹额钐邡维醑点'::blob, 7, 1, 1) from dual;
select compare('麓轹麈痨喙q'::blob, '噤耜腹额钐邡维醑点麓轹麈痨喙q'::blob, 6, 1, 10) from dual;
select compare('麓轹麈痨喙啊qqqqqqq'::blob, '麓轹麈痨喙啊'::blob, 7, 1, 1) from dual;
select compare('x64TcNBa3h'::blob, 'uwVybxZqaP'::blob, 41, 95, 49) from dual;
select compare('Wt80pDnlkP'::blob, 'I3QCzZ8Vki'::blob, 9, 6, 8) from dual;
select compare('身掬惹税伙松组哭难蘸'::blob, '即诣耨埭请魇'::blob, 10, 10, 8) from dual;
select compare('麓轹麈痨喙'::blob, '噤耜腹额钐邡维醑点'::blob, 2, 2, 6) from dual;
select compare('麓轹aa麈痨喙啊'::blob, '麓轹a麈痨喙a'::blob, 5, 4, 3) from dual;
select compare('Wt80pDnlkP'::blob, 'I3QCzZ8DnlkP'::blob, 9, 6, 8) from dual;
select compare('x64TcNBa3h'::blob, 'uwVybxZqaP'::blob, 41, 95, 49) from dual;
select compare('身掬惹税伙松组哭难蘸'::blob, '即诣耨埭请魇'::blob, 10, 10, 8) from dual;
select compare('麓轹麈痨喙'::blob, '噤耜腹额钐邡维醑点'::blob, 2, 2, 6) from dual;
--
-- clob compare function
--
select compare('麓轹麈痨喙q耜腹额钐邡'::clob, '麓轹麈耜腹额钐邡痨喙q噤耜腹额钐邡维醑点'::clob, 7, 1, 1) from dual;
select compare('麓轹麈痨喙q'::clob, '麓轹麈痨喙q噤耜腹额钐邡维醑点'::clob, 7, 1, 1) from dual;
select compare('麓轹麈痨喙q'::clob, '噤耜腹额钐邡维醑点麓轹麈痨喙q'::clob, 6, 1, 10) from dual;
select compare('麓轹麈痨喙啊qqqqqqq'::clob, '麓轹麈痨喙啊'::clob, 7, 1, 1) from dual;
select compare('x64TcNBa3h'::clob, 'uwVybxZqaP'::clob, 41, 95, 49) from dual;
select compare('Wt80pDnlkP'::clob, 'I3QCzZ8Vki'::clob, 9, 6, 8) from dual;
select compare('身掬惹税伙松组哭难蘸'::clob, '即诣耨埭请魇'::clob, 10, 10, 8) from dual;
select compare('麓轹麈痨喙'::clob, '噤耜腹额钐邡维醑点'::clob::clob, 2, 2, 6) from dual;
select compare('麓轹aa麈痨喙啊'::clob, '麓轹a麈痨喙a'::clob, 5, 4, 3) from dual;
select compare('Wt80pDnlkP'::clob, 'I3QCzZ8DnlkP'::clob, 9, 6, 8) from dual;
select compare('x64TcNBa3h'::clob, 'uwVybxZqaP'::clob, 41, 95, 49) from dual;
select compare('身掬惹税伙松组哭难蘸'::clob, '即诣耨埭请魇'::clob, 10, 10, 8) from dual;
select compare('麓轹麈痨喙'::clob, '噤耜腹额钐邡维醑点'::clob, 2, 2, 6) from dual;

RESET client_min_messages;
RESET datestyle;
RESET client_encoding;

create table test_blob(f1 int,f2 blob);
insert into test_blob values (1,'test_opentenbase_ora');
insert into test_blob values (2,'test_opentenbase_ora'),(3,'test_opentenbase_ora');
select * from test_blob order by f1;
delete from test_blob where f1='1';
select * from test_blob order by f1;
update test_blob set f2='test_opentenbase_update' where f1='2';
select * from test_blob order by f1;
select * from test_blob order by f2 desc;
select * from test_blob order by f2 asc;
select * from test_blob where f2>'opentenbase' order by f1;
select * from test_blob where f2<'opentenbase' order by f1;
select * from test_blob where f2<='opentenbase' order by f1;
select * from test_blob where f2>='opentenbase' order by f1;
insert into test_blob values (5,empty_blob());
select * from test_blob where f2 is null order by f1;
select * from test_blob where f2=empty_blob();
create table test_blob1(f1 int, f2 raw(10), f3 long raw, f4 clob);
insert into test_blob1 values(1, empty_blob(), empty_blob(), empty_blob());
create table test_blob2(f1 int, f2 long);
insert into test_blob2 values(1, empty_blob());
create table test_blob3(f1 int, f2 nclob);
insert into test_blob3 values(1, empty_blob());
create table test_blob4(f1 int, f2 bfile);
insert into test_blob4 values(1, empty_blob());

create table test_clob(f1 int,f2 clob);
insert into test_clob values (1,'test_opentenbase_ora');
insert into test_clob values (2,'test_opentenbase_ora'),(3,'test_opentenbase_ora');
select * from test_clob order by f1;
delete from test_clob where f1='1';
select * from test_clob order by f1;
update test_clob set f2='test_opentenbase_update' where f1='2';
select * from test_clob order by f1;
select * from test_clob   order by f2 desc;
select * from test_clob   order by f2 asc;
select * from test_clob where f2>'opentenbase' order by f1;
select * from test_clob where f2<'opentenbase' order by f1;
select * from test_clob where f2<='opentenbase' order by f1;
select * from test_clob where f2>='opentenbase' order by f1;
insert into test_clob values (5,empty_clob());
select * from test_clob where f2 is null order by f1;
select to_char(f2) from test_clob order by f1;
select to_clob(f2) from test_clob order by f1;

create table test_clob1(f1 int,f2 blob);
insert into test_clob1 values(1, empty_clob());
create table test_clob2(f1 int,f2 raw(10));
insert into test_clob2 values(1, empty_clob());
create table test_clob3(f1 int,f2 bfile);
insert into test_clob3 values(1, empty_clob());

drop table test_clob;
drop table test_clob1;
drop table test_clob2;
drop table test_clob3;

drop table test_blob;
drop table test_blob1;
drop table test_blob2;
drop table test_blob3;
drop table test_blob4;

-- test for ID875274667
create table t_blob(id int, info blob);
insert into t_blob values (1,'[B@57e8b764'::varchar);
insert into t_blob values (2,'[B@57e8b764'::varchar2);
insert into t_blob values (3,'[B@57e8b764'::char(20));
insert into t_blob values (4, to_char('[B@57e8b764'));
insert into t_blob values (5,'[B@57e8b764');
select * from t_blob order by id;
drop table t_blob;
