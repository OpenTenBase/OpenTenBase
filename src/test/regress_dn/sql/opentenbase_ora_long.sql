------------------------------------------
-- Test cases for opentenbase_ora long type.
------------------------------------------

\c regression_ora

--------------------------
-- test long
--------------------------
drop table if exists t1;
drop table if exists t2;
create table t1(v int, w long, f long);
create table t1 (v int, w int, f long);
alter table t1 add column k long;
alter table t1 alter column w type long;
alter table t1 alter column f set default 'test';
--enable in 19c
create table t2 (v int, w long default 'test');
drop table t2;
--enable in 19c
create table t2 (v int, w long not null);
select * from t1 where f = 'test';
create index longidx on t1(f);
select * from t1 where f ~ 'test';
select * from t1 group by f;
select * from t1 order by f;
select distinct f from t1;
create materialized view mview as select v, f from t1;
drop table if exists t3;
create table t3 as select v, f from t1;
insert into t2 (select v, f from t1);
drop table t1;
drop table t2;

create table test_long(f1 int, f2 long);
insert into test_long values(1,'long1');
select f2,lpad(f2,10,'long') from test_long;
select f2,rpad(f2,10,'long') from test_long;
select f2,trim(f2||' ') from test_long;
select f2||'opentenbase' from test_long;
drop table test_long;
CREATE TABLE test_long
   (f1    NUMBER  CONSTRAINT test_long_c1
              CHECK (f1 BETWEEN 10 AND 99),
    f2    long  CONSTRAINT test_long_c2
              CHECK (f2 is not null));
drop table test_long;

CREATE OR REPLACE FUNCTION test_long_fun(acc_no IN INTEGER) 
   RETURNS INTEGER AS $$
   DECLARE
      acc_bal long := 'test_long';
   BEGIN
      RAISE NOTICE '%', acc_bal;
      RETURN acc_no;
   END;
$$ LANGUAGE default_plsql;
select test_long_fun(123);
drop function test_long_fun(acc_no IN NUMBER);

CREATE or replace PROCEDURE test_long_pro() AS $$
    DECLARE
        tot_emps long;
    BEGIN
        tot_emps := 'test_long';
        raise notice '%',tot_emps;
    END;
$$ LANGUAGE default_plsql;
call test_long_pro();
drop procedure test_long_pro;

-- test alias
drop table IF EXISTS test_long;
create table test_long(f1 int,f2 long,f3 int);
insert into test_long values(1,'long1',1);
insert into test_long values(2,'long2',2),(3,'long3',3);
insert into test_long (f1,f2) values(4,(select f2 from test_long order by f1 limit 1));
select * from test_long order by 1;
update test_long set f2='long11' where f1=2;
update test_long set(f3,f2)=((1*2)::integer,'long10');
update test_long set test_long.f2='long12' where f1=2;
delete from test_long where f1=2;
create table test_long1(f1 int,f2 long);
insert into test_long1 values(1,'long');
delete from test_long1 using test_long where test_long.f1=test_long1.f1 returning *;
delete from test_long where f1 = 1 returning *;
drop table test_long;
drop table test_long1;
