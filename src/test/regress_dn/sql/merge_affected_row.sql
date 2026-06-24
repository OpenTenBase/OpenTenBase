---TAPD:
---test exec merge update the same row more than one time
---tapd:123732935
\c regression_ora
drop table if exists m_test;
drop table if exists s_test;
create table m_test(i int, j int, k int);
create table s_test(i int, j int, k int);
insert into m_test values (1,2,3);
insert into s_test values (1,2,3);
insert into s_test values (1,3,4);
-- expected success
begin;
merge into m_test using s_test on (m_test.i=s_test.i) when matched then update set j=s_test.j, k=s_test.k;
select * from m_test order by 1;
rollback;
-- expected error
begin;
insert into s_test values (1,3,4);
merge into m_test using s_test on (m_test.i=s_test.i) when matched then update set j=s_test.j, k=s_test.k;
rollback;
select * from m_test order by 1;
-- expected error
begin;
insert into s_test values (1,2,3);
merge into m_test using s_test on (m_test.i=s_test.i) when matched then update set j=s_test.j, k=s_test.k;
rollback;
select * from m_test order by 1;
drop table m_test;
drop table s_test;
