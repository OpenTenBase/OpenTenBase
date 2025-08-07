\c opentenbase_ora_package_function_regression_ora
-- Tests for the nchr(numeric)
drop table if exists nchr_test;
create table nchr_test (f1 int,f2 nvarchar2(20),f3 varchar2(20),f4 number);
insert into nchr_test select 33,nchr(33),nchr(33),33 from dual;
insert into nchr_test select 34,nchr(34),nchr(34),34 from dual;
select * from nchr_test order by f1;

select nchr(f1), nchr(f4) from nchr_test;
select ascii(nchr(f1)), ascii(nchr(f4)) from nchr_test;
select nchr(ascii(nchr(f1))), nchr(ascii(nchr(f4))) from nchr_test;
drop table nchr_test;
