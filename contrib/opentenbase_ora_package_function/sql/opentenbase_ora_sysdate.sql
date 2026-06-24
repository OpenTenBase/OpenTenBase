\c opentenbase_ora_package_function_regression_ora
set TimeZone to 'PRC';
Drop table if exists t_time_range;
create table t_time_range
(f1 bigint, f2 timestamp ,f3 bigint) 
partition by range (f2) begin (timestamp without time zone '2021-06-01 0:0:0')     
step (interval '10 year') 
partitions (12) distribute by shard(f1) 
to group default_group;
explain verbose select * from t_time_range where f2 < sysdate;

prepare fun3 as select * from t_time_range where f2 < sysdate;
explain verbose execute fun3;
Drop table t_time_range;

declare
begin
execute 'select sysdate, sysdate from dual;';
execute 'select sysdate, sysdate from dual;';
execute 'select sysdate, sysdate from dual;';
end;
/
reset TimeZone;

select chr('67'::varchar) from dual;
select chr('67'::text) from dual;
select chr(SUBSTR('67125',1,2)) from dual;
