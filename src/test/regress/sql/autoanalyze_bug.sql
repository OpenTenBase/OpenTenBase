drop table if exists col_test_trans_par_range_20230104_1 cascade;
create table col_test_trans_par_range_20230104_1(names varchar default 'JAMES', age smallint default 18, school varchar default '北京大学', city char(10) default '深圳', address text default '南山', job varchar default '程序员', sal NUMERIC(7,2) default -1, comm  NUMERIC(7,2) default -99, DEPTNO NUMERIC(2) default -1, HIREDATE DATE default '2022-08-06', runtime interval default '1 hour', sleeptime time default '23:59:59', bigobject varchar(8000) default '大对象存储',id int default 10) partition by range(HIREDATE) distribute by shard(id);
create index on col_test_trans_par_range_20230104_1(HIREDATE);

create table col_test_trans_par_range_20230104_1_part_1 partition of col_test_trans_par_range_20230104_1 for values from (minvalue) to ('2023-01-06 17:00:00');
create table col_test_trans_par_range_20230104_1_part_2 partition of col_test_trans_par_range_20230104_1 for values from ('2023-01-06 17:00:00') to ('2023-08-06 17:00:00');
create table col_test_trans_par_range_20230104_1_part_3 partition of col_test_trans_par_range_20230104_1 for values from ('2023-08-06 17:00:00') to ('2024-04-06 17:00:00') ;
create table col_test_trans_par_range_20230104_1_part_4 partition of col_test_trans_par_range_20230104_1 for values from ('2024-04-06 17:00:00') to ('2024-11-06 17:00:00');
create table col_test_trans_par_range_20230104_1_part_5 partition of col_test_trans_par_range_20230104_1 default;

drop procedure if exists col_insert_pro_trans_tbl(num int, tblname varchar) cascade;
create or replace procedure col_insert_pro_trans_tbl(num int, tblname varchar)
as
$$
declare
    vsql varchar;
begin
    vsql := 'insert into '||tblname||' select (array[''SMITH'',''ALLEN'',''WARD'',''JONES'',''MARTIN'',''BLAKE'',''CLARK'',''SCOTT'',''JAMES'', ''SNOW'',''''])[((i%11)+1)],i%99, (array[''清华大学'',''北京大学'',''南京大学'',''中山大学'', ''深圳大学'', ''华南理工大学'', ''湖南大学'',null,''华中科技大学''])[((i%9)+1)], (array[''北京'',''上海'',''深圳'',''广州'', ''重庆'', ''成都'', ''南京'',null,''杭州'',''惠州''])[((i%10)+1)],md5((i%1789)::text),(array[''CLERK'',''SALESMAN'',''MANAGER'',''ANALYST'', ''程序员'', ''自动化工程师'', ''HALY'',null,''投资咨询师'',''心里咨询师''])[((i%10)+1)],(i*1278.956)%20000,(i*256)%3000,(array[20,30,10,40,NULL,50,60,70,80,90,1,2,3,4,5,6,7,8,9,10])[((i%20)+1)],date ''2022-08-06''+(i%1460) * interval ''1 day'',(i%7980)*interval ''1 hour'',time ''17:16:16''+(i%5000)*interval ''1 min'',repeat(''大对象存储 ''||(i%7650),(i%100)+1),i from generate_series(1,'||num||') i';
    -- raise notice '%',vsql;
    execute vsql;
    exception
        when others then
            raise exception '%',SQLERRM;
end;
$$
language plpgsql;

call col_insert_pro_trans_tbl(50000, 'col_test_trans_par_range_20230104_1');

drop table if exists col_transaction_1_1 cascade;
create table col_transaction_1_1 distribute by shard(id) as select * from col_test_trans_par_range_20230104_1;
