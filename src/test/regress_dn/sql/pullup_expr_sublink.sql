\c regression_ora
create table tbl1_pullup_20240731(a int , b int);
create table tbl2_pullup_20240731(a int , b int);

create index on tbl1_pullup_20240731(a);
create index on tbl2_pullup_20240731(a);

set enable_hashjoin TO off;
set enable_mergejoin TO off;
set enable_material TO off;

-- A = 1 should be pushed down
explain (costs off, nodes off) select * from tbl1_pullup_20240731 t1
    where t1.a = (select a from tbl2_pullup_20240731 t2 where t2.a = t1.a) and t1.a = 1;

explain (costs off, nodes off) select * from tbl1_pullup_20240731 t1
    where t1.a = (select max(a) from tbl2_pullup_20240731 t2 where t2.a = t1.a) and t1.a = 1;

reset enable_hashjoin;
reset enable_mergejoin;
reset enable_material;

drop table tbl1_pullup_20240731;
drop table tbl2_pullup_20240731;

-- bug: https://tapd.woa.com/20385652/prong/stories/view/1020385652117781317
create table pullup_bug_t1_20240801(c1 int, c2 int);
create table pullup_bug_t2_20240801(c1 int, c2 int);
insert into pullup_bug_t1_20240801 values(1, 1);
insert into pullup_bug_t2_20240801 values(1, 1);

-- should return one row
select sum(c1) from pullup_bug_t2_20240801 where pullup_bug_t2_20240801.c2 > 100000 and pullup_bug_t2_20240801.c2 = 100001;
select c1 from pullup_bug_t2_20240801 where pullup_bug_t2_20240801.c2 > 100000 and pullup_bug_t2_20240801.c2 = 100001;
select * from pullup_bug_t1_20240801 where pullup_bug_t1_20240801.c1 = nvl((select sum(c1) from pullup_bug_t2_20240801 where pullup_bug_t2_20240801.c2 > 100000 and pullup_bug_t2_20240801.c2 = pullup_bug_t1_20240801.c2), 1);
select * from pullup_bug_t1_20240801 where pullup_bug_t1_20240801.c1 = nvl((select c1 from pullup_bug_t2_20240801 where pullup_bug_t2_20240801.c2 > 100000 and pullup_bug_t2_20240801.c2 = pullup_bug_t1_20240801.c2), 1);

-- test sublink has null results (not empty)
insert into pullup_bug_t1_20240801 values(NULL, 100001);
insert into pullup_bug_t2_20240801 values(NULL, 100001);
select sum(c1) from pullup_bug_t2_20240801 where pullup_bug_t2_20240801.c2 > 100000 and pullup_bug_t2_20240801.c2 = 100001;
select c1 from pullup_bug_t2_20240801 where pullup_bug_t2_20240801.c2 > 100000 and pullup_bug_t2_20240801.c2 = 100001;
select * from pullup_bug_t1_20240801 where pullup_bug_t1_20240801.c1 = nvl((select sum(c1) from pullup_bug_t2_20240801 where pullup_bug_t2_20240801.c2 > 100000 and pullup_bug_t2_20240801.c2 = pullup_bug_t1_20240801.c2), 1);
select * from pullup_bug_t1_20240801 where pullup_bug_t1_20240801.c1 = nvl((select c1 from pullup_bug_t2_20240801 where pullup_bug_t2_20240801.c2 > 100000 and pullup_bug_t2_20240801.c2 = pullup_bug_t1_20240801.c2), 1);

-- clear
drop table if exists pullup_bug_t1_20240801;
drop table if exists pullup_bug_t2_20240801;

create table pullup_casewhen_sublink1(id int,ENDO_TYPE varchar(20), ENDO_DTL_TYPE varchar(20));
insert into pullup_casewhen_sublink1 values( 1,'C01' ,'C103'); 

create table pullup_casewhen_sublink2(id int, CD_DETL_NO varchar(200),CD_DETL_ORGN_NAME1 varchar(500));
insert into pullup_casewhen_sublink2 values( 1 ,'C103','C01'); 

set pullup_target_casewhen_filter = 1;
explain (costs off, nodes off) select  CASE SUBSTR(ENDO_DTL_TYPE, 0, 1) WHEN 'C' THEN 
	(SELECT T.CD_DETL_ORGN_NAME1 FROM pullup_casewhen_sublink2 T WHERE  T.CD_DETL_NO = ENDO_DTL_TYPE)
	ELSE
		ENDO_TYPE
	END ENDO_TYPE
	,ENDO_TYPE       
	,ENDO_DTL_TYPE
	FROM pullup_casewhen_sublink1;

select  CASE SUBSTR(ENDO_DTL_TYPE, 0, 1) WHEN 'C' THEN 
	(SELECT T.CD_DETL_ORGN_NAME1 FROM pullup_casewhen_sublink2 T WHERE  T.CD_DETL_NO = ENDO_DTL_TYPE)
	ELSE
		ENDO_TYPE
	END ENDO_TYPE
	,ENDO_TYPE       
	,ENDO_DTL_TYPE
	FROM pullup_casewhen_sublink1;

set pullup_target_casewhen_filter = 2;
explain (costs off, nodes off) select  CASE SUBSTR(ENDO_DTL_TYPE, 0, 1) WHEN 'C' THEN 
	(SELECT T.CD_DETL_ORGN_NAME1 FROM pullup_casewhen_sublink2 T WHERE  T.CD_DETL_NO = ENDO_DTL_TYPE)
	ELSE
		ENDO_TYPE
	END ENDO_TYPE
	,ENDO_TYPE       
	,ENDO_DTL_TYPE
	FROM pullup_casewhen_sublink1;

select  CASE SUBSTR(ENDO_DTL_TYPE, 0, 1) WHEN 'C' THEN 
	(SELECT T.CD_DETL_ORGN_NAME1 FROM pullup_casewhen_sublink2 T WHERE  T.CD_DETL_NO = ENDO_DTL_TYPE)
	ELSE
		ENDO_TYPE
	END ENDO_TYPE
	,ENDO_TYPE       
	,ENDO_DTL_TYPE
	FROM pullup_casewhen_sublink1;

reset pullup_target_casewhen_filter;
drop table pullup_casewhen_sublink1;
drop table pullup_casewhen_sublink2;

drop table if exists test_casewhen_expr_select_shard_1 cascade;
create table test_casewhen_expr_select_shard_1 (id int, ename varchar, address text, age int, sal numeric, deptno smallint, mey money, dt date, ts timestamp);
insert into test_casewhen_expr_select_shard_1 select i, 'opentenbase'||i%212, case when i%111=0 then '北京' when i%112=0 then '深圳' when i%113=0 then '上海' else '广州' end, i%100, 0.998123*i, (i*1.2321)%256, i%599, date'2025-01-01'+i*interval '1 hour',timestamp'2025-01-01 00:00:00'+i*interval '1 min' from generate_series(1, 10000) i;

drop table if exists test_casewhen_expr_select_replication_1 cascade;
create table test_casewhen_expr_select_replication_1 (id int, ename varchar, address text, age int, sal numeric, deptno smallint, mey money, dt date, ts timestamp);
insert into test_casewhen_expr_select_replication_1 select i, 'opentenbase'||i%212, case when i%111=0 then '北京' when i%112=0 then '深圳' when i%113=0 then '上海' else '广州' end, i%100, 0.998123*i, (i*1.2321)%256, i%599, date'2025-01-01'+i*interval '1 hour',timestamp'2025-01-01 00:00:00'+i*interval '1 min' from generate_series(1, 8000) i;


set pullup_target_casewhen_filter = 1;
select dt,sal,case sum(sal) when sal then (select sal from test_casewhen_expr_select_replication_1 t2 where t1.dt=t2.dt limit 1) else sum(sal) end from test_casewhen_expr_select_shard_1 t1 where address='北京' and deptno=10 group by dt,sal;
set pullup_target_casewhen_filter = 2;
select dt,sal,case sum(sal) when sal then (select sal from test_casewhen_expr_select_replication_1 t2 where t1.dt=t2.dt limit 1) else sum(sal) end from test_casewhen_expr_select_shard_1 t1 where address='北京' and deptno=10 group by dt,sal;

reset pullup_target_casewhen_filter;
drop table if exists test_casewhen_expr_select_shard_1 cascade;
drop table if exists test_casewhen_expr_select_replication_1 cascade;
