--User defined functions have several limitations 
--Basic insert, update, delete test on multiple datanodes using plpgsql function is passing.

--default distributed by HASH(slotname)

create function xl_nodename_from_id1(integer) returns name as $$
declare 
	n name;
BEGIN
	select node_name into n from pgxc_node where node_id = $1;
	RETURN n;
END;$$ language plpgsql;

create table xl_Pline1 (
    slotname	char(20),
    phonenumber	char(20),
    comment	text,
    backlink	char(20)
);

create unique index xl_Pline1_name on xl_Pline1 using btree (slotname bpchar_ops);

--insert Plines
insert into  xl_Pline1 values ('PL.001', '-0', 'Central call', 'PS.base.ta1');
insert into  xl_Pline1 values ('PL.002', '-101', '', 'PS.base.ta2');
insert into  xl_Pline1 values ('PL.003', '-102', '', 'PS.base.ta3');
insert into  xl_Pline1 values ('PL.004', '-103', '', 'PS.base.ta5');
insert into  xl_Pline1 values ('PL.005', '-104', '', 'PS.base.ta6');
insert into  xl_Pline1 values ('PL.006', '-106', '', 'PS.base.tb2');
insert into  xl_Pline1 values ('PL.007', '-108', '', 'PS.base.tb3');
insert into  xl_Pline1 values ('PL.008', '-109', '', 'PS.base.tb4');
insert into  xl_Pline1 values ('PL.009', '-121', '', 'PS.base.tb5');
insert into  xl_Pline1 values ('PL.010', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.011', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.012', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.013', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.014', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.015', '-134', '', 'PS.first.ta1');
insert into  xl_Pline1 values ('PL.016', '-137', '', 'PS.first.ta3');
insert into  xl_Pline1 values ('PL.017', '-139', '', 'PS.first.ta4');
insert into  xl_Pline1 values ('PL.018', '-362', '', 'PS.first.tb1');
insert into  xl_Pline1 values ('PL.019', '-363', '', 'PS.first.tb2');
insert into  xl_Pline1 values ('PL.020', '-364', '', 'PS.first.tb3');
insert into  xl_Pline1 values ('PL.021', '-365', '', 'PS.first.tb5');
insert into  xl_Pline1 values ('PL.022', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.023', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.024', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.025', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.026', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.027', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.028', '-501', 'Fax entrance', 'PS.base.ta2');
insert into  xl_Pline1 values ('PL.029', '-502', 'Fax first floor', 'PS.first.ta1');

create function xl_insert_Pline_test(int) returns boolean as $$
BEGIN
	IF $1 < 20 THEN
		insert into  xl_Pline1 values ('PL.030', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.031', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.032', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.033', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.034', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.035', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.036', '-367', '', 'PS.first.tb6');
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END;$$ language plpgsql;

select xl_insert_Pline_test(1);

select xl_nodename_from_id1(xc_node_id), * from xl_Pline1 order by slotname;


create function xl_update_Pline_test(int) returns boolean as $$
BEGIN
	IF $1 < 20 THEN
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.030';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.031';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.032';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.033';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.034';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.035';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.036';
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END;$$ language plpgsql;

select xl_update_Pline_test(1);

select xl_nodename_from_id1(xc_node_id), * from xl_Pline1 order by slotname;

create function xl_delete_Pline_test(int) returns boolean as $$
BEGIN
	IF $1 < 20 THEN
		delete from xl_Pline1 where slotname = 'PL.030';
		delete from xl_Pline1 where slotname = 'PL.031';
		delete from xl_Pline1 where slotname = 'PL.032';
		delete from xl_Pline1 where slotname = 'PL.033';
		delete from xl_Pline1 where slotname = 'PL.034';
		delete from xl_Pline1 where slotname = 'PL.035';
		delete from xl_Pline1 where slotname = 'PL.036';

		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END;$$ language plpgsql;

select xl_delete_Pline_test(1);

select xl_nodename_from_id1(xc_node_id), * from xl_Pline1 order by slotname;

drop table if exists test_func_replic_table;
create table test_func_replic_table(id int,  id2 int )DISTRIBUTE BY REPLICATION to GROUP default_group;
create or replace function user_defined_func1(v1 int, v2 int) returns int as
$$
begin
insert into test_func_replic_table values(v1, v2);
return v1;
end;
$$
LANGUAGE PLPGSQL;
drop table if exists test_func_table1;
create table test_func_table1 (id int, id2 int);
insert into test_func_table1 values(1, 2);
delete from test_func_replic_table;
select id2, user_defined_func1(id, id2) from test_func_table1 order by 2;
select id2, user_defined_func1(id, id2) from test_func_table1 order by 1;
select id2, user_defined_func1(id, id2) + user_defined_func1(id, id2),user_defined_func1(user_defined_func1(id, id2), id2) from test_func_table1;
select * from test_func_replic_table;
execute direct on (datanode_1) 'select * from test_func_replic_table';
--execute direct on (datanode_2) 'select * from test_func_replic_table';
drop table test_func_table1;

--test execute direct 
--execute direct on (coord1) '';
--execute direct on (coord2) '';
execute direct on (datanode_1) '';
--execute direct on (datanode_2) '';

\c regression_ora
drop sequence  if exists LOG_ID;
create sequence LOG_ID;
drop function if exists get_log_id;
create or replace function get_log_id return number is
  Result number;
begin
  select LOG_ID.nextval into Result from dual;
  return(Result);
end get_log_id;
/
-- TestPoint : group by
drop table if exists func_sort_table;
create table func_sort_table (id int);
insert into func_sort_table values(1);
select  get_log_id from func_sort_table group by get_log_id;
select  get_log_id,count(id) from func_sort_table group by get_log_id;
-- TestPoint : distinct
select distinct get_log_id,id from func_sort_table ;
select distinct id,get_log_id from func_sort_table ;
drop table func_sort_table;
drop sequence LOG_ID;

create table func_order_table (total_terms varchar(8));
create table func_order_table1(v int, empno float,  ename varchar(100));
insert into func_order_table select i from generate_series(1,3) i;
drop function if exists Get_Base_Rate(Pi_Lnircd IN VARCHAR2);
CREATE OR REPLACE FUNCTION Get_Base_Rate(Pi_Lnircd IN VARCHAR2) RETURNs NUMBER as
      Po_Base_Rate NUMBER;
   BEGIN
select empno into Po_Base_Rate from func_order_table1 where ename=Pi_Lnircd;
      RETURN Po_Base_Rate;
   EXCEPTION
      WHEN OTHERS THEN
         RETURN NULL;
   END ;
/
select get_base_rate('SCOTT'),total_terms,get_base_rate('SCOTT') from func_order_table order by 2;
drop table if exists func_order_table;
drop table if exists func_order_table1;
drop function if exists Get_Base_Rate(Pi_Lnircd IN VARCHAR2);


create or replace function timestamp2date(timest numeric) returns timestamp without time zone
language default_plsql
as $function$
declare
result timestamp(0);
begin
result := to_date('1970-01-01','yyyy-mm-dd');
return result;
end
$function$
;

drop table if exists func_sort_table;
create table func_sort_table(a number);
insert into func_sort_table values(100000),(2000000);
select distinct timestamp2date(a) from func_sort_table;
drop table func_sort_table;

create function cte_func_table(p1 varchar) return varchar is
v_sql varchar;
begin
v_sql := 'create table test_func_table3(id int)';
execute v_sql;
return 'create table ok';
end;
/
drop table test_func_table3;
drop function cte_func_table;
drop table test_func_table2;

\c regression

drop table xl_Pline1;
drop function xl_nodename_from_id1(integer);

drop function xl_insert_Pline_test(int);
drop function xl_update_Pline_test(int);
drop function xl_delete_Pline_test(int);

--test execute direct
--execute direct on (coord1) '';
--execute direct on (coord2) '';
execute direct on (datanode_1) '';
--execute direct on (datanode_2) '';
create table udf_subtrans_MJ_t1(id int, f1 int);
insert into udf_subtrans_MJ_t1 select t,t from generate_series(1,10)t;
create table udf_subtrans_MJ_t2(id int, f1 int);
insert into udf_subtrans_MJ_t2 select t,t from generate_series(1,10)t;
create or replace function fun1() returns int
as $$
begin
    --DML inside a udf shouldn't be seen by later ExecRemoteSubplan
    delete from udf_subtrans_MJ_t1;
    return 0;
exception --exception to force subtrans
    when others then
        return -1;
end;
$$ language plpgsql;
set enable_nestloop = off;
set enable_hashjoin = off;
select * from (select id,fun1() from udf_subtrans_MJ_t2 order by id) x
    join udf_subtrans_MJ_t1 using(id) order by id;
reset enable_nestloop;
reset enable_hashjoin;
drop table udf_subtrans_MJ_t1;
drop table udf_subtrans_MJ_t2;
drop function fun1 cascade;

-- allow insert with udf function
drop table if exists revalidate_bug;
create or replace function inverse_20220714(int) returns float8 as
$$
begin
  analyze revalidate_bug;
  return 1::float8/$1;
exception
  when division_by_zero then return 0;
end$$ language plpgsql volatile;
create table revalidate_bug (c float8 unique);
explain (costs off,nodes off,buffers off,summary off, buffers off) 
insert into revalidate_bug values (inverse_20220714(0));
insert into revalidate_bug values (inverse_20220714(0));
select * from revalidate_bug;
drop table revalidate_bug;
drop function inverse_20220714(int);

-- prevent the udf function as the default value from missing the detection and causing data correctness problems
set enable_fast_query_shipping to on;
drop table if exists t1;
drop function if exists f1;
create function f1() returns int as
$$
declare
    v_int int;
begin
    select count(1) into v_int from t1;
        return v_int;
end;
$$
language plpgsql not pushdown;

create table t1(f1 int,f2 bigint default f1());
insert into t1 values(1);
insert into t1 values(1, 2);
select * from t1 order by 1,2;
drop table t1;
drop function f1;
reset enable_fast_query_shipping;
