\c regression_ora

-- 1020421696882003165
-- test column name like opentenbase_ora
set enable_opentenbase_ora_column_name to on;
create table t_opentenbase_ora_colname_20230220("Id " int, "I D" int, num int[]);
insert into t_opentenbase_ora_colname_20230220 values (1, 2, array[1,2,3]), (2, 3, array[1,2,3]), (3, 4, array[1,2,3]);
-- test column name of node T_ColumnRef
select "Id " as id1, "I D" from t_opentenbase_ora_colname_20230220  order by "Id ";
-- test column name of node T_A_Indirection (opentenbase_ora doesn't support)
select num[2] from t_opentenbase_ora_colname_20230220  order by "Id ";
-- test column name of node T_FuncCall
create or replace function f_opentenbase_ora_colname_20230220(id int)
returns int AS
begin
return id;
end;
/
select f_opentenbase_ora_colname_20230220(num[1]) from t_opentenbase_ora_colname_20230220 order by "Id ";
select sum("Id "), avg("I D") from t_opentenbase_ora_colname_20230220;
-- test column name of node T_A_Expr
select sum("Id ") + 3, avg("I D") / 5 from t_opentenbase_ora_colname_20230220;
select 1 + 1, 'a,' || 'b''' || 'c from' from dual;
-- test column name of node T_TypeCast (opentenbase_ora doesn't support)
select "Id "::text, "I D"::varchar(2) from t_opentenbase_ora_colname_20230220 order by "Id ";
-- test column name of node T_collateClause (opentenbase_ora doesn't support)
select "Id "::text collate "C" from t_opentenbase_ora_colname_20230220 order by "Id ";
-- test column name of node T_GroupingFunc
select grouping("Id ") from t_opentenbase_ora_colname_20230220 group by "Id ";
select grouping_id("I D") from t_opentenbase_ora_colname_20230220 group by "I D";
-- test column name of node T_SubLink (opentenbase_ora doesn't support exists, array, all, any)
select exists(select "Id " from t_opentenbase_ora_colname_20230220 order by "Id ") from t_opentenbase_ora_colname_20230220;
select array(select "I D" from t_opentenbase_ora_colname_20230220 order by "I D") from t_opentenbase_ora_colname_20230220;
select (select "I D" from t_opentenbase_ora_colname_20230220 order by "I D" limit 1) from dual;
select "Id " = all(select "Id " from t_opentenbase_ora_colname_20230220 order by "Id ") from t_opentenbase_ora_colname_20230220;
select "I D" > any(select "I D" from t_opentenbase_ora_colname_20230220 order by "I D") from t_opentenbase_ora_colname_20230220 order by "I D";
-- test column name of node T_CaseExpr
select case when "Id " = 1 then 1 else 0 end from t_opentenbase_ora_colname_20230220 order by "Id ";
-- test column name of node T_A_ArrayExpr (opentenbase_ora doesn't support)
select array['1', '2', '3'] from t_opentenbase_ora_colname_20230220;
-- test column name of node T_RowExpr (opentenbase_ora doesn't support)
select row(1, 2, '3') from t_opentenbase_ora_colname_20230220;
-- test column name of node T_CoalesceExpr
select coalesce(NULL, NULL, 3, 4, 5) FROM dual;
-- test column name of node T_MinMaxExpr
select greatest(1, 2, 3, 4) from dual;
select least(1, 2, 3, 4) from dual;
-- test column name of node T_SQLValueFunction
select current_date from dual where 1=0;
select current_timestamp from dual where 1=0;
select current_timestamp(1) from dual where 1=0;
select localtimestamp from dual where 1=0;
select localtimestamp(1) from dual where 1=0;
select uid from dual where 1=0;
-- opentenbase_ora doesn't support the following T_SQLValueFunction
select current_time from dual where 1=0;
select current_time(1) from dual where 1=0;
select localtime from dual where 1=0;
select localtime(1) from dual where 1=0;
select current_role from dual where 1=0;
select current_user from dual where 1=0;
select user from dual where 1=0;
select session_user from dual where 1=0;
select current_catalog from dual where 1=0;
select current_schema from dual where 1=0;
-- test column name of node T_XmlExpr (opentenbase_ora doesn't support is document)
SELECT xmlcomment('hello') from dual;
SELECT xmlconcat(xmlcomment('hello'), xmlcomment('world')) from dual;
SELECT xmlforest('abc' AS "FOO", 123 AS "BAR") from dual;
SELECT xmlparse(content '<abc>x</abc>') from dual;
SELECT xmlpi(name foo) from dual;
SELECT xmlroot( XMLType('<poid>143598</poid>'), VERSION '1.0', STANDALONE YES) from dual;
SELECT xmlserialize(CONTENT xmlconcat(xmlcomment('hello'), xmlcomment('world')) as varchar2(30)) from dual;
SELECT xmlforest('abc' AS foo, 123 AS bar) is document from dual;
-- test column name of node T_XmlSerialize
SELECT xmlserialize(CONTENT XMLTYPE('<Owner>Grandco</Owner>') as varchar2(30)) from dual;
-- test column name of node const
select 1 from dual;
select 1.2 from dual;
select ' 12 2 aaaa' from dual;
select ' 1.3 nnn? 大' from dual;
drop table t_opentenbase_ora_colname_20230220;
drop function f_opentenbase_ora_colname_20230220;
reset enable_opentenbase_ora_column_name;
