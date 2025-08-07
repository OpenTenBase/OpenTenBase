\c regression_ora

-- BEGIN 122214079
create table tbl_copy_numeric_20240329(id int, c1 numeric(10,3), c2 binary_float, c3 binary_double, c4 varchar2(10), c5 float);
insert into tbl_copy_numeric_20240329 values(1, 0.2, 0.3, 0.4, 0.5, 0.6);
insert into tbl_copy_numeric_20240329 values(2, 0.2, 0.3, 0.4, '0.5', 0.6);
insert into tbl_copy_numeric_20240329 values(3, 1.2, 1.3, 1.4, 1.5, 1.6);
insert into tbl_copy_numeric_20240329 values(4, 1.2, 1.3, 1.4, '1.5', 1.6);
select * from tbl_copy_numeric_20240329 order by 1;
\copy (select * from tbl_copy_numeric_20240329 order by 1) to stdout
drop table tbl_copy_numeric_20240329;
-- END 122214079

--
-- Fix the error of Insert2Copy in PL/SQL.
--
set transform_insert_to_copy to on;
create table table_t1_20240102(f1 int,f2 integer,f3 varchar2(20), f4 number) distribute by replication;

CREATE OR REPLACE procedure proc_p1_20240102() as
  vsql varchar2(200);
  v1 int :=11;
  v2 varchar2(20) :='gd';
  v3 int :=33;
BEGIN

   vsql:='insert into table_t1_20240102 (f1,f2,f3,f4) values(100,:1,:2,:3)';
   execute IMMEDIATE vsql using v1,v2,v3;

--     假如这样直接插入，则不报错
--     insert into table_t1_20240102 (f1,f2,f3,f4) values(100,11,'gd',33);
END;
/

call proc_p1_20240102();

select * from table_t1_20240102;

 declare
  vsql varchar2(200);
  v1 int :=11;
  v2 varchar2(20) :='gd';
  v3 int :=33;
BEGIN
 insert into table_t1_20240102 (f1,f2,f3,f4) values(v1,11,'gd',33);
END;
/

select * from table_t1_20240102 order by f1;

drop table table_t1_20240102;
drop procedure proc_p1_20240102;
reset transform_insert_to_copy;
--
-- Over
--