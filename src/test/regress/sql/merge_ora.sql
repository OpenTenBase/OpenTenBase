--
-- MERGE
--
\c regression_ora

--test update in merge into
create table test_d (a int, b int);
create table test_s (a int, b int);
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b;
select * from test_d order by a;
truncate table test_s;
insert into test_s values(generate_series(1,10),20);
merge into test_d d using test_s on(d.a=test_s.a) when matched then update set d.b=test_s.b;
select * from test_d order by a;
-- test when matched and not matched
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b);
select * from test_d order by a;
-- test when matched then delete
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b where test_s.a <= 8 delete where test_s.a = 8 when not matched then insert (a,b) VALUES (test_s.a, test_s.b) where test_s.a >= 3;
select * from test_d order by a;
-- fail for when matched tuple repeatedly
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
insert into test_s values(10,12);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b) where test_s.a >= 3;

-- test execute merge into in anonymous block for when matched and not matched
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
do 
$$
begin
  merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b);
end;
$$
;
select * from test_d order by a;
-- test execute merge into in anonymous block for when matched then delete
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
do 
$$
begin
  merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b where test_s.a <= 8 delete where test_s.a = 8 when not matched then insert (a,b) VALUES (test_s.a, test_s.b) where test_s.a >= 3;
end;
$$
;
select * from test_d order by a;

drop table test_d;
drop table test_s;

-- test merge into for REPLICATION
create table test_d (a int, b int) DISTRIBUTE BY REPLICATION;
create table test_s (a int, b int) DISTRIBUTE BY REPLICATION;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b);
select * from test_d order by a;

drop table test_d;
drop table test_s;
create table test_d (a int, b int) DISTRIBUTE BY REPLICATION;
create table test_s (a int, b int);
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b);
select * from test_d order by a;

-- test merge into REPLICATION with join on non-distribute key, ok
drop table test_d;
drop table test_s;
create table test_d (a int, b int, c int) DISTRIBUTE BY REPLICATION;
create table test_s (a int, b int, c int);
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
insert into test_s values(generate_series(1,10),generate_series(1,10),2);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;

-- test merge into join on non-distribute key, ok
drop table test_d;
drop table test_s;
create table test_d (a int, b int, c int);
create table test_s (a int, b int, c int);
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
insert into test_s values(generate_series(1,10),generate_series(1,10),2);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;
truncate test_d;
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
merge into test_d using test_s on(test_d.a=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;

-- test merge into join on non-distribute key, ok
drop table test_d;
drop table test_s;
create table test_d (a int, b int, c int);
create table test_s (a int, b int, c int) DISTRIBUTE BY REPLICATION;
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
insert into test_s values(generate_series(1,10),generate_series(1,10),2);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;
truncate test_d;
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
merge into test_d using test_s on(test_d.a=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;

-- clean table
drop table test_d;
drop table test_s;

-- test when matched with multi where conditions
create table test1(id int primary key,name varchar2(10));
insert into test1 values(1,'test11');
insert into test1 values(2,'test12');
insert into test1 values(3,'test13');
create table test2(id int primary key,name varchar2(10));
insert into test2 values(2,'test22');
insert into test2 values(3,'test23');
insert into test2 values(4,'test24');
insert into test2 values(5,'test25');
insert into test2 values(6,'test26');
MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.name = t2.name WHERE t.id >= 2 and t.name = 'test12'
    WHEN NOT MATCHED THEN INSERT (id,name) VALUES (t2.id, t2.name) WHERE t2.name = 'test25' and t2.id <= 5 ;
select * from test1 order by id;
--fail for violates unique constraint
merge into test1 a using test2 b on(a.id=b.id) when matched then update set a.id = 12;

truncate test1;
insert into test1 values(1,'test22');
insert into test1 values(3,'test23');
merge into test1 a using test2 b on(a.name=b.name) when matched then update set a.id = 12;

truncate test1;
insert into test1 values(1,'test11');
insert into test1 values(3,'test23');
merge into test1 a using test2 b on(a.name=b.name) when matched then update set a.id = 12;
select * from test1 order by id;
drop table test1;
drop table test2;
-- test array type for merge into
create table test_d(a int[3],b int);
create table test_s(a int[3],b int);
insert into test_d values('{1,2,3}',4);
insert into test_s values('{10,20,30}',4);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values('{11,21,31}',4);
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a=test_s.a;
select * from test_d;
drop table test_d;
drop table test_s;

create type newtype as(a int,b int);
create table test_d(a newtype, b int);
create table test_s(a newtype, b int);
insert into test_d values(ROW(1,2),3);
insert into test_s values(ROW(10,20),3);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values(ROW(11,12),3);
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values(ROW(22,22),3);
merge into test_d a using test_s on(a.b=test_s.b) when matched then update set a.a=21;
merge into test_d a using test_s on(a.b=test_s.b) when matched then update set a.b=22;
select * from test_d;
--fail
merge into test_d a using test_s on(a.b=test_s.b) when matched then update set a.a=test_s.a;
--must compatible with previous features, though not perfect
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a.a=test_s.b;
select * from test_d;
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a.a=test_s.b;
select * from test_d;
drop table test_s;
drop table test_d;
drop type newtype;

--merge into does not support view
drop table if exists empx;
drop table if exists emp2;
drop view if exists empx1;
create table empx(empno int, ename varchar(10), sal int, deptno int);
create table emp2 as select * from empx;
create view empx1 as select * from emp2 where deptno=20;
merge into empx1 a
using empx b
on (a.empno=b.empno)
when matched then
  update set a.sal=a.sal+100
when not matched then
  insert(empno,ename,sal,deptno) values (b.empno,b.ename,b.sal,b.deptno);
drop view empx1;

-- test partition table
create table test1(id int,name varchar2(10),age int) partition by list(name);
CREATE TABLE p_list_test1_201901 PARTITION OF test1 FOR VALUES in ('201901');
CREATE TABLE p_list_test1_201902 PARTITION OF test1 FOR VALUES in ('201902');
CREATE TABLE p_list_test1_201903 PARTITION OF test1 FOR VALUES in ('201903');

create unique index on p_list_test1_201901(id,name);
create unique index on p_list_test1_201902(id,name);
create unique index on p_list_test1_201903(id,name);

insert into test1(id,name,age) values(1,'201901',1);
insert into test1(id,name,age) values(2,'201902',2);
insert into test1(id,name,age) values(3,'201903',3);

create table test2(id int,name varchar2(10),age int) partition by list(name);
CREATE TABLE p_list_test2_201901 PARTITION OF test2 FOR VALUES in ('201901');
CREATE TABLE p_list_test2_201902 PARTITION OF test2 FOR VALUES in ('201902');
CREATE TABLE p_list_test2_201903 PARTITION OF test2 FOR VALUES in ('201903');
create unique index on p_list_test2_201901(id,name);
create unique index on p_list_test2_201902(id,name);
create unique index on p_list_test2_201903(id,name);
insert into test2 values(2,'201901',12);
insert into test2 values(3,'201902',13);
insert into test2 values(4,'201903',14);
insert into test2 values(5,'201901',15);
MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.name = t2.name WHERE t.id = t2.id
    WHEN NOT MATCHED THEN INSERT (id,name) VALUES (t2.id, t2.name) ;
select * from test1 order by id;

truncate table test1;
truncate table test2;
insert into test1(id,name,age) values(1,'201901',1);
insert into test1(id,name,age) values(2,'201902',2);
insert into test1(id,name,age) values(3,'201903',3);
insert into test2 values(2,'201901',12);
insert into test2 values(3,'201902',13);
insert into test2 values(4,'201903',14);
insert into test2 values(5,'201901',15);

MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.name = t2.name, t.age = t2.age WHERE t.id = t2.id
    WHEN NOT MATCHED THEN INSERT (id,name,age) VALUES (t2.id, t2.name, t2.age) ;
select * from test1 order by id;

update test2 set age = age + 100;
MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.age = t2.age WHERE t.id = t2.id
    WHEN NOT MATCHED THEN INSERT (id, age) VALUES (t2.id, t2.age) ;
select * from test1 order by id;

-- fail for when matched tuple repeatedly
truncate table test1;
truncate table test2;
insert into test1(id,name,age) values(1,'201901',1);
insert into test1(id,name,age) values(2,'201902',2);
insert into test1(id,name,age) values(3,'201903',3);
insert into test2 values(2,'201901',12);
insert into test2 values(3,'201902',13);
insert into test2 values(3,'201901',13);
MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.name = t2.name, t.age = t2.age WHERE t.id = t2.id
    WHEN NOT MATCHED THEN INSERT (id,name,age) VALUES (t2.id, t2.name, t2.age) ;

drop table test1;
drop table test2;

-- merge into update distribute key
DROP TABLE IF EXISTS test1;
create table test1(id int primary key,f2 int,name varchar2(10));
insert into test1 values(1,1,'test1');
insert into test1 values(2,2,'test1');
insert into test1 values(3,3,'test1');

DROP TABLE IF EXISTS test2;
create table test2(id int primary key,f2 int,name varchar2(10));
insert into test2 values(12,2,'test2');
insert into test2 values(13,3,'test2');
insert into test2 values(4,4,'test2');
insert into test2 values(5,5,'test2');
MERGE INTO test1 t
USING (
select * from test2
) t2 ON (t.f2  = t2.f2 )
WHEN MATCHED THEN UPDATE SET t.id=t2.id,t.name = t2.name WHERE t.f2 = t2.f2 
WHEN NOT MATCHED THEN INSERT (id,f2,name) VALUES (t2.id,t2.f2, t2.name) ;
select * from test1 order by id;
drop table test1;
drop table test2;

DROP TABLE IF EXISTS newproducts_row,products_row_multi;
CREATE TABLE newproducts_row
(
product_id INTEGER DEFAULT 0,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
);

INSERT INTO newproducts_row VALUES (1502, 'olympus camera', 'electrncs', 200);
INSERT INTO newproducts_row VALUES (1601, 'lamaze', 'toys', 200);
INSERT INTO newproducts_row VALUES (1666, 'harry potter', 'toys', 200);
INSERT INTO newproducts_row VALUES (1700, 'wait interface', 'books', 200);

CREATE TABLE products_row_multi
(
product_id INTEGER,
product_name VARCHAR(60),
category VARCHAR(60),
total INTEGER
);

INSERT INTO products_row_multi VALUES (1501, 'vivitar 35mm', 'electrncs', 100);
INSERT INTO products_row_multi VALUES (1502, 'olympus is50', 'electrncs', 100);
INSERT INTO products_row_multi VALUES (1600, 'play gym', 'toys', 100);
INSERT INTO products_row_multi VALUES (1601, 'lamaze', 'toys', 100);
INSERT INTO products_row_multi VALUES (1666, 'harry potter', 'dvd', 100);

ALTER TABLE products_row_multi DROP COLUMN product_name; 

MERGE INTO products_row_multi p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.category || 'ABC', np.total + 100);

SELECT * FROM products_row_multi ORDER BY product_id;
drop table newproducts_row,products_row_multi;

-- merge into with regexp expr
drop table if exists regexp_count_tb3 ;
create table regexp_count_tb3(id int,id1 varchar2(40),id2 varchar(40),id3 char(3),id4 char(40),id5 int,id6 char(1));
insert into regexp_count_tb3 values(10,'adbadrXXbcdfgdiec,ncdn','adbadrbcdfaagdiec,ncdn','a','adbadrbcdfgdiec,ncdn',1,'i'); 
insert into regexp_count_tb3 values(11,'adbadrabmmcdfBBBbb,ncdn','adbadrbcdfbbbbb,ncdn','B','adba1dr1bcd3fb3bb42b3b,ncdn',1,'c'); 
insert into regexp_count_tb3 values(12,'adbadriaannbcdfgdiec,ncdn','adbadrbCCCcdfgdiec,ncdn','c','adbad2rbcdf113gd2iec,2ncdn',2,'m'); 
insert into regexp_count_tb3 values(13,'adbadriaaabcdfedgdiec,ncdn','adbadrbdddcdfgdiec,ncdn','D','adba,d1rb,cdf,gd1i,ec,nc1dn',1,'n'); 
insert into regexp_count_tb3 values(14,'adb.xmdr,bcxmadf,gdi,ec,ncdn','AAdbdda,drb,cdf,gd,iec,ncdn',',','ad-ba-dr3bcd-fgd3-iec,n3cdn',3,'x'); 
insert into regexp_count_tb3 values(15,'ad-badaxxr-bcxxdccfg-die-c,','adba-drbbb-cdfgdi-ec-ncdn','-','adba#d123c#df321gda#ie33c#ncdn',1,'m'); 
insert into regexp_count_tb3 values(16,'a#adxmrdcbinca#dfingdiec#nc','adb#adqqrb#cdfgd#iec','#','a#',1,'i'); 
insert into regexp_count_tb3 values(17,null,null,null,null,null,null); 
--create table as 
drop table if exists regexp_count_tb5;
create table regexp_count_tb5 as select id,regexp_count(id1,'a',1) as id1 from regexp_count_tb3;

delete from regexp_count_tb5 where id = 10 or id=11;
select * from regexp_count_tb5 order by 1,2;

MERGE INTO regexp_count_tb5 t
USING (
  select * from regexp_count_tb3
) t2 ON (t.id = t2.id+1)
WHEN MATCHED THEN UPDATE SET t.id1 = regexp_count(t2.id1,'bc',1,'c')+4 where (t.id = t2.id+1)
WHEN NOT MATCHED THEN INSERT (id,id1) VALUES (t2.id, regexp_count(t2.id2,'bc',1,'c')) ;
select * from regexp_count_tb5 order by id, id1;

-- set nls_date_format = 'yyyymmdd';
-- http://tapd.woa.com/20421696/bugtrace/bugs/view?bug_id=1020421696095179657
drop table if exists test_date_tb5;
create table test_date_tb5                      
(                                               
    id int primary key,                                     
    col1 numeric,                               
    col2 number,                                
    col3 int,                                
    col4 varchar2(30),                          
    col5 varchar(30),                           
    col6 char(30),
    col7 date                                 
);                                                                                     
insert into test_date_tb5 values(1,  20200101.000+1,  20200102.000+1,  20200201+1,  20200201+1,  20200301+1,  20200302+1,  null); 
insert into test_date_tb5 values(2,  20200101.000+2,  20200102.000+2,  20200201+2,  20200201+2,  20200301+2,  20200302+2,  null); 
insert into test_date_tb5 values(3,  20200101.000+3,  20200102.000+3,  20200201+3,  20200201+3,  20200301+3,  20200302+3,  null); 
insert into test_date_tb5 values(4,  20200101.000+4,  20200102.000+4,  20200201+4,  20200201+4,  20200301+4,  20200302+4,  null); 
insert into test_date_tb5 values(5,  20200101.000+5,  20200102.000+5,  20200201+5,  20200201+5,  20200301+5,  20200302+5,  null); 
insert into test_date_tb5 values(6,  20200101.000+6,  20200102.000+6,  20200201+6,  20200201+6,  20200301+6,  20200302+6,  null); 
insert into test_date_tb5 values(7,  20200101.000+7,  20200102.000+7,  20200201+7,  20200201+7,  20200301+7,  20200302+7,  null); 
insert into test_date_tb5 values(8,  20200101.000+8,  20200102.000+8,  20200201+8,  20200201+8,  20200301+8,  20200302+8,  null); 
insert into test_date_tb5 values(9,  20200101.000+9,  20200102.000+9,  20200201+9,  20200201+9,  20200301+9,  20200302+9,  null); 
insert into test_date_tb5 values(10, 20200101.000+10, 20200102.000+10, 20200201+10, 20200201+10, 20200301+10, 20200302+10, null); 
insert into test_date_tb5 values(11, 20200101.000+11, 20200102.000+11, 20200201+11, 20200201+11, 20200301+11, 20200302+11, null); 
insert into test_date_tb5 values(12, 20200101.000+12, 20200102.000+12, 20200201+12, 20200201+12, 20200301+12, 20200302+12, null);      
insert into test_date_tb5 values(13, 20200101.000+13, 20200102.000+13, 20200201+13, 20200201+13, 20200301+13, 20200302+13, null); 
insert into test_date_tb5 values(14, 20200101.000+14, 20200102.000+14, 20200201+14, 20200201+14, 20200301+14, 20200302+14, null); 
insert into test_date_tb5 values(15, 20200101.000+15, 20200102.000+15, 20200201+15, 20200201+15, 20200301+15, 20200302+15, null); 
drop table if exists test_date_tb6;
create table test_date_tb6 as select * from test_date_tb5;
-- set nls_date_format = 'yyyymmdd';
merge into test_date_tb5 t1 
using test_date_tb6 
on (t1.id = test_date_tb6.id+1)
WHEN MATCHED THEN
UPDATE SET t1.col7 = (test_date_tb6.col2::text::date), t1.col3=cast(test_date_tb6.col4 as int) 
WHEN NOT MATCHED THEN
INSERT (id,col1,col2) VALUES (test_date_tb6.id+1,test_date_tb6.col2,test_date_tb6.col1);
select * from test_date_tb5 order by id;

-- http://tapd.woa.com/20421696/bugtrace/bugs/view?bug_id=1020421696095179655
truncate test_date_tb5;
insert into test_date_tb5 values(1,  20200101.000+1,  20200102.000+1,  20200201+1,  20200201+1,  20200301+1,  20200302+1); 
insert into test_date_tb5 values(2,  20200101.000+2,  20200102.000+2,  20200201+2,  20200201+2,  20200301+2,  20200302+2); 
insert into test_date_tb5 values(3,  20200101.000+3,  20200102.000+3,  20200201+3,  20200201+3,  20200301+3,  20200302+3); 
insert into test_date_tb5 values(4,  20200101.000+4,  20200102.000+4,  20200201+4,  20200201+4,  20200301+4,  20200302+4); 
insert into test_date_tb5 values(5,  20200101.000+5,  20200102.000+5,  20200201+5,  20200201+5,  20200301+5,  20200302+5); 
insert into test_date_tb5 values(6,  20200101.000+6,  20200102.000+6,  20200201+6,  20200201+6,  20200301+6,  20200302+6); 
insert into test_date_tb5 values(7,  20200101.000+7,  20200102.000+7,  20200201+7,  20200201+7,  20200301+7,  20200302+7); 
insert into test_date_tb5 values(8,  20200101.000+8,  20200102.000+8,  20200201+8,  20200201+8,  20200301+8,  20200302+8); 
insert into test_date_tb5 values(9,  20200101.000+9,  20200102.000+9,  20200201+9,  20200201+9,  20200301+9,  20200302+9); 
insert into test_date_tb5 values(10, 20200101.000+10, 20200102.000+10, 20200201+10, 20200201+10, 20200301+10, 20200302+10); 
insert into test_date_tb5 values(11, 20200101.000+11, 20200102.000+11, 20200201+11, 20200201+11, 20200301+11, 20200302+11); 
insert into test_date_tb5 values(12, 20200101.000+12, 20200102.000+12, 20200201+12, 20200201+12, 20200301+12, 20200302+12);      
insert into test_date_tb5 values(13, 20200101.000+13, 20200102.000+13, 20200201+13, 20200201+13, 20200301+13, 20200302+13); 
insert into test_date_tb5 values(14, 20200101.000+14, 20200102.000+14, 20200201+14, 20200201+14, 20200301+14, 20200302+14); 
insert into test_date_tb5 values(15, 20200101.000+15, 20200102.000+15, 20200201+15, 20200201+15, 20200301+15, 20200302+15); 

select * from test_date_tb5 order by id;
merge into test_date_tb5 t1 
using test_date_tb5 
on (t1.id = test_date_tb5.id+1)
WHEN MATCHED THEN
UPDATE SET t1.col7 = (test_date_tb5.col2::text::date), t1.col3=cast(test_date_tb5.col4 as int) 
WHEN NOT MATCHED THEN
INSERT (id,col1,col2) VALUES (test_date_tb5.id+1,test_date_tb5.col2,test_date_tb5.col1);
select * from test_date_tb5 order by id;
-- reset nls_date_format;

--
-- cases for forbidden updating join on column
--
-- case 1: The source table and target table are the same
drop table if exists homo_table;
create table homo_table(f1 int, f2 text, f3 int) distribute by shard(f1);
insert into homo_table values (10, 'a', 15);
insert into homo_table values (15, 'a', 16);
-- error
merge into homo_table t using homo_table s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;

-- error
merge into homo_table t using homo_table s on (t.f1 = s.f1) when matched then update set f1 = f3;

-- case 2: the source is a subquery that contains the target table
drop table if exists homo_table_2;
create table homo_table_2(f1 int, f2 text, f3 int) distribute by shard(f1);
insert into homo_table_2 values (15, 'a', 15);

-- error
merge into homo_table t using (select * from homo_table s1 where f1 < 10) s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;

-- OK
begin;
merge into homo_table t using (select s1.f1, s1.f2, s2.f3 from homo_table s1 join homo_table_2 s2 on (s1.f3=s2.f3)) s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;
select * from homo_table order by f1;
rollback;

-- error
merge into homo_table t using (select s2.f1, s2.f2, s1.f3 from homo_table s1 join homo_table_2 s2 on (s1.f3=s2.f3)) s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;

with src as (select s2.f1, s2.f2, s1.f3 from homo_table s1 join homo_table_2 s2 on (s1.f3=s2.f3))
merge into homo_table t using src s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;

-- set nls_timestamp_format = 'yyyy-mm-dd hh24:mi:ss';
drop table if exists homo_part_list_20220813;
create table homo_part_list_20220813(f1 number(10),f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) partition by list ( f3 ) distribute by shard(f1) to group default_group;
create table homo_part_list_1_20220813 partition of homo_part_list_20220813 for values in ('f3 r0') partition by hash (f2);
create table homo_part_list_1_h1_20220813 partition of homo_part_list_1_20220813 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
create table homo_part_list_1_h2_20220813 partition of homo_part_list_1_20220813 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
create table homo_part_list_2_20220813 partition of homo_part_list_20220813 for values in ('f3 r1') partition by hash (f2);
create table homo_part_list_2_h1_20220813 partition of homo_part_list_2_20220813 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
create table homo_part_list_2_h2_20220813 partition of homo_part_list_2_20220813 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
create table homo_part_list_3_20220813 partition of homo_part_list_20220813 for values in ('f3 r2') partition by range (f2);

-- case 2: The source table is a child partition of the target table
merge into homo_part_list_20220813 t using homo_part_list_2_20220813 s on (t.f1 = s.f2) when matched then update set f2 = s.f1 + 10;

merge into homo_part_list_20220813 t using homo_part_list_2_h1_20220813 s on (t.f1 = s.f2) when matched then update set f2 = s.f1 + 10;

-- case 3: The target table is a child partition of the source table
merge into homo_part_list_2_20220813 t using homo_part_list_20220813 s on (t.f1 = s.f2) when matched then update set f2 = s.f1 + 10;

merge into homo_part_list_2_h1_20220813 t using homo_part_list_20220813 s on (t.f1 = s.f2) when matched then update set f2 = s.f1 + 10;

drop table homo_part_list_20220813;
drop table homo_table;

drop table if exists t_shard_20220815;
create table t_shard_20220815(f1 int not null ,f2 integer generated by default as identity ,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone)distribute by shard(f1) to group default_group;

drop table if exists t_shard_20220815_2;
create table t_shard_20220815_2(f1 int not null ,f2 integer generated always as identity ,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone)distribute by shard(f1) to group default_group;

drop table if exists s_shard_20220815;
create table s_shard_20220815(f1 int not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1) to group default_group;

insert into s_shard_20220815 select n, n + 200, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(1,1) n;
insert into s_shard_20220815 select n, n + 200, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(2,2) n;
insert into s_shard_20220815 select n, n + 200, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(3,3) n;
select * from s_shard_20220815 order by f1, f2, f3, f4, f5;

insert into t_shard_20220815 select n, n+100, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(1,1) n;
select * from t_shard_20220815 order by f1, f2, f3, f4, f5;

insert into t_shard_20220815 overriding user value select n, n+100, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(2,2) n;
select * from t_shard_20220815 order by f1, f2, f3, f4, f5;

insert into t_shard_20220815_2 select n, n+100, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(1,1) n;
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;

insert into t_shard_20220815_2 (f1, f3, f4, f5) select n, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(1,1) n;
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;

insert into t_shard_20220815_2 overriding system value select n, n+100, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(2,2) n;
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;

-- gen by default
begin;
merge into t_shard_20220815 t using s_shard_20220815 s on (t.f1 = s.f1) when matched then update set f2 = s.f2 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from t_shard_20220815 order by f1, f2, f3, f4, f5;
rollback;

begin;
merge into t_shard_20220815 t using s_shard_20220815 s on (t.f1 = s.f1) when matched then update set f2 = s.f2 when not matched then insert overriding user value values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from t_shard_20220815 order by f1, f2, f3, f4, f5;
rollback;

-- gen by always
begin;
merge into t_shard_20220815_2 t using s_shard_20220815 s on (t.f1 = s.f1) when matched then update set f2 = s.f2 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;
rollback;

begin;
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;
merge into t_shard_20220815_2 t using s_shard_20220815 s on (t.f1 = s.f1) when matched then update set t.f2 = default;
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;
rollback;

begin;
merge into t_shard_20220815_2 t using s_shard_20220815 s on (t.f1 = s.f1) when matched then update set f2 = default when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;
rollback;

begin;
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;
merge into t_shard_20220815_2 t using s_shard_20220815 s on (t.f1 = s.f1) when not matched then insert values(s.f1, default, s.f3, s.f4, s.f5);
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;
rollback;

begin;
merge into t_shard_20220815_2 t using s_shard_20220815 s on (t.f1 = s.f1) when matched then update set t.f3 = s.f3 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;
rollback;

begin;
merge into t_shard_20220815_2 t using s_shard_20220815 s on (t.f1 = s.f1) when matched then update set t.f3 = s.f3 when not matched then insert values(s.f1, default, s.f3, s.f4, s.f5);
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;
rollback;

begin;
merge into t_shard_20220815_2 t using s_shard_20220815 s on (t.f1 = s.f1) when matched then update set t.f3 = s.f3 when not matched then insert overriding system value values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from t_shard_20220815_2 order by f1, f2, f3, f4, f5;
rollback;

-- merge into view
drop table if exists tv_t cascade;
drop table if exists tv_t2;
drop table if exists tv_s;
drop table if exists tv_s2;
drop table if exists tv_rp;
create table tv_t (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);
create table tv_t2 (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);
create table tv_rp (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by replication;
create table tv_s (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);
create table tv_s2 (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);
-- set nls_timestamp_format = 'yyyy-mm-dd';
insert into tv_s select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(1,10) n;
insert into tv_s2 select n, n - 4, 'f3 r'|| floor(n/5),'f4 r' || (n - 4), '2022-08-10' from generate_series(5,10) n;

-- error: Views that do not select from a single table or view are not automatically updatable.
create view tv_v as select t1.f1, t2.f2, t1.f3, t2.f4, t1.f5 from tv_t t1 join tv_t2 t2 on t1.f2=t2.f2;
merge into tv_v t using tv_s s on t.f2 = s.f2 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
drop view tv_v;

-- error: View columns that are not columns of their base relation are not updatable.
create or replace view tv_v as select f1 + f2::integer as f1, substr(f3, 5) as f3, f4, f5 from tv_t;
merge into tv_v t using tv_s s on t.f4 = s.f4 when not matched then insert values(s.f1, s.f3, s.f4, s.f5);
-- error: View columns that are not columns of their base relation are not updatable.
merge into tv_v t using tv_s s on t.f4 = s.f4 when matched then update set f3=s.f1::varchar2;
-- ok
merge into tv_v t using tv_s s on t.f4 = s.f4 when not matched then insert(f4, f5) values(s.f4, s.f5);
select * from tv_v order by 1,2,3,4,5;
select * from tv_t order by 1,2,3,4,5;
drop view tv_v;

truncate tv_t;
create or replace view tv_v as select f1, f1 + f2::numeric ff2, f2, substr(f3, 5) as f3, f4, f5 from tv_t;
merge into tv_v t using tv_s s on ff2 = s.f1 * 2 when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
select * from tv_v order by 1,2,3,4,5;
select * from tv_t order by 1,2,3,4,5;
merge into tv_v t using tv_s s on ff2 = s.f1 * 2 when matched then update set f4 = 'updated' where t.f1 % 2 = 0;
select * from tv_v order by 1,2,3,4,5;
select * from tv_t order by 1,2,3,4,5;
merge into tv_v t using tv_s s on ff2 = s.f1 * 2 and t.f4 = 'updated' when matched then update set f1 = s.f1 + 100 delete where t.f2 = 104 when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
merge into tv_v t using tv_s s on ff2 = s.f1 * 2 and t.f4 = 'updated' when matched then update set f2 = s.f1 + 100 delete where t.f2 = 104 when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
merge into tv_v t using tv_s s on ff2 = s.f1 * 2 and t.f4 = 'updated' when matched then update set f5 = s.f5 delete where t.f5 = '2022-08-10' when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
select * from tv_v order by 1,2,3,4,5;
select * from tv_t order by 1,2,3,4,5;
drop view tv_v;

create view tv_v as select t1.f1, t1.f2, t1.f3, t1.f4, t1.f5 from tv_rp t1;
-- error: cannot execute MERGE on relation "tv_rp"
merge into tv_v t using tv_s s on t.f4 = s.f4 when not matched then insert values(s.f1, s.f3, s.f4, s.f5);
drop view tv_v;

truncate tv_t;
create view tv_v as  select f5 ff1, f4 ff2, f3 ff3, f2 ff4, f1 + f2::numeric ff12, f1 ff5 from tv_t;
merge into tv_v t using tv_s s on ff12 = s.f1 * 2 when not matched then insert(ff1, ff2, ff4, ff5) values(s.f5, s.f4, s.f2, s.f1);
select * from tv_v order by 1,2,3,4,5;
select * from tv_t order by 1,2,3,4,5;
merge into tv_v t using tv_s s on ff12 = s.f1 * 2 when matched then update set ff2 = 'updated' where ff5 % 2 = 0;
select * from tv_v order by 1,2,3,4,5;
select * from tv_t order by 1,2,3,4,5;
merge into tv_v t using tv_s s on ff12 = s.f1 * 2 and ff2 = 'updated' when matched then update set ff4 = s.f1 + 100 delete where ff2 = 104 when not matched then insert(ff5, ff4, ff2, ff1) values(s.f1, s.f2, s.f4, s.f5);
merge into tv_v t using tv_s s on ff12 = s.f1 * 2 and ff2 = 'updated' when matched then update set ff12 = s.f1 + 100 delete where ff2 = 104 when not matched then insert(ff5, ff4, ff2, ff1) values(s.f1, s.f2, s.f4, s.f5);
merge into tv_v t using tv_s s on ff12 = s.f1 * 2 and ff2 = 'updated' when matched then update set ff1 = s.f5 delete where ff1 = '2022-08-10' when not matched then insert(ff5, ff4, ff3, ff2, ff1) values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from tv_v order by 1,2,3,4,5;
select * from tv_t order by 1,2,3,4,5;
drop view tv_v;

-- merge into subquery
drop table if exists tv_t cascade;
drop table if exists tv_t2;
drop table if exists tv_s;
drop table if exists tv_s2;
drop table if exists tv_rp;
create table tv_t (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);
create table tv_t2 (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);
create table tv_rp (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by replication;
create table tv_s (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);
create table tv_s2 (f1 int, f2 name, f3 name, f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);
-- set nls_timestamp_format = 'yyyy-mm-dd';
insert into tv_s select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, '2022-08-10' from generate_series(1,10) n;
insert into tv_s2 select n, n - 4, 'f3 r'|| floor(n/5),'f4 r' || (n - 4), '2022-08-10' from generate_series(5,10) n;

-- error: subqury with join
merge into (select t1.f1, t2.f2, t1.f3, t2.f4, t1.f5 from tv_t t1 join tv_t2 t2 on t1.f2=t2.f2) t using tv_s s on t.f2 = s.f2 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);

-- error: update unupdatable column
merge into (select f1 + f2::integer as f1, substr(f3, 5) as f3, f4, f5 from tv_t) t using tv_s s on t.f4 = s.f4 when not matched then insert values(s.f1, s.f3, s.f4, s.f5);
merge into (select f1 + f2::integer as f1, substr(f3, 5) as f3, f4, f5 from tv_t) t using tv_s s on t.f4 = s.f4 when matched then update set f3=s.f1::varchar2;
-- ok
merge into (select f1 + f2::integer as f1, substr(f3, 5) as f3, f4, f5 from tv_t) t using tv_s s on t.f4 = s.f4 when not matched then insert(f4, f5) values(s.f4, s.f5);
select * from tv_t order by 1,2,3,4,5;

truncate tv_t;
merge into (select f1, f1 + f2::numeric ff2, f2, substr(f3, 5) as f3, f4, f5 from tv_t) t using tv_s s on ff2 = s.f1 * 2 when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
select * from tv_t order by 1,2,3,4,5;
merge into (select f1 ff1, f1 + f2::numeric ff12, f2 ff2, substr(f3, 5) as ff3, f4 ff4, f5 ff5 from tv_t) using tv_s s on ff12 = s.f1 * 2 when matched then update set ff4 = 'updated' where ff1 % 2 = 0;
select * from tv_t order by 1,2,3,4,5;
merge into (select f1, f1 + f2::numeric ff2, f2, substr(f3, 5) as f3, f4, f5 from tv_t) t using tv_s s on ff2 = s.f1 * 2 and t.f4 = 'updated' when matched then update set f1 = s.f1 + 100 delete where t.f2 = 104 when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
merge into (select f1, f1 + f2::numeric ff2, f2, substr(f3, 5) as f3, f4, f5 from tv_t) t using tv_s s on ff2 = s.f1 * 2 and t.f4 = 'updated' when matched then update set f2 = s.f1 + 100 delete where t.f2 = 104 when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
merge into (select f1, f1 + f2::numeric ff2, f2, substr(f3, 5) as f3, f4, f5 from tv_t) t using tv_s s on ff2 = s.f1 * 2 and t.f4 = 'updated' when matched then update set f5 = s.f5 delete where t.f5 = '2022-08-10' when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
select * from tv_t order by 1,2,3,4,5;

truncate tv_t;
merge into (select f1, f1 + f2::numeric ff2, f2, substr(f3, 5) as f3, f4, f5 from tv_t t where f2 = (select max(f2) from tv_s2 s where t.f4 = s.f4)) t using tv_s s on ff2 = s.f1 * 2 when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
select * from tv_t order by 1,2,3,4,5;
merge into (select f1 ff1, f1 + f2::numeric ff12, f2 ff2, substr(f3, 5) as ff3, f4 ff4, f5 ff5 from tv_t t where f2 = (select max(f2) from tv_s2 s where t.f4 = s.f4)) using tv_s s on ff12 = s.f1 * 2 when matched then update set ff4 = 'updated' where ff1 % 2 = 0;
-- ok
merge into (select f1 ff1, f1 + f2::numeric ff12, f2 ff2, substr(f3, 5) as ff3, f4 ff4, f5 ff5 from tv_t t where f2 = (select max(f2) from tv_s2 s where t.f4 = s.f4)) using tv_s s on ff12 = s.f1 * 2 when matched then update set ff5 = '2022-10-10' where ff1 % 2 = 0;
select * from tv_t order by 1,2,3,4,5;
merge into (select f1, f1 + f2::numeric ff2, f2, substr(f3, 5) as f3, f4, f5 from tv_t t where f2 = (select max(f2) from tv_s2 s where t.f4 = s.f4)) t using tv_s s on ff2 = s.f1 * 2 when matched then update set f5 = s.f5 delete where t.f5 = '2022-08-10' when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);
select * from tv_t order by 1,2,3,4,5;

explain (costs off, verbose on)merge into (select f1, f1 + f2::numeric ff2, f2, substr(f3, 5) as f3, f4, f5 from tv_t t where f2 = (select max(f2) from tv_s2 s where t.f4 = s.f4)) t using tv_s s on ff2 = s.f1 * 2 when matched then update set f5 = s.f5 delete where t.f5 = '2022-08-10' when not matched then insert(f1, f2, f4, f5) values(s.f1, s.f2, s.f4, s.f5);

explain (costs off, verbose on)merge into (select f1 ff1, f1 + f2::numeric f12, f2 ff2, substr(f3, 5) as ff3, f4 ff4, f5 ff5 from tv_t t where f2 = (select max(f2) from tv_s2 s where t.f4 = s.f4)) using tv_s s on f12 = s.f1 * 2 when matched then update set ff5 = s.f5 delete where ff5 = '2022-08-10' when not matched then insert(ff1, ff2, ff4, ff5) values(s.f1, s.f2, s.f4, s.f5);

-- reset nls_date_format;
-- set nls_timestamp_format = 'yyyy-mm-dd hh24:mi:ss.ff';
truncate tv_t;
-- error: cannot merge column "?column?" of Subquery
merge into (select f1 + f2, f2 ff2, f3 ff3 from tv_t) using tv_s on (ff2=f2) when not matched then insert values(f1, f2, f3);
-- ok
merge into (select f1 ff1, f2 ff2, f3 ff3 from tv_t where f2 = (select max(f2) from tv_s2)) using tv_s on (ff2=f2) when not matched then insert values(f1, f2, f3);
select * from tv_t order by 1,2,3,4,5;
merge into (select f1 ff1, f2 ff2, f3 ff3 from tv_t where f2 = (select max(f2) from tv_s2)) using tv_s on (ff2=f2) when matched then update set ff3 = 'updated';
select * from tv_t order by 1,2,3,4,5;

truncate tv_t;
merge into (select f5 ff1, f4 ff2, f3 ff3, f2 ff4, f1 ff5 from tv_t where f2 = (select max(f2) from tv_s2)) using tv_s on (ff2=f2) when not matched then insert values(f1, f2, f3, f4, f5);
-- error: cannot execute MERGE on relation "tv_rp"
merge into (select t1.f1, t1.f2, t1.f3, t1.f4, t1.f5 from tv_rp t1) t using tv_s s on t.f4 = s.f4 when not matched then insert values(s.f1, s.f3, s.f4, s.f5);

truncate tv_t;
merge into (select ff5, ff4, ff3, ff2, ff1 from (select f5 ff1, f4 ff2, substr(f3, 5) ff3, f2::numeric + f1 ff4, f1 ff5 from tv_t)) using tv_s s on (ff2 = s.f4) when matched then update set ff3 = 'updated';
merge into (select ff5, ff4, ff3, ff2, ff1 from (select f5 ff1, f4 ff2, substr(f3, 5) ff3, f2::numeric + f1 ff4, f1 ff5 from tv_t)) using tv_s s on (ff2 = s.f4) when not matched then insert(ff5, ff2, ff1) values(s.f1, s.f2, s.f5);
select * from tv_t order by 1,2,3,4,5;
merge into (select ff5, ff4, ff3, ff2, ff1 from (select f5 ff1, f4 ff2, substr(f3, 5) ff3, f2::numeric + f1 ff4, f1 ff5 from tv_t)) using tv_s s on ff5 = s.f1 when matched then update set ff1 = s.f5 where ff5 % 2 = 0;
select * from tv_t order by 1,2,3,4,5;


create table test1(id int, info varchar2(32));
create table test2(id int, info varchar2(32));
merge into (select * from test2 where id is not null) t2 using (select * from test1 where id<4) t1 on (t2.id=t1.id) when matched then update set t2.info=t1.info;
drop table test1;
drop table test2;
drop table if exists tp;
create table tp (id int, name name ,age int, class text, CONSTRAINT tp_index PRIMARY KEY (id, class))PARTITION BY LIST (class) 
DISTRIBUTE BY SHARD (id);

drop table if exists tp_c1;
drop table if exists tp_c2;
create table tp_c1 partition of tp for values in ('c1');
create table tp_c2 partition of tp for values in ('c2');

merge into tp using (select 1 id, 'aa' name, 10 age, 'c10' class from dual) s on tp.class = s.class when matched then update set age = s.age;

merge into tp using (select 1 id, 'aa' name, 10 age, 'c10' class from dual) s on tp.class = s.class when matched then update set age = s.age when not matched then insert values(s.id, s.name, s.age, s.class);

-- 复制分区表，无子分区
drop table if exists t_merge_20220928;
create table t_merge_20220928 (id int, name name ,age int, class text, CONSTRAINT t_merge_20220928_index PRIMARY KEY (id, class))PARTITION BY LIST (class) DISTRIBUTE by replication;

merge into t_merge_20220928 using (select id+1 id,name,age,class from t_merge_20220928) s on t_merge_20220928.class = s.class when matched then update set age = s.age+100 when not matched then insert values(s.id, s.name, s.age, s.class);

select * from t_merge_20220928;
drop table t_merge_20220928;

-- shard分布分区表，无子分区
create table t_merge_20220928_2 (id int, name name ,age int, class text, CONSTRAINT t_merge_20220928_2_index PRIMARY KEY (id, class))PARTITION BY LIST (class) 
DISTRIBUTE by shard(id);

merge into t_merge_20220928_2 using (select id+1 id,name,age,class from t_merge_20220928_2) s on t_merge_20220928_2.class = s.class when matched then update set age = s.age+100 when not matched then insert values(s.id, s.name, s.age, s.class);

select * from t_merge_20220928_2;
drop table t_merge_20220928_2;
drop table if exists bug_merge_into_evaluate_1;
create table bug_merge_into_evaluate_1 (id number,name varchar2(20));
insert into bug_merge_into_evaluate_1 select a,'name2'||a from generate_series(1,1000) a;
insert into bug_merge_into_evaluate_1 select a,'name2'||a from generate_series(1,1000) a;
create index idx_bug_merge_into_evaluate_1_id on bug_merge_into_evaluate_1(id);
analyze bug_merge_into_evaluate_1;
select reltuples,relpages from pg_class where relname='bug_merge_into_evaluate_1';
explain (costs off,nodes off,buffers off,summary off, buffers off) 
merge into bug_merge_into_evaluate_1 a 
using (select '1' as id ,'name1' as name from dual) b
on (a.id=b.id)
when matched then
    update set a.name='name1'
when not matched then
    insert (id,name) values('1','name1');
merge into bug_merge_into_evaluate_1 a 
using (select '1' as id ,'name1' as name from dual) b
on (a.id=b.id)
when matched then
    update set a.name='name1'
when not matched then
    insert (id,name) values('1','name1');
select count(1) from bug_merge_into_evaluate_1 where name='name1';
drop table bug_merge_into_evaluate_1;
create schema IF NOT EXISTS mergeview_with_check;
drop table if exists mergeview_with_check.ora_st_shard_20230310;

create table mergeview_with_check.ora_st_shard_20230310(f1 number(10) not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);

drop table if exists mergeview_with_check.ora_st_shard_20230310_t;
create table mergeview_with_check.ora_st_shard_20230310_t(f1 number(10) not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);

drop view if exists mergeview_with_check.ora_st_shard_20230310_v;

create view mergeview_with_check.ora_st_shard_20230310_v as select * from mergeview_with_check.ora_st_shard_20230310 where f1 < 10 ;

drop view if exists mergeview_with_check.ora_st_shard_20230310_v2;

create view mergeview_with_check.ora_st_shard_20230310_v2 as select * from mergeview_with_check.ora_st_shard_20230310 where f2 < 10 ;

drop table if exists mergeview_with_check.ora_st_shard_20230310_2;

create table mergeview_with_check.ora_st_shard_20230310_2(f1 number(10) not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);

drop view if exists mergeview_with_check.ora_st_shard_20230310_v_2;

create view mergeview_with_check.ora_st_shard_20230310_v_2 as select * from mergeview_with_check.ora_st_shard_20230310_2;

truncate mergeview_with_check.ora_st_shard_20230310_2;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-01 00:00:00' from generate_series(1,5) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-02 00:00:00' from generate_series(6,10) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-03 00:00:00' from generate_series(11,15) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-04 00:00:00' from generate_series(16,20) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-05 00:00:00' from generate_series(21,25) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-06 00:00:00' from generate_series(26,30) n;

begin;
-- case 1.1 only not matched
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310_v_2 s on (t.f1 = s.f1) when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from mergeview_with_check.ora_st_shard_20230310_v2 order by f1, f2, f3, f4;
select * from mergeview_with_check.ora_st_shard_20230310 order by f1, f2, f3, f4;
-- case 1.2: matched & not matched
delete from mergeview_with_check.ora_st_shard_20230310_v2 t where MOD (f1, 2) = 0;
delete from mergeview_with_check.ora_st_shard_20230310 t where MOD (f1, 2) = 0;
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310_v_2 s on (t.f1 = s.f1) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
select * from mergeview_with_check.ora_st_shard_20230310_v2 order by f1, f2, f3, f4;
select * from mergeview_with_check.ora_st_shard_20230310 order by f1, f2, f3, f4;
-- case 1.3：matched delete & not matched
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310_v_2 s on (t.f1 = s.f1) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 delete where t.f2 = 31 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 <= 10;
select * from mergeview_with_check.ora_st_shard_20230310_v2 order by f1, f2, f3, f4;
select * from mergeview_with_check.ora_st_shard_20230310 order by f1, f2, f3, f4;
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310_v_2 s on (t.f1 = s.f1) when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from mergeview_with_check.ora_st_shard_20230310_v2 order by f1, f2, f3, f4;
select * from mergeview_with_check.ora_st_shard_20230310 order by f1, f2, f3, f4;
-- case 1.2: matched & not matched
delete from mergeview_with_check.ora_st_shard_20230310_v2 t where MOD (f1, 2) = 0;
delete from mergeview_with_check.ora_st_shard_20230310 t where MOD (f1, 2) = 0;
-- 源表和目标视图引用同一基表
-- 1. 可更新
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using (select * from mergeview_with_check.ora_st_shard_20230310 where f2 < 10) s on (t.f1 = s.f1) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

begin;
-- 1. 不可更新：ORA-38104: Columns referenced in the ON Clause cannot be updated: "S"."F2"
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using (select * from mergeview_with_check.ora_st_shard_20230310 where f2 < 10) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

begin;
-- 1. 不可更新：ORA-38104: Columns referenced in the ON Clause cannot be updated: "S"."F2"
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310 s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

-- 源表来自多个表
begin;
-- 1. 不可更新：on条件中列指向同一基表
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using (select s1.* from mergeview_with_check.ora_st_shard_20230310 s1 join mergeview_with_check.ora_st_shard_20230310_t s2 on s1.f1 = s2.f1 where s1.f2 < 10) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

begin;
-- 2. 可更新
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using (select s2.* from mergeview_with_check.ora_st_shard_20230310 s1 join mergeview_with_check.ora_st_shard_20230310_t s2 on s1.f1 = s2.f1 where s1.f2 < 10) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

create table mergeview_with_check.ora_st_shard_part_list_20220710(f1 number(10) not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) partition by list ( f3 ) distribute by shard(f1) to group default_group;
create table mergeview_with_check.ora_st_shard_part_list_1_20220710 partition of mergeview_with_check.ora_st_shard_part_list_20220710 for values in ('f3 r0');
create table mergeview_with_check.ora_st_shard_part_list_2_20220710 partition of mergeview_with_check.ora_st_shard_part_list_20220710 for values in ('f3 r1');
create table mergeview_with_check.ora_st_shard_part_list_3_20220710 partition of mergeview_with_check.ora_st_shard_part_list_20220710 for values in ('f3 r2');

begin;
merge into mergeview_with_check.ora_st_shard_part_list_1_20220710 t using mergeview_with_check.ora_st_shard_part_list_20220710 s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_20220710 t using mergeview_with_check.ora_st_shard_part_list_1_20220710 s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_20220710 t using (select s1.* from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_1_20220710 t using (select s1.* from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_1_20220710 t using (select s2.* from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_20220710 t using (select s2.f1 f1, s1.f2 f2, s1.f3, s1.f4, s1.f5 from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f1 and t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_3_20220710 t using (select s1.* from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;
drop schema if exists mergeview_with_check cascade;

-- https://tapd.woa.com/20421696/prong/stories/view/1020421696883101603
drop sequence if exists tt;
drop sequence if exists qq;
drop table if exists t1; 
drop table if exists t2; 
create sequence tt start 1;
create sequence qq start 1;

create table t2 (f1 int, f2 varchar(36), create_date pg_catalog.timestamp(6)) distribute by shard(f1);
create table t1 (f1 int, f2 varchar(36), create_date pg_catalog.timestamp(6)) partition by range (create_date) distribute by shard(f1);

create table t1_01 partition of t1 for values from ( '2023-01-01') to ( '2023-02-01');
create table t1_02 partition of t1 for values from ( '2023-02-01') to ( '2023-03-01');
create table t1_03 partition of t1 for values from ( '2023-03-01') to ( '2023-04-01');
create table t1_04 partition of t1 for values from ( '2023-04-01') to ( '2023-05-01');
create table t1_05 partition of t1 for values from ( '2023-05-01') to ( '2023-06-01');
create table t1_06 partition of t1 for values from ( '2023-06-01') to ( '2023-07-01');
create table t1_07 partition of t1 for values from ( '2023-07-01') to ( '2023-08-01');
create table t1_08 partition of t1 for values from ( '2023-08-01') to ( '2023-09-01');
create table t1_09 partition of t1 for values from ( '2023-09-01') to ( '2023-10-01');
create table t1_10 partition of t1 for values from ( '2023-10-01') to ( '2023-11-01');
create table t1_11 partition of t1 for values from ( '2023-11-01') to ( '2023-12-01');
create table t1_12 partition of t1 for values from ( '2023-12-01') to ( '2024-01-01');
create table t1_13 partition of t1 for values from ( '2024-01-01') to ( '2024-02-01');
create table t1_14 partition of t1 for values from ( '2024-02-01') to ( '2024-03-01');
create table t1_15 partition of t1 for values from ( '2024-03-01') to ( '2024-04-01');
create table t1_16 partition of t1 for values from ( '2024-04-01') to ( '2024-05-01');
create table t1_17 partition of t1 for values from ( '2024-05-01') to ( '2024-06-01');
create table t1_18 partition of t1 for values from ( '2024-06-01') to ( '2024-07-01');
create table t1_19 partition of t1 for values from ( '2024-07-01') to ( '2024-08-01');
create index t1_test001 on t1 (f1);
create index t1_test002 on t1 (create_date);


insert into t1 select nextval('tt'), currval('tt'),generate_series(pg_catalog.date'2023-01-01',pg_catalog.date'2024-07-30','1 hour');

insert into t2 select nextval('qq'), currval('qq') + 1, generate_series(pg_catalog.date'2023-01-01',pg_catalog.date'2024-07-30','1 hour');

merge into t1 using t2 on t1.f1 = t2.f1 when matched then update set t1.f2 = t2.f2 when not matched then insert(f1, f2, create_date) values (99, '999', '2023-05-01');

explain (costs off) merge into t1 using t2 on t1.f1 = t2.f1 when matched then update set t1.f2 = t2.f2 when not matched then insert(f1, f2, create_date) values (99, '999', '2023-05-01');

analyze t1;
analyze t2;
set enable_seqscan TO off;
set enable_bitmapscan TO off;
explain (costs off) select f2 from t1 t where create_date >= pg_catalog.timestamp'2023-04-01' and create_date < pg_catalog.timestamp'2023-04-02' order by f2;
select f2 from t1 t where create_date >= pg_catalog.timestamp'2023-04-01' and create_date <  pg_catalog.timestamp'2023-04-02' order by f2;

select distinct f2 from t1 t where create_date >= pg_catalog.timestamp'2023-04-01' and create_date < pg_catalog.timestamp'2023-04-02' order by f2;
reset enable_seqscan;
reset enable_bitmapscan;

create user merge_r1;
grant all on t1 to merge_r1;
grant all on t2 to merge_r1;
grant all on t1_03 to merge_r1;

alter table t1 enable row level security ;
alter table t1_03 enable row level security ;

create policy p_t1 on t1 for insert to merge_r1 with check( create_date < '2023-03-20');
create policy p_t1_s on t1 for select to merge_r1 using( create_date < '2023-03-20');
create policy p_t1_03 on t1_03 for insert to merge_r1 with check( create_date < '2023-03-15');

set session authorization merge_r1;

merge into t1 using t2 on t1.f1 = t2.f1 when not matched then insert(f1, f2, create_date) values (99, '999', '2023-03-20');
merge into t1 using t2 on t1.f1 = t2.f1 when not matched then insert(f1, f2, create_date) values (99, '999', '2023-03-16');
merge into t1 using t2 on t1.f1 = t2.f1 when matched then update set t1.f2 = t2.f2;

reset session authorization;

create policy p_t1_up on t1 for update to merge_r1 with check( create_date < '2023-03-15');

set session authorization merge_r1;

merge into t1 using t2 on t1.f1 = t2.f1 when not matched then insert(f1, f2, create_date) values (99, '999', '2023-03-16');

merge into t1 using t2 on t1.f1 = t2.f1 when matched then update set t1.create_date = '2023-03-20' when not matched then insert(f1, f2, create_date) values (99, '999', '2023-03-14');

reset session authorization;

drop sequence if exists tt;
drop sequence if exists qq;
drop table if exists t1; 
drop table if exists t2; 
drop user merge_r1;

-- merge with dual
CREATE TABLE tlbb_server_log_ora (
    sn varchar2(50),
    group_id numeric(6,0),
    buy_time timestamp(0) without time zone,
    jewel_total numeric(10,0),
    cn varchar2(80),
    ip varchar2(20),
    weaponid varchar2(200),
    put_date timestamp(0) without time zone
)
PARTITION BY RANGE (put_date)
DISTRIBUTE BY SHARD (sn);

-- SET nls_timestamp_format = 'yyyy-mm-dd hh24:mi:ss';
CREATE TABLE tlbb_server_log_ora_2019 PARTITION OF tlbb_server_log_ora
FOR VALUES FROM ('2019-01-01 00:00:00') TO ('2020-01-01 00:00:00');
CREATE TABLE tlbb_server_log_ora_2020q1 PARTITION OF tlbb_server_log_ora
FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2020-04-01 00:00:00');
CREATE TABLE tlbb_server_log_ora_2020q2 PARTITION OF tlbb_server_log_ora
FOR VALUES FROM ('2020-04-01 00:00:00') TO ('2020-07-01 00:00:00');
CREATE TABLE tlbb_server_log_ora_2020q3 PARTITION OF tlbb_server_log_ora
FOR VALUES FROM ('2020-07-01 00:00:00') TO ('2020-10-01 00:00:00');
CREATE TABLE tlbb_server_log_ora_2020q4 PARTITION OF tlbb_server_log_ora
FOR VALUES FROM ('2020-10-01 00:00:00') TO ('2021-01-01 00:00:00');

explain (costs off)
MERGE INTO tlbb_server_log_ora T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

MERGE INTO tlbb_server_log_ora T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

select * from tlbb_server_log_ora;
select * from tlbb_server_log_ora_2020q1;
explain (costs off)
MERGE INTO tlbb_server_log_ora T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN MATCHED THEN UPDATE SET PUT_DATE = ('2020-10-29'::TIMESTAMP) WHERE T.GROUP_ID > 0
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

MERGE INTO tlbb_server_log_ora T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN MATCHED THEN UPDATE SET PUT_DATE = ('2020-10-29'::TIMESTAMP) WHERE T.GROUP_ID > 0
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

select * from tlbb_server_log_ora;
select * from tlbb_server_log_ora_2020q4;

truncate tlbb_server_log_ora;
explain (costs off)
MERGE INTO tlbb_server_log_ora T
USING (SELECT  * from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

MERGE INTO tlbb_server_log_ora T
USING (SELECT  * from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

select * from tlbb_server_log_ora;
select * from tlbb_server_log_ora_2020q1;

truncate tlbb_server_log_ora;
explain (costs off)
MERGE INTO tlbb_server_log_ora T
USING (SELECT  TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

MERGE INTO tlbb_server_log_ora T
USING (SELECT  TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

DROP TABLE IF EXISTS tlbb_server_src_s;
CREATE TABLE tlbb_server_src_s (
    sn varchar2(50),
    group_id numeric(6,0),
    buy_time timestamp(0) without time zone,
    jewel_total numeric(10,0),
    cn varchar2(80),
    ip varchar2(20),
    weaponid varchar2(200),
    put_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (sn);

DROP TABLE IF EXISTS tlbb_server_src_r;
CREATE TABLE tlbb_server_src_r (
    sn varchar2(50),
    group_id numeric(6,0),
    buy_time timestamp(0) without time zone,
    jewel_total numeric(10,0),
    cn varchar2(80),
    ip varchar2(20),
    weaponid varchar2(200),
    put_date timestamp(0) without time zone
)
DISTRIBUTE BY replication;

explain (costs off)
MERGE INTO tlbb_server_log_ora T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM tlbb_server_src_s) U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

explain (costs off)
MERGE INTO tlbb_server_log_ora T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, '2020-03-29 14:29:37'::TIMESTAMP AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, '2020-03-29'::TIMESTAMP AS PUT_DATE
		FROM tlbb_server_src_r) U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

select * from tlbb_server_log_ora;
select * from tlbb_server_log_ora_2020q1;

set enable_datanode_row_triggers to true;
create table merge_trg_rel_20230808(id int, name varchar2(100), salary int);
create table del_merge_trg_rel_20230808(id int, name varchar2(100), salary int);
create table new_merge_trg_rel_20230808(id int, name varchar2(100), salary int);
create view merge_trg_view_20230808_v as select * from merge_trg_rel_20230808 where salary > 15000;
insert into merge_trg_rel_20230808 values (1, 'tom', 10000);
insert into merge_trg_rel_20230808 values (2, 'jim', 13000);
insert into merge_trg_rel_20230808 values (3, 'lucy', 15000);
insert into new_merge_trg_rel_20230808 values (1, 'Tom', 12000);
insert into new_merge_trg_rel_20230808 values (2, 'Jim', 15000);
insert into new_merge_trg_rel_20230808 values (3, 'Lucy', 17000);
insert into new_merge_trg_rel_20230808 values (4, 'Jack', 20000);
-- test before insert or update trigger
CREATE OR REPLACE FUNCTION upd_ins_trg_20230808_f()
RETURNS TRIGGER AS
BEGIN
INSERT INTO del_merge_trg_rel_20230808 (id, name, salary) VALUES (NEW.id, NEW.name, NEW.salary + 1000);
RETURN NEW;
END;
/
CREATE TRIGGER be_up_ins_trg_20230808
    BEFORE INSERT OR UPDATE ON merge_trg_rel_20230808
                         FOR EACH ROW
                         EXECUTE FUNCTION upd_ins_trg_20230808_f();
MERGE INTO merge_trg_rel_20230808 e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        UPDATE SET e.name = n.name, e.salary = n.salary
    WHEN NOT MATCHED THEN
        INSERT (id, name, salary) VALUES (n.id, n.name, n.salary);
select * from merge_trg_rel_20230808 order by 1;
select * from new_merge_trg_rel_20230808 order by 1;
select * from del_merge_trg_rel_20230808 order by 1;
truncate table del_merge_trg_rel_20230808;
truncate table merge_trg_rel_20230808;
drop TRIGGER be_up_ins_trg_20230808 on merge_trg_rel_20230808;
-- test after insert or update trigger
insert into merge_trg_rel_20230808 values (1, 'tom', 10000);
CREATE TRIGGER af_up_ins_trg_20230808
    AFTER INSERT OR UPDATE ON merge_trg_rel_20230808
                        FOR EACH ROW
                        EXECUTE FUNCTION upd_ins_trg_20230808_f();
MERGE INTO merge_trg_rel_20230808 e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        UPDATE SET e.name = n.name, e.salary = n.salary
    WHEN NOT MATCHED THEN
        INSERT (id, name, salary) VALUES (n.id, n.name, n.salary);
select * from merge_trg_rel_20230808 order by 1;
select * from new_merge_trg_rel_20230808 order by 1;
select * from del_merge_trg_rel_20230808 order by 1;
truncate table del_merge_trg_rel_20230808;
truncate table merge_trg_rel_20230808;
drop TRIGGER af_up_ins_trg_20230808 on merge_trg_rel_20230808;
-- test before delete trigger
insert into merge_trg_rel_20230808 values (1, 'tom', 10000);
insert into merge_trg_rel_20230808 values (2, 'jim', 13000);
insert into merge_trg_rel_20230808 values (3, 'lucy', 15000);
CREATE OR REPLACE FUNCTION del_trg_20230808_f()
RETURNS TRIGGER AS
BEGIN
INSERT INTO del_merge_trg_rel_20230808 (id, name, salary)
VALUES (OLD.id, OLD.name, OLD.salary);
RETURN OLD;
END;
/
CREATE TRIGGER bef_del_trg_20230808
    AFTER DELETE ON merge_trg_rel_20230808
    FOR EACH ROW
    EXECUTE FUNCTION del_trg_20230808_f();
MERGE INTO merge_trg_rel_20230808 e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        update set e.name = n.name, e.salary = n.salary delete where e.id = 1;
select * from merge_trg_rel_20230808 order by 1;
select * from del_merge_trg_rel_20230808 order by 1;
truncate table del_merge_trg_rel_20230808;
truncate table merge_trg_rel_20230808;
drop TRIGGER bef_del_trg_20230808 on merge_trg_rel_20230808;
-- test after delete trigger
insert into merge_trg_rel_20230808 values (1, 'tom', 10000);
insert into merge_trg_rel_20230808 values (2, 'jim', 13000);
insert into merge_trg_rel_20230808 values (3, 'lucy', 15000);
CREATE TRIGGER aft_del_trg_20230808
    AFTER DELETE ON merge_trg_rel_20230808
    FOR EACH ROW
    EXECUTE FUNCTION del_trg_20230808_f();
MERGE INTO merge_trg_rel_20230808 e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        update set e.name = n.name, e.salary = n.salary delete where e.id = 1;
select * from merge_trg_rel_20230808 order by 1;
select * from del_merge_trg_rel_20230808 order by 1;
truncate table del_merge_trg_rel_20230808;
drop TRIGGER aft_del_trg_20230808 on merge_trg_rel_20230808;
-- error: test instead of insert or update trigger on view
CREATE TRIGGER ins_of_upins_trg_20230808
    INSTEAD OF INSERT OR UPDATE ON merge_trg_view_20230808_v
                             FOR EACH ROW
                             EXECUTE FUNCTION upd_ins_trg_20230808_f();
MERGE INTO merge_trg_view_20230808_v e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        UPDATE SET e.name = n.name, e.salary = n.salary
    WHEN NOT MATCHED THEN
        INSERT (id, name, salary) VALUES (n.id, n.name, n.salary);
drop TRIGGER ins_of_upins_trg_20230808 on merge_trg_view_20230808_v;
-- error: test instead of delete trigger on view
CREATE TRIGGER ins_of_del_trg_20230808
    INSTEAD OF DELETE ON merge_trg_view_20230808_v
    FOR EACH ROW
    EXECUTE FUNCTION del_trg_20230808_f();
MERGE INTO merge_trg_view_20230808_v e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        update set e.name = n.name, e.salary = n.salary delete where e.id = 1;
drop TRIGGER ins_of_del_trg_20230808 on merge_trg_view_20230808_v;
drop view merge_trg_view_20230808_v;
drop table merge_trg_rel_20230808;
drop table new_merge_trg_rel_20230808;
drop table del_merge_trg_rel_20230808;
drop function upd_ins_trg_20230808_f;
drop function del_trg_20230808_f;

-- BEGIN test place holder error issue
DROP TABLE IF EXISTS TMS_RISK_RPT_DET;
CREATE TABLE TMS_RISK_RPT_DET (
RISK_RPT_DET_ID NUMERIC(10,0) NOT NULL,
RISK_RPT_ID NUMERIC(10,0),
FUND NUMERIC(10,0),
BEGIN_BALANCE NUMERIC(17,2),
FIRST_MONTH_AMT NUMERIC(17,2),
SECOND_MONTH_AMT NUMERIC(17,2),
THIRD_MONTH_AMT NUMERIC(17,2),
TOTAL_AMT NUMERIC(17,2),
INVEST_INCOME NUMERIC(17,2),
OFF_SET_FEE NUMERIC(17,2),
ACCT_FEE NUMERIC(17,2),
STOP_BACK_FEE NUMERIC(17,2),
BACK_FEE NUMERIC(17,2),
CHANGE_TRUST_FEE NUMERIC(17,2),
OTHER_FEE NUMERIC(17,2),
END_BALANCE NUMERIC(17,2),
FCD DATE,
FCU NUMERIC(10,0),
LCD DATE,
LCU NUMERIC(10,0)
);
ALTER TABLE TMS_RISK_RPT_DET ADD CONSTRAINT PK_TMS_RISK_RPT_DET PRIMARY KEY (RISK_RPT_DET_ID);

DROP TABLE IF EXISTS TMS_FINA_FEE_UP_DET;
CREATE TABLE TMS_FINA_FEE_UP_DET (
FINA_FEE_UP_DET_ID NUMERIC(10,0) NOT NULL,
FEE_UPLOAD_ID NUMERIC(10,0) NOT NULL,
LINE_NUM NUMERIC(10,0),
OTHER_ACCT_CODE CHARACTER VARYING(30),
TRANS_AMT NUMERIC(15,2),
ACCOUNTING_DATE DATE,
MEMO CHARACTER VARYING(300),
BANLANCE NUMERIC(15,2),
ACCT_CODE CHARACTER VARYING(30),
ACCT_NAME CHARACTER VARYING(600),
IS_FEE NUMERIC(1,0),
FUND NUMERIC(10,0),
DET_STATUS NUMERIC(1,0),
FCD DATE,
FCU NUMERIC(10,0),
LCD DATE,
LCU NUMERIC(10,0),
OTHER_ACCT_NANE CHARACTER VARYING(600),
PRODUCT NUMERIC(10,0),
IS_IMPORT_VOUCHER CHARACTER VARYING(1),
FLOW_DIRECT CHARACTER VARYING(1),
RISK_BUSI_TYPE CHARACTER VARYING(1),
BUSI_FUND NUMERIC(10,0),
BUSI_MEMO CHARACTER VARYING(300),
BUSI_SET_USER NUMERIC(10,0),
BUSI_SET_DATE DATE,
WRITE_OFF_STATUS CHARACTER VARYING(1),
WRITE_OFF_DET_ID NUMERIC(10,0),
FINA_ACCOUNTING_DATE DATE,
NOT_FEE_MEMO CHARACTER VARYING(300),
DET_TYPE CHARACTER VARYING(1),
STATIS_ID NUMERIC(10,0),
COMPANY_ID NUMERIC(10,0),
FILE_ID NUMERIC(10,0),
FINA_UPLOAD_TASK_ID NUMERIC(10,0),
BUSI_MODEL_CONFIRM_FILE NUMERIC
);
ALTER TABLE TMS_FINA_FEE_UP_DET ADD CONSTRAINT PK_TMS_FINA_FEE_UP_DET PRIMARY KEY (FINA_FEE_UP_DET_ID);

DROP TABLE IF EXISTS TMS_FINA_FEE_UPLOAD;
CREATE TABLE TMS_FINA_FEE_UPLOAD (
FEE_UPLOAD_ID NUMERIC(10,0) NOT NULL,
FINA_FEE_TYPE NUMERIC(1,0),
UPLOAD_SC NUMERIC(1,0),
BEGIN_TRANS_DATE DATE,
END_TRANS_DATE DATE,
UPLOAD_FILE NUMERIC(10,0),
UPLOAD_USER NUMERIC(10,0),
UPLOAD_DATE DATE,
PARSE_RESULT_FILE NUMERIC(10,0),
UPLOAD_STATUS NUMERIC(1,0),
FCD DATE,
FCU NUMERIC(10,0), LCD DATE, LCU NUMERIC(10,0)
);
ALTER TABLE TMS_FINA_FEE_UPLOAD ADD CONSTRAINT PK_TMS_FINA_FEE_UPLOAD PRIMARY KEY (FEE_UPLOAD_ID);

DROP TABLE IF EXISTS TMS_RISK_RPT_TASK;
CREATE TABLE TMS_RISK_RPT_TASK (
RISK_RPT_TASK_ID NUMERIC(10,0) NOT NULL,
TRUSTEE NUMERIC(10,0),
TASK_PERIOD CHARACTER VARYING(6),
PERIOD_TYPE CHARACTER VARYING(1),
RPT_STATUS CHARACTER VARYING(1),
FCD DATE, FU NUMERIC(10,0), LCD DATE, LCU NUMERIC(10,0)
);
ALTER TABLE TMS_RISK_RPT_TASK ADD CONSTRAINT PK_TMS_RISK_RPT_TASK PRIMARY KEY (RISK_RPT_TASK_ID);

MERGE INTO TMS_RISK_RPT_DET D
USING (
SELECT NVL(D.FUND, C.FUND) AS FUND, C.FIRST_MONTH_AMT, C.SECOND_MONTH_AMT ,C.THIRD_MONTH_AMT, C.TOTAL_AMT, C.OFF_SET_FEE, C.ACCT_FEE, C.STOP_BACK_FEE ,C.BACK_FEE, C.CHANGE_TRUST_FEE, C.OTHER_FEE
FROM (
  SELECT NVL(A.FUND, B.FUND) AS FUND, A.FIR AS FIRST_MONTH_AMT, A.SEC AS SECOND_MONTH_AMT ,A.THIR AS THIRD_MONTH_AMT, (NVL(A.FIR, 0) + NVL(A.SEC, 0) + NVL(A.THIR, 0)) AS TOTAL_AMT,B.OFF_SET_FEE, B.ACCT_FEE, B.STOP_BACK_FEE, B.BACK_FEE, B.CHANGE_TRUST_FEE ,B.OTHER_FEE
  FROM (
    SELECT * FROM (
      SELECT D.FUND, (TO_CHAR(D.ACCOUNTING_DATE, 'MM') - SUBSTR(T.TASK_PERIOD, 5, 6) + 3) AS MON ,D.TRANS_AMT
       FROM TMS_FINA_FEE_UP_DET D, TMS_FINA_FEE_UPLOAD U, TMS_RISK_RPT_TASK T
       WHERE T.RISK_RPT_TASK_ID = 1
         AND D.ACCT_CODE = 1
         AND D.FEE_UPLOAD_ID=U.FEE_UPLOAD_ID
         AND D.IS_FEE = 1
         AND U.FINA_FEE_TYPE = 2
         AND D.WRITE_OFF_STATUS IS NULL
         AND D.DET_TYPE=2
         AND TO_CHAR(D.ACCOUNTING_DATE, 'YYYYMM') <= T.TASK_PERIOD
         AND TO_CHAR(ADD_MONTHS(D.ACCOUNTING_DATE, 2), 'YYYYMM') >= T.TASK_PERIOD
         )
         PIVOT (SUM(TRANS_AMT) FOR MON IN (1 AS FIR, 2 AS SEC, 3 AS THIR))
    ) A
    FULL JOIN (
      SELECT * FROM (
        SELECT D.BUSI_FUND AS FUND, D.RISK_BUSI_TYPE, D.TRANS_AMT
        FROM TMS_FINA_FEE_UP_DET D, TMS_FINA_FEE_UPLOAD U, TMS_RISK_RPT_TASK T
        WHERE T.RISK_RPT_TASK_ID = 1
          AND D.ACCT_CODE = 1
          AND D.FEE_UPLOAD_ID = U.FEE_UPLOAD_ID
          AND D.IS_FEE = 1
          AND U.FINA_FEE_TYPE =2
          AND D.WRITE_OFF_STATUS IS NULL
          AND D.FLOW_DIRECT=0
          AND D.DET_TYPE =4
          AND TO_CHAR(D.ACCOUNTING_DATE, 'YYYYMM') < T.TASK_PERIOD
          AND TO_CHAR(ADD_MONTHS(D.ACCOUNTING_DATE, 2), 'YYYYMM') >= T.TASK_PERIOD)
        PIVOT (SUM(TRANS_AMT) FOR RISK_BUSI_TYPE IN (1 AS OFF_SET_FEE, 2 AS ACCT_FEE, 3 AS STOP_BACK_FEE, 4 AS BACK_FEE, 5 AS CHANGE_TRUST_FEE, 6 AS OTHER_FEE))
      )B ON A.FUND = B.FUND
    )C
    FULL JOIN (
      SELECT *
      FROM TMS_RISK_RPT_DET DET
      WHERE DET.RISK_RPT_ID =1
    ) D ON D.FUND = C.FUND
  ) TMP
ON (D.FUND = TMP.FUND AND D.RISK_RPT_ID = 1)
WHEN MATCHED THEN UPDATE SET
  D.FIRST_MONTH_AMT = NVL(TMP.FIRST_MONTH_AMT, 0),
  D.SECOND_MONTH_AMT = NVL(TMP.SECOND_MONTH_AMT, 0),
  D.THIRD_MONTH_AMT = NVL(TMP.THIRD_MONTH_AMT, 0),
  D.TOTAL_AMT = NVL(TMP.TOTAL_AMT, 0),
  D.OFF_SET_FEE =NVL(TMP.OFF_SET_FEE, 0),
  D.ACCT_FEE = NVL(TMP.ACCT_FEE, 0),
  D.STOP_BACK_FEE= NVL(TMP.STOP_BACK_FEE, 0),
  D.BACK_FEE= NVL(TMP.BACK_FEE, 0),
  D.CHANGE_TRUST_FEE=NVL(TMP.CHANGE_TRUST_FEE, 0),
  D.OTHER_FEE = NVL(TMP.OTHER_FEE, 0),
  D.END_BALANCE = NVL(D.BEGIN_BALANCE, 0) + NVL(TMP.TOTAL_AMT, 0) + NVL(D.INVEST_INCOME, 0) + NVL(TMP.OFF_SET_FEE, 0) + NVL(TMP.ACCT_FEE, 0) + NVL(TMP.STOP_BACK_FEE, 0) + NVL(TMP.BACK_FEE, 0) + NVL(TMP.CHANGE_TRUST_FEE, 0) + NVL(TMP.OTHER_FEE, 0),
  D.LCD = SYSDATE, D.LCU =1
WHEN NOT MATCHED THEN INSERT (RISK_RPT_DET_ID, RISK_RPT_ID, FUND, BEGIN_BALANCE, FIRST_MONTH_AMT, SECOND_MONTH_AMT, THIRD_MONTH_AMT, TOTAL_AMT, INVEST_INCOME, OFF_SET_FEE, ACCT_FEE, STOP_BACK_FEE, BACK_FEE, CHANGE_TRUST_FEE, OTHER_FEE, END_BALANCE, FCD, FCU, LCD, LCU)
  VALUES (1, 1, TMP.FUND, 0, NVL(TMP.FIRST_MONTH_AMT, 0), NVL(TMP.SECOND_MONTH_AMT, 0), NVL(TMP.THIRD_MONTH_AMT,0), NVL(TMP.TOTAL_AMT, 0), 0, NVL(TMP.OFF_SET_FEE, 0), NVL(TMP.ACCT_FEE, 0), NVL(TMP.STOP_BACK_FEE, 0), NVL(TMP.BACK_FEE, 0), NVL(TMP.CHANGE_TRUST_FEE, 0), NVL(TMP.OTHER_FEE, 0), NVL(TMP.TOTAL_AMT, 0) + NVL (TMP.OFF_SET_FEE, 0) + NVL(TMP.ACCT_FEE,0) + NVL(TMP.STOP_BACK_FEE, 0) + NVL(TMP.BACK_FEE, 0) + NVL(TMP.CHANGE_TRUST_FEE, 0) + NVL(TMP.OTHER_FEE, 0), SYSDATE, 1, SYSDATE, 1);

DROP TABLE TMS_RISK_RPT_DET;
DROP TABLE TMS_FINA_FEE_UP_DET;
DROP TABLE TMS_FINA_FEE_UPLOAD;
DROP TABLE TMS_RISK_RPT_TASK;
-- END test place holder

--TAPD: 122764029
drop table if exists t1_pk_20220725_1 cascade;
drop table if exists t1_pk_20220725_3 cascade;

CREATE TABLE t1_pk_20220725_1(id int, name varchar(20), age int, constraint t1_pk_20220725_1_01 primary KEY(id));
CREATE table t1_pk_20220725_3 (id int, name varchar(20), age int);

INSERT INTO t1_pk_20220725_1 values(1, 'a', 10);
INSERT INTO t1_pk_20220725_1 values(2, 'b', 20);
INSERT INTO t1_pk_20220725_1 values(3, 'c', 30);
INSERT INTO t1_pk_20220725_3 values(3, 'g', 30);
INSERT INTO t1_pk_20220725_3 values(4, 'h', 40);

drop view if exists v_merge_view_20220725_6 cascade;
create view v_merge_view_20220725_6 as select id,name,age from t1_pk_20220725_1 where exists(select distinct id from t1_pk_20220725_1 group by id having id>1);

\set QUIET off
MERGE into v_merge_view_20220725_6 t USING t1_pk_20220725_3 s ON (t.id=s.id)
WHEN MATCHED THEN UPDATE SET t.name='p',t.age=100
WHEN NOT MATCHED THEN INSERT (t.id, t.name, t.age) values(s.id, s.name, s.age);
\set QUIET on
SELECT t1.id, t1.name, t1.age FROM t1_pk_20220725_1 t1 order by 1,2,3;
--END TAPD: 122764029
\set QUIET off
create table tb_parallel_01(
   id   number primary key,
   col1 number,
   col2 number(5,2),
   col3 char(50),
   col4 varchar2(150),
   col5 date,
   col6 timestamp
 );
 create index  idx_parallel_01_01 on tb_parallel_01(id,col1);
 create index  idx_parallel_01_02 on tb_parallel_01(col1);
 create sequence seq_update_parall_01 START WITH 1 ;
 CREATE OR REPLACE PROCEDURE p_insert_data( starts IN NUMBER,ends IN NUMBER) IS
     v_sql varchar2(5000);
 BEGIN
     FOR i IN starts..ends  LOOP
     	insert into tb_parallel_01 values(seq_update_parall_01.nextval,mod(i,25),null,lpad('hi'||i,5,'ok'),lpad('中国'||i,10,'good'),  null,TO_TIMESTAMP('2024-03-08 12:00:00', 'YYYY-MM-DD HH24:MI:SS'));
         insert into tb_parallel_01 values(seq_update_parall_01.nextval,mod(i,5),trunc(i/10,2),null,lpad('俄罗斯'||i,10,'good'),  TO_DATE('2024-04-08', 'YYYY-MM-DD')+round(i/30),null);
         insert into tb_parallel_01 values(seq_update_parall_01.nextval,null,trunc(i/25,2),lpad('hello'||i,5,'ok'),null,TO_DATE('2024-03-08', 'YYYY-MM-DD')+round(i/30),TO_TIMESTAMP('2024-04-08 12:00:00', 'YYYY-MM-DD HH24:MI:SS'));
     END LOOP;
     COMMIT;
 END p_insert_data;
 /
 create table tb_parallel_02 as select * from tb_parallel_01 where 0>1;
drop table tb_parallel_01 cascade;
drop table tb_parallel_02 cascade;
drop sequence seq_update_parall_01;
drop PROCEDURE p_insert_data(NUMBER, NUMBER);

\set QUIET on

DROP TABLE if exists rqg_table3 cascade;
CREATE TABLE rqg_table3 (
c0 int,
c1 int,
c2 varchar2(200),
c3 varchar2(200),
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)    PARTITION BY hash( c4);
create TABLE rqg_table3_p0 partition of rqg_table3 for values with(modulus 2,remainder 0);
create TABLE rqg_table3_p1 partition of rqg_table3 for values with(modulus 2,remainder 1);
CREATE INDEX idx_rqg_table3_c1 ON rqg_table3(c1);
CREATE INDEX idx_rqg_table3_c3 ON rqg_table3(c3);
CREATE INDEX idx_rqg_table3_c5 ON rqg_table3(c5);
CREATE INDEX idx_rqg_table3_c7 ON rqg_table3(c7);
CREATE INDEX idx_rqg_table3_c9 ON rqg_table3(c9);

DROP TABLE if exists rqg_table5 cascade;
CREATE TABLE rqg_table5 (
c0 int,
c1 int,
c2 varchar2(200),
c3 varchar2(200),
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)    PARTITION BY hash( c4);
create TABLE rqg_table5_p0 partition of rqg_table5 for values with(modulus 4,remainder 0);
create TABLE rqg_table5_p1 partition of rqg_table5 for values with(modulus 4,remainder 1);
create TABLE rqg_table5_p2 partition of rqg_table5 for values with(modulus 4,remainder 2);
create TABLE rqg_table5_p3 partition of rqg_table5 for values with(modulus 4,remainder 3);
CREATE INDEX idx_rqg_table5_c1 ON rqg_table5(c1);
CREATE INDEX idx_rqg_table5_c3 ON rqg_table5(c3);
CREATE INDEX idx_rqg_table5_c5 ON rqg_table5(c5);
CREATE INDEX idx_rqg_table5_c7 ON rqg_table5(c7);
CREATE INDEX idx_rqg_table5_c9 ON rqg_table5(c9);

explain (analyze, costs off, timing off, nodes off, summary off) merge into rqg_table3 t1 using rqg_table5 t2 on 1=1
  when matched then update set c1=1
  when not matched then insert (c0) values(1)
       where t1.c8 = 1;
DROP TABLE if exists rqg_table5 cascade;
DROP TABLE if exists rqg_table3 cascade;
-- TEST rqg merge into insert default values report error data type mismatch
\c regression
create schema chqin;
set search_path to chqin;

CREATE TABLE rqg_table5 (                  c0 int,
c1 int default null,
c2 varchar (2000),
c3 varchar (2000) default null,
c4 date,
c5 date default null,
c6 timestamp,
c7 timestamp default null,
c8 time,
c9 time default null)
;

CREATE OR REPLACE VIEW view_rqg_table5_1 AS SELECT * FROM rqg_table5
;

CREATE TABLE salgrade (
    grade int,
    losal int,
    hisal int
) ;

INSERT INTO SALGRADE VALUES (1,700,1200);
INSERT INTO SALGRADE VALUES (2,1201,1400);
INSERT INTO SALGRADE VALUES (3,1401,2000);
INSERT INTO SALGRADE VALUES (4,2001,3000);
INSERT INTO SALGRADE VALUES (5,3001,9999);

CREATE TABLE members (
    member_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    join_date DATE NOT NULL
);

CREATE TABLE loans (
    loan_id SERIAL PRIMARY KEY,
    book_id INT,
    member_id INT,
    loan_date DATE,
    return_date DATE
);


WITH name AS (
SELECT DISTINCT m.member_id, m.email, l.book_id
FROM chqin.members m
JOIN chqin.loans l ON m.member_id = l.member_id
WHERE m.join_date > '2023-01-01'
WINDOW w AS (PARTITION BY m.member_id ORDER BY l.book_id)
)
MERGE INTO chqin.salgrade AS sg
USING (
SELECT c0, c1, c2, c3, c4, c5, c6, c7, c8, c9
FROM chqin.view_rqg_table5_1
) AS vt
ON sg.grade = vt.c0
WHEN MATCHED AND sg.losal < vt.c1 THEN
UPDATE SET losal = vt.c1
WHEN NOT MATCHED THEN
INSERT DEFAULT VALUES
WHEN MATCHED AND sg.hisal > vt.c1 THEN
DELETE;

drop schema chqin cascade;
