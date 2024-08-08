-- create table
CREATE TABLE public.t1( 
    f1 int not null,
    f2 varchar(20),
    primary key(f1) 
)  distribute by shard(f1) to group default_group;  

-- insert data
INSERT INTO t1 VALUES(1,'Tbase'),(2,'pg');

-- select data
SELECT * FROM t1;

-- update data
explain UPDATE t1 SET f2='tbase' where f1=1;
