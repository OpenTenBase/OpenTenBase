set enable_async_fragment_executor to on;
set enable_nestloop to off;
set enable_mergejoin to off;

create table rowtest(
 c1 int,
 c2 bigint,
 c3 int2,
 c4 bool,
 c5 name, 
 c6 float4,
 c7 float8,
 c9 numeric,
 c10 text,
 c11 char(100),
 c12 varchar,
 c13 money,
 c14 date,
 c15 timestamp,
 c16 timestamp with time zone,
 c17 time,
 c18 time with time zone,
 c19 interval,
 c20 abstime,  
 c21 reltime,
 c22 tinterval,
 c23 box,
 c24 line,
 c25 path,
 c26 point,
 c27 lseg,
 c28 polygon,
 c29 circle,
 c30 inet,
 c31 macaddr
);


create table rowtest1(
 c1 int,
 c2 bigint,
 c3 int2,
 c4 bool,
 c5 name, 
 c6 float4,
 c7 float8,
 c9 numeric,
 c10 text,
 c11 char(100),
 c12 varchar,
 c13 money,
 c14 date,
 c15 timestamp,
 c16 timestamp with time zone,
 c17 time,
 c18 time with time zone,
 c19 interval,
 c20 abstime,  
 c21 reltime,
 c22 tinterval,
 c23 box,
 c24 line,
 c25 path,
 c26 point,
 c27 lseg,
 c28 polygon,
 c29 circle,
 c30 inet,
 c31 macaddr
);

create table rowtest2(
 c1 int,
 c2 bigint,
 c3 int2,
 c4 bool,
 c5 name, 
 c6 float4,
 c7 float8,
 c9 numeric,
 c10 text,
 c11 char(100),
 c12 varchar,
 c13 money,
 c14 date,
 c15 timestamp,
 c16 timestamp with time zone,
 c17 time,
 c18 time with time zone,
 c19 interval,
 c20 abstime,  
 c21 reltime,
 c22 tinterval,
 c23 box,
 c24 line,
 c25 path,
 c26 point,
 c27 lseg,
 c28 polygon,
 c29 circle,
 c30 inet,
 c31 macaddr
);

create table rowtest3(
 c1 int,
 c2 bigint,
 c3 int2,
 c4 bool,
 c5 name, 
 c6 float4,
 c7 float8,
 c9 numeric,
 c10 text,
 c11 char(100),
 c12 varchar,
 c13 money,
 c14 date,
 c15 timestamp,
 c16 timestamp with time zone,
 c17 time,
 c18 time with time zone,
 c19 interval,
 c20 abstime,  
 c21 reltime,
 c22 tinterval,
 c23 box,
 c24 line,
 c25 path,
 c26 point,
 c27 lseg,
 c28 polygon,
 c29 circle,
 c30 inet,
 c31 macaddr
);

-- end query
insert into rowtest values(1,1,1024,bool(1),'0123456789'::name,1,1,1,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(1)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,interval '1 day',abstime('2018-10-26'),interval '1 day',tinterval(abstime('2018-10-26'::timestamp + interval '1 min'),abstime('2018-10-26')), box'((1,1),(2,2))',
line '((1,1),(2,2))', path '((1,1),(2,2),(2,3))', point '(1,2)', lseg '((1,1),(2,2))', polygon '((1,1),(1,2),(2,2),(2,1))', circle'((1,2),3)', '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr);

insert into rowtest1 select i,i,1024,bool(i),'0123456789'::name,i,i,i,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(i)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,10000) i;


select * from rowtest, rowtest1 where rowtest.c1 = rowtest1.c2;

-- null value
truncate table rowtest;
truncate table rowtest1;

insert into rowtest values(1,1,NULL,bool('0123456789'::int),'0123456789'::name,1,1,1,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(1)::varchar::money,NULL,'2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,interval '1 day',NULL,interval '1 day',NULL, box'((1,1),(2,2))',
line '((1,1),(2,2))', NULL, point '(1,2)', lseg '((1,1),(2,2))', polygon '((1,1),(1,2),(2,2),(2,1))', NULL, '92.168.100.128/25'::inet, NULL);

insert into rowtest1 select i,i,1024,NULL,'0123456789'::name,i,i,i,NULL,'0123456789'::varchar,'0123456789'::varchar,(i)::varchar::money,NULL,'2018-10-26','2018-10-26',NULL,'2018-10-26'::timestamp with time zone,i * interval '1 day',NULL, i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
 NULL, path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), NULL, polygon (box(point(i,i),point(i,i+1))), NULL, '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,10000) i;

select * from rowtest, rowtest1 where rowtest.c2 = rowtest1.c2;

-- normal case
truncate table rowtest;
truncate table rowtest1;

insert into rowtest select i,i,1024,bool('0123456789'::int),'0123456789'::name,i,i,i,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(i)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,10000) i;

insert into rowtest1 select i,i+9999,1024,bool('0123456789'::int),'0123456789'::name,i+9999,i+9999,i+9999,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(i * 2)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,10000) i;

select * from rowtest,rowtest1 where rowtest.c7 = rowtest1.c9;

-- fixed length long tuple 
truncate table rowtest;
truncate table rowtest1;

insert into rowtest select i,i,1024,bool('0123456789'::int),'0123456789'::name,i,i,i,repeat('0123456789'::text,1024),'0123456789'::varchar,repeat('0123456789'::text,1024),(i)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,1000) i;

insert into rowtest1 select i,i+999,1024,bool('0123456789'::int),'0123456789'::name,i+999,i+999,i+999,repeat('0123456789'::text,1024),'0123456789'::varchar,repeat('0123456789'::text,1024),(i * 2)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,1000) i;

select * from rowtest,rowtest1 where rowtest.c2 = rowtest1.c2;

-- random length long tuple
truncate table rowtest;
truncate table rowtest1;

insert into rowtest select i,i,1024,bool('0123456789'::int),'0123456789'::name,i,i,i,repeat('0123456789'::text,1024),'0123456789'::varchar,repeat('0123456789'::text,1024),(i)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,100) i;

insert into rowtest1 select i,i+99,1024,bool('0123456789'::int),'0123456789'::name,i+99,i+99,i+99,repeat('0123456789'::text,1024),'0123456789'::varchar,repeat('0123456789'::text,1024),(i * 2)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,100) i;

select * from rowtest,rowtest1 where rowtest.c7 = rowtest1.c9;

truncate table rowtest;
truncate table rowtest1;

-- multi tables join

-- end query
insert into rowtest values(1,1,NULL,bool('0123456789'::int),'0123456789'::name,1,1,1,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(1)::varchar::money,NULL,'2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,interval '1 day',NULL,interval '1 day',NULL, box'((1,1),(2,2))',
line '((1,1),(2,2))', NULL, point '(1,2)', lseg '((1,1),(2,2))', polygon '((1,1),(1,2),(2,2),(2,1))', NULL, '92.168.100.128/25'::inet, NULL);

insert into rowtest1 select i,i,1024,bool('0123456789'::int),'0123456789'::name,i,i,i,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(i)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,10000) i;

insert into rowtest2 values(1,1,NULL,bool('0123456789'::int),'0123456789'::name,1,1,1,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(1)::varchar::money,NULL,'2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,interval '1 day',NULL,interval '1 day',NULL, box'((1,1),(2,2))',
line '((1,1),(2,2))', NULL, point '(1,2)', lseg '((1,1),(2,2))', polygon '((1,1),(1,2),(2,2),(2,1))', NULL, '92.168.100.128/25'::inet, NULL);

insert into rowtest2 values(2,2,NULL,bool('0123456789'::int),'0123456789'::name,2,2,2,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(1)::varchar::money,NULL,'2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,interval '1 day',NULL,interval '1 day',NULL, box'((1,1),(2,2))',
line '((1,1),(2,2))', NULL, point '(1,2)', lseg '((1,1),(2,2))', polygon '((1,1),(1,2),(2,2),(2,1))', NULL, '92.168.100.128/25'::inet, NULL);

insert into rowtest3 select i,i,1024,bool('0123456789'::int),'0123456789'::name,i,i,i,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(i)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,10000) i;

select * from rowtest,rowtest2,rowtest1 where rowtest.c1 = rowtest2.c2 and rowtest2.c6 = rowtest1.c1;
select * from  (select rowtest.* from rowtest,rowtest1 where rowtest.c2 = rowtest1.c2) a,(select rowtest2.* from rowtest2,rowtest3 where rowtest2.c9 = rowtest3.c1) b where a.c2 = b.c2;

-- random length long tuple
truncate table rowtest;
truncate table rowtest1;
truncate table rowtest2;
truncate table rowtest3;

insert into rowtest select i,i,1024,bool('0123456789'::int),'0123456789'::name,i,i,i,repeat('0123456789'::text,1024),'0123456789'::varchar,repeat('0123456789'::text,1024),(i)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,100) i;

insert into rowtest1 select i,i,1024,bool('0123456789'::int),'0123456789'::name,i,i,i,repeat('0123456789'::text,1024),'0123456789'::varchar,repeat('0123456789'::text,1024),(i * 2)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,100) i;

insert into rowtest2 select i,i+99,1024,bool('0123456789'::int),'0123456789'::name,i,i,i,repeat('0123456789'::text,1024),'0123456789'::varchar,repeat('0123456789'::text,1024),(i * 4)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,100) i;

insert into rowtest3 select i,i,1024,bool('0123456789'::int),'0123456789'::name,i+99,i,i,repeat('0123456789'::text,1024),'0123456789'::varchar,repeat('0123456789'::text,1024),(i * 8)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,100) i;

select * from rowtest,rowtest1,rowtest2 where rowtest.c1 = rowtest1.c1 and rowtest.c1 = rowtest2.c2;
select * from (select rowtest2.* from rowtest,rowtest2 where rowtest.c1 = rowtest2.c2) a,  (select rowtest3.* from rowtest1,rowtest3 where rowtest1.c1 = rowtest3.c6) b where a.c1 = b.c2;

drop table rowtest;
drop table rowtest1;
drop table rowtest2;
drop table rowtest3;
