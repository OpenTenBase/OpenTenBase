\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;
-- prepare
select current_database();

-- all types

drop table if exists allrowtest;

create table allrowtest(
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

insert into allrowtest 
	select i,i,1024,bool(i),'0123456789'::name,i,i,i,'0123456789'::text,'0123456789'::varchar,'0123456789'::varchar,(i)::varchar::money,'2018-10-26','2018-10-26','2018-10-26','2018-10-26'::timestamp,'2018-10-26'::timestamp with time zone,i * interval '1 day',abstime('2018-10-26'), i * interval '1 day',tinterval(abstime('2018-10-26'::timestamp + i * interval '1 min'),abstime('2018-10-26')), box(point(1,1),point(i,i)),
	line (point(1,1), point(i,i)), path (polygon (box(point(i,i),point(i,i+1)))), point (i,i+1), lseg (point(i,i),point(i+1,i+1)), polygon (box(point(i,i),point(i,i+1))), circle(point(i,i+1),i+2), '92.168.100.128/25'::inet, '08-00-2b-01-02-03'::macaddr from generate_series(1,500) i ;

-- partition table
-- list
drop table if exists rowlist;

CREATE TABLE rowlist (
c1 int, c2 char(30), c3 float4, c4 date
) PARTITION BY LIST (c1);
CREATE TABLE rlist1 PARTITION OF rowlist FOR VALUES IN (1);
CREATE TABLE rlist2 PARTITION OF rowlist FOR VALUES IN (2);
CREATE TABLE rlist3 PARTITION OF rowlist FOR VALUES IN (3);
CREATE TABLE rlist4 PARTITION OF rowlist FOR VALUES IN (4);
CREATE TABLE rlist5 PARTITION OF rowlist FOR VALUES IN (5);

insert into rowlist
	select i%5+1, 'b', i/2.0, '2018-01-01'::date+ i * interval '1 second' from generate_series(1,500) i;

-- range
drop table if exists rowrange;

CREATE TABLE rowrange (
c1 int, c2 char(30), c3 float4, c4 date
) PARTITION BY range (c4);
CREATE TABLE rrange1 PARTITION OF rowrange FOR VALUES FROM ('2018-01-01') TO ('2018-07-01');
CREATE TABLE rrange2 PARTITION OF rowrange FOR VALUES FROM ('2018-07-01') TO ('2019-01-01');
CREATE TABLE rrange3 PARTITION OF rowrange FOR VALUES FROM ('2019-01-01') TO ('2019-07-01');
CREATE TABLE rrange4 PARTITION OF rowrange FOR VALUES FROM ('2019-07-01') TO ('2020-01-01');
CREATE TABLE rrange5 PARTITION OF rowrange FOR VALUES FROM ('2020-01-01') TO ('2020-07-01');

insert into rowrange
	select i, 'abc', i/2.0, '2018-01-01'::date+ i * interval '1 second' from generate_series(1,500) i;

-- rowtest
delete from allrowtest where c1 < 10 ;
delete from allrowtest where c1 = 100;
delete from allrowtest where c1 > 100 and c1 < 122;
select * from allrowtest order by c1;

delete from rowlist where c3 = 4.5;
delete from rowlist where c3 < 100;
delete from rowlist where c3 < 10;
delete from rowlist where c1 = 4;
select * from rowlist order by c3;

delete from rowrange where c3 = 4.5;
delete from rowrange where c3 < 100;
delete from rowrange where c3 < 101;
delete from rowrange where c1 = 4;
select * from rowrange order by c3;

drop table if exists allrowtest;
drop table if exists rowlist;
drop table if exists rowrange;

reset client_min_messages;
reset datestyle;
reset client_encoding;
