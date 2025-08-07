CREATE TABLE delete_test (
    id SERIAL PRIMARY KEY,
    a INT,
    b text
);

INSERT INTO delete_test (a) VALUES (10);
INSERT INTO delete_test (a, b) VALUES (50, repeat('x', 10000));
INSERT INTO delete_test (a) VALUES (100);

-- allow an alias to be specified for DELETE's target table
DELETE FROM delete_test AS dt WHERE dt.a > 75;

-- if an alias is specified, don't allow the original table name
-- to be referenced
DELETE FROM delete_test dt WHERE delete_test.a > 25;

SELECT id, a, char_length(b) FROM delete_test ORDER BY id;

-- delete a row with a TOASTed value
DELETE FROM delete_test WHERE a > 25;

SELECT id, a, char_length(b) FROM delete_test ORDER BY id;

DROP TABLE delete_test;

create table t1(f11 int, f12 int) distribute by replication;
create table t2(f21 int, f22 int);

explain (costs off) delete from t1 using t2 where t1.f11=t2.f21;
explain (costs off) delete from t2 using t1 where t1.f11=t2.f21;
explain (costs off) delete from t1 test1 using t1 test2 
                    where test2.f11 <= some(select distinct max(f11)
			                    over(partition by f12)
					    from t1);
drop table t1;
drop table t2;

create table test_v(id varchar(10),id1 varchar(10),primary key(id,id1)) distribute by shard(id);
create table test1_v(id varchar(10),id1 varchar(10),primary key(id,id1)) distribute by shard(id1);

explain (costs off)
delete from test_v t1
where exists(select 1 from test1_v t2
             where t1.id=t2.id and t1.id1=t2.id1);

drop table test_v;
drop table test1_v;

CREATE TABLE rqg_table2401 (
c0 int,
c1 int);
CREATE TABLE rqg_table2402 (
c0 int,
c1 int) distribute by replication;
CREATE TABLE rqg_table2403 (
c0 int,
c1 int);

DELETE FROM rqg_table2402 m1 USING (
rqg_table2403 AS a1 INNER JOIN rqg_table2401 AS a2 ON a1.c0 = a2.c0
INNER JOIN rqg_table2401 AS a3 ON a1.c0 = a3.c0);

drop table rqg_table2401;
drop table rqg_table2402;
drop table rqg_table2403;

drop table if exists dt1 cascade;
drop table if exists dt2 cascade;
drop table if exists dt3 cascade;
create table dt1(c0 int, c1 int, c2 int, c3 int);
create table dt2(c0 int, c1 int, c2 int, c3 int);
create table dt3(c0 int, c1 int, c2 int, c3 int);

explain (costs off)
delete from dt1 where c3 not in (select c1 from (select distinct (select c2 from dt2 where c3=c2) from dt3 where dt3.c1 > 100));

drop table if exists dt1 cascade;
drop table if exists dt2 cascade;
drop table if exists dt3 cascade;