--
-- MULTIPLE DISTRIBUTIONS
--

-- test join plans for multi-distribution
create table create_test_r (c1 int8, c2 varchar(64) , c3 varchar(64) ,  c4 bigint , c6 int) distribute by shard (c1,c2,c4);
\d+ create_test_r;
insert into create_test_r select i, '2006-02-22', i,  i from generate_series(1, 10) i;


select * from create_test_r order by c1;

-- number of dist-values less than number of distributions.
explain (costs off) select * from create_test_r where c1 = 5 and c4 = 5 order by c1;
select * from create_test_r where c1 = 5 and c4 = 5 order by c1;
explain (costs off) select * from create_test_r where c1 = 3 and c4 = 3 order by c1;
select * from create_test_r where c1 = 3 and c4 = 3 order by c1;

-- CTAS
drop table if exists aa;
create table aa(id int, name text) distribute by shard(id);
insert into aa select i, 'opentenbase'||i from generate_series(1, 100) i;
select count(*) from aa;
drop table if exists create_test_r ;
drop table aa;
