--explain analyze
create table a1(id int, num int, name text);
create table a2(id int, num int, name text);
create table a3(id int, num int, name text default 'def');
insert into a1 values(1,generate_series(1,100),'a');
insert into a1 values(2,generate_series(1,100),'b');
insert into a1 values(3,generate_series(1,100),'c');
insert into a2 select * from a1;

--setting
explain (costs off,settings) select * from a1;


--normal cases
explain (costs off,timing off,summary off,analyze,verbose)
select count(*) from a1;
explain (costs off,timing off,summary off,analyze,verbose)
select num, count(*) cnt from a2 group by num order by cnt;
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1, a2 where a1.num = a2.num;


--append
explain (costs off,timing off,summary off,analyze,verbose)
select max(num) from a1 union select min(num) from a1 order by 1;


--subplan
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1 where id in (select count(*) from a2 where a1.num=a2.num);


--initplan
explain (costs off,summary off)
select * from a1 where num >= (select count(*) from a2 where name='a');
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1 where num >= (select count(*) from a2 where name='a');

explain (costs off,summary off)
select * from a1 where num >= (select count(*) from a2 where name='b') order by id;
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1 where num >= (select count(*) from a2 where name='b') order by id;

explain (costs off,summary off)
select * from a1 where num >= (select count(*) from a2 where name='c') limit 1;
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1 where num >= (select count(*) from a2 where name='c') limit 1;

explain (costs off,summary off)
select count(*) from a1 group by name having count(*) = (select count(*) from a2 where name='a');
explain (costs off,timing off,summary off,analyze,verbose)
select count(*) from a1 group by name having count(*) = (select count(*) from a2 where name='a');

explain (costs off,summary off)
insert into a3 values(10, 20, '40'), (-1, 2, DEFAULT),
    ((select count(*) from a1), (select i from (values(3)) as foo (i)), 'values are fun!');
insert into a3 values(10, 20, '40'), (-1, 2, DEFAULT),
    ((select count(*) from a1), (select i from (values(3)) as foo (i)), 'values are fun!');
select * from a3 order by 1,2,3;

explain (costs off,summary off)
select name, count(*) from a1 where num >= (select count(*) from a2 where name='a') group by name;
explain (costs off,timing off,summary off,analyze,verbose)
select name, count(*) from a1 where num >= (select count(*) from a2 where name='a') group by name;


--ctescan
explain (costs off,summary off)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num) SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b;
explain (costs off,timing off,summary off,analyze,verbose)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num) SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b;

explain (costs off,summary off)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num)
SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b and exists (select count(*) from a2);
explain (costs off,timing off,summary off,analyze,verbose)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num)
SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b and exists (select count(*) from a2);

explain (costs off,summary off)
WITH v1 AS (SELECT num a, count(*) b FROM a1 group by num),
 v2 AS (SELECT num a, count(*) b FROM a2 group by num)
SELECT * FROM v1, v2, v1 v3, v2 v4 WHERE v1.a = v2.a and v3.b = v4.b and v2.a=v3.b;
explain (costs off,timing off,summary off,analyze,verbose)
WITH v1 AS (SELECT num a, count(*) b FROM a1 group by num),
 v2 AS (SELECT num a, count(*) b FROM a2 group by num)
SELECT * FROM v1, v2, v1 v3, v2 v4 WHERE v1.a = v2.a and v3.b = v4.b and v2.a=v3.b;

explain (costs off,summary off)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num)
SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b and exists (select count(*) from a2 where a2.num = v2.a);
explain (costs off,timing off,summary off,analyze,verbose)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num)
SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b and exists (select count(*) from a2 where a2.num = v2.a);

--shared ctescan
set cte_optimizer = on;
explain (costs off,summary off)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num) SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b;
explain (costs off,timing off,summary off,analyze,verbose)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num) SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b;

explain (costs off,summary off)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num)
SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b and exists (select count(*) from a2);
explain (costs off,timing off,summary off,analyze,verbose)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num)
SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b and exists (select count(*) from a2);

explain (costs off,summary off)
WITH v1 AS (SELECT num a, count(*) b FROM a1 group by num),
 v2 AS (SELECT num a, count(*) b FROM a2 group by num)
SELECT * FROM v1, v2, v1 v3, v2 v4 WHERE v1.a = v2.a and v3.b = v4.b and v2.a=v3.b;
/* wrong explain analyze datanode
explain (costs off,timing off,summary off,analyze,verbose)
WITH v1 AS (SELECT num a, count(*) b FROM a1 group by num),
 v2 AS (SELECT num a, count(*) b FROM a2 group by num)
SELECT * FROM v1, v2, v1 v3, v2 v4 WHERE v1.a = v2.a and v3.b = v4.b and v2.a=v3.b;
*/

explain (costs off,summary off)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num)
SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b and exists (select count(*) from a2 where a2.num = v2.a);
explain (costs off,timing off,summary off,analyze,verbose)
WITH v AS (SELECT num a, count(*) b FROM a1 group by num)
SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b and exists (select count(*) from a2 where a2.num = v2.a);
WITH v AS (SELECT num a, count(*) b FROM a1 group by num)
SELECT * FROM v AS v1, v AS v2 WHERE v1.a = v2.b and exists (select count(*) from a2 where a2.num = v2.a) order by 3;
set cte_optimizer = off;


--parallel
--set max_parallel_workers_per_gather to 2;
--set parallel_tuple_cost to 0;
--set parallel_setup_cost to 0;
--set min_parallel_table_scan_size to 0;
--parallel sort
--explain (costs off,timing off,summary off,analyze)
--select * from a1 order by num;
--parallel hashjoin
--explain (costs off,timing off,summary off,analyze)
--select * from a1,a2 where a1.id = a2.num;
--parallel hashagg
--explain (costs off,timing off,summary off,analyze)
--select name, count(*) cnt, sum(num) sum from a1 group by name having sum(num) > 1000 order by cnt;


--cleanup
drop table a1, a2, a3;