\c regression_ora

drop table tcount_20230710;
create table tcount_20230710(a int, b int, c int);
-- create table tcount_20230710(a int, b binary_float,c number(12,9));
insert into tcount_20230710 values(1,2,3);
insert into tcount_20230710 values(2,null,3);
insert into tcount_20230710 values(1,2,null);
insert into tcount_20230710 values(1,null,3);
insert into tcount_20230710 values(3,4,3);
insert into tcount_20230710 values(null,2,null);
insert into tcount_20230710 values(1,null,3);
insert into tcount_20230710 values(2,null,3);
insert into tcount_20230710 values(1,3,null);
insert into tcount_20230710 values(3,null,3);
insert into tcount_20230710 values(1,4,null);
insert into tcount_20230710 values(null,2,3);
insert into tcount_20230710 values(null,3,3);

drop table tempty_20230713;
create table tempty_20230713(a int, b int,c int);

-- basic usage
select pg_catalog.count(*) as all, pg_catalog.count(a) as ca, pg_catalog.count(b) as cb, pg_catalog.count(c) as cc from tcount_20230710;
select count(nvl(a,'12')) from tcount_20230710;
select count(nvl(a,'')) from tcount_20230710;
select count(nvl(a, NULL)) from tcount_20230710;
select count(1) from tcount_20230710;

select count(decode(a,1,'1','')) from tcount_20230710;
select count(decode(a,2,'2','')) from tcount_20230710;
select count(decode(a,3,'3','')) from tcount_20230710;
select count(decode(a,'',1,'')) from tcount_20230710;

select pg_typeof(count(*)),pg_typeof(count(a)) from tcount_20230710;
select count(a)/count(*), count(a)/count(c) from tcount_20230710;
select sum(a)/count(*) from tcount_20230710;
select count(a)/13, pg_typeof(count(a)/13) from tcount_20230710;
select count(a)/13::int2, pg_typeof(count(a)/13::int2) from tcount_20230710;
select count(a)/13::int4, pg_typeof(count(a)/13::int4) from tcount_20230710;
select count(a)/13::bigint, pg_typeof(count(a)/13::bigint) from tcount_20230710;

select count(a)+13, pg_typeof(count(a)+13) from tcount_20230710;
select count(a)+13::int2, pg_typeof(count(a)+13::int2) from tcount_20230710;
select count(a)+13::int4, pg_typeof(count(a)+13::int4) from tcount_20230710;
select count(a)+13::bigint, pg_typeof(count(a)+13::bigint) from tcount_20230710;

select count(a)-13, pg_typeof(count(a)-13) from tcount_20230710;
select count(a)-13::int2, pg_typeof(count(a)-13::int2) from tcount_20230710;
select count(a)-13::int4, pg_typeof(count(a)-13::int4) from tcount_20230710;
select count(a)-13::bigint, pg_typeof(count(a)-13::bigint) from tcount_20230710;

select count(a)*13, pg_typeof(count(a)*13) from tcount_20230710;
select count(a)*13::int2, pg_typeof(count(a)*13::int2) from tcount_20230710;
select count(a)*13::int4, pg_typeof(count(a)*13::int4) from tcount_20230710;
select count(a)*13::bigint, pg_typeof(count(a)*13::bigint) from tcount_20230710;

select -count(*), -count(a), pg_typeof(-count(*)), pg_typeof(-count(a)) from tcount_20230710;
select +count(*), +count(a), pg_typeof(+count(*)), pg_typeof(+count(a)) from tcount_20230710;

select ((-count(*)*2::bigint)+count(a)+12::int2)/(count(b)-1::int4)*(count(c)/2::int8),
pg_typeof(((-count(*)*2::bigint)+count(a)+12::int2)/(count(b)-1::int4)*(count(c)/2::int8)) from tcount_20230710;

select a, count(a) from tcount_20230710 group by a order by a;
SELECT a, count(a) over (partition by a) FROM tcount_20230710 order by a;
SELECT a, count(a) over (order by a rows between 2 preceding and 2 following) FROM tcount_20230710 order by a;
select a, count(*) from tcount_20230710 group by a order by a;
SELECT a, count(*) over (partition by a) FROM tcount_20230710 order by a;
SELECT a, count(*) over (order by a rows between 2 preceding and 2 following) FROM tcount_20230710 order by a;
SELECT COUNT(a) FILTER (WHERE a > 1) FROM tcount_20230710;

-- agg related functions
select a, numeric_dec(a::numeric) from tcount_20230710 order by 1,2;
select a, numeric_inc_any(a::numeric,0) from tcount_20230710 order by 1,2;;
select a, numeric_dec_any(a::numeric,0) from tcount_20230710 order by 1,2;;

select numeric_dec(1) from dual;
select numeric_inc_any(1::numeric,0) from dual;
select numeric_dec_any(1::numeric,0) from dual;

set distinct_optimizer to off;
set distinct_with_hash to off;

select count(distinct a), count(distinct b),count(distinct c) from tcount_20230710;
select count(distinct a), count(distinct b),count(distinct c) from tempty_20230713;
select pg_typeof(count(distinct a)), pg_typeof(count(distinct b)),pg_typeof(count(distinct c)) from tcount_20230710;
select pg_typeof(count(distinct a)), pg_typeof(count(distinct b)),pg_typeof(count(distinct c)) from tempty_20230713;
select pow(count(distinct a)/sum(distinct a),3) from tcount_20230710;
select count(distinct a) filter (where a > 1) from tcount_20230710;

reset distinct_optimizer;
reset distinct_with_hash;

-- agg filter in row mode
select count(a) filter (where a > 1) from tcount_20230710;
select count(distinct a) filter (where a > 1) from tcount_20230710;
select count(distinct a) filter (where b > 6) from tcount_20230710;

select sum(a) filter (where a+b+c is not null) from tcount_20230710;
select sum(a) filter (where a>1) from tcount_20230710;
select sum(distinct a) filter (where a>1) from tcount_20230710;
select sum(a) filter (where a+b>4) from tcount_20230710;
select sum(a) filter (where a+b+c>6) from tcount_20230710;
select sum(a+b+c) filter (where a+b+c>6) from tcount_20230710;

select sum(a+b+c) filter (where (case when a = 1 then true when b is null then true else false end)) from tcount_20230710;

select string_agg((a||'-'||b||'-'||c),',')
filter (where (case when a = 1 then true when b is null then true else false end))
over(order by a,b,c) from tcount_20230710;
select string_agg((a||'-'||b||'-'||c),',')
filter (where decode(a,1,true,3,true,false) and b is not null and c is not null)
over(order by a,b,c) from tcount_20230710;
select string_agg((a||'-'||b||'-'||c),',')
filter (where (a in (1,2) or a is null) and b is not null and c is not null)
over(order by a,b,c) from tcount_20230710;

SELECT a,b,count(a+b) filter (where a+b >3) over (partition by a) FROM tcount_20230710 order by a,b;
SELECT a,b,sum(a+b) filter (where a+b >3) over (partition by a) FROM tcount_20230710 order by a,b;
SELECT a, sum(a) filter(where a >1) over (order by a rows between 2 preceding and 2 following)
FROM tcount_20230710 order by a;


select max(a) filter (where (a+b+c) > 6) from tcount_20230710;
select min(a) filter (where (a+b+c) > 6) from tcount_20230710;

-- count distinct
set distinct_optimizer to on;
set distinct_with_hash to on;

select count(distinct a), count(distinct b),count(distinct c) from tempty_20230713;
select pg_typeof(count(distinct a)), pg_typeof(count(distinct b)),pg_typeof(count(distinct c)) from tempty_20230713;

reset distinct_optimizer;
reset distinct_with_hash;


set distinct_optimizer to on;
set distinct_with_hash to off;
select count(distinct a), count(distinct b),count(distinct c) from tempty_20230713;
select pg_typeof(count(distinct a)), pg_typeof(count(distinct b)),pg_typeof(count(distinct c)) from tempty_20230713;

reset distinct_optimizer;
reset distinct_with_hash;


set distinct_optimizer to off;
set distinct_with_hash to on;

select count(distinct a), count(distinct b),count(distinct c) from tempty_20230713;
select pg_typeof(count(distinct a)), pg_typeof(count(distinct b)),pg_typeof(count(distinct c)) from tempty_20230713;

reset distinct_optimizer;
reset distinct_with_hash;


set distinct_optimizer to off;
set distinct_with_hash to off;

select count(distinct a), count(distinct b),count(distinct c) from tempty_20230713;
select pg_typeof(count(distinct a)), pg_typeof(count(distinct b)),pg_typeof(count(distinct c)) from tempty_20230713;

reset distinct_optimizer;
reset distinct_with_hash;

-- in postgres
\c postgres

drop table tcount_20230710;
create table tcount_20230710(a int, b int, c int);
-- create table tcount_20230710(a int, b binary_float,c number(12,9));
insert into tcount_20230710 values(1,2,3);
insert into tcount_20230710 values(2,null,3);
insert into tcount_20230710 values(1,2,null);
insert into tcount_20230710 values(1,null,3);
insert into tcount_20230710 values(3,2,3);
insert into tcount_20230710 values(null,2,null);
insert into tcount_20230710 values(1,null,3);
insert into tcount_20230710 values(2,null,3);
insert into tcount_20230710 values(1,2,null);
insert into tcount_20230710 values(3,null,3);
insert into tcount_20230710 values(1,2,null);
insert into tcount_20230710 values(null,2,3);
insert into tcount_20230710 values(null,2,3);

select count(*) as all, count(a) as ca, count(b) as cb, count(c) as cc from tcount_20230710;
select count(distinct a) from tcount_20230710;

select pg_typeof(count(*)),pg_typeof(count(a)) from tcount_20230710;
select count(a)/count(*), count(a)/count(c) from tcount_20230710;
select sum(a)/count(*) from tcount_20230710;
select count(a)/13, pg_typeof(count(a)/13) from tcount_20230710;
select count(a)/13::int2, pg_typeof(count(a)/13::int2) from tcount_20230710;
select count(a)/13::int4, pg_typeof(count(a)/13::int4) from tcount_20230710;
select count(a)/13::bigint, pg_typeof(count(a)/13::bigint) from tcount_20230710;

select count(a)+13, pg_typeof(count(a)+13) from tcount_20230710;
select count(a)+13::int2, pg_typeof(count(a)+13::int2) from tcount_20230710;
select count(a)+13::int4, pg_typeof(count(a)+13::int4) from tcount_20230710;
select count(a)+13::bigint, pg_typeof(count(a)+13::bigint) from tcount_20230710;

select -count(*), -count(a), pg_typeof(-count(*)), pg_typeof(-count(a)) from tcount_20230710;
select +count(*), +count(a), pg_typeof(+count(*)), pg_typeof(+count(a)) from tcount_20230710;

select ((-count(*)*2::bigint)+count(a)+12::int2)/(count(b)-1::int4)*(count(c)/2::int8),
 pg_typeof(((-count(*)*2::bigint)+count(a)+12::int2)/(count(b)-1::int4)*(count(c)/2::int8)) from tcount_20230710;

 \c regression_ora
drop table tr_varchar2;
create table tr_varchar2(f1 varchar2(20), f2 text, f3 number, f4 integer);

insert into tr_varchar2 values(1,1,1,1);
insert into tr_varchar2 values(2,2,2,2);
insert into tr_varchar2 values('10000',10000,10000,10000);
insert into tr_varchar2 values('100.9876543210987',100.9876543210987,100.9876543210987,100);
select avg(f1), avg(f2), avg(f3), avg(f4) as avg from tr_varchar2;

insert into tr_varchar2 values('1234.1qwe','1234.1qwe',0,0);
select avg(f1), avg(f2), avg(f3), avg(f4) as avg from tr_varchar2;
delete from tr_varchar2;
drop table tr_varchar2;
