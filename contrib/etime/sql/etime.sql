SET client_min_messages TO WARNING;

drop table if exists etime cascade;
create table etime (sql text, time bigint);

-- create extension
drop extension if exists etime;
create extension etime;
load '$libdir/etime';
set etime.tablename = "etime";
set etime.min_value = 1000000;
set etime.max_sql_size = 128;


drop table if exists foo cascade;
create table foo(id bigint, str text) distribute by shard(id);
insert into foo values(1, 'tencent'), (2, 'shenzhen');

-- The following sql maybe be recorded because if you set threshold above
-- is very low, like 0.
-- But now it cannot be recorded.
insert into foo select 1, 'asdasd';

-- It cannot be recorded due to fast execution.
select count(*) from foo;

-- It can be recorded for its execution time more than 1s you set above.
select pg_sleep(1);

-- output record count is 1
select count(*) from etime;

-- change threshold
set etime.min_value = 2000000;

-- output record count is still 1
select pg_sleep(1);
select count(*) from etime;

drop table if exists etime cascade;
drop table if exists foo cascade;
