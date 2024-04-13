create extension etime;
load '$libdir/etime';

create table etime (sql text, time bigint);
set etime.tablename = "etime";
set etime.min_value = 1000000;
set etime.max_sql_size = 128;

create table foo(id bigint, str text) distribute by shard(id);
insert into foo values(1, 'tencent'), (2, 'shenzhen');

-- The following sql maybe be recorded because if you set threshold above
-- is very low, like 0.
-- But now it cannot be recorded.
insert into foo select 1, 'asdasd';

-- It cannot be recorded due to fast execution.
select * from foo;

-- It can be recorded for its execution time more than 1s you set above.
select pg_sleep(1);

-- output record
select * from etime;