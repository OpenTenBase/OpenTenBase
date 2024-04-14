> There are another branch for other issue.
# etime 
# Open Source Opentenbase ETime logging

## Introduction
The Opentenbase ETime Extension(eTime) provides power to record query(**select statement**) execution time above the threshold you pre-set.

## How to use
Move this reposity into `${SOURCECODE_PATH}/contri` directory directly.  And under this directory, run `make -sj && make install` to install.

In psql session, you should run `create extension etime;` to create etime extension and `load '$libdir/etime';` to load dynamic file firstly.  And you can set name of table which will be written by record.

Before executing query, you should create a table named `etime` with **specific schema** like:
```sql
create table etime (
	sql text,
	start_time TimestampTz,
	end_time TimestampTz,
	duration_time bigint
);
```

After that, you can run query and see execution time in table you pre-set by `select * from etime;`.  Out of convenience, you also can query info whose duration time is greater than threshold you pre-set by `select * from e_gt_dt(xxx);`.

## Settings
**min_value**
You can set the threshold in mircosecond.  the query execution time above that will be recorded in table.

In Opentenbase psql session, you run `set etime.min_value = 1000;` and the execution time below 1000us will be recorded.

The default is: `0`.


**max_sql_size**
You can set the max *bytes* of query recorded including other info(maybe occupy 40 Bytes).

In Opentenbase psql session, you run `set etime.max_sql_size = 128;` and the bytes of query including other info will less than `128`;

The default is: `1024`.

## Example
*SQL*
```
drop extension if exists etime;
create extension etime;
load '$libdir/etime';
set etime.min_value = 1000000;
set etime.max_sql_size = 512;

drop extension if exists etime;
create table etime (
	sql text,
	start_time TimestampTz,
	end_time TimestampTz,
	duration_time bigint
);

create table foo(id bigint, str text) distribute by shard(id);
insert into foo values(1, 'tencent'), (2, 'shenzhen');

-- The following sql maybe be recorded because if you set threshold above
-- is very low, like 0.
-- But now it cannot be recorded.
insert into foo select 1, 'asdasd';

-- It cannot be recorded due to fast execution.
select * from foo;

-- These can be recorded for their execution time more than 1s you set above.
select pg_sleep(1);
select pg_sleep(2);

-- output record
select * from etime;
-- the following output is 2
-- select count(*) from e_gt_dt(1000000);
-- the following output is 1
-- select count(*) from e_gt_dt(2000000);
```
*Table Output*
```
         sql         |          start_time           |           end_time            | duration_time
---------------------+-------------------------------+-------------------------------+---------------
 select pg_sleep(1); | 2024-04-14 16:53:37.318473+08 | 2024-04-14 16:53:38.319617+08 |       1001144
 select pg_sleep(2); | 2024-04-14 16:53:38.327944+08 | 2024-04-14 16:53:40.330524+08 |       2002580
```

## Caveats
1. Every time you create new psql session, you must run `load '$libdir/etime';` to load dynamic file and set table name to store info.
