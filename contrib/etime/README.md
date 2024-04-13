> There are another branch for other issue.
# etime 
# Open Source Opentenbase ETime logging

## Introduction
The Opentenbase ETime Extension(eTime) provides power to record query(**select statement**) execution time above the threshold you pre-set.

## How to use
Move this reposity into `${SOURCECODE_PATH}/contri` directory directly.  And under this directory, run `make -sj && make install` to install.

In psql session, you should run `create extension etime;` to create etime extension and `load '$libdir/etime';` to load dynamic file firstly.  And you can set name of table which will be written by record.

After that, you run query and see execution time in table you pre-set.

## Settings
**min_value**
You can set the threshold in mircosecond.  the query execution time above that will be recorded in table.

In Opentenbase psql session, you run `set etime.min_value = 1000;` and the execution time below 1000us will be recorded.

The default is: `0`.

**tablename**
You can set the table name.  the query execution time above will be recorded in this table.

In Opentenbase psql session, you run `set etime.tablename = "etime_t";` and record will be written in the table named `etime_t`.

Last but not least, The table must be have `specific schema` like: `(sql text, time bigint);`.

The default is: `NULL`.

**max_sql_size**
You can set the max *bytes* of query recorded including other info(maybe occupy 40 Bytes).

In Opentenbase psql session, you run `set etime.max_sql_size = 128;` and the bytes of query including other info will less than `128`;

The default is: `1024`.

## Example
*SQL*
```
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
```
*Table Output*
```
         sql         |  time
---------------------+---------
 select pg_sleep(1); | 1002178
```

## Caveats
Every time you create new psql session, you must run `load '$libdir/etime';` to load dynamic file and set table name to store info.
