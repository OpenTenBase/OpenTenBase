# etime 
# Open Source Opentenbase ETime logging

## Introduction
The Opentenbase ETime Extension(eTime) provides power to record query(**select statement**) execution time above the threshold you pre-set.

## How to use
Move this reposity into `${SOURCECODE_PATH}/contri` directory directly.  And under this directory, run `make -sj && make install` to install.

In psql session, you should run `create extension etime;` to create etime extension and `load '$libdir/etime';` to load dynamic file firstly.

After that, you run query and see execution time in log file.

## Settings
**min_value**
You can set the threshold in mircosecond.  the query execution time above that will be recorded in log file.

In Opentenbase psql session, You run `set etime.min_value=1000` and the execution time below 1000us will be recorded.

The default is: `0`.

## Example
*SQL*
```
set etime.min_value=1000;

create table foo(id bigint, str text) distribute by shard(id);
insert into foo values(1, 'tencent'), (2, 'shenzhen');
select * from foo;
```
*Key Log Output*
```
SQL: (select * from foo;) Execution Time: 0s, 1045us
```

## Caveats
Every time you create new psql session, you must run `load '$libdir/etime';` to load dynamic file.