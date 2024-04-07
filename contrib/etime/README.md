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

In Opentenbase psql session, You run `set etime.min_value=1000` and the execution time below 1000us will be recorded.

The default is: `0`.

**tablename**
You can set the table name.  the query execution time above will be recorded in this table.

In Opentenbase psql session, You run `set etime.tablename="etime_t"` and record will be written in the table named `etime_t`.

Last but not least, The table must be have `specific schema` like: `(sql text, time bigint);`.

The default is: `NULL`.

## Example
*SQL*
```
set etime.min_value=1000;

create table foo(id bigint, str text) distribute by shard(id);
insert into foo values(1, 'tencent'), (2, 'shenzhen');
insert into foo select 1, 'asdasd';
select * from foo;
```
*Table Output*
```
                 sql                 | time
-------------------------------------+------
 select * from foo;                  | 1051
 insert into foo select 1, 'asdasd'; | 3784
```

## Caveats
Every time you create new psql session, you must run `load '$libdir/etime';` to load dynamic file and set table name to store info.