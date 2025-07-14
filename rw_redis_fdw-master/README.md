# Writable Foreign Data Wrapper for Redis

This PostgreSQL extension provides a Foreign Data Wrapper for read (SELECT) and write (INSERT, UPDATE, DELETE) access to Redis databases (http://redis.io). Supported Redis data types include: string, set, hash, list, zset, and pubsub.

*Note* that the output FDW module is called **redis\_fdw**, even though this repository is called rw\_redis\_fdw and not be confused with https://github.com/pg-redis-fdw/redis_fdw (which was used as a basis for the table schema but with very different code), to enable existing users to migrate to this repo.  Instructions hereon-in refer to this repository only.

redis\_fdw *(nahanni/rw\_redis\_fdw)* was written by Leon Dang, sponsored by Nahanni Systems Inc.

This project is currently work in progress and may have experience significant changes until it becomes stable. Use it with caution and at your own risk!

**PostgreSQL version compatibility**

Currently tested against PostgreSQL 9.4+, 10, 11, 12. Other versions might work but unconfirmed.

## Building
### Dependencies:
- hiredis - usually part of the Redis installation on your system, or obtain from source at https://github.com/redis/hiredis.
- PostgreSQL pgxs - part of the PostgreSQL package or source SDK


### Build
```
  PATH=<pgsql_prefix>/bin:$PATH make
  sudo PATH=<pgsql_prefix>/bin:$PATH make install
```
where pgsql\_prefix is where you've installed PostgreSQL to, e.g. /usr/local/postgresql/9.4.0

## Options

### Server options

Server options are optional if using the defaults.


|option|description|default|
|---|---|---|
|**host**|(or "*address*") Redis server's unix socket absolute path, hostname or IP address|localhost|
|**port**|server network port|6379|
|**password**|Redis-authentication|*no default*|


Example of how to add a Redis backend:
```
  CREATE EXTENSION redis_fdw;
  
  CREATE SERVER redis_server 
     FOREIGN DATA WRAPPER redis_fdw 
     OPTIONS (host '127.0.0.1', port '6379');
 
  CREATE USER MAPPING FOR PUBLIC
     SERVER redis_server
     OPTIONS (password 'secret');
```

## PostgreSQL Table Schema

redis\_fdw expects the tables to have a particular structure to help it issue Redis commands and translate data. The default column names for each table are listed in the following sections; if they are relabeled then the column option `redis <redis_fdw_original>` must be provided to map to the original redis\_fdw expected name. For example:

```
  CREATE FOREIGN TABLE rft_str (
     key  TEXT,
     v    TEXT,
     ...
  )...

  ALTER FOREIGN TABLE rft_str ALTER COLUMN v OPTIONS (ADD redis 'value');
```

Any extraneous columns defined are ignored and untested.

### Table options

```
  CREATE FOREIGN TABLE ftbl (
     ...
  ) ...
  OPTIONS ( <options> )
```

|option|description|
|------|-----------|
| **tabletype**|**Mandatory** option. Specifies the Redis data type: string, hash, mhash, set, zset, list, ttl, len, publish|
|**key**|bind table to a specific key. *Note*: do not specify a "key" column in the table if this option is used. For a **PUBLISH** table, use **channel** instead of **key**.|
|**keyprefix**|prefix all keys in the table with this value. One use is to enable namespace separation from other keys in Redis|
|**readonly**|read-only table, no writes permitted|
|**database**|for Redis database to use (an integer)|

*tabletype*, with column names, can be one of the following (refer to the subsections further below for operations that can be completed on them):

 - **string** - key-value
 - **hash** - key-field-value
 - **mhash** or **hmset** - key-field[]-value[]. *Read-only table*
 - **set** - key-member
 - **zset** - key-member-score-index
 - **list** - key-index-value
 - **publish** - channel-message-len
	 - INSERT issues PUBLISH *channel message*,
	 - SELECT issues PUBSUB NUMSUB *channel*

*tabletype* for non-redis data types, but useful tables:

- **ttl** - key-expiry. Inspect or set/remove the expiry from a key.
- **len** - key-tabletype-len. Retrieve the number of items in a key or the entire database.
- **keys** - retreive all keys in the database

*key* must be either defined as a column or a table option, but not both. 

Note that **expiry** (in seconds) is an optional column in all the tables. If it is specified, then redis\_fdw will also fetch the key's expiry. This is only done once per unique key fetch, so it isn't too expensive.


### String key-value data type

**Read-Write**

```
  CREATE FOREIGN TABLE rft_str(
      key    TEXT,
      value  TEXT,
      expiry INT
  ) SERVER xxx
    OPTIONS (tabletype 'string');
```

### Hash

**Read-Write**

Each row represents a field and value of the hash, so the **key** and **expiry** columns will be the same for all rows.

```
  CREATE FOREIGN TABLE rft_hash(
      key    TEXT,
      field  TEXT,
      value  TEXT,
      expiry INT
  ) SERVER xxx
    OPTIONS (tabletype 'hash');
```

### Multiple hash fields

**Read-only**
- *this might be changed to being writable in the future*

```
  CREATE FOREIGN TABLE rft_mhash(
      key    TEXT,
      field  TEXT[],
      value  TEXT[],
      expiry INT
  ) SERVER xxx
    OPTIONS (tabletype 'mhash');
```

### List

**Read-Write**

```
  CREATE FOREIGN TABLE rft_list(
      key    TEXT,
      value  TEXT,
      "index" INT,
      expiry INT
  ) SERVER xxx
    OPTIONS (tabletype 'list');
```

- INSERT is the equivalent of RPUSH (add to the tail of the list)
- UPDATE uses LSET to change the value of an item at the **index**
- DELETE of `index = 0` uses LPOP (remove first item), otherwise redis\_fdw will rename the item at the specified index to a searchable string and delete that item.
- DELETE of `value = x` uses `LREM key 1 value`, ie deletes only the first left instance of the value. If you specify both `value` and `index`, then only `value` is used for deletion (`index` is ignored)

### Set

**Read-Write**

```
  CREATE FOREIGN TABLE rft_set(
      key    TEXT,
      member TEXT,
      expiry INT
  ) SERVER xxx
    OPTIONS (tabletype 'set');
```

### ZSet

**Read-Write**

```
  CREATE FOREIGN TABLE rft_zset(
      key     TEXT,
      member  TEXT,
      score   INT,
      "index" INT,
      expiry  INT
  ) SERVER localredis
    OPTIONS (tabletype 'zset');
```

### TTL

**Read-Write**

Get or set the time to live (in seconds) of a key.

 - Set expiry = 0 to make the key persistent.
 - DELETE will delete the entire key.

```
  CREATE FOREIGN TABLE rft_ttl(
      key    TEXT,
      expiry INT
  ) SERVER localredis
    OPTIONS (tabletype 'ttl');
```
### Publish

**Read-Write**

* SELECT performs `PUBSUB NUMSUB channel` to fetch the number of subscribers to the channel
* INSERT performs `PUBLISH channel message` and places the number of subscribers who received the message on the RETURNING value of len

```
  CREATE FOREIGN TABLE rft_pub(
      channel   TEXT,
      message   TEXT,
      len       INT
  ) SERVER localredis
    OPTIONS (tabletype 'publish');
```

### Len

**Read-only**

Retrieve the length of a key or the database (if SELECT does not have WHERE key = xxxx).

```
  CREATE FOREIGN TABLE rft_len(
      key       TEXT,
      tabletype TEXT,
      len       INT,
      expiry    INT
  ) SERVER localredis
    OPTIONS (tabletype 'len');
```

### Keys

**Read-only**

Retrieve all keys for the database. Use with special care and only on small key-spaces since it requires fetching all keys from redis which returns all keys as a single array which can hurt memory use significantly.

```
  CREATE FOREIGN TABLE rft_keys(
      key       TEXT
  ) SERVER localredis
    OPTIONS (tabletype 'keys');
```

## Usage

redis\_fdw is able to parse simple WHERE clauses containing the following operators.

**Text Equality**
- *column* **=** *constant/parameter*
 -   e.g. `WHERE key = "foo"`

**Index and Score integer comparisons**
- *index/score column* **<** | **<=** | **=** | **>=** | **>** *constant/parameter*
 -   e.g. `WHERE index > 4 AND index < 9`

**Array contains**
- *array-column* **@\>** *array-constant/array-parameter*
 -   e.g. `WHERE field @> '{"a","b","c"}'`

Refer to the test sql script for real examples.

## Limitations

redis\_fdw can handle most queries ok if the WHERE clause conditions are passed to it from PostgreSQL.

The module can't yet handle JOINs if the **key** is not provided in WHERE clause as a constant or parameter. The reasons are complex and have to do with PostgreSQL's query planner which doesn't (and may not be able to) provide the WHERE conditions to redis\_fdw.

For example, the following will fail because `r.key = u.key` isn't provided to redis\_fdw.

```
  SELECT u.*, r.value, r.expiry
  FROM pgsql_users u
  JOIN rft_sessions r ON r.key = u.key
  WHERE r.key = u.key;
```

The workaround is something like where `r.key = (<subquery>)`:

```
  WITH u AS (SELECT * FROM pgsql_users WHERE userid = 1)
  SELECT u.*, r.value, r.expiry
  FROM rft_sessions r, u
  WHERE r.key = (SELECT u.key FROM u);
```

### PubSub

Only PUBSUB NUMSUB *channel* is implemented.

SUB clients will need to connect to Redis directly.

## License:

Copyright 2015 Leon Dang, Nahanni Systems Inc.
BSD-style license; see LICENSE.

## Support

(this wiki section to be improved upon)

If you encounter an issue with the module, here are some ways that can assist in identifying root causes:

1. If PostgreSQL crashes because of the module:
 * enable core dumps on your system
 * start postgres with `ulimit -c unlimited` to enable core dumps
 * compile this module with gdb enabled (it is enabled by default in the Makefile with the **-g** switch to PG\_CPPFLAGS)
 * `gdb <pgsql-prefix>/bin/postgresql <path-to-core>/<corefile>`
 * get gdb back trace so with `gdb> bt`
 * submit the backtrace

2. Enable debug output from the module
 * `make DEBUG=1` and reinstall (`make install`)
 * configure postgres to write debug messages with log level INFO to the logs
 * obtain and submit the log entries that were printed by this module


## Authors

Leon Dang
