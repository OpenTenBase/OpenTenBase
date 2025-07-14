MySQL Foreign Data Wrapper for PostgreSQL
=========================================

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for
[MySQL][1].

Please note that this version of mysql_fdw works with PostgreSQL and EDB
Postgres Advanced Server 10, 11, 12, 13, 14, and 15.

This project is adapted for OpenTenBase based on https://github.com/EnterpriseDB/mysql_fdw/releases/tag/REL-2_9_0.

Installation
------------

The mysql_fdw submodule is already included under contrib and added to the contrib Makefile. Running `make install` for the entire OpenTenBase project will automatically compile and install this extension.

Test Example
------------

There are two ways to start MySQL server.

Method 1: Use MySQL service on the host machine (macOS example)

```bash
brew install mysql
brew services start mysql
```

Configure the MySQL server:

```bash
mysql -u root -p
```

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS db;
-- Create user (allow connections from any host)
CREATE USER IF NOT EXISTS 'foo'@'%' IDENTIFIED BY 'bar';
-- Grant privileges
GRANT ALL PRIVILEGES ON db.* TO 'foo'@'%';
-- Refresh privileges
FLUSH PRIVILEGES;
-- Create test table
USE db;
CREATE TABLE IF NOT EXISTS warehouse (
    warehouse_id INT PRIMARY KEY,
    warehouse_name VARCHAR(255),
    warehouse_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Verify setup
SELECT User, Host FROM mysql.user WHERE User = 'foo';
SHOW DATABASES;
USE db;
SHOW TABLES;
```

Method 2: Use MySQL service in a Docker container

```bash
# Start MySQL container
# Replace *** with your desired values
# Example: MYSQL_ROOT_PASSWORD=root, MYSQL_DATABASE=db, MYSQL_USER=foo, MYSQL_PASSWORD=bar

docker run -d \
  --name mysql8 \
  -e MYSQL_ROOT_PASSWORD=*** \
  -e MYSQL_DATABASE=*** \
  -e MYSQL_USER=*** \
  -e MYSQL_PASSWORD=*** \
  -p 3306:3306 \
  mysql:8.0
```

Get the MySQL container IP address:

```bash
MYSQL_CONTAINER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mysql8)
echo "MySQL container IP: $MYSQL_CONTAINER_IP"
```

Configure the database as in Method 1.

# Set up OpenTenBase environment

Follow all steps in the `Running Instructions` section of `example/1c_2d_cluster/README` to initialize the OpenTenBase cluster.

Then, start testing:

```sql
-- Load extension after installation
CREATE EXTENSION mysql_fdw;

-- Create server object
CREATE SERVER mysql_server
    FOREIGN DATA WRAPPER mysql_fdw
    OPTIONS (host 'host.docker.internal', port '3306');

-- Create user mapping
CREATE USER MAPPING FOR opentenbase
    SERVER mysql_server
    OPTIONS (username 'foo', password 'bar');

-- Create foreign table
CREATE FOREIGN TABLE warehouse
    (
        warehouse_id int,
        warehouse_name text,
        warehouse_created timestamp
    )
    SERVER mysql_server
    OPTIONS (dbname 'db', table_name 'warehouse');

-- Insert new rows into the table
INSERT INTO warehouse values (1, 'UPS', current_date);
INSERT INTO warehouse values (2, 'TV', current_date);
INSERT INTO warehouse values (3, 'Table', current_date);

-- Select from the table
SELECT * FROM warehouse ORDER BY 1;

warehouse_id | warehouse_name | warehouse_created
-------------+----------------+-------------------
           1 | UPS            | 10-JUL-20 00:00:00
           2 | TV             | 10-JUL-20 00:00:00
           3 | Table          | 10-JUL-20 00:00:00

-- Delete a row from the table
DELETE FROM warehouse where warehouse_id = 3;

-- Update a row in the table
UPDATE warehouse set warehouse_name = 'UPS_NEW' where warehouse_id = 1;

-- Explain a table with verbose option
EXPLAIN VERBOSE SELECT warehouse_id, warehouse_name FROM warehouse WHERE warehouse_name LIKE 'TV' limit 1;

                                   QUERY PLAN
--------------------------------------------------------------------------------------------------------------------
Limit  (cost=10.00..11.00 rows=1 width=36)
    Output: warehouse_id, warehouse_name
    ->  Foreign Scan on public.warehouse  (cost=10.00..1010.00 rows=1000 width=36)
        Output: warehouse_id, warehouse_name
        Local server startup cost: 10
        Remote query: SELECT `warehouse_id`, `warehouse_name` FROM `db`.`warehouse` WHERE ((`warehouse_name` LIKE BINARY 'TV'))
```

Known Issues
------------

Currently, OpenTenBase does not support distributed DELETE or UPDATE operations on MySQL foreign tables.

```bash
postgres=# -- delete row from table
postgres=# DELETE FROM warehouse where warehouse_id = 3;
ERROR:  could not plan this distributed delete
DETAIL:  correlated or complex DELETE is currently not supported in OpenTenBase.
postgres=# 
postgres=# -- update a row of table
postgres=# UPDATE warehouse set warehouse_name = 'UPS_NEW' where warehouse_id = 1;
ERROR:  could not plan this distributed update
DETAIL:  correlated UPDATE or updating distribution column currently not supported in OpenTenBase.
```

Enhancements
------------

The following enhancements are added to the latest version of
`mysql_fdw`:

### Write-able FDW
The previous version was only read-only, the latest version provides the
write capability. The user can now issue an insert, update, and delete
statements for the foreign tables using the mysql_fdw. It uses the PG
type casting mechanism to provide opposite type casting between MySQL
and PG data types.

### Connection Pooling
The latest version comes with a connection pooler that utilises the same
MySQL database connection for all the queries in the same session. The
previous version would open a new MySQL database connection for every
query. This is a performance enhancement.

### WHERE clause push-down
The latest version will push-down the foreign table where clause to
the foreign server. The where condition on the foreign table will be
executed on the foreign server hence there will be fewer rows to bring
across to PostgreSQL. This is a performance feature.

### Column push-down
The previous version was fetching all the columns from the target
foreign table. The latest version does the column push-down and only
brings back the columns that are part of the select target list. This is
a performance feature.

### Prepared Statement
(Refactoring for `select` queries to use prepared statement)

The `select` queries are now using prepared statements instead of simple
query protocol.

### JOIN push-down
mysql_fdw now also supports join push-down. The joins between two
foreign tables from the same remote MySQL server are pushed to a remote
server, instead of fetching all the rows for both the tables and
performing a join locally, thereby enhancing the performance. Currently,
joins involving only relational and arithmetic operators in join-clauses
are pushed down to avoid any potential join failure. Also, only the
INNER and LEFT/RIGHT OUTER joins are supported, and not the FULL OUTER,
SEMI, and ANTI join. This is a performance feature.

### AGGREGATE push-down
mysql_fdw now also supports aggregate push-down. Push aggregates to the
remote MySQL server instead of fetching all of the rows and aggregating
them locally. This gives a very good performance boost for the cases
where aggregates can be pushed down. The push-down is currently limited
to aggregate functions min, max, sum, avg, and count, to avoid pushing
down the functions that are not present on the MySQL server. Also,
aggregate filters and orders are not pushed down.

### ORDER BY push-down
mysql_fdw now also supports order by push-down. If possible, push order by
clause to the remote server so that we get the ordered result set from the
foreign server itself. It might help us to have an efficient merge join.
NULLs behavior is opposite on the MySQL server. Thus to get an equivalent
result, we add the "expression IS NULL" clause at the beginning of each of
the ORDER BY expressions.

### LIMIT OFFSET push-down
mysql_fdw now also supports limit offset push-down. Wherever possible,
perform LIMIT and OFFSET operations on the remote server. This reduces
network traffic between local PostgreSQL and remote MySQL servers.
ALL/NULL options are not supported on the MySQL server, and thus they are
not pushed down. Also, OFFSET without LIMIT is not supported on the MySQL
server hence queries having that construct are not pushed.

Usage
-----

The following parameters can be set on a MySQL foreign server object:

  * `host`: Address or hostname of the MySQL server. Defaults to
    `127.0.0.1`
  * `port`: Port number of the MySQL server. Defaults to `3306`
  * `secure_auth`: Enable or disable secure authentication. Default is
    `true`
  * `init_command`: SQL statement to execute when connecting to the
    MySQL server.
  * `use_remote_estimate`: Controls whether mysql_fdw issues remote
    EXPLAIN commands to obtain cost estimates. Default is `false`
  * `reconnect`: Enable or disable automatic reconnection to the
    MySQL server if the existing connection is found to have been lost.
    Default is `false`.
  * `sql_mode`: Set MySQL sql_mode for established connection. Default
    is `ANSI_QUOTES`.
  * `ssl_key`: The path name of the client private key file.
  * `ssl_cert`: The path name of the client public key certificate file.
  * `ssl_ca`: The path name of the Certificate Authority (CA) certificate
    file. This option, if used, must specify the same certificate used
    by the server.
  * `ssl_capath`: The path name of the directory that contains trusted
    SSL CA certificate files.
  * `ssl_cipher`: The list of permissible ciphers for SSL encryption.
  * `fetch_size`: This option specifies the number of rows mysql_fdw should
    get in each fetch operation. It can be specified for a foreign table or
    a foreign server. The option specified on a table overrides an option
    specified for the server. The default is `100`.
  * `character_set`: The character set to use for MySQL connection. Default
    is `auto` which means autodetect based on the operating system setting.
    Before the introduction of the character_set option, the character set
    was set similar to the PostgreSQL database encoding. To get this older
    behavior set the character_set to special value `PGDatabaseEncoding`.

The following parameters can be set on a MySQL foreign table object:

  * `dbname`: Name of the MySQL database to query. This is a mandatory
    option.
  * `table_name`: Name of the MySQL table, default is the same as
    foreign table.
  * `max_blob_size`: Max blob size to read without truncation.
  * `fetch_size`: Same as `fetch_size` parameter for foreign server.

The following parameters need to supplied while creating user mapping.

  * `username`: Username to use when connecting to MySQL.
  * `password`: Password to authenticate to the MySQL server with.

The following parameters can be set on IMPORT FOREIGN SCHEMA command:

  * `import_default`: This option controls whether column DEFAULT
  expressions are included in the definitions of foreign tables imported
  from a foreign server. The default is `false`.
  * `import_not_null`: This option controls whether column NOT NULL
  constraints are included in the definitions of foreign tables imported
  from a foreign server. The default is `true`.
  * `import_enum_as_text`: This option can be used to map MySQL ENUM type
  to TEXT type in the definitions of foreign tables, otherwise emit a
  warning for type to be created. The default is `false`.
  * `import_generated`: This option controls whether GENERATED column
  expressions are included in the definitions of foreign tables imported from
  a foreign server or not. The default is `true`. The IMPORT will fail
  altogether if an imported generated expression uses a function or operator
  that does not exist on PostgreSQL.

Contributing
------------
If you experience any bug and have a fix for that, or have a new idea,
create a ticket on github page. Before creating a pull request please
read the [contributing guidelines][3].

Support
-------
This project will be modified to maintain compatibility with new
PostgreSQL and EDB Postgres Advanced Server releases.

If you require commercial support, please contact the EnterpriseDB sales
team, or check whether your existing PostgreSQL support provider can
also support mysql_fdw.


License
-------
Copyright (c) 2011-2022, EnterpriseDB Corporation.

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written
agreement is hereby granted, provided that the above copyright notice
and this paragraph and the following two paragraphs appear in all
copies.

See the [`LICENSE`][4] file for full details.

[1]: http://www.mysql.com
[2]: https://github.com/enterprisedb/mysql_fdw/issues/new
[3]: CONTRIBUTING.md
[4]: LICENSE
