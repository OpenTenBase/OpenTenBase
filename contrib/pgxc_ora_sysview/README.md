# pgxc_ora_sysview

Extension to add Oracle SYSTEM VIEW compatibility to OpenTenBase

Information about the Oracle 19C ALL_OBJECTS can be found [here](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ALL_OBJECTS.html)


## [Installation](#installation)
To be able to run this extension, your OpenTenBase version must support extensions (>= 2.0).

1. Copy the source code from repository.
2. set pg_config binary location in PATH environment variable
3. Create default node group & create sharding group 
4. Execute following command to install this extension

```
    make
    sudo make install
```
Test of the extension can be done using:
```
    make installcheck
```

create extension
```
psql -d mydb -c "CREATE EXTENSION pgxc_ora_sysview;"
```

If you don't want to create an extension for this, you can simply import the SQL file and use the complete functionality:
```
psql -d mydb -c "CREATE SCHEMA sys;"

psql -d mydb -f sql/pgxc_ora_sysview--1.0.0.sql
```
This is especially useful for database in DBaas cloud services. To upgrade just import the extension upgrade files using psql.

## [Manage the extension](#manage-the-extension)

Each database that needs to use `pg_dbms_metadata` must creates the extension:
```
    psql -d mydb -c "CREATE EXTENSION pgxc_ora_sysview"
```

To upgrade to a new version execute:
```
    psql -d mydb -c 'ALTER EXTENSION pgxc_ora_sysview UPDATE TO "1.1.0"'
```

## [Functions](#functions)
Oracle-compatible system views, as follows:：

* ALL_OBJECTS
* DBA_OBJECTS
* USER_OBJECTS
* ALL_TABLES
* DBA_TABLES
* USER_TABLES
* ALL_PROCEDURES
* DBA_PROCEDURES
* USER_PROCEDURES

## [Test](#test)
init test case:
```
psql -d mydb -f test/sql/00_ora_init.sql
```

test dba_objects/all_objects/user_objects
```
psql -d mydb -f test/sql/01_ora_objects.sql
```

test dba_procedures/all_procedures/user_procedures
```
psql -d mydb -f test/sql/02_ora_proc.sql
```

test dba_tables/all_tables/user_tables
```
psql -d mydb -f test/sql/03_ora_tables.sql
```

clean test data:
```
psql -d mydb -f test/sql/05_ora_clean.sql
```
