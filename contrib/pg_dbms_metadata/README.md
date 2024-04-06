# pg_dbms_metadata

Propose Oracle DBMS_METADATA compatibility for PostgreSQL PGXC,Based on HexaCluster Corp. Version 1.0

PostgreSQL extension to extract DDL of database objects in a way compatible to Oracle DBMS_METADATA package. This extension serves a dual purpose—not only does it provide compatibility with the Oracle DBMS_METADATA package, but it also establishes a systematic approach to programmatically retrieve DDL for objects. You now have the flexibility to generate DDL for an object either from a plain SQL query or from PL/pgSQL code. This also enables the extraction of DDL using any client that can execute plain SQL queries. These features distinguishes it from standard methods like pg_dump. 

Information about the Oracle DBMS_metadata package can be found [here](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_METADATA.html)

* [Description](#description)
* [Installation](#installation)
* [Manage the extension](#manage-the-extension)
* [Functions](#functions)
  - [GET_DDL](#get_ddl)
  - [GET_DEPENDENT_DDL](#get_dependent_ddl)
  - [GET_GRANTED_DDL](#get_granted_ddl)
  - [SET_TRANSFORM_PARAM](#set_transform_param)
* [Authors](#authors)
* [License](#license)

## [Description](#description)

This PostgreSQL extension provide compatibility with the DBMS_METADATA Oracle package's API to extract DDL. This extension only supports DDL extraction through GET_xxx functions. Support to FETCH_xxx functions and XML support is not added. As of now, any user can get the ddl of any object in the database. Like Oracle, we have flexibility of omitting schema name while trying to get ddl of an object. This will use search_path to find the object and gets required ddl. However when schema name is omitted, the current user should atleast have USAGE access on schema in which target object is present. 

Also see [ROADMAP](ROADMAP.md) for a precise understanding of what has been implemented and what is part of the future plans.

The following functions are implemented:

* `GET_DDL()` This function extracts DDL of specified object.  
* `GET_DEPENDENT_DDL()` This function extracts DDL of all dependent objects of specified type for a specified base object.
* `GET_GRANTED_DDL()` This function extracts the SQL statements to recreate granted privileges and roles for a specified grantee.
* `SET_TRANSFORM_PARAM()` This function is used to customize DDL through configuring session-level transform params. 

## [Installation](#installation)

To be able to run this extension, your PostgreSQL version must support extensions (>= 9.1).

1. Copy the source code from repository.
2. set pg_config binary location in PATH environment variable
3. Execute following command to install this extension

```
    make
    sudo make install
```

Test of the extension can be done using:
```
    make installcheck
```

If you don't want to create an extension for this, you can simply import the SQL file and use the complete functionality:
```
psql -d mydb -c "CREATE SCHEMA dbms_metadata;"

psql -d mydb -f sql/pg_dbms_metadata--1.0.0.sql
```
This is especially useful for database in DBaas cloud services. To upgrade just import the extension upgrade files using psql.

## [Manage the extension](#manage-the-extension)

Each database that needs to use `pg_dbms_metadata` must creates the extension:
```
    psql -d mydb -c "CREATE EXTENSION pg_dbms_metadata"
```

To upgrade to a new version execute:
```
    psql -d mydb -c 'ALTER EXTENSION pg_dbms_metadata UPDATE TO "1.1.0"'
```

## [Functions](#functions)

### [GET_DDL](#get_ddl)

This function extracts DDL of database objects. 

Below is list of currently supported object types. To get a ddl of a check constraint, unlike Oracle you need to use CHECK_CONSTRAINT object type. In Oracle, we will use the CONSTRAINT object type to get ddl of a check constraint.
* TABLE 
* VIEW 
* SEQUENCE 
* FUNCTION 
* TRIGGER
* INDEX 
* CONSTRAINT 
* CHECK_CONSTRAINT
* REF_CONSTRAINT
* TYPE

Syntax:
```
dbms_metadata.get_ddl (
   object_type      IN text,
   name             IN text,
   schema           IN text DEFAULT NULL)
   RETURNS text;
```
Parameters:

- object_type: Object type for which DDL is needed.
- name: Name of object 
- schema: Schema in which object is present. When schema is not provided search_path is used to find the object and get ddl. However schema cannot be NULL for object types TRIGGER, REF_CONSTRAINT.

Example:
```
SELECT dbms_metadata.get_ddl('TABLE','employees','gdmmm');
```

### [GET_DEPENDENT_DDL](#get_dependent_ddl)

This function extracts DDL of all dependent objects of specified object type for a specified base object. 

Below is the list of currently supported dependent object types
* SEQUENCE
* TRIGGER
* CONSTRAINT
* REF_CONSTRAINT
* INDEX.

Syntax:
```
dbms_metadata.get_dependent_ddl (
   object_type          IN text,
   base_object_name     IN text,
   base_object_schema   IN text DEFAULT NULL)
   RETURNS text;
```
Parameters:

- object_type: Object type of dependent objects for which DDL is needed.
- base_object_name: Name of base object 
- base_object_schema: Schema in which base object is present. When base object schema is not provided search_path is used to find the base object.

Example:
```
SELECT dbms_metadata.get_dependent_ddl('CONSTRAINT','employees','gdmmm');
```

### [GET_GRANTED_DDL](#get_granted_ddl)

This function extracts the SQL statements to recreate granted privileges and roles for a specified grantee.

Below is the list of currently supported object types
* ROLE_GRANT

Syntax:
```
dbms_metadata.get_granted_ddl (
    object_type  IN text, 
    grantee      IN text)
    RETURNS text;
```
Parameters:

- object_type: Type of grants to retrieve 
- grantee: User/role for whom we need to retrieve grants

Example:
```
SELECT dbms_metadata.get_granted_ddl('ROLE_GRANT','user_test');
```

### [SET_TRANSFORM_PARAM](#set_transform_param)

This function is used to configure session-level transform params, with which we can customize the DDL of objects. This only supports session-level transform params, not setting transform params through any other transform handles like Oracle. GET_DDL, GET_DEPENDENT_DDL inherit these params when they invoke the DDL transform. There is a small change in the function signature when compared to the one of Oracle.

Syntax:
```
dbms_metadata.set_transform_param (
   name          IN text,
   value         IN text);

dbms_metadata.set_transform_param (
   name          IN text,
   value         IN boolean);
```
Parameters:

- name: The name of the transform parameter.
- value: The value of the transform.

List of currently supported transform params

| Object types          | Name                                  | Datatype      | Meaning & Notes      
| --------------------- | ------------------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------
| All objects           | SQLTERMINATOR                         | boolean       | If TRUE, append the SQL terminator( ; ) to each DDL statement. Defaults to FALSE.  
| TABLE                 | CONSTRAINTS                           | boolean       | If TRUE, include all non-referential table constraints. If FALSE, omit them. Defaults to TRUE.  
| TABLE                 | REF_CONSTRAINTS                       | boolean       | If TRUE, include all referential constraints (foreign keys). If FALSE, omit them. Defaults to TRUE.  
| TABLE                 | CONSTRAINTS_AS_ALTER (UNSUPPORTED)    | boolean       | If TRUE, include table constraints as separate ALTER TABLE. This is not yet implemented in this extension. Currently all constraints are being given as separate DDL.  
| TABLE                 | PARTITIONING                          | boolean       | If TRUE, include partitioning clauses in the DDL. Defaults to TRUE. Unlike oracle, this extension does not support INDEX object type for this transform param as postgres indexes doesn't got any partitioning clauses.
| TABLE                 | SEGMENT_ATTRIBUTES                    | boolean       | If TRUE, include segment attributes in the DDL. Defaults to TRUE. Currently only logging attribute is supported and only TABLE object type is supported for this transform param.
| All objects           | DEFAULT                               | boolean       | Calling dbms_metadata.set_transform_param with this parameter set to TRUE has the effect of resetting all transform params to their default values. Setting this FALSE has no effect. There is no default.  
| TABLE                 | STORAGE                               | boolean       | If TRUE, include storage parameters in the DDL. Defaults to TRUE. Currently only TABLE object type is supported for this transform param. Index DDL will always retrieved with storage parameters, if there are any.

Example:
```
CALL dbms_metadata.set_transform_param('SQLTERMINATOR',true);
```

## [License](#license)

This extension is free software distributed under the PostgreSQL
License.
