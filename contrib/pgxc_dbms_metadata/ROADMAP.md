Functions Implemented
---------------------
The list below includes the functions that have been implemented and those that are part of future plans. Have a look at [README](README.md) to understand the meaning of any of the items below.
- ✔︎ GET_DDL()
- ✔︎ GET_DEPENDENT_DDL()
- ✔︎ GET_GRANTED_DDL()
- ✔︎ SET_TRANSFORM_PARAM()

GET_DDL
-------
The following outlines the supported PostgreSQL objects for GET_DDL, along with those that are currently not implemented but are included in the roadmap.
- ✔︎ TABLE
- ✔︎ VIEW
- ✔︎ SEQUENCE
- ✔︎ FUNCTION
- ✔︎ TRIGGER
- ✔︎ INDEX
- ✔︎ CONSTRAINT
- ✔︎ CHECK_CONSTRAINT
- ✔︎ REF_CONSTRAINT
- ✔︎ TYPE
- MATERIALIZED_VIEW
- USER/ROLE
- ✔︎ PARTITION TABLES(ONLY PARENT)
- PARTITION TABLES(CHILD TABLES)
- Generating DDL for OVERLOADED FUNCTIONS
- TEMP TABLES
- Including IF NOT EXISTS & CREATE OR REPLACE clauses in DDL


GET_DEPENDENT_DDL
-----------------
The following lists the supported dependent PostgreSQL objects for a table in GET_DEPENDENT_DDL.
- ✔︎ SEQUENCE
- ✔︎ TRIGGER
- ✔︎ CONSTRAINT
- ✔︎ REF_CONSTRAINT
- ✔︎ INDEX
- VIEW

GET_GRANTED_DDL
---------------
Following lists the supported type of grants for a user/role.
- ✔︎ ROLE_GRANT
- SYSTEM GRANT

SET_TRANSFORM_PARAM
-------------------
The following outlines the supported transform params and those that are part of future plans. To understand the meaning of the items below, please refer the [README](README.md) and [Oracle DBMS_METADATA package documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_METADATA.html).
- ✔︎ SQLTERMINATOR
- ✔︎ CONSTRAINTS
- ✔︎ REF_CONSTRAINTS
- ✔︎ CONSTRAINTS_AS_ALTER
- ✔︎ PARTITIONING
- ✔︎ SEGMENT_ATTRIBUTES
- ✔︎ DEFAULT
- ✔︎ STORAGE
- PRETTY
- TABLESPACE

MISCELLANEOUS FUTURE PLANS
---------------------------
- ENABLE clause in trigger DDL
- DEFERRED constraints 
- [SET_REMAP_PARAM of Oracle](https://docs.oracle.com/database/121/ARPLS/d_metada.htm#ARPLS66910)
- Extracting DDL of heterogeneous object types like SCHEMA_EXPORT 