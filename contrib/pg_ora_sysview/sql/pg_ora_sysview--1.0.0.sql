----
-- Script to create the objects of the pg_ora_sysview extension
----

----
-- SYS.DBA_OBJECTS
----

CREATE VIEW sys.dba_objects AS
SELECT
    pg_catalog.pg_get_userbyid(c.relowner)::VARCHAR2(128) AS owner,
    c.relname::VARCHAR2(128) AS object_name,
    NULL::VARCHAR2(128) AS subobject_name,
    c.oid::INT AS object_id,
    NULL::NUMBER AS data_object_id,
    CASE
        WHEN c.relkind = 'r' THEN 'TABLE'::VARCHAR2(23)
        WHEN c.relkind = 'v' THEN 'VIEW'::VARCHAR2(23)
        WHEN c.relkind = 'i' THEN 'INDEX'::VARCHAR2(23)
        WHEN c.relkind = 'S' THEN 'SEQUENCE'::VARCHAR2(23)
        WHEN c.relkind = 'c' THEN 'COMPOSITE TYPE'::VARCHAR2(23)
        WHEN c.relkind = 't' THEN 'TOAST TABLE'::VARCHAR2(23)
        WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE'::VARCHAR2(23)
        WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW'::VARCHAR2(23)
        WHEN c.relkind = 'f' THEN 'FOREIGN TABLE'::VARCHAR2(23)
        WHEN c.relkind = 'I' THEN 'PARTITIONED INDEX'::VARCHAR2(23)
        ELSE 'UNKOWN'::VARCHAR2(23)
    END AS object_type,
    NULL::DATE AS created,
    NULL::DATE AS last_ddl_time,
    NULL::VARCHAR2(19) AS timestamp,
    CASE
        WHEN c.relkind IN ('i','I') AND idx.indisvalid = false THEN 'INVALID'::VARCHAR2(7)
        ELSE 'VALID'::VARCHAR2(7)
    END AS status, -- Index invalid status
    CASE
        WHEN c.relpersistence = 't' THEN 'Y'::VARCHAR2(1)
        ELSE 'N'::VARCHAR2(1)
    END AS temporary,
    CASE
        WHEN c.relnamespace::regnamespace IN ('pg_catalog', 'information_schema', 'pg_toast') THEN 'Y'::VARCHAR2(1)
        ELSE 'N'::VARCHAR2(1)
    END AS generated,
    NULL::VARCHAR2(1) AS secondary,
    c.relnamespace::INT AS namespace,
    NULL::VARCHAR2(128) AS edition_name,
    NULL::VARCHAR2(18) AS sharing,
    NULL::VARCHAR2(1) AS editionable,
    NULL::VARCHAR2(1) AS oracle_maintained,
    NULL::VARCHAR2(1) AS application,
    NULL::VARCHAR2(100) AS default_collation,
    NULL::VARCHAR2(1) AS duplicated,
    NULL::VARCHAR2(1) AS sharded,
    NULL::NUMBER AS created_appid,
    NULL::NUMBER AS created_vsnid,
    NULL::NUMBER AS modified_appid,
    NULL::NUMBER AS modified_vsnid
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_index idx ON c.oid = idx.indrelid
UNION ALL
SELECT
    pg_catalog.pg_get_userbyid(p.proowner)::VARCHAR2(128) AS owner,
    p.proname::VARCHAR2(128) AS object_name,
    NULL::VARCHAR2(128) AS subobject_name,
    p.oid::INT AS object_id,
    NULL::NUMBER AS data_object_id,
    'FUNCTION'::VARCHAR2(23) AS object_type, -- PGXC only has functions
    NULL::DATE AS created,
    NULL::DATE AS last_ddl_time,
    NULL::VARCHAR2(19) AS timestamp,
    NULL::VARCHAR2(7) AS status,
    NULL::VARCHAR2(1) AS temporary,
    CASE
        WHEN p.pronamespace::regnamespace IN ('pg_catalog', 'information_schema', 'pg_toast') THEN 'Y'::VARCHAR2(1)
        ELSE 'N'::VARCHAR2(1)
    END AS generated,
    NULL::VARCHAR2(1) AS secondary,
    p.pronamespace::INT AS namespace,
    NULL::VARCHAR2(128) AS edition_name,
    NULL::VARCHAR2(18) AS sharing,
    NULL::VARCHAR2(1) AS editionable,
    NULL::VARCHAR2(1) AS oracle_maintained,
    NULL::VARCHAR2(1) AS application,
    NULL::VARCHAR2(100) AS default_collation,
    NULL::VARCHAR2(1) AS duplicated,
    NULL::VARCHAR2(1) AS sharded,
    NULL::NUMBER AS created_appid,
    NULL::NUMBER AS created_vsnid,
    NULL::NUMBER AS modified_appid,
    NULL::NUMBER AS modified_vsnid
FROM pg_catalog.pg_proc p
UNION ALL
SELECT
    pg_catalog.pg_get_userbyid(c.relowner)::VARCHAR2(128) AS owner,
    c.relname::VARCHAR2(128) AS object_name,
    NULL::VARCHAR2(128) AS subobject_name,
    c.oid::INT AS object_id,
    NULL::NUMBER AS data_object_id,
    'TRIGGER'::VARCHAR2(23) AS object_type,
    NULL::DATE AS created,
    NULL::DATE AS last_ddl_time,
    NULL::VARCHAR2(19) AS timestamp,
    CASE 
        WHEN t.tgenabled = 'D' THEN 'INVALID'::VARCHAR2(7) -- TRIGGER disable
        ELSE 'VALID'::VARCHAR2(7)
    END AS status,
    CASE
        WHEN c.relpersistence = 't' THEN 'Y'::VARCHAR2(1)
        ELSE 'N'::VARCHAR2(1)
    END AS temporary,
    CASE
        WHEN c.relnamespace::regnamespace IN ('pg_catalog', 'information_schema', 'pg_toast') THEN 'Y'::VARCHAR2(1)
        ELSE 'N'::VARCHAR2(1)
    END AS generated,
    NULL::VARCHAR2(1) AS secondary,
    c.relnamespace::INT AS namespace,
    NULL::VARCHAR2(128) AS edition_name,
    NULL::VARCHAR2(18) AS sharing,
    NULL::VARCHAR2(1) AS editionable,
    NULL::VARCHAR2(1) AS oracle_maintained,
    NULL::VARCHAR2(1) AS application,
    NULL::VARCHAR2(100) AS default_collation,
    NULL::VARCHAR2(1) AS duplicated,
    NULL::VARCHAR2(1) AS sharded,
    NULL::NUMBER AS created_appid,
    NULL::NUMBER AS created_vsnid,
    NULL::NUMBER AS modified_appid,
    NULL::NUMBER AS modified_vsnid
FROM pg_catalog.pg_trigger t
LEFT JOIN pg_catalog.pg_class c ON t.tgrelid = c.oid
UNION ALL
SELECT
    pg_catalog.pg_get_userbyid(y.typowner)::VARCHAR2(128) AS owner,
    y.typname::VARCHAR2(128) AS object_name,
    NULL::VARCHAR2(128) AS subobject_name,
    y.oid::INT AS object_id,
    NULL::NUMBER AS data_object_id,
    'TYPE'::VARCHAR2(23) AS object_type,
    NULL::DATE AS created,
    NULL::DATE AS last_ddl_time,
    NULL::VARCHAR2(19) AS timestamp,
    'VALID'::VARCHAR2(7) AS status,
    'N'::VARCHAR2(1) AS temporary,
    CASE
        WHEN y.typnamespace::regnamespace IN ('pg_catalog', 'information_schema', 'pg_toast') THEN 'Y'::VARCHAR2(1)
        ELSE 'N'::VARCHAR2(1)
    END AS generated,
    NULL::VARCHAR2(1) AS secondary,
    y.typnamespace::INT AS namespace,
    NULL::VARCHAR2(128) AS edition_name,
    NULL::VARCHAR2(18) AS sharing,
    NULL::VARCHAR2(1) AS editionable,
    NULL::VARCHAR2(1) AS oracle_maintained,
    NULL::VARCHAR2(1) AS application,
    NULL::VARCHAR2(100) AS default_collation,
    NULL::VARCHAR2(1) AS duplicated,
    NULL::VARCHAR2(1) AS sharded,
    NULL::NUMBER AS created_appid,
    NULL::NUMBER AS created_vsnid,
    NULL::NUMBER AS modified_appid,
    NULL::NUMBER AS modified_vsnid
FROM pg_catalog.pg_type y;

COMMENT ON VIEW sys.dba_objects IS 'View providing information about all objects(privileged view).';

----
-- SYS.ALL_OBJECTS
----
CREATE VIEW sys.all_objects AS
SELECT
    owner,
    object_name,
    subobject_name,
    object_id,
    data_object_id,
    object_type,
    created,
    last_ddl_time,
    timestamp,
    status,
    temporary,
    generated,
    secondary,
    namespace,
    edition_name,
    sharing,
    editionable,
    oracle_maintained,
    application,
    default_collation,
    duplicated,
    sharded,
    created_appid,
    created_vsnid,
    modified_appid,
    modified_vsnid
FROM sys.dba_objects 
WHERE pg_catalog.has_schema_privilege(namespace, 'USAGE')
AND 
(CASE 
 WHEN object_type ='SEQUENCE' THEN pg_catalog.has_sequence_privilege(object_id,'SELECT')
 WHEN object_type ='TABLE' THEN pg_catalog.has_table_privilege(object_id,'SELECT')
 WHEN object_type ='FUNCTION' THEN pg_catalog.has_function_privilege(object_id,'EXECUTE')
END
);

COMMENT ON VIEW sys.all_objects IS 'View providing information about all accessible objects.';

----
-- SYS.USER_OBJECTS
----
CREATE VIEW sys.user_objects AS
SELECT
    object_name,
    subobject_name,
    object_id,
    data_object_id,
    object_type,
    created,
    last_ddl_time,
    timestamp,
    status,
    temporary,
    generated,
    secondary,
    namespace,
    edition_name,
    sharing,
    editionable,
    oracle_maintained,
    application,
    default_collation,
    duplicated,
    sharded,
    created_appid,
    created_vsnid,
    modified_appid,
    modified_vsnid
FROM sys.all_objects 
WHERE owner=current_user;

COMMENT ON VIEW sys.user_objects IS 'View providing information about all accessible objects owned by the current user.';

----
-- SYS.DBA_PROCEDURES
----
CREATE VIEW sys.dba_procedures AS
SELECT
  pg_catalog.pg_get_userbyid(proowner)::VARCHAR2(128) AS owner,
  proname::VARCHAR2(128) AS object_name,
  proname::VARCHAR2(128) AS procedure_name, -- opentenbase does not support procedure
  oid::INT AS object_id,
  NULL::NUMBER AS subprogram_id,
  NULL::VARCHAR2(40) AS overload,   
  'FUNCTION'::VARCHAR2(13) AS object_type, 
  CASE WHEN proisagg = true THEN 'Y'::VARCHAR2(3) ELSE 'N'::VARCHAR2(3) END AS aggregate,
  NULL::VARCHAR2(3) AS pipelined,
  NULL::VARCHAR2(128) AS impltypeowner,
  NULL::VARCHAR2(128) AS impltypename,
  NULL::VARCHAR2(3) AS parallel,  
  NULL::VARCHAR2(3) AS interface,  
  NULL::VARCHAR2(3) AS deterministic,  
  NULL::VARCHAR2(12) AS authid,
  NULL::VARCHAR2(3) AS result_cache,
  NULL::VARCHAR2(256) AS origin_con_id,
  NULL::VARCHAR2(5) AS polymorphic
FROM pg_catalog.pg_proc;

COMMENT ON VIEW sys.dba_procedures IS 'View providing information about all procedures(privileged view).';

----
-- SYS.ALL_PROCEDURES
----
CREATE VIEW sys.all_procedures AS
SELECT
   owner,        
   object_name,
   procedure_name,
   object_id,
   subprogram_id,
   overload,
   object_type,  
   aggregate, 
   pipelined, 
   impltypeowner,
   impltypename, 
   parallel,
   interface,    
   deterministic,
   authid,
   result_cache, 
   origin_con_id,
   polymorphic 
FROM dba_procedures
WHERE pg_catalog.has_function_privilege(object_id,'EXECUTE');

COMMENT ON VIEW sys.all_procedures IS 'View providing information about all accessible procedures.';

----
-- SYS.USER_PROCEDURES
----
CREATE VIEW sys.user_procedures AS
SELECT
   owner,        
   object_name,
   procedure_name,
   object_id,
   subprogram_id,
   overload,
   object_type,  
   aggregate, 
   pipelined, 
   impltypeowner,
   impltypename, 
   parallel,
   interface,    
   deterministic,
   authid,
   result_cache, 
   origin_con_id,
   polymorphic 
FROM all_procedures
WHERE owner=current_user;

COMMENT ON VIEW sys.user_procedures IS 'View providing information about all accessible procedures owned by the current user.';

------------------------------------------------------------------------------
-- GRANTS
------------------------------------------------------------------------------
GRANT USAGE ON SCHEMA sys TO PUBLIC;

------------------------------------------------------------------------------
-- REVOKE
------------------------------------------------------------------------------
REVOKE ALL ON sys.dba_objects FROM PUBLIC;
REVOKE ALL ON sys.all_objects FROM PUBLIC;
REVOKE ALL ON sys.user_objects FROM PUBLIC;
REVOKE ALL ON sys.dba_procedures FROM PUBLIC;
REVOKE ALL ON sys.all_procedures FROM PUBLIC;
REVOKE ALL ON sys.user_procedures FROM PUBLIC;

GRANT SELECT ON sys.all_objects TO PUBLIC;
GRANT SELECT ON sys.user_objects TO PUBLIC;
GRANT SELECT ON sys.all_procedures TO PUBLIC;
GRANT SELECT ON sys.user_procedures TO PUBLIC;