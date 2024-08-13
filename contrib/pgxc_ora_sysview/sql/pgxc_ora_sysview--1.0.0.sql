----
-- Script to create the objects of the pgxc_ora_sysview extension
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
    NULL::INT AS data_object_id,
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
    NULL::BIGINT AS created_appid,
    NULL::BIGINT AS created_vsnid,
    NULL::BIGINT AS modified_appid,
    NULL::BIGINT AS modified_vsnid
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_index idx ON c.oid = idx.indrelid
UNION ALL
SELECT
    pg_catalog.pg_get_userbyid(p.proowner)::VARCHAR2(128) AS owner,
    p.proname::VARCHAR2(128) AS object_name,
    NULL::VARCHAR2(128) AS subobject_name,
    p.oid::INT AS object_id,
    NULL::INT AS data_object_id,
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
    NULL::BIGINT AS created_appid,
    NULL::BIGINT AS created_vsnid,
    NULL::BIGINT AS modified_appid,
    NULL::BIGINT AS modified_vsnid
FROM pg_catalog.pg_proc p
UNION ALL
SELECT
    pg_catalog.pg_get_userbyid(c.relowner)::VARCHAR2(128) AS owner,
    c.relname::VARCHAR2(128) AS object_name,
    NULL::VARCHAR2(128) AS subobject_name,
    c.oid::INT AS object_id,
    NULL::INT AS data_object_id,
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
    NULL::BIGINT AS created_appid,
    NULL::BIGINT AS created_vsnid,
    NULL::BIGINT AS modified_appid,
    NULL::BIGINT AS modified_vsnid
FROM pg_catalog.pg_trigger t
LEFT JOIN pg_catalog.pg_class c ON t.tgrelid = c.oid
UNION ALL
SELECT
    pg_catalog.pg_get_userbyid(y.typowner)::VARCHAR2(128) AS owner,
    y.typname::VARCHAR2(128) AS object_name,
    NULL::VARCHAR2(128) AS subobject_name,
    y.oid::INT AS object_id,
    NULL::INT AS data_object_id,
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
    NULL::BIGINT AS created_appid,
    NULL::BIGINT AS created_vsnid,
    NULL::BIGINT AS modified_appid,
    NULL::BIGINT AS modified_vsnid
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
END);

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
-- SYS.DBA_TABLES
----

CREATE VIEW sys.dba_tables AS
SELECT
    pg_catalog.pg_get_userbyid(c.relowner)::VARCHAR2(128) AS owner,
    c.relname::VARCHAR2(128) AS table_name,
    (SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace)::VARCHAR2(30) AS tablespace_name,
    --c.relnamespace::regnamespace::VARCHAR2(30) AS schema_name,
    NULL::VARCHAR2(128) AS cluster_name,
    NULL::VARCHAR2(128) AS iot_name,
    'VALID'::VARCHAR2(7) AS status,
    CASE 
        WHEN c.relkind = 'p' THEN NULL
        WHEN array_to_string(reloptions, '') LIKE '%fillfactor=%' THEN
            100 - (regexp_match(array_to_string(reloptions, ''), 'fillfactor=([0-9]+)'))[1]::NUMBER
        ELSE '0'::NUMBER
    END AS pct_free,
    NULL::NUMBER AS pct_used,
    NULL::NUMBER AS ini_trans,
    NULL::NUMBER AS max_trans,
    NULL::NUMBER AS initial_extent,
    NULL::NUMBER AS next_extent,
    NULL::NUMBER AS min_extents,
    NULL::NUMBER AS max_extents,
    NULL::NUMBER AS pct_increase,
    NULL::NUMBER AS freelists,
    NULL::NUMBER AS freelist_groups,
    CASE 
        WHEN c.relkind = 'p' THEN NULL
        WHEN relpersistence = 'u' THEN 'YES'::VARCHAR2(3)
        ELSE 'NO'::VARCHAR2(3)
    END AS logging,
    NULL::VARCHAR2(1) AS backed_up,
    c.reltuples::NUMBER AS num_rows,
    c.relpages::NUMBER AS blocks,
    NULL::NUMBER AS empty_blocks,
    NULL::NUMBER AS avg_space,
    NULL::NUMBER AS chain_cnt,
    NULL::NUMBER AS avg_row_len,
    NULL::NUMBER AS avg_space_freelist_blocks,
    NULL::NUMBER AS num_freelist_blocks,
    'DEFAULT'::VARCHAR2(10) AS degree,
    'DEFAULT'::VARCHAR2(10) AS instances,
    NULL::VARCHAR2(5) AS cache,  -- Using extension is possible but impacts performance,so it is not recommended to add it.
    NULL::VARCHAR2(8) AS table_lock,
    NULL::NUMBER AS sample_size,
    s.last_analyze::DATE AS last_analyzed,
    CASE 
        WHEN c.relkind = 'p' THEN 'YES'
        ELSE 'NO'
    END AS partitioned,
    NULL::VARCHAR2(12) AS iot_type,
    CASE
        WHEN c.relpersistence = 't' THEN 'Y'::VARCHAR2(1)
        ELSE 'N'::VARCHAR2(1)
    END AS temporary,
    'N'::VARCHAR2(1) AS secondary,
    'NO'::VARCHAR2(3) AS nested,
    NULL::VARCHAR2(7) AS buffer_pool,
    NULL::VARCHAR2(7) AS flash_cache,
    NULL::VARCHAR2(7) AS cell_flash_cache,
    NULL::VARCHAR2(8) AS row_movement,
    NULL::VARCHAR2(3) AS global_stats,
    'NO'::VARCHAR2(3) AS user_stats,
    'NUll'::VARCHAR2(15) AS duration,
    -- CASE THEN current_setting('zero_damaged_pages') = on THEN 'ENABLED' ELSE
    'DISABLED'::VARCHAR2(8) AS skip_corrupt,
    NULL::VARCHAR2(3) AS monitoring,
    NULL::VARCHAR2(128) AS cluster_owner,
    NULL::VARCHAR2(8) AS dependencies,
    NULL::VARCHAR2(8) AS compression, -- Compression can be specified at compile time
    NULL::VARCHAR2(30) AS compress_for, 
    'NO'::VARCHAR2(3) AS dropped, -- need recycle bin
    CASE 
        WHEN c.relkind = 'p' THEN 'N/A'::VARCHAR2(3)
        ELSE 'NO'::VARCHAR2(3)
    END AS read_only,
    CASE 
        WHEN c.relkind = 'p' THEN 'N/A'::VARCHAR2(3)
        ELSE 'NO'::VARCHAR2(3)
    END AS segment_created,
    NULL::VARCHAR2(7) AS result_cache,
    'NO'::VARCHAR2(3) AS clustering,
    NULL::VARCHAR2(23) AS activity_tracking,
    NULL::DATE AS dml_timestamp,
    NULL::VARCHAR2(3) AS has_identity,
    'NO'::VARCHAR2(3) AS container_data,
    NULL::VARCHAR2(8) AS inmemory,
    NULL::VARCHAR2(8) AS inmemory_priority,
    NULL::VARCHAR2(15) AS inmemory_distribute,
    NULL::VARCHAR2(17) AS inmemory_compression,
    NULL::VARCHAR2(13) AS inmemory_duplicate,
    (select datcollate from pg_catalog.pg_database WHERE datname = current_database())::VARCHAR2(100) AS default_collation,
    'Y'::VARCHAR2(1) AS duplicated,
    'Y'::VARCHAR2(1) AS sharded,
    'NO'::VARCHAR2(3) AS external,
    NULL::VARCHAR2(3) AS hybrid,
    NULL::VARCHAR2(24) AS cellmemory,
    NULL::VARCHAR2(3) AS containers_default,
    NULL::VARCHAR2(3) AS container_map,
    NULL::VARCHAR2(3) AS extended_data_link,
    NULL::VARCHAR2(3) AS extended_data_link_map,
    NULL::VARCHAR2(12) AS inmemory_service,
    NULL::VARCHAR2(1000) AS inmemory_service_name,
    NULL::VARCHAR2(3) AS container_map_object,
    NULL::VARCHAR2(8) AS memoptimize_read,
    NULL::VARCHAR2(8) AS memoptimize_write,
    NULL::VARCHAR2(3) AS has_sensitive_column,
    NULL::VARCHAR2(3) AS admit_null,
    NULL::VARCHAR2(3) AS data_link_dml_enabled,
    CASE
        WHEN c.relreplident = 'n' THEN 'DISABLED'
        ELSE 'ENABLED'
    END AS logical_replication
FROM pg_catalog.pg_class c 
LEFT JOIN pg_catalog.pg_stat_all_tables s ON c.relname = s.relname 
WHERE c.relkind IN ('r','p') 
AND relnamespace::regnamespace NOT IN ('pg_toast','pg_toast_temp_1','sys');

COMMENT ON VIEW sys.dba_tables IS 'View providing information about all tables(privileged view).';

----
-- SYS.ALL_TABLES
----
CREATE VIEW sys.all_tables AS
SELECT
    pg_catalog.pg_get_userbyid(c.relowner)::VARCHAR2(128) AS owner,
    c.relname::VARCHAR2(128) AS table_name,
    (SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace)::VARCHAR2(30) AS tablespace_name,
    --c.relnamespace::regnamespace::VARCHAR2(30) AS schema_name,
    NULL::VARCHAR2(128) AS cluster_name,
    NULL::VARCHAR2(128) AS iot_name,
    'VALID'::VARCHAR2(7) AS status,
    CASE 
        WHEN c.relkind = 'p' THEN NULL
        WHEN array_to_string(reloptions, '') LIKE '%fillfactor=%' THEN
            100 - (regexp_match(array_to_string(reloptions, ''), 'fillfactor=([0-9]+)'))[1]::NUMBER
        ELSE '0'::NUMBER
    END AS pct_free,
    NULL::NUMBER AS pct_used,
    NULL::NUMBER AS ini_trans,
    NULL::NUMBER AS max_trans,
    NULL::NUMBER AS initial_extent,
    NULL::NUMBER AS next_extent,
    NULL::NUMBER AS min_extents,
    NULL::NUMBER AS max_extents,
    NULL::NUMBER AS pct_increase,
    NULL::NUMBER AS freelists,
    NULL::NUMBER AS freelist_groups,
    CASE 
        WHEN c.relkind = 'p' THEN NULL
        WHEN relpersistence = 'u' THEN 'YES'::VARCHAR2(3)
        ELSE 'NO'::VARCHAR2(3)
    END AS logging,
    NULL::VARCHAR2(1) AS backed_up,
    c.reltuples::NUMBER AS num_rows,
    c.relpages::NUMBER AS blocks,
    NULL::NUMBER AS empty_blocks,
    NULL::NUMBER AS avg_space,
    NULL::NUMBER AS chain_cnt,
    NULL::NUMBER AS avg_row_len,
    NULL::NUMBER AS avg_space_freelist_blocks,
    NULL::NUMBER AS num_freelist_blocks,
    'DEFAULT'::VARCHAR2(10) AS degree,
    'DEFAULT'::VARCHAR2(10) AS instances,
    NULL::VARCHAR2(5) AS cache,  -- Using extension is possible but impacts performance,so it is not recommended to add it.
    NULL::VARCHAR2(8) AS table_lock,
    NULL::NUMBER AS sample_size,
    s.last_analyze::DATE AS last_analyzed,
    CASE 
        WHEN c.relkind = 'p' THEN 'YES'
        ELSE 'NO'
    END AS partitioned,
    NULL::VARCHAR2(12) AS iot_type,
    CASE
        WHEN c.relpersistence = 't' THEN 'Y'::VARCHAR2(1)
        ELSE 'N'::VARCHAR2(1)
    END AS temporary,
    'N'::VARCHAR2(1) AS secondary,
    'NO'::VARCHAR2(3) AS nested,
    NULL::VARCHAR2(7) AS buffer_pool,
    NULL::VARCHAR2(7) AS flash_cache,
    NULL::VARCHAR2(7) AS cell_flash_cache,
    NULL::VARCHAR2(8) AS row_movement,
    NULL::VARCHAR2(3) AS global_stats,
    'NO'::VARCHAR2(3) AS user_stats,
    'NUll'::VARCHAR2(15) AS duration,
    -- CASE THEN current_setting('zero_damaged_pages') = on THEN 'ENABLED' ELSE
    'DISABLED'::VARCHAR2(8) AS skip_corrupt,
    NULL::VARCHAR2(3) AS monitoring,
    NULL::VARCHAR2(128) AS cluster_owner,
    NULL::VARCHAR2(8) AS dependencies,
    NULL::VARCHAR2(8) AS compression, -- Compression can be specified at compile time
    NULL::VARCHAR2(30) AS compress_for, 
    'NO'::VARCHAR2(3) AS dropped, -- need recycle bin
    CASE 
        WHEN c.relkind = 'p' THEN 'N/A'::VARCHAR2(3)
        ELSE 'NO'::VARCHAR2(3)
    END AS read_only,
    CASE 
        WHEN c.relkind = 'p' THEN 'N/A'::VARCHAR2(3)
        ELSE 'NO'::VARCHAR2(3)
    END AS segment_created,
    NULL::VARCHAR2(7) AS result_cache,
    'NO'::VARCHAR2(3) AS clustering,
    NULL::VARCHAR2(23) AS activity_tracking,
    NULL::DATE AS dml_timestamp,
    NULL::VARCHAR2(3) AS has_identity,
    'NO'::VARCHAR2(3) AS container_data,
    NULL::VARCHAR2(8) AS inmemory,
    NULL::VARCHAR2(8) AS inmemory_priority,
    NULL::VARCHAR2(15) AS inmemory_distribute,
    NULL::VARCHAR2(17) AS inmemory_compression,
    NULL::VARCHAR2(13) AS inmemory_duplicate,
    (select datcollate from pg_catalog.pg_database WHERE datname = current_database())::VARCHAR2(100) AS default_collation,
    'Y'::VARCHAR2(1) AS duplicated,
    'Y'::VARCHAR2(1) AS sharded,
    'NO'::VARCHAR2(3) AS external,
    NULL::VARCHAR2(3) AS hybrid,
    NULL::VARCHAR2(24) AS cellmemory,
    NULL::VARCHAR2(3) AS containers_default,
    NULL::VARCHAR2(3) AS container_map,
    NULL::VARCHAR2(3) AS extended_data_link,
    NULL::VARCHAR2(3) AS extended_data_link_map,
    NULL::VARCHAR2(12) AS inmemory_service,
    NULL::VARCHAR2(1000) AS inmemory_service_name,
    NULL::VARCHAR2(3) AS container_map_object,
    NULL::VARCHAR2(8) AS memoptimize_read,
    NULL::VARCHAR2(8) AS memoptimize_write,
    NULL::VARCHAR2(3) AS has_sensitive_column,
    NULL::VARCHAR2(3) AS admit_null,
    NULL::VARCHAR2(3) AS data_link_dml_enabled,
    CASE
        WHEN c.relreplident = 'n' THEN 'DISABLED'
        ELSE 'ENABLED'
    END AS logical_replication
FROM pg_catalog.pg_class c 
LEFT JOIN pg_catalog.pg_stat_all_tables s ON c.relname = s.relname 
LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind IN ('r','p') 
AND n.nspname NOT IN ('pg_toast','pg_toast_temp_1','sys')
AND pg_catalog.has_schema_privilege(c.relnamespace, 'USAGE') 
AND has_table_privilege(quote_ident(n.nspname) || '.' || quote_ident(c.relname), 'SELECT');


COMMENT ON VIEW sys.all_tables IS 'View providing information about all accessible tables.';

----
-- SYS.USER_TABLES
----
CREATE VIEW sys.user_tables AS
SELECT 
    table_name,
    tablespace_name,
    cluster_name,
    iot_name,
    status,
    pct_free,
    pct_used,
    ini_trans,
    max_trans,
    initial_extent,
    next_extent,
    min_extents,
    max_extents,
    pct_increase,
    freelists,
    freelist_groups,
    logging,
    backed_up,
    num_rows,
    blocks,
    empty_blocks,
    avg_space,
    chain_cnt,
    avg_row_len,
    avg_space_freelist_blocks,
    num_freelist_blocks,
    degree,
    instances,
    cache,
    table_lock,
    sample_size,
    last_analyzed,
    partitioned,
    iot_type,
    temporary,
    secondary,
    nested,
    buffer_pool,
    flash_cache,
    cell_flash_cache,
    row_movement,
    global_stats,
    user_stats,
    duration,
    skip_corrupt,
    monitoring,
    cluster_owner,
    dependencies,
    compression,
    compress_for,
    dropped,
    read_only,
    segment_created,
    result_cache,
    clustering,
    activity_tracking,
    dml_timestamp,
    has_identity,
    container_data,
    inmemory,
    inmemory_priority,
    inmemory_distribute,
    inmemory_compression,
    inmemory_duplicate,
    default_collation,
    duplicated,
    sharded,
    external,
    hybrid,
    cellmemory,
    containers_default,
    container_map,
    extended_data_link,
    extended_data_link_map,
    inmemory_service,
    inmemory_service_name,
    container_map_object,
    memoptimize_read,
    memoptimize_write,
    has_sensitive_column,
    admit_null,
    data_link_dml_enabled,
    logical_replication 
FROM sys.all_tables 
WHERE owner=current_user;

COMMENT ON VIEW sys.user_objects IS 'View providing information about all accessible tables owned by the current user.';

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
FROM sys.dba_procedures
WHERE pg_catalog.has_function_privilege(object_id,'EXECUTE');

COMMENT ON VIEW sys.all_procedures IS 'View providing information about all accessible procedures.';

----
-- SYS.USER_PROCEDURES
----
CREATE VIEW sys.user_procedures AS
SELECT
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
FROM sys.all_procedures
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
REVOKE ALL ON sys.dba_tables FROM PUBLIC;
REVOKE ALL ON sys.all_tables FROM PUBLIC;
REVOKE ALL ON sys.user_tables FROM PUBLIC;
REVOKE ALL ON sys.dba_procedures FROM PUBLIC;
REVOKE ALL ON sys.all_procedures FROM PUBLIC;
REVOKE ALL ON sys.user_procedures FROM PUBLIC;

GRANT SELECT ON sys.all_objects TO PUBLIC;
GRANT SELECT ON sys.user_objects TO PUBLIC;
GRANT SELECT ON sys.all_tables TO PUBLIC;
GRANT SELECT ON sys.user_tables TO PUBLIC;
GRANT SELECT ON sys.all_procedures TO PUBLIC;
GRANT SELECT ON sys.user_procedures TO PUBLIC;
