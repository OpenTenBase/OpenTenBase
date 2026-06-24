-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.24'" to load this file. \quit


CREATE OR REPLACE VIEW all_tables AS
 select cast(OWNER as varchar2(128)) AS OWNER,
		SCHEMA_NAME,
	    cast(TABLE_NAME as varchar2(128)) AS TABLE_NAME,
		cast(TABLESPACE_NAME as varchar2(30)) AS TABLESPACE_NAME,
		cast(CLUSTER_NAME as varchar2(128)) AS CLUSTER_NAME,
		cast(IOT_NAME as varchar2(128)) AS IOT_NAME,
		cast(STATUS as varchar2(8)) AS STATUS,
		cast(PCT_FREE as number) AS PCT_FREE,
		cast(PCT_USED as number) AS PCT_USED,
		cast(INI_TRANS as number) AS INI_TRANS,
		cast(MAX_TRANS as number) AS MAX_TRANS,
		cast(INITIAL_EXTENT as number) AS INITIAL_EXTENT,
		cast(NEXT_EXTENT as number) AS NEXT_EXTENT,
		cast(MIN_EXTENTS as number) AS MIN_EXTENTS,
		cast(MAX_EXTENTS as number) AS MAX_EXTENTS,
		cast(PCT_INCREASE as number) AS PCT_INCREASE,
		cast(FREELISTS as number) AS FREELISTS,
		cast(FREELIST_GROUPS as number) AS FREELIST_GROUPS,
		cast(LOGGING as varchar2(3)) AS LOGGING,
		cast(BACKED_UP as varchar2(1)) AS BACKED_UP,
		cast(NUM_ROWS as number) AS NUM_ROWS,
		cast(BLOCKS as number) AS BLOCKS,
		cast(EMPTY_BLOCKS as number) AS EMPTY_BLOCKS,
		cast(AVG_SPACE as number) AS AVG_SPACE,
		cast(CHAIN_CNT as number) AS CHAIN_CNT,
		cast(AVG_ROW_LEN as number) AS AVG_ROW_LEN,
		cast(AVG_SPACE_FREELIST_BLOCKS as number) AS AVG_SPACE_FREELIST_BLOCKS,
		cast(NUM_FREELIST_BLOCKS as number) AS NUM_FREELIST_BLOCKS,
		cast(DEGREE as varchar2(10)) AS DEGREE,
		cast(INSTANCES as varchar2(10)) AS INSTANCES,
		cast(CACHE as varchar2(5)) AS CACHE,
		cast(TABLE_LOCK as varchar2(8)) AS TABLE_LOCK,
		cast(SAMPLE_SIZE as number) AS SAMPLE_SIZE,
		cast(LAST_ANALYZED as pg_catalog.date) AS LAST_ANALYZED,
		cast(PARTITIONED as varchar2(3)) AS PARTITIONED,
		cast(IOT_TYPE as varchar2(12)) AS IOT_TYPE,
		cast(TEMPORARY as varchar2(1)) AS TEMPORARY,
		cast(SECONDARY as varchar2(1)) AS SECONDARY,
		cast(NESTED as varchar2(3)) AS NESTED,
		cast(BUFFER_POOL as varchar2(7)) AS BUFFER_POOL,
		cast(FLASH_CACHE as varchar2(7)) AS FLASH_CACHE,
		cast(CELL_FLASH_CACHE as varchar2(7)) AS CELL_FLASH_CACHE,
		cast(ROW_MOVEMENT as varchar2(8)) AS ROW_MOVEMENT,
		cast(GLOBAL_STATS as varchar2(3)) AS GLOBAL_STATS,
		cast(USER_STATS as varchar2(3)) AS USER_STATS,
		cast(DURATION as varchar2(15)) AS DURATION,
		cast(SKIP_CORRUPT as varchar2(8)) AS SKIP_CORRUPT,
		cast(MONITORING as varchar2(3)) AS MONITORING,
		cast(CLUSTER_OWNER as varchar2(128)) AS CLUSTER_OWNER,
		cast(DEPENDENCIES as varchar2(8)) AS DEPENDENCIES,
		cast(COMPRESSION as varchar2(8)) AS COMPRESSION,
		cast(COMPRESS_FOR as varchar2(30)) AS COMPRESS_FOR,
		cast(DROPPED as varchar2(3)) AS DROPPED,
		cast(READ_ONLY as varchar2(3)) AS READ_ONLY,
		cast(SEGMENT_CREATED as varchar2(3)) AS SEGMENT_CREATED,
		cast(RESULT_CACHE as varchar2(7)) AS RESULT_CACHE
	from ( select (SELECT pg_authid.rolname
           FROM pg_authid
          WHERE pg_authid.oid = c.relowner) AS OWNER, n.nspname as SCHEMA_NAME, c.relname as TABLE_NAME, t.spcname as TABLESPACE_NAME,
			NULL as CLUSTER_NAME, NULL as IOT_NAME, 'VALID' as STATUS, NULL as PCT_FREE, NULL as PCT_USED, 
			NULL as INI_TRANS, NULL as MAX_TRANS, NULL as INITIAL_EXTENT, NULL as NEXT_EXTENT, NULL as MIN_EXTENTS, 
			NULL as MAX_EXTENTS, NULL as PCT_INCREASE, NULL as FREELISTS, NULL as FREELIST_GROUPS,
			case when c.relpersistence = 'p' then 'YES' else 'NO' end as LOGGING, NULL as BACKED_UP,
			c.reltuples as NUM_ROWS, c.relpages as BLOCKS,
			NULL as EMPTY_BLOCKS, NULL as AVG_SPACE, NULL as CHAIN_CNT, NULL as AVG_ROW_LEN, NULL as AVG_SPACE_FREELIST_BLOCKS, 
			NULL as NUM_FREELIST_BLOCKS, NULL as DEGREE, NULL as INSTANCES, NULL as CACHE, NULL as TABLE_LOCK, NULL as SAMPLE_SIZE,
			s.last_analyze as LAST_ANALYZED, 
			case when c.relkind = 'p' then 'YES' else 'NO' end as PARTITIONED, NULL as IOT_TYPE, 
			case when c.relpersistence in ('t','g') then 'Y' else 'N' end as TEMPORARY, NULL as SECONDARY, NULL as NESTED,
			NULL as BUFFER_POOL, NULL as FLASH_CACHE, NULL as CELL_FLASH_CACHE, NULL as ROW_MOVEMENT, NULL as GLOBAL_STATS,
			NULL as USER_STATS, 
			case when s.last_analyze is not null then systimestamp - s.last_analyze else NULL END  AS DURATION, 
			NULL as SKIP_CORRUPT, NULL as MONITORING, NULL as CLUSTER_OWNER,
			NULL as DEPENDENCIES, NULL as COMPRESSION, NULL as COMPRESS_FOR, NULL as DROPPED, NULL as READ_ONLY,
			NULL as SEGMENT_CREATED, NULL as RESULT_CACHE
			from pg_class c left join pg_namespace n on n.oid = c.relnamespace
							left join pg_tablespace t on t.oid = c.reltablespace
							left join pg_stat_all_tables s on s.relid = c.oid
			where (NOT pg_is_other_temp_schema(n.oid)) AND c.relkind IN ('r', 'f', 'p') 
			AND (pg_has_role(c.relowner, 'USAGE') OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
				OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES'))
		  );
GRANT SELECT ON all_tables TO PUBLIC;

CREATE OR REPLACE VIEW dba_tables AS
 select cast(OWNER as varchar2(128)) AS OWNER,
		SCHEMA_NAME,
	    cast(TABLE_NAME as varchar2(128)) AS TABLE_NAME,
		cast(TABLESPACE_NAME as varchar2(30)) AS TABLESPACE_NAME,
		cast(CLUSTER_NAME as varchar2(128)) AS CLUSTER_NAME,
		cast(IOT_NAME as varchar2(128)) AS IOT_NAME,
		cast(STATUS as varchar2(8)) AS STATUS,
		cast(PCT_FREE as number) AS PCT_FREE,
		cast(PCT_USED as number) AS PCT_USED,
		cast(INI_TRANS as number) AS INI_TRANS,
		cast(MAX_TRANS as number) AS MAX_TRANS,
		cast(INITIAL_EXTENT as number) AS INITIAL_EXTENT,
		cast(NEXT_EXTENT as number) AS NEXT_EXTENT,
		cast(MIN_EXTENTS as number) AS MIN_EXTENTS,
		cast(MAX_EXTENTS as number) AS MAX_EXTENTS,
		cast(PCT_INCREASE as number) AS PCT_INCREASE,
		cast(FREELISTS as number) AS FREELISTS,
		cast(FREELIST_GROUPS as number) AS FREELIST_GROUPS,
		cast(LOGGING as varchar2(3)) AS LOGGING,
		cast(BACKED_UP as varchar2(1)) AS BACKED_UP,
		cast(NUM_ROWS as number) AS NUM_ROWS,
		cast(BLOCKS as number) AS BLOCKS,
		cast(EMPTY_BLOCKS as number) AS EMPTY_BLOCKS,
		cast(AVG_SPACE as number) AS AVG_SPACE,
		cast(CHAIN_CNT as number) AS CHAIN_CNT,
		cast(AVG_ROW_LEN as number) AS AVG_ROW_LEN,
		cast(AVG_SPACE_FREELIST_BLOCKS as number) AS AVG_SPACE_FREELIST_BLOCKS,
		cast(NUM_FREELIST_BLOCKS as number) AS NUM_FREELIST_BLOCKS,
		cast(DEGREE as varchar2(10)) AS DEGREE,
		cast(INSTANCES as varchar2(10)) AS INSTANCES,
		cast(CACHE as varchar2(5)) AS CACHE,
		cast(TABLE_LOCK as varchar2(8)) AS TABLE_LOCK,
		cast(SAMPLE_SIZE as number) AS SAMPLE_SIZE,
		cast(LAST_ANALYZED as pg_catalog.date) AS LAST_ANALYZED,
		cast(PARTITIONED as varchar2(3)) AS PARTITIONED,
		cast(IOT_TYPE as varchar2(12)) AS IOT_TYPE,
		cast(TEMPORARY as varchar2(1)) AS TEMPORARY,
		cast(SECONDARY as varchar2(1)) AS SECONDARY,
		cast(NESTED as varchar2(3)) AS NESTED,
		cast(BUFFER_POOL as varchar2(7)) AS BUFFER_POOL,
		cast(FLASH_CACHE as varchar2(7)) AS FLASH_CACHE,
		cast(CELL_FLASH_CACHE as varchar2(7)) AS CELL_FLASH_CACHE,
		cast(ROW_MOVEMENT as varchar2(8)) AS ROW_MOVEMENT,
		cast(GLOBAL_STATS as varchar2(3)) AS GLOBAL_STATS,
		cast(USER_STATS as varchar2(3)) AS USER_STATS,
		cast(DURATION as varchar2(15)) AS DURATION,
		cast(SKIP_CORRUPT as varchar2(8)) AS SKIP_CORRUPT,
		cast(MONITORING as varchar2(3)) AS MONITORING,
		cast(CLUSTER_OWNER as varchar2(128)) AS CLUSTER_OWNER,
		cast(DEPENDENCIES as varchar2(8)) AS DEPENDENCIES,
		cast(COMPRESSION as varchar2(8)) AS COMPRESSION,
		cast(COMPRESS_FOR as varchar2(30)) AS COMPRESS_FOR,
		cast(DROPPED as varchar2(3)) AS DROPPED,
		cast(READ_ONLY as varchar2(3)) AS READ_ONLY,
		cast(SEGMENT_CREATED as varchar2(3)) AS SEGMENT_CREATED,
		cast(RESULT_CACHE as varchar2(7)) AS RESULT_CACHE
	from ( select (SELECT pg_authid.rolname
           FROM pg_authid
          WHERE pg_authid.oid = c.relowner) AS OWNER, n.nspname as SCHEMA_NAME, c.relname as TABLE_NAME, t.spcname as TABLESPACE_NAME,
			NULL as CLUSTER_NAME, NULL as IOT_NAME, 'VALID' as STATUS, NULL as PCT_FREE, NULL as PCT_USED, 
			NULL as INI_TRANS, NULL as MAX_TRANS, NULL as INITIAL_EXTENT, NULL as NEXT_EXTENT, NULL as MIN_EXTENTS, 
			NULL as MAX_EXTENTS, NULL as PCT_INCREASE, NULL as FREELISTS, NULL as FREELIST_GROUPS,
			case when c.relpersistence = 'p' then 'YES' else 'NO' end as LOGGING, NULL as BACKED_UP,
			c.reltuples as NUM_ROWS, c.relpages as BLOCKS,
			NULL as EMPTY_BLOCKS, NULL as AVG_SPACE, NULL as CHAIN_CNT, NULL as AVG_ROW_LEN, NULL as AVG_SPACE_FREELIST_BLOCKS, 
			NULL as NUM_FREELIST_BLOCKS, NULL as DEGREE, NULL as INSTANCES, NULL as CACHE, NULL as TABLE_LOCK, NULL as SAMPLE_SIZE,
			s.last_analyze as LAST_ANALYZED, 
			case when c.relkind = 'p' then 'YES' else 'NO' end as PARTITIONED, NULL as IOT_TYPE, 
			case when c.relpersistence in ('t','g') then 'Y' else 'N' end as TEMPORARY, NULL as SECONDARY, NULL as NESTED,
			NULL as BUFFER_POOL, NULL as FLASH_CACHE, NULL as CELL_FLASH_CACHE, NULL as ROW_MOVEMENT, NULL as GLOBAL_STATS,
			NULL as USER_STATS,
			case when s.last_analyze is not null then systimestamp - s.last_analyze else NULL END  AS DURATION, 
			NULL as SKIP_CORRUPT, NULL as MONITORING, NULL as CLUSTER_OWNER,
			NULL as DEPENDENCIES, NULL as COMPRESSION, NULL as COMPRESS_FOR, NULL as DROPPED, NULL as READ_ONLY,
			NULL as SEGMENT_CREATED, NULL as RESULT_CACHE
			from pg_class c left join pg_namespace n on n.oid = c.relnamespace
							left join pg_tablespace t on t.oid = c.reltablespace
							left join pg_stat_all_tables s on s.relid = c.oid
			where (NOT pg_is_other_temp_schema(n.oid)) AND c.relkind IN ('r', 'f', 'p') 
		  );
GRANT SELECT ON dba_tables TO PUBLIC;

CREATE OR REPLACE VIEW user_tables AS
  select SCHEMA_NAME,
	    cast(TABLE_NAME as varchar2(128)) AS TABLE_NAME,
		cast(TABLESPACE_NAME as varchar2(30)) AS TABLESPACE_NAME,
		cast(CLUSTER_NAME as varchar2(128)) AS CLUSTER_NAME,
		cast(IOT_NAME as varchar2(128)) AS IOT_NAME,
		cast(STATUS as varchar2(8)) AS STATUS,
		cast(PCT_FREE as number) AS PCT_FREE,
		cast(PCT_USED as number) AS PCT_USED,
		cast(INI_TRANS as number) AS INI_TRANS,
		cast(MAX_TRANS as number) AS MAX_TRANS,
		cast(INITIAL_EXTENT as number) AS INITIAL_EXTENT,
		cast(NEXT_EXTENT as number) AS NEXT_EXTENT,
		cast(MIN_EXTENTS as number) AS MIN_EXTENTS,
		cast(MAX_EXTENTS as number) AS MAX_EXTENTS,
		cast(PCT_INCREASE as number) AS PCT_INCREASE,
		cast(FREELISTS as number) AS FREELISTS,
		cast(FREELIST_GROUPS as number) AS FREELIST_GROUPS,
		cast(LOGGING as varchar2(3)) AS LOGGING,
		cast(BACKED_UP as varchar2(1)) AS BACKED_UP,
		cast(NUM_ROWS as number) AS NUM_ROWS,
		cast(BLOCKS as number) AS BLOCKS,
		cast(EMPTY_BLOCKS as number) AS EMPTY_BLOCKS,
		cast(AVG_SPACE as number) AS AVG_SPACE,
		cast(CHAIN_CNT as number) AS CHAIN_CNT,
		cast(AVG_ROW_LEN as number) AS AVG_ROW_LEN,
		cast(AVG_SPACE_FREELIST_BLOCKS as number) AS AVG_SPACE_FREELIST_BLOCKS,
		cast(NUM_FREELIST_BLOCKS as number) AS NUM_FREELIST_BLOCKS,
		cast(DEGREE as varchar2(10)) AS DEGREE,
		cast(INSTANCES as varchar2(10)) AS INSTANCES,
		cast(CACHE as varchar2(5)) AS CACHE,
		cast(TABLE_LOCK as varchar2(8)) AS TABLE_LOCK,
		cast(SAMPLE_SIZE as number) AS SAMPLE_SIZE,
		cast(LAST_ANALYZED as pg_catalog.date) AS LAST_ANALYZED,
		cast(PARTITIONED as varchar2(3)) AS PARTITIONED,
		cast(IOT_TYPE as varchar2(12)) AS IOT_TYPE,
		cast(TEMPORARY as varchar2(1)) AS TEMPORARY,
		cast(SECONDARY as varchar2(1)) AS SECONDARY,
		cast(NESTED as varchar2(3)) AS NESTED,
		cast(BUFFER_POOL as varchar2(7)) AS BUFFER_POOL,
		cast(FLASH_CACHE as varchar2(7)) AS FLASH_CACHE,
		cast(CELL_FLASH_CACHE as varchar2(7)) AS CELL_FLASH_CACHE,
		cast(ROW_MOVEMENT as varchar2(8)) AS ROW_MOVEMENT,
		cast(GLOBAL_STATS as varchar2(3)) AS GLOBAL_STATS,
		cast(USER_STATS as varchar2(3)) AS USER_STATS,
		cast(DURATION as varchar2(15)) AS DURATION,
		cast(SKIP_CORRUPT as varchar2(8)) AS SKIP_CORRUPT,
		cast(MONITORING as varchar2(3)) AS MONITORING,
		cast(CLUSTER_OWNER as varchar2(128)) AS CLUSTER_OWNER,
		cast(DEPENDENCIES as varchar2(8)) AS DEPENDENCIES,
		cast(COMPRESSION as varchar2(8)) AS COMPRESSION,
		cast(COMPRESS_FOR as varchar2(30)) AS COMPRESS_FOR,
		cast(DROPPED as varchar2(3)) AS DROPPED,
		cast(READ_ONLY as varchar2(3)) AS READ_ONLY,
		cast(SEGMENT_CREATED as varchar2(3)) AS SEGMENT_CREATED,
		cast(RESULT_CACHE as varchar2(7)) AS RESULT_CACHE
	from (select n.nspname as SCHEMA_NAME, c.relname as TABLE_NAME, t.spcname as TABLESPACE_NAME,
			NULL as CLUSTER_NAME, NULL as IOT_NAME, 'VALID' as STATUS, NULL as PCT_FREE, NULL as PCT_USED, 
			NULL as INI_TRANS, NULL as MAX_TRANS, NULL as INITIAL_EXTENT, NULL as NEXT_EXTENT, NULL as MIN_EXTENTS, 
			NULL as MAX_EXTENTS, NULL as PCT_INCREASE, NULL as FREELISTS, NULL as FREELIST_GROUPS,
			case when c.relpersistence = 'p' then 'YES' else 'NO' end as LOGGING, NULL as BACKED_UP,
			c.reltuples as NUM_ROWS, c.relpages as BLOCKS,
			NULL as EMPTY_BLOCKS, NULL as AVG_SPACE, NULL as CHAIN_CNT, NULL as AVG_ROW_LEN, NULL as AVG_SPACE_FREELIST_BLOCKS, 
			NULL as NUM_FREELIST_BLOCKS, NULL as DEGREE, NULL as INSTANCES, NULL as CACHE, NULL as TABLE_LOCK, NULL as SAMPLE_SIZE,
			s.last_analyze as LAST_ANALYZED, 
			case when c.relkind = 'p' then 'YES' else 'NO' end as PARTITIONED, NULL as IOT_TYPE, 
			case when c.relpersistence in ('t', 'g') then 'Y' else 'N' end as TEMPORARY, NULL as SECONDARY, NULL as NESTED,
			NULL as BUFFER_POOL, NULL as FLASH_CACHE, NULL as CELL_FLASH_CACHE, NULL as ROW_MOVEMENT, NULL as GLOBAL_STATS,
			NULL as USER_STATS, 
			case when s.last_analyze is not null then systimestamp - s.last_analyze else NULL END  AS DURATION, 
			NULL as SKIP_CORRUPT, NULL as MONITORING, NULL as CLUSTER_OWNER,
			NULL as DEPENDENCIES, NULL as COMPRESSION, NULL as COMPRESS_FOR, NULL as DROPPED, NULL as READ_ONLY,
			NULL as SEGMENT_CREATED, NULL as RESULT_CACHE
			from pg_class c left join pg_namespace n on n.oid = c.relnamespace
							left join pg_tablespace t on t.oid = c.reltablespace
							left join pg_stat_all_tables s on s.relid = c.oid
			where (NOT pg_is_other_temp_schema(n.oid)) AND c.relkind IN ('r', 'f', 'p') 
				AND pg_get_userbyid(c.relowner) = current_user
		  );
GRANT SELECT ON user_tables TO PUBLIC;

create or replace package dbms_obfuscation_toolkit as
$p$
	FUNCTION md5(input_string IN varchar2) returns VARCHAR2(16);
    FUNCTION md5(input in raw) returns RAW(16);
$p$;

create or replace package body dbms_obfuscation_toolkit as
$p$
	FUNCTION md5(input_string IN varchar2) returns VARCHAR2(16) AS
	'MODULE_PATHNAME','dbms_obfuscation_toolkit_md5'
	LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

	FUNCTION md5(input in raw) returns RAW(16) AS
	'MODULE_PATHNAME','dbms_obfuscation_toolkit_md5_raw'
	LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
$p$;

create or replace package dbms_refresh as
$p$
	procedure refresh(name IN VARCHAR2);
$p$;

create or replace package body dbms_refresh as
$p$
	procedure refresh(name IN VARCHAR2) as 
	'MODULE_PATHNAME','dbms_refresh_refresh'
	LANGUAGE C;
$p$;

CREATE OR REPLACE FUNCTION opentenbase_ora.bfilename(text, text) RETURN bfile AS
$p$ SELECT opentenbase_ora.bfilename($1::cstring, $2::cstring); $p$
LANGUAGE SQL IMMUTABLE;
