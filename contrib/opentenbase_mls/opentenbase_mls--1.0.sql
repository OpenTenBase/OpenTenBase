/* contrib/pg_cls/pg_cls.sql */

-- complain if script is sourced in psql, 
\echo Use "CREATE EXTENSION PG_CLS '1.0'" to load this file. \quit

----------------------------datamask begin-------------------------
--create datamask, input args could indentify one unique datamask policy, input 'defaultval' default value for integer is 9999, means for string type is 4
CREATE OR REPLACE FUNCTION MLS_DATAMASK_CREATE(schema_name name, 
                                           table_name  name, 
                                           attr_name   name, 
                                           optionval   INTEGER, 
                                           defaultval  numeric DEFAULT 9999)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    attnum_v INTEGER;
    atttypid INTEGER;
    maskfunc INTEGER := 0;
    relid_check    INTEGER;
    attnum_check   INTEGER;
    datamask_check BIGINT;
    datamask BIGINT  := 0;
    support  boolean;
    relkind  char;
    idx      INTEGER;
    childname TEXT[];
    childoid  INTEGER[];
    DATAMASK_KIND_VALUE         INTEGER := 1;
    DATAMASK_KIND_STR_PREFIX    INTEGER := 2;
    DATAMASK_KIND_DEFAULT_VAL   INTEGER := 3;
    DATAMASK_KIND_STR_POSTFIX   INTEGER := 4;
    DEFAULT_STRING_MASK_LEN     INTEGER := 4;
    MAX_STRING_MASK_LEN         INTEGER := 32;
    TIMESTAMP_TYPEOID           INTEGER := 1114;
    TIMESTAMPTZ_TYPEOID         INTEGER := 1184;
    FirstNormalObjectId         INTEGER := 16384;
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;
    
    IF (optionval <> DATAMASK_KIND_VALUE AND optionval <> DATAMASK_KIND_STR_PREFIX AND optionval <> DATAMASK_KIND_STR_POSTFIX AND optionval <> DATAMASK_KIND_DEFAULT_VAL ) THEN
        RAISE EXCEPTION 'INVALID INPUT:optionval:%', optionval;
    END IF;
    --RAISE NOTICE 'optionval:%, DATAMASK_KIND_VALUE:%, DATAMASK_KIND_STR_PREFIX:%, DATAMASK_KIND_STR_POSTFIX:%, datamask:%', optionval,DATAMASK_KIND_VALUE,DATAMASK_KIND_STR_PREFIX, DATAMASK_KIND_STR_POSTFIX, datamask;
        
    sql_cmd := 'select attnum , atttypid, attrelid from pg_attribute a where a.attname = '''||attr_name||''' and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO attnum, atttypid, relid;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    IF relid < FirstNormalObjectId THEN 
        RAISE EXCEPTION 'system table:% is unsupported', table_name;
    END IF;
    
    sql_cmd:= 'select pg_transparent_crypt_support_datatype('||atttypid||')';
    EXECUTE sql_cmd INTO support;
    IF support = false THEN 
        RAISE EXCEPTION 'INVALID INPUT:unsupported datatype, table:%.%, attribute name:%', schema_name, table_name, attr_name;
    END IF;
    
    IF (TIMESTAMP_TYPEOID = atttypid OR TIMESTAMPTZ_TYPEOID = atttypid ) AND DATAMASK_KIND_DEFAULT_VAL = optionval AND 9999 = defaultval THEN 
        RAISE EXCEPTION 'INVALID INPUT:timestamp type needs explict input mask value, table:%.%, attribute name:%', schema_name, table_name, attr_name;
    END IF;

    datamask := defaultval::bigint;

    --special treat for string type(varchar\bpchar\varchar2\text), datamask records mask string length
    IF (optionval = DATAMASK_KIND_STR_PREFIX OR optionval = DATAMASK_KIND_STR_POSTFIX) THEN 
        IF datamask = 9999 THEN
            datamask := DEFAULT_STRING_MASK_LEN;
        ELSIF datamask <= 0 OR datamask > MAX_STRING_MASK_LEN THEN 
            RAISE EXCEPTION 'INVALID INPUT:mask length is % is out of range', datamask;
        END IF;
        --RAISE NOTICE 'string optionval:%, mask length is %', optionval, datamask;
    END IF;
    
    --condition view could not use column has datamask policy
    sql_cmd:= 'SELECT a.attnum '||
        'FROM pg_namespace nv, pg_class v, pg_depend dv, pg_depend dt, pg_class t, pg_namespace nt, pg_attribute a ' ||
        'WHERE nv.oid = v.relnamespace AND v.relkind in(''v''::"char", ''m''::"char") AND v.oid = dv.refobjid AND ' ||
        'dv.refclassid = ''pg_class''::regclass::oid AND dv.classid = ''pg_rewrite''::regclass::oid AND ' ||
        'dv.deptype = ''i''::"char" AND dv.objid = dt.objid AND dv.refobjid <> dt.refobjid AND dt.classid = ''pg_rewrite''::regclass::oid AND ' ||
        'dt.refclassid = ''pg_class''::regclass::oid AND dt.refobjid = t.oid AND t.relnamespace = nt.oid  '||
        'AND t.oid = a.attrelid AND dt.refobjsubid = a.attnum and t.relname = '''||table_name||''' and nv.nspname = '''||schema_name||''' and a.attname = '''||attr_name||''';';
    EXECUTE sql_cmd INTO attnum_v;
    IF attnum_v IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT: %.% attr_name:% has bound datamask policy', schema_name,table_name,attr_name;
    END IF;
  
    --check main table
    sql_cmd := 'select relid, attnum, datamask from pg_data_mask_map where relid = '||relid||' and attnum = '||attnum||';';
    EXECUTE sql_cmd INTO relid_check, attnum_check, datamask_check;
    IF relid_check IS NOT NULL THEN
            RAISE EXCEPTION 'table:%.% has attnum:% bound datamask policy, datamask:%', schema_name, table_name, attnum_check, datamask_check;
    END IF;
  
    --check if orignal partition table
    sql_cmd := 'select c.relkind, c.oid from pg_class c, pg_namespace n where c.relname = ''' 
            ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
    EXECUTE sql_cmd INTO relkind, relid;
  
     --if its children has bound policy, report error
    IF 'p' = relkind THEN 
        sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
        EXECUTE sql_cmd into childname, childoid;
        
        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            sql_cmd := 'select relid, attnum, datamask from pg_data_mask_map where relid = '||childoid[idx]||' and attnum = '||attnum||';';
            EXECUTE sql_cmd INTO relid_check, attnum_check, datamask_check;
            IF relid_check IS NOT NULL THEN
                    RAISE EXCEPTION 'table:%.% has attnum:% bound datamask policy, datamask:%', schema_name, childname[idx], attnum_check, datamask_check;
            END IF;
            
            idx := idx+1;
        END LOOP;
        --RAISE NOTICE 'is orignal partition table %', table_name;
    END IF;
  
    --pass all judgement, go
    sql_mod := 'insert into pg_data_mask_map(relid, attnum, enable, option, datamask, nspname, tblname, maskfunc, defaultval) values (' 
    ||' pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')' ||','||attnum||','''''||'t'||''''','||optionval||','
    ||datamask||','''''||schema_name||''''','''''||table_name||''''','||maskfunc 
    ||','''''||defaultval||'''''::text);'; 

    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --loop to bind children
    IF 'p' = relkind THEN 
        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            --pass all judgement, go
            sql_mod := 'insert into pg_data_mask_map(relid, attnum, enable, option, datamask, nspname, tblname, maskfunc, defaultval) values (' 
            ||' pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')' ||','||attnum||','''''||'t'||''''','||optionval||','
            ||datamask||','''''||schema_name||''''','''''||childname[idx]||''''','||maskfunc 
            ||','''''||defaultval||'''''::text);'; 
        
            sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
            --RAISE NOTICE '%', sql_cmd;
            EXECUTE sql_cmd;
            
            idx := idx+1;
        END LOOP;
    END IF;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--defaultval:timestamp
CREATE OR REPLACE FUNCTION MLS_DATAMASK_CREATE(schema_name name, 
                                           table_name  name, 
                                           attr_name   name, 
                                           optionval   INTEGER, 
                                           defaultval  timestamp)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    typname  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    attnum_v INTEGER;
    atttypid INTEGER;
    relid_check    INTEGER;
    attnum_check   INTEGER;
    datamask_check BIGINT;
    relkind  char;
    idx      INTEGER;
    childname TEXT[];
    childoid  INTEGER[];
    maskfunc INTEGER := 0;
    datamask BIGINT  := 0;
    support  boolean;
    tmpvalue timestamp;
    DATAMASK_KIND_DEFAULT_VAL   INTEGER := 3;
    FirstNormalObjectId         INTEGER := 16384;
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null, defaultval is null';
    END IF;
    
    IF optionval <> DATAMASK_KIND_DEFAULT_VAL THEN
        RAISE EXCEPTION 'INVALID INPUT:optionval:%', optionval;
    END IF;
    --RAISE NOTICE 'optionval:%, DATAMASK_KIND_VALUE:%, DATAMASK_KIND_STR_PREFIX:%, DATAMASK_KIND_STR_POSTFIX:%, datamask:%', optionval,DATAMASK_KIND_VALUE,DATAMASK_KIND_STR_PREFIX, DATAMASK_KIND_STR_POSTFIX, datamask;
        
    sql_cmd := 'select attnum , attrelid, atttypid from pg_attribute a where a.attname = '''||attr_name||''' and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO attnum, relid, atttypid;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    --system table can not bind
    IF relid < FirstNormalObjectId THEN 
        RAISE EXCEPTION 'system table:% is unsupported', table_name;
    END IF;
    
    sql_cmd:= 'select pg_transparent_crypt_support_datatype('||atttypid||')';
    EXECUTE sql_cmd INTO support;
    IF support = false THEN 
        RAISE EXCEPTION 'INVALID INPUT:unsupported datatype, table:%.%, attribute name:%', schema_name, table_name, attr_name;
    END IF;
    
    --condition view could not use column has datamask policy
    sql_cmd:= 'SELECT a.attnum '||
        'FROM pg_namespace nv, pg_class v, pg_depend dv, pg_depend dt, pg_class t, pg_namespace nt, pg_attribute a ' ||
        'WHERE nv.oid = v.relnamespace AND v.relkind in(''v''::"char", ''m''::"char")  AND v.oid = dv.refobjid AND ' ||
        'dv.refclassid = ''pg_class''::regclass::oid AND dv.classid = ''pg_rewrite''::regclass::oid AND ' ||
        'dv.deptype = ''i''::"char" AND dv.objid = dt.objid AND dv.refobjid <> dt.refobjid AND dt.classid = ''pg_rewrite''::regclass::oid AND ' ||
        'dt.refclassid = ''pg_class''::regclass::oid AND dt.refobjid = t.oid AND t.relnamespace = nt.oid  '||
        'AND t.oid = a.attrelid AND dt.refobjsubid = a.attnum and t.relname = '''||table_name||''' and nv.nspname = '''||schema_name||''' and a.attname = '''||attr_name||''';';
    EXECUTE sql_cmd INTO attnum_v;
    IF attnum_v IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT: %.% attr_name:% has bound datamask policy', schema_name,table_name,attr_name;
    END IF;
  
    --check input default value could be used as the type of column
    sql_cmd := 'select t.typname from pg_type t, pg_attribute a where a.attname = '''||attr_name
        ||''' and t.oid = a.atttypid and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name
        ||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO typname;
    IF typname IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    sql_cmd := 'SELECT '''||defaultval||'''::'||typname||';';
    EXECUTE sql_cmd INTO tmpvalue;   
  
    --check main table
    sql_cmd := 'select relid, attnum, datamask from pg_data_mask_map where relid = '||relid||' and attnum = '||attnum||';';
    EXECUTE sql_cmd INTO relid_check, attnum_check, datamask_check;
    IF relid_check IS NOT NULL THEN
            RAISE EXCEPTION 'table:%.% has attnum:% bound datamask policy, datamask:%', schema_name, table_name, attnum_check, datamask_check;
    END IF;
    
    --check if orignal partition table
    sql_cmd := 'select c.relkind, c.oid from pg_class c, pg_namespace n where c.relname = ''' 
            ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
    EXECUTE sql_cmd INTO relkind, relid;
  
    --if its children has bound policy, report error
    IF 'p' = relkind THEN 
        sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
        EXECUTE sql_cmd into childname, childoid;
        
        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            sql_cmd := 'select relid, attnum, datamask from pg_data_mask_map where relid = '||childoid[idx]||' and attnum = '||attnum||';';
            EXECUTE sql_cmd INTO relid_check, attnum_check, datamask_check;
            IF relid_check IS NOT NULL THEN
                RAISE EXCEPTION 'table:%.% has attnum:% bound datamask policy, datamask:%', schema_name, childname[idx], attnum_check, datamask_check;
            END IF;
            
            idx := idx+1;
        END LOOP;
        --RAISE NOTICE 'is orignal partition table %', table_name;
    END IF;
    
    --pass all judgement, go
    sql_mod := 'insert into pg_data_mask_map(relid, attnum, enable, option, datamask, nspname, tblname, maskfunc, defaultval) values (' 
        ||' pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')' ||','||attnum||','''''||'t'||''''','||optionval||','||datamask||','''''||schema_name||''''','''''||table_name||''''','||maskfunc 
        ||','''''||defaultval||'''''::text);'; 
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --loop to bind children
    IF 'p' = relkind THEN 
        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            --pass all judgement, go
            sql_mod := 'insert into pg_data_mask_map(relid, attnum, enable, option, datamask, nspname, tblname, maskfunc, defaultval) values (' 
                    ||' pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')' ||','||attnum||','''''||'t'||''''','||optionval||','||datamask||','''''
                    ||schema_name||''''','''''||childname[idx]||''''','||maskfunc ||','''''||defaultval||'''''::text);'; 
        
            sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
            --RAISE NOTICE '%', sql_cmd;
            EXECUTE sql_cmd;
            
            idx := idx+1;
        END LOOP;
    END IF;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--defaultval:text
CREATE OR REPLACE FUNCTION MLS_DATAMASK_CREATE(schema_name name, 
                                           table_name  name, 
                                           attr_name   name, 
                                           optionval   INTEGER, 
                                           defaultval  text )
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    typname  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    attnum_v INTEGER;
    atttypid INTEGER;
    relid_check    INTEGER;
    attnum_check   INTEGER;
    datamask_check BIGINT;
    relkind  char;
    idx      INTEGER;
    childname TEXT[];
    childoid  INTEGER[];
    maskfunc INTEGER := 0;
    datamask BIGINT  := 0;
    support  boolean;
    DATAMASK_KIND_DEFAULT_VAL   INTEGER := 3;
    FirstNormalObjectId         INTEGER := 16384;
BEGIN
    
    IF schema_name = '' OR defaultval = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null, defaultval is null';
    END IF;
    
    IF optionval <> DATAMASK_KIND_DEFAULT_VAL THEN
        RAISE EXCEPTION 'INVALID INPUT:optionval:%', optionval;
    END IF;
    --RAISE NOTICE 'optionval:%, DATAMASK_KIND_VALUE:%, DATAMASK_KIND_STR_PREFIX:%, DATAMASK_KIND_STR_POSTFIX:%, datamask:%', optionval,DATAMASK_KIND_VALUE,DATAMASK_KIND_STR_PREFIX, DATAMASK_KIND_STR_POSTFIX, datamask;
    
    sql_cmd := 'select attnum , attrelid, atttypid from pg_attribute a where a.attname = '''||attr_name||''' and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO attnum, relid, atttypid;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    --system table can not bind
    IF relid < FirstNormalObjectId THEN 
        RAISE EXCEPTION 'system table:% is unsupported', table_name;
    END IF;
    
    sql_cmd:= 'select pg_transparent_crypt_support_datatype('||atttypid||')';
    EXECUTE sql_cmd INTO support;
    IF support = false THEN 
        RAISE EXCEPTION 'INVALID INPUT:unsupported datatype, table:%.%, attribute name:%', schema_name, table_name, attr_name;
    END IF;
    
    --condition view could not use column has datamask policy
    sql_cmd:= 'SELECT a.attnum '||
        'FROM pg_namespace nv, pg_class v, pg_depend dv, pg_depend dt, pg_class t, pg_namespace nt, pg_attribute a ' ||
        'WHERE nv.oid = v.relnamespace AND v.relkind in(''v''::"char", ''m''::"char")  AND v.oid = dv.refobjid AND ' ||
        'dv.refclassid = ''pg_class''::regclass::oid AND dv.classid = ''pg_rewrite''::regclass::oid AND ' ||
        'dv.deptype = ''i''::"char" AND dv.objid = dt.objid AND dv.refobjid <> dt.refobjid AND dt.classid = ''pg_rewrite''::regclass::oid AND ' ||
        'dt.refclassid = ''pg_class''::regclass::oid AND dt.refobjid = t.oid AND t.relnamespace = nt.oid  '||
        'AND t.oid = a.attrelid AND dt.refobjsubid = a.attnum and t.relname = '''||table_name||''' and nv.nspname = '''||schema_name||''' and a.attname = '''||attr_name||''';';
    EXECUTE sql_cmd INTO attnum_v;
    IF attnum_v IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT: %.% attr_name:% has bound datamask policy', schema_name,table_name,attr_name;
    END IF;
  
    --check input default value could be used as the type of column
    sql_cmd := 'select t.typname from pg_type t, pg_attribute a where a.attname = '''||attr_name
        ||''' and t.oid = a.atttypid and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name
        ||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO typname;
    IF typname IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    sql_cmd := 'SELECT '''||defaultval||'''::'||typname||';';
    EXECUTE sql_cmd INTO typname; 
    
    --check main table
    sql_cmd := 'select relid, attnum, datamask from pg_data_mask_map where relid = '||relid||' and attnum = '||attnum||';';
    EXECUTE sql_cmd INTO relid_check, attnum_check, datamask_check;
    IF relid_check IS NOT NULL THEN
            RAISE EXCEPTION 'table:%.% has attnum:% bound datamask policy, datamask:%', schema_name, table_name, attnum_check, datamask_check;
    END IF;
    
    --check if orignal partition table
    sql_cmd := 'select c.relkind, c.oid from pg_class c, pg_namespace n where c.relname = ''' 
            ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
    EXECUTE sql_cmd INTO relkind, relid;
  
    --if its children has bound policy, report error
    IF 'p' = relkind THEN 
        sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
        EXECUTE sql_cmd into childname, childoid;
        
        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            sql_cmd := 'select relid, attnum, datamask from pg_data_mask_map where relid = '||childoid[idx]||' and attnum = '||attnum||';';
            EXECUTE sql_cmd INTO relid_check, attnum_check, datamask_check;
            IF relid_check IS NOT NULL THEN
                    RAISE EXCEPTION 'table:%.% has attnum:% bound datamask policy, datamask:%', schema_name, childname[idx], attnum_check, datamask_check;
            END IF;
            
            idx := idx+1;
        END LOOP;
        --RAISE NOTICE 'is orignal partition table %', table_name;
    END IF;
  
    --pass all judgement, go
    sql_mod := 'insert into pg_data_mask_map(relid, attnum, enable, option, datamask, nspname, tblname, maskfunc, defaultval) values (' 
        ||' pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')' ||','
        ||attnum||','''''||'t'||''''','||optionval||','||datamask||','''''||schema_name||''''','''''||table_name||''''','||maskfunc 
        ||','''''||defaultval||'''''::text);'; 
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --loop to bind children
    IF 'p' = relkind THEN 
        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            --pass all judgement, go
            sql_mod := 'insert into pg_data_mask_map(relid, attnum, enable, option, datamask, nspname, tblname, maskfunc, defaultval) values (' 
                    ||' pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')' ||','
                    ||attnum||','''''||'t'||''''','||optionval||','||datamask||','''''||schema_name||''''','''''||childname[idx]||''''','||maskfunc 
                    ||','''''||defaultval||'''''::text);'; 
        
            sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
            --RAISE NOTICE '%', sql_cmd;
            EXECUTE sql_cmd;
            idx := idx + 1;
        END LOOP;
    END IF;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--identify user in white list for one column
CREATE OR REPLACE FUNCTION MLS_DATAMASK_CREATE_USER_POLICY(schema_name name, 
                                           table_name  name, 
                                           attr_name   name, 
                                           user_name   name)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    idx      INTEGER;
    relkind  CHAR;
    childname TEXT[];
    childoid  INTEGER[];
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;

    sql_cmd := 'SELECT OID FROM pg_authid WHERE rolname = '''||user_name||''';';
    EXECUTE sql_cmd INTO userid;
    IF userid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:user_name:% not exists', user_name;
    END IF;
    --RAISE NOTICE 'userid:%', userid;
    
    sql_cmd := 'select attnum , attrelid from pg_attribute a where a.attname = '''||attr_name||''' and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO attnum, relid;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    sql_mod := 'insert into pg_data_mask_user(relid, userid, attnum, enable, username, nspname, tblname) values (' 
    ||' pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')' ||','||'pg_get_role_oid_by_name('''''||user_name||''''')'
    ||','||attnum||','''''||'t'||''''','''''||user_name||''''','''''||schema_name||''''','''''||table_name||''''');'; 

    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --check if orignal partition table
    sql_cmd := 'select c.relkind, c.oid from pg_class c, pg_namespace n where c.relname = ''' 
            ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
    EXECUTE sql_cmd INTO relkind, relid;
    
    --loop to bind children
    IF 'p' = relkind THEN 
        sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
        EXECUTE sql_cmd into childname, childoid;

        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            --pass all judgement, go
            sql_mod := 'insert into pg_data_mask_user(relid, userid, attnum, enable, username, nspname, tblname) values (' 
                    ||' pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')' ||','||'pg_get_role_oid_by_name('''''||user_name||''''')'
                    ||','||attnum||','''''||'t'||''''','''''||user_name||''''','''''||schema_name||''''','''''||childname[idx]||''''');'; 
        
            sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
            --RAISE NOTICE '%', sql_cmd;
            EXECUTE sql_cmd;
            idx := idx + 1;
        END LOOP;
    END IF;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--identify user in white list for all columns
CREATE OR REPLACE FUNCTION MLS_DATAMASK_CREATE_USER_POLICY(schema_name name, 
                                           table_name  name, 
                                           user_name   name)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    idx      INTEGER;
    relkind  CHAR;
    childname TEXT[];
    childoid  INTEGER[];
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;

    sql_cmd := 'SELECT OID FROM pg_authid WHERE rolname = '''||user_name||''';';
    EXECUTE sql_cmd INTO userid;
    IF userid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:user_name:% not exists', user_name;
    END IF;
    --RAISE NOTICE 'userid:%', userid;
    
    attnum:= -1;
    
    sql_mod := 'insert into pg_data_mask_user(relid, userid, attnum, enable, username, nspname, tblname) values (' 
    ||' pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')' ||','||'pg_get_role_oid_by_name('''''||user_name||''''')'
    ||','||attnum||','''''||'t'||''''','''''||user_name||''''','''''||schema_name||''''','''''||table_name||''''');'; 

    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --check if orignal partition table
    sql_cmd := 'select c.relkind, c.oid from pg_class c, pg_namespace n where c.relname = ''' 
            ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
    EXECUTE sql_cmd INTO relkind, relid;
    
    --loop to bind children
    IF 'p' = relkind THEN 
        sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
        EXECUTE sql_cmd into childname, childoid;

        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            --pass all judgement, go
            sql_mod := 'insert into pg_data_mask_user(relid, userid, attnum, enable, username, nspname, tblname) values (' 
                    ||' pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')' ||','||'pg_get_role_oid_by_name('''''||user_name||''''')'
                    ||','||attnum||','''''||'t'||''''','''''||user_name||''''','''''||schema_name||''''','''''||childname[idx]||''''');'; 
        
            sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
            --RAISE NOTICE '%', sql_cmd;
            EXECUTE sql_cmd;
            idx := idx + 1;
        END LOOP;
    END IF;   
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--drop user policy in white list on the indentified column
CREATE OR REPLACE FUNCTION MLS_DATAMASK_DROP_USER_POLICY(schema_name name, 
                                                               table_name  name, 
                                                               attr_name   name, 
                                                               user_name   name,
                                                               need_cascade boolean default false)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    relkind  CHAR;
    idx      INTEGER;
    childname TEXT[];
    childoid  INTEGER[];
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;

    sql_cmd := 'SELECT OID FROM pg_authid WHERE rolname = '''||user_name||''';';
    EXECUTE sql_cmd INTO userid;
    IF userid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:user_name:% not exists', user_name;
    END IF;
    --RAISE NOTICE 'userid:%', userid;
    
    sql_cmd := 'select attnum , attrelid from pg_attribute a where a.attname = '''||attr_name||''' and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO attnum, relid;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists, use MLS_DATAMASK_CLEAN instead', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    sql_mod := 'DELETE FROM pg_data_mask_user WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')'
            ||' AND userid = '||' pg_get_role_oid_by_name('''''||user_name||''''')' ||' AND attnum = '||attnum||';'; 
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    
    --check need cascade drop
    IF need_cascade = TRUE THEN
    
        --check if orignal partition table
        sql_cmd := 'select c.relkind, c.oid from pg_class c, pg_namespace n where c.relname = ''' 
                ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
        EXECUTE sql_cmd INTO relkind, relid;
        
        --loop to bind children
        IF 'p' = relkind THEN 
            sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
            EXECUTE sql_cmd into childname, childoid;

            idx := 1;
            WHILE idx <= array_length(childoid, 1) LOOP 
                --pass all judgement, go
                sql_mod := 'DELETE FROM pg_data_mask_user WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')'
                        ||' AND userid = '||' pg_get_role_oid_by_name('''''||user_name||''''')' ||' AND attnum = '||attnum||';'; 
            
                sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
                --RAISE NOTICE '%', sql_cmd;
                EXECUTE sql_cmd;
                idx := idx + 1;
            END LOOP;
        END IF;       
    END IF;       
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;


--drop user policy in white list on all columns
CREATE OR REPLACE FUNCTION MLS_DATAMASK_DROP_USER_POLICY(schema_name name, 
                                                               table_name  name, 
                                                               user_name   name,
                                                               need_cascade boolean default false)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    idx      INTEGER;
    relkind  CHAR;
    childname TEXT[];
    childoid  INTEGER[];
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;

    sql_cmd := 'SELECT OID FROM pg_authid WHERE rolname = '''||user_name||''';';
    EXECUTE sql_cmd INTO userid;
    IF userid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:user_name:% not exists', user_name;
    END IF;
    --RAISE NOTICE 'userid:%', userid;
        
    sql_mod := 'DELETE FROM pg_data_mask_user WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')'
            ||' AND userid = '||' pg_get_role_oid_by_name('''''||user_name||''''');'; 
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --check need cascade drop
    IF need_cascade = TRUE THEN
        --check if orignal partition table
        sql_cmd := 'select c.relkind, c.oid from pg_class c, pg_namespace n where c.relname = ''' 
                ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
        EXECUTE sql_cmd INTO relkind, relid;
        
        --loop to bind children
        IF 'p' = relkind THEN 
            sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
            EXECUTE sql_cmd into childname, childoid;

            idx := 1;
            WHILE idx <= array_length(childoid, 1) LOOP 
                --pass all judgement, go
                sql_mod := 'DELETE FROM pg_data_mask_user WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')'
                        ||' AND userid = '||' pg_get_role_oid_by_name('''''||user_name||''''');'; 
            
                sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
                --RAISE NOTICE '%', sql_cmd;
                EXECUTE sql_cmd;
                idx := idx + 1;
            END LOOP;
        END IF;       
    END IF;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--inner procedure of MLS_DATAMASK_CLEAN, distribute execute on node
CREATE OR REPLACE FUNCTION MLS_DATAMASK_CLEAN_INNER()
RETURNS VOID
AS $$
DECLARE 
    sql_cmd  TEXT;
    v_relid    INTEGER;
    v_userid   INTEGER;
    v_attnum   INTEGER;
    v_rec RECORD;
BEGIN
    --STEP 1 CLEAN DATAMASK_USER
    
    --1.1 clean datamask_user by userid
    FOR v_rec IN SELECT DISTINCT userid FROM pg_data_mask_user LOOP
        --RAISE NOTICE '%',v_rec.node_name;
        sql_cmd := 'SELECT OID FROM pg_authid WHERE OID = '||v_rec.userid||';';
        EXECUTE sql_cmd INTO v_userid;
        IF v_userid IS NULL THEN 
            sql_cmd := 'DELETE FROM pg_data_mask_user WHERE userid = '||v_rec.userid||';';
            EXECUTE sql_cmd;
        END IF;
    END LOOP;

    --1.2 clean datamask_map by relid
    FOR v_rec IN SELECT DISTINCT relid FROM pg_data_mask_user LOOP
        --RAISE NOTICE '%',v_rec.node_name;
        sql_cmd := 'SELECT OID FROM pg_class WHERE OID = '||v_rec.relid||';';
        EXECUTE sql_cmd INTO v_relid;
        IF v_relid IS NULL THEN 
            sql_cmd := 'DELETE FROM pg_data_mask_user WHERE relid = '||v_rec.relid||';';
            EXECUTE sql_cmd;
        END IF;
    END LOOP;
    
    --1.3 clean datamask_map by relid and attnum
    FOR v_rec IN SELECT relid, attnum FROM pg_data_mask_user LOOP
        --RAISE NOTICE '%',v_rec.node_name;
        sql_cmd := 'SELECT attnum FROM pg_attribute WHERE attrelid = '||v_rec.relid
                ||' AND attnum = '||v_rec.attnum||' AND attisdropped = ''f'';';
        EXECUTE sql_cmd INTO v_attnum;
        IF v_attnum IS NULL THEN 
            sql_cmd := 'DELETE FROM pg_data_mask_user WHERE relid = '||v_rec.relid||' AND attnum = '||v_rec.attnum||';';
            EXECUTE sql_cmd;
        END IF;
    END LOOP;
    
    --STEP2 CLEAN DATAMASK_MAP
    --2.1 clean datamask_map by relid
    FOR v_rec IN SELECT DISTINCT relid FROM pg_data_mask_map LOOP
        --RAISE NOTICE '%',v_rec.node_name;
        sql_cmd := 'SELECT OID FROM pg_class WHERE OID = '||v_rec.relid||';';
        EXECUTE sql_cmd INTO v_relid;
        IF v_relid IS NULL THEN 
            sql_cmd := 'DELETE FROM pg_data_mask_map WHERE relid = '||v_rec.relid||';';
            EXECUTE sql_cmd;
        END IF;
    END LOOP;
    
    --2.2 clean datamask_map by relid and attnum
    FOR v_rec IN SELECT relid, attnum FROM pg_data_mask_map LOOP
        --RAISE NOTICE '%',v_rec.node_name;
        sql_cmd := 'SELECT attnum FROM pg_attribute WHERE attrelid = '||v_rec.relid
                ||' AND attnum = '||v_rec.attnum||' AND attisdropped = ''f'';';
        EXECUTE sql_cmd INTO v_attnum;
        IF v_attnum IS NULL THEN 
            sql_cmd := 'DELETE FROM pg_data_mask_map WHERE relid = '||v_rec.relid||' AND attnum = '||v_rec.attnum||';';
            EXECUTE sql_cmd;
        END IF;
    END LOOP;
    
END;
$$ 
LANGUAGE default_plsql;

--clean invalid record in datamask_map and datamask_user
CREATE OR REPLACE FUNCTION MLS_DATAMASK_CLEAN()
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    v_rec RECORD;
BEGIN
    FOR v_rec IN SELECT node_name FROM pgxc_node WHERE node_type = 'C' OR node_type = 'D' LOOP
        RAISE NOTICE 'CLEAN INVALID MLS META DATA ON (%)', v_rec.node_name;
        sql_cmd := 'EXECUTE DIRECT ON ('||v_rec.node_name||') ''select MLS_DATAMASK_CLEAN_INNER()''';
        EXECUTE sql_cmd;
    END LOOP;

    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--drop mask policy on identified column
CREATE OR REPLACE FUNCTION MLS_DATAMASK_DROP_TABLE_POLICY(schema_name name, 
                                                               table_name  name, 
                                                               attr_name   name,
                                                               need_cascade boolean default false)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    relkind  CHAR;
    childname TEXT[];
    childoid  INTEGER[];
    idx       INTEGER;
    attnum   INTEGER;
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;
    
    sql_cmd := 'select attnum , attrelid from pg_attribute a where a.attname = '''||attr_name||''' and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO attnum, relid;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    --!! clear datamask policy directly, and clear dependence on datamask_user 
    --sql_cmd := 'select pg_relation_size('''||schema_name||'.'||table_name||''');';
    
    --STEP1: clear datamask map
    sql_mod := 'DELETE FROM pg_data_mask_map WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')'||' AND attnum = '||attnum||';'; 
    --RAISE NOTICE '%', sql_cmd;
  
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
  
    --STEP2: clear datamask user, ATTENTION, search by relid and attnum, so all white user would be delete.
    sql_mod := 'DELETE FROM pg_data_mask_user WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')'||' AND attnum = '||attnum||';'; 
    --RAISE NOTICE '%', sql_cmd;
  
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --check if orignal partition table
    sql_cmd := 'select c.relkind, c.oid from pg_class c, pg_namespace n where c.relname = ''' 
            ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
    EXECUTE sql_cmd INTO relkind, relid;
    
    IF need_cascade = true THEN 
        IF 'p' = relkind THEN 
            sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
            EXECUTE sql_cmd into childname, childoid;
            
            idx := 1;
            WHILE idx <= array_length(childoid, 1) LOOP 
            
                --clear data mask policy
                sql_mod := 'DELETE FROM pg_data_mask_map WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')'||' AND attnum = '||attnum||';'; 
                sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
                EXECUTE sql_cmd;
                
                --clear white user
                sql_mod := 'DELETE FROM pg_data_mask_user WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')'||' AND attnum = '||attnum||';'; 
                sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
                --RAISE NOTICE '%', sql_cmd;
                EXECUTE sql_cmd;

                idx := idx+1;
            END LOOP;
        END IF;
    END IF;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--drop mask policy of all column 
CREATE OR REPLACE FUNCTION MLS_DATAMASK_DROP_TABLE_POLICY(schema_name name, 
                                                               table_name  name,
                                                               need_cascade boolean default false)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relkind  CHAR;
    childname TEXT[];
    childoid  INTEGER[];
    idx       INTEGER;
    relid    INTEGER;
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;
    
    sql_cmd := 'select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
    EXECUTE sql_cmd INTO relid;
    IF relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% ', table_name, schema_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    --!! clear datamask policy directly, and clear dependence on datamask_user 
    --sql_cmd := 'select pg_relation_size('''||schema_name||'.'||table_name||''');';
  
    --step1: clear datamask map
    sql_mod := 'DELETE FROM pg_data_mask_map WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')'||';'; 
    --RAISE NOTICE '%', sql_cmd;
  
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
  
    --step2: clear datamask user
    sql_mod := 'DELETE FROM pg_data_mask_user WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')'||';'; 
    --RAISE NOTICE '%', sql_cmd;
  
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --check if orignal partition table
    sql_cmd := 'select c.relkind, c.oid from pg_class c, pg_namespace n where c.relname = ''' 
            ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ;';
    EXECUTE sql_cmd INTO relkind, relid;
    
    IF need_cascade = true THEN
        IF 'p' = relkind THEN 
            sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
            EXECUTE sql_cmd into childname, childoid;
            
            idx := 1;
            WHILE idx <= array_length(childoid, 1) LOOP 
            
                --clear data mask policy
                sql_mod := 'DELETE FROM pg_data_mask_map WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')'||';'; 
                sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
                EXECUTE sql_cmd;
                
                --clear white user
                sql_mod := 'DELETE FROM pg_data_mask_user WHERE relid = '||'pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''')'||';'; 
                sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
                --RAISE NOTICE '%', sql_cmd;
                EXECUTE sql_cmd;

                idx := idx+1;
            END LOOP;
        END IF;
    END IF;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--cover int2/int4/int8 and char/text with a string list 'xxxx'
CREATE OR REPLACE FUNCTION MLS_DATAMASK_UPDATE_TABLE_POLICY(schema_name name, 
                                                               table_name  name, 
                                                               attr_name   name, 
                                                               datamask    numeric default 9999)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    maskfunc INTEGER := 0;
    relkind  CHAR;
    idx      INTEGER;
    doption  INTEGER;
    childname TEXT[];
    tempval   TEXT;
    typname   TEXT;
    childoid  INTEGER[];
    DATAMASK_KIND_VALUE         INTEGER := 1;
    DATAMASK_KIND_STR_PREFIX    INTEGER := 2;
    DATAMASK_KIND_STR_POSTFIX   INTEGER := 4;
    DEFAULT_STRING_MASK_LEN     INTEGER := 4;
    DATAMASK_KIND_DEFAULT_VAL   INTEGER := 3;
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;
    
    --make sure attribute exists and having datamask policy bound, in case to create new datamask policy
    sql_cmd := 'select a.attnum , a.attrelid, d.option from pg_attribute a , pg_data_mask_map d where a.attname = '''||attr_name||''' and a.attrelid = '
                ||'(select c.oid from pg_class c, pg_namespace n where c.relname = '''
                    ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid )  '
                ||'and d.relid = a.attrelid and d.attnum = a.attnum;';
    EXECUTE sql_cmd INTO attnum, relid, doption;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    --check input default value could be used as the type of column
    sql_cmd := 'select t.typname from pg_type t, pg_attribute a where a.attname = '''||attr_name
        ||''' and t.oid = a.atttypid and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name
        ||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO typname;
    IF typname IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    sql_cmd := 'SELECT '''||datamask||'''::'||typname||';';
    EXECUTE sql_cmd INTO tempval;   
    
    IF doption = DATAMASK_KIND_DEFAULT_VAL THEN 
        --treat current table, maybe this is a parent, and its children would be treated later.
        sql_mod := 'update pg_data_mask_map set defaultval = '''''||datamask||'''''::text WHERE relid = '
                ||'pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''') '
                ||' AND attnum = '||attnum||';'; 
        --RAISE NOTICE '%', sql_cmd;
    ELSE 
        --treat current table, maybe this is a parent, and its children would be treated later.
        sql_mod := 'update pg_data_mask_map set datamask = '||datamask||' WHERE relid = '
                ||'pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''') '
                ||' AND attnum = '||attnum||';'; 
        --RAISE NOTICE '%', sql_cmd;
    END IF;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --get relkind
    sql_cmd := 'select relkind from pg_class where oid = '||relid||';';
    EXECUTE sql_cmd INTO relkind;
    
    --check its children
    IF 'p' = relkind THEN 
        sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
        EXECUTE sql_cmd into childname, childoid;
        
        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            --pass all judgement, go
            
            IF doption = DATAMASK_KIND_DEFAULT_VAL THEN 
                sql_mod := 'update pg_data_mask_map defaultval = '''''||datamask||'''''::text WHERE relid = '
                        ||'pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''') '
                        ||' AND attnum = '||attnum||';'; 
            ELSE 
                sql_mod := 'update pg_data_mask_map set datamask = '||datamask||' WHERE relid = '
                        ||'pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''') '
                        ||' AND attnum = '||attnum||';'; 
            END IF;
            
            sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
            --RAISE NOTICE '%', sql_cmd;
            EXECUTE sql_cmd;
            idx := idx + 1;
        END LOOP;
    END IF;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--cover text\timestamp, to update defaultval column
CREATE OR REPLACE FUNCTION MLS_DATAMASK_UPDATE_TABLE_POLICY(schema_name name, 
                                                               table_name  name, 
                                                               attr_name   name, 
                                                               defaultval  text)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    maskfunc INTEGER := 0;
    relkind  CHAR;
    idx      INTEGER;
    childname TEXT[];
    childoid  INTEGER[];
    tempval   TEXT;
    typname   TEXT;
    DATAMASK_KIND_VALUE         INTEGER := 1;
    DATAMASK_KIND_STR_PREFIX    INTEGER := 2;
    DATAMASK_KIND_STR_POSTFIX   INTEGER := 4;
    DEFAULT_STRING_MASK_LEN     INTEGER := 4;
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;
    
    --make sure attribute exists and having datamask policy bound, in case to create new datamask policy
    sql_cmd := 'select a.attnum , a.attrelid from pg_attribute a , pg_data_mask_map d where a.attname = '''||attr_name||''' and a.attrelid = '
                ||'(select c.oid from pg_class c, pg_namespace n where c.relname = '''
                    ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid )  '
                ||'and d.relid = a.attrelid and d.attnum = a.attnum;';
    EXECUTE sql_cmd INTO attnum, relid;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    
    --check input default value could be used as the type of column
    sql_cmd := 'select t.typname from pg_type t, pg_attribute a where a.attname = '''||attr_name
        ||''' and t.oid = a.atttypid and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name
        ||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO typname;
    IF typname IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    sql_cmd := 'SELECT '''||defaultval||'''::'||typname||';';
    EXECUTE sql_cmd INTO tempval;   
    
    --treat current table, maybe this is a parent, and its children would be treated later.
    sql_mod := 'update pg_data_mask_map set defaultval = '''''||defaultval||'''''::text WHERE relid = '
            ||'pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''') '
            ||' AND attnum = '||attnum||';'; 
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --get relkind
    sql_cmd := 'select relkind from pg_class where oid = '||relid||';';
    EXECUTE sql_cmd INTO relkind;
    
    --check its children
    IF 'p' = relkind THEN 
        sql_cmd := 'select * from MLS_GET_PARTITION_CHILDREN ('||relid||');';
        EXECUTE sql_cmd into childname, childoid;
        
        idx := 1;
        WHILE idx <= array_length(childoid, 1) LOOP 
            --pass all judgement, go
            sql_mod := 'update pg_data_mask_map set defaultval = '''''||defaultval||'''''::text WHERE relid = '
                        ||'pg_get_table_oid_by_name('''''||schema_name||'.'||childname[idx]||''''') '
                        ||' AND attnum = '||attnum||';'; 
            
            sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
            --RAISE NOTICE '%', sql_cmd;
            EXECUTE sql_cmd;
            idx := idx + 1;
        END LOOP;
    END IF;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--
CREATE OR REPLACE FUNCTION MLS_DATAMASK_ENABLE_USER_POLICY(schema_name name, 
                                                               table_name  name, 
                                                               attr_name   name, 
                                                               user_name   name)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    maskfunc INTEGER := 0;
    DATAMASK_KIND_VALUE         INTEGER := 1;
    DATAMASK_KIND_STR_PREFIX    INTEGER := 2;
    DATAMASK_KIND_STR_POSTFIX   INTEGER := 4;
    DEFAULT_STRING_MASK_LEN     INTEGER := 4;
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;
        
    sql_cmd := 'SELECT OID FROM pg_authid WHERE rolname = '''||user_name||''';';
    EXECUTE sql_cmd INTO userid;
    IF userid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:user_name:% not exists', user_name;
    END IF;
    --RAISE NOTICE 'userid:%', userid;
    
    sql_cmd := 'select attnum , attrelid from pg_attribute a where a.attname = '''||attr_name||''' and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO attnum, relid;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    sql_mod := 'update pg_data_mask_user set enable = true WHERE relid = '
            ||' pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')  '
            ||' AND userid = '||'pg_get_role_oid_by_name('''''||user_name||''''')'||' AND attnum = '||attnum||';'; 
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--
CREATE OR REPLACE FUNCTION MLS_DATAMASK_DISABLE_USER_POLICY(schema_name name, 
                                                               table_name  name, 
                                                               attr_name   name, 
                                                               user_name   name)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod  TEXT;
    sql_cmd  TEXT;
    relid    INTEGER;
    userid   INTEGER;
    attnum   INTEGER;
    maskfunc INTEGER := 0;
    DATAMASK_KIND_VALUE         INTEGER := 1;
    DATAMASK_KIND_STR_PREFIX    INTEGER := 2;
    DATAMASK_KIND_STR_POSTFIX   INTEGER := 4;
    DEFAULT_STRING_MASK_LEN     INTEGER := 4;
BEGIN
    
    IF schema_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:schema_name is null';
    END IF;
        
    sql_cmd := 'SELECT OID FROM pg_authid WHERE rolname = '''||user_name||''';';
    EXECUTE sql_cmd INTO userid;
    IF userid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:user_name:% not exists', user_name;
    END IF;
    --RAISE NOTICE 'userid:%', userid;
    
    sql_cmd := 'select attnum , attrelid from pg_attribute a where a.attname = '''
            ||attr_name||''' and a.attrelid = (select c.oid from pg_class c, pg_namespace n where c.relname = '''
            ||table_name||''' and n.nspname = '''||schema_name||''' and c.relnamespace = n.oid ) ;';
    EXECUTE sql_cmd INTO attnum, relid;
    IF attnum IS NULL OR relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table_name:% or schema_name:% or attr_name:% not exists', table_name, schema_name, attr_name;
    END IF;
    --RAISE NOTICE '%,%', attnum, relid;
    
    sql_mod := 'update pg_data_mask_user set enable = false WHERE relid = '
            ||' pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||''''')  AND userid = '
            || 'pg_get_role_oid_by_name('''''||user_name||''''')'||' AND attnum = '||attnum||';'; 
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--count invalid and total record 
CREATE OR REPLACE FUNCTION MLS_DATAMASK_INVALID_COUNT_INNER(OUT total_cnt_map INTEGER, OUT invalid_cnt_map INTEGER, OUT total_cnt_user INTEGER, OUT invalid_cnt_user INTEGER)
RETURNS record
AS $$
DECLARE 
    sql_cmd             TEXT;
    v_relid             INTEGER;
    v_userid            INTEGER;
    v_invalid_cnt_tmp   INTEGER;
    v_invalid_cnt       INTEGER;
    v_total_cnt         INTEGER;
    v_rec               RECORD;
    v_rec_2             RECORD;
BEGIN
    
    --STEP 1 count pg_data_mask_map
    v_invalid_cnt := 0;
    FOR v_rec IN SELECT DISTINCT relid FROM pg_data_mask_map LOOP
        --RAISE NOTICE '%',v_rec.node_name;
        sql_cmd := 'SELECT OID FROM pg_class WHERE OID = '||v_rec.relid||';';
        EXECUTE sql_cmd INTO v_relid;
        
        --1.1 count invalid record in pg_data_mask_map by relid
        IF v_relid IS NULL THEN 
            sql_cmd := 'SELECT COUNT(1) FROM pg_data_mask_map WHERE relid = '||v_rec.relid||';';
            EXECUTE sql_cmd INTO v_invalid_cnt_tmp;
            
            v_invalid_cnt := v_invalid_cnt + v_invalid_cnt_tmp;
            
            --RAISE NOTICE 'v_del_cnt %', v_del_cnt;
        ELSE
            --1.2 count invalid record in pg_data_mask_map by relid and attnum
            sql_cmd :=   'SELECT COUNT(1) '
                       ||'FROM pg_attribute a, pg_data_mask_map m '
                       ||'WHERE a.attrelid = m.relid AND a.attnum = m.attnum and attisdropped = ''t'' ;';
            EXECUTE sql_cmd INTO v_invalid_cnt_tmp; 
            
            v_invalid_cnt := v_invalid_cnt + v_invalid_cnt_tmp;
            
        END IF;
    END LOOP;
    
    --collect results
    invalid_cnt_map:= v_invalid_cnt;
    
    sql_cmd := 'SELECT COUNT(1) FROM pg_data_mask_map ';
    EXECUTE sql_cmd INTO v_total_cnt;
    total_cnt_map := v_total_cnt;
    
    
    --STEP 2 count pg_data_mask_user
    --2.1 count datamask_user by userid
    FOR v_rec IN SELECT DISTINCT userid FROM pg_data_mask_user LOOP
        --RAISE NOTICE '%',v_rec.node_name;
        sql_cmd := 'SELECT OID FROM pg_authid WHERE OID = '||v_rec.userid||';';
        EXECUTE sql_cmd INTO v_userid;
        
        IF v_userid IS NULL THEN 
            
            sql_cmd := 'SELECT COUNT(1) FROM pg_data_mask_user WHERE userid = '||v_rec.userid||';';
            EXECUTE sql_cmd into v_invalid_cnt_tmp;
            
            v_invalid_cnt := v_invalid_cnt + v_invalid_cnt_tmp;
            
        ELSE
            
            --2.2 userid is valid, check relid validition
            FOR v_rec_2 IN SELECT DISTINCT relid FROM pg_data_mask_user 
                           WHERE userid = v_userid
            LOOP
                sql_cmd := 'SELECT OID FROM pg_class WHERE OID = '||v_rec_2.relid||';';
                EXECUTE sql_cmd INTO v_relid;
                
                --2.2.1 count invalid record in pg_data_mask_user by relid and userid
                IF v_relid IS NULL THEN 
                    sql_cmd := 'SELECT COUNT(1) FROM pg_data_mask_user '
                                ||'WHERE relid = '||v_rec_2.relid||' AND userid = '||v_userid||';';
                    EXECUTE sql_cmd INTO v_invalid_cnt_tmp;
                    
                    v_invalid_cnt := v_invalid_cnt + v_invalid_cnt_tmp;
                    
                    --RAISE NOTICE 'v_del_cnt %', v_del_cnt;
                ELSE
                    --2.2.2 relid is valid, count invalid record in pg_data_mask_user by relid and attnum and userid
                    sql_cmd :=   'SELECT COUNT(1) '
                               ||'FROM pg_attribute a, pg_data_mask_user u '
                               ||'WHERE a.attrelid = u.relid AND a.attnum = u.attnum and a.attisdropped = ''t'' '
                               ||'AND u.userid = '||v_userid||' AND u.relid = '||v_relid||';';
                    EXECUTE sql_cmd INTO v_invalid_cnt_tmp; 
                    
                    v_invalid_cnt := v_invalid_cnt + v_invalid_cnt_tmp;
                    
                END IF;
        
            END LOOP;
        
        END IF;
    END LOOP;
    
    --collect results
    invalid_cnt_user := v_invalid_cnt;
    
    sql_cmd := 'SELECT COUNT(1) FROM pg_data_mask_user ';
    EXECUTE sql_cmd INTO v_total_cnt;
    total_cnt_user := v_total_cnt;
    
    return;
END;
$$ 
LANGUAGE default_plsql;

--can be called directly, show datamask meta data locally
CREATE OR REPLACE FUNCTION MLS_DATAMASK_SHOW_LOCAL()
RETURNS BOOLEAN
AS $$
DECLARE 
    sql_cmd             TEXT;
    v_relid             INTEGER;
    v_userid            INTEGER;
    v_rec               RECORD;
    v_rec_2             RECORD;
    v_rec_3             RECORD;
BEGIN
    
    --STEP 1 show pg_data_mask_map
    RAISE NOTICE '--------SHOW pg_data_mask_map--------';
    FOR v_rec IN SELECT DISTINCT relid FROM pg_data_mask_map LOOP
        --RAISE NOTICE '%',v_rec.node_name;
        sql_cmd := 'SELECT OID FROM pg_class WHERE OID = '||v_rec.relid||';';
        EXECUTE sql_cmd INTO v_relid;
        
        --1.1 count invalid record in pg_data_mask_map by relid
        IF v_relid IS NULL THEN 
            FOR v_rec_2 IN SELECT relid, attnum, enable, option, nspname, tblname  
                           FROM pg_data_mask_map WHERE relid = v_rec.relid 
                           ORDER BY attnum
            LOOP 
                RAISE NOTICE '  !!  INVALID RECORD: relid:%, attnum:%, enable:%, option:%, nspname:%, tblname:%',
                            v_rec_2.relid, v_rec_2.attnum, v_rec_2.enable, v_rec_2.option, v_rec_2.nspname, v_rec_2.tblname;
            END LOOP;
            --RAISE NOTICE 'v_del_cnt %', v_del_cnt;
        ELSE
            --1.2 show invalid record by relid and attnum
            FOR v_rec_2 IN SELECT m.relid, m.attnum, m.enable, m.option, m.nspname, m.tblname 
                           FROM pg_attribute a, pg_data_mask_map m 
                           WHERE a.attrelid = m.relid AND a.attnum = m.attnum and a.attisdropped = 't' and m.relid = v_relid 
                           ORDER BY attnum 
            LOOP
                RAISE NOTICE '  !!  INVALID RECORD: relid:%, attnum:%, enable:%, option:%, nspname:%, tblname:%',
                            v_rec_2.relid, v_rec_2.attnum, v_rec_2.enable, v_rec_2.option, v_rec_2.nspname, v_rec_2.tblname;
      
            END LOOP;
            
            --1.3 show valid record by relid and attnum
            FOR v_rec_2 IN SELECT m.relid, m.attnum, m.enable, m.option, m.nspname, m.tblname 
                           FROM pg_attribute a, pg_data_mask_map m 
                           WHERE a.attrelid = m.relid AND a.attnum = m.attnum and a.attisdropped = 'f' and m.relid = v_relid 
                           ORDER BY attnum 
            LOOP
                RAISE NOTICE '        VALID RECORD: relid:%, attnum:%, enable:%, option:%, nspname:%, tblname:%',
                            v_rec_2.relid, v_rec_2.attnum, v_rec_2.enable, v_rec_2.option, v_rec_2.nspname, v_rec_2.tblname;
            END LOOP;
        END IF;
    END LOOP;
        
    --STEP 2 show pg_data_mask_user
    RAISE NOTICE '--------SHOW pg_data_mask_user--------';
    --2.1 show datamask_user by userid
    FOR v_rec IN SELECT DISTINCT userid FROM pg_data_mask_user LOOP
        --RAISE NOTICE '%',v_rec.node_name;
        sql_cmd := 'SELECT OID FROM pg_authid WHERE OID = '||v_rec.userid||';';
        EXECUTE sql_cmd INTO v_userid;
        
        IF v_userid IS NULL THEN 
            
            FOR v_rec_2 IN SELECT relid, userid, attnum, enable, username, nspname, tblname  
                           FROM pg_data_mask_user WHERE userid = v_rec.userid 
                           ORDER BY relid, attnum
            LOOP 
                RAISE NOTICE '  !!  INVALID RECORD: relid:%, userid:%, attnum:%, enable:%, username:%, nspname:%, tblname:%',
                            v_rec_2.relid, v_rec_2.userid, v_rec_2.attnum, v_rec_2.enable, v_rec_2.username, v_rec_2.nspname, v_rec_2.tblname;
            END LOOP;
            
        ELSE
            
            --2.2 userid is valid, check relid validition
            FOR v_rec_2 IN SELECT DISTINCT relid FROM pg_data_mask_user 
                           WHERE userid = v_userid
            LOOP
                sql_cmd := 'SELECT OID FROM pg_class WHERE OID = '||v_rec_2.relid||';';
                EXECUTE sql_cmd INTO v_relid;
        
                IF v_relid IS NULL THEN 
                    --2.2.1
                    FOR v_rec_3 IN SELECT relid, userid, attnum, enable, username, nspname, tblname 
                                   FROM pg_data_mask_user WHERE relid = v_rec_2.relid 
                                   ORDER BY relid, attnum 
                    LOOP 
                        RAISE NOTICE '  !!  INVALID RECORD: relid:%, userid:%, attnum:%, enable:%, username:%, nspname:%, tblname:%',
                                    v_rec_3.relid, v_rec_3.userid, v_rec_3.attnum, v_rec_3.enable, v_rec_3.username, v_rec_3.nspname, v_rec_3.tblname;
                    END LOOP;
                    --RAISE NOTICE 'v_del_cnt %', v_del_cnt;
                ELSE
                    --2.2.2 show invalid record by relid and attnum
                    FOR v_rec_3 IN SELECT m.relid, m.userid, m.attnum, m.enable, m.username, m.nspname, m.tblname 
                                   FROM pg_attribute a, pg_data_mask_user m 
                                   WHERE a.attrelid = m.relid AND a.attnum = m.attnum and a.attisdropped = 't' and m.relid = v_relid 
                                   ORDER BY attnum 
                    LOOP
                        RAISE NOTICE '  !!  INVALID RECORD: relid:%, userid:%, attnum:%, enable:%, username:%, nspname:%, tblname:%',
                                    v_rec_3.relid, v_rec_3.userid, v_rec_3.attnum, v_rec_3.enable, v_rec_3.username, v_rec_3.nspname, v_rec_3.tblname;
                    END LOOP;
                    
                    --2.2.3 show valid record by relid and attnum
                    FOR v_rec_3 IN SELECT m.relid, m.userid, m.attnum, m.enable, m.username, m.nspname, m.tblname 
                                   FROM pg_attribute a, pg_data_mask_user m 
                                   WHERE a.attrelid = m.relid AND a.attnum = m.attnum and attisdropped = 'f' and m.relid = v_relid 
                                   ORDER BY attnum 
                    LOOP
                        RAISE NOTICE '        VALID RECORD: relid:%, userid:%, attnum:%, enable:%, username:%, nspname:%, tblname:%',
                                    v_rec_3.relid, v_rec_3.userid, v_rec_3.attnum, v_rec_3.enable, v_rec_3.username, v_rec_3.nspname, v_rec_3.tblname;
                    END LOOP;
                END IF;
            END LOOP;
        END IF;
    END LOOP;

    return true;
END;
$$ 
LANGUAGE default_plsql;


--show global datamask meta data, and check data consistency among nodes
CREATE OR REPLACE FUNCTION MLS_DATAMASK_SHOW()
RETURNS BOOLEAN
AS $$
DECLARE 
    sql_cmd                  TEXT;
    v_local_name             TEXT;
    v_invalid_local_cnt_map  INTEGER;
    v_total_local_cnt_map    INTEGER;
    v_invalid_local_cnt_user INTEGER;
    v_total_local_cnt_user   INTEGER;
    v_invalid_cnt_map        INTEGER;
    v_total_cnt_map          INTEGER;
    v_invalid_cnt_user       INTEGER;
    v_total_cnt_user         INTEGER;
    v_rec                    RECORD;
    v_ret_rec                RECORD;
BEGIN
    --show 
    sql_cmd := 'show pgxc_node_name';
    EXECUTE sql_cmd INTO v_local_name;
    RAISE NOTICE 'local node name:%', v_local_name;
    
    --show local infos
    PERFORM MLS_DATAMASK_SHOW_LOCAL();
    
    --count local invalid record
    sql_cmd := 'EXECUTE DIRECT ON ('||v_local_name||') ''select * from MLS_DATAMASK_INVALID_COUNT_INNER()''';
    EXECUTE sql_cmd INTO v_ret_rec;

    v_total_local_cnt_map    := v_ret_rec.total_cnt_map;
    v_invalid_local_cnt_map  := v_ret_rec.invalid_cnt_map;
    v_total_local_cnt_user   := v_ret_rec.total_cnt_user;
    v_invalid_local_cnt_user := v_ret_rec.invalid_cnt_user;

    --loop for other nodes
    RAISE NOTICE '--------COMPARE WITH OTHER NODES--------';
    FOR v_rec IN SELECT node_name FROM pgxc_node WHERE node_type = 'C' OR node_type = 'D' LOOP
        sql_cmd := 'EXECUTE DIRECT ON ('||v_rec.node_name||') ''select * from MLS_DATAMASK_INVALID_COUNT_INNER()''';
        EXECUTE sql_cmd INTO v_ret_rec;
        
        v_total_cnt_map    := v_ret_rec.total_cnt_map;
        v_invalid_cnt_map  := v_ret_rec.invalid_cnt_map;
        v_total_cnt_user   := v_ret_rec.total_cnt_user;
        v_invalid_cnt_user := v_ret_rec.invalid_cnt_user;
        
        IF v_invalid_cnt_map <> v_invalid_local_cnt_map THEN
            RAISE NOTICE 'There are % invalid record(s) in pg_data_mask_map,  different from local:%. remote nodename:%', 
                        v_invalid_cnt_map, v_invalid_local_cnt_map, v_rec.node_name;
        END IF;
        
        IF v_invalid_cnt_user <> v_invalid_local_cnt_user THEN
            RAISE NOTICE 'There are % invalid record(s) in pg_data_mask_user, different from local:%. remote nodename:%', 
                        v_invalid_cnt_user, v_invalid_local_cnt_user, v_rec.node_name;
        END IF;
        
        IF (v_total_cnt_map - v_invalid_cnt_map) <> (v_total_local_cnt_map - v_invalid_local_cnt_map) THEN
            RAISE NOTICE 'There are %   valid record(s) in pg_data_mask_map,  different from local:%. remote nodename:%', 
                        v_total_cnt_map - v_invalid_cnt_map, v_total_local_cnt_map - v_invalid_local_cnt_map, v_rec.node_name;
        END IF;
        
        IF (v_total_cnt_user - v_invalid_cnt_user) <> (v_total_local_cnt_user - v_invalid_local_cnt_user) THEN
            RAISE NOTICE 'There are %   valid record(s) in pg_data_mask_user,  different from local:%. remote nodename:%', 
                        v_total_cnt_user - v_invalid_cnt_user, v_total_local_cnt_user - v_invalid_local_cnt_user, v_rec.node_name;
        END IF;
        
    END LOOP;
    
    RETURN true;
END;
$$ 
LANGUAGE default_plsql;

----------------------------datamask end-------------------------
--get name and oid of orignal partition children
CREATE OR REPLACE FUNCTION MLS_GET_PARTITION_CHILDREN(IN relid INTEGER, OUT childname VARCHAR[], OUT childoid INTEGER[])
RETURNS record
AS $$
DECLARE 
    v_rec   RECORD;
    idx     INTEGER;
BEGIN
    
    childname = '{}';
    childoid = '{}';
    
    FOR v_rec IN select c.relname, c.oid from pg_class c where c.oid in (select inhrelid from pg_inherits h, pg_class cc where cc.oid = h.inhparent and cc.oid = relid) LOOP
        
        IF v_rec.relname IS NOT NULL THEN 
            childname := array_append(childname, v_rec.relname::varchar);
            childoid := array_append(childoid, v_rec.oid::integer);
            --RAISE NOTICE 'get child name%, oid:%', v_rec.relname, v_rec.oid;
        END IF;
    END LOOP;
    
    idx := 1;
    WHILE idx <= array_length(childoid, 1) LOOP 
        
        FOR v_rec IN select c.relname, c.oid from pg_class c where c.oid in (select inhrelid from pg_inherits h, pg_class cc where cc.oid = h.inhparent and cc.oid = childoid[idx]) LOOP
        
            IF v_rec.relname IS NOT NULL THEN 
                childname := array_append(childname, v_rec.relname::varchar);
                childoid := array_append(childoid, v_rec.oid::integer);
                --RAISE NOTICE 'get child name%, oid:%', v_rec.relname, v_rec.oid;
            END IF;
        
        END LOOP;
        
        idx:=idx+1;
    END LOOP;
    
    return;
END;
$$ 
LANGUAGE default_plsql;

----------------------------cls begin-------------------------
--level policy 
CREATE OR REPLACE FUNCTION MLS_CLS_CREATE_POLICY(policy_name name,
                                            policyid integer)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    polid        int2;
    MAX_POLICY_ID int2 := 100;
    MIN_POLICY_ID int2 := 1;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is null';
    END IF;
    
    IF policyid < MIN_POLICY_ID OR policyid > MAX_POLICY_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:label_id:% IS OUT OF RANGE', label_id;
    END IF;

    --check policyname and policyid
    sql_cmd := 'select polid from pg_cls_policy where polname = '''||policy_name||''' or polid = '||policyid||';';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:%, policyid:% already exists', policy_name, policyid;
    END IF;
    --RAISE NOTICE '%,%', polid;
    
    --ready, just do it
    sql_mod := 'insert into pg_cls_policy(polid, enable, polname) values (' 
                ||policyid||', true, '''''||policy_name||''''');';
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--level create
CREATE OR REPLACE FUNCTION MLS_CLS_CREATE_LEVEL(policy_name name,
                                            level_id integer,
                                            short_name name,
                                            long_name text)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    polid        integer;
    colon_1      TEXT;
    shortname    TEXT;
    MAX_LEVEL_ID int2 := 32767;
    MIN_LEVEL_ID int2 := 0;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is null';
    END IF;
    
    IF level_id < MIN_LEVEL_ID OR level_id > MAX_LEVEL_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:level_id:% IS OUT OF RANGE', level_id;
    END IF;
    
    colon_1 := substring(short_name from '[ |\t]');
    IF colon_1 IS NOT NULL THEN
        RAISE EXCEPTION 'INVALID INPUT:short_name:%, could not contain blank or tab ', short_name;
    END IF;
    
    --check policyname
    sql_cmd := 'select polid from pg_cls_policy where polname = '''||policy_name||''';';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% not exists', policy_name;
    END IF;
    --RAISE NOTICE '%,%', polid;
    
    --check if already exists
    sql_cmd := 'select shortname from pg_cls_level where shortname = '''||short_name||''';';
    EXECUTE sql_cmd INTO shortname;
    IF shortname IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:duplicate level_id:% or short_name:%', level_id, short_name;
    END IF;
    
    --ready, just do it
    sql_mod := 'insert into pg_cls_level(polid, levelid, shortname, longname) values (' 
                ||polid||','||level_id||','''''||short_name||''''','''''||long_name||''''');';
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--alter level long name
CREATE OR REPLACE FUNCTION MLS_CLS_ALTER_LEVEL_DESCRIPTION(policy_name name,
                                            level_id integer,
                                            long_name text)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    polid        int2;
    shortname    TEXT;
    MAX_LEVEL_ID int2 := 32767;
    MIN_LEVEL_ID int2 := 0;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is null';
    END IF;
    
    IF level_id < MIN_LEVEL_ID OR level_id > MAX_LEVEL_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:level_id:% IS OUT OF RANGE', level_id;
    END IF;
    
    IF long_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:long_name is NULL ';
    END IF;
    
    sql_cmd := 'select p.polid from pg_cls_policy p, pg_cls_level l where p.polname = '''
                ||policy_name||''' AND l.levelid = '||level_id||' AND p.polid = l.polid;';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% and levelid:% not exists', policy_name, level_id;
    END IF;
    --RAISE NOTICE '%,%', polid;
    
    --prepare
    sql_mod := 'update pg_cls_level set longname = '''''||long_name||''''' where polid = '||polid||' and levelid = '||level_id||');';
        
    --ready, just do it    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--drop level with level_id if not used
CREATE OR REPLACE FUNCTION MLS_CLS_DROP_LEVEL(policy_name name,
                                            level_id integer)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    polid        int2;
    shortname    TEXT;
    MAX_LEVEL_ID int2 := 32767;
    MIN_LEVEL_ID int2 := 0;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is null';
    END IF;
    
    IF level_id < MIN_LEVEL_ID OR level_id > MAX_LEVEL_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:level_id:% IS OUT OF RANGE', level_id;
    END IF;
    
    sql_cmd := 'select p.polid from pg_cls_policy p, pg_cls_level l where p.polname = '''
                ||policy_name||''' AND l.levelid = '||level_id||' AND p.polid = l.polid;';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% and levelid:% not exists', policy_name, level_id;
    END IF;
    --RAISE NOTICE '%,%', polid;
    
    --prepare
    sql_mod := 'delete from pg_cls_level where polid = '||polid||' and levelid = '||level_id||';';
    
    --ready, just do it    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--compartment create
CREATE OR REPLACE FUNCTION MLS_CLS_CREATE_COMPARTMENT(policy_name name,
                                            compartment_id integer,
                                            short_name name,
                                            long_name text)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    polid        integer;
    colon_1      TEXT;
    shortname    TEXT;
    MAX_COMPARTMENT_ID int2 := 32767;
    MIN_COMPARTMENT_ID int2 := 0;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is null';
    END IF;
    
    IF compartment_id < MIN_COMPARTMENT_ID OR compartment_id > MAX_COMPARTMENT_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:compartment_id:% IS OUT OF RANGE', compartment_id;
    END IF;
    
    colon_1 := substring(short_name from '[ |\t]');
    IF colon_1 IS NOT NULL THEN
        RAISE EXCEPTION 'INVALID INPUT:short_name:%, could not contain blank or tab ', short_name;
    END IF;
    
    --check policyname
    sql_cmd := 'select polid from pg_cls_policy where polname = '''||policy_name||''';';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% not exists', policy_name;
    END IF;
    --RAISE NOTICE '%,%', polid;
    
    --check if already exists
    sql_cmd := 'select shortname from pg_cls_compartment where shortname = '''||short_name||''';';
    EXECUTE sql_cmd INTO shortname;
    IF shortname IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:duplicate level_id:% or short_name:%', level_id, short_name;
    END IF;
    
    --ready, just do it
    sql_mod := 'insert into pg_cls_compartment(polid, compartmentid, shortname, longname) values (' 
                ||polid||','||compartment_id||','''''||short_name||''''','''''||long_name||''''');';
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--group create root node
CREATE OR REPLACE FUNCTION MLS_CLS_CREATE_GROUP_ROOT(policy_name name,
                                            root_id integer,
                                            short_name name,
                                            long_name text)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    polid        integer;
    colon_1      TEXT;
    shortname    TEXT;
    INVALID_GROUP_NODE int2 := -1;
    MAX_GROUP_ID int2 := 32767;
    MIN_GROUP_ID int2 := 0;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is null';
    END IF;
    
    colon_1 := substring(short_name from '[ |\t]');
    IF colon_1 IS NOT NULL THEN
        RAISE EXCEPTION 'INVALID INPUT:short_name:%, could not contain blank or tab ', short_name;
    END IF;
    
    IF root_id < MIN_GROUP_ID OR root_id > MAX_GROUP_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:root_id:% IS OUT OF RANGE', root_id;
    END IF;
            
    --check policyname
    sql_cmd := 'select polid from pg_cls_policy where polname = '''||policy_name||''';';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% not exists', policy_name;
    END IF;
    --RAISE NOTICE '%,%', polid;
    
    --check if this node already exists
    sql_cmd := 'select shortname from pg_cls_group where shortname = '''||short_name||''' OR groupid = '||root_id||';';
    EXECUTE sql_cmd INTO shortname;
    IF shortname IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:duplicate root_id:% or short_name:%', root_id, short_name;
    END IF;
    --check if root exists
    sql_cmd := 'select shortname from pg_cls_group where parentid = '||INVALID_GROUP_NODE||';';
    EXECUTE sql_cmd INTO shortname;
    IF shortname IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:root:% already exists', short_name;
    END IF;
        
    --ready, just do it
    sql_mod := 'insert into pg_cls_group(polid, groupid, parentid, shortname, longname) values (' 
                ||polid||','||root_id||', '||INVALID_GROUP_NODE||', '''''||short_name||''''','''''||long_name||''''');';
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--create node and bind with its parent
CREATE OR REPLACE FUNCTION MLS_CLS_CREATE_GROUP_NODE(policy_name name,
                                            child_id integer,
                                            short_name name,
                                            long_name text,
                                            parent_short_name name)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    polid        integer;
    parentid     integer;
    colon_1      TEXT;
    shortname    TEXT;
    INVALID_GROUP_NODE int2 := -1;
    MAX_GROUP_ID int2 := 32767;
    MIN_GROUP_ID int2 := 0;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is null';
    END IF;
    
    colon_1 := substring(short_name from '[ |\t]');
    IF colon_1 IS NOT NULL THEN
        RAISE EXCEPTION 'INVALID INPUT:short_name:%, could not contain blank or tab ', short_name;
    END IF;
    
    IF child_id < MIN_GROUP_ID OR child_id > MAX_GROUP_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:child_id:% IS OUT OF RANGE', child_id;
    END IF;
    
    IF parent_short_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:parent_short_name is null';
    END IF;
    
    --check parent_short_name
    colon_1 := substring(parent_short_name from '[ |\t]');
    IF colon_1 IS NOT NULL THEN
        RAISE EXCEPTION 'INVALID INPUT:parent_short_name:%, could not contain blank or tab ', parent_short_name;
    END IF;
        
    --check policyname
    sql_cmd := 'select polid from pg_cls_policy where polname = '''||policy_name||''';';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% not exists', policy_name;
    END IF;
    --RAISE NOTICE '%,%', polid;
    
    --check if already exists
    sql_cmd := 'select shortname from pg_cls_group where shortname = '''||short_name||''' OR groupid = '||child_id||';';
    EXECUTE sql_cmd INTO shortname;
    IF shortname IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:duplicate child_id:% or short_name:%', child_id, short_name;
    END IF;
    
    --check if parent exists
    sql_cmd := 'select groupid from pg_cls_group where shortname = '''||parent_short_name||''';';
    EXECUTE sql_cmd INTO parentid;
    IF parentid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:parent_short_name:% not exists', parent_short_name;
    END IF;
        
    --ready, just do it
    sql_mod := 'insert into pg_cls_group(polid, groupid, parentid, shortname, longname) values (' 
                ||polid||','||child_id||','||parentid||','''''||short_name||''''','''''||long_name||''''');';
    --RAISE NOTICE '%', sql_cmd;
    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--inner function 
CREATE OR REPLACE FUNCTION parse_label_str(IN polid integer, 
                                           IN label_str text, 
                                           OUT level_id integer, 
                                           OUT compt_id_arr integer [],
                                           OUT group_id_arr integer [])
RETURNS record 
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    level_str    TEXT;
    cmpt_str     TEXT;
    group_str    TEXT;
    colon_1      int4;
    colon_2      int4;
    label_len    int4;
    arr_len      int4;
    loop_idx     int4;
    shortname    TEXT;
    cmpt_str_arr TEXT[];
    group_str_arr TEXT[];
    groupid      int4;
    compartmentid int4;
    group_str_arr_element TEXT;
    cmpt_str_arr_element  TEXT;
BEGIN
    level_str := '';
    cmpt_str  := '';
    group_str := '';
    
    level_id := -1;
    compt_id_arr := '{}';
    group_id_arr := '{}';
    
    label_len := length(label_str);
    
    --pre check: the valid format must has 2 ':'
    colon_1 := instr(label_str, ':', 1, 1);
    IF 0 = colon_1 THEN
        RAISE EXCEPTION 'INVALID INPUT:label_str:%', label_str;
    END IF;
    
    colon_2 := instr(label_str, ':', 1, 2);
    IF 0 = colon_2 THEN
        RAISE EXCEPTION 'INVALID INPUT:label_str:%', label_str;
    END IF;
    
    --1. get level_id
    level_str := substr(label_str, 1, colon_1 - 1);
    sql_cmd:='select levelid from pg_cls_level where shortname = '''||level_str||''';';
    EXECUTE sql_cmd INTO level_id;
    IF level_id IS NULL THEN
        RAISE EXCEPTION 'INVALID INPUT: level_id:% does not exists',  level_str;
    END IF;
    --RAISE NOTICE 'level_id:%, level_str:%', level_id, level_str;
    
    --2. get compartment id
    cmpt_str := substr(label_str, colon_1 + 1, colon_2 - colon_1 - 1);    
    cmpt_str_arr := regexp_split_to_array(cmpt_str, ',');
    --RAISE NOTICE 'cmpt_str:%, cmpt_str_arr:%, colon_1:%, colon_2:%', cmpt_str, cmpt_str_arr, colon_1, colon_2;

    --2.1 loop check compartment shortname validity, and get correspond compartment id, put them into compt_id_arr
    arr_len := array_length(cmpt_str_arr, 1);
    loop_idx := 1;
    LOOP 
        IF loop_idx > arr_len THEN
            EXIT;
        END IF;
        --RAISE NOTICE 'loop_idx:%, cmpt_str_arr:%', loop_idx, cmpt_str_arr[loop_idx];
        
        cmpt_str_arr_element := replace(cmpt_str_arr[loop_idx], '[ |\t]', '');
        
        IF '' = cmpt_str_arr_element THEN 
            loop_idx := loop_idx + 1;
            CONTINUE;
        END IF;
        
        sql_cmd:='select compartmentid from pg_cls_compartment where shortname = '''||cmpt_str_arr[loop_idx]||''';';
        EXECUTE sql_cmd INTO compartmentid;
        IF compartmentid IS NULL THEN
            RAISE EXCEPTION 'INVALID INPUT: compartment:% does not exists',  cmpt_str_arr[loop_idx];
        END IF;
        
        compt_id_arr := array_append(compt_id_arr, compartmentid);
        --RAISE NOTICE 'current compt_id_arr:%', compt_id_arr;
        
        loop_idx := loop_idx + 1;
    END LOOP;
    
    --2.2 sort array    
    SELECT ARRAY(SELECT DISTINCT UNNEST(compt_id_arr::int[]) ORDER BY 1) into compt_id_arr;
        
    --3. get group id
    group_str := substr(label_str, colon_2 + 1, label_len - colon_2);    
    group_str_arr := regexp_split_to_array(group_str, ',');
    --RAISE NOTICE 'group_str:%, group_str_arr:%, label_len:%, colon_2:%', group_str, group_str_arr, label_len, colon_2;
    
    --3.1 loop check group shortname validity, and get correspond group id, put them into group_id_arr
    arr_len := array_length(group_str_arr, 1);
    loop_idx := 1;
    LOOP 
        IF loop_idx > arr_len THEN
            EXIT;
        END IF;
        --RAISE NOTICE 'loop_idx:%, group_str_arr:%', loop_idx, group_str_arr[loop_idx];
        
        group_str_arr_element := replace(group_str_arr[loop_idx], '[ |\t]', '');
        
        IF '' = group_str_arr_element THEN 
            loop_idx := loop_idx + 1;
            CONTINUE;
        END IF;
        
        sql_cmd:='select groupid from pg_cls_group where shortname = '''||group_str_arr_element||''';';
        EXECUTE sql_cmd INTO groupid;
        IF groupid IS NULL THEN
            RAISE EXCEPTION 'INVALID INPUT: group:% does not exists',  group_str_arr_element;
        END IF;
        
        group_id_arr := array_append(group_id_arr, groupid);
        --RAISE NOTICE 'current group_id_arr:%', group_id_arr;
        
        loop_idx := loop_idx + 1;
    END LOOP;

END;
$$
LANGUAGE default_plsql;

--create one label with 'label string', valid string format as follows:
--levelid:compartmentid1,compartmentid2,...compartmentidN:groupid1,groupid2,...groupidN
--levelid:compartmentid:groupid
--levelid:compartmentid:
--levelid::groupid
--levelid::
CREATE OR REPLACE FUNCTION MLS_CLS_CREATE_LABEL(policy_name name,
                                            label_id integer,
                                            label_str text)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    polid        int2;
    labelid      int2;
    label_len    int4;
    MAX_LABLE_ID int2 := 32767;
    MIN_LABLE_ID int2 := 0;
    MAX_DEFAULT_LABEL_STR_LEN int2 := 256;
    v_rec        record;
BEGIN
    --check args validation
    IF policy_name = '' OR label_str = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is NULL or label_str is NULL';
    END IF;
    
    IF label_id < MIN_LABLE_ID OR label_id > MAX_LABLE_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:label_id:% IS OUT OF RANGE', label_id;
    END IF;
    
    label_len := length(label_str);
    IF label_len > MAX_DEFAULT_LABEL_STR_LEN THEN
        RAISE NOTICE 'label_str length is%, attention!', label_len;
    END IF;
    --RAISE NOTICE 'label_len:%', label_len;
    
    --get polid
    sql_cmd := 'select polid from pg_cls_policy where polname = '''||policy_name||''' ;';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% dose not exists', policy_name;
    END IF;
    --RAISE NOTICE 'polid:%', polid;
    
    --get label_id
    sql_cmd := 'select l.labelid from pg_cls_policy p, pg_cls_label l where p.polname = '''
                ||policy_name||''' AND l.labelid = '||label_id||' AND p.polid = l.polid;';
    EXECUTE sql_cmd INTO labelid;
    IF labelid IS NOT NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% and labelid:% already exists', policy_name, label_id;
    END IF;
    --RAISE NOTICE 'labelid:%', labelid;
    
    --analyze label_str 
    select * into v_rec from parse_label_str(polid, label_str);
    --RAISE NOTICE 'v_rec:%, level_id:%, cmpt_id_arr:%, group_id_arr:%', v_rec, v_rec.level_id, v_rec.compt_id_arr, v_rec.group_id_arr;
    
    --prepare
    select format('insert into pg_cls_label(polid, labelid, levelid, compartmentid, groupid) '
                ' values( %s, %s, %s, ''''%s'''', ''''%s'''' );',
                polid, label_id, v_rec.level_id, v_rec.compt_id_arr, v_rec.group_id_arr) into sql_mod;
    --RAISE NOTICE '%', sql_mod;
    
    --all is ready, just do it    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--bind label to user, need def_read_label, def_write_label, def_row_label
CREATE OR REPLACE FUNCTION MLS_CLS_CREATE_USER_LABEL(policy_name name,
                                            username name, 
                                            def_read_label integer, 
                                            def_write_label integer, 
                                            def_row_label integer)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    userid       int4;
    polid        int2;
    labelid      int2;
    MAX_LABLE_ID int2 := 32767;
    MIN_LABLE_ID int2 := 0;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is NULL';
    END IF;
    
    IF def_read_label < MIN_LABLE_ID OR def_read_label > MAX_LABLE_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:def_read_label:% IS OUT OF RANGE', label_id;
    END IF;
    
    IF def_write_label < MIN_LABLE_ID OR def_write_label > MAX_LABLE_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:def_write_label:% IS OUT OF RANGE', label_id;
    END IF;
    
    IF def_row_label < MIN_LABLE_ID OR def_row_label > MAX_LABLE_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:def_row_label:% IS OUT OF RANGE', label_id;
    END IF;
        
    --get polid
    sql_cmd := 'select polid from pg_cls_policy where polname = '''||policy_name||''' ;';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% dose not exists', policy_name;
    END IF;
    --RAISE NOTICE 'polid:%', polid;
    
    --get userid
    sql_cmd := 'select oid from pg_authid where rolname = '''||username||''' ;';
    EXECUTE sql_cmd INTO userid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:user name:% dose not exists', username;
    END IF;
    --RAISE NOTICE 'polid:%', polid;
    
    --check def_read_label exists
    sql_cmd := 'select labelid from pg_cls_label  where labelid = '||def_read_label||' AND polid = '||polid||';';
    EXECUTE sql_cmd INTO labelid;
    IF labelid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% and labelid:% not exists', policy_name, def_read_label;
    END IF;

    --check def_write_label exists
    sql_cmd := 'select labelid from pg_cls_label  where labelid = '||def_write_label||' AND polid = '||polid||';';
    EXECUTE sql_cmd INTO labelid;
    IF labelid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% and labelid:% not exists', policy_name, def_write_label;
    END IF;
    
    --check def_row_label exists
    sql_cmd := 'select labelid from pg_cls_label  where labelid = '||def_row_label||' AND polid = '||polid||';';
    EXECUTE sql_cmd INTO labelid;
    IF labelid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% and labelid:% not exists', policy_name, def_row_label;
    END IF;
    
    --ready, go
    sql_mod := 'insert into pg_cls_user(polid, privilege, userid, max_read_label, max_write_label, min_write_label, default_read_label, '
                'default_write_label, default_row_label, username) values( '||polid||', 0, pg_get_role_oid_by_name('''''
                ||username||'''''), -1, -1, -1, '
                || def_read_label ||','|| def_write_label ||','||def_row_label||','''''||username||''''');';
 
    --all is ready, just do it    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    EXECUTE sql_cmd;
    
    --disconnect current connections
    sql_cmd := 'select pgxc_pool_disconnect('''', '''||username||''');';
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--unbind user from policy
CREATE OR REPLACE FUNCTION MLS_CLS_DROP_USER_LABEL(username name)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    userid       int4;
BEGIN
    --get userid
    sql_cmd := 'select oid from pg_authid where rolname = '''||username||''' ;';
    EXECUTE sql_cmd INTO userid;
    IF userid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:user name:% dose not exists', username;
    END IF;
    --RAISE NOTICE 'polid:%', polid;
        
    --ready, go
    sql_mod := 'delete from  pg_cls_user where userid = pg_get_role_oid_by_name('''''
                ||username||''''');';
                
    --all is ready, just do it    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    
    EXECUTE sql_cmd;
    
    --disconnect current connections
    sql_cmd := 'select pgxc_pool_disconnect('''', '''||username||''');';
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

--bind label to table row as default value
CREATE OR REPLACE FUNCTION MLS_CLS_CREATE_TABLE_LABEL(policy_name name,
                                            labelid integer, 
                                            schema_name name, 
                                            table_name name)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    relid        int4;
    attnum       int4;
    polid        int4;
    MAX_LABLE_ID int2 := 32767;
    MIN_LABLE_ID int2 := 0;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is NULL';
    END IF;
    
    IF labelid < MIN_LABLE_ID OR labelid > MAX_LABLE_ID THEN
        RAISE EXCEPTION 'INVALID INPUT:labelid:% IS OUT OF RANGE', label_id;
    END IF;
            
    --get polid
    sql_cmd := 'select polid from pg_cls_policy where polname = '''||policy_name||''' ;';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% dose not exists', policy_name;
    END IF;
    --RAISE NOTICE 'polid:%', polid;
    
    --get relid
    sql_cmd := 'select c.oid from pg_class c, pg_namespace n where c.relnamespace = n.oid and c.relname= '''
                ||table_name||''' and n.nspname = '''||schema_name||''';';
    EXECUTE sql_cmd INTO relid;
    IF relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table %.% dose not exists', schema_name,table_name;
    END IF;
    
    --check labelid exists
    sql_cmd := 'select labelid from pg_cls_label  where labelid = '||labelid||' AND polid = '||polid||';';
    EXECUTE sql_cmd INTO labelid;
    IF labelid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% and labelid:% not exists', policy_name, labelid;
    END IF;
        
    --check cls column exists
    sql_cmd := 'select attnum from pg_attribute where attrelid = '||relid||' AND attname = ''_cls'';';
    EXECUTE sql_cmd INTO attnum;
    IF attnum IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:cls column of table %.% not exists', schema_name,table_name;
    END IF;

    --ready, go
    sql_mod := 'insert into pg_cls_table(polid, attnum, relid, enable, nspname, tblname) values( '
                ||polid||', '||attnum||', pg_get_table_oid_by_name('''''||schema_name||'.'||table_name||'''''), true, '''''||schema_name||''''', '''''||table_name||''''');';
 
    --all is ready, just do it    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;


--unbind label from one table 
CREATE OR REPLACE FUNCTION MLS_CLS_DROP_TABLE_LABEL(policy_name name,
                                            schema_name name, 
                                            table_name name)
RETURNS BOOL
AS $$
DECLARE 
    sql_mod      TEXT;
    sql_cmd      TEXT;
    relid        int4;
    polid        int2;
BEGIN
    --check args validation
    IF policy_name = '' THEN
        RAISE EXCEPTION 'INVALID INPUT:policy_name is NULL';
    END IF;
            
    --get polid
    sql_cmd := 'select polid from pg_cls_policy where polname = '''||policy_name||''' ;';
    EXECUTE sql_cmd INTO polid;
    IF polid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:policy_name:% dose not exists', policy_name;
    END IF;
    --RAISE NOTICE 'polid:%', polid;
       
    --check cls table exists
    sql_cmd := 'select c.oid from pg_cls_table t, pg_cls_policy p, pg_namespace n, pg_class c, pg_cls_label l '
             ||'where c.oid = t.relid and c.relnamespace = n.oid and t.polid = p.polid and l.polid = p.polid '
             ||'and p.polname = '''||policy_name||''' '
             ||'and c.relname = '''||table_name||''' and n.nspname = '''||schema_name||''';';
    EXECUTE sql_cmd INTO relid;
    IF relid IS NULL THEN 
        RAISE EXCEPTION 'INVALID INPUT:table %.% does not have policy_name:% bound', 
                        schema_name, table_name, policy_name;
    END IF;
        
    --ready, go
    sql_mod := 'delete from pg_cls_table where polid = '||polid||' and relid = pg_get_table_oid_by_name('''''
                ||schema_name||'.'||table_name||''''');';
 
    --all is ready, just do it    
    sql_cmd := 'select pg_execute_query_on_all_nodes('''||sql_mod||''');';
    --RAISE NOTICE '%', sql_cmd;
    
    EXECUTE sql_cmd;
    
    RETURN TRUE;
END;
$$ 
LANGUAGE default_plsql;

----------------------------cls end-------------------------
