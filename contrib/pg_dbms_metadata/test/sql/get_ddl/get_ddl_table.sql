SELECT dbms_metadata.get_ddl('TABLE','table_all','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('TABLE','table_all_child','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('TABLE','sales','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('TABLE','Sample unlogged table','gdmmm') from dual;

-- tests for schema as NULL
SET search_path TO public, gdmmm;

SELECT dbms_metadata.get_ddl('TABLE','table_all') from dual;

SELECT dbms_metadata.get_ddl('TABLE','table_all_child') from dual;

SELECT dbms_metadata.get_ddl('TABLE','sales') from dual;

SELECT dbms_metadata.get_ddl('TABLE','Sample unlogged table') from dual;
