SELECT dbms_metadata.get_dependent_ddl('SEQUENCE','table_all','gdmmm') from dual;

SELECT dbms_metadata.get_dependent_ddl('SEQUENCE','Sample unlogged table','gdmmm') from dual;

-- tests for schema as NULL
SET search_path TO public, gdmmm;

SELECT dbms_metadata.get_dependent_ddl('SEQUENCE','table_all') from dual;

SELECT dbms_metadata.get_dependent_ddl('SEQUENCE','Sample unlogged table') from dual;
