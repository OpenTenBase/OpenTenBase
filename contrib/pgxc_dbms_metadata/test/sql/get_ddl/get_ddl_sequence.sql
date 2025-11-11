SELECT dbms_metadata.get_ddl('SEQUENCE','attach Line','gdmmm') from dual;

-- tests for schema as NULL
SET search_path TO public, gdmmm;

SELECT dbms_metadata.get_ddl('SEQUENCE','attach Line') from dual;
