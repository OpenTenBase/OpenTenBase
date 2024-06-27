SELECT dbms_metadata.get_ddl('FUNCTION','Merge objects','gdmmm') from dual;

-- tests for schema as NULL
SET search_path TO public, gdmmm;

SELECT dbms_metadata.get_ddl('FUNCTION','Merge objects') from dual;