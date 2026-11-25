SELECT dbms_metadata.get_ddl('TYPE','Address','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('TYPE','location','gdmmm') from dual;

-- tests for schema as NULL
SET search_path TO public, gdmmm;

SELECT dbms_metadata.get_ddl('TYPE','Address') from dual;

SELECT dbms_metadata.get_ddl('TYPE','location') from dual;
