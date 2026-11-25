SELECT dbms_metadata.get_ddl('VIEW','gg_d','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('VIEW','Global fines','gdmmm') from dual;

-- tests for schema as NULL
SET search_path TO public, gdmmm;

SELECT dbms_metadata.get_ddl('VIEW','gg_d') from dual;

SELECT dbms_metadata.get_ddl('VIEW','Global fines') from dual;
