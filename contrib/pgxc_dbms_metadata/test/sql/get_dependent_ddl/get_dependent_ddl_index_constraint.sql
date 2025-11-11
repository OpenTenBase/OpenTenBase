SELECT dbms_metadata.get_dependent_ddl('INDEX','table_all','gdmmm') from dual;

SELECT dbms_metadata.get_dependent_ddl('INDEX','Sample unlogged table','gdmmm') from dual;

SELECT dbms_metadata.get_dependent_ddl('CONSTRAINT','table_all','gdmmm') from dual;

SELECT dbms_metadata.get_dependent_ddl('CONSTRAINT','table_all_child','gdmmm') from dual;

SELECT dbms_metadata.get_dependent_ddl('CONSTRAINT','Sample unlogged table','gdmmm') from dual;

SELECT dbms_metadata.get_dependent_ddl('REF_CONSTRAINT','table_all_child','gdmmm') from dual;

-- tests for schema as NULL
SET search_path TO public, gdmmm;

SELECT dbms_metadata.get_dependent_ddl('INDEX','table_all') from dual;

SELECT dbms_metadata.get_dependent_ddl('INDEX','Sample unlogged table') from dual;

SELECT dbms_metadata.get_dependent_ddl('CONSTRAINT','table_all') from dual;

SELECT dbms_metadata.get_dependent_ddl('CONSTRAINT','table_all_child') from dual;

SELECT dbms_metadata.get_dependent_ddl('CONSTRAINT','Sample unlogged table') from dual;
