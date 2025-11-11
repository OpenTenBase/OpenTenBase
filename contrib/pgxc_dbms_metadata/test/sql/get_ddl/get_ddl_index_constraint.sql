SELECT dbms_metadata.get_ddl('INDEX','perf_index','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('INDEX','Gin_Index_Name','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('CONSTRAINT','child_uniq','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('CONSTRAINT','Unique age','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('CHECK_CONSTRAINT','check','gdmmm') from dual;

SELECT dbms_metadata.get_ddl('REF_CONSTRAINT','Fk_customer','gdmmm') from dual;

-- tests for schema as NULL
SET search_path TO public, gdmmm;

SELECT dbms_metadata.get_ddl('INDEX','perf_index') from dual;

SELECT dbms_metadata.get_ddl('INDEX','Gin_Index_Name') from dual;

SELECT dbms_metadata.get_ddl('CONSTRAINT','child_uniq') from dual;

SELECT dbms_metadata.get_ddl('CONSTRAINT','Unique age') from dual;
