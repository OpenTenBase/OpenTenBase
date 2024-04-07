SELECT dbms_metadata.get_ddl('TABLE','table_all','gdmmm') from dual;
SELECT dbms_metadata.set_transform_param('SQLTERMINATOR',false) from dual;
SELECT dbms_metadata.get_ddl('TABLE','table_all','gdmmm') from dual;
SELECT dbms_metadata.set_transform_param('DEFAULT') from dual;
SELECT dbms_metadata.get_ddl('TABLE','table_all','gdmmm') from dual;
