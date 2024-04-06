SELECT dbms_metadata.get_ddl('TABLE','table_all','gdmmm');
SELECT dbms_metadata.set_transform_param('SQLTERMINATOR',false);
SELECT dbms_metadata.get_ddl('TABLE','table_all','gdmmm');
SELECT dbms_metadata.set_transform_param('DEFAULT');
SELECT dbms_metadata.get_ddl('TABLE','table_all','gdmmm');