\c opentenbase_ora_package_function_regression_ora

SET client_min_messages = error;
create extension if not exists opentenbase_ora_package_function;
reset client_min_messages;

-- md5
select utl_raw.cast_to_raw(dbms_obfuscation_toolkit.md5(input_string => 168)) from dual;
select utl_raw.cast_to_raw(dbms_obfuscation_toolkit.md5(input_string => '中国')) from dual;
select utl_raw.cast_to_raw(dbms_obfuscation_toolkit.md5(input_string => '中*国')) from dual;
select utl_raw.cast_to_raw(dbms_obfuscation_toolkit.md5(input_string => '中*国')) from dual;
select utl_raw.cast_to_raw(dbms_obfuscation_toolkit.md5(input_string => 'abc')) from dual;
select dbms_obfuscation_toolkit.md5(input => utl_raw.cast_to_raw('abc')) from dual;
