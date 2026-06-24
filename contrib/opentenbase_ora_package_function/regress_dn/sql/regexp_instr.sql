\c opentenbase_ora_package_function_regression_ora
--
-- test regexp_instr
--
-- good case
SELECT REGEXP_INSTR('foobarbaz', 'b..', 1, 2) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz', 'b..', 1, 2, 0) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz', 'b..', 1, 2, 1) FROM DUAL;
SELECT REGEXP_INSTR('500 OpenTenBase_Ora Parkway, Redwood Shores, CA', '[^ ]+', 1, 6) FROM DUAL;
SELECT REGEXP_INSTR('500 OpenTenBase_Ora Parkway, Redwood Shores, CA', '[s|r|p][[:alpha:]]{6}', 3, 2, 1, 'i') FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 1) FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 2) FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 4) FROM DUAL;
SELECT REGEXP_INSTR('中国人美国人', '[中|美]国人', 1, 2) FROM DUAL;
SELECT REGEXP_INSTR('', 'b..', 1, 2) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz', NULL, 1, 2) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz', '', 1, 2, 1) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz'::varchar, 'b..'::varchar, 1, 2, 1) FROM DUAL;
SELECT REGEXP_INSTR(NULL, NULL, NULL, NULL, NULL, NULL, NULL) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz', 'bbbbbbbbbbbbbbbb', 1, 2) FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 100) FROM DUAL;
-- bad case
SELECT REGEXP_INSTR('foobarbaz', 'b..', -1, 2) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz', 'b..', 1, -2) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz', 'b..', 1, 2, -1) FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'a', 4) FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', -1) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz', 'b..', 99999999999999999, 2) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz'::varchar2, 'b..'::varchar2, 1::bigint, 2::real, 1::double precision) FROM DUAL;
SELECT REGEXP_INSTR('foobarbaz', '', 1, 2, 99999999999999999) FROM DUAL;
select regexp_instr('中国人不打中国人', '中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人 中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人', 1, 2) from dual;
