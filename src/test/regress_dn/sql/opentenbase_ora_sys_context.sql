create user u_test_sys_context_20231017 with login superuser;
\c regression_ora u_test_sys_context_20231017

--
-- test sys_context
--
select sys_context('USERENV', 'CURRENT_USER');
--select sys_context('USERENV', 'CURRENT_USERID');
select sys_context('USERENV', 'DB_NAME');
select sys_context('USERENV', 'IP_ADDRESS');
select sys_context('USERENV', 'ISDBA');
--select sys_context('USERENV', 'HOST');
--select sys_context('USERENV', 'INSTANCE_NAME');
--select sys_context('USERENV', 'INSTANCE');
--select sys_context('USERENV', 'OPENTENBASE_ORA_HOME');
select sys_context('USERENV', 'CURRENT_SCHEMA');
select sys_context('USERENV', 'HAHAHA');
select sys_context('userenv', 'current_user', 1);
--select sys_context('USERENV'::text, 'current_USERID', 2);
select sys_context('userenv', 'DB_NaME', 3);
select sys_context('USERENV', 'IP_AddRESS', 0);
select sys_context('userenv'::varchar, 'IsdBA', 1000);
--select sys_context('USERENV', 'HOST', 4000);
--select sys_context('userenv'::varchar2, 'INSTANCE_NAME', 8000);
--select sys_context('USERENV', 'INstaNCE', -1);
--select sys_context('userenv', 'OPENTENBASE_ORA_HOME', -8);
select sys_context('USERENV', 'CURRENT_SCHEMA', -11111);
select sys_context('OPENTENBASE', 'CURRENT_USER');
select sys_context('OPENTENBASE', '');
select sys_context('', '');
select sys_context('OPENTENBASE'); --fail

--
-- test xs_sys_context
--
select xs_sys_context('XS$SESSION', 'CREATE_BY') from dual;
select xs_sys_context('XS$SESSION', 'CREATE_TIME') from dual;
select xs_sys_context('XS$SESSION', 'COOKIE') from dual;
select xs_sys_context('XS$SESSION', 'CURRENT_XS_USER') from dual;
--select xs_sys_context('XS$SESSION', 'CURRENT_XS_USER_GUID') from dual;
select xs_sys_context('XS$SESSION', 'INACTIVITY_TIMEOUT') from dual;
select xs_sys_context('XS$SESSION', 'LAST_ACCESS_TIME') from dual;
select xs_sys_context('XS$SESSION', 'LAST_AUTHENTICATION_TIME') from dual;
select xs_sys_context('XS$SESSION', 'LAST_UPDATED_BY') from dual;
--select xs_sys_context('XS$SESSION', 'PROXY_GUID') from dual;
--select xs_sys_context('XS$SESSION', 'SESSION_ID') from dual;
select xs_sys_context('XS$SESSION', 'SESSION_XS_USER') from dual;
--select xs_sys_context('XS$SESSION', 'SESSION_XS_USER_GUID') from dual;
select xs_sys_context('XS$SESSION', 'USERNAME') from dual;
--select xs_sys_context('XS$SESSION', 'USER_ID') from dual;

BEGIN;
CREATE SCHEMA SYS;
CREATE VIEW SYS.DUAL AS SELECT * FROM DUAL;
SELECT DECODE(USER, 'XS$NULL', XS_SYS_CONTEXT('XS$SESSION','USERNAME'), USER) FROM SYS.DUAL;
ROLLBACK;

SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') is not null FROM dual;
