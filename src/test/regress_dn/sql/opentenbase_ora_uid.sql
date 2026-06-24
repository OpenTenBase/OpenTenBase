\c regression_ora
create table t_uid_001(uid int, "UID" varchar(20), "Uid" varchar(20));
create index idx_uid_11 on t_uid_001(uid);
create index idx_uid_12 on t_uid_001("UID");
create index idx_uid_13 on t_uid_001("Uid");
\d t_uid_001;

CREATE PROCEDURE insert_t_uid_001(uid int, "UID" varchar2)
LANGUAGE SQL
AS $$
	insert into t_uid_001 values(uid, "UID", "UID");
$$;

CREATE FUNCTION insert_t_uid_001_func(uid int, "UID" varchar2) return void
LANGUAGE SQL
AS $$
	insert into t_uid_001 values(uid, "UID", "UID");
$$;
call insert_t_uid_001(1, '1');
select insert_t_uid_001_func(2, '2');

select uid, "UID", "Uid" from t_uid_001 order by uid;
select uid, "UID", "Uid" from t_uid_001 where uid = 1 order by uid;
select uid, "UID", "Uid" from t_uid_001 where "UID" = '1' order by "UID";
select uid, "UID", "Uid" from t_uid_001 where "Uid" = '1' order by "Uid";

drop procedure insert_t_uid_001;
drop function insert_t_uid_001_func;
drop table t_uid_001;

-- test in opentenbase_ora mode 
\c regression_ora

create table t_uid_002("UID" varchar(20), "uid" varchar(20));
create index idx_uid_21 on t_uid_002("UID");
create index idx_uid_22 on t_uid_002("uid");
\d t_uid_002;

create or replace procedure insert_t_uid_002("UID" varchar2, "uid" varchar2)
AS
BEGIN
		insert into t_uid_002 values("UID", "uid");
END;
/
CREATE OR REPLACE FUNCTION insert_t_uid_002_func("UID" varchar2, "uid" varchar2)
RETURNS VOID AS $$
BEGIN
    INSERT INTO t_uid_002 VALUES ("UID", "uid");
END;
$$ LANGUAGE default_plsql;
create or replace procedure insert_t_uid_002_proc()
AS
DECLARE
	"UID" varchar2(256) := '2';
	"uid" varchar2(256) := '2';
BEGIN
	perform insert_t_uid_002_func("UID", "uid" => "uid");
END;
/
call insert_t_uid_002('1', '1');
call insert_t_uid_002_proc();

select "UID", "uid" from t_uid_002 order by "UID";
select "UID", "uid" from t_uid_002 where "UID" = '1' order by "UID";
select "UID", "uid" from t_uid_002 where "uid" = '1' order by "uid";

drop procedure insert_t_uid_002;
drop function insert_t_uid_002_func;
drop table t_uid_002;

-- other cases
CREATE user test_role1;
CREATE user "Test_role2";
CREATE user "TEST_ROLE3";
SET SESSION ROLE TEST_ROLE1;
SELECT USER, UID > 0 from dual;
SET SESSION ROLE "Test_role2";
SELECT USER, UID > 0 from dual;
SET SESSION ROLE "TEST_ROLE3";
SELECT USER, UID > 0 from dual;

-- switch to superuser
\c -
DROP ROLE TEST_ROLE1;
DROP ROLE "Test_role2";
DROP ROLE "TEST_ROLE3";
-- DONE --

-- TAPD: https://tapd.woa.com/20385652/bugtrace/bugs/view?bug_id=1020385652126736661
select coalesce(1.123,uid) from dual;
select +uid/2 from dual;
select +uid/2 * 3.5 - 7 from dual;
