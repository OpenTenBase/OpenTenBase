/*
 * Support synonym.
 */

/*
 * Test for synonym:
 * - synonym DDL
 *  - create(or replace)/drop publc synonym(PUBLIC with schema)
 *  - create/drop private synonym
 *  - alter synonym owner to
 * - synonym object replacement
 *  - table/view/MV/sequence in SELECT, UPDATE, DELETE, EXPLAIN, MERGE INTO
 *  - function/procedure/package
 *  - table name same as synonym
 *  - synonym cycled
 *  - dblink
 * - synonym supports DDL
 *  - AUDIT
 *  - GRANT/REVOKE/COMMENT ON
 */

\c regression_ora
-- - synonym DDL
--  - create(or replace)/drop publc synonym(PUBLIC with schema)
create public synonym s_x for x;
create public synonym s_x for x; -- dup error
select  synname, (select nspname from pg_namespace where oid = synnamespace), objnamespace, objname, objdblink from pg_synonym order by 1, 2;
create or replace public synonym s_x for re_x;
select  synname, (select nspname from pg_namespace where oid = synnamespace), objnamespace, objname, objdblink from pg_synonym order by 1, 2;
create or replace public synonym s_x for re_x @ dbk;
select  synname, (select nspname from pg_namespace where oid = synnamespace), objnamespace, objname, objdblink from pg_synonym order by 1, 2;

create synonym y_x for x;
select  synname, (select nspname from pg_namespace where oid = synnamespace), objnamespace, objname, objdblink from pg_synonym order by 1, 2;
drop public synonym s_x;
drop public synonym y_x;
select  synname, (select nspname from pg_namespace where oid = synnamespace), objnamespace, objname, objdblink from pg_synonym order by 1, 2;

--  - create/drop private synonym
create schema my_synonym_sch_20231030;
create public synonym my_synonym_sch_20231030.x for y; -- error
create synonym my_synonym_sch_20231030.x for y;
select  synname, (select nspname from pg_namespace where oid = synnamespace), objnamespace, objname, objdblink from pg_synonym order by 1, 2;
drop synonym my_synonym_sch_20231030.x;

--  - alter synonym owner to
drop user u1;
create user U1;
create user SUER with SUPERUSER;
\c - U1

create public synonym s_x_1 for x;
\c - SUER

create public synonym s_x for x;
\c - U1

drop public synonym s_x;
\c - SUER

select synname, (select rolname from pg_authid where oid = synowner) from pg_synonym order by 1;
alter public synonym s_x owner to U1;
select synname, (select rolname from pg_authid where oid = synowner) from pg_synonym order by 1;
drop synonym s_x;

create synonym my_synonym_sch_20231030.s_x for x;
select synname, (select rolname from pg_authid where oid = synowner) from pg_synonym order by 1;
alter public synonym my_synonym_sch_20231030.s_x owner to U1;
alter synonym my_synonym_sch_20231030.s_x owner to U1;
select synname, (select rolname from pg_authid where oid = synowner) from pg_synonym order by 1;
drop synonym my_synonym_sch_20231030.s_x;

-- - synonym object replacement
--  - table/view/MV/sequence in SELECT, UPDATE, DELETE, EXPLAIN, MERGE INTO
--  table
set search_path to my_synonym_sch_20231030,public;

create table g_tbl(x text, y text);
insert into g_tbl values('table', 'syn');
create public synonym p_s_gt for g_tbl;
create synonym my_synonym_sch_20231030.pv_s_gt for g_tbl;
-- select
select * from p_s_gt;
select * from my_synonym_sch_20231030.pv_s_gt;
-- update
update p_s_gt set x=x;
update my_synonym_sch_20231030.pv_s_gt set x=x;
-- delete
delete from p_s_gt;
delete from my_synonym_sch_20231030.pv_s_gt;
-- insert
insert into p_s_gt values('table', 'syn');
insert into my_synonym_sch_20231030.pv_s_gt values('table', 'syn');
-- explain
explain(costs off) select * from p_s_gt;
drop public synonym p_s_gt;

--  view
create view g_v as select * from g_tbl;
create synonym my_synonym_sch_20231030.s_g_v for g_v;
select * from my_synonym_sch_20231030.s_g_v;
--  view ddl uses synonym
create view g_v_s as select * from my_synonym_sch_20231030.pv_s_gt;
select * from g_v_s;
drop synonym my_synonym_sch_20231030.s_g_v;

--  meterialize view
create materialized view mv as select * from g_tbl;
create synonym my_synonym_sch_20231030.s_vm for mv;
select * from my_synonym_sch_20231030.s_vm;
drop synonym my_synonym_sch_20231030.s_vm;
drop materialized view mv;

create materialized view mv as select * from my_synonym_sch_20231030.pv_s_gt;
select * from mv;
drop materialized view mv;

-- sequence
create sequence g_seq;
create synonym my_synonym_sch_20231030.s_g_seq for g_seq;
select * from my_synonym_sch_20231030.s_g_seq;
create table p(p1 int, p2 int default my_synonym_sch_20231030.s_g_seq.nextval);
drop table p;
drop synonym my_synonym_sch_20231030.s_g_seq;
drop sequence g_seq;

-- merge into
create table test_d (a int, b int);
create table test_s (a int, b int);
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
create synonym s_d for test_d;
create synonym s_s for test_s;
merge into s_d using s_s on(s_d.a=s_s.a) when matched then update set s_d.b=s_s.b;
drop table test_d;
drop table test_s;
-- also test public synonym does not exist, but private synonym present
drop public synonym s_d;
drop public synonym s_s;
drop synonym my_synonym_sch_20231030.s_d;
drop synonym my_synonym_sch_20231030.s_s;

--  - function/procedure/package
-- function
create function raise_test1(int) returns int as $$
begin
    raise notice 'This message has too many parameters %', $1;
    return $1;
end;
$$ language default_plsql;
create synonym my_synonym_sch_20231030.s_raise_test1 for raise_test1;
select my_synonym_sch_20231030.s_raise_test1(1);

-- procedure
CREATE OR REPLACE PROCEDURE opentenbase_ora_proc_1() as
    i int:= 10;
begin
    raise notice 'last value: %',i;
end;
/
create synonym my_synonym_sch_20231030.s_opentenbase_ora_proc_1 for opentenbase_ora_proc_1;
call my_synonym_sch_20231030.s_opentenbase_ora_proc_1();
drop synonym my_synonym_sch_20231030.s_opentenbase_ora_proc_1;

-- package
CREATE PACKAGE mpkg as
$p$
    FUNCTION hobbies(int) RETURNS int;
    FUNCTION hobby_construct4other(text, text) RETURNS int;
$p$;

CREATE PACKAGE BODY mpkg as
$p$
    FUNCTION hobbies(int)
        RETURNS int
        AS '
		begin
			return 1;
		end;'
        LANGUAGE default_plsql;

    FUNCTION hobby_construct4other(text, text)
        RETURNS int
        AS '
		begin
			return 1;
		end;'
        LANGUAGE default_plsql;
$p$;

SELECT mpkg.hobby_construct4other('n1', 'l2');

create synonym my_synonym_sch_20231030.s_mpkg for mpkg;
SELECT s_mpkg.hobby_construct4other('n1', 'l2');
drop synonym my_synonym_sch_20231030.s_mpkg;

--  - table name same as synonym
create table n_1(a text);
insert into n_1 values('local table');
create synonym my_synonym_sch_20231030.n_1 for bad_obj;
create public synonym n_1 for bad_obj;
select * from n_1; -- directly use local table.
drop synonym my_synonym_sch_20231030.n_1;
drop public synonym n_1;

--  - synonym cycled
create synonym g_x for g_y;
create synonym g_y for g_x;
select * from g_x;
drop synonym g_x;
drop synonym g_y;

create table g_dbt(x int);
insert into g_dbt values(1);
create synonym my_synonym_sch_20231030.x1 for my_synonym_sch_20231030.x2;
create synonym my_synonym_sch_20231030.x2 for my_synonym_sch_20231030.x3;
create synonym my_synonym_sch_20231030.x3 for g_dbt;
select * from my_synonym_sch_20231030.x1;

-- - synonym supports DDL
--  - AUDIT
\c - audit_admin
set search_path to my_synonym_sch_20231030,public;

audit create synonym;
audit drop synonym;
audit alter synonym;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;

\c - SUER
set search_path to my_synonym_sch_20231030,public;

--  - GRANT/REVOKE/COMMENT ON
create table g_ctbl(x1 int);
create view v_g_ctbl as select * from g_ctbl;
-- COMMENT ON
create synonym my_synonym_sch_20231030.s_vg_ctbl for v_g_ctbl;
create synonym my_synonym_sch_20231030.s_g_ctbl for g_ctbl;
comment on table my_synonym_sch_20231030.s_g_ctbl is 'comment test'; -- table
comment on view my_synonym_sch_20231030.s_vg_ctbl is 'comment test'; -- view
comment on function my_synonym_sch_20231030.s_raise_test1 is 'comment test'; --function
-- GRANT/REVOKE
grant all on table my_synonym_sch_20231030.s_g_ctbl to U1;
grant all on my_synonym_sch_20231030.s_vg_ctbl to U1;
grant all on function my_synonym_sch_20231030.s_raise_test1 to U1;

revoke all on table my_synonym_sch_20231030.s_g_ctbl from  U1;
revoke all on my_synonym_sch_20231030.s_vg_ctbl from U1;
revoke all on function my_synonym_sch_20231030.s_raise_test1 from U1;
drop table g_ctbl cascade;

--
-- USER BUGS HERE !!
--
-- PARTITION(table)
reset search_path;
create table lp (a char) partition by list (a);
create table lp_default partition of lp default;
create table lp_ef partition of lp for values in ('e', 'f');
create table lp_null partition of lp for values in (null);

-- for main partition table
create public synonym ps_lp for lp;
select * from ps_lp;
select * from ps_lp partition(lp_ef);

create synonym my_synonym_sch_20231030.vs_lp for lp;
select * from my_synonym_sch_20231030.vs_lp;
select * from my_synonym_sch_20231030.vs_lp partition(lp_ef);

-- for child table
create public synonym ps_cld for lp_ef;
select * from my_synonym_sch_20231030.vs_lp partition(ps_cld);
select * from ps_lp partition(ps_cld);

drop table lp;

-- PARTITION FOR(expr)
create table syn_tbl(f1 bigint,f2 timestamp default now(), f3 integer) partition by range (f3) begin (1) step (50) partitions (2) distribute by shard(f1) to group default_group;
ALTER TABLE syn_tbl ADD PARTITIONS 2;
insert into syn_tbl select generate_series(1,10), null, 10;
create synonym my_synonym_sch_20231030.ia for syn_tbl;
select count(*) from my_synonym_sch_20231030.ia partition for(20);
drop table syn_tbl;

-- user case
create schema zjcoms;
create schema ccmp;
CREATE TABLE zjcoms.ch_bl_rwpayabledetail (
    region numeric(5,0) NOT NULL,
    billcycle numeric(8,0) NOT NULL,
    oldbillcycle numeric(8,0) NOT NULL,
    waytype character varying(4),
    wayid character varying(18) NOT NULL,
    wayname character varying(256),
    comlcls character varying(32),
    comlclsname character varying(64),
    comscls character varying(32),
    comsclsname character varying(64),
    opnid character varying(18) NOT NULL,
    opname character varying(64),
    datasource numeric(2,0),
    servnumber character varying(20),
    imei character varying(15),
    periods numeric(2,0),
    rewardval numeric(16,0),
    bcoefficient numeric(6,4),
    mcoefficient numeric(6,4),
    finalrewardval numeric(16,0)
)
PARTITION BY LIST (billcycle)
DISTRIBUTE BY SHARD (region) to GROUP default_group;
CREATE TABLE zjcoms.ch_bl_rwpayabledetail_p202004 PARTITION OF zjcoms.ch_bl_rwpayabledetail FOR VALUES in ('202004') as p202004;
insert into zjcoms.ch_bl_rwpayabledetail(region, billcycle, oldbillcycle, wayid, opnid) values(759, 202004, 202004, 'QFYJBAD666', 'SH1140001');

create synonym ccmp.zj_ch_bl_rwpayabledetail for zjcoms.ch_bl_rwpayabledetail;
select * from ccmp.ZJ_CH_BL_RWPAYABLEDETAIL PARTITION (P202004) limit 10;
drop table zjcoms.ch_bl_rwpayabledetail;
--

drop synonym my_synonym_sch_20231030.s_g_ctbl;
drop synonym my_synonym_sch_20231030.s_vg_ctbl;

-- bugs of 1020421696871065893
create or replace procedure ppp() is
begin
  null;
end;
/
grant EXECUTE ON PROCEDURE ppp TO SUER;
drop procedure ppp;

reset search_path;
/* -- End Story ID882003173 -- */

/* -- Begin Story ID115365659 -- */
CREATE SCHEMA SMS_ID882003173;
CREATE SCHEMA PENSIONMAIN_ZG_ID115365659;
set search_path to PENSIONMAIN_ZG_ID115365659,SMS_ID882003173,"$user", public;

CREATE OR REPLACE SYNONYM "SMS_ID882003173"."PKG_TMS_PUB_ID115365659" FOR "PENSIONMAIN"."PKG_TMS_PUB_ID115365659";
CREATE OR REPLACE SYNONYM "SMS_ID882003173"."TMS_PRE_PREM_ARRIVE" FOR "PENSIONMAIN"."TMS_PRE_PREM_ARRIVE";
CREATE TABLE "PENSIONMAIN_ZG_ID115365659"."TMS_PRE_PERM_ARRIVE" (
	"PRE_PREM_ID" NUMERIC(10,0) NOT NULL,
	"PRODUCT_ID" NUMERIC(10,0) NOT NULL,
	"ARRIVE_DATE" DATE NOT NULL
);
insert into "PENSIONMAIN_ZG_ID115365659"."TMS_PRE_PERM_ARRIVE" values (1, 1, systimestamp);

create or replace package "PENSIONMAIN_ZG_ID115365659"."PKG_TMS_PUB_ID115365659" is
procedure proc_test;
end;
/
create or replace package body "PENSIONMAIN_ZG_ID115365659"."PKG_TMS_PUB_ID115365659" is
procedure proc_test is
v int;
begin 
	select PRE_PREM_ID into v from "PENSIONMAIN_ZG_ID115365659"."TMS_PRE_PERM_ARRIVE" tpp where tpp.product_id = 1 limit 1;
	raise notice '--> %', v;
end;
end;
/
call PKG_TMS_PUB_ID115365659.proc_test();
call PKG_TMS_PUB_ID115365659.proc_test();
call PKG_TMS_PUB_ID115365659.proc_test();
call PENSIONMAIN_ZG_ID115365659.PKG_TMS_PUB_ID115365659.proc_test();
call "PENSIONMAIN_ZG_ID115365659"."PKG_TMS_PUB_ID115365659".proc_test();

-- cleanup
drop package "PENSIONMAIN_ZG_ID115365659"."PKG_TMS_PUB_ID115365659";
drop table "PENSIONMAIN_ZG_ID115365659"."TMS_PRE_PERM_ARRIVE";
drop synonym "SMS_ID882003173"."TMS_PRE_PREM_ARRIVE";
drop synonym "SMS_ID882003173"."PKG_TMS_PUB_ID115365659";

/* -- End Story ID115365659 -- */

-- TAPD: --bug=118596113
create user u_synonym_20231222 with superuser login;
SET SESSION AUTHORIZATION u_synonym_20231222;
set search_path=pkg_synonym_20231222,'$user','public';
CREATE schema u_synonym_20231222;
create or replace synonym u_synonym_20231222.p for pkg_synonym_20231222_not_exist.p;
CREATE OR REPLACE PACKAGE public.pkg_synonym_20231222 as
    procedure p(v1 varchar2, v2 varchar2, v3 varchar2, v4 varchar2);
END pkg_synonym_20231222;
/
CREATE OR REPLACE PACKAGE BODY public.pkg_synonym_20231222 as
    procedure p(v1 varchar2, v2 varchar2, v3 varchar2, v4 varchar2) as
begin
null;
end;
END pkg_synonym_20231222
/
declare
v1 varchar2(20) := null;
v2 varchar2(20) := null;
v4 varchar2(20) := null;
begin
call p(v1,v2,'xxxx',v4);
end;
/
drop package public.pkg_synonym_20231222;
drop schema u_synonym_20231222 cascade;
RESET SESSION AUTHORIZATION;
drop user u_synonym_20231222;
reset search_path;

-- DONE
