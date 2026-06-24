\c regression_ora

\set ECHO all
SET client_encoding = utf8;
SET datestyle TO ISO;
\pset null "*NULL*"

--
-- BEGIN ID858211109
-- DESC: add minus support
--
-- exercise both hashed and sorted implementations of INTERSECT/EXCEPT/MINUSE
set enable_indexonlyscan to off;
set enable_hashagg to on;

explain (costs off)
select unique1 from tenk1 minus select unique2 from tenk1 where unique2 != 10;
select unique1 from tenk1 minus select unique2 from tenk1 where unique2 != 10;

set enable_hashagg to off;

explain (costs off)
select count(*) from
    ( select unique1 from tenk1 intersect select fivethous from tenk1 ) ss;
select count(*) from
    ( select unique1 from tenk1 intersect select fivethous from tenk1 ) ss;

explain (costs off)
select unique1 from tenk1 minus select unique2 from tenk1 where unique2 != 10;
select unique1 from tenk1 minus select unique2 from tenk1 where unique2 != 10;

reset enable_hashagg;
reset enable_indexonlyscan;

--
-- Mixed types
--
CREATE TABLE FLOAT8_TBL(f1 float8) DISTRIBUTE BY REPLICATION;

INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1004.30  ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e+200');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e-200');

CREATE TABLE INT8_TBL(q1 int8, q2 int8) DISTRIBUTE BY REPLICATION;

INSERT INTO INT8_TBL VALUES('  123   ','  456');
INSERT INTO INT8_TBL VALUES('123   ','4567890123456789');
INSERT INTO INT8_TBL VALUES('4567890123456789','123');
INSERT INTO INT8_TBL VALUES(+4567890123456789,'4567890123456789');
INSERT INTO INT8_TBL VALUES('+4567890123456789','-4567890123456789');

--XL: because of how it is used later, make replicated to avoid failures
--    to avoid partition column update
CREATE TABLE INT4_TBL(f1 int4) DISTRIBUTE BY REPLICATION;
INSERT INTO INT4_TBL(f1) VALUES ('   0  ');
INSERT INTO INT4_TBL(f1) VALUES ('123456     ');
INSERT INTO INT4_TBL(f1) VALUES ('    -123456');
INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

SELECT f1 FROM float8_tbl MINUS SELECT f1 FROM int4_tbl ORDER BY 1;

--
-- Operator precedence and (((((extra))))) parentheses
--
SELECT q1 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl MINUS SELECT q1 FROM int8_tbl ORDER BY 1;
SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl MINUS SELECT q1 FROM int8_tbl ORDER BY 1))) ORDER BY 1;
(((SELECT q1 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl))) MINUS SELECT q1 FROM int8_tbl ORDER BY 1;

--
-- Subqueries with ORDER BY & LIMIT clauses
--
-- In this syntax, ORDER BY/LIMIT apply to the result of the MINUSE
SELECT q1,q2 FROM int8_tbl MINUS SELECT q2,q1 FROM int8_tbl ORDER BY q2,q1;
-- This should fail, because q2 isn't a name of an MINUSE output column
SELECT q1 FROM int8_tbl MINUS SELECT q2 FROM int8_tbl ORDER BY q2 LIMIT 1;
-- But this should work:
SELECT q1 FROM int8_tbl MINUS (((SELECT q2 FROM int8_tbl ORDER BY q2 LIMIT 1))) ORDER BY 1;

drop table INT8_TBL;
drop table INT4_TBL;
drop table FLOAT8_TBL;
--
-- END ID858211109
--

--
-- BEGIN ID859911259
-- DESC: opentenbase_ora UNION type mismatch for NULL constant in target list.
--
-- CTE case
drop table if exists x;
create table x(x1 int, x2 varchar(10));
insert into x values(1, 't');

with dt as (select null n from x) select x1 from x union select n from dt;
with dt as (select * from (select null n from x) k) select x1 from x union select n from dt;

select 1 from x union (with dt as (select 1) select * from dt);
select 1 from x union (with dt as (select null) select * from dt);
select x1 from x union (with dt as (select null) select * from dt);
select * from x dt where not exists (select distinct null from x where dt.x1 > 0 union all select x1 from x);
with dt as (select null n from x) select x1 from x union select n from dt;

with dt as (select null n from x) select x1 from x union select * from (select * from (select n from dt));
-- join
with dt as (select n from x join (select null n) k on (1=1)) select x1 from x union select n from dt;

-- recursive CTE
WITH RECURSIVE t(n, nn) AS (
    select 1, null
UNION ALL
    SELECT n+1, null FROM t WHERE n < 100
)
SELECT nn FROM t where n = 1 union select x1 from x;

-- more free test
select * from (select 1 from x union (with dt as (select 1) select * from dt));
select * from (select 1 from x union (with dt as (select null n) select * from dt));
select * from x where not exists (select 1 from x union (with dt as (select null) select * from dt));
select * from (select * from (select null)) union select x1 from x;
select 1 c union select distinct n  from (values (null)) as dt(n) order by 1;
drop table x;

-- user query
CREATE TABLE ch_td_taskdetail (
    taskid numeric(14,0) NOT NULL,
    infoid numeric(14,0) NOT NULL,
    receiver character varying(32) NOT NULL,
    dispatchedby character varying(32),
    parenttaskid numeric(14,0),
    tasktype numeric(1,0) NOT NULL,
    state numeric(1,0) NOT NULL,
    startdate timestamp(0) without time zone NOT NULL,
    enddate timestamp(0) without time zone NOT NULL,
    ismainreceiver numeric(1,0) NOT NULL,
    finisheddate timestamp(0) without time zone,
    wayid character varying(32),
    isneedfeedback numeric(1,0) NOT NULL,
    issubtask numeric(1,0) NOT NULL,
    iscollection numeric(1,0) NOT NULL,
    ismultiway numeric(1,0) NOT NULL,
    isread numeric(1,0) NOT NULL,
    updatedate timestamp(0) without time zone,
    istoprecord numeric(1,0) DEFAULT 0 NOT NULL,
    readdate timestamp(0) without time zone,
    ischecked numeric(1,0) DEFAULT 0 NOT NULL,
    carryhours character varying(16),
    approvallevel character varying(3),
    confirm numeric(1,0),
    region numeric(5,0)
)
DISTRIBUTE BY SHARD (taskid) to GROUP default_group;
CREATE TABLE sa_db_dictitem (
    dictid character varying(32) NOT NULL,
    groupid character varying(32) NOT NULL,
    region numeric(5,0) NOT NULL,
    dictname character varying(64),
    sortorder numeric(3,0),
    status numeric(1,0),
    statusdate timestamp(0) without time zone,
    description character varying(128),
    parentdictid character varying(128)
)
DISTRIBUTE BY SHARD (dictid) to GROUP default_group;
CREATE TABLE ch_td_inforeceiverrela (
    infoid numeric(14,0) NOT NULL,
    receiver character varying(32) NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    isread numeric(1,0) NOT NULL,
    iscollection numeric(1,0) NOT NULL,
    readdate timestamp(0) without time zone,
    readtool numeric(1,0),
    updatedate timestamp(0) without time zone,
    istoprecord numeric(1,0) DEFAULT 0 NOT NULL
)
DISTRIBUTE BY SHARD (infoid) to GROUP default_group;
CREATE TABLE ch_td_generalinfodetail (
    infoid numeric(14,0) NOT NULL,
    region numeric(5,0) NOT NULL,
    title character varying(512) NOT NULL,
    infotype character varying(32) NOT NULL,
    startdate timestamp(0) without time zone NOT NULL,
    enddate timestamp(0) without time zone NOT NULL,
    neededtrain numeric(1,0),
    isemergent numeric(1,0),
    isimportant numeric(1,0),
    frequency numeric(1,0) NOT NULL,
    isneedfeedback numeric(1,0) NOT NULL,
    module character varying(32) NOT NULL,
    infocolumn character varying(32) NOT NULL,
    draftwayid character varying(32) NOT NULL,
    owner character varying(32) NOT NULL,
    publishtype numeric(1,0) NOT NULL,
    summary character varying(2048),
    content character varying(4000) NOT NULL,
    publishdate timestamp(0) without time zone NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    state numeric(2,0) NOT NULL,
    infolabel character varying(32),
    traindate timestamp(0) without time zone,
    approvallevel character varying(3),
    iswaytask numeric(1,0) DEFAULT 0 NOT NULL
)
DISTRIBUTE BY SHARD (infoid) to GROUP default_group;

WITH TASKBASE AS (SELECT TASKDETAIL.INFOID,INFODETAIL.MODULE,INFODETAIL.INFOCOLUMN AS INFOCOLUMN,(SELECT DICT.DICTNAME FROM SA_DB_DICTITEM DICT
WHERE DICT.DICTID = INFODETAIL.INFOCOLUMN AND DICT.GROUPID = 'ColumnType'
AND DICT.REGION = 999) AS DISPLAYINFOCOLUMN,INFODETAIL.TITLE,INFODETAIL.PUBLISHDATE::text DISPLAYPUBLISHDATE,
(SELECT COUNT(*) FROM CH_TD_TASKDETAIL DETAIL WHERE DETAIL.TASKID = TASKDETAIL.TASKID AND DETAIL.STATE IN ('0', '1') AND DETAIL.TASKTYPE = 0 AND ((DETAIL.ISMULTIWAY = 1) OR (DETAIL.ISMULTIWAY = 0 AND DETAIL.WAYID IS NULL))) AS TOTALTASKCOUNT,
(SELECT COUNT(*) FROM CH_TD_TASKDETAIL DETAIL WHERE DETAIL.TASKID = TASKDETAIL.TASKID AND DETAIL.STATE = '1' AND DETAIL.TASKTYPE = 0 AND ((DETAIL.ISMULTIWAY = 1) OR(DETAIL.ISMULTIWAY = 0 AND DETAIL.WAYID IS NULL)))         AS FINISHEDCOUNT,INFODETAIL.INFOTYPE,INFODETAIL.ISIMPORTANT,INFODETAIL.REGION,
TASKDETAIL.ISMULTIWAY,TASKDETAIL.TASKID
,TASKDETAIL.STATE AS TASKDETAILSTATE,TASKDETAIL.TASKTYPE,TASKDETAIL.ISREAD,INFODETAIL.CREATEDATE
FROM CH_TD_TASKDETAIL TASKDETAIL
INNER JOIN CH_TD_GENERALINFODETAIL INFODETAIL ON TASKDETAIL.INFOID = INFODETAIL.INFOID
WHERE TASKDETAIL.RECEIVER = 'zjadmin' AND INFODETAIL.STATE = '3' AND TASKDETAIL.STATE IN ('0', '1') AND TASKDETAIL.ISSUBTASK = 0 AND INFODETAIL.INFOTYPE = 'task'),
NOTICEBASE AS (SELECT INFO_DETAIL.INFOID,
INFO_DETAIL.MODULE,INFO_DETAIL.INFOCOLUMN AS
INFOCOLUMN,(SELECT DICT.DICTNAME FROM SA_DB_DICTITEM DICT WHERE DICT.DICTID = INFO_DETAIL.INFOCOLUMN AND DICT.GROUPID = 'ColumnType' AND DICT.REGION = 999) AS
DISPLAYINFOCOLUMN,INFO_DETAIL.TITLE,INFO_DETAIL.PUBLISHDATE::text
DISPLAYPUBLISHDATE,(SELECT COUNT(*) FROM CH_TD_INFORECEIVERRELA INFO_REVEIVE WHERE INFO_REVEIVE.INFOID = INFO_DETAIL.INFOID) AS
TOTALREADCOUNT,(SELECT COUNT(*) FROM CH_TD_INFORECEIVERRELA INFO_REVEIVE WHERE INFO_REVEIVE.INFOID = INFO_DETAIL.INFOID AND INFO_REVEIVE.ISREAD = '1') AS
READCOUNT,INFO_DETAIL.INFOTYPE,INFO_DETAIL.ISIMPORTANT,INFO_DETAIL.REGION,
NULL AS ISMULTIWAY,NULL AS TASKID,NULL AS TASKDETAILSTATE,NULL AS TASKTYPE,INFORECEIVER_RELA.ISREAD,INFO_DETAIL.CREATEDATE
FROM CH_TD_INFORECEIVERRELA INFORECEIVER_RELA
INNER JOIN CH_TD_GENERALINFODETAIL INFO_DETAIL ON INFORECEIVER_RELA.INFOID = INFO_DETAIL.INFOID
WHERE INFORECEIVER_RELA.RECEIVER = 'zjadmin' AND INFO_DETAIL.STATE = '3' AND INFO_DETAIL.INFOTYPE = 'notice')
SELECT BASE.INFOID,
       BASE.MODULE,
       BASE.INFOCOLUMN,
       BASE.DISPLAYINFOCOLUMN,
       BASE.TITLE,
       BASE.DISPLAYPUBLISHDATE,
       BASE.FINISHEDRATE,
       BASE.READRATE,
       BASE.INFOTYPE,
       BASE.ISIMPORTANT,
       BASE.REGION,
       BASE.ISMULTIWAY,
       BASE.TASKID,
       BASE.TASKDETAILSTATE,
       BASE.TASKTYPE,
       BASE.ISREAD,
       BASE.CREATEDATE FROM(SELECT INFOID,MODULE, INFOCOLUMN, DISPLAYINFOCOLUMN, TITLE, DISPLAYPUBLISHDATE,
                                         DECODE(TOTALTASKCOUNT, 0, 0,
                                                FLOOR(FINISHEDCOUNT / TOTALTASKCOUNT * 100)) FINISHEDRATE,
                                         NULL AS READRATE, INFOTYPE, ISIMPORTANT, REGION, ISMULTIWAY, TASKID,
                                         TASKDETAILSTATE, TASKTYPE, ISREAD,
                                         CREATEDATE FROM TASKBASE
										 UNION ALL
										 SELECT INFOID, MODULE, INFOCOLUMN,
                                         DISPLAYINFOCOLUMN, TITLE, DISPLAYPUBLISHDATE, NULL AS FINISHEDRATE,
                                         DECODE(TOTALREADCOUNT, 0, 0, FLOOR(READCOUNT / TOTALREADCOUNT * 100)) READRATE,
                                         INFOTYPE, ISIMPORTANT, REGION, ISMULTIWAY, TASKID, TASKDETAILSTATE, TASKTYPE,
                                         ISREAD, CREATEDATE FROM NOTICEBASE) BASE ORDER BY BASE.CREATEDATE DESC;

DROP TABLE ch_td_taskdetail;
DROP TABLE sa_db_dictitem;
DROP TABLE ch_td_inforeceiverrela;
DROP TABLE ch_td_generalinfodetail;
--
-- END ID859911259
--

--
-- BEGIN: ID89972815
-- DESC: check targettype for type cast
--
create table t1(id int,name varchar(20));
create table t2(id int,name varchar(20));
drop table if exists t3;
create table t3(id int,name varchar(20));
insert into t1 select i,'a' from generate_series(1,40) i;
insert into t2 select i,'a' from generate_series(1,40) i;
insert into t3 select i,'a' from generate_series(1,40) i;
select id,name,null as hehe from t1 where id < 2
union all select id,name,null as hehe from t2 where id < 2
union all select id,name,null as hehe from t3 where id < 2;
drop table t1;
drop table t2;
drop table t3;
--
-- END: ID89972815
--

--
-- BEGIN ID:89823137
-- DESC: Fix levelsup for targetEntryGetOrigExpr.
--
explain (costs off)
with va as(select * from pg_tables),
vb as (select * from va)
select * from va
union all
select * from vb;

explain (costs off)
with va as (
	with vb as (
		select * from pg_tables
	)
	select * from vb order by 1
)
select * from va
union all
select * from va;
--
-- END ID89823137
--

--
-- BEGIN ID84480195
-- DESC:Fix bugs of text to numeric in UNION ALL.
--

--
-- END ID859911259
--

--
-- BEGIN ID884180763
-- 	migration from v5.06.4.3
--

-- support distinct 
select 1 from dual union distinct select 1 from dual;
select 1 from dual intersect distinct select 1 from dual;
select 1 from dual minus distinct select 1 from dual;
select 1 from dual except distinct select 1 from dual;
select 1 from generate_series(1, 3) minus distinct select 2 from generate_series(1, 3) order by 1;

-- support all
select 1 from dual union all select 1 from dual;
select 1 from generate_series(1, 3) intersect all select 1 from generate_series(1, 3) order by 1;
select 1 from generate_series(1, 5) minus all select 1 from generate_series(1, 2) order by 1;
select 1 from generate_series(1, 5) except all select 1 from generate_series(1, 2) order by 1;

-- support minus
select 1 from dual minus select 1 from dual;

-- support blob, clob, bfile(todo)
create table tbl_set_test_20230608_1 (c clob, b blob);
insert into tbl_set_test_20230608_1 values('clob1', 'b10b');
insert into tbl_set_test_20230608_1 values(null, null);
select b from tbl_set_test_20230608_1 union select b from tbl_set_test_20230608_1;
select b from tbl_set_test_20230608_1 union all select b from tbl_set_test_20230608_1;
select b from tbl_set_test_20230608_1 intersect select b from tbl_set_test_20230608_1;
select b from tbl_set_test_20230608_1 minus select b from tbl_set_test_20230608_1;
select c from tbl_set_test_20230608_1 union select c from tbl_set_test_20230608_1;
select c from tbl_set_test_20230608_1 union all select c from tbl_set_test_20230608_1;
select c from tbl_set_test_20230608_1 intersect select c from tbl_set_test_20230608_1;
select c from tbl_set_test_20230608_1 minus select c from tbl_set_test_20230608_1;

--select bf from tbl_set_test_20230608_1 union all select bf from tbl_set_test_20230608_1;
--select bf from tbl_set_test_20230608_1 union select bf from tbl_set_test_20230608_1;
--select bf from tbl_set_test_20230608_1 intersect select bf from tbl_set_test_20230608_1;
drop table tbl_set_test_20230608_1;

-- support characters:  char && char && varchar2 && varchar2
create table tbl_set_test_20230608_2 (c1 char(8), c2 char(16), v1 varchar2(32), 
									v2 varchar2(64), nc1 nchar(8), nc2 nchar(16),
									vnc1 nvarchar2(32), vnc2 nvarchar2(64) );
insert into tbl_set_test_20230608_2 values('c1', 'c2', 'v1', 'v2', 'nc1', 'nc2', 'vnc1', 'vnc2');

select c1 from tbl_set_test_20230608_2
union select c1 from tbl_set_test_20230608_2 
union select c2 from tbl_set_test_20230608_2 
union select v1 from tbl_set_test_20230608_2 
union select v2 from tbl_set_test_20230608_2 order by 1;

select c1 from tbl_set_test_20230608_2
intersect select c1 from tbl_set_test_20230608_2 
intersect select c2 from tbl_set_test_20230608_2 
intersect select v1 from tbl_set_test_20230608_2
intersect select v2 from tbl_set_test_20230608_2;

select c1 from tbl_set_test_20230608_2
minus select c1 from tbl_set_test_20230608_2
minus select c2 from tbl_set_test_20230608_2
minus select v1 from tbl_set_test_20230608_2
minus select v2 from tbl_set_test_20230608_2;
select c1 from tbl_set_test_20230608_2 union select nc1 from tbl_set_test_20230608_2 order by 1;
select c1 from tbl_set_test_20230608_2 intersect select nc1 from tbl_set_test_20230608_2;
select c1 from tbl_set_test_20230608_2 minus select nc1 from tbl_set_test_20230608_2;

-- nchar && nchar && nvarchar && nvarchar2
select nc1 from tbl_set_test_20230608_2
union select nc1 from tbl_set_test_20230608_2
union select nc2 from tbl_set_test_20230608_2
union select vnc1 from tbl_set_test_20230608_2
union select vnc2 from tbl_set_test_20230608_2 order by 1;

select nc1 from tbl_set_test_20230608_2
intersect select nc1 from tbl_set_test_20230608_2
intersect select nc2 from tbl_set_test_20230608_2 
intersect select vnc1 from tbl_set_test_20230608_2
intersect select vnc2 from tbl_set_test_20230608_2;

select nc1 from tbl_set_test_20230608_2
minus select nc1 from tbl_set_test_20230608_2
minus select nc2 from tbl_set_test_20230608_2
minus select vnc1 from tbl_set_test_20230608_2
minus select vnc2 from tbl_set_test_20230608_2;
drop table tbl_set_test_20230608_2;

-- support: int && float && double && number 与 BINARY_FLOAT && BINARY_DOUBLE
create table tbl_set_test_20230608_3 (i int, f float, d double precision, n1 number(10,2), 
				n2 number(20, 4), bf binary_float, bd binary_double);
insert into tbl_set_test_20230608_3 values(1, 2.1, 3.11, 4.11, 5.21, 6.01, 7.23);

select i from tbl_set_test_20230608_3 
union select f from tbl_set_test_20230608_3 
union select d from tbl_set_test_20230608_3
union select n1 from tbl_set_test_20230608_3
union select n2 from tbl_set_test_20230608_3 order by 1;

select i from tbl_set_test_20230608_3 union select bf from tbl_set_test_20230608_3 order by 1;
select i from tbl_set_test_20230608_3 union select bd from tbl_set_test_20230608_3 order by 1;
select n1 from tbl_set_test_20230608_3 union select bd from tbl_set_test_20230608_3 order by 1;
select bf from tbl_set_test_20230608_3 union select bf from tbl_set_test_20230608_3 
union select bd from tbl_set_test_20230608_3 union select bf from tbl_set_test_20230608_3 order by 1;

select bf from tbl_set_test_20230608_3 
union all select bf from tbl_set_test_20230608_3
union all select bd from tbl_set_test_20230608_3
union all select bf from tbl_set_test_20230608_3 order by 1;

select bf from tbl_set_test_20230608_3 
minus select bf from tbl_set_test_20230608_3
minus select bd from tbl_set_test_20230608_3
minus select bf from tbl_set_test_20230608_3;
drop table tbl_set_test_20230608_3;

-- support subquery
select * from  (select 1 from dual union select 2 from dual);
select 1 from dual where 1 in (select 1 from dual union select 2 from dual);
select 1 from dual where 1 in (select 1 from dual minus select 2 from dual);
select 1 from dual where 1 in (select 1 from dual intersect select 2 from dual);

-- support cte
with va as(select 1 from dual),
vb as (select * from va)
select * from va
union all
select * from vb;

-- support view
create view v_set_20230608_1 as select 1 from dual union select 2 from dual;
drop view v_set_20230608_1;

-- support for update
create table tbl_set_test_20230608_7(a number, b varchar2(30));
insert into tbl_set_test_20230608_7 values(1, 'xxx');
insert into tbl_set_test_20230608_7 values(2, 'yyy');

-- should failed
select a from tbl_set_test_20230608_7 for update union select b from tbl_set_test_20230608_7;
select a from tbl_set_test_20230608_7 union select b from tbl_set_test_20230608_7 for update;
select b from (select b from tbl_set_test_20230608_7 for update) union select b from tbl_set_test_20230608_7 order by 1;
select b from tbl_set_test_20230608_7 union select b from  (select b from tbl_set_test_20230608_7 for update) order by 1;
select a from tbl_set_test_20230608_7 for update union all select b from tbl_set_test_20230608_7;
select a from tbl_set_test_20230608_7 for update union all select b from tbl_set_test_20230608_7;
select b from tbl_set_test_20230608_7 union all select a from tbl_set_test_20230608_7 for update;
select b from (select b from tbl_set_test_20230608_7 for update) union all select b from tbl_set_test_20230608_7 order by 1;
select b from tbl_set_test_20230608_7 union all select b from  (select b from tbl_set_test_20230608_7 for update) order by 1;
select a from tbl_set_test_20230608_7 for update intersect select b from tbl_set_test_20230608_7;
(select a from tbl_set_test_20230608_7 for update) intersect select b from tbl_set_test_20230608_7 order by 1;
select b from (select b from tbl_set_test_20230608_7 for update) intersect select b from tbl_set_test_20230608_7 order by 1;
select b from tbl_set_test_20230608_7 intersect select b from  (select b from tbl_set_test_20230608_7 for update) order by 1;
select b from tbl_set_test_20230608_7 intersect select b from tbl_set_test_20230608_7 for update;
select a from tbl_set_test_20230608_7 for update minus select b from tbl_set_test_20230608_7;
select a from tbl_set_test_20230608_7  minus select b from tbl_set_test_20230608_7  for update;
select b from (select b from tbl_set_test_20230608_7 for update) minus select b from tbl_set_test_20230608_7 order by 1;
select b from tbl_set_test_20230608_7 minus select b from  (select b from tbl_set_test_20230608_7 for update) order by 1;
drop table tbl_set_test_20230608_7;

-- priority
create table tbl_set_test_20230608_8(a number, b varchar2(30));
insert into tbl_set_test_20230608_8 values(1, 'xxx');
insert into tbl_set_test_20230608_8 values(2, 'yyy');
insert into tbl_set_test_20230608_8 values(3, 'mmm');
insert into tbl_set_test_20230608_8 values(4, 'nnn');
insert into tbl_set_test_20230608_8 values(5, 'kkk');
select 3 from dual union select a from tbl_set_test_20230608_8 intersect select 1 from dual order by 1;
select a from tbl_set_test_20230608_8 intersect select 1 from dual union select 3 from dual order by 1;
select 3 from dual union select a from tbl_set_test_20230608_8 minus select 1 from dual order by 1;
select a from tbl_set_test_20230608_8 minus select 1 from dual union select 3 from dual order by 1;
select a from tbl_set_test_20230608_8 intersect select a from tbl_set_test_20230608_8 where a < 4 minus select 2 from dual order by 1;
select a from tbl_set_test_20230608_8 minus select a from tbl_set_test_20230608_8 where a < 4 intersect select 2 from dual order by 1;
drop table tbl_set_test_20230608_8;

-- create table
create table tbl_set_test_20230608_9 as select 1 from dual union select 2 from dual;
\d tbl_set_test_20230608_9
create table tbl_set_test_20230608_10 as select 1 as a from dual minus select 2 as b from dual;
\d tbl_set_test_20230608_10
drop table tbl_set_test_20230608_9;
drop table tbl_set_test_20230608_10;

--
-- END ID884180763
--

--
-- UNION types TEXT and NUMERIC cannot be matched
--
select null, null from dual union all select null,null from dual union all select '1'::number, null from dual;
select null, null from dual union select null,null from dual intersect select null, '1'::number from dual;
select null, null from dual union select null,null from dual union select null, '1'::number from dual;
select null, null from dual minus select null,null from dual union all select 1, '1'::number from dual;

select null, null from dual minus select null,null from dual union all select '1'::number, null from dual;
select null, null from dual minus select null,null from dual minus select '1'::number, null from dual;
select null, null from dual intersect select null,null from dual minus select '1'::number, null from dual;
select null, null from dual intersect select null,null from dual intersect select '1'::number, null from dual;
select null, null from dual union select null,null from dual intersect select '1'::number, null from dual;
select null, null from dual union select null,null from dual union select '1'::number, null from dual;

create table tbl_set_test_20231222_1(c1 int, c2 varchar(1));
create table tbl_set_test_20231222_2(c1 int, c2 numeric(10,0));
select 1, null, null from dual union all
select 1, null, null from dual union all
select 1, null, null from dual union all
select 1, t2.c2, null from tbl_set_test_20231222_1 t1, tbl_set_test_20231222_2 t2;

drop table tbl_set_test_20231222_1;
drop table tbl_set_test_20231222_2;

-- clean up
reset client_min_messages;
reset datestyle;
reset intervalstyle;
reset client_encoding;
-- DONE --
