\c regression_ora

CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;
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
create schema cbs;
SET search_path = cbs, pg_catalog;
CREATE TABLE so_master (
    id numeric(15,0) NOT NULL,
    so_no character varying(15) NOT NULL,
    so_type character varying(2) NOT NULL,
    so_period character varying(6) NOT NULL,
    so_date timestamp(0) without time zone NOT NULL,
    so_store_no character varying(15) NOT NULL,
    so_doc_no character varying(10) NOT NULL,
    so_doc_status character varying(2) NOT NULL,
    so_prom_flag character varying(1) NOT NULL,
    so_whole_prom_code character varying(8),
    so_entry_class character varying(1) NOT NULL,
    so_entry_time timestamp(6) without time zone NOT NULL,
    so_entry_by character varying(20) NOT NULL,
    app_error_status character varying(2) NOT NULL,
    order_dealer_no character varying(15) NOT NULL,
    order_dealer_name character varying(50),
    order_type character varying(4) NOT NULL,
    ref_so_no character varying(15),
    ref_so_period character varying(6),
    ref_so_date timestamp(0) without time zone,
    total_sale_amt numeric(11,2) NOT NULL,
    total_sale_product_amt numeric(11,2) NOT NULL,
    total_sale_non_product_amt numeric(11,2) NOT NULL,
    total_sale_mkp_coupon_01 numeric(11,2) NOT NULL,
    total_sale_mkp_coupon_02 numeric(11,2) NOT NULL,
    total_sale_salary_coupon numeric(11,2) NOT NULL,
    total_sale_net_amt numeric(11,2) NOT NULL,
    is_calc_first_so numeric(1,0) NOT NULL,
    calc_period character varying(6),
    calc_point numeric(11,2) NOT NULL,
    calc_discount_point numeric(11,2) NOT NULL,
    calc_rebate numeric(11,2) NOT NULL,
    comments character varying(200),
    last_updated_time timestamp(6) without time zone NOT NULL,
    last_updated_by character varying(20) NOT NULL,
    pre_order_dealer_no character varying(15),
    total_sale_discount_amt numeric(11,2) DEFAULT 0 NOT NULL,
    company_no character varying(15) DEFAULT '3001'::character varying NOT NULL,
    po_process_code character varying(4) DEFAULT '*'::character varying NOT NULL,
    customer_no character varying(15),
    calc_rebate_ds numeric(11,2) DEFAULT 0 NOT NULL,
    total_rebate_dis_amt numeric(11,2) DEFAULT 0 NOT NULL,
    is_combine numeric(1,0) DEFAULT 0 NOT NULL
)
DISTRIBUTE BY SHARD (id);

CREATE TABLE po_return_app_service (
    id numeric(15,0) NOT NULL,
    return_app_no character varying(15) NOT NULL,
    ref_po_no character varying(15) NOT NULL,
    order_dealer_no character varying(15) NOT NULL,
    order_dealer_name character varying(50) NOT NULL,
    service_status character varying(2) NOT NULL,
    up_door_addr_province character varying(20),
    up_door_addr_city character varying(40),
    up_door_addr_county character varying(40),
    up_door_addr_district character varying(50),
    up_door_addr_tail character varying(100),
    sender_full_name character varying(50),
    sender_teles character varying(50),
    sender_certificate_no character varying(50),
    return_reason character varying(1024),
    end_reason character varying(200),
    up_door_amt numeric(11,2) NOT NULL,
    is_wechat_pay_order numeric(1,0) NOT NULL,
    is_backtract numeric(1,0) NOT NULL,
    backtract_tran_no character varying(20),
    sap_recovery_no character varying(20),
    shipment_no character varying(15),
    shipment_logistics_status character varying(10),
    receive_area character varying(10),
    shipment_desc character varying(50),
    shipment_qty numeric(10,0) NOT NULL,
    comments character varying(200),
    created_time timestamp(6) without time zone NOT NULL,
    created_by character varying(20) NOT NULL,
    last_updated_time timestamp(6) without time zone NOT NULL,
    last_updated_by character varying(20) NOT NULL,
    up_door_by character varying(20),
    up_door_time timestamp(6) without time zone,
    refund_by character varying(20),
    refund_time timestamp(6) without time zone,
    finish_by character varying(20),
    finish_time timestamp(6) without time zone,
    receive_date timestamp(0) without time zone,
    company_no character varying(15) DEFAULT '3001'::character varying NOT NULL,
    actual_return_product_amt numeric(11,2) DEFAULT 0 NOT NULL,
    actual_return_tax_amt numeric(11,2) DEFAULT 0 NOT NULL,
    po_process_code character varying(4) DEFAULT 'G001'::character varying NOT NULL,
    is_up_door numeric(1,0) DEFAULT 1 NOT NULL,
    return_memo character varying(1024),
    return_so_no character varying(15)
)
DISTRIBUTE BY SHARD (id);

CREATE TABLE po_master (
    id numeric(15,0) NOT NULL,
    po_no character varying(15) NOT NULL,
    po_process_code character varying(4) NOT NULL,
    po_type character varying(2) NOT NULL,
    po_period character varying(6) NOT NULL,
    po_date timestamp(0) without time zone NOT NULL,
    po_store_no character varying(15) NOT NULL,
    po_branch_no character varying(4) NOT NULL,
    po_region_no character varying(4) NOT NULL,
    po_status character varying(2) NOT NULL,
    order_dealer_no character varying(15) NOT NULL,
    order_dealer_name character varying(50),
    order_type character varying(4) NOT NULL,
    ref_po_no character varying(15),
    ref_po_period character varying(6),
    ref_po_date timestamp(0) without time zone,
    total_sale_amt numeric(11,2) NOT NULL,
    total_sale_product_amt numeric(11,2) NOT NULL,
    total_sale_non_product_amt numeric(11,2) NOT NULL,
    total_sale_discount_amt numeric(11,2) NOT NULL,
    total_sale_coupon_amt numeric(11,2) NOT NULL,
    total_sale_net_amt numeric(11,2) NOT NULL,
    total_transport_amt numeric(11,2) NOT NULL,
    total_calc_point numeric(11,2) NOT NULL,
    total_calc_discount_point numeric(11,2) NOT NULL,
    total_calc_rebate numeric(11,2) NOT NULL,
    po_entry_class character varying(1) NOT NULL,
    po_entry_dealer_no character varying(15) NOT NULL,
    po_entry_time timestamp(6) without time zone NOT NULL,
    po_entry_by character varying(20) NOT NULL,
    po_entry_memo character varying(200),
    po_app_no character varying(20),
    po_lcl_no character varying(20),
    po_group_no character varying(20),
    ref_selected_no character varying(20),
    po_prom_flag character varying(1) NOT NULL,
    po_whole_prom_code character varying(8),
    po_price_group_type character varying(5) NOT NULL,
    po_price_attr character varying(1) NOT NULL,
    po_return_restrict_flag character varying(1) NOT NULL,
    payment_total_amt numeric(11,2) NOT NULL,
    payment_status character varying(1) NOT NULL,
    payment_time timestamp(6) without time zone NOT NULL,
    payment_doc_no character varying(20),
    payment_memo character varying(200),
    sap_posting_flag character varying(1) NOT NULL,
    sap_posting_date timestamp(0) without time zone,
    sap_posting_doc_no character varying(20),
    comments character varying(200),
    last_updated_time timestamp(6) without time zone NOT NULL,
    last_updated_by character varying(20) NOT NULL,
    po_owe_no character varying(20),
    order_dealer_branch_no character varying(4),
    order_desc character varying(200),
    company_no character varying(15) DEFAULT '3001'::character varying NOT NULL,
    profit_center_no character varying(10),
    order_customer_no character varying(15),
    total_weight numeric(12,3) DEFAULT 0 NOT NULL,
    total_calc_rebate_ds numeric(11,2) DEFAULT 0 NOT NULL,
    total_vat_amt numeric(11,2) DEFAULT 0 NOT NULL,
    total_consum_tax_amt numeric(11,2) DEFAULT 0 NOT NULL,
    total_compre_tax_amt numeric(11,2) DEFAULT 0 NOT NULL,
    total_rebate_dis_amt numeric(11,2) DEFAULT 0 NOT NULL,
    total_transport_coupon_amt numeric(11,2) DEFAULT 0 NOT NULL,
    po_entry_store_no character varying(15)
)
DISTRIBUTE BY SHARD (id);
CREATE TABLE po_addr_detail (
    id numeric(15,0) NOT NULL,
    po_no character varying(15) NOT NULL,
    delivery_hold_stock_flag character varying(1) NOT NULL,
    delivery_wh_code character varying(4),
    delivery_wh_loc_code character varying(4),
    delivery_plan_date timestamp(0) without time zone,
    delivery_plan_proc_flag character varying(1) NOT NULL,
    delivery_plan_doc_no character varying(20),
    delivery_attr character varying(1) NOT NULL,
    delivery_status character varying(1) NOT NULL,
    delivery_type character varying(2) NOT NULL,
    delivery_dealer_no character varying(15) NOT NULL,
    delivery_store_run_type character varying(1),
    addr_send_id character varying(36),
    addr_area_code character varying(32),
    addr_province character varying(50),
    addr_city character varying(50),
    addr_county character varying(50),
    addr_district character varying(50),
    addr_detail character varying(100),
    r01_full_name character varying(50),
    r01_certificate_no character varying(50),
    r01_teles character varying(50),
    r02_full_name character varying(50),
    r02_certificate_no character varying(50),
    r02_teles character varying(50),
    r03_full_name character varying(50),
    r03_certificate_no character varying(50),
    r03_teles character varying(50),
    stock_proc_memo character varying(100),
    stock_recheck_flag character varying(1),
    stock_recheck_time timestamp(6) without time zone,
    pick_up_verify_code character varying(10),
    pick_up_status character varying(1) NOT NULL,
    pick_up_date timestamp(0) without time zone,
    pick_up_entry_time timestamp(6) without time zone,
    pick_up_entry_by character varying(20),
    comments character varying(200),
    last_updated_time timestamp(6) without time zone NOT NULL,
    last_updated_by character varying(20) NOT NULL,
    shipment_no character varying(15),
    shipment_date timestamp(0) without time zone,
    real_delivery_date timestamp(0) without time zone,
    shipment_logistics_status character varying(2) DEFAULT '*'::character varying,
    shipment_receive_status character varying(2) DEFAULT '00'::character varying,
    r01_certificate_type character varying(2),
    r02_certificate_type character varying(2),
    r03_certificate_type character varying(2),
    is_partial_shipment character varying(2),
    triger_date timestamp(0) without time zone,
    ship_date timestamp(0) without time zone,
    onroad_date timestamp(0) without time zone,
    receive_date timestamp(0) without time zone,
    delivery_package_type character varying(10)
)
DISTRIBUTE BY SHARD (id);

SELECT
/*[PO]-[poApi-dataset.listMyPaidOrder]*/
    *
FROM
    (
    SELECT
        sm.so_date AS created_time,
        sm.so_doc_no AS so_doc_no,
        sm.po_process_code AS po_process_code,
        sm.so_no AS po_no,
        sm.total_sale_amt AS po_total_amt,
        sm.total_sale_product_amt AS total_sale_product_amt,
        sm.calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        sm.ref_so_no AS refOrderNo,
        sm.so_entry_class AS po_entry_class,
        sm.so_entry_time AS so_entry_time,
        sm.pre_order_dealer_no AS pre_order_dealer_no,
        sm.total_sale_net_amt AS totalSaleNetAmt,
        sm.calc_discount_point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        NULL AS total_transport_amt,
        sm.total_sale_discount_amt AS totalSaleDiscountAmt,
        NULL AS coupon_amt,
        NULL AS calc_rebate,
        sm.COMPANY_NO AS companyNo,
        sm.CUSTOMER_NO AS customerNo,
        0 AS totalCompreTaxAmt,
        substr( sm.so_no, 0, 11 ) AS poNo2,
        sm.is_combine AS isCombineOrder,
        '' AS poWholePromCode
    FROM
        so_master sm
    WHERE
        sm.order_dealer_no = '350234001'
        AND sm.so_no NOT LIKE'POWE%'
        AND sm.so_no NOT IN (
        SELECT
            so_no
        FROM
            so_master
        WHERE
            order_dealer_no = '350234001'
            AND substr( so_doc_no, 0, 2 ) = '01'
            AND ref_so_no IS NOT NULL
            AND so_type <> '01'
            AND pre_order_dealer_no IS NULL
        )
        AND (
            EXISTS (
            SELECT PAD
                .po_no
            FROM
                cbs.po_addr_detail PAD
            WHERE
                PAD.po_no = sm.so_no
                AND NOT (
                    ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                    OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                    OR sm.po_process_code = 'G305'
                    OR sm.po_process_code = 'G306'
                )
            )
        )
        AND sm.po_process_code NOT IN ( 'G018', 'G019', 'G001', 'G002', 'G003', 'G020', 'G021', 'G022', 'G023', 'G024' )
        AND sm.so_period >= to_char(sysdate - 2 * interval '1 month', 'yyyyMM')
        AND sm.customer_no IS NULL
        UNION ALL
    SELECT
        sm.so_date AS created_time,
        sm.so_doc_no AS so_doc_no,
        sm.po_process_code AS po_process_code,
        sm.so_no AS po_no,
        sm.total_sale_amt AS po_total_amt,
        sm.total_sale_product_amt AS total_sale_product_amt,
        sm.calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        sm.ref_so_no AS refOrderNo,
        sm.so_entry_class AS po_entry_class,
        sm.so_entry_time AS so_entry_time,
        sm.pre_order_dealer_no AS pre_order_dealer_no,
        sm.total_sale_net_amt AS totalSaleNetAmt,
        sm.calc_discount_point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        NULL AS total_transport_amt,
        sm.total_sale_discount_amt AS totalSaleDiscountAmt,
        NULL AS coupon_amt,
        NULL AS calc_rebate,
        sm.COMPANY_NO AS companyNo,
        sm.CUSTOMER_NO AS customerNo,
        0 AS totalCompreTaxAmt,
        substr( sm.so_no, 0, 11 ) AS poNo2,
        sm.is_combine AS isCombineOrder,
        '' AS poWholePromCode
    FROM
        so_master sm
    WHERE
        sm.order_dealer_no = '350234001'
        AND sm.so_no NOT LIKE'POWE%'
        AND (
            EXISTS (
            SELECT PAD
                .po_no
            FROM
                cbs.po_addr_detail PAD
            WHERE
                PAD.po_no = sm.so_no
                AND NOT (
                    ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                    OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                    OR sm.po_process_code = 'G305'
                    OR sm.po_process_code = 'G306'
                )
            )
        )
        AND sm.so_no NOT IN (
        SELECT
            so_no
        FROM
            so_master
        WHERE
            order_dealer_no = '350234001'
            AND substr( so_doc_no, 0, 2 ) = '01'
            AND ref_so_no IS NOT NULL
            AND so_type <> '01'
            AND pre_order_dealer_no IS NULL
        )
        AND sm.po_process_code NOT IN ( 'G018', 'G019' )
        AND EXISTS ( SELECT pras.return_so_no FROM po_return_app_service pras WHERE pras.return_so_no = sm.so_no )
        AND sm.so_period >= to_char(sysdate - 2 * interval '1 month', 'yyyyMM')
        AND sm.customer_no IS NULL
        UNION ALL
    SELECT
        pm.po_date AS created_time,
        '*' AS so_doc_no,
        pm.po_process_code AS po_process_code,
        pm.po_no AS po_no,
        pm.total_sale_amt AS po_total_amt,
        pm.total_sale_product_amt AS total_sale_product_amt,
        pm.total_calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        pm.ref_po_no AS refOrderNo,
        pm.po_entry_class AS po_entry_class,
        pm.po_entry_time AS so_entry_time,
        NULL AS pre_order_dealer_no,
        NULL AS totalSaleNetAmt,
        pm.total_Calc_Discount_Point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        pm.total_transport_amt AS total_transport_amt,
        pm.total_sale_discount_amt AS totalSaleDiscountAmt,
        pm.total_sale_coupon_amt AS coupon_amt,
        pm.total_calc_rebate AS calc_rebate,
        pm.COMPANY_NO AS companyNo,
        NULL AS customerNo,
        pm.total_compre_tax_amt AS totalCompreTaxAmt,
        substr( pm.po_no, 0, 11 ) AS poNo2,
        0 AS isCombineOrder,
        pm.po_whole_prom_code AS poWholePromCode
    FROM
        po_master pm
    WHERE
        pm.order_dealer_no = '350234001'
        AND pm.po_process_code IN ( 'G010', 'G310', 'G018', 'G019', 'G001', 'G002', 'G003', 'G020', 'G022', 'G023' )
        AND EXISTS (
        SELECT PAD
            .po_no
        FROM
            cbs.po_addr_detail PAD
        WHERE
            PAD.po_no = pm.po_no
            AND NOT (
                ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                OR pm.po_process_code = 'G305'
                OR pm.po_process_code = 'G306'
            )
        )
        AND pm.po_status <> '99'
        AND pm.order_customer_no IS NULL
        AND pm.po_period >= to_char(sysdate - 2 * interval '1 month', 'yyyyMM')
    ) spm
ORDER BY
    spm.created_time DESC,
    spm.poNo2 DESC,
    spm.po_no ASC;

SELECT
/*[PO]-[poApi-dataset.listMyPaidOrder]*/
    *
FROM
    (
    SELECT
        sm.so_date AS created_time,
        sm.so_doc_no AS so_doc_no,
        sm.po_process_code AS po_process_code,
        sm.so_no AS po_no,
        sm.total_sale_amt AS po_total_amt,
        sm.total_sale_product_amt AS total_sale_product_amt,
        sm.calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        sm.ref_so_no AS refOrderNo,
        sm.so_entry_class AS po_entry_class,
        sm.so_entry_time AS so_entry_time,
        sm.pre_order_dealer_no AS pre_order_dealer_no,
        sm.total_sale_net_amt AS totalSaleNetAmt,
        sm.calc_discount_point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        NULL AS total_transport_amt,
        sm.total_sale_discount_amt AS totalSaleDiscountAmt,
        NULL AS coupon_amt,
        NULL AS calc_rebate,
        sm.COMPANY_NO AS companyNo,
        sm.CUSTOMER_NO AS customerNo,
        0 AS totalCompreTaxAmt,
        substr( sm.so_no, 0, 11 ) AS poNo2,
        sm.is_combine AS isCombineOrder,
        '' AS poWholePromCode
    FROM
        so_master sm
    WHERE
        sm.order_dealer_no = '350234001'
        AND sm.so_no NOT LIKE'POWE%'
        AND sm.so_no NOT IN (
        SELECT
            so_no
        FROM
            so_master
        WHERE
            order_dealer_no = '350234001'
            AND substr( so_doc_no, 0, 2 ) = '01'
            AND ref_so_no IS NOT NULL
            AND so_type <> '01'
            AND pre_order_dealer_no IS NULL
        )
        AND (
            EXISTS (
            SELECT PAD
                .po_no
            FROM
                po_addr_detail PAD
            WHERE
                PAD.po_no = sm.so_no
                AND NOT (
                    ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                    OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                    OR sm.po_process_code = 'G305'
                    OR sm.po_process_code = 'G306'
                )
            )
        )
        AND sm.po_process_code NOT IN ( 'G018', 'G019', 'G001', 'G002', 'G003', 'G020', 'G021', 'G022', 'G023', 'G024' )
        AND sm.so_period >= to_char(current_timestamp - 2 * interval '1 month', 'yyyyMM')
        AND sm.customer_no IS NULL
        UNION ALL
    SELECT
        sm.so_date AS created_time,
        sm.so_doc_no AS so_doc_no,
        sm.po_process_code AS po_process_code,
        sm.so_no AS po_no,
        sm.total_sale_amt AS po_total_amt,
        sm.total_sale_product_amt AS total_sale_product_amt,
        sm.calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        sm.ref_so_no AS refOrderNo,
        sm.so_entry_class AS po_entry_class,
        sm.so_entry_time AS so_entry_time,
        sm.pre_order_dealer_no AS pre_order_dealer_no,
        sm.total_sale_net_amt AS totalSaleNetAmt,
        sm.calc_discount_point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        NULL AS total_transport_amt,
        sm.total_sale_discount_amt AS totalSaleDiscountAmt,
        NULL AS coupon_amt,
        NULL AS calc_rebate,
        sm.COMPANY_NO AS companyNo,
        sm.CUSTOMER_NO AS customerNo,
        0 AS totalCompreTaxAmt,
        substr( sm.so_no, 0, 11 ) AS poNo2,
        sm.is_combine AS isCombineOrder,
        '' AS poWholePromCode
    FROM
        so_master sm
    WHERE
        sm.order_dealer_no = '350234001'
        AND sm.so_no NOT LIKE'POWE%'
        AND (
            EXISTS (
            SELECT PAD
                .po_no
            FROM
                po_addr_detail PAD
            WHERE
                PAD.po_no = sm.so_no
                AND NOT (
                    ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                    OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                    OR sm.po_process_code = 'G305'
                    OR sm.po_process_code = 'G306'
                )
            )
        )
        AND sm.so_no NOT IN (
        SELECT
            so_no
        FROM
            so_master
        WHERE
            order_dealer_no = '350234001'
            AND substr( so_doc_no, 0, 2 ) = '01'
            AND ref_so_no IS NOT NULL
            AND so_type <> '01'
            AND pre_order_dealer_no IS NULL
        )
        AND sm.po_process_code NOT IN ( 'G018', 'G019' )
        AND EXISTS ( SELECT pras.return_so_no FROM po_return_app_service pras WHERE pras.return_so_no = sm.so_no )
        AND sm.so_period >= to_char(current_timestamp - 2 * interval '1 month', 'yyyyMM')
        AND sm.customer_no IS NULL
        UNION ALL
    SELECT
        pm.po_date AS created_time,
        '*' AS so_doc_no,
        pm.po_process_code AS po_process_code,
        pm.po_no AS po_no,
        pm.total_sale_amt AS po_total_amt,
        pm.total_sale_product_amt AS total_sale_product_amt,
        pm.total_calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        pm.ref_po_no AS refOrderNo,
        pm.po_entry_class AS po_entry_class,
        pm.po_entry_time AS so_entry_time,
        NULL AS pre_order_dealer_no,
        NULL AS totalSaleNetAmt,
        pm.total_Calc_Discount_Point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        pm.total_transport_amt AS total_transport_amt,
        pm.total_sale_discount_amt AS totalSaleDiscountAmt,
        pm.total_sale_coupon_amt AS coupon_amt,
        pm.total_calc_rebate AS calc_rebate,
        pm.COMPANY_NO AS companyNo,
        NULL AS customerNo,
        pm.total_compre_tax_amt AS totalCompreTaxAmt,
        substr( pm.po_no, 0, 11 ) AS poNo2,
        0 AS isCombineOrder,
        pm.po_whole_prom_code AS poWholePromCode
    FROM
        po_master pm
    WHERE
        pm.order_dealer_no = '350234001'
        AND pm.po_process_code IN ( 'G010', 'G310', 'G018', 'G019', 'G001', 'G002', 'G003', 'G020', 'G022', 'G023' )
        AND EXISTS (
        SELECT PAD
            .po_no
        FROM
            po_addr_detail PAD
        WHERE
            PAD.po_no = pm.po_no
            AND NOT (
                ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                OR pm.po_process_code = 'G305'
                OR pm.po_process_code = 'G306'
            )
        )
        AND pm.po_status <> '99'
        AND pm.order_customer_no IS NULL
        AND pm.po_period >= to_char(current_timestamp - 2 * interval '1 month', 'yyyyMM')
    ) spm
ORDER BY
    spm.created_time DESC,
    spm.poNo2 DESC,
    spm.po_no ASC;

select null union select null union select 1;
select null union all select null union all select 1;

SELECT
/*[PO]-[poApi-dataset.listMyPaidOrder]*/
    *
FROM
    (
    SELECT
        sm.so_date AS created_time,
        sm.so_doc_no AS so_doc_no,
        sm.po_process_code AS po_process_code,
        sm.so_no AS po_no,
        sm.total_sale_amt AS po_total_amt,
        sm.total_sale_product_amt AS total_sale_product_amt,
        sm.calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        sm.ref_so_no AS refOrderNo,
        sm.so_entry_class AS po_entry_class,
        sm.so_entry_time AS so_entry_time,
        sm.pre_order_dealer_no AS pre_order_dealer_no,
        sm.total_sale_net_amt AS totalSaleNetAmt,
        sm.calc_discount_point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        NULL AS total_transport_amt,
        sm.total_sale_discount_amt AS totalSaleDiscountAmt,
        NULL AS coupon_amt,
        NULL AS calc_rebate,
        sm.COMPANY_NO AS companyNo,
        sm.CUSTOMER_NO AS customerNo,
        0 AS totalCompreTaxAmt,
        substr( sm.so_no, 0, 11 ) AS poNo2,
        sm.is_combine AS isCombineOrder,
        '' AS poWholePromCode
    FROM
        so_master sm
    WHERE
        sm.order_dealer_no = '350234001'
        AND sm.so_no NOT LIKE'POWE%'
        AND sm.so_no NOT IN (
        SELECT
            so_no
        FROM
            so_master
        WHERE
            order_dealer_no = '350234001'
            AND substr( so_doc_no, 0, 2 ) = '01'
            AND ref_so_no IS NOT NULL
            AND so_type <> '01'
            AND pre_order_dealer_no IS NULL
        )
        AND (
            EXISTS (
            SELECT PAD
                .po_no
            FROM
                cbs.po_addr_detail PAD
            WHERE
                PAD.po_no = sm.so_no
                AND NOT (
                    ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                    OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                    OR sm.po_process_code = 'G305'
                    OR sm.po_process_code = 'G306'
                )
            )
        )
        AND sm.po_process_code NOT IN ( 'G018', 'G019', 'G001', 'G002', 'G003', 'G020', 'G021', 'G022', 'G023', 'G024' )
        AND sm.so_period >= to_char(sysdate- 2 * interval '1 month', 'yyyyMM')
        AND sm.customer_no IS NULL
        UNION ALL
    SELECT
        sm.so_date AS created_time,
        sm.so_doc_no AS so_doc_no,
        sm.po_process_code AS po_process_code,
        sm.so_no AS po_no,
        sm.total_sale_amt AS po_total_amt,
        sm.total_sale_product_amt AS total_sale_product_amt,
        sm.calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        sm.ref_so_no AS refOrderNo,
        sm.so_entry_class AS po_entry_class,
        sm.so_entry_time AS so_entry_time,
        sm.pre_order_dealer_no AS pre_order_dealer_no,
        sm.total_sale_net_amt AS totalSaleNetAmt,
        sm.calc_discount_point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        NULL AS total_transport_amt,
        sm.total_sale_discount_amt AS totalSaleDiscountAmt,
        NULL AS coupon_amt,
        NULL AS calc_rebate,
        sm.COMPANY_NO AS companyNo,
        sm.CUSTOMER_NO AS customerNo,
        0 AS totalCompreTaxAmt,
        substr( sm.so_no, 0, 11 ) AS poNo2,
        sm.is_combine AS isCombineOrder,
        '' AS poWholePromCode
    FROM
        so_master sm
    WHERE
        sm.order_dealer_no = '350234001'
        AND sm.so_no NOT LIKE'POWE%'
        AND (
            EXISTS (
            SELECT PAD
                .po_no
            FROM
                cbs.po_addr_detail PAD
            WHERE
                PAD.po_no = sm.so_no
                AND NOT (
                    ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                    OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                    OR sm.po_process_code = 'G305'
                    OR sm.po_process_code = 'G306'
                )
            )
        )
        AND sm.so_no NOT IN (
        SELECT
            so_no
        FROM
            so_master
        WHERE
            order_dealer_no = '350234001'
            AND substr( so_doc_no, 0, 2 ) = '01'
            AND ref_so_no IS NOT NULL
            AND so_type <> '01'
            AND pre_order_dealer_no IS NULL
        )
        AND sm.po_process_code NOT IN ( 'G018', 'G019' )
        AND EXISTS ( SELECT pras.return_so_no FROM po_return_app_service pras WHERE pras.return_so_no = sm.so_no )
        AND sm.so_period >= to_char(sysdate- 2 * interval '1 month', 'yyyyMM')
        AND sm.customer_no IS NULL
        UNION ALL
    SELECT
        pm.po_date AS created_time,
        '*' AS so_doc_no,
        pm.po_process_code AS po_process_code,
        pm.po_no AS po_no,
        pm.total_sale_amt AS po_total_amt,
        pm.total_sale_product_amt AS total_sale_product_amt,
        pm.total_calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        pm.ref_po_no AS refOrderNo,
        pm.po_entry_class AS po_entry_class,
        pm.po_entry_time AS so_entry_time,
        NULL AS pre_order_dealer_no,
        NULL AS totalSaleNetAmt,
        pm.total_Calc_Discount_Point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        pm.total_transport_amt AS total_transport_amt,
        pm.total_sale_discount_amt AS totalSaleDiscountAmt,
        pm.total_sale_coupon_amt AS coupon_amt,
        pm.total_calc_rebate AS calc_rebate,
        pm.COMPANY_NO AS companyNo,
        NULL AS customerNo,
        pm.total_compre_tax_amt AS totalCompreTaxAmt,
        substr( pm.po_no, 0, 11 ) AS poNo2,
        0 AS isCombineOrder,
        pm.po_whole_prom_code AS poWholePromCode
    FROM
        po_master pm
    WHERE
        pm.order_dealer_no = '350234001'
        AND pm.po_process_code IN ( 'G010', 'G310', 'G018', 'G019', 'G001', 'G002', 'G003', 'G020', 'G022', 'G023' )
        AND EXISTS (
        SELECT PAD
            .po_no
        FROM
            cbs.po_addr_detail PAD
        WHERE
            PAD.po_no = pm.po_no
            AND NOT (
                ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                OR pm.po_process_code = 'G305'
                OR pm.po_process_code = 'G306'
            )
        )
        AND pm.po_status <> '99'
        AND pm.order_customer_no IS NULL
        AND pm.po_period >= to_char(sysdate- 2 * interval '1 month', 'yyyyMM')
    ) spm
ORDER BY
    spm.created_time DESC,
    spm.poNo2 DESC,
    spm.po_no ASC;

SELECT
/*[PO]-[poApi-dataset.listMyPaidOrder]*/
    *
FROM
    (
    SELECT
        sm.so_date AS created_time,
        sm.so_doc_no AS so_doc_no,
        sm.po_process_code AS po_process_code,
        sm.so_no AS po_no,
        sm.total_sale_amt AS po_total_amt,
        sm.total_sale_product_amt AS total_sale_product_amt,
        sm.calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        sm.ref_so_no AS refOrderNo,
        sm.so_entry_class AS po_entry_class,
        sm.so_entry_time AS so_entry_time,
        sm.pre_order_dealer_no AS pre_order_dealer_no,
        sm.total_sale_net_amt AS totalSaleNetAmt,
        sm.calc_discount_point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        NULL AS total_transport_amt,
        sm.total_sale_discount_amt AS totalSaleDiscountAmt,
        NULL AS coupon_amt,
        NULL AS calc_rebate,
        sm.COMPANY_NO AS companyNo,
        sm.CUSTOMER_NO AS customerNo,
        0 AS totalCompreTaxAmt,
        substr( sm.so_no, 0, 11 ) AS poNo2,
        sm.is_combine AS isCombineOrder,
        '' AS poWholePromCode
    FROM
        so_master sm
    WHERE
        sm.order_dealer_no = '350234001'
        AND sm.so_no NOT LIKE'POWE%'
        AND sm.so_no NOT IN (
        SELECT
            so_no
        FROM
            so_master
        WHERE
            order_dealer_no = '350234001'
            AND substr( so_doc_no, 0, 2 ) = '01'
            AND ref_so_no IS NOT NULL
            AND so_type <> '01'
            AND pre_order_dealer_no IS NULL
        )
        AND (
            EXISTS (
            SELECT PAD
                .po_no
            FROM
                po_addr_detail PAD
            WHERE
                PAD.po_no = sm.so_no
                AND NOT (
                    ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                    OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                    OR sm.po_process_code = 'G305'
                    OR sm.po_process_code = 'G306'
                )
            )
        )
        AND sm.po_process_code NOT IN ( 'G018', 'G019', 'G001', 'G002', 'G003', 'G020', 'G021', 'G022', 'G023', 'G024' )
        AND sm.so_period >= to_char(current_timestamp - 2 * interval '1 month', 'yyyyMM')
        AND sm.customer_no IS NULL
        UNION ALL
    SELECT
        sm.so_date AS created_time,
        sm.so_doc_no AS so_doc_no,
        sm.po_process_code AS po_process_code,
        sm.so_no AS po_no,
        sm.total_sale_amt AS po_total_amt,
        sm.total_sale_product_amt AS total_sale_product_amt,
        sm.calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        sm.ref_so_no AS refOrderNo,
        sm.so_entry_class AS po_entry_class,
        sm.so_entry_time AS so_entry_time,
        sm.pre_order_dealer_no AS pre_order_dealer_no,
        sm.total_sale_net_amt AS totalSaleNetAmt,
        sm.calc_discount_point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        NULL AS total_transport_amt,
        sm.total_sale_discount_amt AS totalSaleDiscountAmt,
        NULL AS coupon_amt,
        NULL AS calc_rebate,
        sm.COMPANY_NO AS companyNo,
        sm.CUSTOMER_NO AS customerNo,
        0 AS totalCompreTaxAmt,
        substr( sm.so_no, 0, 11 ) AS poNo2,
        sm.is_combine AS isCombineOrder,
        '' AS poWholePromCode
    FROM
        so_master sm
    WHERE
        sm.order_dealer_no = '350234001'
        AND sm.so_no NOT LIKE'POWE%'
        AND (
            EXISTS (
            SELECT PAD
                .po_no
            FROM
                po_addr_detail PAD
            WHERE
                PAD.po_no = sm.so_no
                AND NOT (
                    ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                    OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                    OR sm.po_process_code = 'G305'
                    OR sm.po_process_code = 'G306'
                )
            )
        )
        AND sm.so_no NOT IN (
        SELECT
            so_no
        FROM
            so_master
        WHERE
            order_dealer_no = '350234001'
            AND substr( so_doc_no, 0, 2 ) = '01'
            AND ref_so_no IS NOT NULL
            AND so_type <> '01'
            AND pre_order_dealer_no IS NULL
        )
        AND sm.po_process_code NOT IN ( 'G018', 'G019' )
        AND EXISTS ( SELECT pras.return_so_no FROM po_return_app_service pras WHERE pras.return_so_no = sm.so_no )
        AND sm.so_period >= to_char(current_timestamp - 2 * interval '1 month', 'yyyyMM')
        AND sm.customer_no IS NULL
        UNION ALL
    SELECT
        pm.po_date AS created_time,
        '*' AS so_doc_no,
        pm.po_process_code AS po_process_code,
        pm.po_no AS po_no,
        pm.total_sale_amt AS po_total_amt,
        pm.total_sale_product_amt AS total_sale_product_amt,
        pm.total_calc_point AS po_total_point,
        '02' AS po_app_tran_status,
        pm.ref_po_no AS refOrderNo,
        pm.po_entry_class AS po_entry_class,
        pm.po_entry_time AS so_entry_time,
        NULL AS pre_order_dealer_no,
        NULL AS totalSaleNetAmt,
        pm.total_Calc_Discount_Point AS totalCalcDiscountPoint,
        'N' AS ovrOrderFlag,
        pm.total_transport_amt AS total_transport_amt,
        pm.total_sale_discount_amt AS totalSaleDiscountAmt,
        pm.total_sale_coupon_amt AS coupon_amt,
        pm.total_calc_rebate AS calc_rebate,
        pm.COMPANY_NO AS companyNo,
        NULL AS customerNo,
        pm.total_compre_tax_amt AS totalCompreTaxAmt,
        substr( pm.po_no, 0, 11 ) AS poNo2,
        0 AS isCombineOrder,
        pm.po_whole_prom_code AS poWholePromCode
    FROM
        po_master pm
    WHERE
        pm.order_dealer_no = '350234001'
        AND pm.po_process_code IN ( 'G010', 'G310', 'G018', 'G019', 'G001', 'G002', 'G003', 'G020', 'G022', 'G023' )
        AND EXISTS (
        SELECT PAD
            .po_no
        FROM
            po_addr_detail PAD
        WHERE
            PAD.po_no = pm.po_no
            AND NOT (
                ( PAD.delivery_type = '12' AND PAD.shipment_logistics_status = 'K' )
                OR ( PAD.delivery_type <> '12' AND PAD.pick_up_status = 'Y' )
                OR pm.po_process_code = 'G305'
                OR pm.po_process_code = 'G306'
            )
        )
        AND pm.po_status <> '99'
        AND pm.order_customer_no IS NULL
        AND pm.po_period >= to_char(current_timestamp - 2 * interval '1 month', 'yyyyMM')
    ) spm
ORDER BY
    spm.created_time DESC,
    spm.poNo2 DESC,
    spm.po_no ASC;

select null union select null union select 1;
select null union all select null union all select 1;
drop schema cbs cascade;
reset search_path;
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
create table tbl_set_test_20230608_1 (c clob, b blob) distribute by replication;
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

-- support: date，timestamp，timestamp with time zone, timestamp with local time zone
SET timezone = PRC;

create table tbl_set_test_20230608_4 (d date, t timestamp, tz timestamptz, iym interval year to month, iday interval day to second);
insert into tbl_set_test_20230608_4 values(to_date('2023-05-10 11:30:45', 'yyyy-mm-dd hh24:mi:ss'),
	TO_TIMESTAMP('2023-05-10 11:40:45.12345', 'yyyy-mm-dd hh24:mi:ss.US'),
	'2023-05-09 11:40:45.12345'::timestamptz,
	interval '10-5' year to month,
	'21 1:23:45');

select d from tbl_set_test_20230608_4 union select d from tbl_set_test_20230608_4 union select t from tbl_set_test_20230608_4 union select tz from tbl_set_test_20230608_4 order by 1;
select d from tbl_set_test_20230608_4 intersect select d from tbl_set_test_20230608_4 intersect select t from tbl_set_test_20230608_4 intersect select tz from tbl_set_test_20230608_4;
select d from tbl_set_test_20230608_4 minus select d from tbl_set_test_20230608_4 minus select t from tbl_set_test_20230608_4 minus select tz from tbl_set_test_20230608_4;
select d from tbl_set_test_20230608_4 union select iym from tbl_set_test_20230608_4;
select t from tbl_set_test_20230608_4 union select iym from tbl_set_test_20230608_4;
select iym from tbl_set_test_20230608_4 union select iym from tbl_set_test_20230608_4 order by 1;
select iym from tbl_set_test_20230608_4 union select iday from tbl_set_test_20230608_4 order by 1;
drop table tbl_set_test_20230608_4;

-- support: null
create table tbl_set_test_20230608_6 (c1 char(8), c2 char(16), v1 varchar2(32), v2 varchar2(64), nc1 nchar(8), nc2 nchar(16), vnc1 nvarchar2(32), vnc2 nvarchar2(64),
				i int, f float, d double precision, n1 number(10,2), n2 number(20, 4), bf binary_float, bd binary_double, 
				d1 date, t1 timestamp, tz timestamptz, iym interval year to month, iday interval day to second);
insert into tbl_set_test_20230608_6 values('c1', 'c2', 'v1', 'v2', 'nc1', 'nc2', 'vnc1', 'vnc2',
	1, 2.1, 3.11, 4.11, 5.21, 6.01, 7.23,
	to_date('2023-05-10 11:30:45', 'yyyy-mm-dd hh24:mi:ss'),
	TO_TIMESTAMP('2023-05-10 11:40:45.12345', 'yyyy-mm-dd hh24:mi:ss.FF6'),
	'2023-05-09 11:40:45.12345+08',
	interval '10-5' year to month, '21 1:23:45');

insert into tbl_set_test_20230608_6(c1) values(NULL);

select * from (select c1 from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select v1 from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select nc1 from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select vnc1 from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select i from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select f from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select d from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select n1 from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select bf from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select bd from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select d1 from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select t1 from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select tz from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select iym from tbl_set_test_20230608_6 union select null from dual) order by 1;
select * from (select iday from tbl_set_test_20230608_6 union select null from dual) order by 1;

select * from (select c1 from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select v1 from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select nc1 from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select vnc1 from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select i from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select f from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select d from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select n1 from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select bf from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select bd from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select d1 from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select t1 from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select tz from tbl_set_test_20230608_6 intersect  select null from dual) order by 1;
select * from (select iym from tbl_set_test_20230608_6 intersect select null from dual) order by 1;
select * from (select iday from tbl_set_test_20230608_6 intersect select null from dual) order by 1;

select * from (select c1 from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select v1 from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select nc1 from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select vnc1 from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select i from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select f from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select d from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select n1 from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select bf from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select bd from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select d1 from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select t1 from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select tz from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select iym from tbl_set_test_20230608_6 minus select null from dual) order by 1;
select * from (select iday from tbl_set_test_20230608_6 minus select null from dual) order by 1;
drop table tbl_set_test_20230608_6;

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
