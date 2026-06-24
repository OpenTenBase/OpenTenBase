\c regression_ora
create extension if not exists opentenbase_ora_package_function;
CREATE SCHEMA sync;

SET search_path = sync, pg_catalog;



--
-- Name: sp_b03_ts_remetrade(varchar2, varchar2, varchar2, varchar2); Type: PROCEDURE; Schema: sync; Owner: gregsun
--

CREATE PROCEDURE sp_b03_ts_remetrade(p_start_date varchar2, p_work_date varchar2, INOUT err_num varchar2 DEFAULT 0, INOUT err_msg varchar2 DEFAULT NULL::varchar2)
    AS $$
 declare   
  V_START_DATE     DATE;
  V_END_DATE       DATE;
  V_WORK_DATE      DATE;
  V_SP_NAME        VARCHAR(30);
  V_TAB_LEVEL      VARCHAR(20);
  V_LOG_STEP_NO    VARCHAR(20);
  V_LOG_BEGIN_TIME DATE := SYSDATE;
  V_LOG_END_TIME   DATE;
  V_LOG_ROWCOUNT   NUMBER := 0;
  V_ELAPSED        NUMBER;
  V_ALL_ELAPSED    NUMBER;
  V_STEP_DESC      sys_stat_error_log.STEP_DESC%TYPE;
BEGIN
  
  V_SP_NAME   := 'SP_B03_TS_REMETRADE';
  V_TAB_LEVEL := 'B';
  
  IF P_START_DATE IS NULL
  THEN
    RAISE EXCEPTION 'P_START_DATE IS NULL!';
  ELSE
    V_START_DATE := TO_DATE(P_START_DATE, 'YYYY-MM-DD');
  END IF;
  IF P_WORK_DATE IS NULL
  THEN
    RAISE EXCEPTION 'P_WORK_DATE IS NULL!';
  ELSE
    V_WORK_DATE := TO_DATE(P_WORK_DATE, 'YYYY-MM-DD');
  END IF;
  IF P_WORK_DATE IS NULL
  THEN
    RAISE EXCEPTION 'P_WORK_DATE IS NULL!';
  ELSE
    V_END_DATE := TO_DATE(P_WORK_DATE, 'YYYY-MM-DD');
  END IF;
  
  
  
  V_LOG_STEP_NO    := 'STEP_01';
  V_STEP_DESC      := '清除目标表数据';
  V_LOG_BEGIN_TIME := SYSDATE;
  V_LOG_ROWCOUNT   := NULL;
  CALL SP_PUB_INSERT_LOG_DATE(V_SP_NAME 
                        ,
                         V_TAB_LEVEL 
                        ,
                         V_LOG_STEP_NO 
                        ,
                         V_STEP_DESC 
                        ,
                         V_LOG_BEGIN_TIME 
                        ,
                         V_LOG_END_TIME 
                        ,
                         V_WORK_DATE 
                        ,
                         V_LOG_ROWCOUNT 
                        ,
                         V_ELAPSED 
                        ,
                         V_ALL_ELAPSED);
  
  CALL SP_PUB_DEL_TB('B03_TS_REMETRADE');
  /*DELETE FROM B03_TS_REMETRADE Y
  WHERE Y.ENDDATE >=V_START_DATE;*/
  
 GET DIAGNOSTICS V_LOG_ROWCOUNT = ROW_COUNT;
  
  
  
  CALL SP_PUB_UPDATE_LOG_DATE(V_SP_NAME 
                        ,
                         V_TAB_LEVEL 
                        ,
                         V_LOG_STEP_NO 
                        ,
                         V_LOG_BEGIN_TIME 
                        ,
                        SYSDATE::DATE 
                        ,
                         V_WORK_DATE 
                        ,
                         V_LOG_ROWCOUNT 
                        ,
                         (SYSDATE - V_LOG_BEGIN_TIME)::NUMERIC 
                        ,
                         V_ALL_ELAPSED);
  
  V_LOG_STEP_NO    := 'STEP_02';
  V_STEP_DESC      := '插入目标表B03_TS_REMETRADE';
  V_LOG_BEGIN_TIME := SYSDATE;
  V_LOG_ROWCOUNT   := NULL;
  CALL  SP_PUB_INSERT_LOG_DATE(V_SP_NAME, 
                         V_TAB_LEVEL, 
                         V_LOG_STEP_NO, 
                         V_STEP_DESC, 
                         V_LOG_BEGIN_TIME, 
                         V_LOG_END_TIME, 
                         V_WORK_DATE, 
                         V_LOG_ROWCOUNT, 
                         V_ELAPSED, 
                         V_ALL_ELAPSED);
  
  INSERT INTO B03_TS_REMETRADE
    (C_FUNDCODE,
     C_FUNDNAME,
     C_FUNDACCO,
     F_NETVALUE,
     C_AGENCYNAME,
     C_CUSTNAME,
     D_DATE,
     D_CDATE,
     F_CONFIRMBALANCE,
     F_TRADEFARE,
     F_CONFIRMSHARES,
     F_RELBALANCE,
     F_INTEREST,
     INFO,
     WORK_DATE,
     LOAD_DATE)
    SELECT A.C_FUNDCODE,
           A.C_FUNDNAME,
           A.C_FUNDACCO,
           A.F_NETVALUE,
           A.C_AGENCYNAME,
           A.C_CUSTNAME,
           A.D_DATE,
           A.D_CDATE,
           A.F_CONFIRMBALANCE,
           A.F_TRADEFARE,
           A.F_CONFIRMSHARES,
           ABS(NVL(B.F_OCCURBALANCE, A.F_RELBALANCE)) F_RELBALANCE,
           A.F_INTEREST,
           NVL(DECODE(B.C_BUSINFLAG,
                      '02',
                      '申购',
                      '50',
                      '申购',
                      '74',
                      '申购',
                      '03',
                      '赎回'),
               DECODE(A.C_BUSINFLAG,
                      '01',
                      '认购',
                      '02',
                      '申购',
                      '03',
                      '赎回',
                      '53',
                      '强制赎回',
                      '50',
                      '产品成立')) AS INFO,
           V_WORK_DATE,
           SYSDATE AS LOAD_DATE
      FROM (SELECT A.C_FUNDCODE,
                   C.C_FUNDNAME,
                   A.C_FUNDACCO,
                   FUNC_GETLASTNETVALUE(A.C_FUNDCODE, A.D_CDATE) F_NETVALUE,
                   (SELECT C_AGENCYNAME
                      FROM S017_TAGENCYINFO
                     WHERE A.C_AGENCYNO = C_AGENCYNO) C_AGENCYNAME,
                   B.C_CUSTNAME,
                   TO_CHAR(A.D_DATE, 'yyyy-mm-dd') D_DATE,
                   TO_CHAR(A.D_CDATE, 'yyyy-mm-dd') D_CDATE,
                   DECODE(A.C_BUSINFLAG,
                          '03',
                          A.F_CONFIRMBALANCE + A.F_TRADEFARE,
                          '53',
                          A.F_CONFIRMBALANCE + A.F_TRADEFARE,
                          A.F_CONFIRMBALANCE) F_CONFIRMBALANCE,
                   A.F_TRADEFARE,
                   A.F_CONFIRMSHARES,
                   DECODE(A.C_BUSINFLAG,
                          '03',
                          A.F_CONFIRMBALANCE,
                          '53',
                          A.F_CONFIRMBALANCE,
                          A.F_CONFIRMBALANCE - A.F_TRADEFARE) F_RELBALANCE,
                   A.F_INTEREST,
                   A.C_BUSINFLAG,
                   A.C_CSERIALNO
              FROM (SELECT D_DATE,
                           C_AGENCYNO,
                           DECODE(C_BUSINFLAG,
                                  '03',
                                  DECODE(C_IMPROPERREDEEM,
                                         '3',
                                         '100',
                                         '5',
                                         '100',
                                         C_BUSINFLAG),
                                  C_BUSINFLAG) C_BUSINFLAG,
                           C_FUNDACCO,
                           D_CDATE,
                           C_FUNDCODE,
                           F_CONFIRMBALANCE,
                           F_CONFIRMSHARES,
                           C_REQUESTNO,
                           F_TRADEFARE,
                           C_TRADEACCO,
                           F_INTEREST,
                           C_CSERIALNO,
                           L_SERIALNO,
                           L_CONTRACTSERIALNO
                      FROM S017_TCONFIRM_ALL T3
                    UNION
                    SELECT D_DATE,
                           C_AGENCYNO,
                           '02' C_BUSINFLAG,
                           C_FUNDACCO,
                           D_LASTDATE AS D_CDATE, 
                           C_FUNDCODE,
                           F_REINVESTBALANCE F_CONFIRMBALANCE,
                           F_REALSHARES F_CONFIRMSHARES,
                           '' C_REQUESTNO,
                           0 F_TRADEFARE,
                           C_TRADEACCO,
                           0 F_INTEREST,
                           C_CSERIALNO,
                           0 L_SERIALNO,
                           L_CONTRACTSERIALNO
                      FROM S017_TDIVIDENDDETAIL T1
                     WHERE T1.C_FLAG = '0') A
              LEFT JOIN S017_TACCONET TACN
                ON A.C_TRADEACCO = TACN.C_TRADEACCO
              LEFT JOIN (SELECT * FROM S017_TACCOINFO WHERE C_ACCOUNTTYPE = 'A') X
                ON A.C_FUNDACCO = X.C_FUNDACCO
              LEFT JOIN S017_TTRUSTCLIENTINFO_ALL B
                ON X.C_CUSTNO = B.C_CUSTNO
             INNER JOIN S017_TFUNDINFO C
                ON A.C_FUNDCODE = C.C_FUNDCODE
                       ) A
      LEFT JOIN (SELECT ST1.D_CDATE,
                        ST1.C_FUNDCODE,
                        ST1.F_OCCURBALANCE,
                        ST1.C_BUSINFLAG,
                        ST1.C_FUNDACCO,
                        ST1.C_CSERIALNO
                   FROM S017_TSHARECURRENTS_ALL ST1
                  WHERE ST1.C_BUSINFLAG <> '74'
                 UNION ALL
                 SELECT ST2.D_DATE AS D_CDATE,
                        ST2.C_FUNDCODE,
                        ST2.F_TOTALPROFIT AS F_OCCURBALANCE,
                        '74' AS C_BUSINFLAG,
                        ST2.C_FUNDACCO,
                        ST2.C_CSERIALNO
                   FROM S017_TDIVIDENDDETAIL ST2
                  WHERE ST2.C_FLAG = '0') B
        ON A.C_FUNDCODE = B.C_FUNDCODE
       AND A.C_FUNDACCO = B.C_FUNDACCO
       AND TO_DATE(A.D_CDATE, 'YYYY-MM-DD') = B.D_CDATE
       AND A.C_CSERIALNO = B.C_CSERIALNO;
 GET DIAGNOSTICS V_LOG_ROWCOUNT = ROW_COUNT;
  
  
  CALL SP_PUB_UPDATE_LOG_DATE(V_SP_NAME, 
                         V_TAB_LEVEL, 
                         V_LOG_STEP_NO, 
                         V_LOG_BEGIN_TIME, 
                         SYSDATE, 
                         V_WORK_DATE, 
                         V_LOG_ROWCOUNT, 
                         (SYSDATE - V_LOG_BEGIN_TIME)::NUMERIC, 
                         V_ALL_ELAPSED);
  ERR_NUM := 0;
  ERR_MSG := 'NORMAL,SUCCESSFUL COMPLETION';
END;
 $$;


--
-- Name: sp_pub_del_tb(varchar2); Type: PROCEDURE; Schema: sync; Owner: gregsun
--

CREATE PROCEDURE sp_pub_del_tb(p_tab_name varchar2)
    AS $$
 declare  n_sql varchar2(4000);
begin
   
   n_sql := 'truncate table '||p_tab_name;
   
   execute immediate n_sql;
exception
   when no_data_found then null;
   when others then raise;
end ;
 $$;


--
-- Name: sp_pub_insert_log_date(varchar2, varchar2, varchar2, varchar2, date, date, date, numeric, numeric, numeric); Type: PROCEDURE; Schema: sync; Owner: gregsun
--

CREATE PROCEDURE sp_pub_insert_log_date(p_in_proc_name varchar2, p_in_tab_level varchar2, p_in_step_no varchar2, p_in_step_desc varchar2, p_in_begin_time date, p_in_end_time date, p_in_work_date date, p_in_row_num numeric, p_in_elapsed numeric, p_in_all_elapsed numeric)
    AS $$
 declare   
  BEGIN
    INSERT INTO SYNC.SYS_STAT_ERROR_LOG
      (PROC_NAME
      ,TAB_LEVEL
      ,STEP_NO
      ,STEP_DESC
      ,BEGIN_TIME
      ,END_TIME
      ,WORKDATE
      ,ROW_NUM
      ,ELAPSED
      ,ALL_ELAPSED)
    VALUES
      (P_IN_PROC_NAME
      ,P_IN_TAB_LEVEL
      ,P_IN_STEP_NO
      ,P_IN_STEP_DESC
      ,P_IN_BEGIN_TIME
      ,P_IN_END_TIME
      ,P_IN_WORK_DATE
      ,P_IN_ROW_NUM
      ,P_IN_ELAPSED
      ,P_IN_ALL_ELAPSED);
    COMMIT;
  END ;
 $$;

--
-- Name: sp_pub_update_log_date(varchar2, varchar2, varchar2, date, date, date, numeric, numeric, numeric); Type: PROCEDURE; Schema: sync; Owner: gregsun
--

CREATE PROCEDURE sp_pub_update_log_date(p_in_proc_name varchar2, p_in_tab_level varchar2, p_in_step_no varchar2, p_in_begin_time date, p_in_end_time date, p_in_work_date date, p_in_row_num numeric, p_in_elapsed numeric, p_in_all_elapsed numeric)
    AS $$   BEGIN
    UPDATE SYNC.SYS_STAT_ERROR_LOG
       SET END_TIME = P_IN_END_TIME
          ,ROW_NUM = P_IN_ROW_NUM
          ,ELAPSED = P_IN_ELAPSED
          ,ALL_ELAPSED = P_IN_ALL_ELAPSED
     WHERE PROC_NAME = P_IN_PROC_NAME
       AND TAB_LEVEL = P_IN_TAB_LEVEL
       AND STEP_NO = P_IN_STEP_NO
       AND BEGIN_TIME = P_IN_BEGIN_TIME
       AND WORKDATE = P_IN_WORK_DATE;
    COMMIT;
  END ;
 $$;


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: b03_ts_remetrade; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE b03_ts_remetrade (
    c_fundcode character varying(500) NOT NULL,
    c_fundname character varying(4000),
    c_fundacco character varying(30),
    f_netvalue numeric(16,2),
    c_agencyname character varying(4000),
    c_custname character varying(4000),
    d_date character varying(100),
    d_cdate character varying(100),
    f_confirmbalance numeric(16,2),
    f_tradefare numeric(16,2),
    f_confirmshares numeric(16,2),
    f_relbalance numeric(16,2),
    f_interest numeric(16,2),
    info character varying(500),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_fundcode) to GROUP default_group;


--
-- Name: b03_ts_remetrade_bak; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE b03_ts_remetrade_bak (
    c_fundcode character varying(500) NOT NULL,
    c_fundname character varying(4000),
    c_fundacco character varying(30),
    f_netvalue numeric(16,2),
    c_agencyname character varying(4000),
    c_custname character varying(4000),
    d_date character varying(100),
    d_cdate character varying(100),
    f_confirmbalance numeric(16,2),
    f_tradefare numeric(16,2),
    f_confirmshares numeric(16,2),
    f_relbalance numeric(16,2),
    f_interest numeric(16,2),
    info character varying(500),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_fundcode) to GROUP default_group;


--
-- Name: ks0_fund_base_26; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE ks0_fund_base_26 (
    id1 numeric(48,0) NOT NULL,
    acc_cd character varying(500) NOT NULL,
    tdate timestamp(0) without time zone NOT NULL,
    ins_cd character varying(500) NOT NULL,
    cost_price_asset numeric(30,8),
    pcol character varying(50)
)
DISTRIBUTE BY SHARD (id1) to GROUP default_group;

--
-- Name: p; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE p (
    p1 text,
    p2 text
)
DISTRIBUTE BY HASH (p1);


--
-- Name: s017_taccoinfo; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_taccoinfo (
    c_custno character varying(30) NOT NULL,
    c_accounttype character(1),
    c_fundacco character varying(30),
    c_agencyno character(3),
    c_netno character varying(30),
    c_childnetno character varying(30),
    d_opendate timestamp(0) without time zone,
    d_lastmodify timestamp(0) without time zone,
    c_accostatus character(1),
    c_freezecause character(1),
    d_backdate timestamp(0) without time zone,
    l_changetime numeric(10,0),
    d_firstinvest timestamp(0) without time zone,
    c_password character varying(100),
    c_bourseflag character(1),
    c_operator character varying(100),
    jy_custid numeric(10,0),
    work_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_custno) to GROUP default_group;


--
-- Name: s017_tacconet; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tacconet (
    c_fundacco character varying(30) NOT NULL,
    c_agencyno character varying(6),
    c_netno character varying(30),
    c_tradeacco character varying(100),
    c_openflag character varying(2),
    c_bonustype character varying(2),
    c_bankno character varying(500),
    c_bankacco character varying(500),
    c_nameinbank character varying(1000),
    d_appenddate timestamp(0) without time zone,
    c_childnetno character varying(30),
    c_tradeaccobak character varying(100),
    c_bankname character varying(500),
    c_banklinecode character varying(100),
    c_channelbankno character varying(30),
    c_bankprovincecode character varying(30),
    c_bankcityno character varying(30),
    sys_id character varying(10),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_fundacco) to GROUP default_group;


--
-- Name: s017_tagencyinfo; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tagencyinfo (
    c_agencyno character varying(6) NOT NULL,
    c_agencyname character varying(1000),
    c_fullname character varying(1000),
    c_agncyaddress character varying(500),
    c_agncyzipcode character varying(30),
    c_agncycontact character varying(30),
    c_agncyphone character varying(100),
    c_agncyfaxno character varying(100),
    c_agncymail character varying(100),
    c_agncybankno character varying(24),
    c_agncybankacco character varying(100),
    c_agncybankname character varying(500),
    d_agncyregdate timestamp(0) without time zone,
    c_agncystatus character varying(2),
    d_lastdate timestamp(0) without time zone,
    c_agencytype character varying(2),
    c_detail character varying(2),
    c_right character varying(2),
    c_zdcode character varying(30),
    l_liquidateredeem numeric(10,0),
    l_liquidateallot numeric(10,0),
    l_liquidatebonus numeric(10,0),
    l_liquidatesub numeric(10,0),
    c_sharetypes character varying(30),
    f_agio numeric(5,4),
    c_ztgonestep character varying(2),
    c_preassign character varying(2),
    l_cserialno numeric(10,0),
    c_comparetype character varying(2),
    c_liquidatetype character varying(2),
    c_multitradeacco character varying(2),
    c_iversion character varying(6),
    c_imode character varying(2),
    c_changeonstep character varying(2),
    f_outagio numeric(5,4),
    f_agiohint numeric(5,4),
    f_outagiohint numeric(5,4),
    c_allotliqtype character varying(2),
    c_redeemliqtype character varying(2),
    c_centerflag character varying(2),
    c_netno character varying(6),
    c_littledealtype character varying(2),
    c_overtimedeal character varying(2),
    d_lastinputtime timestamp(0) without time zone,
    f_interestrate numeric(5,4),
    c_clearsite character varying(2),
    c_isdeal character varying(2),
    c_agencyenglishname character varying(100),
    l_fundaccono numeric(10,0),
    c_rationflag character varying(2),
    c_splitflag character varying(2),
    c_tacode character varying(30),
    c_outdataflag character varying(2),
    c_hasindex character varying(2),
    c_transferbyadjust character varying(2),
    c_sharedetailexptype character varying(2),
    c_navexptype character varying(2),
    c_ecdmode character varying(2),
    c_agencytypedetail character varying(2),
    c_advanceshrconfirm character varying(2),
    c_ecdversion character varying(2),
    c_capmode character varying(2),
    c_internetplatform character varying(2),
    c_capautoarrive character varying(2),
    c_outcapitaldata character varying(30),
    c_ecdcheckmode character varying(30),
    c_ecddealmode character varying(30),
    c_fileimpmode character varying(30),
    c_isotc character varying(2),
    c_enableecd character varying(30),
    c_autoaccotype character varying(30),
    c_tncheckmode numeric(10,0),
    c_captureidinfo character varying(30),
    c_realfreeze character varying(30),
    sys_id character varying(10),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_agencyno) to GROUP default_group;


--
-- Name: s017_tconfirm_all; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tconfirm_all (
    c_businflag character(2) NOT NULL,
    d_cdate timestamp(0) without time zone,
    c_cserialno character varying(100),
    d_date timestamp(0) without time zone,
    l_serialno numeric(10,0),
    c_agencyno character(3),
    c_netno character varying(30),
    c_fundacco character varying(30),
    c_tradeacco character varying(100),
    c_fundcode character varying(30),
    c_sharetype character(1),
    f_confirmbalance numeric(16,2),
    f_confirmshares numeric(16,2),
    f_tradefare numeric(16,2),
    f_tafare numeric(16,2),
    f_stamptax numeric(16,2),
    f_backfare numeric(16,2),
    f_otherfare1 numeric(16,2),
    f_interest numeric(16,2),
    f_interesttax numeric(16,2),
    f_totalfare numeric(16,2),
    f_agencyfare numeric(16,2),
    f_netvalue numeric(12,4),
    f_frozenbalance numeric(16,2),
    f_unfrozenbalance numeric(16,2),
    c_status character(1),
    c_cause character varying(100),
    c_taflag character(1),
    c_custtype character(1),
    c_custno character varying(30),
    f_gainbalance numeric(16,2),
    f_orifare numeric(16,2),
    c_requestendflag character(1),
    f_unbalance numeric(16,2),
    f_unshares numeric(16,2),
    c_reserve character varying(500),
    f_interestshare numeric(16,2),
    f_chincome numeric(16,2),
    f_chshare numeric(16,2),
    f_confirmincome numeric(16,2),
    f_oritradefare numeric(16,2),
    f_oritafare numeric(16,2),
    f_oribackfare numeric(16,2),
    f_oriotherfare1 numeric(16,2),
    c_requestno character varying(100),
    f_balance numeric(16,2),
    f_shares numeric(16,2),
    f_agio numeric(5,4),
    f_lastshares numeric(16,2),
    f_lastfreezeshare numeric(16,2),
    c_othercode character varying(30),
    c_otheracco character varying(30),
    c_otheragency character(3),
    c_othernetno character varying(30),
    c_bonustype character(1),
    c_foriginalno character varying(500),
    c_exceedflag character(1),
    c_childnetno character varying(30),
    c_othershare character(1),
    c_actcode character(3),
    c_acceptmode character(1),
    c_freezecause character(1),
    c_freezeenddate character varying(100),
    f_totalbalance numeric(16,2),
    f_totalshares numeric(16,2),
    c_outbusinflag character(3),
    c_protocolno character varying(30),
    c_memo character varying(500),
    f_registfare numeric(16,2),
    f_fundfare numeric(16,2),
    f_oriagio numeric(5,4),
    c_shareclass character(1),
    d_cisdate timestamp(0) without time zone,
    c_bourseflag character(1),
    c_fundtype character(1),
    f_backfareagio numeric(5,4),
    c_bankno character varying(30),
    c_subfundmethod character varying(30),
    c_combcode character varying(30),
    f_returnfare numeric(16,2),
    c_contractno character varying(100),
    c_captype character(1),
    l_contractserialno numeric(10,0),
    l_othercontractserialno numeric(10,0),
    d_exportdate timestamp(0) without time zone,
    f_transferfee numeric(16,2),
    f_oriconfirmbalance numeric(16,2),
    f_extendnetvalue numeric(23,15),
    l_remitserialno numeric(10,0),
    c_zhxtht character varying(500),
    c_improperredeem character(1),
    f_untradefare numeric(16,2),
    f_untradeinfare numeric(16,2),
    f_untradeoutfare numeric(16,2),
    c_profitnottransfer character(1),
    f_outprofit numeric(9,6),
    f_inprofit numeric(9,6),
    c_totrustcontractid character varying(500),
    d_repurchasedate timestamp(0) without time zone,
    f_chengoutbalance numeric(16,2),
    c_exporting character(1),
    jy_fundid numeric(10,0),
    jy_contractbh character varying(100),
    jy_custid numeric(10,0),
    jy_tocustid numeric(10,0),
    jy_fare numeric(16,2),
    c_trustcontractid character varying(500),
    f_taagencyfare numeric(16,2),
    f_taregisterfare numeric(16,2),
    d_cdate_jy timestamp(0) without time zone,
    jy_adjust character(1),
    jy_subfundid numeric,
    jy_adjust1114 character(1),
    jy_cdate timestamp(0) without time zone,
    c_bankacco character varying(500),
    c_bankname character varying(500),
    c_nameinbank character varying(1000),
    f_riskcapital numeric(16,2),
    f_replenishriskcapital numeric(16,2),
    c_fromfundcode character varying(30),
    c_fromtrustcontractid character varying(500),
    c_trustagencyno character varying(100),
    l_rdmschserialno numeric(10,0),
    f_redeemprofit numeric(16,2),
    f_redeemproyieldrate numeric(13,10),
    d_redeemprobigdate timestamp(0) without time zone,
    d_redeemproenddate timestamp(0) without time zone,
    c_changeownerincomebelong character(1),
    l_midremitserialno numeric(10,0),
    c_fromtype character(1),
    c_iscycinvest character(1),
    l_fromserialno numeric(10,0),
    l_frominterestconserialno numeric(10,0),
    c_changeownerinterest character(1),
    c_msgsendflag character(1),
    l_sharedelaydays numeric(3,0),
    c_istodayconfirm character(1),
    f_newincome numeric(16,2),
    f_floorincome numeric(10,9),
    l_incomeremitserialno numeric(10,0),
    c_isnetting character(1),
    l_bankserialno numeric(10,0),
    c_subfundcode character varying(30),
    f_chengoutsum numeric(16,2),
    f_chengoutprofit numeric(16,2),
    l_confirmtransserialno numeric(10,0),
    c_shareadjustgzexpflag character(1),
    c_issend character(1),
    c_exchangeflag character(1),
    yh_date_1112 timestamp(0) without time zone,
    l_banktocontractserialno numeric(10,0),
    c_payfeetype character(1),
    c_tobankno character varying(30),
    c_tobankacco character varying(500),
    c_tobankname character varying(500),
    c_tonameinbank character varying(1000),
    c_tobanklinecode character varying(100),
    c_tobankprovincecode character varying(30),
    c_tobankcityno character varying(30),
    l_assetseperateno numeric(10,0),
    c_sharecserialno character varying(100),
    c_redeemprincipaltype character(1),
    work_date timestamp(0) without time zone,
    c_businname character varying(100)
)
DISTRIBUTE BY SHARD (c_businflag) to GROUP default_group;


--
-- Name: s017_tdividenddetail; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tdividenddetail (
    d_cdate timestamp(0) without time zone NOT NULL,
    c_cserialno character varying(100),
    d_regdate timestamp(0) without time zone,
    d_date timestamp(0) without time zone,
    c_fundacco character varying(30),
    c_tradeacco character varying(100),
    c_fundcode character varying(30),
    c_sharetype character varying(2),
    c_agencyno character varying(6),
    c_netno character varying(30),
    f_totalshare numeric(16,2),
    f_unitprofit numeric(7,4),
    f_totalprofit numeric(16,2),
    f_tax numeric(16,2),
    c_flag character varying(2),
    f_realbalance numeric(16,2),
    f_reinvestbalance numeric(16,2),
    f_realshares numeric(16,2),
    f_fare numeric(16,2),
    d_lastdate timestamp(0) without time zone,
    f_netvalue numeric(7,4),
    f_frozenbalance numeric(16,2),
    f_frozenshares numeric(16,2),
    f_incometax numeric(9,4),
    c_reserve character varying(100),
    d_requestdate timestamp(0) without time zone,
    c_shareclass character varying(30),
    l_contractserialno numeric(10,0),
    l_specprjserialno numeric(10,0),
    f_investadvisorratio numeric(9,8),
    f_transferfee numeric(16,2),
    l_profitserialno numeric(10,0),
    d_exportdate timestamp(0) without time zone,
    c_custid character varying(30),
    jy_fundid numeric,
    jy_subfundid numeric,
    jy_custid numeric,
    jy_contractbh character varying(100),
    jy_profitsn numeric,
    jy_profitmoney numeric,
    jy_capitalmoney numeric,
    jy_adjust character varying(2),
    c_reinvestnetvalue character varying(2),
    f_transferbalance numeric(16,2),
    l_relatedserialno numeric(10,0),
    c_printoperator character varying(100),
    c_printauditor character varying(100),
    sys_id character varying(10),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone,
    f_remainshares numeric(16,2)
)
DISTRIBUTE BY SHARD (d_cdate) to GROUP default_group;


--
-- Name: s017_tfundday; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tfundday (
    d_date timestamp(0) without time zone,
    d_cdate timestamp(0) without time zone,
    c_fundcode varchar2(30),
    c_todaystatus varchar2(2),
    c_status varchar2(2),
    f_netvalue numeric(7,4),
    f_lastshares numeric(16,2),
    f_lastasset numeric(16,2),
    f_asucceed numeric(16,2),
    f_rsucceed numeric(16,2),
    c_vastflag varchar2(2),
    f_encashratio numeric(9,8),
    f_changeratio numeric(9,8),
    c_excessflag varchar2(2),
    f_subscriberatio numeric(9,8),
    c_inputpersonnel varchar2(100),
    c_checkpersonnel varchar2(100),
    f_income numeric(16,2),
    f_incomeratio numeric(9,6),
    f_unassign numeric(16,2),
    f_incomeunit numeric(10,5),
    f_totalnetvalue numeric(7,4),
    f_servicefare numeric(16,2),
    f_assign numeric(16,2),
    f_growthrate numeric(9,8),
    c_netvalueflag varchar2(2),
    f_managefare numeric(16,2),
    d_exportdate timestamp(0) without time zone,
    c_flag varchar2(2),
    f_advisorfee numeric(16,2),
    d_auditdate timestamp(0) without time zone,
    f_extendnetvalue numeric(23,15),
    f_extendtotalnetvalue numeric(23,15),
    jy_fundcode varchar2(30),
    f_yearincomeratio numeric(9,6),
    f_riskcapital numeric(16,2),
    f_totalincome numeric(16,2),
    f_agencyexpyearincomeration numeric(9,6),
    f_agencyexpincomeunit numeric(10,5),
    f_agencyexpincomeration numeric(9,6),
    f_agencyexpincome numeric(16,2),
    c_isspecflag varchar2(2),
    c_isasync varchar2(2),
    sys_id varchar2(10),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone DEFAULT sysdate
)
DISTRIBUTE BY HASH (d_date);


--
-- Name: s017_tfundinfo; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tfundinfo (
    c_fundcode character varying(30) NOT NULL,
    c_fundname character varying(1000),
    c_moneytype character varying(6),
    c_managername character varying(100),
    c_trusteecode character varying(30),
    f_parvalue numeric(7,4),
    f_issueprice numeric(12,4),
    c_trusteeacco character varying(100),
    d_issuedate timestamp(0) without time zone,
    d_setupdate timestamp(0) without time zone,
    f_maxbala numeric(16,2),
    f_maxshares numeric(16,2),
    f_minbala numeric(16,2),
    f_minshares numeric(16,2),
    l_elimitday numeric(10,0),
    l_slimitday numeric(10,0),
    l_alimitday numeric(10,0),
    l_mincount numeric(10,0),
    l_climitday numeric(10,0),
    f_maxallot numeric(9,8),
    f_maxredeem numeric(9,8),
    c_fundcharacter character varying(500),
    c_fundstatus character varying(2),
    c_subscribemode character varying(2),
    l_timelimit numeric(10,0),
    l_subscribeunit numeric(10,0),
    c_sharetypes character varying(30),
    c_issuetype character varying(2),
    f_factcollect numeric(16,2),
    d_failuedate timestamp(0) without time zone,
    f_allotratio numeric(9,8),
    c_feeratiotype1 character varying(2),
    c_feeratiotype2 character varying(2),
    c_feetype character varying(2),
    c_exceedpart character varying(2),
    c_bonustype character varying(2),
    c_forceredeem character varying(2),
    c_interestdealtype character varying(2),
    f_redeemfareratio numeric(5,4),
    f_changefareratio numeric(5,4),
    f_managerfee numeric(7,6),
    f_right numeric(5,4),
    c_property character varying(2),
    d_evendate timestamp(0) without time zone,
    f_totalbonus numeric(7,4),
    c_changefree character varying(2),
    c_reportcode character varying(30),
    c_backfarecal character varying(2),
    l_moneydate numeric(10,0),
    l_netprecision numeric(10,0),
    c_corpuscontent character varying(2),
    f_corpusratio numeric(5,4),
    c_farecaltype character varying(2),
    l_liquidateallot numeric(10,0),
    l_liquidateredeem numeric(10,0),
    l_liquidatebonus numeric(10,0),
    l_taspecialacco numeric(10,0),
    c_fareprecision character varying(2),
    d_issueenddate timestamp(0) without time zone,
    c_farebelongasset character varying(2),
    l_liquidatechange numeric(10,0),
    l_liquidatefail numeric(10,0),
    l_liquidateend numeric(10,0),
    c_sharedetail character varying(2),
    c_trusteebankname character varying(500),
    c_boursetradeflag character varying(2),
    c_fundenglishname character varying(100),
    l_bankaccono numeric(10,0),
    c_cleanflag character varying(2),
    c_precision character varying(2),
    c_upgradeflag character varying(2),
    c_isdeal character varying(2),
    c_farecltprecision character varying(2),
    c_balanceprecision character varying(2),
    c_shareprecision character varying(2),
    c_bonusprecision character varying(2),
    c_interestprecision character varying(2),
    f_maxallotasset numeric(16,2),
    f_maxallotshares numeric(16,2),
    c_foreigntrustee character varying(6),
    l_tnconfirm numeric(3,0),
    c_rationallotstatus character varying(2),
    f_trusteefee numeric(7,6),
    c_fundacco character varying(30),
    c_financetype character varying(2),
    l_liquidatechangein numeric(10,0),
    c_custname character varying(500),
    c_identitytype character varying(2),
    c_custtype character varying(2),
    c_identityno character varying(100),
    c_deductschemecode character varying(30),
    c_customermanager character varying(30),
    c_templateid character varying(30),
    f_pr0 numeric(7,4),
    f_deductratio numeric(5,4),
    c_farecalculatetype character varying(2),
    c_saletype character varying(2),
    l_maxcount numeric(10,0),
    l_zhallotliqdays numeric(10,0),
    l_zhredeemliqdays numeric(10,0),
    f_liqasset numeric(16,2),
    l_zhallotexpdays numeric(10,0),
    l_zhredeemexpdays numeric(10,0),
    c_limitmode character varying(2),
    c_ordermode character varying(2),
    c_acntlmtdealmode character varying(2),
    l_informdays numeric(2,0),
    c_allowpartredeem character varying(2),
    c_fundendmode character varying(2),
    f_fundendagio numeric(10,9),
    c_minbalalimitisconfirm character varying(2),
    c_gradetype character varying(2),
    c_qryfreqtype character varying(2),
    l_qrydaysltd numeric(2,0),
    d_contractenddate timestamp(0) without time zone,
    c_useinopenday character varying(2),
    c_allotcalinterst character varying(2),
    c_fundrisk character varying(2),
    c_exitallot character varying(2),
    c_subinterestcalc character varying(2),
    c_earlyexitredfee character varying(2),
    c_navexpfqy character varying(2),
    l_navexpday numeric(10,0),
    c_isbounded character varying(2),
    c_earlyexitfeecalc character varying(2),
    c_designdptid character varying(100),
    c_fixeddividway character varying(2),
    c_trusttype character varying(2),
    f_maxnaturalmoney numeric(16,2),
    c_projectid character varying(30),
    c_trustclass character varying(2),
    f_trustscale numeric(16,2),
    c_structflag character varying(2),
    c_priconveyflag character varying(2),
    c_repurchasetype character varying(2),
    c_iswholerepurchase character varying(2),
    f_repurchaseminbala numeric(16,2),
    c_repurchasemainbody character varying(2),
    c_canelyrepurchase character varying(2),
    c_earlybacktime character varying(2),
    c_repurchaseprice character varying(2),
    c_premiumpaymenttime character varying(2),
    c_liquisource character varying(2),
    l_period numeric(3,0),
    c_canextensionflag character varying(2),
    c_canelyliquidflag character varying(2),
    c_trustassetdesc character varying(100),
    c_returnside character varying(2),
    c_returnpaymentway character varying(2),
    c_returnbase character varying(2),
    c_refepaymentway character varying(2),
    c_refeside character varying(2),
    c_refebase character varying(2),
    f_warnline numeric(5,4),
    f_stopline numeric(5,4),
    f_collectinterest numeric(11,8),
    f_durationinterest numeric(7,4),
    f_investadvisorratio numeric(7,6),
    c_bonusschema character varying(2),
    c_guaranteetype character varying(2),
    c_guaranteedesc character varying(100),
    c_expectedyieldtype character varying(2),
    f_minexpectedyield numeric(12,4),
    f_maxexpectedyield numeric(12,4),
    c_incomecycletype character varying(2),
    f_incomecyclevalue numeric(10,0),
    c_subaccotype character varying(2),
    c_allotaccotype character varying(2),
    c_fundtype character varying(2),
    c_cootype character varying(1000),
    c_projecttype character varying(2),
    c_investdirection character varying(30),
    c_investdirectionfractionize character varying(2),
    c_industrydetail character varying(1000),
    c_initeresttype character varying(2),
    c_isextended character varying(2),
    d_extenddate timestamp(0) without time zone,
    c_dealmanagetype character varying(2),
    c_investarea character varying(2),
    c_projectcode character varying(1000),
    c_fundshortname character varying(500),
    c_contractid character varying(500),
    c_functype character varying(2),
    c_specialbusintype character varying(1000),
    c_investindustry character varying(2),
    c_managetype character varying(2),
    c_area character varying(500),
    c_risk character varying(2),
    c_iscommitteedisscuss character varying(2),
    c_structtype character varying(2),
    c_commendplace character varying(2),
    l_npmaxcount numeric(5,0),
    c_client character varying(100),
    c_clientcusttype character varying(2),
    c_clientidtype character varying(2),
    c_clientidno character varying(100),
    c_clientbankname character varying(100),
    c_clientaccono character varying(100),
    c_clientaddress character varying(500),
    c_clientzipcode character varying(30),
    c_clientphoneno1 character varying(100),
    c_clientphoneno2 character varying(100),
    c_clientfax character varying(100),
    c_beneficiary character varying(100),
    c_collectbankname character varying(500),
    c_collectbankno character varying(6),
    c_collectaccountname character varying(500),
    c_collectbankacco character varying(100),
    c_keeperbankname character varying(500),
    c_keeperaccountname character varying(500),
    c_keeperaccountno character varying(100),
    c_keepername character varying(500),
    c_keepercorporation character varying(500),
    c_keeperaddress character varying(500),
    c_keeperzipcode character varying(30),
    c_keeperphoneno1 character varying(100),
    c_keeperphoneno2 character varying(100),
    c_keeperfax character varying(100),
    c_incomedistributetype character varying(2),
    c_alarmline character varying(1000),
    c_stoplossline character varying(1000),
    f_investadvisorfee numeric(12,2),
    c_investadvisordeduct character varying(1000),
    c_capitalacco character varying(500),
    c_stockacconame character varying(500),
    c_stocksalesdept character varying(500),
    c_thirdpartybankno character varying(6),
    c_thirdpartybankname character varying(500),
    c_thirdpartyacconame character varying(500),
    c_thirdpartyaccono character varying(100),
    c_investadvisor character varying(500),
    c_investadvisorbankno character varying(6),
    c_investadvisorbankname character varying(500),
    c_investadvisoracconame character varying(500),
    c_investadvisoraccono character varying(100),
    c_investadvisorcorporation character varying(500),
    c_investadvisoraddress character varying(500),
    c_investadvisorzipcode character varying(30),
    c_investadvisorphoneno1 character varying(100),
    c_investadvisorphoneno2 character varying(100),
    c_investadvisorfax character varying(100),
    c_authdelegate character varying(100),
    c_loanfinanceparty character varying(500),
    c_loanfinancepartycorporation character varying(500),
    c_loanfinancepartyaddress character varying(500),
    c_loanfinancepartyzipcode character varying(30),
    c_loanfinancepartyphoneno1 character varying(100),
    c_loanfinancepartyphoneno2 character varying(100),
    c_loanfinancepartyfax character varying(100),
    c_loaninteresttype character varying(2),
    f_loaninterestrate numeric(7,4),
    f_loanduration numeric(5,0),
    c_loanmanagebank character varying(500),
    f_loanmanagefee numeric(9,2),
    f_loanfinancecost numeric(9,2),
    f_creditattornduration numeric(5,0),
    f_creditattorninterestduration numeric(7,4),
    f_creditattornprice numeric(12,2),
    f_billattornduration numeric(5,0),
    f_billattorninterestduration numeric(7,4),
    f_billattornprice numeric(12,2),
    c_stkincfincparty character varying(1000),
    c_stkincfincpartycorporation character varying(500),
    c_stkincfincpartyaddress character varying(500),
    c_stkincfincpartyzipcode character varying(30),
    c_stkincfincpartyphoneno1 character varying(100),
    c_stkincfincpartyphoneno2 character varying(100),
    c_stkincfincpartyfax character varying(100),
    c_stkincincomeannualizedrate numeric(7,4),
    c_stkincinteresttype character varying(2),
    f_stkincattornprice numeric(12,2),
    f_stkincattornduration numeric(5,0),
    f_stkincbail numeric(12,2),
    f_stkincfinccost numeric(9,2),
    c_stkincmemo1 character varying(1000),
    c_stkincmemo2 character varying(1000),
    c_debtincfincparty character varying(500),
    c_debtincfincpartycorporation character varying(500),
    c_debtincfincpartyaddress character varying(500),
    c_debtincfincpartyzipcode character varying(30),
    c_debtincfincpartyphoneno1 character varying(100),
    c_debtincfincpartyphoneno2 character varying(100),
    c_debtincfincpartyfax character varying(100),
    c_debtincincomerate numeric(7,4),
    c_debtincinteresttype character varying(2),
    f_debtincattornprice numeric(12,2),
    f_debtincattornduration numeric(5,0),
    f_debtincbail numeric(12,2),
    f_debtincfinccost numeric(9,2),
    c_debtincmemo1 character varying(1000),
    c_othinvfincparty character varying(500),
    c_othinvfincpartycorporation character varying(500),
    c_othinvfincpartyaddress character varying(500),
    c_othinvfincpartyzipcode character varying(30),
    c_othinvfincpartyphoneno1 character varying(100),
    c_othinvfincpartyphoneno2 character varying(100),
    c_othinvfincpartyfax character varying(100),
    f_othinvfinccost numeric(9,2),
    c_othinvmemo1 character varying(1000),
    c_othinvmemo2 character varying(1000),
    c_othinvmemo3 character varying(1000),
    c_banktrustcoobank character varying(500),
    c_banktrustproductname character varying(500),
    c_banktrustproductcode character varying(100),
    c_banktrustundertakingletter character varying(2),
    c_trustgovgovname character varying(500),
    c_trustgovprojecttype character varying(1000),
    c_trustgovcootype character varying(4),
    c_trustgovoptype character varying(4),
    c_housecapital character varying(4),
    c_houseispe character varying(2),
    c_tradetype character varying(2),
    c_businesstype character varying(2),
    c_trustname character varying(500),
    c_trustidtype character varying(2),
    c_trustidno character varying(100),
    d_trustidvaliddate timestamp(0) without time zone,
    c_trustbankname character varying(500),
    c_trustaccounttype character varying(2),
    c_trustnameinbank character varying(100),
    c_zhtrustbankname character varying(500),
    c_zhtrustbankacco character varying(100),
    c_issecmarket character varying(2),
    c_fundoperation character varying(2),
    c_trustmanager character varying(100),
    c_tradeother character varying(4000),
    c_watchdog character varying(500),
    c_memo character varying(1000),
    c_benefittype character varying(2),
    c_redeemaccotype character varying(2),
    c_bonusaccotype character varying(2),
    c_fundendaccotype character varying(2),
    c_collectfailaccotype character varying(2),
    d_lastmodifydate timestamp(0) without time zone,
    c_shareholdlimtype character varying(2),
    c_redeemtimelimtype character varying(2),
    c_isprincipalrepayment character varying(2),
    c_principalrepaymenttype character varying(2),
    l_interestyeardays numeric(3,0),
    l_incomeyeardays numeric(3,0),
    c_capuseprovcode character varying(30),
    c_capusecitycode character varying(30),
    c_capsourceprovcode character varying(30),
    c_banktrustcoobankcode character varying(30),
    c_banktrustisbankcap character varying(2),
    c_trusteefeedesc character varying(4000),
    c_managefeedesc character varying(4000),
    c_investfeedesc character varying(4000),
    f_investadvisordeductratio numeric(7,6),
    c_investdeductdesc character varying(4000),
    c_investadvisor2 character varying(500),
    f_investadvisorratio2 numeric(7,6),
    f_investadvisordeductratio2 numeric(7,6),
    c_investfeedesc2 character varying(4000),
    c_investdeductdesc2 character varying(4000),
    c_investadvisor3 character varying(500),
    f_investadvisorratio3 numeric(7,6),
    f_investadvisordeductratio3 numeric(7,6),
    c_investfeedesc3 character varying(4000),
    c_investdeductdesc3 character varying(4000),
    c_profitclassdesc character varying(4000),
    c_deductratiodesc character varying(4000),
    c_redeemfeedesc character varying(4000),
    l_defaultprecision numeric(10,0),
    c_allotfeeaccotype character varying(2),
    c_isposf character varying(2),
    c_opendaydesc character varying(4000),
    c_actualmanager character varying(100),
    c_subindustrydetail character varying(30),
    c_isbankleading character varying(2),
    c_subprojectcode character varying(500),
    c_iscycleinvest character varying(2),
    f_liquidationinterest numeric(13,10),
    c_liquidationinteresttype character varying(2),
    c_isbonusinvestfare character varying(2),
    c_subfeeaccotype character varying(2),
    c_redeemfeeaccotype character varying(2),
    c_fundrptcode character varying(30),
    c_ordertype character varying(2),
    c_flag character varying(2),
    c_allotliqtype character varying(2),
    l_sharelimitday numeric(5,0),
    c_iseverydayopen character varying(2),
    c_tradebynetvalue character varying(2),
    c_isstage character varying(2),
    c_specbenfitmemo character varying(4000),
    d_effectivedate timestamp(0) without time zone,
    c_issueendflag character varying(2),
    c_resharehasrdmfee character varying(2),
    jy_fundcode numeric,
    jy_fundid numeric,
    jy_subfundid numeric,
    jy_dptid numeric,
    c_iswealth character varying(2),
    c_interestcalctype character varying(2),
    c_allotinterestcalctype character varying(2),
    c_isriskcapital character varying(2),
    c_fundstatus_1225 character varying(2),
    c_isincomeeverydaycalc character varying(2),
    c_isredeemreturninterest character varying(2),
    c_isrefundrtninterest character varying(2),
    d_estimatedsetupdate timestamp(0) without time zone,
    f_estimatedfactcollect numeric(16,2),
    c_isfinancialproducts character varying(2),
    c_fundredeemtype character varying(2),
    c_trademanualinput character varying(2),
    f_clientmanageration numeric(7,6),
    c_profitclassadjustment character varying(2),
    c_mainfundcode character varying(30),
    c_contractsealoff character varying(2),
    c_permitnextperiod character varying(2),
    c_preprofitschematype character varying(2),
    c_fundredeemprofit character varying(2),
    f_incomeration numeric(9,8),
    c_incomecalctype character varying(2),
    c_allocateaccoid character varying(30),
    c_outfundcode character varying(500),
    c_matchprofitclass character varying(30),
    l_lastdays numeric(5,0),
    c_contractprofitflag character varying(2),
    c_agencysaleliqtype character varying(2),
    l_delaydays numeric(3,0),
    c_profitclassperiod character varying(2),
    c_reportshowname character varying(1000),
    c_currencyincometype character varying(2),
    c_beforeredeemcapital character varying(2),
    c_contractversion character varying(30),
    c_confirmacceptedflag character varying(2),
    c_selectcontract character varying(2),
    f_schemainterest numeric(11,8),
    c_riskgrade character varying(30),
    l_sharedelaydays numeric(3,0),
    l_reservationdays numeric(3,0),
    c_transfertype character varying(2),
    c_schemavoluntarily character varying(2),
    l_schemadetaildata numeric(4,0),
    c_schemadetailtype character varying(2),
    c_iscurrencyconfirm character varying(2),
    c_allowmultiaccobank character varying(2),
    d_capverif timestamp(0) without time zone,
    c_templatetype character varying(12),
    c_capitalprecision character varying(2),
    c_fundno character varying(100),
    c_profittype character varying(2),
    d_paydate timestamp(0) without time zone,
    d_shelvedate timestamp(0) without time zone,
    d_offshelvedate timestamp(0) without time zone,
    c_schemabegindatetype character varying(2),
    l_schemabegindatedays numeric(3,0),
    c_isautoredeem character varying(2),
    c_isnettingrequest character varying(2),
    c_issuingquotedtype character varying(2),
    d_firstdistributedate timestamp(0) without time zone,
    c_bonusfrequency character varying(2),
    c_interestbigdatetype character varying(2),
    c_gzdatatype character varying(2),
    f_allotfareratio numeric(5,4),
    f_subfareratio numeric(5,4),
    c_begindatebeyond character varying(2),
    c_profitnotinterest character varying(2),
    c_setuplimittype character varying(2),
    c_limitredeemtype character varying(2),
    c_bonusfrequencytype character varying(2),
    c_rfaccotype character varying(2),
    c_capitalfee character varying(2),
    c_exceedflag character varying(2),
    c_enableecd character varying(2),
    c_isfixedtrade character varying(2),
    c_profitcaltype character varying(2),
    f_ominbala numeric(16,2),
    f_stepbala numeric(16,2),
    c_remittype character varying(30),
    c_interestcycle character varying(30),
    c_repayguaranteecopy character varying(30),
    c_repaytype character varying(30),
    c_fundprofitdes character varying(4000),
    c_fundinfodes character varying(4000),
    c_riskeval character varying(2),
    l_maxage numeric(3,0),
    l_minage numeric(3,0),
    c_fundriskdes character varying(1000),
    mig_l_assetid numeric(48,0),
    l_faincomedays numeric(10,0),
    c_producttype character varying(2),
    c_otherbenefitproducttype character varying(2),
    c_isotc character varying(2),
    c_iseverydayprovision character varying(2),
    c_incometogz character varying(2),
    c_setuptransfundacco character varying(30),
    c_issuefeeownerrequired character varying(2),
    c_calcinterestbeforeallot character varying(30),
    c_islimit300wnature character varying(2),
    c_allowoverflow character varying(30),
    c_trustfundtype character varying(30),
    c_disclose character varying(2),
    c_collectaccoid character varying(30),
    c_isissuebymarket character varying(2),
    c_setupstatus character varying(30),
    c_isentitytrust character varying(2),
    l_liquidatesub numeric(10,0),
    c_incomeassigndesc character varying(4000),
    c_keeporgancode character varying(30),
    d_defaultbegincacldate timestamp(0) without time zone,
    c_zcbborrower character varying(100),
    c_zcbborroweridno character varying(100),
    c_zcbremittype character varying(100),
    c_registcode character varying(100),
    c_redeeminvestaccotype character varying(2),
    c_bonusinvestaccotype character varying(2),
    c_isabsnotopentrade character varying(2),
    l_interestdiffdays numeric(5,0),
    c_outfundstatus character varying(2),
    c_reqsyntype character varying(2),
    c_allredeemtype character varying(2),
    c_isabsopentrade character varying(2),
    c_funddesc character varying(1000),
    l_allotliquidays numeric(3,0),
    l_subliquidays numeric(3,0),
    c_autoupcontractenddaterule character varying(2),
    c_fcsubaccotype character varying(2),
    c_fcallotaccotype character varying(2),
    c_fcredeemaccotype character varying(2),
    c_fcbonusaccotype character varying(2),
    c_captranslimitflag character varying(30),
    c_redeemprincipaltype character varying(2),
    c_interestcalcdealtype character varying(30),
    c_collectconfirm character varying(30),
    d_oldcontractenddate timestamp(0) without time zone,
    c_tnvaluation character varying(30),
    c_contractendnotify character varying(2),
    c_rdmfeebase character varying(30),
    c_exceedcfmratio character varying(30),
    c_allowallotcustlimittype character varying(2),
    c_yeardayscalctype character varying(2),
    c_iscompoundinterest character varying(30),
    c_dbcfm character varying(30),
    c_limitaccountstype character varying(2),
    c_cycleinvestrange character varying(2),
    c_tncheckmode character varying(2),
    c_enableearlyredeem character varying(2),
    c_ispurceandredeemset character varying(30),
    c_perfpaydealtype character varying(2),
    c_allowappend character varying(2),
    c_allowredeem character varying(2),
    c_inputstatus character varying(2),
    c_profitbalanceadjust character varying(2),
    c_profitperiodadjust character varying(2),
    c_autogeneratecontractid character varying(2),
    c_transferneednetting character varying(100),
    underwrite character varying(1000),
    undertook character varying(1000),
    undertake character varying(1000),
    c_issmsend character varying(2),
    d_contractshortenddate timestamp(0) without time zone,
    d_contractlongenddate timestamp(0) without time zone,
    c_assetseperatefundcodesrc character varying(30),
    f_averageprofit numeric(11,8),
    c_currencycontractlimittype character varying(2),
    l_profitlastdays numeric(5,0),
    l_liquidationlastdays numeric(5,0),
    c_arlimitincludeallreq character varying(2),
    c_reqfundchange character varying(2),
    c_dealnetvaluerule character varying(2),
    c_contractdealtype character varying(2),
    c_bonusplanbeginday timestamp(0) without time zone,
    c_contractbalaupright character varying(2),
    c_isneedinterestrate character varying(2),
    c_isneedexcessratio character varying(2),
    c_riskgraderemark character varying(1000),
    c_lossprobability character varying(2),
    c_suitcusttype character varying(2),
    c_createbonusschema character varying(2),
    d_closedenddate timestamp(0) without time zone,
    c_timelimitunit character varying(30),
    c_exceedredeemdealtype character varying(2),
    c_profitperiod character varying(2),
    l_navgetintervaldays numeric(3,0),
    load_date timestamp(0) without time zone,
    sys_id character varying(10) DEFAULT 'S017'::character varying,
    work_date timestamp(0) without time zone,
    c_limittransfertype character varying(1),
    c_transaccotype character varying(1),
    c_incometaxbase character varying(1),
    c_isredeemfareyearcalc character varying(1),
    c_otherbenefitinputmode character varying(1),
    c_aftdefaultinterestdeducttype character varying(1),
    c_allowzerobalanceconfirm character varying(1),
    c_incomejoinassign character varying(1),
    l_liquidateliqbonus numeric(10,0),
    c_predefaultinterestdeducttype character varying(1),
    c_worktype character varying(1),
    c_defaultinterestadduptype character varying(1),
    c_issupportsubmode character varying(1),
    f_expectedyield numeric(14,0),
    c_recodecode character varying(40),
    l_liquidatetransfer numeric(10,0),
    c_ispayincometax character varying(1),
    c_groupmainfundcode character varying(6),
    c_redeemfeesplittype character varying(1),
    c_capitalfromcrmorta character varying(1),
    c_needcalcdefaultinterest character varying(1),
    c_issuercode character varying(10),
    l_redeemfareyeardays numeric(10,0),
    c_floatyield character varying(30),
    l_minriskscore numeric(3,0),
    c_islocalmoneytypecollect character varying(1)
)
DISTRIBUTE BY SHARD (c_fundcode) to GROUP default_group;


--
-- Name: s017_tsharecurrents_all; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tsharecurrents_all (
    d_cdate timestamp(0) without time zone NOT NULL,
    c_cserialno character varying(100),
    c_businflag character(2),
    d_requestdate timestamp(0) without time zone,
    c_requestno character varying(100),
    c_custno character varying(30),
    c_fundacco character varying(30),
    c_tradeacco character varying(100),
    c_fundcode character varying(30),
    c_sharetype character(1),
    c_agencyno character(3),
    c_netno character varying(30),
    f_occurshares numeric(16,2),
    f_occurbalance numeric(16,2),
    f_lastshares numeric(16,2),
    f_occurfreeze numeric(16,2),
    f_lastfreezeshare numeric(16,2),
    c_summary character varying(100),
    f_gainbalance numeric(16,2),
    d_sharevaliddate timestamp(0) without time zone,
    c_bonustype character(1),
    c_custtype character(1),
    c_shareclass character(1),
    c_bourseflag character varying(20),
    d_exportdate timestamp(0) without time zone,
    l_contractserialno numeric(10,0),
    c_issend character(1),
    c_sendbatch character varying(30),
    work_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (d_cdate) to GROUP default_group;


--
-- Name: s017_ttrustclientinfo_all; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_ttrustclientinfo_all (
    c_custno character varying(30) NOT NULL,
    c_custtype character(1),
    c_custname character varying(500),
    c_shortname character varying(500),
    c_helpcode character varying(30),
    c_identitytype character(1),
    c_identityno character varying(500),
    c_zipcode character varying(30),
    c_address character varying(1000),
    c_phone character varying(100),
    c_faxno character varying(500),
    c_mobileno character varying(100),
    c_email character varying(500),
    c_sex character(1),
    c_birthday character varying(30),
    c_vocation character(2),
    c_education character(2),
    c_income character varying(30),
    c_contact character varying(100),
    c_contype character(1),
    c_contno character varying(100),
    c_billsendflag character(1),
    c_callcenter character(1),
    c_internet character(1),
    c_secretcode character varying(30),
    c_nationality character(3),
    c_cityno character varying(30),
    c_lawname character varying(100),
    c_shacco character varying(30),
    c_szacco character varying(30),
    c_broker character varying(100),
    f_agio numeric(5,4),
    c_memo character varying(4000),
    c_reserve character varying(500),
    c_corpname character varying(100),
    c_corptel character varying(100),
    c_specialcode character varying(100),
    c_actcode character varying(30),
    c_billsendpass character(1),
    c_addressinvalid character(1),
    d_appenddate timestamp(0) without time zone,
    d_backdate timestamp(0) without time zone,
    c_invalidaddress character varying(500),
    c_backreason character varying(500),
    c_modifyinfo character(2),
    c_riskcontent character varying(4000),
    l_querydaysltd numeric(3,0),
    c_customermanager character varying(100),
    c_custproperty character(1),
    c_custclass character(1),
    c_custright character varying(4000),
    c_daysltdtype character(1),
    d_idvaliddate timestamp(0) without time zone,
    l_custgroup numeric(10,0),
    c_recommender character varying(100),
    c_recommendertype character(1),
    d_idnovaliddate timestamp(0) without time zone,
    c_organcode character(10),
    c_othercontact character varying(100),
    c_taxregistno character varying(100),
    c_taxidentitytype character(1),
    c_taxidentityno character varying(100),
    d_legalvaliddate timestamp(0) without time zone,
    c_shareholder character varying(500),
    c_shareholderidtype character(1),
    c_shareholderidno character varying(100),
    d_holderidvaliddate timestamp(0) without time zone,
    c_leader character varying(500),
    c_leaderidtype character(1),
    c_leaderidno character varying(100),
    d_leadervaliddate timestamp(0) without time zone,
    c_managercode character varying(100),
    c_linemanager character varying(100),
    c_clientinfoid character varying(30),
    c_provincecode character varying(30),
    c_countytown character varying(1000),
    c_phone2 character varying(100),
    c_clienttype character(1),
    c_agencyno character(3),
    c_industrydetail character varying(30),
    c_isqualifiedcust character(1),
    c_industryidentityno character varying(100),
    c_lawidentitytype character(1),
    c_lawidentityno character varying(100),
    d_lawidvaliddate timestamp(0) without time zone,
    d_conidvaliddate timestamp(0) without time zone,
    c_conisrevmsg character(1),
    c_conmobileno character varying(100),
    c_conmoaddress character varying(1000),
    c_conzipcode character varying(30),
    c_conphone1 character varying(100),
    c_conphone2 character varying(100),
    c_conemail character varying(100),
    c_confaxno character varying(500),
    c_incomsource character varying(500),
    c_zhidentityno character varying(500),
    c_zhidentitytype character(1),
    c_eastcusttype character varying(30),
    jy_custid numeric(10,0),
    c_idtype201201030 character(1),
    c_emcontact character varying(500),
    c_emcontactphone character varying(100),
    c_instiregaddr character varying(1000),
    c_regcusttype character varying(30),
    c_riskgrade character varying(30),
    c_riskgraderemark character varying(1000),
    d_idvaliddatebeg timestamp(0) without time zone,
    d_industryidvaliddatebeg timestamp(0) without time zone,
    d_industryidvaliddate timestamp(0) without time zone,
    c_incomesourceotherdesc character varying(1000),
    c_vocationotherdesc character varying(1000),
    c_businscope character varying(4000),
    d_conidvaliddatebeg timestamp(0) without time zone,
    d_lawidvaliddatebeg timestamp(0) without time zone,
    c_regmoneytype character(3),
    f_regcapital numeric(15,2),
    c_orgtype character(2),
    c_contrholderno character varying(100),
    c_contrholdername character varying(500),
    c_contrholderidtype character(2),
    c_contrholderidno character varying(500),
    d_contrholderidvalidatebeg timestamp(0) without time zone,
    d_contrholderidvalidate timestamp(0) without time zone,
    c_responpername character varying(500),
    c_responperidtype character(2),
    c_responperidno character varying(500),
    d_responperidvalidatebeg timestamp(0) without time zone,
    d_responperidvalidate timestamp(0) without time zone,
    c_lawphone character varying(100),
    c_contrholderphone character varying(100),
    c_responperphone character varying(100),
    c_consex character(1),
    c_conrelative character varying(500),
    l_riskserialno numeric(10,0),
    c_convocation character(2),
    c_iscustrelated character(1),
    c_businlicissuorgan character varying(500),
    c_manageridno character varying(500),
    c_manageridtype character varying(500),
    c_managername character varying(500),
    d_companyregdate timestamp(0) without time zone,
    c_electronicagreement character(1),
    c_householdregno character varying(500),
    c_guardianrela character varying(500),
    c_guardianname character varying(500),
    c_guardianidtype character(1),
    c_guardianidno character varying(500),
    c_isfranchisingidstry character(1),
    c_franchidstrybusinlic character varying(500),
    c_workunittype character(2),
    c_normalresidaddr character varying(1000),
    c_domicile character varying(1000),
    c_finainvestyears character(2),
    c_parentidtype character(1),
    c_parentidno character varying(500),
    c_videono character varying(1000),
    c_bonustype character(1),
    d_retirementdate timestamp(0) without time zone,
    c_issendbigcustbill character(1),
    c_idaddress character varying(1000),
    c_isproinvestor character(1),
    c_sendkfflag character(1),
    c_sendkfcause character varying(1000),
    c_sendsaflag character(1),
    c_sendsacause character varying(1000),
    c_custrelationchannel character(1),
    c_companytype character(1),
    c_businlocation character varying(1000),
    c_custodian character varying(500),
    d_elecsigndate timestamp(0) without time zone,
    d_riskinputdate timestamp(0) without time zone,
    c_circno character varying(1000),
    c_financeindustrydetail character varying(30),
    c_outclientinfoid character varying(30),
    d_duediligencedate timestamp(0) without time zone,
    c_duediligencestatus character(1),
    c_inputstatus character(1),
    c_address2 character varying(1000),
    c_reportcusttype character(1),
    c_reportcusttypedetail character varying(30),
    c_custsource character varying(30),
    work_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_custno) to GROUP default_group;


--
-- Name: sys_stat_error_log; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE sys_stat_error_log (
    proc_name varchar2(50) NOT NULL,
    tab_level varchar2(20),
    step_no varchar2(20),
    step_desc varchar2(500),
    begin_time timestamp(0) without time zone,
    end_time timestamp(0) without time zone,
    workdate timestamp(0) without time zone,
    row_num numeric,
    elapsed numeric,
    all_elapsed numeric,
    sql_code varchar2(20),
    sql_errm varchar2(500)
)
DISTRIBUTE BY SHARD (proc_name) to GROUP default_group;


--
-- Data for Name: b03_ts_remetrade; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY b03_ts_remetrade (c_fundcode, c_fundname, c_fundacco, f_netvalue, c_agencyname, c_custname, d_date, d_cdate, f_confirmbalance, f_tradefare, f_confirmshares, f_relbalance, f_interest, info, work_date, load_date) FROM stdin;
\.


--
-- Data for Name: b03_ts_remetrade_bak; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY b03_ts_remetrade_bak (c_fundcode, c_fundname, c_fundacco, f_netvalue, c_agencyname, c_custname, d_date, d_cdate, f_confirmbalance, f_tradefare, f_confirmshares, f_relbalance, f_interest, info, work_date, load_date) FROM stdin;
\.


--
-- Data for Name: ks0_fund_base_26; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY ks0_fund_base_26 (id1, acc_cd, tdate, ins_cd, cost_price_asset, pcol) FROM stdin;
\.


--
-- Data for Name: p; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY p (p1, p2) FROM stdin;
2021-12-12	2021-12-12
2021-12-13	2021-12-12
2020-12-13	2021-12-12
\.


--
-- Data for Name: s017_taccoinfo; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_taccoinfo (c_custno, c_accounttype, c_fundacco, c_agencyno, c_netno, c_childnetno, d_opendate, d_lastmodify, c_accostatus, c_freezecause, d_backdate, l_changetime, d_firstinvest, c_password, c_bourseflag, c_operator, jy_custid, work_date) FROM stdin;
\.


--
-- Data for Name: s017_tacconet; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tacconet (c_fundacco, c_agencyno, c_netno, c_tradeacco, c_openflag, c_bonustype, c_bankno, c_bankacco, c_nameinbank, d_appenddate, c_childnetno, c_tradeaccobak, c_bankname, c_banklinecode, c_channelbankno, c_bankprovincecode, c_bankcityno, sys_id, work_date, load_date) FROM stdin;
\.


--
-- Data for Name: s017_tagencyinfo; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tagencyinfo (c_agencyno, c_agencyname, c_fullname, c_agncyaddress, c_agncyzipcode, c_agncycontact, c_agncyphone, c_agncyfaxno, c_agncymail, c_agncybankno, c_agncybankacco, c_agncybankname, d_agncyregdate, c_agncystatus, d_lastdate, c_agencytype, c_detail, c_right, c_zdcode, l_liquidateredeem, l_liquidateallot, l_liquidatebonus, l_liquidatesub, c_sharetypes, f_agio, c_ztgonestep, c_preassign, l_cserialno, c_comparetype, c_liquidatetype, c_multitradeacco, c_iversion, c_imode, c_changeonstep, f_outagio, f_agiohint, f_outagiohint, c_allotliqtype, c_redeemliqtype, c_centerflag, c_netno, c_littledealtype, c_overtimedeal, d_lastinputtime, f_interestrate, c_clearsite, c_isdeal, c_agencyenglishname, l_fundaccono, c_rationflag, c_splitflag, c_tacode, c_outdataflag, c_hasindex, c_transferbyadjust, c_sharedetailexptype, c_navexptype, c_ecdmode, c_agencytypedetail, c_advanceshrconfirm, c_ecdversion, c_capmode, c_internetplatform, c_capautoarrive, c_outcapitaldata, c_ecdcheckmode, c_ecddealmode, c_fileimpmode, c_isotc, c_enableecd, c_autoaccotype, c_tncheckmode, c_captureidinfo, c_realfreeze, sys_id, work_date, load_date) FROM stdin;
1	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: s017_tconfirm_all; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tconfirm_all (c_businflag, d_cdate, c_cserialno, d_date, l_serialno, c_agencyno, c_netno, c_fundacco, c_tradeacco, c_fundcode, c_sharetype, f_confirmbalance, f_confirmshares, f_tradefare, f_tafare, f_stamptax, f_backfare, f_otherfare1, f_interest, f_interesttax, f_totalfare, f_agencyfare, f_netvalue, f_frozenbalance, f_unfrozenbalance, c_status, c_cause, c_taflag, c_custtype, c_custno, f_gainbalance, f_orifare, c_requestendflag, f_unbalance, f_unshares, c_reserve, f_interestshare, f_chincome, f_chshare, f_confirmincome, f_oritradefare, f_oritafare, f_oribackfare, f_oriotherfare1, c_requestno, f_balance, f_shares, f_agio, f_lastshares, f_lastfreezeshare, c_othercode, c_otheracco, c_otheragency, c_othernetno, c_bonustype, c_foriginalno, c_exceedflag, c_childnetno, c_othershare, c_actcode, c_acceptmode, c_freezecause, c_freezeenddate, f_totalbalance, f_totalshares, c_outbusinflag, c_protocolno, c_memo, f_registfare, f_fundfare, f_oriagio, c_shareclass, d_cisdate, c_bourseflag, c_fundtype, f_backfareagio, c_bankno, c_subfundmethod, c_combcode, f_returnfare, c_contractno, c_captype, l_contractserialno, l_othercontractserialno, d_exportdate, f_transferfee, f_oriconfirmbalance, f_extendnetvalue, l_remitserialno, c_zhxtht, c_improperredeem, f_untradefare, f_untradeinfare, f_untradeoutfare, c_profitnottransfer, f_outprofit, f_inprofit, c_totrustcontractid, d_repurchasedate, f_chengoutbalance, c_exporting, jy_fundid, jy_contractbh, jy_custid, jy_tocustid, jy_fare, c_trustcontractid, f_taagencyfare, f_taregisterfare, d_cdate_jy, jy_adjust, jy_subfundid, jy_adjust1114, jy_cdate, c_bankacco, c_bankname, c_nameinbank, f_riskcapital, f_replenishriskcapital, c_fromfundcode, c_fromtrustcontractid, c_trustagencyno, l_rdmschserialno, f_redeemprofit, f_redeemproyieldrate, d_redeemprobigdate, d_redeemproenddate, c_changeownerincomebelong, l_midremitserialno, c_fromtype, c_iscycinvest, l_fromserialno, l_frominterestconserialno, c_changeownerinterest, c_msgsendflag, l_sharedelaydays, c_istodayconfirm, f_newincome, f_floorincome, l_incomeremitserialno, c_isnetting, l_bankserialno, c_subfundcode, f_chengoutsum, f_chengoutprofit, l_confirmtransserialno, c_shareadjustgzexpflag, c_issend, c_exchangeflag, yh_date_1112, l_banktocontractserialno, c_payfeetype, c_tobankno, c_tobankacco, c_tobankname, c_tonameinbank, c_tobanklinecode, c_tobankprovincecode, c_tobankcityno, l_assetseperateno, c_sharecserialno, c_redeemprincipaltype, work_date, c_businname) FROM stdin;
1 	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: s017_tdividenddetail; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tdividenddetail (d_cdate, c_cserialno, d_regdate, d_date, c_fundacco, c_tradeacco, c_fundcode, c_sharetype, c_agencyno, c_netno, f_totalshare, f_unitprofit, f_totalprofit, f_tax, c_flag, f_realbalance, f_reinvestbalance, f_realshares, f_fare, d_lastdate, f_netvalue, f_frozenbalance, f_frozenshares, f_incometax, c_reserve, d_requestdate, c_shareclass, l_contractserialno, l_specprjserialno, f_investadvisorratio, f_transferfee, l_profitserialno, d_exportdate, c_custid, jy_fundid, jy_subfundid, jy_custid, jy_contractbh, jy_profitsn, jy_profitmoney, jy_capitalmoney, jy_adjust, c_reinvestnetvalue, f_transferbalance, l_relatedserialno, c_printoperator, c_printauditor, sys_id, work_date, load_date, f_remainshares) FROM stdin;
2021-04-26 20:34:00	\N	\N	\N	\N	\N	2	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: s017_tfundday; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tfundday (d_date, d_cdate, c_fundcode, c_todaystatus, c_status, f_netvalue, f_lastshares, f_lastasset, f_asucceed, f_rsucceed, c_vastflag, f_encashratio, f_changeratio, c_excessflag, f_subscriberatio, c_inputpersonnel, c_checkpersonnel, f_income, f_incomeratio, f_unassign, f_incomeunit, f_totalnetvalue, f_servicefare, f_assign, f_growthrate, c_netvalueflag, f_managefare, d_exportdate, c_flag, f_advisorfee, d_auditdate, f_extendnetvalue, f_extendtotalnetvalue, jy_fundcode, f_yearincomeratio, f_riskcapital, f_totalincome, f_agencyexpyearincomeration, f_agencyexpincomeunit, f_agencyexpincomeration, f_agencyexpincome, c_isspecflag, c_isasync, sys_id, work_date, load_date) FROM stdin;
\.


--
-- Data for Name: s017_tfundinfo; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tfundinfo (c_fundcode, c_fundname, c_moneytype, c_managername, c_trusteecode, f_parvalue, f_issueprice, c_trusteeacco, d_issuedate, d_setupdate, f_maxbala, f_maxshares, f_minbala, f_minshares, l_elimitday, l_slimitday, l_alimitday, l_mincount, l_climitday, f_maxallot, f_maxredeem, c_fundcharacter, c_fundstatus, c_subscribemode, l_timelimit, l_subscribeunit, c_sharetypes, c_issuetype, f_factcollect, d_failuedate, f_allotratio, c_feeratiotype1, c_feeratiotype2, c_feetype, c_exceedpart, c_bonustype, c_forceredeem, c_interestdealtype, f_redeemfareratio, f_changefareratio, f_managerfee, f_right, c_property, d_evendate, f_totalbonus, c_changefree, c_reportcode, c_backfarecal, l_moneydate, l_netprecision, c_corpuscontent, f_corpusratio, c_farecaltype, l_liquidateallot, l_liquidateredeem, l_liquidatebonus, l_taspecialacco, c_fareprecision, d_issueenddate, c_farebelongasset, l_liquidatechange, l_liquidatefail, l_liquidateend, c_sharedetail, c_trusteebankname, c_boursetradeflag, c_fundenglishname, l_bankaccono, c_cleanflag, c_precision, c_upgradeflag, c_isdeal, c_farecltprecision, c_balanceprecision, c_shareprecision, c_bonusprecision, c_interestprecision, f_maxallotasset, f_maxallotshares, c_foreigntrustee, l_tnconfirm, c_rationallotstatus, f_trusteefee, c_fundacco, c_financetype, l_liquidatechangein, c_custname, c_identitytype, c_custtype, c_identityno, c_deductschemecode, c_customermanager, c_templateid, f_pr0, f_deductratio, c_farecalculatetype, c_saletype, l_maxcount, l_zhallotliqdays, l_zhredeemliqdays, f_liqasset, l_zhallotexpdays, l_zhredeemexpdays, c_limitmode, c_ordermode, c_acntlmtdealmode, l_informdays, c_allowpartredeem, c_fundendmode, f_fundendagio, c_minbalalimitisconfirm, c_gradetype, c_qryfreqtype, l_qrydaysltd, d_contractenddate, c_useinopenday, c_allotcalinterst, c_fundrisk, c_exitallot, c_subinterestcalc, c_earlyexitredfee, c_navexpfqy, l_navexpday, c_isbounded, c_earlyexitfeecalc, c_designdptid, c_fixeddividway, c_trusttype, f_maxnaturalmoney, c_projectid, c_trustclass, f_trustscale, c_structflag, c_priconveyflag, c_repurchasetype, c_iswholerepurchase, f_repurchaseminbala, c_repurchasemainbody, c_canelyrepurchase, c_earlybacktime, c_repurchaseprice, c_premiumpaymenttime, c_liquisource, l_period, c_canextensionflag, c_canelyliquidflag, c_trustassetdesc, c_returnside, c_returnpaymentway, c_returnbase, c_refepaymentway, c_refeside, c_refebase, f_warnline, f_stopline, f_collectinterest, f_durationinterest, f_investadvisorratio, c_bonusschema, c_guaranteetype, c_guaranteedesc, c_expectedyieldtype, f_minexpectedyield, f_maxexpectedyield, c_incomecycletype, f_incomecyclevalue, c_subaccotype, c_allotaccotype, c_fundtype, c_cootype, c_projecttype, c_investdirection, c_investdirectionfractionize, c_industrydetail, c_initeresttype, c_isextended, d_extenddate, c_dealmanagetype, c_investarea, c_projectcode, c_fundshortname, c_contractid, c_functype, c_specialbusintype, c_investindustry, c_managetype, c_area, c_risk, c_iscommitteedisscuss, c_structtype, c_commendplace, l_npmaxcount, c_client, c_clientcusttype, c_clientidtype, c_clientidno, c_clientbankname, c_clientaccono, c_clientaddress, c_clientzipcode, c_clientphoneno1, c_clientphoneno2, c_clientfax, c_beneficiary, c_collectbankname, c_collectbankno, c_collectaccountname, c_collectbankacco, c_keeperbankname, c_keeperaccountname, c_keeperaccountno, c_keepername, c_keepercorporation, c_keeperaddress, c_keeperzipcode, c_keeperphoneno1, c_keeperphoneno2, c_keeperfax, c_incomedistributetype, c_alarmline, c_stoplossline, f_investadvisorfee, c_investadvisordeduct, c_capitalacco, c_stockacconame, c_stocksalesdept, c_thirdpartybankno, c_thirdpartybankname, c_thirdpartyacconame, c_thirdpartyaccono, c_investadvisor, c_investadvisorbankno, c_investadvisorbankname, c_investadvisoracconame, c_investadvisoraccono, c_investadvisorcorporation, c_investadvisoraddress, c_investadvisorzipcode, c_investadvisorphoneno1, c_investadvisorphoneno2, c_investadvisorfax, c_authdelegate, c_loanfinanceparty, c_loanfinancepartycorporation, c_loanfinancepartyaddress, c_loanfinancepartyzipcode, c_loanfinancepartyphoneno1, c_loanfinancepartyphoneno2, c_loanfinancepartyfax, c_loaninteresttype, f_loaninterestrate, f_loanduration, c_loanmanagebank, f_loanmanagefee, f_loanfinancecost, f_creditattornduration, f_creditattorninterestduration, f_creditattornprice, f_billattornduration, f_billattorninterestduration, f_billattornprice, c_stkincfincparty, c_stkincfincpartycorporation, c_stkincfincpartyaddress, c_stkincfincpartyzipcode, c_stkincfincpartyphoneno1, c_stkincfincpartyphoneno2, c_stkincfincpartyfax, c_stkincincomeannualizedrate, c_stkincinteresttype, f_stkincattornprice, f_stkincattornduration, f_stkincbail, f_stkincfinccost, c_stkincmemo1, c_stkincmemo2, c_debtincfincparty, c_debtincfincpartycorporation, c_debtincfincpartyaddress, c_debtincfincpartyzipcode, c_debtincfincpartyphoneno1, c_debtincfincpartyphoneno2, c_debtincfincpartyfax, c_debtincincomerate, c_debtincinteresttype, f_debtincattornprice, f_debtincattornduration, f_debtincbail, f_debtincfinccost, c_debtincmemo1, c_othinvfincparty, c_othinvfincpartycorporation, c_othinvfincpartyaddress, c_othinvfincpartyzipcode, c_othinvfincpartyphoneno1, c_othinvfincpartyphoneno2, c_othinvfincpartyfax, f_othinvfinccost, c_othinvmemo1, c_othinvmemo2, c_othinvmemo3, c_banktrustcoobank, c_banktrustproductname, c_banktrustproductcode, c_banktrustundertakingletter, c_trustgovgovname, c_trustgovprojecttype, c_trustgovcootype, c_trustgovoptype, c_housecapital, c_houseispe, c_tradetype, c_businesstype, c_trustname, c_trustidtype, c_trustidno, d_trustidvaliddate, c_trustbankname, c_trustaccounttype, c_trustnameinbank, c_zhtrustbankname, c_zhtrustbankacco, c_issecmarket, c_fundoperation, c_trustmanager, c_tradeother, c_watchdog, c_memo, c_benefittype, c_redeemaccotype, c_bonusaccotype, c_fundendaccotype, c_collectfailaccotype, d_lastmodifydate, c_shareholdlimtype, c_redeemtimelimtype, c_isprincipalrepayment, c_principalrepaymenttype, l_interestyeardays, l_incomeyeardays, c_capuseprovcode, c_capusecitycode, c_capsourceprovcode, c_banktrustcoobankcode, c_banktrustisbankcap, c_trusteefeedesc, c_managefeedesc, c_investfeedesc, f_investadvisordeductratio, c_investdeductdesc, c_investadvisor2, f_investadvisorratio2, f_investadvisordeductratio2, c_investfeedesc2, c_investdeductdesc2, c_investadvisor3, f_investadvisorratio3, f_investadvisordeductratio3, c_investfeedesc3, c_investdeductdesc3, c_profitclassdesc, c_deductratiodesc, c_redeemfeedesc, l_defaultprecision, c_allotfeeaccotype, c_isposf, c_opendaydesc, c_actualmanager, c_subindustrydetail, c_isbankleading, c_subprojectcode, c_iscycleinvest, f_liquidationinterest, c_liquidationinteresttype, c_isbonusinvestfare, c_subfeeaccotype, c_redeemfeeaccotype, c_fundrptcode, c_ordertype, c_flag, c_allotliqtype, l_sharelimitday, c_iseverydayopen, c_tradebynetvalue, c_isstage, c_specbenfitmemo, d_effectivedate, c_issueendflag, c_resharehasrdmfee, jy_fundcode, jy_fundid, jy_subfundid, jy_dptid, c_iswealth, c_interestcalctype, c_allotinterestcalctype, c_isriskcapital, c_fundstatus_1225, c_isincomeeverydaycalc, c_isredeemreturninterest, c_isrefundrtninterest, d_estimatedsetupdate, f_estimatedfactcollect, c_isfinancialproducts, c_fundredeemtype, c_trademanualinput, f_clientmanageration, c_profitclassadjustment, c_mainfundcode, c_contractsealoff, c_permitnextperiod, c_preprofitschematype, c_fundredeemprofit, f_incomeration, c_incomecalctype, c_allocateaccoid, c_outfundcode, c_matchprofitclass, l_lastdays, c_contractprofitflag, c_agencysaleliqtype, l_delaydays, c_profitclassperiod, c_reportshowname, c_currencyincometype, c_beforeredeemcapital, c_contractversion, c_confirmacceptedflag, c_selectcontract, f_schemainterest, c_riskgrade, l_sharedelaydays, l_reservationdays, c_transfertype, c_schemavoluntarily, l_schemadetaildata, c_schemadetailtype, c_iscurrencyconfirm, c_allowmultiaccobank, d_capverif, c_templatetype, c_capitalprecision, c_fundno, c_profittype, d_paydate, d_shelvedate, d_offshelvedate, c_schemabegindatetype, l_schemabegindatedays, c_isautoredeem, c_isnettingrequest, c_issuingquotedtype, d_firstdistributedate, c_bonusfrequency, c_interestbigdatetype, c_gzdatatype, f_allotfareratio, f_subfareratio, c_begindatebeyond, c_profitnotinterest, c_setuplimittype, c_limitredeemtype, c_bonusfrequencytype, c_rfaccotype, c_capitalfee, c_exceedflag, c_enableecd, c_isfixedtrade, c_profitcaltype, f_ominbala, f_stepbala, c_remittype, c_interestcycle, c_repayguaranteecopy, c_repaytype, c_fundprofitdes, c_fundinfodes, c_riskeval, l_maxage, l_minage, c_fundriskdes, mig_l_assetid, l_faincomedays, c_producttype, c_otherbenefitproducttype, c_isotc, c_iseverydayprovision, c_incometogz, c_setuptransfundacco, c_issuefeeownerrequired, c_calcinterestbeforeallot, c_islimit300wnature, c_allowoverflow, c_trustfundtype, c_disclose, c_collectaccoid, c_isissuebymarket, c_setupstatus, c_isentitytrust, l_liquidatesub, c_incomeassigndesc, c_keeporgancode, d_defaultbegincacldate, c_zcbborrower, c_zcbborroweridno, c_zcbremittype, c_registcode, c_redeeminvestaccotype, c_bonusinvestaccotype, c_isabsnotopentrade, l_interestdiffdays, c_outfundstatus, c_reqsyntype, c_allredeemtype, c_isabsopentrade, c_funddesc, l_allotliquidays, l_subliquidays, c_autoupcontractenddaterule, c_fcsubaccotype, c_fcallotaccotype, c_fcredeemaccotype, c_fcbonusaccotype, c_captranslimitflag, c_redeemprincipaltype, c_interestcalcdealtype, c_collectconfirm, d_oldcontractenddate, c_tnvaluation, c_contractendnotify, c_rdmfeebase, c_exceedcfmratio, c_allowallotcustlimittype, c_yeardayscalctype, c_iscompoundinterest, c_dbcfm, c_limitaccountstype, c_cycleinvestrange, c_tncheckmode, c_enableearlyredeem, c_ispurceandredeemset, c_perfpaydealtype, c_allowappend, c_allowredeem, c_inputstatus, c_profitbalanceadjust, c_profitperiodadjust, c_autogeneratecontractid, c_transferneednetting, underwrite, undertook, undertake, c_issmsend, d_contractshortenddate, d_contractlongenddate, c_assetseperatefundcodesrc, f_averageprofit, c_currencycontractlimittype, l_profitlastdays, l_liquidationlastdays, c_arlimitincludeallreq, c_reqfundchange, c_dealnetvaluerule, c_contractdealtype, c_bonusplanbeginday, c_contractbalaupright, c_isneedinterestrate, c_isneedexcessratio, c_riskgraderemark, c_lossprobability, c_suitcusttype, c_createbonusschema, d_closedenddate, c_timelimitunit, c_exceedredeemdealtype, c_profitperiod, l_navgetintervaldays, load_date, sys_id, work_date, c_limittransfertype, c_transaccotype, c_incometaxbase, c_isredeemfareyearcalc, c_otherbenefitinputmode, c_aftdefaultinterestdeducttype, c_allowzerobalanceconfirm, c_incomejoinassign, l_liquidateliqbonus, c_predefaultinterestdeducttype, c_worktype, c_defaultinterestadduptype, c_issupportsubmode, f_expectedyield, c_recodecode, l_liquidatetransfer, c_ispayincometax, c_groupmainfundcode, c_redeemfeesplittype, c_capitalfromcrmorta, c_needcalcdefaultinterest, c_issuercode, l_redeemfareyeardays, c_floatyield, l_minriskscore, c_islocalmoneytypecollect) FROM stdin;
2	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	S017	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: s017_tsharecurrents_all; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tsharecurrents_all (d_cdate, c_cserialno, c_businflag, d_requestdate, c_requestno, c_custno, c_fundacco, c_tradeacco, c_fundcode, c_sharetype, c_agencyno, c_netno, f_occurshares, f_occurbalance, f_lastshares, f_occurfreeze, f_lastfreezeshare, c_summary, f_gainbalance, d_sharevaliddate, c_bonustype, c_custtype, c_shareclass, c_bourseflag, d_exportdate, l_contractserialno, c_issend, c_sendbatch, work_date) FROM stdin;
\.


--
-- Data for Name: s017_ttrustclientinfo_all; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_ttrustclientinfo_all (c_custno, c_custtype, c_custname, c_shortname, c_helpcode, c_identitytype, c_identityno, c_zipcode, c_address, c_phone, c_faxno, c_mobileno, c_email, c_sex, c_birthday, c_vocation, c_education, c_income, c_contact, c_contype, c_contno, c_billsendflag, c_callcenter, c_internet, c_secretcode, c_nationality, c_cityno, c_lawname, c_shacco, c_szacco, c_broker, f_agio, c_memo, c_reserve, c_corpname, c_corptel, c_specialcode, c_actcode, c_billsendpass, c_addressinvalid, d_appenddate, d_backdate, c_invalidaddress, c_backreason, c_modifyinfo, c_riskcontent, l_querydaysltd, c_customermanager, c_custproperty, c_custclass, c_custright, c_daysltdtype, d_idvaliddate, l_custgroup, c_recommender, c_recommendertype, d_idnovaliddate, c_organcode, c_othercontact, c_taxregistno, c_taxidentitytype, c_taxidentityno, d_legalvaliddate, c_shareholder, c_shareholderidtype, c_shareholderidno, d_holderidvaliddate, c_leader, c_leaderidtype, c_leaderidno, d_leadervaliddate, c_managercode, c_linemanager, c_clientinfoid, c_provincecode, c_countytown, c_phone2, c_clienttype, c_agencyno, c_industrydetail, c_isqualifiedcust, c_industryidentityno, c_lawidentitytype, c_lawidentityno, d_lawidvaliddate, d_conidvaliddate, c_conisrevmsg, c_conmobileno, c_conmoaddress, c_conzipcode, c_conphone1, c_conphone2, c_conemail, c_confaxno, c_incomsource, c_zhidentityno, c_zhidentitytype, c_eastcusttype, jy_custid, c_idtype201201030, c_emcontact, c_emcontactphone, c_instiregaddr, c_regcusttype, c_riskgrade, c_riskgraderemark, d_idvaliddatebeg, d_industryidvaliddatebeg, d_industryidvaliddate, c_incomesourceotherdesc, c_vocationotherdesc, c_businscope, d_conidvaliddatebeg, d_lawidvaliddatebeg, c_regmoneytype, f_regcapital, c_orgtype, c_contrholderno, c_contrholdername, c_contrholderidtype, c_contrholderidno, d_contrholderidvalidatebeg, d_contrholderidvalidate, c_responpername, c_responperidtype, c_responperidno, d_responperidvalidatebeg, d_responperidvalidate, c_lawphone, c_contrholderphone, c_responperphone, c_consex, c_conrelative, l_riskserialno, c_convocation, c_iscustrelated, c_businlicissuorgan, c_manageridno, c_manageridtype, c_managername, d_companyregdate, c_electronicagreement, c_householdregno, c_guardianrela, c_guardianname, c_guardianidtype, c_guardianidno, c_isfranchisingidstry, c_franchidstrybusinlic, c_workunittype, c_normalresidaddr, c_domicile, c_finainvestyears, c_parentidtype, c_parentidno, c_videono, c_bonustype, d_retirementdate, c_issendbigcustbill, c_idaddress, c_isproinvestor, c_sendkfflag, c_sendkfcause, c_sendsaflag, c_sendsacause, c_custrelationchannel, c_companytype, c_businlocation, c_custodian, d_elecsigndate, d_riskinputdate, c_circno, c_financeindustrydetail, c_outclientinfoid, d_duediligencedate, c_duediligencestatus, c_inputstatus, c_address2, c_reportcusttype, c_reportcusttypedetail, c_custsource, work_date) FROM stdin;
\.


--
-- Data for Name: sys_stat_error_log; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY sys_stat_error_log (proc_name, tab_level, step_no, step_desc, begin_time, end_time, workdate, row_num, elapsed, all_elapsed, sql_code, sql_errm) FROM stdin;
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
\.


--
-- Name: ks0_fund_base_26 pk_ks0_fund_base_26; Type: CONSTRAINT; Schema: sync; Owner: gregsun
--

ALTER TABLE ONLY ks0_fund_base_26
    ADD CONSTRAINT pk_ks0_fund_base_26 PRIMARY KEY (id1, acc_cd, ins_cd);


--
-- PostgreSQL database dump complete
--

create table newtab as 
    SELECT A.C_FUNDCODE,
           A.C_FUNDNAME,
           A.C_FUNDACCO,
           A.F_NETVALUE,
           A.C_AGENCYNAME,
           A.C_CUSTNAME,
           A.D_DATE,
           A.D_CDATE,
           A.F_CONFIRMBALANCE,
           A.F_TRADEFARE,
           A.F_CONFIRMSHARES,
           ABS(NVL(B.F_OCCURBALANCE, A.F_RELBALANCE)) F_RELBALANCE,
           A.F_INTEREST,
           NVL(DECODE(B.C_BUSINFLAG,
                      '02',
                      '申购',
                      '50',
                      '申购',
                      '74',
                      '申购',
                      '03',
                      '赎回'),
               DECODE(A.C_BUSINFLAG,
                      '01',
                      '认购',
                      '02',
                      '申购',
                      '03',
                      '赎回',
                      '53',
                      '强制赎回',
                      '50',
                      '产品成立')) AS INFO,
           null,
           SYSDATE AS LOAD_DATE
      FROM (SELECT A.C_FUNDCODE,
                   C.C_FUNDNAME,
                   A.C_FUNDACCO,
                   FUNC_GETLASTNETVALUE(A.C_FUNDCODE, A.D_CDATE::date) F_NETVALUE,
                   (SELECT C_AGENCYNAME
                      FROM S017_TAGENCYINFO
                     WHERE A.C_AGENCYNO = C_AGENCYNO) C_AGENCYNAME,
                   B.C_CUSTNAME,
                   TO_CHAR(A.D_DATE, 'yyyy-mm-dd') D_DATE,
                   TO_CHAR(A.D_CDATE, 'yyyy-mm-dd') D_CDATE,
                   DECODE(A.C_BUSINFLAG,
                          '03',
                          A.F_CONFIRMBALANCE + A.F_TRADEFARE,
                          '53',
                          A.F_CONFIRMBALANCE + A.F_TRADEFARE,
                          A.F_CONFIRMBALANCE) F_CONFIRMBALANCE,
                   A.F_TRADEFARE,
                   A.F_CONFIRMSHARES,
                   DECODE(A.C_BUSINFLAG,
                          '03',
                          A.F_CONFIRMBALANCE,
                          '53',
                          A.F_CONFIRMBALANCE,
                          A.F_CONFIRMBALANCE - A.F_TRADEFARE) F_RELBALANCE,
                   A.F_INTEREST,
                   A.C_BUSINFLAG,
                   A.C_CSERIALNO
              FROM (SELECT D_DATE,
                           C_AGENCYNO,
                           DECODE(C_BUSINFLAG,
                                  '03',
                                  DECODE(C_IMPROPERREDEEM,
                                         '3',
                                         '100',
                                         '5',
                                         '100',
                                         C_BUSINFLAG),
                                  C_BUSINFLAG) C_BUSINFLAG,
                           C_FUNDACCO,
                           D_CDATE,
                           C_FUNDCODE,
                           F_CONFIRMBALANCE,
                           F_CONFIRMSHARES,
                           C_REQUESTNO,
                           F_TRADEFARE,
                           C_TRADEACCO,
                           F_INTEREST,
                           C_CSERIALNO,
                           L_SERIALNO,
                           L_CONTRACTSERIALNO
                      FROM S017_TCONFIRM_ALL T3
                    UNION
                    SELECT D_DATE,
                           C_AGENCYNO,
                           '02' C_BUSINFLAG,
                           C_FUNDACCO,
                           D_LASTDATE AS D_CDATE, 
                           C_FUNDCODE,
                           F_REINVESTBALANCE F_CONFIRMBALANCE,
                           F_REALSHARES F_CONFIRMSHARES,
                           '' C_REQUESTNO,
                           0 F_TRADEFARE,
                           C_TRADEACCO,
                           0 F_INTEREST,
                           C_CSERIALNO,
                           0 L_SERIALNO,
                           L_CONTRACTSERIALNO
                      FROM S017_TDIVIDENDDETAIL T1
                     /*WHERE T1.C_FLAG = '0'*/) A
              LEFT JOIN S017_TACCONET TACN
                ON A.C_TRADEACCO = TACN.C_TRADEACCO
              LEFT JOIN (SELECT * FROM S017_TACCOINFO WHERE C_ACCOUNTTYPE = 'A') X
                ON A.C_FUNDACCO = X.C_FUNDACCO
              LEFT JOIN S017_TTRUSTCLIENTINFO_ALL B
                ON X.C_CUSTNO = B.C_CUSTNO
             INNER JOIN S017_TFUNDINFO C
                ON A.C_FUNDCODE = C.C_FUNDCODE
                       ) A
      LEFT JOIN (SELECT ST1.D_CDATE,
                        ST1.C_FUNDCODE,
                        ST1.F_OCCURBALANCE,
                        ST1.C_BUSINFLAG,
                        ST1.C_FUNDACCO,
                        ST1.C_CSERIALNO
                   FROM S017_TSHARECURRENTS_ALL ST1
                 -- WHERE ST1.C_BUSINFLAG <> '74'
                 UNION ALL
                 SELECT ST2.D_DATE AS D_CDATE,
                        ST2.C_FUNDCODE,
                        ST2.F_TOTALPROFIT AS F_OCCURBALANCE,
                        '74' AS C_BUSINFLAG,
                        ST2.C_FUNDACCO,
                        ST2.C_CSERIALNO
                   FROM S017_TDIVIDENDDETAIL ST2
              --    WHERE ST2.C_FLAG = '0'
				  ) B
        ON A.C_FUNDCODE = B.C_FUNDCODE
		/*
       AND A.C_FUNDACCO = B.C_FUNDACCO
       AND TO_DATE(A.D_CDATE, 'YYYY-MM-DD') = B.D_CDATE
       AND A.C_CSERIALNO = B.C_CSERIALNO*/;

DROP SCHEMA sync cascade;

--
-- support issue commit/rollback in plpgsql with exception.
--
\c
create table t_exception(a int, b text);

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin          
  insert into t_exception values(a_id,a_nc);
  commit;
  exception when others then
    get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
    raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;  
end; $$;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin          
  insert into t_exception values(a_id,a_nc);
  rollback;
  exception when others then
    get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
    raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;  
end; $$;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
  	begin
  	  raise notice 'exp1';
  	  insert into t_exception values(a_id,a_nc);
  	  commit;
  	exception when others then
  	  raise notice 'exp2 %', v_message_text;
  	end;
end; $$;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
  	begin
  	  raise notice 'exp1';
  	  insert into t_exception values(a_id,a_nc);
  	  commit;
  	  insert into t_exception values(a_id,a_nc);
  	  rollback;
  	  raise exception 'check';
  	exception when others then
  	  raise notice 'exp2 %', v_message_text;
  	end;
end; $$;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

-- nest call
create or replace procedure exp1(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
  	begin
  	  raise notice 'exp1';
  	  insert into t_exception values(a_id,a_nc);
  	  commit;
  	  insert into t_exception values(a_id,a_nc);
  	  rollback;
  	  raise exception 'check';
  	exception when others then
  	  raise notice 'exp2 %', v_message_text;
  	end;
end; $$;
create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
    call exp1(a_id, a_nc);
end; $$;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  insert into t_exception values(a_id,a_nc);
  commit;
  insert into t_exception values(a_id,a_nc);
  rollback;
  insert into t_exception values(a_id,a_nc);
  commit;
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
    call exp1(a_id, a_nc);
end; $$;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  insert into t_exception values(a_id,a_nc);
  commit;
  insert into t_exception values(a_id,a_nc);
  rollback;
  insert into t_exception values(a_id,a_nc);
  commit;
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
    call exp1(a_id, a_nc);
end; $$;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

\c postgres
create table t_exception(a int, b text);

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin          
  insert into t_exception values(a_id,a_nc);
  commit;
  exception when others then
    get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
    raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;  
end; $$ language default_plsql;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin          
  insert into t_exception values(a_id,a_nc);
  rollback;
  exception when others then
    get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
    raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;  
end; $$ language default_plsql;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
  	begin
  	  raise notice 'exp1';
  	  insert into t_exception values(a_id,a_nc);
  	  commit;
  	exception when others then
  	  raise notice 'exp2 %', v_message_text;
  	end;
end; $$ language default_plsql;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
  	begin
  	  raise notice 'exp1';
  	  insert into t_exception values(a_id,a_nc);
  	  commit;
  	  insert into t_exception values(a_id,a_nc);
  	  rollback;
  	  raise exception 'check';
  	exception when others then
  	  raise notice 'exp2 %', v_message_text;
  	end;
end; $$ language default_plsql;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

-- nest call
create or replace procedure exp1(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
  	begin
  	  raise notice 'exp1';
  	  insert into t_exception values(a_id,a_nc);
  	  commit;
  	  insert into t_exception values(a_id,a_nc);
  	  rollback;
  	  raise exception 'check';
  	exception when others then
  	  raise notice 'exp2 %', v_message_text;
  	end;
end; $$ language default_plsql;
create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
    call exp1(a_id, a_nc);
end; $$ language default_plsql;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  insert into t_exception values(a_id,a_nc);
  commit;
  insert into t_exception values(a_id,a_nc);
  rollback;
  insert into t_exception values(a_id,a_nc);
  commit;
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
    call exp1(a_id, a_nc);
end; $$ language default_plsql;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

create or replace procedure p_exception_error(a_id integer,a_nc text) as
$$   
declare   
  v_sqlstate text;
  v_context text;
  v_message_text text;
begin
  insert into t_exception values(a_id,a_nc);
  commit;
  insert into t_exception values(a_id,a_nc);
  rollback;
  insert into t_exception values(a_id,a_nc);
  commit;
  raise exception 'start';
  exception when others then
  	get stacked diagnostics v_sqlstate = returned_sqlstate,
                               v_message_text = message_text,
                               v_context = pg_exception_context;    
  	raise notice 'code: % msg: % stat: %',v_sqlstate ,v_message_text,v_context;
    call exp1(a_id, a_nc);
end; $$ language default_plsql;
delete from t_exception;
call p_exception_error(1,'2');
select * from t_exception;

\c regression_ora
-- package can be created use non-superuser
create user PL_SU with superuser;
create user PL_U1;
\c regression_ora PL_U1
create or replace package pk is
procedure f();
end;
/
create or replace package body pk is
procedure f() is begin null; end;
end;
/

-- 
create or replace function f1(a int, b out int) return varchar is
begin
  b = 10;
  return 'ok';
end;
/

declare
a varchar;
b int;
begin
  a = f1(1, b);
  raise notice 'a=% b=%', a, b;
end;
/

create or replace function f2(a varchar, b out varchar, c out varchar) return int is
begin
  b = 'b -'||a;
  c = 'c -'||a;
  return 1;
end;
/

declare
a varchar;
b varchar;
c varchar;
begin
  a = f2('aa', b, c);
  raise notice 'a=% b=% c=%', a, b, c;
end;
/

declare
a varchar;
b varchar;
c varchar;
begin
  a = f2('aa', b, c);
  raise notice 'a=% b=% c=%', a, b, c;
end;
/

create or replace function f3(a int, b out int) return int is
begin
  b = 10;
  return b+a;
end;
/

declare
a varchar;
b varchar;
c varchar;
d int;
begin
  a = 10 + f2('aa', b, c) + f3(1, d);
  raise notice 'a=% b=% c=% d=%', a, b, c, d;
end;
/

-- call in procedure
create or replace procedure out_proc(a int) is
a varchar;
b varchar;
c varchar;
d int;
begin
  a = 10 + f2('aa', b, c) + f3(1, d);
  raise notice 'a=% b=% c=% d=%', a, b, c, d;
end;
/

call out_proc(1);

-- nested call func
create or replace function out_func_n(a int) return int is
a varchar;
b varchar;
c varchar;
d int;
begin
  a = 10 + f2('aa', b, c) + f3(1, d);
  raise notice 'a=% b=% c=% d=%', a, b, c, d;
  return 1;
end;
/

select out_func_n(1);


-- compatible, old behavior does not change.

create or replace function foff(a int, b out int, c out int) as $$ begin
b = 10;
c = 20;
end;
$$ language default_plsql;
select foff(1); -- failed
select foff(1,1,1); -- ok
DO
$$
declare
 r text;
 b int;
 c int;
begin
 r = foff(1,b,c);
 raise notice '%', r;
end; $$ language default_plsql;
select foff(1,1,1);
DO
$$
declare
 r text;
 b int;
 c int;
begin
 r = foff(1,b,c);
 raise notice '%', r;
end; $$ language default_plsql;

drop function f1;
drop function f2;
drop function f3;
drop function out_func_n;
drop procedure out_proc;
drop function foff(int, int, int);

-- 1020421696089122981
-- function support commit
create table comm_t (a int);
create or replace function fcommit(a int) return int is
begin
  insert into comm_t values(1);
  commit;
  raise notice 'fcommit';
  return 1;
end;
/

-- simple case
declare
a int;
begin
  a = fcommit(1) + fcommit(1);
end;
/
select * from comm_t;
delete from comm_t;

-- nested call
create or replace function fcommit2(a int) return int is
declare
  a int;
begin
  a = fcommit(1);
  raise notice 'fcommit2';
  return 1;
end;
/

create or replace procedure pcomm(a int) is
declare
 a int;
begin
  a = fcommit2(a);
  raise notice 'pcomm';
end;
/

declare
a int;
begin
  call pcomm(a);
end;
/
select * from comm_t;
delete from comm_t;
call pcomm(1);
select * from comm_t;
delete from comm_t;

-- mulit commit and rollback
create or replace function fcommit(a int) return int is
begin
  insert into comm_t values(1);
  commit;
  raise notice 'fcommit';
  insert into comm_t values(2);
  rollback;
  insert into comm_t values(3);
  commit;
  return 1;
end;
/

declare
a int;
begin
a = fcommit(1);
end;
/
select * from comm_t order by 1;
delete from comm_t;

-- recursive case
create or replace function rec1(a int) return int is
res int;
begin
  insert into comm_t values(1);
  commit;
  if a > 0 then
    res = rec1(a - 1);
  end if;
  return 1;
end;
/

declare
a int;
begin
a = rec1(10);
end;
/
select * from comm_t;
delete from comm_t;

-- call -> select -> funct
declare
a int;
begin
perform rec1(20);
end;
/

-- select SQL
select rec1(20);

-- BEGIN..END
begin;
call pcomm(1);
end;

-- exception
drop table if exists t1;
create table t1 (a int, b varchar);
create or replace function f1(id int,name varchar(20)) returns int as 
$$
begin
  insert into t1 values (1,'ddd');
  commit;
  return 1;
exception when others then
  raise notice '1';
end;
$$;

delete from t1;
declare
a text;
begin
a = f1(1, 'aa');
end;
/
select * from t1;
drop function f1;

create or replace function f1(id int,name varchar(20)) returns int as 
$$
begin
  insert into t1 values (1,'ddd');
  commit;
  id = 1/0;
  return 1;
exception when others then
  raise notice '1';
  return 2;
end;
$$;

delete from t1;
declare
a text;
begin
a = f1(1, 'aa');
end;
/
select * from t1;
drop function f1;

-- with out and commit
create or replace function out_commit(a int, b out int, c out int) return varchar is
begin
  b=10+a;
  c=10+a;
  insert into t1 values(b, c);
  commit;
  b=b+10;
  c=b+10;
  insert into t1 values(b, c);
  commit;
  return 1;
end;
/

delete from t1;
declare
  a text;
  b int;
  c int;
begin
  a = out_commit(1, b, c);
  raise notice 'a=% b=% c=%', a, b, c;
end;
/
select * from t1 order by 1;
drop function out_commit;
-- 1020421696090183667
create or replace procedure t1(names varchar2 default 'abc') as
begin
		     raise notice '%', 'name is default values:'||names;
end;
/
call t1();

-- fix bugs of commit/rollback in function of packages
drop table t1;
create table t1(id int, name text);
create or replace package test1 is
  procedure test;
end test1;
/

create or replace package body test1 is 
  procedure test  is
    aa int;
  begin
    insert into t1(id,name) values (1,'a');
    commit;
  exception when others then
    raise notice 'error';
    rollback;
  end;
end;
/

call test1.test();
select * from t1;

create or replace package opkg is
  procedure otest;
end;
/

create or replace package body opkg is
  procedure otest is
  begin
    call test1.test();
  exception when others then
    raise notice 'error2';
    rollback;
  end;
end;
/

call opkg.otest();
select * from t1;

create or replace procedure pf() as
$$
declare
va pg_class%rowtype; a cursor for select * from pg_class;
begin
open a;
fetch a into va;
insert into t1 values(1, 'a');
commit;
end;
$$;
call pf();
select * from t1;

begin;
call pf();
end;

drop package opkg;
--
drop table t1;
create table t1 (id int check (id <> 45));
create or replace package test1 is
procedure test;
procedure test2;
end;
/
create or replace package body test1 is
procedure test  is
aa int;
begin
insert into t1(id,name) values (1,'a');
commit;
exception when others then
rollback;
end test;

procedure test2 is
begin
for i in 1..100 loop
  insert into t1 values (i);
  if mod(i,10) = 0 then
    commit;
  end if;
end loop;
exception when others then
  raise notice 'error';
  commit;
  return;
return;
end test2;
end test1;
/
delete from t1;
call test1.test2();
select * from t1 order by 1;
drop table t1;

-- 1020421696090538997
select 'a' || old;
select 'a' || old.x;
create or replace function f_trigger_hr_salary_items() returns trigger as 
$$
declare   
   log_content text;   
begin
  if (tg_op = 'update') then
  log_content := 'item_id:'||new.item_id ;
  end if;  
  return null;
end;
$$;

create or replace function f_trigger_hr_salary_items() returns trigger as 
$$
declare   
   log_content text;   
begin
  if (tg_op = 'update') then
  log_content := 'item_id:'||old.item_id ;
  end if;  
  return null;
end;
$$;

-- 1020421696089758085
\c
drop function fact;
create or replace function fact (n number) return number authid definer
is
begin
    if n = 1 then                 
        return n;
    else
        return n * fact(n-1); 
    end if;
end;
/
declare 
    results number;
begin
    select fact(10::number) into results from dual;
    raise notice '%', results;
end;
/

-- 1020421696089756741
drop package pkg2;
drop procedure p_test;
CREATE OR REPLACE PACKAGE pkg2 AUTHID DEFINER IS
    t1 varchar2(100);
    t2 integer := 0;
    t3 integer := 0;
    PROCEDURE s (p varchar2);
    PROCEDURE s (p int);
    PROCEDURE s;
END pkg2;
/
create or replace package body pkg2 IS
    PROCEDURE s (p varchar2) is
    begin
        t1 := ' '||t1||' '||p;
        raise notice '% %', t1, p;
    end;
    PROCEDURE s (p int) is
    begin
        t2 := t2+p;
        raise notice '%', t2;
    end;
    PROCEDURE s is
    begin
        t3 := t3 + 1;
        raise notice 'current t1 and t2 values: % %', t1, t2;
        raise notice 'current call this procedure times is : %', t3;
    end;
end;
/
CREATE OR REPLACE PROCEDURE p_test AUTHID DEFINER IS
BEGIN
    pkg2.t1 := 'hi';
    pkg2.t2 := 100;
    pkg2.s('china');
    pkg2.s(',hi world');
    pkg2.s(200);
    pkg2.s(200);
    pkg2.s;
    pkg2.s;
    pkg2.s;
    pkg2.s;
END p_test;
/
call p_test();
drop procedure p_test;

/*
 * other testing.
 */
create or replace function infunc() return int authid definer is
begin
  return 1;
end;
/

create or replace function outfunc() return int authid definer is
begin
  perform infunc();
  return 1;
end;
/

select outfunc();

-- call in remote node;
create table dt(a int, b int);
insert into dt values(generate_series(1, 5), generate_series(1, 5));
explain (costs off) select * from dt where b > outfunc() order by 1;
select * from dt where b > outfunc() order by 1;

-- insert table
create or replace function insertfunc() return int authid definer is
begin
  insert into dt values(10, 2);
  insert into dt values(generate_series(1, 5), generate_series(1, 5));
  perform * from dt where b > outfunc() order by 1;
  return 1;
end;
/

select insertfunc();

-- non-definer call definer
create or replace function f_nodef() return int authid current_user is
begin
  perform infunc();
  return 1;
end;
/
select f_nodef();

create or replace procedure p_nodef() authid current_user is
begin
  perform infunc();
end;
/
call p_nodef();

drop table dt;
drop function infunc;
drop function outfunc;

-- 1020421696090866075
create or replace procedure p2 as
declare
    cursor c2 is select pid,case when state='active' then 'this pid is active' when state='idle' then 'this pid is idle' end lock_status from pg_stat_active;
begin
    null;
end;
/

drop procedure p2();

-- 1020421696089678103
drop procedure pow2;
create procedure pow2(num_in number , num_out out number) is
  begin
    num_out := num_in * num_in;
  end;
/

declare
  x integer;
begin
  pow2(3,x);
  raise notice '--%', x;
end;
/

declare
  x integer default 1;
begin
  pow2(3, cast(x as int));
  raise notice '--%', x;
end;
/

declare
  x integer default 1;
begin
  pow2(3, -x);
  raise notice '--%', x;
end;
/

declare
  x integer default 1;
begin
  pow2(3, 1-x);
  raise notice '--%', x;
end;
/

-- function
drop function pfun;
create function pfun(num_in number, num_out out number) returns int is
  begin
    num_out := num_in * num_in;
    return 1;
  end;
/

declare
  x integer default 1;
  r integer;
begin
  r := pfun(3,x);
  raise notice '--%', x;
end;
/

drop procedure pow2;
drop function pfun;

-- 1020421696090885299
\c - PL_SU
drop table emp cascade;
create table emp(empno number(4) constraint pk_emp primary key,ename varchar2(20),job varchar2(9),mgr number(4), hiredate date,sal number(7,2),comm number(7,2), deptno number(4));
drop table dept cascade;
create table dept (deptno number(4), dname varchar2(14), loc varchar2(13));

create or replace procedure t_xuanze(names in varchar)
is
    tname varchar2(10);
begin
    tname := names;
    dbms_output.serveroutput('t');
    if(tname='emp') then
        declare
            cursor cc is select * from emp where sal>(select avg(sal) from emp where deptno=100);
            ccrec cc%rowtype;
        begin
            open cc;
            loop
                fetch cc into ccrec;
                exit when cc%notfound;
                dbms_output.put_line(ccrec.empno||'--'||ccrec.ename||'--'||ccrec.job||'--'||ccrec.sal);
            end loop;
            close cc;
        end;
    elsif(tname='dept') then
        declare
            cursor cc is select b.dname,count(a.empno) as zongshu from emp a,dept b where a.deptno=b.deptno group by b.dname;
            ccrec cc%rowtype;
        begin
            open cc;
            loop
            fetch cc into ccrec;
                exit when cc%notfound;
                dbms_output.put_line(ccrec.dname||'----'||ccrec.zongshu);
            end loop;
            close cc;
        end;
    else
        dbms_output.put_line('this is error!');
    end if;
end;
/

drop procedure t_xuanze;
drop table if exists emp;

-- 1020421696090870971
create or replace  trigger chqin_dml_trigger  before update of sal on emp1
for each row
declare
   v1 number;
BEGIN
    v1:= :new.sal-:old.sal;
    if v1<0 then
        raise_application_error(-20003,'sal is not correct');
    end if;

END;
/

-- 1020421696090687079
create table product_class(
id serial primary key,
name varchar,
code varchar
)distribute by replication to group default_group;
\copy product_class(name, code) from stdin;
'a'	1
\.
select name, code from product_class;
drop table product_class;

\c
-- 1020421696089603153
-- 1020421696090877507
-- Cursor still valid after COMMIT but not rollback.
-- one procedure/function
create table cur_test(a int, b int);
insert into cur_test values(1, 2);
insert into cur_test values(2, 2);
insert into cur_test values(3, 2);
insert into cur_test values(4, 2);

create or replace procedure cur_proc() is
  cs refcursor;
  cv cur_test%rowtype;
begin
  open cs for select * from cur_test order by 1, 2;
  fetch cs into cv;
  raise notice '%', cv.a;
  commit;
  fetch cs into cv;
  raise notice '%', cv.a;
  commit;
  fetch cs into cv;
  raise notice '%', cv.a;
end;
/
call cur_proc();
drop procedure cur_proc;

-- return a cursor from a function/procedure
create or replace procedure retcur(rco out refcursor) as $$
declare
  rc refcursor;
begin
  open rc for select * from cur_test order by 1, 2;
  rco = rc;
end
$$;

create or replace procedure cur_proc() is
$$
declare
  cs refcursor;
  cv cur_test%rowtype;
begin
  call retcur(cs);
  fetch cs into cv;
  raise notice '%', cv.a;
  commit;
  fetch cs into cv;
  raise notice '%', cv.a;
  commit;
  fetch cs into cv;
  raise notice '%', cv.a;
end;
$$;

call cur_proc();

-- in transaction call not closed cursor
create or replace procedure cur_proc() is
$$
declare
  cs refcursor;
  cv cur_test%rowtype;
begin
  call retcur(cs);
  fetch cs into cv;
end;
$$;

begin;
call cur_proc();
fetch next in cs;
end;

-- open/close/open/close..
create or replace procedure cur_proc() is
$$
declare
  cs refcursor;
  cv cur_test%rowtype;
begin
  for i in 1..10 loop
    open cs for select * from cur_test order by 1, 2;
    fetch cs into cv;
    raise notice '%', cv.a;
    close cs;
    commit;
  end loop;
end;
$$;
call cur_proc();

create or replace procedure cur_proc() is
  cs refcursor;
  cv cur_test%rowtype;
begin
  open cs for select * from cur_test order by 1, 2;
  fetch cs into cv;
  raise notice '%', cv.a;
  commit;
  close cs;
  fetch cs into cv;
  raise notice '%', cv.a;
end;
/
call cur_proc();

create or replace procedure cur_proc() is
  cs cursor for select * from cur_test order by 1, 2;
  cv cur_test%rowtype;
begin
  open cs;
  fetch cs into cv;
  raise notice '%', cv.a;
  commit;
  fetch cs into cv;
  raise notice '%', cv.a;
  close cs;
  open cs;
  fetch cs into cv;
  raise notice '%', cv.a;
end;
/
call cur_proc();

-- rollback case not support
create or replace procedure cur_proc() is
  cs cursor for select * from cur_test order by 1, 2;
  cv cur_test%rowtype;
begin
  open cs;
  fetch cs into cv;
  raise notice '%', cv.a;
  rollback;
  fetch cs into cv;
  raise notice '%', cv.a;
end;
/
call cur_proc();
-- tapd case
drop table if exists emp_3153;
create table emp_3153(empno number(4) constraint pk_emp primary key,ename varchar2(20),job varchar2(9),mgr number(4), hiredate date,sal number(7,2),comm number(7,2), deptno number(4));
insert into emp_3153 values(7369,'smith','clerk',7902,to_date('17-12-1980','dd-mm-yyyy'),800,null,200);
insert into emp_3153 values(7499,'allen','salesman',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,300);

create or replace procedure p_test(forraise in number)
as
    cursor c1 is select * from emp_3153;
    v_row emp_3153%rowtype;
begin
    open c1;
    loop
        fetch c1 into v_row;
        exit when c1%notfound;
        if v_row.sal<5000 then
            update emp_3153 set sal=(v_row.sal+forraise) where empno=v_row.empno;
            commit;
        end if;
    end loop;
    close c1;
end;
/
call p_test(500);

--
create table t_ref(f1 int,f2 int);
insert into t_ref values(1,1),(2,2),(3,3);
create or replace procedure p_refcursor() as
$$
declare
  v_ref refcursor;
  v_rec record;
begin
  open v_ref for select * from t_ref;
  fetch next from v_ref into v_rec ;
  commit;
  fetch next from v_ref into v_rec ;
end;
$$;

call p_refcursor();

drop procedure cur_proc;
-- 1020421696091445361
-- failed to create package. More test cases about BEGIN END...
create table case_tbl(a int, b int);
create or replace function case_func(ab int) return int is
  cursor a is select case when a = 1 then 1 else 2 end x from case_tbl;
begin
 return ab;
end;
/
select case_func(1) from dual;

create or replace function case_func(ab int) return int is
  cursor a is select case when a = 1 then 1 else 2 end x from case_tbl;
begin
 for i in 1..2 loop
   raise notice '%', i;
   declare
     f float;
   begin
     f = 10;
     raise notice '%', f;
   end;
 end loop;
 return ab;
end;
/
select case_func(1) from dual;

create or replace function case_func(ab int) return int is
  cursor a is select case when a = 1 then 1 else 2 end x from case_tbl;
  procedure fproc(a int) is
  begin
    null;
  end;
begin
 for i in 1..2 loop
   raise notice '%', i;
   declare
     f float;
   begin
     f = 10;
     raise notice '%', f;
   end;
 end loop;
 return ab;
end;
/
select case_func(1) from dual;

-- package
create or replace package case_pkg is
  function case_func(ab int) return int;
end;
/
create or replace package body case_pkg is
  function case_func(ab int) return int is
    cursor a is select case when a = 1 then 1 else 2 end x from case_tbl;
  begin
   for i in 1..2 loop
     raise notice '%', i;
     declare
       f float;
     begin
       f = 10;
       raise notice '%', f;
     end;
   end loop;
   return ab;
  end;
end;
/
drop table case_tbl;

-- Fix memory leak in plpgsql's CALL processing.
Drop table if exists tb1;
Drop procedure if exists p60(_errno int, INOUT o_return_msg character varying);
Drop procedure if exists f30(a int, INOUT o_return_msg text);
Create table tb1(a int);
Insert into tb1 select generate_series(1,10);
create or replace procedure p60(_errno int, INOUT o_return_msg character varying)
as
$$
begin
    select * from tb1 where a = _errno;
    o_return_msg := '输入参数不是6位';
end;
$$ language default_plsql;
CREATE OR REPLACE procedure f30(a int, INOUT o_return_msg text)
    language default_plsql
    as $$
BEGIN
    call p60(-20344,o_return_msg);
END;
$$;
call f30(1, 'success');
call f30(1, 'success');
call f30(1, 'success');
Drop table if exists tb1;
Drop procedure if exists p60(_errno int, INOUT o_return_msg character varying);
Drop procedure if exists f30(a int, INOUT o_return_msg text);

-- 1020421696089794567

create table pkg5_t(id int, val number, name varchar2(100));
create or replace package pkg5 AUTHID DEFINER as
   results number;
   v_sql varchar2(100);
   records number := 1;
   procedure p(n in number, name in varchar2 default 'abc');
end pkg5;
/
create or replace package body pkg5 as
   procedure p(n in number, name in varchar2 default 'abc')
   is
       nums int;
   begin
      if name = 'abc' then
         nums := length(name);
            for i in 1..nums
            loop
                records := records*(n-i+1);
                delete from pkg5_t where id=i and val=records;
            end loop;
            results := results + n;
            commit;
      else
         nums := length(name);
            for i in 1..nums
            loop
                records := records*(n-i+1);
                delete from pkg5_t where id=i and val=records;
            end loop;
            results := results + n;
            rollback;
      end if;
   end p;
end pkg5;
/

call pkg5.p(2);

--
create schema s1020421696094451065;
set search_path to s1020421696094451065;
drop table if exists res_paycard_month_202108;
CREATE TABLE res_paycard_month_202108 (
    id numeric(16,0) ,
    chk_acct_type character varying(8),
    sequence character varying(20),
    accept_month numeric DEFAULT to_number(to_char(date'2021-09-22', 'MM'::text)),
    file_name character varying(50),
    batch character varying(20),
    countatal numeric(8,0),
    cardflag character(1),
    feeaccountstart character varying(8),
    feeaccountstop character varying(8),
    tradetime character varying(14),
    tradetype character varying(2),
    msisdn character varying(20),
    supplycardkind character varying(2),
    timetamp character varying(14),
    useridentifier character varying(32),
    card_state character varying(2),
    active_flag character(1),
    comp_state character(1) ,
    done_time timestamp(0) without time zone ,
    comp_date timestamp(0) without time zone,
    deal_tag character(1) ,
    remarks character varying(100)
)
 
DISTRIBUTE BY SHARD (id) to GROUP default_group;

drop table if exists res_origin_backup_202108;
drop table if exists res_used_backup_202108;
create table res_origin_backup_202108(paycard_no character varying(20),res_state character(1));
create table res_used_backup_202108(paycard_no character varying(20),res_state character(1) );


drop table if exists res_paycard_vchis;
CREATE TABLE res_paycard_vchis (
    paycard_no character varying(64) NOT NULL,
    vc_state character varying(2),
    done_time timestamp(0) without time zone,
    remarks character varying(200)
)
 
DISTRIBUTE BY SHARD (paycard_no) to GROUP default_group;
insert into res_paycard_vchis(paycard_no) values('001');


drop function if exists p10_test(last_number character, INOUT return_code character varying, INOUT return_msg character varying) ;
CREATE or replace FUNCTION p10_test(last_number character,  return_code in out character varying,  return_msg in out character varying) RETURN varchar2
is
    cur_type REFCURSOR;
    v_paycard_no res_paycard_month_202108.sequence%TYPE;
    v_vc_state res_paycard_month_202108.cardflag%TYPE;
    v_id res_paycard_month_202108.id%TYPE;
    v_card_state varchar(2);
    v_used_card_state varchar(2);
    v_isflag decimal(38);
    v_used_isflag decimal(38);
    v_month_sql varchar(4000);
    v_month varchar(20);
    v_origin_backup_name varchar(100);
    v_used_backup_name varchar(100);
    vc_month_compare REFCURSOR;

BEGIN
    return_code := '0';
    return_msg := 'ok';
    v_month := to_char(to_date('2021-08-01 18:43:11'), 'YYYYMM');
    v_origin_backup_name := 'res_origin_backup_' || v_month;
    v_used_backup_name := 'res_used_backup_' || v_month;
    v_month_sql := ' select a.sequence, a.id,a.cardflag
      from res_paycard_month_' || (v_month || (' a
     where a.deal_tag = ''1''
       and a.chk_acct_type = ''MONTH'' and substr(a.id,-1)=''' || (last_number || '''')));
    OPEN vc_month_compare FOR  EXECUTE v_month_sql;
    LOOP
        FETCH vc_month_compare INTO v_paycard_no, v_id, v_vc_state;
        EXIT WHEN vc_month_compare % notfound;
        --EXIT WHEN  not found;
        v_month_sql := 'UPDATE res_paycard_month_' || (v_month || (' a
         SET a.deal_tag = ''2''
         WHERE a.sequence = ''' || (v_paycard_no || ('''
         and a.id = ''' || (v_id || '''')))));
        EXECUTE v_month_sql;
        v_card_state := '';
        v_used_card_state := '';
        v_month_sql := 'select count(*)
            FROM (SELECT a.paycard_no
            FROM ' || (v_origin_backup_name || (' A
            WHERE a.paycard_no = ''' || (v_paycard_no || ''')')));
        EXECUTE v_month_sql INTO v_isflag;
       -- perform dbms_output.serveroutput('t');
       -- perform DBMS_OUTPUT.PUT_LINE('---v_isflag'||v_isflag);
        v_month_sql := 'select count(*)
            FROM (SELECT d.paycard_no
            FROM ' || (v_used_backup_name || (' D
            WHERE D.paycard_no = ''' || (v_paycard_no || ''')')));
        EXECUTE v_month_sql INTO v_used_isflag;
       -- perform DBMS_OUTPUT.PUT_LINE('---v_used_isflag'||v_used_isflag);
        IF v_isflag = 0
            AND v_used_isflag = 0
        THEN
            v_month_sql := 'UPDATE res_paycard_month_' || (v_month || (' a
             SET a.card_state =''16'',
             a.comp_state = ''3'',
             a.deal_tag = ''3'',
             a.comp_date  = to_date(''2021-08-01 18:43:11''),
             a.remarks    = ''remarks00''
             WHERE a.sequence = ''' || (v_paycard_no || ('''
             and a.id = ''' || (v_id || '''')))));
            EXECUTE v_month_sql;
        ELSE
            IF v_isflag > 0 THEN
                 v_month_sql := 'select res_state
                  FROM (SELECT a.res_state
                  FROM ' || (v_origin_backup_name || (' A
                  WHERE a.paycard_no = ''' || (v_paycard_no || ''')')));
                 EXECUTE v_month_sql INTO v_card_state;
            -- --DBMS_OUTPUT.PUT_LINE(V_MONTH_SQL);
               --  perform DBMS_OUTPUT.PUT_LINE('---v_card_state'||v_card_state);
            ELSE
                IF v_used_isflag > 0 THEN
                    v_month_sql := 'select res_state
                        FROM (SELECT E.res_state
                        FROM ' || (v_used_backup_name || (' E
                        WHERE E.paycard_no = ''' || (v_paycard_no || ''')')));
                   -- perform dbms_output.serveroutput('t');
                    --perform DBMS_OUTPUT.PUT_LINE(V_MONTH_SQL);
                    EXECUTE v_month_sql INTO v_used_card_state;
                  --  perform DBMS_OUTPUT.PUT_LINE(v_used_card_state);
                END IF;
            END IF;

            IF v_vc_state = '0' THEN
                IF v_used_card_state = '4'
                  OR v_used_card_state = 'b'
                THEN
                    v_month_sql := 'UPDATE res_paycard_month_' || (v_month || (' a
                        SET a.card_state =''' || (v_used_card_state || (''',
                        a.comp_state = ''1'',
                        a.comp_date  = to_date(''2021-08-01 18:43:11''),
                        a.deal_tag = ''3'',
                        a.remarks    = ''remarks04b''
                        WHERE a.sequence = ''' || (v_paycard_no || ('''
                        and a.id = ''' || (v_id || '''')))))));
                    -- --DBMS_OUTPUT.put_line('2');
                    EXECUTE v_month_sql;
                ELSE
                    v_month_sql := 'UPDATE res_paycard_month_' || (v_month || (' a
                        SET a.card_state =''' || (nvl(v_used_card_state, v_card_state) || (''',
                        a.comp_state = ''2'',
                        a.comp_date  = to_date(''2021-08-01 18:43:11''),
                        a.deal_tag = ''3'',
                        a.remarks    = ''remarks0''
                        WHERE a.sequence = ''' || (v_paycard_no || ('''
                        and a.id = ''' || (v_id || '''')))))));
                    -- --DBMS_OUTPUT.put_line('3');
                    EXECUTE v_month_sql;
                END IF;

            END IF;

        END IF;
        COMMIT;
    END LOOP;
    CLOSE vc_month_compare;
   -- EXECUTE v_month_sql;
    v_month_sql := 'UPDATE res_paycard_month_' || (v_month || ' a
         SET a.card_state =''17'',
             a.comp_date  = to_date(''2021-08-01 18:43:11''),
             a.remarks    = ''remarks_last''
       WHERE  a.chk_acct_type=''MONTH''
         and a.card_state=''16'' and a.sequence in  (SELECT o.paycard_no FROM res_paycard_vchis o)');
    -- --DBMS_OUTPUT.put_line('123');
    EXECUTE v_month_sql;
    COMMIT;
    return 'ok';
EXCEPTION
    WHEN others THEN
        ROLLBACK;
       -- return_code := TO_CHAR(sqlcode);
        return_code := TO_CHAR(-1);

        return_msg := 'Procedure EXCEPTION: ' || (TO_CHAR(-1) || ('->' || SUBSTR(sqlerrm, 1, 100)));
        return 'EXCEPTION';
END ;
/

delete from res_paycard_month_202108;
delete from res_used_backup_202108;
delete from res_origin_backup_202108;
insert into res_paycard_month_202108 (sequence,id,cardflag,deal_tag,chk_acct_type)values('003',111,'0','1','MONTH');
insert into res_used_backup_202108 values('003','4');

declare
code character varying:='';
msg character varying:='';
res varchar2(200);
begin
res:= p10_test('1',code,msg);
raise notice 'code: %,msg: %',code,msg;
end;
/
select sequence,id,cardflag,deal_tag,chk_acct_type from res_paycard_month_202108;

drop table if exists res_paycard_month_202108;
drop table if exists res_origin_backup_202108;
drop table if exists res_used_backup_202108;
drop table if exists res_paycard_vchis;
drop function  if exists p10_test(last_number character, INOUT return_code character varying, INOUT return_msg character varying);
drop schema s1020421696094451065 cascade;
reset search_path;

-- 1020421696099831481
create or replace function f_commit(v1 character varying) return varchar2
is
     res varchar2(20);
     num number;
begin
    perform p8_test(v1,'','');

    return res;
exception
    when others then
        raise notice 'error: (%)', sqlerrm;
        return 'exception1';
end;
/

create or replace function p8_test(p_date character varying, inout v_back_id character varying, inout v_back_msg character varying) return varchar2
is

begin
    execute 'drop table a';
--    commit;
  return 'ok';
exception
    when others then
        rollback;
        return 'exception';

end ;
/
select * from f_commit('20210629');
drop function f_commit;
drop function p8_test;
-- udt can be used in out parameter
drop package pkg1;
create or replace package pkg1 is
  TYPE sum_multiples IS TABLE OF int INDEX BY int;
  n  int := 5;   -- number of multiples to sum for display
  sn int := 10;  -- number of multiples to sum
  m  int := 3;   -- multiple
  type rec_t is record (c1 text, c2 text);
end pkg1;
/
create schema s1020421696092703229;
set search_path to public,s1020421696092703229;
create or replace function get_sum_multiples (
    multiple IN int,
    num      IN int,
    v1       out pkg1.sum_multiples
  ) return varchar2
IS
    s pkg1.sum_multiples;
BEGIN
    FOR i IN 1..num LOOP
      s(i) := multiple * ((i * (i + 1)) / 2);  -- sum of multiples
    END LOOP;
    FOR i IN 1..num LOOP
      raise notice '%', s(i);
    END LOOP;
     v1:=s;
     return 'ok';
END get_sum_multiples;
/
declare
  v pkg1.sum_multiples;
  i int;
  res varchar2(20);
begin
   res:=get_sum_multiples (pkg1.m, pkg1.sn,v);
  raise notice 'Sum of the first % multiples of % is %',pkg1.n,pkg1.m, v(pkg1.n);
  i := v.LAST;
  WHILE i IS NOT NULL LOOP
    raise notice 'Loop[%] is %',i, v(i);
    i := v.PRIOR(i);
  END LOOP;
end;
/

declare
  v pkg1.sum_multiples;
  res varchar2(20);
begin
  res:=get_sum_multiples (pkg1.m, pkg1.sn,v);
  raise notice 'Sum of the first % multiples of % is %',pkg1.n,pkg1.m, v(pkg1.n);
  FOR i IN 1..pkg1.sn LOOP
    raise notice '%', v(i);
  END LOOP;
end;
/

drop function get_sum_multiples;
create or replace function get_sum_multiples (
    multiple IN int,
    v1       out pkg1.sum_multiples,
    num      IN int,
    v2       out pkg1.sum_multiples
  ) return varchar2
IS
    s pkg1.sum_multiples;
    s2 pkg1.sum_multiples;
BEGIN
    FOR i IN 1..num LOOP
      s(i) := multiple * ((i * (i + 1)) / 2);  -- sum of multiples
      s2(i) := i;  -- sum of multiples
    END LOOP;
     v1:=s;
     v2=s2;
     return 'ok';
END get_sum_multiples;
/
declare
  v pkg1.sum_multiples;
  v2 pkg1.sum_multiples;
  i int;
  res varchar2(20);
begin
   res:=get_sum_multiples (pkg1.m, v, pkg1.sn,v2);
  raise notice 'Sum of the first % multiples of % is %',pkg1.n,pkg1.m, v(pkg1.n);
  i := v.LAST;
  WHILE i IS NOT NULL LOOP
    raise notice 'Loop[%] is %, v2 si %',i, v(i), v2(i);
    i := v.PRIOR(i);
  END LOOP;
end;
/
create or replace procedure get_sum_multiples_proc (
    multiple IN int,
    v1       out pkg1.sum_multiples,
    num      IN int,
    v2       out pkg1.sum_multiples
  )
IS
    s pkg1.sum_multiples;
    s2 pkg1.sum_multiples;
BEGIN
    FOR i IN 1..num LOOP
      s(i) := multiple * ((i * (i + 1)) / 2);  -- sum of multiples
      s2(i) := i;  -- sum of multiples
    END LOOP;
     v1:=s;
     v2=s2;
END;
/
declare
  v pkg1.sum_multiples;
  v2 pkg1.sum_multiples;
  i int;
  res varchar2(20);
begin
  get_sum_multiples_proc(pkg1.m, v, pkg1.sn,v2);
  raise notice 'Sum of the first % multiples of % is %',pkg1.n,pkg1.m, v(pkg1.n);
  i := v.LAST;
  WHILE i IS NOT NULL LOOP
    raise notice 'Loop[%] is %, v2 si %',i, v(i), v2(i);
    i := v.PRIOR(i);
  END LOOP;
end;
/
drop procedure  get_sum_multiples_proc;
create or replace procedure get_sum_multiples_proc (
    multiple IN int,
    v1       out pkg1.rec_t,
    num      IN int,
    v2       out pkg1.rec_t
  )
IS
    s pkg1.rec_t;
    s2 pkg1.rec_t;
BEGIN
    s.c1 = 's1';
    s.c2 = 's2';
    s2.c1 = 's2c1';
    s2.c2 = 's2c2';
    v1:=s;
    v2=s2;
END;
/
declare
  v pkg1.rec_t;
  v2 pkg1.rec_t;
  i int;
  res varchar2(20);
begin
  get_sum_multiples_proc(pkg1.m, v, pkg1.sn,v2);
  raise notice '% %', v, v2;
end;
/
reset search_path;
drop schema s1020421696092703229 cascade;

-- 1020421696093042793
create schema S1020421696093042793;
create or replace procedure S1020421696093042793.proc1 is
begin
insert into t1 values (10);
insert into t1 values (20);
commit;
insert into t1 values (40);
insert into t1 values (45);
commit;
exception when others then 
commit;
end proc1; 
/

drop procedure S1020421696093042793.proc1;
drop schema S1020421696093042793;

-- 1020421696089717715
create schema s1020421696089717715;
set search_path to s1020421696089717715;

CREATE or REPLACE procedure pro_get_students_info(stids number)
as
begin
  raise notice 'pro_get_students_info(stids number)';
end;
/
call pro_get_students_info(1);

CREATE or REPLACE procedure pro_get_students_info(names varchar2)
as
begin
  raise notice 'pro_get_students_info(names varchar2)';
end;
/
call pro_get_students_info(1);
call pro_get_students_info('a');

CREATE or REPLACE procedure pro_get_students_info(names varchar)
as
begin
  raise notice 'pro_get_students_info(names varchar)';
end;
/
call pro_get_students_info(1);
call pro_get_students_info('a');

reset search_path;
drop schema s1020421696089717715 cascade;

-- tapd
CREATE or REPLACE procedure pro_get_students_info(stids number)
as
begin
  raise notice 'number';
end;
/

CREATE or REPLACE procedure pro_get_students_info(names varchar2)
as
begin
  raise notice 'varchar2';
end;
/
call pro_get_students_info(2);
call pro_get_students_info('aa');
call pro_get_students_info(2::number);
select proname from pg_proc where proname = 'pro_get_students_info';
drop procedure pro_get_students_info(number);
drop procedure pro_get_students_info(varchar2);

-- pkg
create or replace package tapd_pkg is
  procedure pro_get_students_info(stids number);
  procedure pro_get_students_info(names varchar2);
end;
/
create or replace package body tapd_pkg is
  procedure pro_get_students_info(stids number)
  as
  begin
    raise notice 'number';
  end;
  procedure pro_get_students_info(names varchar2)
  as
  begin
    raise notice 'varchar2';
  end;
end;
/

call tapd_pkg.pro_get_students_info(1);
call tapd_pkg.pro_get_students_info('a');

drop package tapd_pkg;

-- all
create or replace package pkg_fun is
  function fa(a number) return int;
  function fa(a varchar2) return int;
  function fa(a float) return int;
  function fa(a int) return int;
end;
/

create or replace package body pkg_fun is
  function fa(a number) return int is
  begin
    dbms_output.put_line('number');
    return 1;
  end;

  function fa(a varchar2) return int is
  begin
    dbms_output.put_line('varchar2');
    return 1;
  end;

  function fa(a float) return int is
  begin
    dbms_output.put_line('float');
    return 1;
  end;

  function fa(a int) return int is
  begin
    dbms_output.put_line('int');
    return 1;
  end;

end;
/

select pkg_fun.fa(1);
select pkg_fun.fa(1.1);
select pkg_fun.fa('a');
drop package pkg_fun;

--
create or replace package pkg_fun is
  function fa(a number) return int;
  function fa(a int) return int;
end;
/

create or replace package body pkg_fun is
  function fa(a number) return int is
  begin
    dbms_output.put_line('number');
    return 1;
  end;

  function fa(a int) return int is
  begin
    dbms_output.put_line('float');
    return 1;
  end;

end;
/

select pkg_fun.fa(1);
drop package pkg_fun;

-- 1020421696092659085
drop package my_types;

create or replace package my_types authid current_user is
  type my_aa is table of varchar2(20) index by pls_integer;
  function init_my_aa (ret1 my_aa) return my_aa;
end my_types;
/
create or replace package body my_types is
  function init_my_aa(ret1 my_aa) return my_aa is
    ret my_aa;
  begin
    ret:=ret1;
    ret(-10) := '-ten';
    ret(0) := 'zero';
    ret(1) := 'one';
    ret(2) := 'two';
    ret(3) := 'three';
    ret(4) := 'four';
    ret(9) := 'nine';
    return ret;
  end init_my_aa;
end my_types;
/

declare
  v1 my_types.my_aa;
  v2 number;
  v constant my_types.my_aa := my_types.init_my_aa(v1);
begin
  declare
    idx pls_integer := v.first();
  begin
    while idx is not null loop
      raise notice '%,%',idx,v(idx);

      --dbms_output.put_line(to_char(idx, '999')||lpad(v(idx), 7));
      idx := v.next(idx);
    end loop;
  end;
end;
/

declare
  type my_map is table of varchar2(15) index by integer;
  test1 my_map;
  i pls_integer;
begin
  test1(1) := 'value 1';
  test1(2) := 'value 2';
  test1(3) := 'value 3';
  dbms_output.serveroutput('t');
  dbms_output.put_line('min is: ' || test1.first());
  dbms_output.put_line('max is: ' || test1.last());
  raise notice '%', test1.count;
  --perform dbms_output.put_line('count is: ' || test1.count);

  i := test1.first();
  while i is not null loop
   -- raise notice 'key is %,value is %', i,test1(i);
    dbms_output.serveroutput('t');
    dbms_output.put_line('key is ' || i || ', value is ' || test1(i));
    i := test1.next(i);
  end loop;

  test1.delete(2);
  test1.delete();
end;
/

drop package my_types;

-- 1020421696092470841
create table t102(a int);
create or replace function t102p(a int) return int is
begin
  insert into t102 values(1);
  commit;
  return 1;
end;
/

declare
begin
perform t102p(1) + t102p(1);
end;
/

declare
begin
perform t102p(1) + (select count(*) from t102 where t102p(1) > 0);
end;
/

select * from t102;
drop function t102p;
drop table t102;

-- 1020421696092706225
create schema s1020421696092706225;
set search_path to s1020421696092706225;
create table emp
       (empno number(4),
        ename varchar2(10),
        job varchar2(9),
        mgr number(4),
        hiredate date,
        sal number(7,2),
        comm number(7,2),
        deptno number(2));

insert into emp values
(7369,'smith','clerk',7902,to_date('17-12-1980','dd-mm-yyyy'),800,null,20);
insert into emp values
(7499,'allen','salesman',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
insert into emp values
(7521,'ward','salesman',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
insert into emp values
(7566,'jones','manager',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,null,20);
insert into emp values
(7654,'martin','salesman',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
insert into emp values
(7698,'blake','manager',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,null,30);
insert into emp values
(7782,'clark','manager',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,null,10);
insert into emp values
(7788,'scott','analyst',7566,to_date('19-04-87','dd-mm-rr'),3000,null,20);
insert into emp values
(7839,'king','president',null,to_date('17-11-1981','dd-mm-yyyy'),5000,null,10);
insert into emp values
(7844,'turner','salesman',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
insert into emp values
(7876,'adams','clerk',7788,to_date('23-05-87', 'dd-mm-rr'),1100,null,20);
insert into emp values
(7900,'james','clerk',7698,to_date('3-12-1981','dd-mm-yyyy'),950,null,30);
insert into emp values
(7902,'ford','analyst',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,null,20);
insert into emp values
(7934,'miller','clerk',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,null,10);

declare
	type arrays is table of emp%rowtype;
	v_arrays arrays;
	val emp%rowtype;
	v_sql varchar2(100);
begin
	v_sql := 'select * from emp where deptno=30 order by empno';
	execute immediate v_sql bulk collect into v_arrays;
	
	for i in v_arrays.first..v_arrays.last
	loop
		val := v_arrays[i];
		dbms_output.put_line(val.empno);
		dbms_output.put_line(v_arrays[i].empno);
	end loop;
end;
/
declare
	type arrays is table of emp%rowtype;
	v_arrays arrays;
	val emp%rowtype;
	v_sql varchar2(100);
begin
	v_sql := 'select * from emp where deptno=30 order by empno';
	execute immediate v_sql bulk collect into v_arrays;
	
	for i in v_arrays.first..v_arrays.last
	loop
		val := v_arrays(i);
		dbms_output.put_line(v_arrays(i).empno);
		-- insert into emp_t(empno) values(val.empno);
	end loop;
end;
/
reset search_path;
drop schema s1020421696092706225 cascade;
reset search_path;

-- 1020421696093862797
drop package emp_mgmt;
create schema s1020421696093862797;
set search_path to s1020421696093862797;
create table emp
       (empno number(4) constraint pk_emp primary key,
        ename varchar2(10),
        job varchar2(9),
        mgr number(4),
        hiredate date,
        sal number(7,2),
        comm number(7,2),
        deptno number(2));
insert into emp values
(7369,'smith','clerk',7902,to_date('17-12-1980','dd-mm-yyyy'),800,null,20);
insert into emp values
(7499,'allen','salesman',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
insert into emp values
(7521,'ward','salesman',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
insert into emp values
(7566,'jones','manager',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,null,20);
insert into emp values
(7654,'martin','salesman',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
insert into emp values
(7698,'blake','manager',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,null,30);
insert into emp values
(7782,'clark','manager',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,null,10);
insert into emp values
(7788,'scott','analyst',7566,to_date('19-04-1987','dd-mm-yyyy'),3000,null,20);
insert into emp values
(7839,'king','president',null,to_date('17-11-1981','dd-mm-yyyy'),5000,null,10);
insert into emp values
(7844,'turner','salesman',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
insert into emp values
(7876,'adams','clerk',7788,to_date('23-05-1987', 'dd-mm-yyyy'),1100,null,20);
insert into emp values
(7900,'james','clerk',7698,to_date('3-12-1981','dd-mm-yyyy'),950,null,30);
insert into emp values
(7902,'ford','analyst',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,null,20);
insert into emp values
(7934,'miller','clerk',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,null,10);

drop package emp_mgmt;
drop schema emp_mgmt cascade;
create or replace package emp_mgmt as
  type emprectyp is record (empno number, sal number);
  empinfo emprectyp;
  f1 number :=7521;
   procedure p2();
end emp_mgmt;
/

create or replace package body emp_mgmt as

  procedure p2() is
    begin
      select empno,sal into empinfo from emp where empno=f1;
      raise notice '%,%',empinfo.empno,empinfo.sal;

    end;

end emp_mgmt;
/

declare
  v1 emp_mgmt.emprectyp;
  v2 emp_mgmt.emprectyp;
begin
  v1:=row(3,1000);
  raise notice '%,%',v1.empno,v1.sal;
  select empno,sal from emp into v1 where empno=emp_mgmt.f1;
  raise notice '%,%',v1.empno,v1.sal;
  select empno,sal from emp into emp_mgmt.empinfo where empno=emp_mgmt.f1;
  raise notice '%,%',emp_mgmt.empinfo.empno,emp_mgmt.empinfo.sal;
  v2:=emp_mgmt.empinfo;
  raise notice '%,%',v2.empno,v2.sal;
  call emp_mgmt.p2();

end;
/

drop package emp_mgmt;
create or replace package emp_mgmt as
  type emprectyp is table of emp.empno%type;
  empinfo emprectyp;
  f1 number :=7521;
end emp_mgmt;
/


declare
  v1 emp_mgmt.emprectyp;
  v2 emp_mgmt.emprectyp;
begin
  select empno from emp order by 1 bulk collect into emp_mgmt.empinfo;
  v2:=emp_mgmt.empinfo;
  raise notice '%',v2[1];
end;
/

drop package emp_mgmt;
drop schema s1020421696093862797 cascade;

reset search_path;

create schema s1020421696094077139;
set search_path to s1020421696094077139;

CREATE OR REPLACE TYPE student_obj_type AS OBJECT (stu_no NUMBER, --学号
                                                    stu_name VARCHAR2(255), --姓名
                                                    stu_sex VARCHAR2(3),--性别
                                                    score NUMBER --成绩
                                                  );

drop function if exists get_students_by_sex(varchar2);
CREATE OR REPLACE FUNCTION get_students_by_sex(in_sex VARCHAR2) RETURN varchar2
IS
    type v_arr is varray(10) of student_obj_type;
    my_arr v_arr;
BEGIN
    dbms_output.serveroutput('t');
    IF in_sex = '男'
    THEN
        my_arr := v_arr(student_obj_type(1,'张三','男',98), student_obj_type(2,'李四','男',88));
    ELSE
        my_arr := v_arr(student_obj_type(3,'小红','女',78), student_obj_type(4,'小娟','女',95));
    END IF;

    for i in 1..my_arr.count
    loop
    dbms_output.put_line(my_arr[i].stu_no||'-->'||my_arr[i].stu_name||'-->'||my_arr[i].stu_sex||'-->'||my_arr[i].score);
        end loop;

    RETURN 'DISASSOCIATE';
END get_students_by_sex;
/
select get_students_by_sex('男') from dual;
select get_students_by_sex('女') from dual;
drop function if exists get_students_by_sex(varchar2);
drop type if exists student_obj_type cascade;
drop schema s1020421696094077139 cascade;

reset search_path;

--
create schema s1020421696094192807;
set search_path to s1020421696094192807;
drop table if exists emp;
CREATE TABLE EMP(EMPNO NUMBER(4) CONSTRAINT PK_EMP PRIMARY KEY,ENAME VARCHAR2(20),JOB VARCHAR2(9),MGR NUMBER(4), HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2), DEPTNO NUMBER(4));
INSERT INTO EMP VALUES(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,200);
INSERT INTO EMP VALUES(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,300);
INSERT INTO EMP VALUES(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,300);
INSERT INTO EMP VALUES(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,200);
INSERT INTO EMP VALUES(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,300);
INSERT INTO EMP VALUES(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,300);
INSERT INTO EMP VALUES(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,100);
INSERT INTO EMP VALUES(7788,'SCOTT','ANALYST',7566,to_date('19-04-87','dd-mm-rr'),3000,NULL,200);
INSERT INTO EMP VALUES(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,100);
INSERT INTO EMP VALUES(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,300);
INSERT INTO EMP VALUES(7876,'ADAMS','CLERK',7788,to_date('23-05-87', 'dd-mm-rr'),1100,NULL,200);
INSERT INTO EMP VALUES(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,300);
INSERT INTO EMP VALUES(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,200);
INSERT INTO EMP VALUES(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,100);

-- TestPoint：package里的procedure使用动态数组进行表的插入及更新
drop table if exists emp_t;
create table emp_t (like emp)  distribute by replication;

create or replace package TestArray
as
	TestID int;
	TestName varchar2(100);

	--最好跟查询表返回结果Table结构相同
	type UserList is record (
		ListID varchar2(9),
		ListName varchar2(30),
		ListAddress varchar2(30),
		ListMail varchar2(30)
	);
	
	-- bug
	-- type myList is table of UserList; --定义myList为UserList数组；
	type myList is table of emp%rowtype; --定义myList为UserList数组；
	my_List myList;      --声明变量my_List为数组
	
	procedure Test(TestID in int, TestName in varchar2);
	function ListInsert(ListResult in myList) return boolean;
end TestArray;
/

create or replace package body TestArray is
      
	--方法调用开始
	procedure Test(TestID in int, TestName in varchar2)
	is
		ListAddress varchar2(30) := 'da lian';
		ListMail varchar2(30) :='mzyluokai@hotmail.com';    
		lst_sql varchar2(100);    
	begin
		lst_sql := 'select * from emp';

		EXECUTE IMMEDIATE lst_sql    --动态SQL执行
		BULK COLLECT INTO my_List;   --将查询结果放入数组my_List
		
		-- bug:这样调用就会报错：ERROR:  invalid transaction termination
		if ListInsert(my_List) = true 
		then
			update emp_t set ename = TestName where empno = TestID;
		end if;        
	end Test;

	--带返回值的方法
	function ListInsert(ListResult in myList) return boolean	
	is
		data_count number(10) := 0;
	begin
		data_count :=ListResult.count;

		--循环
		FOR i IN 1..data_count LOOP
			-- bug
			insert into emp_t values(ListResult[i].empno, ListResult[i].ename, ListResult[i].job, ListResult[i].mgr, ListResult[i].hiredate, ListResult[i].sal, ListResult[i].comm, ListResult[i].deptno);
		END LOOP;

		commit;
		return true;

-- 		exception when others then
-- 			rollback;
-- 			return false;
	end ListInsert;
end TestArray;
/
call TestArray.Test(7521, 'update');
select ename from emp_t where empno=7521;
drop package TestArray;
drop schema s1020421696094192807 cascade;
reset search_path;

-- 1020421696102213821
drop table if exists aa;
create table aa (id int, names varchar);
insert into aa values(1, 'opentenbase1');
insert into aa values(4, null);
create or replace function ttff return int
as
  type arrdt is table of aa%rowtype;
  dtarr arrdt;
  vsql varchar(1000);
begin
  dbms_output.serveroutput('t');
  vsql := 'select * from aa';
  execute immediate vsql bulk collect into dtarr;
  for i in 1..dtarr.count
  loop
    if dtarr[i].names||'a' = 'a' then
      dbms_output.put_line('****');
    end if;
  end loop;
  return 0;
end;
/
select ttff();

drop table if exists aa;
create table aa (id int, names varchar);
insert into aa values(1, 'opentenbase1');
insert into aa values(2, 'opentenbase2');
insert into aa values(3, null);
insert into aa values(4, null);
insert into aa values(5, null);
insert into aa values(6, 'opentenbase6');
insert into aa values(7, 'opentenbase7');
insert into aa values(8, null);
insert into aa values(9, null);
insert into aa values(10, null);
create or replace function ttff return int
as
  ret int;
  type arrdt is table of aa%rowtype;
  dtarr arrdt;
  vsql varchar(1000);
begin
  dbms_output.serveroutput('t');
  ret := 0;
  vsql := 'select * from aa order by id';
  execute immediate vsql bulk collect into dtarr;
  for i in 1..dtarr.count
  loop
    if dtarr[i].names||'a' = 'a' and dtarr[i].id >= 5 then
      dbms_output.put_line(dtarr[i].names||'_ge5_'||dtarr[i].id);
      ret := ret + 1;
    end if;
  end loop;
  return ret;
end;
/
select ttff();

create or replace function ttff return int
as
  ret int;
  type arrdt is table of aa%rowtype;
  dtarr arrdt;
  vsql varchar(1000);
begin
  dbms_output.serveroutput('t');
  ret := 0;
  vsql := 'select * from aa order by id';
  execute immediate vsql bulk collect into dtarr;
  for i in 1..dtarr.count
  loop
    if dtarr[i].names||'a' = 'a' then
      ret := ret + 1;
      if dtarr[i].id <= 4 then
        dbms_output.put_line(dtarr[i].names||'_le4_'||dtarr[i].id);
      elseif dtarr[i].id <= 8 then
        dbms_output.put_line(dtarr[i].names||'_le8_'||dtarr[i].id);
      else
        dbms_output.put_line(dtarr[i].names||'_gt8_'||dtarr[i].id);
      end if;
    end if;
  end loop;
  return ret;
end;
/
select ttff();

create or replace function ttff return int
as
  ret int;
  type arrdt is table of aa%rowtype;
  dtarr arrdt;
  vsql varchar(1000);
begin
    dbms_output.serveroutput('t');
    ret := 0;
    vsql := 'select * from aa order by id';
    execute immediate vsql bulk collect into dtarr;
    for i in 1..dtarr.count
    loop
      if dtarr[i].names||'a' = 'a' then
        ret := ret + 1;
        if dtarr[i].id >= 2 then
          if dtarr[i].id >= 4 then
            if dtarr[i].id >= 6 then
              if dtarr[i].id >= 8 then
                dbms_output.put_line(dtarr[i].names||'_ge8_'||dtarr[i].id);
              else
                dbms_output.put_line(dtarr[i].names||'_lt8_'||dtarr[i].id);
              end if;
            else
              dbms_output.put_line(dtarr[i].names||'_lt6_'||dtarr[i].id);
            end if;
          else
            dbms_output.put_line(dtarr[i].names||'_lt4_'||dtarr[i].id);
          end if;
        else
          dbms_output.put_line(dtarr[i].names||'_lt2_'||dtarr[i].id);
        end if;
      end if;
    end loop;
    return ret;
end;
/
select ttff();

drop table if exists bb;
create table bb(f1 int,f2 int);
create or replace function ttff return int
as
    type arrdt is table of aa%rowtype;
    dtarr arrdt;
    vsql varchar(1000);
begin
    dbms_output.serveroutput('t');
    vsql := 'select * from aa order by id';
    execute immediate vsql bulk collect into dtarr;
    forall i in 1..dtarr.count
        insert into bb values(dtarr[i].id,dtarr[i].id);
	return dtarr.count;
end;
/
select ttff();
select * from bb order by f1;

drop function ttff;
drop table if exists aa;
drop table if exists bb;

-- 1020421696095180579
-- TestPoint : forall and cursor
-- TestPoint : procedure in package body use the ref cursor type
drop table if exists emp;
create table emp(empno number(4) primary key,ename varchar2(10),job varchar2(9),mgr number(4), hiredate date,sal number(7,2),comm number(7,2), deptno number(4));
insert into emp values(7369,'smith','clerk',7902,to_date('17-12-1980','dd-mm-yyyy'),800,null,200);
insert into emp values(7499,'allen','salesman',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,300);
insert into emp values(7521,'ward','salesman',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,300);
insert into emp values(7566,'jones','manager',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,null,200);
insert into emp values(7654,'martin','salesman',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,300);
insert into emp values(7698,'blake','manager',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,null,300);
insert into emp values(7782,'clark','manager',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,null,100);
insert into emp values(7788,'scott','analyst',7566,to_date('19-04-87','dd-mm-rr'),3000,null,200);
insert into emp values(7839,'king','president',null,to_date('17-11-1981','dd-mm-yyyy'),5000,null,100);
insert into emp values(7844,'turner','salesman',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,300);
insert into emp values(7876,'adams','clerk',7788,to_date('23-05-87', 'dd-mm-rr'),1100,null,200);
insert into emp values(7900,'james','clerk',7698,to_date('3-12-1981','dd-mm-yyyy'),950,null,300);
insert into emp values(7902,'ford','analyst',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,null,200);
insert into emp values(7934,'miller','clerk',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,null,100);
drop table if exists chqin;
create table chqin (id number,f1 varchar2(20));

CREATE or REPLACE PACKAGE emp_mgmt AS
 TYPE empcurtyp IS REF CURSOR RETURN emp%ROWTYPE;
 TYPE genericcurtyp IS REF CURSOR;
 PROCEDURE p1(n number,sal out number);
END emp_mgmt;
/
CREATE or REPLACE PACKAGE BODY emp_mgmt AS
PROCEDURE p1(n number,sal out number) is
 cursor1 empcurtyp;
 v_employee emp%rowtype;
 type nt is table of emp%rowtype;
 v1 nt;
begin
  select * bulk collect into v1 from emp order by sal,hiredate;
  open cursor1 for select * from emp order by empno ;
  for i in 1..n loop
  fetch cursor1 into v_employee;
    raise notice 'loop:% % %',i,v_employee.ename,v_employee.sal;
    forall j in 1..n
      insert into chqin values(v1[j].sal,v_employee.ename);
  end loop;
  close cursor1;
  sal := v_employee.sal;
end;
end;
/

declare
n number := 2;
sal number;
begin
emp_mgmt.p1(n,sal);
raise notice '%',sal;
end;
/
select * from chqin order by 1,2;

drop table if exists chqin;
create table chqin (no_c number,sal_c number,name_c varchar2(20),sal number,name varchar2(20));

CREATE or REPLACE PACKAGE emp_mgmt AS
 TYPE empcurtyp IS REF CURSOR RETURN emp%ROWTYPE;
 TYPE genericcurtyp IS REF CURSOR;
 PROCEDURE p1(n number,empname out varchar,empsal out number);
END emp_mgmt;
/
CREATE or REPLACE PACKAGE BODY emp_mgmt AS
PROCEDURE p1(n number,empname out varchar,empsal out number) is
 cursor1 empcurtyp;
 v_employee emp%rowtype;
 type nt is table of emp%rowtype;
 v1 nt;
begin
  select * bulk collect into v1 from emp order by sal,hiredate;
  open cursor1 for select * from emp order by empno ;
  for i in 1..n loop
  fetch cursor1 into v_employee;
    if v1[i].sal >= 1000 then
      raise notice 'ge1000: % % %',i,v1[i].ename,v1[i].sal;
      forall j in 1..i
        insert into chqin values(v_employee.empno,v_employee.sal,v_employee.ename,v1[j].sal,v1[j].ename||'_'||v_employee.ename);
    elseif v1[i].sal >= 900 then
      raise notice 'ge900: % % %',i,v1[i].ename,v1[i].sal;
      forall j in 1..i
        insert into chqin values(v_employee.empno,v_employee.sal,v_employee.ename,v1[j].sal,v1[j].ename||'_'||v_employee.ename);
    else
      raise notice 'lt900: % % %',i,v1[i].ename,v1[i].sal;
      forall j in 1..i
        insert into chqin values(v_employee.empno,v_employee.sal,v_employee.ename,v1[j].sal,v1[j].ename||'_'||v_employee.ename);
    end if;
  end loop;
  close cursor1;
  empname := v_employee.ename;
  empsal := v_employee.sal;
end;
end;
/

declare
n number := 3;
empsal number;
empname varchar;
begin
emp_mgmt.p1(n,empname,empsal);
raise notice '% %',empname,empsal;
end;
/
select * from chqin order by 1,2;

drop table if exists chqin;
create table chqin (no_c number,sal_c number,name_c varchar2(20),sal number,name varchar2(20));

CREATE or REPLACE PACKAGE emp_mgmt AS
 TYPE empcurtyp IS REF CURSOR RETURN emp%ROWTYPE;
 TYPE genericcurtyp IS REF CURSOR;
 PROCEDURE p1(n number,empname out varchar,empsal out number);
END emp_mgmt;
/
CREATE or REPLACE PACKAGE BODY emp_mgmt AS
PROCEDURE p1(n number,empname out varchar,empsal out number) is
 cursor1 empcurtyp;
 v_employee emp%rowtype;
 type nt is table of emp%rowtype;
 v1 nt;
begin
  select * bulk collect into v1 from emp order by sal,hiredate;
  open cursor1 for select * from emp order by empno ;
  for i in 1..n loop
  fetch cursor1 into v_employee;
    if v1[i].sal >= 800 then
      if v1[i].sal >= 900 then
        if v1[i].sal >= 1000 then
          raise notice 'ge1000: % % %',i,v1[i].ename,v1[i].sal;
          forall j in 1..i
            insert into chqin values(v_employee.empno,v_employee.sal,v_employee.ename,v1[j].sal,v1[j].ename||'_'||v_employee.ename);
        else
          raise notice 'lt1000: % % %',i,v1[i].ename,v1[i].sal;
          forall j in 1..i
            insert into chqin values(v_employee.empno,v_employee.sal,v_employee.ename,v1[j].sal,v1[j].ename||'_'||v_employee.ename);
        end if;
      else
        raise notice 'lt900: % % %',i,v1[i].ename,v1[i].sal;
        forall j in 1..i
          insert into chqin values(v_employee.empno,v_employee.sal,v_employee.ename,v1[j].sal,v1[j].ename||'_'||v_employee.ename);
      end if;
    end if;
  end loop;
  close cursor1;
  empname := v_employee.ename;
  empsal := v_employee.sal;
end;
end;
/

declare
n number := 3;
empsal number;
empname varchar;
begin
emp_mgmt.p1(n,empname,empsal);
raise notice '% %',empname,empsal;
end;
/
select * from chqin order by 1,2;

drop table if exists chqin;

-- 1020421696093707211
create schema s1020421696093707211;
set search_path to s1020421696093707211;
create or replace procedure pr_out(a int, b out int) is
begin
  raise notice 'a=%', a;
  raise notice 'b=%', b;

  b=1000;
end;
/

-- error case??
declare
ao int;
bo int;
begin
execute 'call pr_out(:ao, :bo)' using ao, bo;
raise notice 'a=%, b=%', ao, bo;
end;
/

declare
ao int;
bo int;
begin
execute 'call pr_out(:ao, :bo)' using ao, out bo;
raise notice 'a=%, b=%', ao, bo;
end;
/

declare
ao int;
bo int;
begin
execute 'call pr_out(:ao, :bo)' using in ao, out in bo;
raise notice 'a=%, b=%', ao, bo;
end;
/

-- many outs
create or replace procedure pr_out(a int, b out int, c out int, d out int, f in out int) is
begin
  raise notice 'pr_out: a=%, b=%, c=%, d=%, f=%', a, b, c, d, f;
  a = 1;
  b = 2;
  c = 3;
  d = 4;
  f = 5;
end;
/

declare
ao int;
bo int;
co int;
duo int;
fo int default 10;
begin
execute 'call pr_out(:ao, :bo, :co, :duo, :fo)' using in ao, out in bo, out co, out duo, in out fo;
raise notice 'a=%, b=%, c=%, d=%, f=%', ao, bo, co, duo, fo;
execute 'call pr_out(:ao, :bo, :co, :duo, :fo)' using in ao, out in bo, out co, out duo, out fo;
raise notice 'a=%, b=%, c=%, d=%, f=%', ao, bo, co, duo, fo;
end;
/

declare
ao int;
bo int;
co int;
duo int;
fo int default 10;
begin
execute 'call pr_out(:ao, :bo, :co, :duo, :fo)' using in ao, out in bo, out co, out duo, in out fo;
end;
/

create or replace procedure p_1() as
DECLARE
vsql text;
begin
vsql:='call p_2()';
execute vsql;
commit;
END;
/

create or replace procedure p_2() as
DECLARE
vsql text;
begin
  commit;
END;
/

call p_1();

create table pt(a int);
create or replace procedure p_1() as
DECLARE
vsql text;
begin
  vsql:='call p_2();insert into pt values(1);';
  execute vsql;
  commit;
END;
/
call p_1();

create or replace procedure p_1() as
DECLARE
vsql text;
begin
  vsql:='insert into pt values(1);call p_2();insert into pt values(1);';
  execute vsql;
  commit;
END;
/
call p_1();

create or replace procedure p_1() as
DECLARE
vsql text;
begin
  for i in 1..10
  loop
    vsql:='insert into pt values(1);call p_2();insert into pt values(1);';
  end loop;
  execute vsql;
  commit;
END;
/
call p_1();

select * from pt;

--
create or replace procedure mp(a out int) is
begin
  a = 10;
  commit;
end;
/

create or replace procedure cmp() is
declare
  a int;
begin
  execute 'call mp(:a)' using out a;
  raise notice '%', a;
end;
/

call cmp();

drop schema s1020421696093707211 cascade;
reset search_path;

-- 1020421696093862797
drop package emp_mgmt;
create schema s1020421696093862797;
set search_path to s1020421696093862797;
create table emp
       (empno number(4) constraint pk_emp primary key,
        ename varchar2(10),
        job varchar2(9),
        mgr number(4),
        hiredate date,
        sal number(7,2),
        comm number(7,2),
        deptno number(2));
insert into emp values
(7369,'smith','clerk',7902,to_date('17-12-1980','dd-mm-yyyy'),800,null,20);
insert into emp values
(7499,'allen','salesman',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
insert into emp values
(7521,'ward','salesman',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
insert into emp values
(7566,'jones','manager',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,null,20);
insert into emp values
(7654,'martin','salesman',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
insert into emp values
(7698,'blake','manager',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,null,30);
insert into emp values
(7782,'clark','manager',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,null,10);
insert into emp values
(7788,'scott','analyst',7566,to_date('19-04-1987','dd-mm-yyyy'),3000,null,20);
insert into emp values
(7839,'king','president',null,to_date('17-11-1981','dd-mm-yyyy'),5000,null,10);
insert into emp values
(7844,'turner','salesman',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
insert into emp values
(7876,'adams','clerk',7788,to_date('23-05-1987', 'dd-mm-yyyy'),1100,null,20);
insert into emp values
(7900,'james','clerk',7698,to_date('3-12-1981','dd-mm-yyyy'),950,null,30);
insert into emp values
(7902,'ford','analyst',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,null,20);
insert into emp values
(7934,'miller','clerk',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,null,10);

drop package emp_mgmt;
drop schema emp_mgmt cascade;
create or replace package emp_mgmt as
  type emprectyp is record (empno number, sal number);
  empinfo emprectyp;
  f1 number :=7521;
   procedure p2();
end emp_mgmt;
/

create or replace package body emp_mgmt as

  procedure p2() is
    begin
      select empno,sal into empinfo from emp where empno=f1;
      raise notice '%,%',empinfo.empno,empinfo.sal;

    end;

end emp_mgmt;
/

declare
  v1 emp_mgmt.emprectyp;
  v2 emp_mgmt.emprectyp;
begin
  v1:=row(3,1000);
  raise notice '%,%',v1.empno,v1.sal;
  select empno,sal from emp into v1 where empno=emp_mgmt.f1;
  raise notice '%,%',v1.empno,v1.sal;
  select empno,sal from emp into emp_mgmt.empinfo where empno=emp_mgmt.f1;
  raise notice '%,%',emp_mgmt.empinfo.empno,emp_mgmt.empinfo.sal;
  v2:=emp_mgmt.empinfo;
  raise notice '%,%',v2.empno,v2.sal;
  call emp_mgmt.p2();

end;
/

drop package emp_mgmt;
create or replace package emp_mgmt as
  type emprectyp is table of emp.empno%type;
  empinfo emprectyp;
  f1 number :=7521;
end emp_mgmt;
/


declare
  v1 emp_mgmt.emprectyp;
  v2 emp_mgmt.emprectyp;
begin
  select empno from emp order by 1 bulk collect into emp_mgmt.empinfo;
  v2:=emp_mgmt.empinfo;
  raise notice '%',v2[1];
end;
/

drop package emp_mgmt;
drop schema s1020421696093862797 cascade;
reset search_path;

create schema s1020421696094077139;
set search_path to s1020421696094077139;

CREATE OR REPLACE TYPE student_obj_type AS OBJECT (stu_no NUMBER, --学号
                                                    stu_name VARCHAR2(255), --姓名
                                                    stu_sex VARCHAR2(3),--性别
                                                    score NUMBER --成绩
                                                  );
select set_config('search_path', current_setting('search_path') || ', student_obj_type', false) from dual where current_setting('search_path') not like '%student_obj_type%';
drop function if exists get_students_by_sex(varchar2);
CREATE OR REPLACE FUNCTION get_students_by_sex(in_sex VARCHAR2) RETURN varchar2
IS
    type v_arr is varray(10) of student_obj_type;
    my_arr v_arr;
BEGIN
    dbms_output.serveroutput('t');
    IF in_sex = '男'
    THEN
        my_arr := v_arr(student_obj_type(1,'张三','男',98), student_obj_type(2,'李四','男',88));
    ELSE
        my_arr := v_arr(student_obj_type(3,'小红','女',78), student_obj_type(4,'小娟','女',95));
    END IF;

    for i in 1..my_arr.count
    loop
    dbms_output.put_line(my_arr[i].stu_no||'-->'||my_arr[i].stu_name||'-->'||my_arr[i].stu_sex||'-->'||my_arr[i].score);
        end loop;

    RETURN 'DISASSOCIATE';
END get_students_by_sex;
/
select get_students_by_sex('男') from dual;
select get_students_by_sex('女') from dual;
drop function if exists get_students_by_sex(varchar2);
drop type if exists student_obj_type cascade;
drop schema s1020421696094077139 cascade;
reset search_path;

--
create schema s1020421696094192807;
set search_path to s1020421696094192807;
drop table if exists emp;
CREATE TABLE EMP(EMPNO NUMBER(4) CONSTRAINT PK_EMP PRIMARY KEY,ENAME VARCHAR2(20),JOB VARCHAR2(9),MGR NUMBER(4), HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2), DEPTNO NUMBER(4));
INSERT INTO EMP VALUES(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,200);
INSERT INTO EMP VALUES(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,300);
INSERT INTO EMP VALUES(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,300);
INSERT INTO EMP VALUES(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,200);
INSERT INTO EMP VALUES(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,300);
INSERT INTO EMP VALUES(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,300);
INSERT INTO EMP VALUES(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,100);
INSERT INTO EMP VALUES(7788,'SCOTT','ANALYST',7566,to_date('19-04-87','dd-mm-rr'),3000,NULL,200);
INSERT INTO EMP VALUES(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,100);
INSERT INTO EMP VALUES(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,300);
INSERT INTO EMP VALUES(7876,'ADAMS','CLERK',7788,to_date('23-05-87', 'dd-mm-rr'),1100,NULL,200);
INSERT INTO EMP VALUES(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,300);
INSERT INTO EMP VALUES(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,200);
INSERT INTO EMP VALUES(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,100);

-- TestPoint：package里的procedure使用动态数组进行表的插入及更新
drop table if exists emp_t;
create table emp_t (like emp)  distribute by replication;

create or replace package TestArray
as
	TestID int;
	TestName varchar2(100);

	--最好跟查询表返回结果Table结构相同
	type UserList is record (
		ListID varchar2(9),
		ListName varchar2(30),
		ListAddress varchar2(30),
		ListMail varchar2(30)
	);
	
	-- bug
	-- type myList is table of UserList; --定义myList为UserList数组；
	type myList is table of emp%rowtype; --定义myList为UserList数组；
	my_List myList;      --声明变量my_List为数组
	
	procedure Test(TestID in int, TestName in varchar2);
	function ListInsert(ListResult in myList) return boolean;
end TestArray;
/

create or replace package body TestArray is
      
	--方法调用开始
	procedure Test(TestID in int, TestName in varchar2)
	is
		ListAddress varchar2(30) := 'da lian';
		ListMail varchar2(30) :='mzyluokai@hotmail.com';    
		lst_sql varchar2(100);    
	begin
		lst_sql := 'select * from emp';

		EXECUTE IMMEDIATE lst_sql    --动态SQL执行
		BULK COLLECT INTO my_List;   --将查询结果放入数组my_List
		
		-- bug:这样调用就会报错：ERROR:  invalid transaction termination
		if ListInsert(my_List) = true 
		then
			update emp_t set ename = TestName where empno = TestID;
		end if;        
	end Test;

	--带返回值的方法
	function ListInsert(ListResult in myList) return boolean	
	is
		data_count number(10) := 0;
	begin
		data_count :=ListResult.count;

		--循环
		FOR i IN 1..data_count LOOP
			-- bug
			insert into emp_t values(ListResult[i].empno, ListResult[i].ename, ListResult[i].job, ListResult[i].mgr, ListResult[i].hiredate, ListResult[i].sal, ListResult[i].comm, ListResult[i].deptno);
		END LOOP;

		commit;
		return true;

-- 		exception when others then
-- 			rollback;
-- 			return false;
	end ListInsert;
end TestArray;
/
call TestArray.Test(7521, 'update');
select ename from emp_t where empno=7521;
drop package TestArray;
drop schema s1020421696094192807 cascade;
reset search_path;

--
create schema s1020421696094249383;
set search_path to s1020421696094249383;

create or replace  function f_inout(in a int,inout b character varying,out c character varying) return number is
begin
  b:=0;
  c:=0;
  return a+b+c;
exception
  when others then
    b:=-1;
    c:=-1;
    return a+b+c;
end;
/

declare
  b character varying:='';
  c character varying;
  res number;
begin
 perform  f_inout(1,b,c);
 raise notice 'b:%,c:%', b,c;
end;
/

declare
  b character varying:='1234';
  c character varying='4568';
  res number;
begin
 perform  f_inout(1,b,c) + f_inout(1, b, c);
 raise notice 'b:%,c:%', b,c;
end;
/

declare
  b character varying:='1234';
  c character varying='4568';
  res number;
begin
 perform  f_inout(1,b,c) + f_inout(1, b, c) from pg_class;
 raise notice 'b:%,c:%', b,c;
end;
/
drop schema s1020421696094249383 cascade;
reset search_path;

--
declare
  type aa_type is table of integer index by int;
  aa aa_type;                          -- associative array
begin
  dbms_output.serveroutput('t');
  aa(1):=3; aa(2):=6; aa(3):=9;  aa(4):= 12;
  raise notice 'aa.count = %', aa.count;
end;
/

-- 1020421696094277221
drop table if exists emp_cursor_tb2loyees;
create table emp_cursor_tb2loyees(last_name varchar2(20), emp_cursor_tb2loyee_id int, manager_id varchar2(10), hire_date DATE, salary number);
insert into emp_cursor_tb2loyees values('abc',90, 'AD_VP', to_date('2021/06/22', 'yyyy/mm/dd'), 1000);
insert into emp_cursor_tb2loyees values('abd',104, 'AD_VD', to_date('2021/06/23', 'yyyy/mm/dd'), 2000);
insert into emp_cursor_tb2loyees values('abe',103, 'AV_VP', to_date('2021/06/21', 'yyyy/mm/dd'), 3000);
insert into emp_cursor_tb2loyees values('abf',102, 'AB_VP', to_date('2021/06/24', 'yyyy/mm/dd'), 4000);
drop package cur_pkg2;
create or replace package cur_pkg2
as
   CURSOR c4(dept_cursor_tb2_id NUMBER, j_id VARCHAR2)
   IS
      SELECT last_name f_name, hire_date FROM emp_cursor_tb2loyees
      WHERE emp_cursor_tb2loyee_id = dept_cursor_tb2_id AND manager_id = j_id;

    v_emp_cursor_tb2_record c4%ROWTYPE;
        procedure p1(j_id VARCHAR2, dept_cursor_tb2_id NUMBER);
end cur_pkg2;
/
create or replace package body cur_pkg2
as
        procedure p1(j_id VARCHAR2, dept_cursor_tb2_id NUMBER)
        as
        begin
                dbms_output.serveroutput('t');
                OPEN c4(dept_cursor_tb2_id, j_id);
                LOOP
                        FETCH c4 INTO v_emp_cursor_tb2_record;
                        IF c4%FOUND THEN
                                DBMS_OUTPUT.PUT_LINE(v_emp_cursor_tb2_record.f_name||':'||v_emp_cursor_tb2_record.hire_date);
                        ELSE
                                DBMS_OUTPUT.PUT_LINE('finish');
                                EXIT;
                        END IF;
                END LOOP;
                CLOSE c4;
                if c4%ISOPEN then
                        DBMS_OUTPUT.PUT_LINE('failed');
                else
                        DBMS_OUTPUT.PUT_LINE('success');
                end if;
        end;
end;
/
call cur_pkg2.p1('AD_VP', 90);
call cur_pkg2.p1('AD_VP', 90);
drop package cur_pkg2;

--
create schema s1020421696094451065;
set search_path to s1020421696094451065;
drop table if exists res_paycard_month_202108;
CREATE TABLE res_paycard_month_202108 (
    id numeric(16,0) ,
    chk_acct_type character varying(8),
    sequence character varying(20),
    accept_month numeric DEFAULT to_number(to_char(date'2021-09-22', 'MM'::text)),
    file_name character varying(50),
    batch character varying(20),
    countatal numeric(8,0),
    cardflag character(1),
    feeaccountstart character varying(8),
    feeaccountstop character varying(8),
    tradetime character varying(14),
    tradetype character varying(2),
    msisdn character varying(20),
    supplycardkind character varying(2),
    timetamp character varying(14),
    useridentifier character varying(32),
    card_state character varying(2),
    active_flag character(1),
    comp_state character(1) ,
    done_time timestamp(0) without time zone ,
    comp_date timestamp(0) without time zone,
    deal_tag character(1) ,
    remarks character varying(100)
)
 
DISTRIBUTE BY SHARD (id) to GROUP default_group;

drop table if exists res_origin_backup_202108;
drop table if exists res_used_backup_202108;
create table res_origin_backup_202108(paycard_no character varying(20),res_state character(1));
create table res_used_backup_202108(paycard_no character varying(20),res_state character(1) );


drop table if exists res_paycard_vchis;
CREATE TABLE res_paycard_vchis (
    paycard_no character varying(64) NOT NULL,
    vc_state character varying(2),
    done_time timestamp(0) without time zone,
    remarks character varying(200)
)
 
DISTRIBUTE BY SHARD (paycard_no) to GROUP default_group;
insert into res_paycard_vchis(paycard_no) values('001');


drop function if exists p10_test(last_number character, INOUT return_code character varying, INOUT return_msg character varying) ;
CREATE or replace FUNCTION p10_test(last_number character,  return_code in out character varying,  return_msg in out character varying) RETURN varchar2
is
    cur_type REFCURSOR;
    v_paycard_no res_paycard_month_202108.sequence%TYPE;
    v_vc_state res_paycard_month_202108.cardflag%TYPE;
    v_id res_paycard_month_202108.id%TYPE;
    v_card_state varchar(2);
    v_used_card_state varchar(2);
    v_isflag decimal(38);
    v_used_isflag decimal(38);
    v_month_sql varchar(4000);
    v_month varchar(20);
    v_origin_backup_name varchar(100);
    v_used_backup_name varchar(100);
    vc_month_compare REFCURSOR;

BEGIN
    return_code := '0';
    return_msg := 'ok';
    v_month := to_char(to_date('2021-08-01 18:43:11'), 'YYYYMM');
    v_origin_backup_name := 'res_origin_backup_' || v_month;
    v_used_backup_name := 'res_used_backup_' || v_month;
    v_month_sql := ' select a.sequence, a.id,a.cardflag
      from res_paycard_month_' || (v_month || (' a
     where a.deal_tag = ''1''
       and a.chk_acct_type = ''MONTH'' and substr(a.id,-1)=''' || (last_number || '''')));
    OPEN vc_month_compare FOR  EXECUTE v_month_sql;
    LOOP
        FETCH vc_month_compare INTO v_paycard_no, v_id, v_vc_state;
        EXIT WHEN vc_month_compare % notfound;
        --EXIT WHEN  not found;
        v_month_sql := 'UPDATE res_paycard_month_' || (v_month || (' a
         SET a.deal_tag = ''2''
         WHERE a.sequence = ''' || (v_paycard_no || ('''
         and a.id = ''' || (v_id || '''')))));
        EXECUTE v_month_sql;
        v_card_state := '';
        v_used_card_state := '';
        v_month_sql := 'select count(*)
            FROM (SELECT a.paycard_no
            FROM ' || (v_origin_backup_name || (' A
            WHERE a.paycard_no = ''' || (v_paycard_no || ''')')));
        EXECUTE v_month_sql INTO v_isflag;
       -- perform dbms_output.serveroutput('t');
       -- perform DBMS_OUTPUT.PUT_LINE('---v_isflag'||v_isflag);
        v_month_sql := 'select count(*)
            FROM (SELECT d.paycard_no
            FROM ' || (v_used_backup_name || (' D
            WHERE D.paycard_no = ''' || (v_paycard_no || ''')')));
        EXECUTE v_month_sql INTO v_used_isflag;
       -- perform DBMS_OUTPUT.PUT_LINE('---v_used_isflag'||v_used_isflag);
        IF v_isflag = 0
            AND v_used_isflag = 0
        THEN
            v_month_sql := 'UPDATE res_paycard_month_' || (v_month || (' a
             SET a.card_state =''16'',
             a.comp_state = ''3'',
             a.deal_tag = ''3'',
             a.comp_date  = to_date(''2021-08-01 18:43:11''),
             a.remarks    = ''remarks00''
             WHERE a.sequence = ''' || (v_paycard_no || ('''
             and a.id = ''' || (v_id || '''')))));
            EXECUTE v_month_sql;
        ELSE
            IF v_isflag > 0 THEN
                 v_month_sql := 'select res_state
                  FROM (SELECT a.res_state
                  FROM ' || (v_origin_backup_name || (' A
                  WHERE a.paycard_no = ''' || (v_paycard_no || ''')')));
                 EXECUTE v_month_sql INTO v_card_state;
            -- --DBMS_OUTPUT.PUT_LINE(V_MONTH_SQL);
               --  perform DBMS_OUTPUT.PUT_LINE('---v_card_state'||v_card_state);
            ELSE
                IF v_used_isflag > 0 THEN
                    v_month_sql := 'select res_state
                        FROM (SELECT E.res_state
                        FROM ' || (v_used_backup_name || (' E
                        WHERE E.paycard_no = ''' || (v_paycard_no || ''')')));
                   -- perform dbms_output.serveroutput('t');
                    --perform DBMS_OUTPUT.PUT_LINE(V_MONTH_SQL);
                    EXECUTE v_month_sql INTO v_used_card_state;
                  --  perform DBMS_OUTPUT.PUT_LINE(v_used_card_state);
                END IF;
            END IF;

            IF v_vc_state = '0' THEN
                IF v_used_card_state = '4'
                  OR v_used_card_state = 'b'
                THEN
                    v_month_sql := 'UPDATE res_paycard_month_' || (v_month || (' a
                        SET a.card_state =''' || (v_used_card_state || (''',
                        a.comp_state = ''1'',
                        a.comp_date  = to_date(''2021-08-01 18:43:11''),
                        a.deal_tag = ''3'',
                        a.remarks    = ''remarks04b''
                        WHERE a.sequence = ''' || (v_paycard_no || ('''
                        and a.id = ''' || (v_id || '''')))))));
                    -- --DBMS_OUTPUT.put_line('2');
                    EXECUTE v_month_sql;
                ELSE
                    v_month_sql := 'UPDATE res_paycard_month_' || (v_month || (' a
                        SET a.card_state =''' || (nvl(v_used_card_state, v_card_state) || (''',
                        a.comp_state = ''2'',
                        a.comp_date  = to_date(''2021-08-01 18:43:11''),
                        a.deal_tag = ''3'',
                        a.remarks    = ''remarks0''
                        WHERE a.sequence = ''' || (v_paycard_no || ('''
                        and a.id = ''' || (v_id || '''')))))));
                    -- --DBMS_OUTPUT.put_line('3');
                    EXECUTE v_month_sql;
                END IF;

            END IF;

        END IF;
        COMMIT;
    END LOOP;
    CLOSE vc_month_compare;
   -- EXECUTE v_month_sql;
    v_month_sql := 'UPDATE res_paycard_month_' || (v_month || ' a
         SET a.card_state =''17'',
             a.comp_date  = to_date(''2021-08-01 18:43:11''),
             a.remarks    = ''remarks_last''
       WHERE  a.chk_acct_type=''MONTH''
         and a.card_state=''16'' and a.sequence in  (SELECT o.paycard_no FROM res_paycard_vchis o)');
    -- --DBMS_OUTPUT.put_line('123');
    EXECUTE v_month_sql;
    COMMIT;
    return 'ok';
EXCEPTION
    WHEN others THEN
        ROLLBACK;
       -- return_code := TO_CHAR(sqlcode);
        return_code := TO_CHAR(-1);

        return_msg := 'Procedure EXCEPTION: ' || (TO_CHAR(-1) || ('->' || SUBSTR(sqlerrm, 1, 100)));
        return 'EXCEPTION';
END ;
/

delete from res_paycard_month_202108;
delete from res_used_backup_202108;
delete from res_origin_backup_202108;
insert into res_paycard_month_202108 (sequence,id,cardflag,deal_tag,chk_acct_type)values('003',111,'0','1','MONTH');
insert into res_used_backup_202108 values('003','4');

declare
code character varying:='';
msg character varying:='';
res varchar2(200);
begin
res:= p10_test('1',code,msg);
raise notice 'code: %,msg: %',code,msg;
end;
/
select sequence,id,cardflag,deal_tag,chk_acct_type from res_paycard_month_202108;

drop table if exists res_paycard_month_202108;
drop table if exists res_origin_backup_202108;
drop table if exists res_used_backup_202108;
drop table if exists res_paycard_vchis;
drop function  if exists p10_test(last_number character, INOUT return_code character varying, INOUT return_msg character varying);
drop schema s1020421696094451065 cascade;
reset search_path;
drop table if exists a_cursor_table1;
create table a_cursor_table1(id int, mc varchar2(10));
insert into a_cursor_table1 values(1, 'OpenTenBase');
insert into a_cursor_table1 values(2, 'OpenTenBase1');
insert into a_cursor_table1 values(2, 'OpenTenBase2');
create or replace procedure implicate_cursor_pro_bug(v_type varchar2, v_id int)
as
    v_data a_cursor_table1%rowtype;
begin
dbms_output.serveroutput('t');
if v_type = 'select' then
    dbms_output.put_line('select');
    select * into v_data from a_cursor_table1 WHERE id=v_id;
    dbms_output.put_line(v_data.mc);
end if;
exception
    when no_data_found then
        dbms_output.put_line('Sorry No data');
    when too_many_rows then
        dbms_output.put_line('Too Many rows');
end;
/
call implicate_cursor_pro_bug('select', 1);
call implicate_cursor_pro_bug('select', 2);
call implicate_cursor_pro_bug('select', 3);
drop table a_cursor_table1;
drop procedure implicate_cursor_pro_bug;

--
-- grant/revoke package:
-- grant/revoke usage for schema.
-- grant/revoke privi for function
\c - PL_SU

create or replace package pkg is
function f1() return int;
procedure p1();
end;
/

create or replace package body pkg is
  function f1() return int is
  begin
    return 1;
  end;
  procedure p1() is
  begin
    raise notice 'ok';
  end;
end;
/

\c - PL_U1
select pkg.f1();
call pkg.p1();

\c - PL_SU
grant execute on package pkg to pl_u1;

\c - PL_U1
select pkg.f1();
call pkg.p1();

\c - PL_SU
grant execute on package pkg to pl_u1; -- no error
revoke execute on package pkg from pl_u1; -- no error

revoke execute on package pkg from pl_u1;

\c - PL_U1
select pkg.f1();
call pkg.p1();

\c - PL_SU
drop package pkg;

--
\c - PL_SU

create or replace package pkg is
function f1() return int;
procedure p1();
end;
/

create or replace package body pkg is
  function f1() return int is
  begin
    return 1;
  end;
  procedure p1() is
  begin
    raise notice 'ok';
  end;
end;
/

grant create on package pkg to pl_u1;
grant all on package pkg to pl_u1;

\c - PL_SU
drop package pkg;

--
\c - PL_SU

create or replace package pkg is
function f1() return int;
procedure p1();
end;
/

create or replace package body pkg is
  function f1() return int is
  begin
    return 1;
  end;
  procedure p1() is
  begin
    raise notice 'ok';
  end;
end;
/

grant all on package pkg to pl_u1;

\c - PL_U1
select pkg.f1();
call pkg.p1();

\c - PL_SU
revoke all on package pkg from pl_u1;

\c - PL_U1
select pkg.f1();
select pkg.p1();

\c - PL_SU
drop package pkg;
--
create schema s1020421696093600415;
set search_path to s1020421696093600415;
create table emp
       (empno number(4) constraint pk_emp primary key,
        ename varchar2(10),
        job varchar2(9),
        mgr number(4),
        hiredate date,
        sal number(7,2),
        comm number(7,2),
        deptno number(2));
insert into emp values
(7369,'smith','clerk',7902,to_date('17-12-1980','dd-mm-yyyy'),800,null,20);
insert into emp values
(7499,'allen','salesman',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
insert into emp values
(7521,'ward','salesman',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
insert into emp values
(7566,'jones','manager',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,null,20);
insert into emp values
(7654,'martin','salesman',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
insert into emp values
(7698,'blake','manager',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,null,30);
insert into emp values
(7782,'clark','manager',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,null,10);
insert into emp values
(7788,'scott','analyst',7566,to_date('19-04-1987','dd-mm-yyyy'),3000,null,20);
insert into emp values
(7839,'king','president',null,to_date('17-11-1981','dd-mm-yyyy'),5000,null,10);
insert into emp values
(7844,'turner','salesman',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
insert into emp values
(7876,'adams','clerk',7788,to_date('23-05-1987', 'dd-mm-yyyy'),1100,null,20);
insert into emp values
(7900,'james','clerk',7698,to_date('3-12-1981','dd-mm-yyyy'),950,null,30);
insert into emp values
(7902,'ford','analyst',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,null,20);
insert into emp values
(7934,'miller','clerk',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,null,10);

create table dept (deptno number(4), dname varchar2(14), loc varchar2(13));
insert into dept values (10,'accounting','new york');
insert into dept values (20,'research','dallas');
insert into dept values (30,'sales','chicago');
insert into dept values (40,'operations','boston');

CREATE OR REPLACE PACKAGE emp_mgmt AUTHID DEFINER AS
  TYPE cv_type IS REF CURSOR;
  PROCEDURE open_cv1 (discrim  IN  number);
  END ;
/

CREATE OR REPLACE PACKAGE BODY emp_mgmt AS
  PROCEDURE open_cv1 (discrim IN  number) IS
  cv cv_type;
  empinfo emp.empno%type;
  deptinfo dept%rowtype;
  ename varchar2;
  BEGIN
    IF discrim = 1 THEN
    OPEN cv FOR
      SELECT hiredate FROM emp ORDER BY empno;
    ELSIF discrim = 2 THEN
    OPEN cv FOR
      SELECT * FROM dept ORDER BY deptno;
    END IF;
    fetch cv into empinfo;
    raise notice '%',empinfo;
    --DBMS_OUTPUT.PUT_LINE(empinfo);
    close cv;

  END;
end;
/

begin
emp_mgmt.open_cv1(2);
end;
/

drop schema s1020421696093600415 cascade;
reset search_path;

\h create package
\h create package body
\h alter package
\h drop package

--
create schema s1020421696094574295;
set search_path to s1020421696094574295;
drop table if exists departments_cursor_tb1;
create table departments_cursor_tb1(id int, manager_id number, department_id number, department_name varchar2(100));
insert into departments_cursor_tb1 values(2, 7980, 3, 'abd');
insert into departments_cursor_tb1 values(4, 7900, 2, 'CLERK');
insert into departments_cursor_tb1 values(5, 7654, 5, 'SALESMAN');
create or replace procedure implicit_cur_pro(deptno integer)
as
BEGIN
        dbms_output.serveroutput('t');
    DELETE FROM departments_cursor_tb1 WHERE department_id=deptno;
    IF SQL%NOTFOUND THEN
                DBMS_OUTPUT.PUT_LINE(' department_i='||deptno);
                for i in 1..deptno*10
                loop
                        insert into departments_cursor_tb1 values(deptno, deptno*10, deptno);
                        commit;
                end loop;
                DBMS_OUTPUT.PUT_LINE('insert='||SQL%ROWCOUNT);
        elsif SQL%FOUND then
                DBMS_OUTPUT.PUT_LINE('delete='||SQL%ROWCOUNT);
    END IF;

        if SQL%ISOPEN then
                DBMS_OUTPUT.PUT_LINE('error');
        end if;
END;
/
call implicit_cur_pro(1);

-- opentenbase_ora & opentenbase

drop table t;
create table t(t1 int, t2 int);

create or replace procedure fimp(a int) is
var int;
begin
  dbms_output.put_line('start: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);

  insert into t values(1, 2);
  dbms_output.put_line('insert: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
  delete from t;
  dbms_output.put_line('delete: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
  var := 1;
  dbms_output.put_line('assign: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
--
  for i in 1..10
  loop
    insert into t values(1, 2);
    commit;
  end loop;
  dbms_output.put_line('for: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
		
--
  dbms_output.put_line('start: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
  insert into t values(1, 2);
  dbms_output.put_line('insert1: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
  dbms_output.put_line('insert1: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
  commit;
  dbms_output.put_line('insert-commit: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
end;
/

delete from t;
call fimp(1);

create or replace procedure fcomm(a int) is
begin
  delete from t;
  commit;
end;
/
create or replace procedure fouter(a int) is
var int;
begin
  insert into t values(1, 2);
  dbms_output.put_line('insert-commit: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
  fcomm(1);
  dbms_output.put_line('insert-commit: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
end;
/
call fouter(1);

create or replace procedure fcomm(a int) is
begin
  delete from t;
  commit;
  insert into t values(1, 2);
end;
/
call fouter(1);
create or replace procedure fouter(a int) is
var int;
begin
  fcomm(1);
  dbms_output.put_line('insert-commit: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
end;
/
call fouter(1);

create or replace procedure fcomm(a int) is
begin
  dbms_output.put_line('insert-fcomm innercall: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
end;
/
create or replace procedure fouter(a int) is
var int;
begin
  insert into t values(1, 2);
  dbms_output.put_line('insert-1: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
  fcomm(1);
  dbms_output.put_line('insert-2: rowcnt=' || sql%rowcount ||
		' @rowcount=' || case when sql%rowcount is null then '(null)' else '(not null)' end ||
		' @found=' || case when sql%found then 'found' else 'not found' end ||
		' @found=' || case when sql%found is null then '(null)' else '(not null)' end ||
		' @notfound=' || case when sql%notfound then 'not found' else 'found' end ||
		' @isopen=' || case when sql%isopen then 'opened' else 'not opened' end);
end;
/
call fouter(1);

-- 1020421696094875041
drop table  if exists t_table;
create table t_table(f1 int,f2 varchar2(36),f3 varchar2(36));
insert into t_table values(1,'opentenbase','opentenbase');
insert into t_table values(2,'OpenTenBase','OpenTenBase');
insert into t_table values(3,null,null);

drop type ty_row cascade;

create or replace type ty_row as object
(
  col1 int,
  col2 varchar2(36),
  col3 varchar2(36)
);

drop type ty_table cascade;

create or replace type ty_table as table of ty_row;

drop function if exists f_table;
create or replace function f_table  return ty_table as
  v_ty_table ty_table;
begin
  select ty_row(f1,f2,f3) bulk collect  into v_ty_table from t_table;
  return v_ty_table;
end;
/

drop function if exists f1_fun();
create or replace function f1_fun return number is
cnt number;
cursor c1 is select * from table(f_table()) order by 1;
tmp c1%rowtype;
begin
  dbms_output.serveroutput('t');
  open c1  ;
  loop
  fetch c1  into tmp;
   exit when c1%notfound;
   -- perform dbms_output.put_line(tmp.col1::int);
   raise notice '%', tmp.col1;
   end loop;
   close c1;
  return tmp.col1;
end;
/
select f1_fun from dual;

drop function f1_fun;
drop table  t_table;
drop function  f_table;
drop type ty_table cascade;
drop type ty_row cascade;
reset search_path;
drop schema s1020421696094574295 cascade;

-- 1020421696094928897
create user user1 with password 'user1';
create user user2 with password 'user2';
alter role user2 with login;
alter role user1 with login;

set role user2;

drop table t_test;
create table t_test(id int, num int, name character(10));

-- define
CREATE OR REPLACE package pkg_test_func IS   
    -- function
    function addnum(a_1 number,a_2 number) return number;
    -- procedure
    procedure insert_table(istart integer, iend integer);
end;
/

-- body 
CREATE OR REPLACE package body pkg_test_func is

    function addnum(a_1 number,a_2 number) return number is
        num number;
    begin
        num:=a_1+a_2;
        return num;
    end;

    procedure insert_table(istart integer, iend integer)
    as
        v_sql varchar2(1000);
    begin
       -- raise notice 'istart: %, iend: %', istart, iend;
        for i in istart..iend
        loop
            insert into t_test values(i,i,to_char(i));
        end loop;
        commit;

    end;
end;
/

-- TestPoint: 
call pkg_test_func.insert_table(1, 10);
select * from t_test order by id;

-- TestPoint: no grant
reset role;
set role user1;

call pkg_test_func.insert_table(11, 20);
select * from t_test order by id;

-- TestPoint: grant
reset role;
set role user2;
grant EXECUTE on package pkg_test_func to user1;
reset role;
set role user1;
call pkg_test_func.insert_table(21, 30);
select * from t_test order by id;

reset role;
drop package pkg_test_func;
drop table t_test;
drop user user1 ;
drop user user2;

-- 
\c
create schema s1020421696094885261;
set search_path to s1020421696094885261;

drop table if exists t1;
create table t1(id int);
drop function if exists p1_test(v1 number);
CREATE OR REPLACE function p1_test(v1 number) return varchar2
is
BEGIN
IF v1=1 OR v1=2
then
--RAISE EXCEPTION 'x=1 or 2' USING ERRCODE = 'no_data_found';
Raise_application_error(-20324,'Might not change');
END IF;
return 'ok';
EXCEPTION
WHEN others then
insert into t1 values(v1);
commit;
--RAISE EXCEPTION ' exception' USING ERRCODE = 'no_data_found';
RAISE EXCEPTION ' EXCEPTION ' USING ERRCODE = '23505';
END;
/

drop procedure if exists p1();
create or replace procedure p1
is
  num1 number;
  num2 number;
begin
  select count(1) from t1 into num1;
  perform p1_test(1);
 exception
  when others then
    select count(1) from t1 into num2;
    if num2-num1!=1 then
      raise notice 'v1:%,v2:%',num1,num2;
      raise;
    else
      raise notice 'ok';
    end if;
end;
/

call p1();

-- rollback
CREATE OR REPLACE function p1_test(v1 number) return varchar2
is
BEGIN
IF v1=1 OR v1=2
then
--RAISE EXCEPTION 'x=1 or 2' USING ERRCODE = 'no_data_found';
Raise_application_error(-20324,'Might not change');
END IF;
return 'ok';
EXCEPTION
WHEN others then
insert into t1 values(v1);
rollback;
--RAISE EXCEPTION ' exception' USING ERRCODE = 'no_data_found';
RAISE EXCEPTION ' EXCEPTION ' USING ERRCODE = '23505';
END;
/

call p1();
drop schema s1020421696094885261 cascade;
reset search_path;

-- 1020421696095898111
drop table  rownum_pkg_tb1;
create table rownum_pkg_tb1 (id int,id1 varchar2(10) ) ;
insert into rownum_pkg_tb1 values(1,'a1');
insert into rownum_pkg_tb1 values(2,'a2');
insert into rownum_pkg_tb1 values(3,'a3');
insert into rownum_pkg_tb1 values(4,'a4');
insert into rownum_pkg_tb1 values(5,'a5');
insert into rownum_pkg_tb1 values(6,'a6');
insert into rownum_pkg_tb1 values(7,'a7');
insert into rownum_pkg_tb1 values(8,'a8');
insert into rownum_pkg_tb1 values(9,'a9');
insert into rownum_pkg_tb1 values(10,'a10');
insert into rownum_pkg_tb1(id) values(1);
insert into rownum_pkg_tb1(id) values(1);
insert into rownum_pkg_tb1(id) values(1);
insert into rownum_pkg_tb1(id) values(1);
insert into rownum_pkg_tb1(id) values(1);

--
set enable_datanode_row_triggers to on;
drop table emp cascade;
drop table emp_log cascade;
drop table emp1 cascade;

create table emp
(empno number(4) constraint pk_emp2 primary key,
ename varchar2(10),
job varchar2(9),
mgr number(4),
hiredate date,
sal number(7,2),
comm number(7,2),
deptno number(2)) distribute by shard(empno);
create table emp1
(empno number(4) constraint pk_emp1 primary key,
ename varchar2(10),
job varchar2(9),
mgr number(4),
hiredate date,
sal number(7,2),
comm number(7,2),
deptno number(2)) distribute by shard(empno);

insert into emp values
(7369,'smith','clerk',7902,to_date('17-12-1980','dd-mm-yyyy'),800,null,20);
insert into emp values
(7499,'allen','salesman',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
insert into emp values
(7521,'ward','salesman',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
insert into emp values
(7566,'jones','manager',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,null,20);
insert into emp values
(7654,'martin','salesman',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
insert into emp values
(7698,'blake','manager',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,null,30);
insert into emp values
(7782,'clark','manager',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,null,10);
insert into emp values
(7788,'scott','analyst',7566,to_date('19-04-1987','dd-mm-yyyy'),3000,null,20);
insert into emp values
(7839,'king','president',null,to_date('17-11-1981','dd-mm-yyyy'),5000,null,10);
insert into emp values
(7844,'turner','salesman',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
insert into emp values
(7876,'adams','clerk',7788,to_date('23-05-1987', 'dd-mm-yyyy'),1100,null,20);
insert into emp values
(7900,'james','clerk',7698,to_date('3-12-1981','dd-mm-yyyy'),950,null,30);
insert into emp values
(7902,'ford','analyst',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,null,20);
insert into emp values
(7934,'miller','clerk',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,null,10);

create table emp_log (
  emp_id     number ,
  log_date   date,
  new_salary number,
  action     varchar2(20));
insert into emp1 select * from emp;

--testpoint : insert  --  push down
drop package  p1_27;
create or replace package p1_27 is
v1 int:=1;
end;
/

create or replace trigger trigger_op28
  after update of empno,sal on emp1
  for each row
begin
  insert into emp_log (emp_id, log_date, new_salary, action)
 values (:new.empno, to_date('2021-08-09 19:45:45','yyyy-mm-dd hh24:mi:ss'), 800,'new sal'||p1_27.v1 );
 p1_27.v1:=p1_27.v1+1;
end;
/

delete from  emp_log;
delete from emp1;
insert into emp1  select * from emp;

update emp1
set sal = sal + 1000.0
where empno = 7788;

select * from emp_log order by emp_id;

update emp1
set sal = sal + 1000.0
where empno = 7788;

select * from emp_log order by emp_id;

-- should be 3
begin
  raise notice '%',p1_27.v1;
end;
/

drop table emp cascade;
drop table emp_log cascade;
drop table emp1 cascade;
drop package p1_27;

-- 1020421696099831481
create or replace function f_commit(v1 character varying) return varchar2
is
     res varchar2(20);
     num number;
begin
    perform p8_test(v1,'','');

    return res;
exception
    when others then
        raise notice 'error: (%)', sqlerrm;
        return 'exception1';
end;
/

create or replace function p8_test(p_date character varying, inout v_back_id character varying, inout v_back_msg character varying) return varchar2
is

begin
    execute 'drop table a';
--    commit;
  return 'ok';
exception
    when others then
        rollback;
        return 'exception';

end ;
/
select * from f_commit('20210629');
drop function f_commit;
drop function p8_test;

-- 1020421696870900519
drop table if exists emp_debug_tb3loyees_table_debug;
create table emp_debug_tb3loyees_table_debug(last_name varchar2(20), emp_debug_tb3loyee_id int, job_id varchar2(30), salary number(10, 3));
insert into emp_debug_tb3loyees_table_debug values('a', 1, 'ACADFIMKSA_MANGR', 10005.45);
insert into emp_debug_tb3loyees_table_debug values('b', 2, 'b_M_MANGR', 47956);
insert into emp_debug_tb3loyees_table_debug values('c', 3, 'c_cursor', 1457.79);
insert into emp_debug_tb3loyees_table_debug values('d', 4, 'deptcurtyp', 1458.256);
insert into emp_debug_tb3loyees_table_debug values('e', 5, 'SHT_CLERK', 1458.256);
insert into emp_debug_tb3loyees_table_debug values('f', 6, 'SH_CLERK', 1458.256);
insert into emp_debug_tb3loyees_table_debug values('g', 7, 'SA_REP', 1458.256);
insert into emp_debug_tb3loyees_table_debug values('h', 8, 'AD_REP', 1458.256);

create or replace procedure ref_debug_pro3pro
as
begin
        drop table if exists a_partition_tb;
        create table a_partition_tb (like emp_debug_tb3loyees_table_debug) partition by range (emp_debug_tb3loyee_id) begin (1) step (5) partitions (2)   ;
end;
/
call ref_debug_pro3pro();
call ref_debug_pro3pro();
drop procedure ref_debug_pro3pro;
drop table if exists emp_debug_tb3loyees_table_debug;
--1020421696102453719
create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
 return status;
 end;
/
select 1;
create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
 return status;
 end;
	/
select 1;
create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
 return status;
 end;
/   
select 1;

create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
 return status;
 end;
   /   
select 1;

create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
 return status;
 end;
   /   -- test
select 1;
-- error
create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
 return status;
 end;
/* a */   /
/
select 1;
--
create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
  1 / 1;
 return status;
 end;
/
select 1;
--
create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
/
 return status;
 end;
/
select 1;

create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
/1
 return status;
 end;
/
select 1;

create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
   /1
 return status;
 end;
/
select 1;

create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
 status = 1/1
;
 return status;
 end;
/
select 1;

create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
 status = 1
/1
;
 return status;
 end;
/
select 1;

create or replace function ttff return int
 as
 status int;
 begin
 status := 0;
 status = 1
/
1
;
 return status;
 end;
/
select 1;

\c postgres
-- 1020418349877220603
-- Support call function with in args which has assign type coercion in opentenbase_ora mode.
create or replace procedure test_call_assign_cast(a_int int)
as
$$
begin
   raise notice '%', a_int;
end;
$$ language plpgsql;

do
$$
declare
a_numeric numeric:=1;
begin
call test_call_assign_cast(a_numeric);
end;
$$ language plpgsql;

do
$$
declare
a_float float:=1.0;
begin
call test_call_assign_cast(a_float);
end;
$$ language plpgsql;

do
$$
declare
a_real real:=1.0;
begin
call test_call_assign_cast(a_real);
end;
$$ language plpgsql;

do
$$
declare
a_numeric NUMBER:=1;
begin
call test_call_assign_cast(a_numeric);
end;
$$ language plpgsql;

do
$$
declare
a_float BINARY_DOUBLE:=1.0;
begin
call test_call_assign_cast(a_float);
end;
$$ language plpgsql;

do
$$
declare
a_real BINARY_FLOAT:=1.0;
begin
call test_call_assign_cast(a_real);
end;
$$ language plpgsql;

do
$$
declare
a_numeric numeric:=1;
begin
call test_call_assign_cast(a_numeric);
end;
$$ language plpgsql;

do
$$
declare
a_float float:=1.0;
begin
call test_call_assign_cast(a_float);
end;
$$ language plpgsql;

do
$$
declare
a_real real:=1.0;
begin
call test_call_assign_cast(a_real);
end;
$$ language plpgsql;

do
$$
declare
a_numeric NUMBER:=1;
begin
call test_call_assign_cast(a_numeric);
end;
$$ language plpgsql;

do
$$
declare
a_float BINARY_DOUBLE:=1.0;
begin
call test_call_assign_cast(a_float);
end;
$$ language plpgsql;

do
$$
declare
a_real BINARY_FLOAT:=1.0;
begin
call test_call_assign_cast(a_real);
end;
$$ language plpgsql;
drop procedure test_call_assign_cast(a_int int);

\c regression_ora
create or replace procedure test_call_assign_cast(a_int inout int, a_out_int out int)
is
begin
   a_int = 2;
   raise notice '%', a_int;
   a_out_int = 3;
   raise notice '%', a_out_int;
end;
/
declare
a_num numeric:=1;
begin
call test_call_assign_cast(a_num, a_num);
raise notice '%', a_num;
end;
/
declare
a_float float:=1.0;
begin
call test_call_assign_cast(a_float, a_float);
raise notice '%', a_float;
end;
/
declare
a_real real:=1.0;
begin
call test_call_assign_cast(a_real, a_real);
raise notice '%', a_real;
end;
/
declare
a_num NUMBER:=1;
begin
call test_call_assign_cast(a_num, a_num);
raise notice '%', a_num;
end;
/
declare
a_float BINARY_DOUBLE:=1.0;
begin
call test_call_assign_cast(a_float, a_float);
raise notice '%', a_float;
end;
/
declare
a_real BINARY_FLOAT:=1.0;
begin
call test_call_assign_cast(a_real, a_real);
raise notice '%', a_real;
end;
/
declare
a_num numeric:=1;
begin
call test_call_assign_cast(a_num, a_num);
raise notice '%', a_num;
end;
/
declare
a_float float:=1.0;
begin
call test_call_assign_cast(a_float, a_float);
raise notice '%', a_float;
end;
/
declare
a_real real:=1.0;
begin
call test_call_assign_cast(a_real, a_real);
raise notice '%', a_real;
end;
/
declare
a_num NUMBER:=1;
begin
call test_call_assign_cast(a_num, a_num);
raise notice '%', a_num;
end;
/
declare
a_float BINARY_DOUBLE:=1.0;
begin
call test_call_assign_cast(a_float, a_float);
raise notice '%', a_float;
end;
/
declare
a_real BINARY_FLOAT:=1.0;
begin
call test_call_assign_cast(a_real, a_real);
raise notice '%', a_real;
end;
/
drop procedure test_call_assign_cast(a_int inout int, a_out_int out int);

create or replace procedure test_call_assign_cast(a_real inout BINARY_FLOAT, a_out_real out BINARY_FLOAT)
is
begin
   a_real = 2.0;
   raise notice '%', a_real;
   a_out_real = 3.0;
   raise notice '%', a_out_real;
end;
/
declare
a_float BINARY_DOUBLE:=1.0;
begin
call test_call_assign_cast(a_float, a_float);
raise notice '%', a_float;
end;
/
declare
a_float float:=1.0;
begin
call test_call_assign_cast(a_float, a_float);
raise notice '%', a_float;
end;
/
declare
a_float BINARY_DOUBLE:=1.0;
begin
call test_call_assign_cast(a_float, a_float);
raise notice '%', a_float;
end;
/
declare
a_float float:=1.0;
begin
call test_call_assign_cast(a_float, a_float);
raise notice '%', a_float;
end;
/
drop procedure test_call_assign_cast(a_real inout BINARY_FLOAT, a_out_int out BINARY_FLOAT);

--clean up

-- 1020418349875823791
-- The return cursor of a procedure/function can be used out the procedure/function.
\c
-- setup
set plpgsql_hold_cursor to on;
create table a_rtcursor_rel(a int, b int);
insert into a_rtcursor_rel values (1,1), (2,2), (3,3);
close ret;
close ret1;
close ret2;
-- test call a procedure with a cursor INOUT arg.
create or replace procedure a_rtcursor_p(rtcursor IN OUT refcursor default null)
is
begin
OPEN rtcursor FOR
  SELECT * from a_rtcursor_rel order by a;
end;
/
-- test it within a transaction
begin;
-- call
call a_rtcursor_p('ret');
-- fetch the cursor
fetch all in "ret";
-- clean
close "ret";
commit;
-- test it in different transaction
begin;
-- call
call a_rtcursor_p('ret');
commit;
begin;
-- fetch the cursor
fetch all in "ret";
commit;
begin;
-- clean
close "ret";
commit;
drop procedure a_rtcursor_p(rtcursor IN OUT refcursor);

-- test call a function with a cursor INTOUT arg.
create or replace function a_rtcursor_f(rtcursor INOUT refcursor)
as $$
begin
OPEN rtcursor FOR
  SELECT * from a_rtcursor_rel order by a;
end;
$$ language default_plsql;
-- test it within a transaction
begin;
-- run it
select a_rtcursor_f('ret');
-- fetch the cursor
fetch all in "ret";
-- clean
close "ret";
commit;
-- test it in different transaction
begin;
-- run it
select a_rtcursor_f('ret');
commit;
begin;
-- fetch the cursor
fetch all in "ret";
commit;
begin;
-- clean
close "ret";
commit;
drop function a_rtcursor_f;

-- test call a procedure with a cursor OUT arg.
create or replace procedure a_rtcursor_p(rtcursor OUT refcursor)
is
declare
ret cursor is SELECT * from a_rtcursor_rel order by a;
begin
open ret;
rtcursor = ret;
end;
/
-- test it within a transaction
begin;
-- call
call a_rtcursor_p('ret');
-- fetch the cursor
fetch all in ret;
-- clean
close ret;
commit;
-- test it in different transaction
begin;
-- call
call a_rtcursor_p('ret');
commit;
begin;
-- fetch the cursor
fetch all in ret;
commit;
begin;
-- clean
close ret;
commit;
drop procedure a_rtcursor_p(rtcursor OUT refcursor);

-- test call a function with a cursor OUT arg.
create or replace function a_rtcursor_f(rtcursor OUT refcursor)
as $$
declare
ret cursor is SELECT * from a_rtcursor_rel order by a;
begin
open ret;
rtcursor = ret;
end;
$$ language default_plsql;
-- test it within a transaction
begin;
-- run it
select a_rtcursor_f('ret');
-- fetch the cursor
fetch all in ret;
-- clean
close ret;
commit;
-- test it in different transaction
begin;
-- run it
select a_rtcursor_f('ret');
commit;
begin;
-- fetch the cursor
fetch all in ret;
commit;
begin;
-- clean
close ret;
commit;
drop function a_rtcursor_f;

-- test call a procedure with a cursor INOUT arg, a cursor OUT arg and two IN args.
create or replace procedure a_rtcursor_p(a integer, b integer, rtcursor1 IN OUT refcursor default null, rtcursor2 OUT refcursor)
is
declare
ret2 cursor is SELECT * from a_rtcursor_rel order by a;
begin
INSERT INTO a_rtcursor_rel VALUES (a,b);
OPEN rtcursor1 FOR
  SELECT * from a_rtcursor_rel order by a;
OPEN ret2;
rtcursor2 = ret2;
end;
/
-- test it within a transaction
begin;
-- call
call a_rtcursor_p(1, 1, 'ret1', 'ret2');
-- fetch the cursor
fetch all in "ret1";
fetch all in ret2;
-- clean
close "ret1";
close ret2;
commit;
-- test it in different transaction
begin;
-- call
call a_rtcursor_p(1, 1, 'ret1', 'ret2');
commit;
begin;
-- fetch the cursors
fetch all in "ret1";
commit;
begin;
fetch all in ret2;
commit;
begin;
-- clean
close "ret1";
commit;
begin;
close ret2;
commit;
drop procedure a_rtcursor_p(a integer, b integer, rtcursor1 IN OUT refcursor, rtcursor2 OUT refcursor);

-- test call a function with a cursor INOUT arg, a cursor OUT arg and two IN args.
create or replace function a_rtcursor_f(a integer, b integer, rtcursor1 IN OUT refcursor default null, rtcursor2 OUT refcursor)
as $$
declare
ret2 cursor is SELECT * from a_rtcursor_rel order by a;
begin
INSERT INTO a_rtcursor_rel VALUES (a,b);
OPEN rtcursor1 FOR
  SELECT * from a_rtcursor_rel order by a;
OPEN ret2;
rtcursor2 = ret2;
end;
$$ language default_plsql;
-- test it within a transaction
begin;
-- call
select a_rtcursor_f(1, 1, 'ret1', 'ret2');
-- fetch the cursor
fetch all in "ret1";
fetch all in ret2;
-- clean
close "ret1";
close ret2;
commit;
-- test it in different transaction
begin;
-- call
select a_rtcursor_f(1, 1, 'ret1', 'ret2');
commit;
begin;
-- fetch the cursor
fetch all in "ret1";
commit;
begin;
fetch all in ret2;
commit;
begin;
-- clean
close "ret1";
commit;
begin;
close ret2;
commit;
drop function a_rtcursor_f;

-- test call a function with a cursor return and a cursor IN arg.
create or replace function a_rtcursor_f(rtcursor1 IN refcursor default null)
returns refcursor
as $$
begin
OPEN rtcursor1 FOR
  SELECT * from a_rtcursor_rel order by a;
return rtcursor1;
end;
$$ language default_plsql;
-- test it within a transaction
begin;
-- call
select a_rtcursor_f('ret');
-- fetch the cursor
fetch all in "ret";
-- clean
close "ret";
commit;
-- test it in different transaction
begin;
-- call
select a_rtcursor_f('ret');
commit;
begin;
-- fetch the cursor
fetch all in "ret";
commit;
begin;
-- clean
close "ret";
commit;
drop function a_rtcursor_f;

-- test call a function with a cursor return and a diff type (int) arg.
create or replace function a_rtcursor_f(a int, id out int)
returns refcursor
as $$
declare
ret cursor for SELECT * from a_rtcursor_rel order by a;
begin
id := a;
raise notice '%', id;
OPEN ret;
return ret;
end;
$$ language default_plsql;
-- test it within a transaction
begin;
-- call
declare
a int;
b refcursor;
begin
b = a_rtcursor_f(1, a);
end;
/
-- fetch the cursor
fetch all in ret;
-- clean
close ret;
commit;
-- test it in different transaction
begin;
-- call
declare
a int;
b refcursor;
begin
b = a_rtcursor_f(1, a);
end;
/
commit;
begin;
-- fetch the cursor
fetch all in ret;
commit;
begin;
-- clean
close ret;
commit;
drop function a_rtcursor_f;

-- clean up for 1020418349875823791 case
drop table a_rtcursor_rel;

-- test multi-line comments
CREATE OR REPLACE PACKAGE test AS
-- multi
-- line
-- comments
END test;
/

CREATE OR REPLACE PACKAGE BODY test AS
-- multi
-- line
-- comments
	var1 varchar2(4096) := 'test';
	function func return varchar2 is
    var2 INTEGER;
  begin 
    var2 = 12;
  return gstr;
  end func;
END test;
/
/*
 * test end the procedures named unreserved keywords in 
 * format of 'END {unreserved_keyword};'
 */
 -- log
CREATE OR REPLACE PACKAGE PLOG IS 
PROCEDURE log();
END PLOG;
/

CREATE OR REPLACE PACKAGE BODY PLOG IS 
PROCEDURE log() is
BEGIN
    raise notice 'function:log';
END log;
END PLOG;
/
call plog.log();

-- error
CREATE OR REPLACE PACKAGE PLOG IS 
PROCEDURE error();
END PLOG;
/

CREATE OR REPLACE PACKAGE BODY PLOG IS 
PROCEDURE error() IS
BEGIN
    raise notice 'function:error';
END error;
END PLOG;
/
call plog.error();

-- debug
CREATE OR REPLACE PACKAGE PLOG IS 
PROCEDURE debug();
END PLOG;
/

CREATE OR REPLACE PACKAGE BODY PLOG IS 
PROCEDURE debug() IS
BEGIN
    raise notice 'function:debug';
END debug;
END PLOG;
/
call plog.debug();

--TAPD:882507909
drop sequence if exists slog1;

create or replace package ptest1 as
function test_sequence(p1 out number) return number;
function ret_func(p1 out number) return varchar2;
procedure pproc();
end ptest1;
/

create or replace package body ptest1 as
function test_sequence(p1 out number) return number 
as
temp number;
begin
	 raise notice '**********ptest1 test sequence********';
	 select slog1.nextval into temp from dual;
	 raise notice '**********ptest1 test sequence********';
     return temp;
end;

procedure pproc 
as
temp number;
begin
 	raise notice '**********ptest1 pproc********';
 	temp := 1;
	raise notice '**********ptest1 pproc********';
end;

function ret_func(p1 out number) return varchar2
as
res varchar2(200);
begin
	raise notice '**********ptest1 ret_func********';
	p1 := 100 + p1;
	res := '122';
	raise notice '**********ptest1 ret_func********';
return res;
end;

end ptest1;
/

declare
p1 number := 10;
temp number;
ret_var varchar2(100) := ptest1.ret_func(p1);
begin
     raise notice '**********inline code block********';
	 ptest1.pproc();
	 temp := ptest1.test_sequence(p1);
	 raise notice '% --- % --- %',p1,temp,ret_var;
     raise notice '**********inline code block********';
end;
/
create sequence slog1 minvalue 1 maxvalue 999999999999999999 increment by 1 start with 168255439 cache 20  cycle ;

declare
p1 number := 10;
temp number;
ret_var varchar2(100) := ptest1.ret_func(p1);
begin
     raise notice '**********inline code block********';
	 ptest1.pproc();
	 temp := ptest1.test_sequence(p1);
	 raise notice '% --- % --- %',p1,temp,ret_var;
     raise notice '**********inline code block********';
end;
/
drop sequence slog1;
drop package ptest1;

-- type conversion
drop package pkgcv01;
create package pkgcv01 as
    procedure proc1(p1 clob, p2 opentenbase_ora.date, p3 numeric);
end pkgcv01;
/
create package body pkgcv01 as
    procedure proc1(p1 clob, p2 opentenbase_ora.date, p3 numeric) as
	  aa text;
	begin
		raise notice '%', p1;
		raise notice '%', p2;
		raise notice '%', p3;
	end;
end pkgcv01;
/

do $$
declare
  a varchar;
  b varchar2;
  c timestamp;
  d pg_catalog.timestamp;
begin
  a := 'a';
  b := 'b';
  --c := '2000-01-01 00:00:00 BC'::timestamp;
  c := '2000-01-01 00:00:00'::timestamp;
  d := '2000-01-01 00:00:00 BC'::pg_catalog.timestamp;
  pkgcv01.proc1(a, c, 1);
  pkgcv01.proc1(b, d, 2);
end;
$$;
drop package pkgcv01;
DROP TABLE if exists rqg_table2 cascade;
DROP TABLE T_TYP_20230731;
CREATE TABLE T_TYP_20230731(C0 INT, F1 NUMBER, F2 VARCHAR2(200));
DROP TABLE log_20230731;
create table log_20230731(f1 number,f2 varchar2(200),f3 number, f4 number);
CREATE TABLE rqg_table2 (
c0 int,
f1 number,
f2 varchar2(200))   distribute by replication;
alter table rqg_table2 alter column c0 drop not null;
DELETE FROM rqg_table2 ;
INSERT INTO rqg_table2 VALUES(1,1,1) ;
CREATE OR REPLACE PROCEDURE PROC_NT_20230803 ( v1 number, v2 varchar2, v3 in number, v4 in number ) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
BEGIN var1:=v2+var4 ;
 var1:=v1+var1;
 var1:=nvl(var4,0)+nvl(var3,0);
var2:=v1;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=v1 THEN var3:=var4;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=var3 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var3:=var4+v3 ;
 var4:=var4+v4;
 var1:=nvl(var1,0)+nvl(v3,0);
var2:=var4;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=var3 OR var2=var2 THEN var1:=v4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN var4:=INNERFUN(var1,var2,var3,var4);
 var1:=v4+var1 ;
 var4:=v3+v2;
 var4:=nvl(var3,0)+nvl(var1,0);
var2:=var2;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=v3 THEN var3:=var4;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=var1 ;
 EXCEPTION WHEN OTHERS THEN var3:=var1;
var1:=var1+10 ;
 var2:=var4;
var2:=(v1 || v1 );
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=v3 THEN var1:=var1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 RETURN;
 END ;
FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
BEGIN var4:=v3+v1 ;
 var3:=var3+var1;
 var4:=nvl(v4,0)+nvl(v1,0);
var2:=var3;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=v3 OR var2=v3 THEN var4:=var1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=v4 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var1:=var4;
var4:=var3+10 ;
 var2:=var4;
var2:=(var1 || v1 );
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=var1 OR var2=v1 THEN var4:=var4;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN var3:=INNERFUN(var1,var2,var3,var4);
 var4:=var1+v1 ;
 var4:=var3+v4;
 var4:=nvl(v2,0)+nvl(var4,0);
var2:=var4;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=var3 OR var2=v1 THEN var1:=v1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=v2 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var3:=var3;
var1:=v4+10 ;
 var2:=v3;
var2:=(v2 || var3 );
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var3 OR var2=var4 THEN var1:=v2;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var1:=INNERFUN(var1,var2,var3,var4);
 var4:=v4;
var4:=var1+10 ;
 var2:=v4;
var2:=(var3 || v4 );
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=var4 THEN var1:=v4;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=v3 ;
 EXCEPTION WHEN OTHERS THEN var1:=v1+v3 ;
 var1:=v3+v2;
 var3:=nvl(v3,0)+nvl(v1,0);
var2:=var4;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=v4 OR var2=var4 THEN var3:=v1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 RETURN;
 END;
 BEGIN var4:=var3+var1 ;
 var1:=v1+var1;
 var1:=nvl(v4,0)+nvl(v3,0);
var2:=var4;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=var1 THEN var4:=var3;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=v2 ;
 EXCEPTION WHEN OTHERS THEN var4:=v2+v3 ;
 var4:=var1+v1;
 var3:=nvl(v3,0)+nvl(v3,0);
var2:=v1;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=v1 OR var2=v4 THEN var3:=v4;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 END IF ;
 RETURN;
 END ;
FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 BEGIN var4:=v2+v1 ;
 var3:=v3+var4;
 var4:=nvl(v1,0)+nvl(var4,0);
var2:=var3;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=v3 THEN var3:=var3;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=v3 ;
 EXCEPTION WHEN OTHERS THEN var1:=var3;
var4:=var3+10 ;
 var2:=v1;
var2:=(var3 || var3 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=v2 OR var2=v2 THEN var3:=v3;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 RETURN;
 END ;
FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 BEGIN var1:=v4;
var1:=v3+10 ;
 var2:=var4;
var2:=(var2 || v3 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=v2 OR var2=var1 THEN var4:=v4;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=v4 ;
 EXCEPTION WHEN OTHERS THEN var3:=v2+var3 ;
 var4:=v3+v3;
 var4:=nvl(var3,0)+nvl(var4,0);
var2:=v3;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v2 OR var2=var3 THEN var1:=var4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 RETURN;
 END;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var4:=v3+var1 ;
 var1:=var4+v4;
 var4:=nvl(v1,0)+nvl(v4,0);
var2:=v1;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v3 OR var2=var1 THEN var3:=var3;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=var1 ;
 EXCEPTION WHEN OTHERS THEN var1:=var1;
var3:=v3+10 ;
 var2:=v3;
var2:=(v4 || v1 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var3 OR var2=v1 THEN var3:=var1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 RETURN;
 END ;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var3:=v2+var3 ;
 var4:=v3+var3;
 var3:=nvl(var1,0)+nvl(var1,0);
var2:=v2;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=var4 THEN var4:=v1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=v1 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var4:=var4;
var3:=v2+10 ;
 var2:=v1;
var2:=(v2 || v1 );
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=v4 THEN var1:=var3;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var4:=INNERFUN(var1,var2,var3,var4);
 var1:=v1;
var3:=v2+10 ;
 var2:=v2;
var2:=(v4 || v3 );
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=v4 OR var2=v2 THEN var4:=v2;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=v3 ;
 EXCEPTION WHEN OTHERS THEN var3:=var3+v2 ;
 var4:=v1+v2;
 var4:=nvl(var3,0)+nvl(var3,0);
var2:=v3;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v3 OR var2=var1 THEN var4:=v4;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 END IF ;
 RETURN;
 END ;
FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
BEGIN var4:=var1;
var1:=v4+10 ;
 var2:=var3;
var2:=(var2 || var1 );
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=var3 OR var2=var1 THEN var4:=v3;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE f1=var3 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var1:=v4;
var4:=v2+10 ;
 var2:=var2;
var2:=(var4 || var1 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=v4 OR var2=v3 THEN var3:=var3;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var4:=INNERFUN(var1,var2,var3,var4);
 var1:=v1+var1 ;
 var3:=v1+var1;
 var4:=nvl(v3,0)+nvl(v1,0);
var2:=v3;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var3 OR var2=v4 THEN var4:=v2;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 END IF ;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE f1=v1 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var4:=v4+var3 ;
 var4:=var3+var1;
 var3:=nvl(var4,0)+nvl(var4,0);
var2:=var1;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v4 OR var2=v1 THEN var1:=v1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var3:=INNERFUN(var1,var2,var3,var4);
 var3:=var1;
var3:=v1+10 ;
 var2:=v1;
var2:=(var4 || v2 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v4 OR var2=var1 THEN var3:=var4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 END IF ;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE f1=v4 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var1:=v1+v4 ;
 var3:=var1+v2;
 var1:=nvl(v4,0)+nvl(var4,0);
var2:=v2;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=var4 OR var2=var3 THEN var4:=var1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 BEGIN var4:=v3;
var4:=v4+10 ;
 var2:=v3;
var2:=(var3 || v3 );
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=var3 OR var2=var2 THEN var3:=v4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=v3 ;
 EXCEPTION WHEN OTHERS THEN var3:=v2+v3 ;
 var4:=v1+v3;
 var1:=nvl(v4,0)+nvl(v3,0);
var2:=v1;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=v1 OR var2=var2 THEN var4:=var1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 END IF ;
 RETURN;
 END ;
FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 BEGIN var3:=v3;
var3:=v3+10 ;
 var2:=var2;
var2:=(var2 || v2 );
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var3 OR var2=v4 THEN var1:=var3;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 END IF ;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE f1=var3 ;
 EXCEPTION WHEN OTHERS THEN var1:=v1;
var3:=v2+10 ;
 var2:=v4;
var2:=(v1 || v3 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v3 OR var2=v1 THEN var4:=var3;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 RETURN;
 END ;
FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
BEGIN var3:=v2+var4 ;
 var4:=var3+var3;
 var1:=nvl(var4,0)+nvl(var1,0);
var2:=var2;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=v4 THEN var4:=var4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE f1=var4 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var3:=v1+var3 ;
 var3:=v2+var3;
 var4:=nvl(v2,0)+nvl(v2,0);
var2:=var1;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=v3 OR var2=var4 THEN var1:=v3;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN var4:=INNERFUN(var1,var2,var3,var4);
 var1:=v1;
var3:=v1+10 ;
 var2:=var3;
var2:=(var1 || var3 );
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v3 OR var2=v4 THEN var1:=v4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=var1 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var4:=v1+v1 ;
 var3:=var4+var4;
 var4:=nvl(var1,0)+nvl(v4,0);
var2:=v4;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var1 OR var2=v3 THEN var3:=var1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var3:=INNERFUN(var1,var2,var3,var4);
 var4:=var3+v2 ;
 var3:=v4+v3;
 var3:=nvl(var4,0)+nvl(var4,0);
var2:=var3;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var3 OR var2=var3 THEN var4:=var3;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=v2 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var1:=var3+v1 ;
 var1:=v3+var3;
 var4:=nvl(v1,0)+nvl(v4,0);
var2:=v1;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v4 OR var2=var2 THEN var3:=var3;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN var3:=INNERFUN(var1,var2,var3,var4);
 var1:=v2+v3 ;
 var4:=var4+v4;
 var4:=nvl(var1,0)+nvl(var3,0);
var2:=var2;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=var3 OR var2=v3 THEN var1:=var1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=var1 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var4:=var3;
var4:=v2+10 ;
 var2:=var3;
var2:=(var3 || var1 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v4 OR var2=v3 THEN var3:=v4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN var3:=INNERFUN(var1,var2,var3,var4);
 var3:=var1+v2 ;
 var3:=v1+v3;
 var4:=nvl(var1,0)+nvl(v4,0);
var2:=var3;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=var1 OR var2=var1 THEN var3:=var3;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 END IF ;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE f1=v3 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var4:=v1;
var1:=v1+10 ;
 var2:=v1;
var2:=(v3 || var3 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var3 OR var2=v3 THEN var4:=var4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var4:=INNERFUN(var1,var2,var3,var4);
 var3:=v3+v3 ;
 var1:=v3+v1;
 var1:=nvl(var4,0)+nvl(var4,0);
var2:=v2;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var3 OR var2=var3 THEN var1:=var3;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=v4 ;
 EXCEPTION WHEN OTHERS THEN var4:=v1;
var1:=var1+10 ;
 var2:=v1;
var2:=(var1 || var2 );
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var1 OR var2=v4 THEN var3:=var1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 RETURN;
 END ;
FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
 PROCEDURE INNERPROC(v1 number, v2 varchar2, v3 in number, v4 in number) IS var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 FUNCTION INNERFUN (v1 number, v2 varchar2, v3 in number, v4 in number) RETURN NUMBER IS 
 var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number ;
BEGIN var4:=v2;
var3:=var1+10 ;
 var2:=v1;
var2:=(var3 || v2 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=v2 OR var2=v3 THEN var1:=var1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=v2 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var1:=var3+v1 ;
 var1:=v3+v2;
 var1:=nvl(v1,0)+nvl(var3,0);
var2:=var2;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v2 OR var2=var1 THEN var3:=var1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN var1:=INNERFUN(var1,var2,var3,var4);
 var1:=v1+v4 ;
 var1:=var3+v3;
 var3:=nvl(v2,0)+nvl(v2,0);
var2:=v2;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=v1 OR var2=v4 THEN var4:=var3;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE f1=var3 ;
 EXCEPTION WHEN OTHERS THEN var1:=var4;
var3:=v3+10 ;
 var2:=v4;
var2:=(var4 || v3 );
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var2;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=var4 OR var2=v3 THEN var1:=v4;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 END IF ;
 RETURN;
 END ;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var3:=var1+var1 ;
 var1:=v1+var4;
 var3:=nvl(var4,0)+nvl(v1,0);
var2:=var2;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var4=v3 OR var2=var3 THEN var3:=v3;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=v4 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var3:=v2;
var1:=var3+10 ;
 var2:=var1;
var2:=(var2 || v4 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=v2 OR var2=v1 THEN var4:=var4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var2;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN var3:=INNERFUN(var1,var2,var3,var4);
 var4:=var3;
var3:=v4+10 ;
 var2:=var1;
var2:=(var3 || v2 );
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=var1 OR var2=var4 THEN var3:=v4;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v2;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE f1=v2 ;
 RETURN 1 ;
 EXCEPTION WHEN OTHERS THEN var4:=v2;
var4:=v3+10 ;
 var2:=var2;
var2:=(v1 || var3 );
 SELECT F1 INTO var3 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var3=var1 OR var2=var3 THEN var1:=var1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 RETURN -1;
 END ;
 BEGIN INNERPROC(var1,var2,var3,var4);
 var1:=INNERFUN(var1,var2,var3,var4);
 var1:=v3;
var3:=v2+10 ;
 var2:=var4;
var2:=(v1 || v3 );
 SELECT F1 INTO var4 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var4;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=var4 OR var2=v3 THEN var1:=v4;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=var1;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v1;
 COMMIT;
 END IF ;
 SELECT F1 INTO var3 FROM rqg_table2 WHERE f1=var4 ;
 EXCEPTION WHEN OTHERS THEN var1:=v4+v4 ;
 var3:=var4+v3;
 var4:=nvl(var4,0)+nvl(v4,0);
var2:=var4;
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=var4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=var1;
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v3 OR var2=var3 THEN var1:=v3;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=var3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 END IF ;
 RETURN;
 END;
 BEGIN var4:=v4+v2 ;
 var3:=v3+var1;
 var4:=nvl(v1,0)+nvl(v1,0);
var2:=v2;
 var4:=INNERFUN(var1,var2,var3,var4);
 INNERPROC(var1,var2,var3,var4);
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var3 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v2;
 COMMIT;
 INSERT INTO LOG_20230731 VALUES(var1,var2,var3,var4);
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v4 OR var2=var2 THEN var1:=v2;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v3;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 END IF ;
 SELECT F1 INTO var4 FROM rqg_table2 WHERE f1=v2 ;
 EXCEPTION WHEN OTHERS THEN var1:=v2;
var1:=v2+10 ;
 var2:=var2;
var2:=(var2 || v4 );
 var1:=INNERFUN(var1,var2,var3,var4);
 INNERPROC(var1,var2,var3,var4);
 SELECT F1 INTO var1 FROM rqg_table2 WHERE C0=1;
 SELECT F2 INTO var2 FROM rqg_table2 WHERE C0=1;
 UPDATE rqg_table2 SET f1=var4 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v3;
 COMMIT;
 INSERT INTO LOG_20230731 VALUES(var1,var2,var3,var4);
 COMMIT;
 execute immediate 'CREATE TABLE T3_TYP_20230731 as select * from rqg_table2 ' ;
 execute immediate ' drop table T3_TYP_20230731 ';
 COMMIT;
 IF var1=v1 OR var2=var1 THEN var3:=var3;
 UPDATE rqg_table2 SET f1=var1 WHERE f1=v4;
 UPDATE rqg_table2 SET f2=var2 WHERE f2=v4;
 COMMIT;
 END IF ;
 RETURN ;
 END;
/

 CREATE OR REPLACE PROCEDURE PROC1_NT_20230803 (v1 number, v2 varchar2, v3 in number, v4 in number) is var1 number;
 var2 varchar2(200);
 var3 number;
 var4 number;
 begin var3:=v3;
 var4:=v4;
 PROC_NT_20230803 (v1,v2,var3,var4);
 end;
/
    
call PROC1_NT_20230803( 1,1,1,1 ) ;
create or replace function aak(a int) returns int is $$
select a from dual;
$$ language sql;

create or replace procedure ppk(a int) is  
   var2 varchar2(200) default '1';
 var3 number default 2;
 var4 number default 3;
 begin
   var4 = 1;
   commit;
    var3:=nvl(var2,0)+nvl(var2,0);
	var3 = aak(a);
end;
/
call ppk(1);
drop function aak;
drop procedure ppk;

DROP PROCEDURE PROC1_NT_20230803;
DROP PROCEDURE PROC_NT_20230803 ;
DROP TABLE if exists rqg_table2 cascade;
DROP TABLE T_TYP_20230731;
DROP TABLE log_20230731;

\c - PL_U1
select 1::opentenbase_ora.varchar2(20) from dual;

\c - PL_SU

do $$
declare
outer_arg int:= '1';
begin
  raise notice '%',outer_arg;
end;
$$language default_plsql;

do $$
declare
outer_arg int:= '1';
begin
   outer_arg := 1/0;
EXCEPTION
    WHEN division_by_zero THEN
      RAISE EXCEPTION 'DIVISION_BY_ZERO' USING ERRCODE = 'DIVISION_BY_ZERO';
end;
$$language default_plsql;

do $$
declare
outer_arg int:= '1';
begin
   outer_arg := 1/0;
EXCEPTION
    WHEN "division_by_zero" THEN
      RAISE EXCEPTION 'DIVISION_BY_ZERO' USING ERRCODE = 'division_by_zero';
end;
$$language default_plsql;

-- test DQOUTE IDENT in plsql
declare
    "var1" int;
begin
    select 1 into "var1" from dual;
    dbms_output.put_line("var1");
end;
/

-- 
-- Fixed the problem of pull-up connection condition in LEFT JOIN when the connection condition is stable function.
-- 
DROP SCHEMA if exists chqin CASCADE;
CREATE SCHEMA chqin;
SET search_path TO chqin,public;
CREATE TABLE rqg_table1 (
c0 int,
c1 int,
c2 varchar2(200),
c3 varchar2(200),
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 number,
c9 number)   ;
alter table rqg_table1 alter column c0 drop not null;
CREATE INDEX idx_rqg_table1_c1 ON rqg_table1(c1);
CREATE INDEX idx_rqg_table1_c3 ON rqg_table1(c3);
CREATE INDEX idx_rqg_table1_c5 ON rqg_table1(c5);
CREATE INDEX idx_rqg_table1_c7 ON rqg_table1(c7);
CREATE INDEX idx_rqg_table1_c9 ON rqg_table1(c9);
INSERT INTO rqg_table1 VALUES  (NULL, NULL, 'ibus', 'bar', '1999-06-04 18:49:59', '2009-06-18 08:57:05', '1976-11-23 06:03:49.044998', '2025-02-15 04:50:02.009096', -1.23456789123457e+25, -0.123456789123457) ,  (NULL, NULL, 'foo', 'bar', '2006-09-28 03:50:22', '2011-03-15', '2034-03-17', '1979-09-23 10:31:26', 3.73263966616001e+18, -5.729980468323e+125) ,  (NULL, NULL, 'busbyryaduthpikidveaouaihqsnidmpgzrckxzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvma', 'us', '2020-11-19 03:49:52', '1986-12-05 12:37:49', '1984-10-18 11:31:10.001103', '2034-09-11 23:48:39.053487', -1.23456789123457e+43, 1.23456789123457e-09) ,  (NULL, NULL, 'foo', 'sbyryad', '1982-08-14 04:48:24', '1997-02-25', '1980-07-26 14:19:52', '1972-10-24 09:40:11.004678', -1.23456789123457e+39, 1.23456789123457e-09) ,  (NULL, NULL, 'foo', 'byry', '2019-01-02', '1988-03-09 09:19:19', '2021-05-21 20:29:20.029736', '2020-09-03', -1.23456789123457e+30, -1.23456789123457e+44) ,  (NULL, NULL, 'yryadut', 'foo', '1992-08-22', '2023-09-11', '2019-09-08 18:46:08.014120', '1984-11-04 14:35:38.044635', 0.123456789123457, -0.123456789123457) ,  (NULL, NULL, 'ry', 'bar', '2007-08-01 00:30:23', '2005-04-06', '2018-04-04 22:59:10.025671', '1973-05-22', 1.23456789123457e-09, 1.23456789123457e+43) ,  (NULL, NULL, 'bar', 'bar', '2016-12-26 13:32:04', '1986-10-01 06:23:23', '2025-01-17 16:18:36.031298', NULL, -1.23456789123457e-09, 1.23456789123457e+44) ,  (NULL, NULL, 'yadut', 'foo', '1997-07-26', '2008-06-25 12:09:00', '2029-07-10 07:21:20', '1995-01-25 18:21:58.044905', 0.123456789123457, 1.23456789123457e+44) ,  (NULL, NULL, 'aduthpiki', 'bar', '2008-01-16', '2005-06-06', '2033-02-25 21:07:20', '1978-04-11 02:11:48', 1.23456789123457e-09, 1.23456789123457e-09) ;
CREATE TABLE rqg_table2 (
c0 int,
c1 int,
c2 varchar2(200),
c3 varchar2(200),
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 number,
c9 number)   distribute by replication;
alter table rqg_table2 alter column c0 drop not null;
CREATE INDEX idx_rqg_table2_c1 ON rqg_table2(c1);
CREATE INDEX idx_rqg_table2_c3 ON rqg_table2(c3);
CREATE INDEX idx_rqg_table2_c5 ON rqg_table2(c5);
CREATE INDEX idx_rqg_table2_c7 ON rqg_table2(c7);
CREATE INDEX idx_rqg_table2_c9 ON rqg_table2(c9);
INSERT INTO rqg_table2 VALUES  (NULL, NULL, 'bar', 'duthpikidveaouaihqsnidmpgzrckxzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvmakbmevzlqieninrchrmehhmohpesrjdsqtzpafzavakupdrpjzhvetuqcfrmsrlmhsxplpltpd', '1993-08-08 19:36:47', '1983-09-12 01:52:32', NULL, '2003-04-04 23:12:52.053488', -0.123456789123457, 0.123456789123457) ,  (NULL, NULL, 'u', 'foo', '2013-03-14 22:32:58', '2023-03-12 07:18:58', '1991-05-09 00:05:20.029181', '2003-10-06', -1.23456789123457e+25, -2.6762390129395e+125) ,  (NULL, NULL, 'thpikidveaouaihqsnidmpgzrckxzxuqqsrkgy', 'foo', '2022-01-19 13:42:31', '1986-12-14 23:18:10', '2028-05-08 10:59:55.029853', '1998-12-24', -1.23456789123457e+43, 5.66355800639511e+18) ,  (NULL, NULL, 'bar', 'bar', '2019-11-07', '1977-10-09', '1986-04-04 01:21:25.002660', '1975-01-03 09:19:08', 1.23456789123457e-09, 1.23456789123457e+30) ,  (NULL, NULL, 'hpikidveao', 'pikidveaouaihqsnidmpgzrckxzxuqqsrkgyxv', '2035-07-22', '2035-08-18 17:36:27', '1976-12-19 17:58:53.022642', '2001-07-04', 1.23456789123457e+43, -1.23456789123457e-09) ,  (NULL, NULL, 'ikidveaouaihqsnidmpgzrckxzxu', 'bar', '1993-05-11 20:03:30', '2001-04-11 21:46:37', '2029-11-10 01:01:33', '1985-01-21', -1.23456789123457e+43, -0.123456789123457) ,  (NULL, NULL, 'k', 'bar', '1992-03-15', '1974-03-18', '1993-04-19 21:41:04.008796', '2032-01-15 22:19:29', 0.659378051757812, 1.23456789123457e+30) ,  (NULL, NULL, 'idveaou', 'bar', '1982-09-07', '2010-05-17 05:18:07', '2008-10-04 02:31:44', '1986-01-13 14:25:05.035070', -1.23456789123457e+39, 0.224166870117188) ,  (NULL, NULL, 'foo', 'foo', '2026-03-08', '2034-02-23', '2030-02-14', '2023-08-06 05:48:08.040374', 8.9334528108428e+18, 5.09131937874235e+18) ,  (NULL, NULL, 'bar', 'dveaou', '2017-10-20', '1982-10-20 12:50:05', '2033-09-01 10:41:56.022974', '2005-02-21 18:13:23.007135', 1.23456789123457e-09, -1.23456789123457e+30) ;
CREATE TABLE rqg_table3 (
c0 int,
c1 int,
c2 varchar2(200),
c3 varchar2(200),
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 number,
c9 number)    PARTITION BY hash( c5) ;
create TABLE rqg_table3_p0 partition of rqg_table3 for values with(modulus 2,remainder 0);
create TABLE rqg_table3_p1 partition of rqg_table3 for values with(modulus 2,remainder 1);
alter table rqg_table3 alter column c0 drop not null;
CREATE INDEX idx_rqg_table3_c1 ON rqg_table3(c1);
CREATE INDEX idx_rqg_table3_c3 ON rqg_table3(c3);
CREATE INDEX idx_rqg_table3_c5 ON rqg_table3(c5);
CREATE INDEX idx_rqg_table3_c7 ON rqg_table3(c7);
CREATE INDEX idx_rqg_table3_c9 ON rqg_table3(c9);
INSERT INTO rqg_table3 VALUES  (NULL, NULL, 'veaoua', 'bar', '1985-11-14 00:13:45', '1979-12-16', '2000-03-25', '1979-01-07 10:31:37.026042', 0.8265380859375, 1.23456789123457e-09) ,  (NULL, NULL, 'eaouaihqsnidmpgzrckxzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvmakbmevzlqieninrchrmehhmohpesrjdsqtzpafzavakupdrpjzhvetuqcfrmsr', 'aouaihqsn', '2015-08-27', '2003-12-18', '1971-05-24 23:50:40.061985', '2001-05-17 20:11:21.038233', -1.23456789123457e+39, -0.123456789123457) ,  (NULL, NULL, 'foo', 'foo', '2015-11-09 17:15:30', '2029-07-16', '2031-04-13 04:56:45.009895', '2019-11-21 09:07:14.028914', 0.123456789123457, 0.123456789123457) ,  (NULL, NULL, 'bar', 'ouaihqsnidmpgzrckxzxu', '2033-05-10 03:54:07', '2021-10-23 15:13:34', '1989-10-11', '1999-06-09 19:41:01', 1.23456789123457e+39, -1.23456789123457e-09) ,  (NULL, NULL, 'uai', 'aihqsnidmpgzrckxzxuq', '2011-08-06 11:15:30', '2008-02-28 03:04:49', NULL, '1985-08-20 15:51:36.003184', -950009856, 0.123456789123457) ,  (NULL, NULL, 'foo', 'ih', '2034-06-09', '2031-09-20', NULL, '1971-01-08', -1.23456789123457e+25, -3.15399169853415e+125) ,  (NULL, NULL, 'bar', 'hqs', '2032-06-03', '1974-01-16 07:43:56', '2017-09-19 00:54:16.007553', '1991-11-05', -1.23456789123457e+39, 1.23456789123457e+43) ,  (NULL, NULL, 'qsnidmpgzrckxzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupe', 'snidmpgzrckxzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvmakbmevzlqieninrchrmehhmohpesrjdsqtzp', '1981-03-08 23:57:38', '1986-08-25 20:16:25', '1998-06-09 13:42:42', '1993-02-21', -1.23456789123457e+30, -1.23456789123457e+39) ,  (NULL, NULL, 'nid', 'bar', '1990-05-03', '1999-12-03', '1986-01-01 12:20:35.028728', NULL, 1.23456789123457e+44, -1.23456789123457e-09) ,  (NULL, NULL, 'idmpgzrckxzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvmakbmevzlqieninrchrmehhmohpes', 'dmpgzrckxzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupej', '1989-04-10 03:32:15', '2022-04-03', '1992-10-23 18:45:58.022547', '2034-05-06 00:46:47', 1.23456789123457e+39, 0.123456789123457) ;
CREATE TABLE rqg_table4 (
c0 int,
c1 int,
c2 varchar2(200),
c3 varchar2(200),
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 number,
c9 number)    PARTITION BY hash( c9) distribute by replication;
create TABLE rqg_table4_p0 partition of rqg_table4 for values with(modulus 2,remainder 0);
create TABLE rqg_table4_p1 partition of rqg_table4 for values with(modulus 2,remainder 1);
alter table rqg_table4 alter column c0 drop not null;
CREATE INDEX idx_rqg_table4_c1 ON rqg_table4(c1);
CREATE INDEX idx_rqg_table4_c3 ON rqg_table4(c3);
CREATE INDEX idx_rqg_table4_c5 ON rqg_table4(c5);
CREATE INDEX idx_rqg_table4_c7 ON rqg_table4(c7);
CREATE INDEX idx_rqg_table4_c9 ON rqg_table4(c9);
INSERT INTO rqg_table4 VALUES  (NULL, NULL, 'bar', 'mpg', '1996-03-21', '1981-01-13 12:33:41', '2023-10-05 23:55:41.003029', '1975-10-11 02:03:48', -1.23456789123457e-09, -1.23456789123457e-09) ,  (NULL, NULL, 'bar', 'pgzrckxzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnu', '2022-05-05 21:42:26', '1981-07-18 22:28:58', '1999-11-12 21:37:04.039761', '2010-07-18 22:33:13', -1.23456789123457e+39, -8.54461669907321e+125) ,  (NULL, NULL, 'gzrck', 'foo', '2027-01-01 11:33:00', '1991-10-22', '1994-09-23 23:39:48.017266', '2028-11-17 19:26:23', 1.23456789123457e-09, -1.23456789123457e-09) ,  (NULL, NULL, 'foo', 'foo', '2001-08-20 05:49:04', '1992-03-17', '2027-08-09 01:30:57', '2017-05-28 18:15:46', -1.23456789123457e-09, 1.23456789123457e-09) ,  (NULL, NULL, 'zrckxzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdv', 'bar', '2022-10-23 11:41:11', '2017-06-15 18:09:30', NULL, '2028-11-12', -1.23456789123457e-09, -1.23456789123457e-09) ,  (NULL, NULL, 'foo', 'rckxzxuq', '1989-02-27', '2009-03-20', '2016-03-25 09:12:10.019103', '2017-10-16 11:00:17.006604', -1.23456789123457e-09, -1.23456789123457e+44) ,  (NULL, NULL, 'ckxzxuqqs', 'kxzxuqqsrkgyxvvipagaffqcwakhacv', '2026-08-17', '2009-09-05', '2019-03-19 00:09:57.028844', '1979-04-16 09:52:45.033468', 1.23456789123457e+30, -1.23456789123457e+43) ,  (NULL, NULL, 'xzxuqqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvmakbmevzlqieninrchrmehhmohpesrjdsqtzpafzavakupdrpjzhvetuqcfrmsrlmhsxplpltpdibopmhruqxjsafrz', 'foo', '1983-01-21 06:02:17', '1983-05-02 12:15:50', '2008-03-10 14:56:22', '2003-08-25 01:18:01', -1.23456789123457e+44, -730333184) ,  (NULL, NULL, 'foo', 'zx', '1985-08-20 08:57:16', '2017-12-22 04:37:34', '2033-08-23', NULL, -1.23456789123457e+44, -1.23456789123457e+30) ,  (NULL, NULL, 'xuq', 'foo', '2004-01-25 20:28:38', '1978-04-10', '2027-02-23 10:27:28.005316', '1993-11-12', 1.23456789123457e+30, 0.123456789123457) ;
CREATE TABLE rqg_table5 (
c0 int,
c1 int,
c2 varchar2(200),
c3 varchar2(200),
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 number,
c9 number)    PARTITION BY hash( c8) ;
create TABLE rqg_table5_p0 partition of rqg_table5 for values with(modulus 4,remainder 0);
create TABLE rqg_table5_p1 partition of rqg_table5 for values with(modulus 4,remainder 1);
create TABLE rqg_table5_p2 partition of rqg_table5 for values with(modulus 4,remainder 2);
create TABLE rqg_table5_p3 partition of rqg_table5 for values with(modulus 4,remainder 3);
alter table rqg_table5 alter column c0 drop not null;
CREATE INDEX idx_rqg_table5_c1 ON rqg_table5(c1);
CREATE INDEX idx_rqg_table5_c3 ON rqg_table5(c3);
CREATE INDEX idx_rqg_table5_c5 ON rqg_table5(c5);
CREATE INDEX idx_rqg_table5_c7 ON rqg_table5(c7);
CREATE INDEX idx_rqg_table5_c9 ON rqg_table5(c9);
INSERT INTO rqg_table5 VALUES  (NULL, NULL, 'uqqs', 'foo', '2033-05-02', '2031-08-08', '2022-04-17 07:19:14', '2026-11-20 09:00:39.013312', -1.23456789123457e+30, 1.23456789123457e+25) ,  (NULL, NULL, 'qqsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvmakbmevzlqieninrchrmehhmohpesrjdsqtzpafzavakupdrpjzhvetuqcfrmsrlmhsxplpltpdibopmhruqxjsafrzavkqihjvxcogltydojoqixykgvzwa', 'qsrkgyxvvipagaffqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvmakbmevzlqieninrchrmehhmohpesrjdsqtzpafzavakupdrpjzhvetuqcfrmsrlmhsxplpltpdibopmhruqxjsafrzavkqihjvxcogltydojoqixykg', '2013-09-10 15:24:51', '1981-08-22', NULL, '1995-07-07 14:18:13', 1.23456789123457e-09, -1.23456789123457e+44) ,  (NULL, NULL, 'bar', 'srkg', '1974-05-18', '1977-07-08 06:37:09', '1992-09-20', '1996-04-23 17:52:43', 1.23456789123457e-09, -1.23456789123457e-09) ,  (NULL, NULL, 'bar', 'bar', '1999-08-26', '1978-08-26', '2025-10-13 14:21:23', '2017-04-27 10:15:44.037435', -5.66046178165129e+17, 1.23456789123457e+39) ,  (NULL, NULL, 'rkgyxvvipa', 'kgyxvvi', '2033-05-03 06:11:58', '2009-02-16 22:00:54', '2027-12-25 06:21:32', '2021-04-12 13:30:21', 1.23456789123457e+30, -8.79524859727787e+18) ,  (NULL, NULL, 'gyxvvipag', 'yxvvipagaf', '1985-02-10 19:39:55', '1986-07-23 19:40:47', '1998-09-18 23:06:34.007695', '1996-11-14 08:02:31.001771', 1.23456789123457e+44, -1.23456789123457e-09) ,  (NULL, NULL, 'foo', 'xvv', '1982-07-07 13:36:38', '1981-11-08 14:54:18', '2019-02-25 07:10:24.005052', '2020-09-22', 1.23456789123457e+43, -1.23456789123457e-09) ,  (NULL, NULL, 'vvipagaf', 'vipagaffqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjb', '2025-07-01 09:28:14', '1989-04-27 09:12:26', '1979-03-11 13:04:44', NULL, -0.123456789123457, 0.123456789123457) ,  (NULL, NULL, 'foo', 'foo', '2029-02-07', '2023-07-21 12:24:04', '1994-12-20', '1982-06-28', 1.23456789123457e+30, 1.23456789123457e+44) ,  (NULL, NULL, 'ipagaffqc', 'bar', '2019-09-23 00:17:51', '1985-08-06 18:56:53', '2009-10-02 02:43:23.024834', '2003-08-08 01:27:00', 1.23456789123457e+43, 1.23456789123457e-09) ;
CREATE TABLE rqg_table6 (
c0 int,
c1 int,
c2 varchar2(200),
c3 varchar2(200),
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 number,
c9 number)    PARTITION BY hash( c3) distribute by replication;
create TABLE rqg_table6_p0 partition of rqg_table6 for values with(modulus 4,remainder 0);
create TABLE rqg_table6_p1 partition of rqg_table6 for values with(modulus 4,remainder 1);
create TABLE rqg_table6_p2 partition of rqg_table6 for values with(modulus 4,remainder 2);
create TABLE rqg_table6_p3 partition of rqg_table6 for values with(modulus 4,remainder 3);
alter table rqg_table6 alter column c0 drop not null;
CREATE INDEX idx_rqg_table6_c1 ON rqg_table6(c1);
CREATE INDEX idx_rqg_table6_c3 ON rqg_table6(c3);
CREATE INDEX idx_rqg_table6_c5 ON rqg_table6(c5);
CREATE INDEX idx_rqg_table6_c7 ON rqg_table6(c7);
CREATE INDEX idx_rqg_table6_c9 ON rqg_table6(c9);
INSERT INTO rqg_table6 VALUES  (NULL, NULL, 'foo', 'bar', '2013-10-03', '2029-07-04', '1972-10-27 22:26:15.062943', '1978-05-02 08:34:09.058271', 1.23456789123457e-09, 1234698240) ,  (NULL, NULL, 'pagaffqcwakhacvzrdvjcwtpgdvk', 'bar', '1991-10-19', '1981-06-24 18:40:16', '2014-01-15 20:41:21.006931', '1997-05-15 07:32:51.018514', -1.23456789123457e+25, -1.23456789123457e-09) ,  (NULL, NULL, 'aga', 'gaffqcwakhacvzrdvjcwtpgdvkzhvnupej', '1987-01-09 09:03:49', '1977-01-22 14:22:04', '2014-06-04 16:34:44.041452', '1983-06-16 05:16:12.026991', -2028077056, -9.46380615234375e+80) ,  (NULL, NULL, 'affqcwakh', 'bar', '2018-09-01', '2030-05-23 16:08:04', '2008-07-27 08:27:30.054634', '2011-12-16 03:27:47', -1.23456789123457e+30, -1.23456789123457e+44) ,  (NULL, NULL, 'bar', 'ffqc', '2002-06-18', '2029-09-25 06:39:27', '2021-07-28', '2026-11-05', -1.23456789123457e+25, -0.123456789123457) ,  (NULL, NULL, 'fqcwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvmakbmevzlqieninrchrmehhmohpesrjdsqtzpafzavakupdrpjzhvetuqcf', 'foo', '2033-10-26 18:20:29', '2000-03-26', '2010-05-05 05:34:07', '2007-03-02 21:24:26.014840', 1.23456789123457e+43, 0.123456789123457) ,  (NULL, NULL, 'bar', 'foo', '2009-11-18 04:09:06', '1988-02-02 14:39:32', '1983-09-03 21:06:36.051242', '2011-01-04 22:34:49.061111', 1.23456789123457e+30, -0.123456789123457) ,  (NULL, NULL, 'foo', 'qcwakhacvzrdvjcwtpgdvkzhvn', '2001-01-23', '2019-03-05 01:46:42', NULL, '2017-01-11 02:47:35.042258', 1.23456789123457e-09, 0.123456789123457) ,  (NULL, NULL, 'cwakhacvzrdvjcwtpgdvkzhvnupejzsrvjbvkvmakbmevzlqieninrchrmehhmohpesrjdsqtzpafzavakupdrpjzhvetuqcfrmsrlmhsxplpltpdibopmhruqxjsafrzavkqihjvxcogltydojoq', 'bar', '2035-06-02 02:47:57', '1981-01-10 13:26:39', NULL, '2004-08-10', -1.23456789123457e-09, 0.685409545898438) ,  (NULL, NULL, 'bar', 'wakhacvz', '1972-09-01', '2035-06-03', '2025-11-16 20:29:16.049282', '2003-11-27 22:35:36', -1020657664, 1.23456789123457e+25) ;
SET search_path TO chqin,public;

CREATE or replace view VIEW_INCLINE_TARGET_20230816 AS SELECT * FROM ( SELECT a1.c0, a1.c1, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table4 AS a1 WHERE NOT ( NOT EXISTS ( SELECT MIN( c7 ) FROM ( SELECT a1.c0, a1.c1, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table2 AS a1 WHERE a1.c2 not LIKE 'N9%' ) ) ) AND a1.c7 NOT BETWEEN '2004-07-02 04:25:43.028222' AND TO_TIMESTAMP(TO_TIMESTAMP( '2004-07-02 04:25:43.028222','YYYY-MM-DD HH24:MI:SS.FF') + 1 ) )  ;

CREATE OR REPLACE FUNCTION F_INCLINE_TARGET_20230816 (v1 rqg_table1.c1%type) RETURNS rqg_table1.c1%type AS $$ with cte as ( SELECT a1.c0, a1.c1, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table6 AS a1 WHERE NOT ( a1.c0 = a1.c0 ) AND a1.c1 = a1.c0 ) SELECT c1 FROM CTE WHERE c1 != v1 ORDER BY c1 limit 1; $$ LANGUAGE SQL STABLE ;  

-- 结果不一致
with cte as ( SELECT F_INCLINE_TARGET_20230816(c1) c0 FROM ( SELECT a1.c0, a1.c1, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table2 AS a1 WHERE a1.c5 >= '2016-05-17' ) ), cte2 as (SELECT F_INCLINE_TARGET_20230816(c1) c1 FROM ( SELECT a1.c0, a1.c1, a2.c2, a2.c3, a1.c4, a2.c5, a1.c6, a2.c7, a1.c8, a2.c9 FROM ( rqg_table4 AS a1 INNER JOIN rqg_table4 AS a2 ON ( NOT ( a1.c1 = 3 ) AND ( a1.c0 = a2.c0 ) ) ) INNER JOIN rqg_table2 AS a3 ON ( ( a1.c1 = 3 AND ( a1.c0 = a3.c0 ) ) AND ( a3.c0 = a1.c0 ) AND ( a1.c0 = a3.c0 ) ) WHERE ( a2.c1 = 1 AND a1.c1 = 9 ) OR ( a3.c9 != -1234567891234567891234567890000000000000 ) AND a2.c1 = a2.c1 GROUP BY a1.c0, a1.c1, a2.c2, a2.c3, a1.c4, a2.c5, a1.c6, a2.c7, a1.c8, a2.c9 HAVING a1.c1 = a1.c0 )) SELECT F_INCLINE_TARGET_20230816 ( F_INCLINE_TARGET_20230816(c1)) FROM cte t1 left join cte2 t2 on (t1.c0>t2.c1) order by 1  ;

DROP SCHEMA if exists chqin CASCADE;
