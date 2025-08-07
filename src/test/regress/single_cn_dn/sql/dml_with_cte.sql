\c regression
create or replace function explain_filter(text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in execute $1
    loop
        -- In sort output, the above won't match units-suffixed numbers
        ln := regexp_replace(ln, '\m\d+kB', 'NkB', 'g');
        -- For query_mem
        ln := regexp_replace(ln, '\m\d+MB', 'NMB', 'g');
        -- Text-mode buffers output varies depending on the system state
        ln := regexp_replace(ln, '^( +Buffers: shared)( hit=N)?( read=N)?', '\1 [read]');
        return next ln;
    end loop;
end;
$$;
create table aa(i int, j int);
insert into aa select i,i from generate_series(1,1000) t(i);
create table bb (i int, j int);
explain (costs off)with t as materialized (select * from aa) insert into bb select * from t;
explain (costs off)with t as materialized (select * from aa) delete from bb using t where bb.j=t.j-1;
explain (costs off)with t as materialized (select * from aa) update bb set j=t.j+1 from t where bb.i=t.i;

select explain_filter('explain (analyze, summary off, costs off, timing off)with t as materialized (select * from aa) insert into bb select * from t;');
select explain_filter('explain (analyze, summary off, costs off, timing off)with t as materialized (select * from aa) delete from bb using t where bb.j=t.j-1;');
select explain_filter('explain (analyze, summary off, costs off, timing off)with t as materialized (select * from aa) update bb set j=t.j+1 from t where bb.i=t.i;');

\c regression_ora
--tapd:117146666
CREATE TABLE TM_JY_MF_FUNDARCHIVESATTACH(COMPANYCODE NUMBER(10,0) DEFAULT NULL ,DATACODE NUMBER(10,0) DEFAULT NULL ,DATANAME VARCHAR2(50) DEFAULT NULL ,DATAVALUE NUMBER(10,0) DEFAULT NULL ,ENDDATE DATE DEFAULT NULL ,ID NUMBER(19,0) DEFAULT 0  NOT NULL ,INFOSOURCE VARCHAR2(50) DEFAULT NULL ,INNERCODE NUMBER(10,0) DEFAULT NULL ,JSID NUMBER(19,0) DEFAULT NULL ,REMARK VARCHAR2(500) DEFAULT NULL ,SECUCODE VARCHAR2(30) DEFAULT NULL ,SECUMARKET NUMBER(10,0) DEFAULT NULL ,STARTDATE DATE DEFAULT NULL ,TYPECODE NUMBER(10,0) DEFAULT NULL ,TYPENAME VARCHAR2(50) DEFAULT NULL ,UPDATETIME DATE DEFAULT NULL ) distribute by shard(ID);

CREATE TABLE TN_FUND_PARAM(C_CHECK_STATE CHAR(1) DEFAULT '1' ,C_DT_CT_USER VARCHAR2(20) DEFAULT ' '  NOT NULL ,C_DT_MD_USER VARCHAR2(20) DEFAULT ' '  NOT NULL ,C_FUND_MANAGER VARCHAR2(20) DEFAULT NULL ,C_FUND_NATURE CHAR(1) DEFAULT '0' ,C_FUND_STYLE VARCHAR2(20) DEFAULT NULL ,C_FUND_TYPE CHAR(1) DEFAULT NULL ,C_SOURCE_CODE VARCHAR2(50) DEFAULT NULL ,C_SOURCE_INFO VARCHAR2(200) DEFAULT NULL ,C_SOURCE_TYPE CHAR(1) DEFAULT ' '  NOT NULL ,C_STOCK_CODE VARCHAR2(30) DEFAULT ' '  NOT NULL ,C_STYLE_PROPERTY VARCHAR2(20) DEFAULT NULL ,D_DT_CT_DATE NUMBER(8,0) DEFAULT 0  NOT NULL ,D_DT_CT_TIME NUMBER(8,0) DEFAULT 0  NOT NULL ,D_DT_MD_DATE NUMBER(8,0) DEFAULT 0  NOT NULL ,D_DT_MD_TIME NUMBER(8,0) DEFAULT 0  NOT NULL ,D_INCEPTION_DATE NUMBER(8,0) DEFAULT NULL ,N_FUND_TOTAL_VALUE NUMBER(20,6) DEFAULT 0 ,N_FUND_VALUE NUMBER(20,6) DEFAULT 0 ,N_GP_RATIO NUMBER(30,6) DEFAULT 0  NOT NULL ,N_OPERATION_PERIOD NUMBER(30,6) DEFAULT 0 ,N_PRE_FUND_VALUE NUMBER(30,6) DEFAULT 0  NOT NULL ,N_RUN_SCALE NUMBER(30,6) DEFAULT 0 ,C_ISSUE_TYPE VARCHAR2(10) DEFAULT '-2' ,C_FUND_INVEST_TYPE VARCHAR2(10) DEFAULT '-2' ,N_B_RE_DAY_TERM NUMBER(30,6) DEFAULT '0' ,D_NAERLY_OPEN_DATE NUMBER(8,0) DEFAULT 0 ,C_INTERVAL_YIELD VARCHAR2(1000) DEFAULT NULL ,N_MAXIMUM_DRAWDOWN NUMBER(30,6) DEFAULT NULL ,C_RAISE_TYPE CHAR(1) DEFAULT NULL ,C_MF_LEVERAGE CHAR(1) DEFAULT 0 ,C_PF_LEVERAGE CHAR(1) DEFAULT 0 ,C_PUBLISH_CATEGORY CHAR(1) DEFAULT 0 ,C_FUND_CODE VARCHAR2(20) DEFAULT -1 ,C_REPORT_CODE VARCHAR2(50) DEFAULT ' '  NOT NULL ,N_RE_OP_PERIOD_YEAR NUMBER(22,6) DEFAULT 0 ,N_OP_PERIOD_YEAR NUMBER(22,6) DEFAULT 0 ,C_FIRST_CLASS_JY NUMBER(10,0) DEFAULT NULL ,C_SECOND_CLASS_JY NUMBER(10,0) DEFAULT NULL ,C_THIRD_CLASS_JY NUMBER(10,0) DEFAULT NULL ,C_FIRST_CLASS_WD VARCHAR2(20) DEFAULT '-2' ,C_SECOND_CLASS_WD VARCHAR2(20) DEFAULT '-2' ,C_THIRD_CLASS_WD VARCHAR2(20) DEFAULT '-2' ) distribute by shard(C_STOCK_CODE);

CREATE TABLE TB_TRADEDAY(C_DT_CT_USER VARCHAR2(40) DEFAULT ' '  NOT NULL ,C_DT_MD_USER VARCHAR2(40) DEFAULT ' '  NOT NULL ,C_SOURCE_CODE VARCHAR2(100) DEFAULT NULL ,C_SOURCE_INFO VARCHAR2(400) DEFAULT NULL ,C_SOURCE_TYPE VARCHAR2(10) DEFAULT ' '  NOT NULL ,C_TRADEDAY_CODE VARCHAR2(40) DEFAULT ' '  NOT NULL ,D_DT_CT_DATE NUMBER(8,0) DEFAULT 0  NOT NULL ,D_DT_CT_TIME NUMBER(8,0) DEFAULT 0  NOT NULL ,D_DT_MD_DATE NUMBER(8,0) DEFAULT 0  NOT NULL ,D_DT_MD_TIME NUMBER(8,0) DEFAULT 0  NOT NULL ,D_TRADE_DATE NUMBER(8,0) DEFAULT 0  NOT NULL ) distribute by shard(C_TRADEDAY_CODE);

CREATE OR REPLACE FUNCTION  FN_GET_TRADE_DATE(PI_DATE  IN NUMBER,
                                            PI_OFFSET_DAY  IN INT DEFAULT 0)



 RETURN NUMBER AS
  PI_DAY       NUMBER;
  PI_TRADE_DATE NUMBER;
  PI_ABS     INT;
begin

   PI_DAY := 0;
  if PI_DATE = 0 then
    select to_number(to_char(sysdate,'YYYYMMDD'))
    into PI_TRADE_DATE
    from dual;
  else
    PI_TRADE_DATE := PI_DATE;
  end if;

 PI_ABS  := abs(PI_OFFSET_DAY);

  IF PI_OFFSET_DAY > 0 THEN
    FOR X IN ( SELECT A.D_TRADE_DATE
                FROM TB_TRADEDAY A
               WHERE (A.C_TRADEDAY_CODE = '0' OR A.C_TRADEDAY_CODE = '001002')
                 AND A.D_TRADE_DATE > PI_TRADE_DATE
               ORDER BY A.D_TRADE_DATE) LOOP
      PI_DAY := PI_DAY + 1;
      IF PI_DAY = PI_ABS THEN
        RETURN X.D_TRADE_DATE;
      END IF;

    END LOOP;
  ELSE
    FOR X IN ( SELECT A.D_TRADE_DATE
                FROM TB_TRADEDAY A
               WHERE (A.C_TRADEDAY_CODE = '0' OR A.C_TRADEDAY_CODE = '001002')
                 AND A.D_TRADE_DATE < PI_TRADE_DATE
               ORDER BY A.D_TRADE_DATE DESC) LOOP
      PI_DAY := PI_DAY + 1;
      IF PI_DAY = PI_ABS THEN
        RETURN X.D_TRADE_DATE;
      END IF;
    END LOOP;
  END IF;

 RETURN PI_TRADE_DATE;

EXCEPTION
  WHEN OTHERS THEN
    RAISE_APPLICATION_ERROR(-20001,
                            SQLERRM || ';' ||
                            DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
END FN_GET_TRADE_DATE;
/
CREATE OR REPLACE FUNCTION FN_GET_JY_STOCKCODE(IN_SECUMARKET IN NUMBER,
                                             IN_SECUCODE IN VARCHAR2)
  RETURN VARCHAR2 AS
  C_MARKET     VARCHAR2(10) := '';
  C_STOCK_CODE VARCHAR2(50) := '';
BEGIN
    C_STOCK_CODE := IN_SECUCODE;
    C_MARKET     := CASE IN_SECUMARKET
                      WHEN 89 THEN
                       'YH'
                      WHEN 83 THEN
                       'SS'
                      WHEN 90 THEN
                       'SZ'
                      WHEN 71 THEN
                       'CW'
                      WHEN 72 THEN
                       'HK'
                      WHEN 81 THEN
                       'GZ'
                      WHEN 18 THEN
                       'BJ'
                    ELSE 'CW'
END;
RETURN C_STOCK_CODE || ' ' || C_MARKET;
END FN_GET_JY_STOCKCODE;
/
alter function FN_GET_JY_STOCKCODE pushdown;
alter function FN_GET_TRADE_DATE pushdown;

explain (analyze, summary off, costs off, timing off) MERGE INTO TN_FUND_PARAM A

USING (

  WITH T1 AS

   (SELECT STARTDATE,

           ENDDATE,

           NVL(LAG(STARTDATE) OVER(PARTITION BY SECUMARKET,

                    SECUCODE ORDER BY STARTDATE DESC),

               TO_DATE('99991231', 'YYYYMMDD')) AS NSTARTDATE,

           FN_GET_JY_STOCKCODE(SECUMARKET, SECUCODE) AS C_STOCK_CODE

      FROM TM_JY_MF_FUNDARCHIVESATTACH T

     WHERE TYPECODE = '51'

       AND STARTDATE IS NOT NULL

       AND ENDDATE IS NOT NULL)

  SELECT C.C_STOCK_CODE,

         MIN(CASE

               WHEN TO_DATE(TO_CHAR(20210128), 'YYYYMMDD') BETWEEN STARTDATE AND

                    ENDDATE THEN

                FN_GET_TRADE_DATE(TO_NUMBER(TO_CHAR(ENDDATE, 'YYYYMMDD')), 

                                  1)

               WHEN ENDDATE < TO_DATE(TO_CHAR(20210128), 'YYYYMMDD') AND

                    TO_DATE(TO_CHAR(20210128), 'YYYYMMDD') < NSTARTDATE THEN

                FN_GET_TRADE_DATE(TO_NUMBER(TO_CHAR(ENDDATE, 'YYYYMMDD')), 

                                  1)

               WHEN TO_DATE(TO_CHAR(20210128), 'YYYYMMDD') < MINSTARTDATE THEN

                FN_GET_TRADE_DATE(TO_NUMBER(TO_CHAR(MINSTARTDATE, 'YYYYMMDD')), 

                                  -1)

             END) AS D_NAERLY_OPEN_DATE

    FROM T1 C

    JOIN (SELECT MIN(STARTDATE) AS MINSTARTDATE, C_STOCK_CODE

            FROM T1

           GROUP BY C_STOCK_CODE) D

      ON C.C_STOCK_CODE = D.C_STOCK_CODE

   GROUP BY C.C_STOCK_CODE) B

      ON (A.C_STOCK_CODE = B.C_STOCK_CODE) WHEN MATCHED THEN 

    UPDATE SET A.D_NAERLY_OPEN_DATE = B.D_NAERLY_OPEN_DATE;