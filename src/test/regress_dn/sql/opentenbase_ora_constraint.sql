\c regression_ora

-- 1. test primary key constraint name same as current table name
CREATE TABLE "ACCT_PAY_FILE_LIST" (
    "PAY_SUM_ID" BIGINT NOT NULL,
    "PRODUCT_ID" BIGINT NOT NULL,
    "PLAN_COMP_ID" BIGINT NOT NULL,
    "SPAY_SUM" DECIMAL(14,2),
    "SPAY_NUM" BIGINT,
    "ACCT_PAY_FILE" BIGINT,
    "PRICE_DATE" TIMESTAMP(0),
    "FCU" BIGINT,
    "FCD" TIMESTAMP(0),
    "LCU" BIGINT,
    "LCD" TIMESTAMP(0),
    "PDF_FILE" BIGINT,
    "SEAL_PDF_FILE" BIGINT,
    CONSTRAINT "ACCT_PAY_FILE_LIST" PRIMARY KEY ("PRODUCT_ID", "PLAN_COMP_ID", "PRICE_DATE")
    USING INDEX
);

\d+ ACCT_PAY_FILE_LIST

drop table "ACCT_PAY_FILE_LIST";

-- 2. test primary key constraint name same as other table name

create table test_a(a int, b int);

create table test_b(b1 int, b2 int, CONSTRAINT "TEST_A" PRIMARY KEY(b1) USING INDEX);

drop table test_a;
drop table test_b;

create table TEST_A(a int, b int);

create table test_b(b1 int, b2 int, CONSTRAINT "TEST_A" PRIMARY KEY(b1) USING INDEX);

drop table TEST_A;
drop table test_b;

-- 3. test primary key constraint name same as other constraint name
-- 对于不存在约束名与表名相同场景, 无需改名场景. 需要检查是否有namespace全局是否同名约束，行为与opentenbase_ora保持一致
CREATE TABLE CHQIN(ID INT);
ALTER TABLE CHQIN ADD CONSTRAINT CC1 PRIMARY KEY(ID);

CREATE TABLE CHQIN1(ID INT);
ALTER TABLE CHQIN1 ADD CONSTRAINT CC1 PRIMARY KEY(ID);

DROP TABLE CHQIN;
DROP TABLE CHQIN1;

-- opentenbase_ora 支持约束名与表名相同，opentenbase为支持需改名约束名
CREATE TABLE CHQIN(ID INT);
ALTER TABLE CHQIN ADD CONSTRAINT CHQIN PRIMARY KEY(ID);

-- 约束名与上面的约束名相同，表名相同，上面case约束名已改, 这里与上面表名相同所以opentenbase为支持需改名约束名
CREATE TABLE CHQIN1(ID INT);
ALTER TABLE CHQIN1 ADD CONSTRAINT CHQIN PRIMARY KEY(ID);

DROP TABLE CHQIN;
DROP TABLE CHQIN1;
drop table if exists x_20250119;
create table x_20250119(a int, b int);
alter table x_20250119 add constraint xpk primary key (a);
insert into x_20250119 values(1, 1);
alter table x_20250119 disable constraint xpk;
insert into x_20250119 values(1, 2);
alter table x_20250119 disable novalidate constraint xpk;
select indrelid::regclass, INDEXRELID::regclass, indisready, indislive from pg_index where indrelid='X_20250119'::regclass;
alter table x_20250119 enable constraint xpk; -- error
select indrelid::regclass, INDEXRELID::regclass, indisready, indislive from pg_index where indrelid='X_20250119'::regclass;
set enable_seqscan = off;
explain (costs off, nodes off) select *from x_20250119 where a = 1;
select *from x_20250119 where a = 1;
reset enable_seqscan;
select *from x_20250119 where a - 1 = 0;
drop table x_20250119;
