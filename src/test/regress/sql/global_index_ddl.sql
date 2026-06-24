-- =====================================================================
-- Title     : global index ddl
-- Author    : azurezhao
-- Date      : 2022-08-22
-- Level     : P0
-- Tag       :
-- Status    : Ready
-- =====================================================================


/* normal */
drop table if exists gindex_test;
create table gindex_test(a int,b int,c varchar,d numeric,e float) distribute by shard(a) ;

/* index type */
create global index on gindex_test using btree (b);
create global index on gindex_test using hash (b);

/* index with column qualifier */
create global index on gindex_test(b nulls first);
create global index on gindex_test(b desc);
--create global index on gindex_test(b COLLATE "de_DE");

/* index with options */
create global index on gindex_test(b) with (fillfactor=70);

/* tablespace */
create global index on gindex_test(b) TABLESPACE pg_default;

/* distribute key */
create global index on gindex_test(a);
create global index on gindex_test(c);
create global index on gindex_test(d);
create global index on gindex_test(e);
create global index on gindex_test(e);
/* multiple key */
create global index on gindex_test(e,b);

/* concurrently */
create global index concurrently on gindex_test(e);

/* create table like */
create table gindex_test_like3 (like gindex_test);

\d+ gindex_test;
insert into gindex_test values (1,2,3,4,5);

/* unique */
insert into gindex_test values (1,2,3,4,5);
create global unique index on gindex_test(b);

truncate gindex_test;
create global unique index on gindex_test(b);
insert into gindex_test values (1,2,3,4,5);
insert into gindex_test values (1,2,3,4,5);


/* alter table */
alter table gindex_test_c_idx drop COLUMN location;
alter table gindex_test_c_idx add test varchar(10);

alter table gindex_test add test varchar(10);
alter table gindex_test alter column c TYPE text;
alter table gindex_test alter column c set NOT NULL;
alter table gindex_test alter column c set default 1000;
alter table gindex_test drop column c;
\d+ gindex_test_c_idx

/* replicate table */
create table t_rep (id int,mc text) distribute by replication to group default_group;
create global index on t_rep(mc);

/* trigger */
drop table if exists COMPANY,COMPANY1;
CREATE TABLE COMPANY(
		ID INT PRIMARY KEY     NOT NULL,
		NAME           TEXT    NOT NULL,
		AGE            INT     NOT NULL,
		ADDRESS        CHAR(50),
		SALARY         REAL
		) distribute by shard(ID);
CREATE TABLE COMPANY1(
		EMP_ID INT NOT NULL,
		ENTRY_DATE TEXT NOT NULL
		)distribute by shard(EMP_ID);;
CREATE OR REPLACE FUNCTION auditlogfunc() RETURNS TRIGGER AS $example_table$
BEGIN
INSERT INTO COMPANY1(EMP_ID, ENTRY_DATE) VALUES (new.ID, current_timestamp);
RETURN NEW;
END;
$example_table$ LANGUAGE plpgsql;
CREATE TRIGGER example_trigger AFTER INSERT ON COMPANY FOR EACH ROW EXECUTE PROCEDURE auditlogfunc();
create global index on COMPANY(NAME);

drop trigger example_trigger on COMPANY;
create global index on COMPANY(NAME);
CREATE TRIGGER example_trigger AFTER INSERT ON COMPANY FOR EACH ROW EXECUTE PROCEDURE auditlogfunc();

drop table COMPANY,COMPANY1;

/* truncate */
truncate gindex_test;
truncate gindex_test_d_idx;

/* vacuum */
vacuum gindex_test;
vacuum gindex_test_d_idx;

/* vacuum full */
vacuum full gindex_test;
vacuum full gindex_test_d_idx;

/* reindex */
reindex table gindex_test_d_idx;
reindex index gindex_test_b_idx3_b_idx;
reindex table gindex_test;

create table gindex_hash(a int,b int,c varchar(10),d varchar) distribute by hash(a);
create global unique index on gindex_hash(b); --fail

/* drop table */
drop table gindex_test_b_idx;
drop index gindex_test_b_idx;
drop table gindex_test;
drop table gindex_hash;
drop function auditlogfunc;

drop table gindex_test_like3;
drop table t_rep;
