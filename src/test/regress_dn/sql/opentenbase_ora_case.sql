---------------------------------------------
-- Test cases for opentenbase_ora case problems.
---------------------------------------------

\c regression_ora

-- md5 function displays text in upper format. 
select md5('');
select md5('a');
select md5('abc');
select md5('') = 'D41D8CD98F00B204E9800998ECF8427E' AS "TRUE";
select md5('a') = '0CC175B9C0F1B6A831C399E269772661' AS "TRUE";
select md5('abc') = '900150983CD24FB0D6963F7D28E17F72' AS "TRUE";

drop table if exists tcase_20230808_1;
drop table if exists tcase_20230808_2;
create table tcase_20230808_1
(
    c1 serial primary key,
    c2 int
);

create table tcase_20230808_2
(
    c1 serial primary key,
    c2 serial,
    c3 int unique,
    c4 int references tcase_20230808_1(c1),
    c5 int check (c5 > 0),
    c6 int,
    c7 int,
    EXCLUDE USING btree (c6 WITH =)
);

create index on tcase_20230808_2(c7);

\d+ tcase_20230808_1
\d+ tcase_20230808_2

alter table tcase_20230808_2 drop constraint TCASE_20230808_2_C4_FKEY;
alter table tcase_20230808_2 drop constraint TCAsE_20230808_2_C5_check;
alter table tcase_20230808_2 drop constraint TCASE_20230808_2_c6_excl;
alter table tcase_20230808_2 drop constraint TcASE_20230808_2_C3_key;
alter table tcase_20230808_2 drop constraint tCASE_20230808_2_pkey;
drop index tcase_20230808_2_c7_idx;
drop sequence TCaSE_20230808_2_C2_SeQ cascade;
drop table if exists tcase_20230808_1;
drop table if exists tcase_20230808_1;
-- bugfix
drop table if exists merge_conmd_t_shard_20220726;
create table merge_conmd_t_shard_20220726
(
    f1 number(10) not null,
    f2 integer,
    f3 varchar2(20),
    f4 varchar2(20),
    f5 timestamp without time zone,
    primary key (f1, f2)
) distribute by shard(f1) to group default_group;
\d+ merge_conmd_t_shard_20220726
alter table merge_conmd_t_shard_20220726 drop constraint merge_conmd_t_shard_20220726_pkey;
drop table if exists merge_conmd_t_shard_20220726;

-- Fix the failure of CREATE STATISTICS when comparing the statistics kind.
drop table if exists sync_analyze_st_shard_20221026;
create table sync_analyze_st_shard_20221026(f1 number(10),f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone, f7 blob, f8 tsvector, f9 bytea, f10 char, f11 nchar(10), f12 tsvector, f13 clob, f14 blob, f15 nclob, f16 box) distribute by shard(f1) to group default_group;

CREATE STATISTICS ext_stat_1 (ndistinct) on f2, f3 from sync_analyze_st_shard_20221026;
CREATE STATISTICS ext_stat_2 (dependencies) on f10, f11 from sync_analyze_st_shard_20221026;

drop table if exists sync_analyze_st_shard_20221026 cascade;

-- Fix the problems of null expr in VALUES clause.
SELECT * FROM ( VALUES ( 1, NULL :: text,  NULL :: text ), ( NULL :: integer, NULL :: text, NULL :: text)) vala;

SELECT vala.column1, vala.column2, vala.column3 FROM ( VALUES ( 1, NULL :: text,  NULL :: text ), ( NULL :: integer, NULL :: text, NULL :: text)) vala;

select * from (values (1,null::text,null::text),(null::text,null::text,null::text));

select * from (values (null::text,null::text,null::text),(null::text,null::text,null::text));

-- Fix the problem of default lowercase attr name in VALUES clause.
explain (costs off, verbose)
select * from ( values (1,23),(2,34) );

create table valt (a int, b int);

explain(costs off, verbose)
insert into valt values(1,2),(3,4);

drop table valt;

-- TAPD: https://tapd.woa.com/20385652/prong/stories/view/1020385652886039553
drop table test_uppercase_rel_20230913;
create table test_uppercase_rel_20230913(a text,b text);
insert into test_uppercase_rel_20230913 values('2312','13123');
select * from test_uppercase_rel_20230913 where substr(a, 1) = '2312' collate "C";
select * from test_uppercase_rel_20230913 where substr(a, 1) = '2312' collate "POSIX";
select * from test_uppercase_rel_20230913 where substr(a, 1) = '2312' collate "C" and substr(a, 1) = '2312' collate "POSIX";

-- test Common_IDENT in AlterOptRoleElem
create role test_uppercase_role_20230913;
alter role test_uppercase_role_20230913 with "CREATEROLE";
alter role test_uppercase_role_20230913 with nocreaterole;

-- test create and comment and drop for OBJECT_TSPARSER OBJECT_TSDICTIONARY OBJECT_TSTEMPLATE OBJECT_TSCONFIGURATION
CREATE TEXT SEARCH PARSER test_uppercase_parser_20230913 (
    START = PRSD_START,
    GETTOKEN = PRSD_NEXTTOKEN,
    END = PRSD_END,
    LEXTYPES = PRSD_LEXTYPE,
    HEADLINE = PRSD_HEADLINE
);
-- error
CREATE TEXT SEARCH PARSER "test_uppercase_parser_20230913" (
    START = PRSD_START,
    GETTOKEN = PRSD_NEXTTOKEN,
    END = PRSD_END,
    LEXTYPES = PRSD_LEXTYPE,
    HEADLINE = PRSD_HEADLINE
);
CREATE TEXT SEARCH PARSER "TEST_UPPERCASE_PARSER_20230913" (
    START = PRSD_START,
    GETTOKEN = PRSD_NEXTTOKEN,
    END = PRSD_END,
    LEXTYPES = PRSD_LEXTYPE,
    HEADLINE = PRSD_HEADLINE
);
COMMENT ON TEXT SEARCH PARSER test_uppercase_parser_20230913 is 'test parser';
COMMENT ON TEXT SEARCH PARSER "TEST_UPPERCASE_PARSER_20230913" is 'TEST PARSER';
DROP TEXT SEARCH PARSER test_uppercase_parser_20230913;
DROP TEXT SEARCH PARSER IF EXISTS "TEST_UPPERCASE_PARSER_20230913";

CREATE TEXT SEARCH DICTIONARY TEST_UPPERCASE_DICT_20230913 (
    template = snowball,
    language = russian,
    stopwords = russian
);
-- error
CREATE TEXT SEARCH DICTIONARY "test_uppercase_dict_20230913" (
    template = snowball,
    language = russian,
    stopwords = russian
);
CREATE TEXT SEARCH DICTIONARY "TEST_UPPERCASE_DICT_20230913" (
    template = snowball,
    language = russian,
    stopwords = russian
);
COMMENT ON TEXT SEARCH DICTIONARY test_uppercase_dict_20230913 is 'test dict';
COMMENT ON TEXT SEARCH DICTIONARY "TEST_UPPERCASE_DICT_20230913" is 'TEST DICT';
DROP TEXT SEARCH DICTIONARY test_uppercase_dict_20230913;
DROP TEXT SEARCH DICTIONARY IF EXISTS "TEST_UPPERCASE_DICT_20230913";

CREATE TEXT SEARCH TEMPLATE test_uppercase_template_20230913(
	LEXIZE = DSIMPLE_LEXIZE,
	INIT  = DSIMPLE_INIT
);
-- error
CREATE TEXT SEARCH TEMPLATE "test_uppercase_template_20230913"(
	LEXIZE = DSIMPLE_LEXIZE,
	INIT  = DSIMPLE_INIT
);
CREATE TEXT SEARCH TEMPLATE "TEST_UPPERCASE_TEMPLATE_20230913" (
	LEXIZE = DSIMPLE_LEXIZE,
	INIT  = DSIMPLE_INIT
);
COMMENT ON TEXT SEARCH TEMPLATE test_uppercase_template_20230913 is 'test template';
COMMENT ON TEXT SEARCH TEMPLATE "TEST_UPPERCASE_TEMPLATE_20230913" is 'TEST TEMPLATE';
DROP TEXT SEARCH TEMPLATE "test_uppercase_template_20230913";
DROP TEXT SEARCH TEMPLATE "TEST_UPPERCASE_TEMPLATE_20230913";

CREATE TEXT SEARCH CONFIGURATION test_uppercase_config_20230913(
	PARSER = default
);
--error
CREATE TEXT SEARCH CONFIGURATION "test_uppercase_config_20230913"(
	COPY = pg_catalog.english
);
CREATE TEXT SEARCH CONFIGURATION "TEST_UPPERCASE_CONFIG_20230913"(
	COPY = pg_catalog.english
);
COMMENT ON TEXT SEARCH CONFIGURATION test_uppercase_config_20230913 is 'test configuration';
COMMENT ON TEXT SEARCH CONFIGURATION "TEST_UPPERCASE_CONFIG_20230913" is 'TEST CONFIGURATION';
DROP TEXT SEARCH CONFIGURATION test_uppercase_config_20230913;
DROP TEXT SEARCH CONFIGURATION IF EXISTS "TEST_UPPERCASE_CONFIG_20230913";
-- test create and comment and drop access method
CREATE ACCESS METHOD test_uppercase_am_20230913 TYPE INDEX HANDLER gisthandler;
CREATE ACCESS METHOD "TEST_UPPERCASE_AM_20230913" TYPE INDEX HANDLER gisthandler;
COMMENT ON ACCESS METHOD test_uppercase_am_20230913 is 'test access mothed';
COMMENT ON ACCESS METHOD "TEST_UPPERCASE_AM_20230913" is 'TEST ACCESS MOTHED';
DROP ACCESS METHOD test_uppercase_am_20230913;
DROP ACCESS METHOD "TEST_UPPERCASE_AM_20230913";

-- begin ID884863681
create table k0031(k1 int, k2 varchar, k3 varchar(1), k4 char(1));
insert into k0031 values(1, generate_series(1, 8), generate_series(1, 8), generate_series(1, 8));
insert into k0031 select * from k0031;
insert into k0031 select * from k0031;
insert into k0031 select * from k0031;
insert into k0031 select * from k0031;
insert into k0031 select * from k0031;
analyze k0031;
explain select * from k0031 where k2=1;
explain select * from k0031 where k3=1;
explain select * from k0031 where k4=1;
explain select * from k0031 where k2='1';
explain select * from k0031 where k3='1';
explain select * from k0031 where k4='1';
-- end ID884863681

-- TAPD: 886100683
CREATE OR REPLACE FUNCTION test_case_ops_f1_20231026(int4, int8) RETURNS int8 AS
$$SELECT coalesce($1,0)::int8$$ LANGUAGE sql IMMUTABLE;
CREATE OPERATOR CLASS test_case_opc1_20231026 FOR TYPE int4 USING HASH AS
OPERATOR 1 = , FUNCTION 2 test_case_ops_f1_20231026(int4, int8);
CREATE OR REPLACE FUNCTION test_case_ops_f2_20231026(text, int8) RETURNS int8 AS
$$SELECT length(coalesce($1,''))::int8$$ LANGUAGE sql IMMUTABLE;
CREATE OPERATOR CLASS test_case_opc2_20231026 FOR TYPE text USING HASH AS
OPERATOR 1 = , FUNCTION 2 test_case_ops_f2_20231026(text, int8);
CREATE TABLE test_case_ops_rel_20231026 (a int, b text, c jsonb)
    PARTITION BY HASH (a test_case_opc1_20231026, b test_case_opc2_20231026);
DROP TABLE test_case_ops_rel_20231026;
DROP OPERATOR CLASS test_case_opc1_20231026 USING HASH;
DROP OPERATOR CLASS test_case_opc2_20231026 USING HASH;
DROP FUNCTION test_case_ops_f1_20231026;
DROP FUNCTION test_case_ops_f2_20231026;

-- START TAPD: 887262271
select regexp_split_to_array(' ', ' ');
select regexp_split_to_array(' ', '');
select regexp_split_to_array('', ' ');
select regexp_split_to_array('   ', '  ');
select regexp_split_to_array('    ', '  ');
-- END TPAD: 887262271
--
-- test to_binary_float
--
select to_binary_float(123);
select to_binary_float(123.456);
select to_binary_float('123.456'::text);
select to_binary_float('123.456'::varchar);
select to_binary_float('123.456'::varchar2);
select to_binary_float(123::smallint);
select to_binary_float(123::int);
select to_binary_float(123::bigint);
select to_binary_float(123::numeric);
select to_binary_float(123.456::real);
select to_binary_float(123.456::double precision);
select to_binary_float(123.456::binary_float);
select to_binary_float(123.456::binary_double);
select to_binary_float('123.456', '999.99');
select to_binary_float(123.45678901234);
select to_binary_float('123.4567890123456789');
select to_binary_float('NaN');
select to_binary_float(123.456, '999.99');

--
-- test to_binary_double
--
select to_binary_double(123);
select to_binary_double(123.456);
select to_binary_double('123.456'::text);
select to_binary_double('123.456'::varchar);
select to_binary_double('123.456'::varchar2);
select to_binary_double(123::smallint);
select to_binary_double(123::int);
select to_binary_double(123::bigint);
select to_binary_double(123::numeric);
select to_binary_double(123.456::real);
select to_binary_double(123.456::double precision);
select to_binary_double(123.456::binary_float);
select to_binary_double(123.456::binary_double);
select to_binary_double('123.456', '999.99');
select to_binary_double(123.45678901234);
select to_binary_double('123.4567890123456789');
select to_binary_double('NaN');
select to_binary_double(123.456, '999.99');
