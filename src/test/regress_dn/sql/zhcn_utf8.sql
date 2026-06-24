--
-- gbk
--
CREATE DATABASE db_gbk template template0_ora encoding = gbk LC_COLLATE = 'zh_CN.gbk' LC_CTYPE = 'zh_CN.gbk';
\c db_gbk;
set client_encoding = utf8;

CREATE TABLE tbl_gbk(f1 varchar(3));
INSERT INTO tbl_gbk (f1) VALUES ('邓东宝');
INSERT INTO tbl_gbk (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gbk
INSERT INTO tbl_gbk (f1) VALUES ('朱镕非');
INSERT INTO tbl_gbk (f1) VALUES ('王家坝');
INSERT INTO tbl_gbk (f1) VALUES ('王一位');
INSERT INTO tbl_gbk (f1) VALUES ('怡宝');
-- error
INSERT INTO tbl_gbk (f1) VALUES ('王家坝2');

-- order by
SELECT * FROM tbl_gbk ORDER BY f1;

-- regular expression query
SELECT * FROM tbl_gbk WHERE f1 ~ '^王' ORDER BY f1;

-- query encoding length
SELECT OCTET_LENGTH(f1) FROM tbl_gbk ORDER BY f1;

-- MATERIALIZED VIEW join
CREATE TABLE T_PERSON(i int, n varchar(32));
INSERT INTO T_PERSON VALUES (1, '韩梅梅');
INSERT INTO T_PERSON VALUES (2, '张雷');
CREATE TABLE T_NICK(id int, name varchar(32));
INSERT INTO T_NICK VALUES (1, '叶子');
INSERT INTO T_NICK VALUES (2, '蓝天');
CREATE MATERIALIZED VIEW T_MATER AS SELECT * FROM T_PERSON  WITH NO DATA;
REFRESH MATERIALIZED VIEW T_MATER;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by i;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by name;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by n;
DROP MATERIALIZED VIEW T_MATER;
CREATE MATERIALIZED VIEW T_MATER AS SELECT * FROM T_PERSON;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by i;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by name;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by n;
INSERT INTO T_PERSON VALUES (3, '韩红');
INSERT INTO T_NICK VALUES (3, '白色');
REFRESH MATERIALIZED VIEW T_MATER;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by i;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by name;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by n;
DROP MATERIALIZED VIEW T_MATER;
DROP TABLE T_PERSON;
DROP TABLE T_NICK;

-- test redistribution
CREATE TABLE T_PERSON(i int, n varchar(32));
INSERT INTO T_PERSON VALUES (1, '韩梅梅');
INSERT INTO T_PERSON VALUES (2, '张雷');
CREATE TABLE TT_PERSON(i int, n varchar(32)) DISTRIBUTE BY SHARD(n);
INSERT INTO TT_PERSON SELECT * FROM T_PERSON;
SELECT * FROM TT_PERSON ORDER BY 1;
DROP TABLE T_PERSON;

CREATE TABLE T_PERSON(i int, n name);
INSERT INTO T_PERSON VALUES (1, '韩梅梅');
INSERT INTO T_PERSON VALUES (2, '张雷');
CREATE TABLE TTT_PERSON(i int, n name) DISTRIBUTE BY SHARD(n);
INSERT INTO TTT_PERSON SELECT * FROM T_PERSON;
SELECT * FROM TTT_PERSON ORDER BY 1;

DROP TABLE T_PERSON;
DROP TABLE TT_PERSON;
DROP TABLE TTT_PERSON;

--
-- gb18030
--
CREATE DATABASE db_gb18030 template template0_ora encoding = gb18030 LC_COLLATE = 'zh_CN.gb18030' LC_CTYPE = 'zh_CN.gb18030';
\c "DB_GB18030";

CREATE TABLE tbl_gb18030(f1 varchar(3));
INSERT INTO tbl_gb18030 (f1) VALUES ('邓东宝');
INSERT INTO tbl_gb18030 (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_gb18030 (f1) VALUES ('朱镕非');
INSERT INTO tbl_gb18030 (f1) VALUES ('王家坝');
INSERT INTO tbl_gb18030 (f1) VALUES ('王一位');
INSERT INTO tbl_gb18030 (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_gb18030 (f1) VALUES ('𣘗𧄧');
-- out of bound error
INSERT INTO tbl_gb18030 (f1) VALUES ('王家坝2');
INSERT INTO tbl_gb18030 (f1) VALUES ('𣘗𧄧2');

-- text
CREATE TABLE tbl_text(i int, f1 text);
INSERT INTO tbl_text (f1) VALUES ('邓东宝');
INSERT INTO tbl_text (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_text (f1) VALUES ('朱镕非');
INSERT INTO tbl_text (f1) VALUES ('王家坝');
INSERT INTO tbl_text (f1) VALUES ('王一位');
INSERT INTO tbl_text (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_text (f1) VALUES ('𣘗𧄧');
SELECT * FROM tbl_text ORDER BY f1;

-- nvarchar2
CREATE TABLE tbl_nvarchar2(i int, f1 nvarchar2(3) );
INSERT INTO tbl_nvarchar2 (f1) VALUES ('邓东宝');
INSERT INTO tbl_nvarchar2 (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_nvarchar2 (f1) VALUES ('朱镕非');
INSERT INTO tbl_nvarchar2 (f1) VALUES ('王家坝');
INSERT INTO tbl_nvarchar2 (f1) VALUES ('王一位');
INSERT INTO tbl_nvarchar2 (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_nvarchar2 (f1) VALUES ('𣘗𧄧');
SELECT * FROM tbl_nvarchar2 ORDER BY f1;

-- bpchar
CREATE TABLE tbl_bpchar(i int, f1 bpchar(3) );
INSERT INTO tbl_bpchar (f1) VALUES ('邓东宝');
INSERT INTO tbl_bpchar (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_bpchar (f1) VALUES ('朱镕非');
INSERT INTO tbl_bpchar (f1) VALUES ('王家坝');
INSERT INTO tbl_bpchar (f1) VALUES ('王一位');
INSERT INTO tbl_bpchar (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_bpchar (f1) VALUES ('𣘗𧄧');
SELECT * FROM tbl_bpchar ORDER BY f1;

-- char
CREATE TABLE tbl_char(i int, f1 char(3) );
INSERT INTO tbl_char (f1) VALUES ('邓东宝');
INSERT INTO tbl_char (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_char (f1) VALUES ('朱镕非');
INSERT INTO tbl_char (f1) VALUES ('王家坝');
INSERT INTO tbl_char (f1) VALUES ('王家1');
INSERT INTO tbl_char (f1) VALUES ('王家2');
INSERT INTO tbl_char (f1) VALUES ('王一位');
INSERT INTO tbl_char (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_char (f1) VALUES ('𣘗𧄧');
SELECT * FROM tbl_char ORDER BY f1;

-- order by
SELECT * FROM tbl_gb18030 ORDER BY f1;

-- regular expression query
SELECT * FROM tbl_gb18030 WHERE f1 ~ '^王' ORDER BY f1;

-- query encoding length
SELECT OCTET_LENGTH(f1) FROM tbl_gb18030 ORDER BY f1;

-- MATERIALIZED VIEW join
CREATE TABLE T_PERSON(i int, n varchar(32));
INSERT INTO T_PERSON VALUES (1, '韩梅梅');
INSERT INTO T_PERSON VALUES (2, '李雷');
CREATE TABLE T_NICK(id int, name varchar(32));
INSERT INTO T_NICK VALUES (1, '叶子');
INSERT INTO T_NICK VALUES (2, '蓝天');
CREATE MATERIALIZED VIEW T_MATER AS SELECT * FROM T_PERSON  WITH NO DATA;
REFRESH MATERIALIZED VIEW T_MATER;
SELECT * FROM T_NICK n JOIN T_MATER p on n.id=p.i order by i;
SELECT * FROM T_NICK n JOIN T_MATER p on n.id=p.i order by name;
SELECT * FROM T_NICK n JOIN T_MATER p on n.id=p.i order by n;
DROP MATERIALIZED VIEW T_MATER;
DROP TABLE T_PERSON;
DROP TABLE T_NICK;

--
-- utf8
--
CREATE DATABASE db_utf8 template template0_ora encoding = UTF8;
\c DB_UTF8
CREATE TABLE tbl_utf8(f1 varchar(3));
INSERT INTO tbl_utf8 (f1) VALUES ('邓东宝');
INSERT INTO tbl_utf8 (f1) VALUES ('李尔王');
DROP TABLE tbl_utf8;

drop table if exists instr_t cascade;
create table instr_t(id int, c1 char(10), c2 varchar2(20), c3 nchar(20), c4 nvarchar2(20), c5 clob, c7 varchar2(100), c8 integer);
insert into instr_t values(1, '&|GQ4|F', '\ox}B+6"fW', '2~"(yvfAwb', ')_Wrq@deO]K^h hU', '4//^7Ngnp{kN/x0}|SKN+PM$ZYlC_&([NvN^72B}~BVQm_cIdK69e+Boi{#wUpJ?s;-G,R||u" y02N@', '贴贴贴贴贴贴贴贴贴缿', 64);
insert into instr_t values(2, '你好', 'sasas', 'als打算', 'daasda打算', 'ask了解奥斯陆扩大斯柯达打算看了大量开始', '打卡军事科技大厦321sda', 22);
insert into instr_t values(3, '你好123', '大苏打', '465打', '879as我', '恶趣味45', '打卡军事科sd技大厦321sda', 33);
insert into instr_t values(4, 'dasd11', '12阿大撒', '13手打是', '132大撒', '而我却', '123eqw', 10);
insert into instr_t values(5, '尿尿', '79RAs=/+OM', '3zR8pg3AISz大撒', '@7[-K[djS.BZQ 2P1大撒', 'M0c^o!ov/|28[f/p$n', '大苏打', 5);
-- 插入null值
insert into instr_t values(6, '', '79RAs=/+OM', '', '', 'M0c^o!ov/|28[f/p$n', '', null);
insert into instr_t values(7, 'das ', '', '123', '', '', '', null);

select 
instr(c1, '你好', -2, 1) as c1,
instr(c2, '大', -3, 1) as c2,
instr(c3, '456', 3, 1) as c3,
instr(c4, '我', 1, 2) as c4,
instr(c5, '恶趣', 0, 1) as c5 ,
instr(c7, '苏打', 1, 1) as c7
from instr_t order by id;
drop table instr_t;

-- Trim parameter length detection, suitable for different codes
select trim(both '啊' from '啊中国啊') "test_trim" from dual;
select trim(leading '啊' from '啊中国啊') "test_trim" from dual;
select trim(trailing '啊' from '啊中国啊') "test_trim" from dual;
select trim(both '报错' from '啊中国啊') "test_trim" from dual;
select trim(leading '报错' from '啊中国啊') "test_trim" from dual;
select trim(trailing '报错' from '啊中国啊') "test_trim" from dual;
SELECT trim(leading 'df' from 'dfssa') FROM dual;
SELECT trim(both '13' from '123sfd111') FROM dual;
SELECT trim(trailing '23' from '213dsq12') FROM dual;

\c regression

CREATE DATABASE db_gb18030_2022 template template0_ora encoding = gb18030_2022 LC_COLLATE = 'zh_CN.gb18030' LC_CTYPE = 'zh_CN.gb18030';
\c db_gb18030_2022
create table test_gb18030_2022_ora(id int, f1 varchar(64), f2 text);
insert into test_gb18030_2022_ora(id, f2, f1) values (1, '你好dhdh', '这真是一个难的SQL');
-- support gb18030-2005
insert into test_gb18030_2022_ora(id, f2, f1) values (2, '𣘗𧄧', '朱镕非鶣个');
insert into test_gb18030_2022_ora(id, f2, f1) values (3, '，！????/dfd', '45￥%@￥#￥@%^@^!@@*!>>][]][,,..，。，');

-- support new encode
insert into test_gb18030_2022_ora(id, f2, f1) values (4, '%^%^//︐、、、]]]】】', 'insert is ok');

-- test like
insert into test_gb18030_2022_ora(id, f2, f1) values (5, 'ddser f︐jjdfhd', '鎐ok');
insert into test_gb18030_2022_ora(id, f2, f1) values (6, 'nih好︐nice', '𡗗ok');
insert into test_gb18030_2022_ora(id, f2, f1) values (7, 'f地方会计师︐dfhiu回复的', 'insert is ok');

-- PG also not format these column
select * from test_gb18030_2022_ora order by id desc;

select * from test_gb18030_2022_ora where f2 = '︐' order by id desc ;
select * from test_gb18030_2022_ora where f2 like '%︐%' order by id desc ;
-- test new encode hex
select '︐'::bytea;
select '𡗗'::bytea;

\c regression
create table test_gb18030_2022(id int, f1 varchar(64), f2 text);

insert into test_gb18030_2022(id, f2, f1) values (1, '你好dhdh', '这真是一个难的SQL');
-- support gb18030-2005
insert into test_gb18030_2022(id, f2, f1) values (2, '𣘗𧄧', '朱镕非鶣个');
insert into test_gb18030_2022(id, f2, f1) values (3, '，！????/dfd', '45￥%@￥#￥@%^@^!@@*!>>][]][,,..，。，');

-- support new encode
insert into test_gb18030_2022(id, f2, f1) values (4, '︐', 'insert is ok');

-- test like
insert into test_gb18030_2022(id, f2, f1) values (5, '︐jjdfhd', '鎐ok');
insert into test_gb18030_2022(id, f2, f1) values (6, 'nih好︐', '𡗗ok');
insert into test_gb18030_2022(id, f2, f1) values (7, 'f地方会计师︐dfhiu回复的', 'insert is ok');


select * from test_gb18030_2022 order by id desc;

select * from test_gb18030_2022 where f2 = '︐' order by id desc ;
select * from test_gb18030_2022 where f2 like '%︐%' order by id desc ;
-- test new encode hex
select '︐'::bytea;
select '𡗗'::bytea;
