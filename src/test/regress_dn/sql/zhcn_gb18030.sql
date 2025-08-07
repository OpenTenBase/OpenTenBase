--
-- gbk
--
\c db_gbk;
SET client_encoding = gbk;

-- regular expression query
SELECT * FROM tbl_gbk WHERE f1 ~ '^��' ORDER BY f1;

DROP TABLE tbl_gbk;
CREATE TABLE tbl_gbk(f1 varchar(3));
INSERT INTO tbl_gbk (f1) VALUES ('�˶���');
INSERT INTO tbl_gbk (f1) VALUES ('�����');
-- �F is not support by euc_cn, but support on gbk
INSERT INTO tbl_gbk (f1) VALUES ('���F��');
INSERT INTO tbl_gbk (f1) VALUES ('���Ұ�');
INSERT INTO tbl_gbk (f1) VALUES ('��һλ');
INSERT INTO tbl_gbk (f1) VALUES ('����');
-- error
INSERT INTO tbl_gbk (f1) VALUES ('���Ұ�2');

-- order by
SELECT * FROM tbl_gbk ORDER BY f1;

-- regular expression query
SELECT * FROM tbl_gbk WHERE f1 ~ '^��' ORDER BY f1;

-- query encoding length
SELECT OCTET_LENGTH(f1) FROM tbl_gbk ORDER BY f1;


--
-- gb18030
--
\c "DB_GB18030";

SET client_encoding = gb18030;
-- regular expression query
SELECT * FROM tbl_gb18030 WHERE f1 ~ '^��' ORDER BY f1;
SELECT * FROM tbl_gb18030 WHERE f1 ~ '^?0?0' ORDER BY f1;

DROP TABLE tbl_gb18030;
CREATE TABLE tbl_gb18030(f1 varchar(3));
INSERT INTO tbl_gb18030 (f1) VALUES ('�˶���');
INSERT INTO tbl_gb18030 (f1) VALUES ('�����');
-- �F is not support by euc_cn, but support on gb18030
INSERT INTO tbl_gb18030 (f1) VALUES ('���F��');
INSERT INTO tbl_gb18030 (f1) VALUES ('���Ұ�');
INSERT INTO tbl_gb18030 (f1) VALUES ('��һλ');
INSERT INTO tbl_gb18030 (f1) VALUES ('����');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_gb18030 (f1) VALUES ('?0?0?3?3?5?3');
-- out of bound error
INSERT INTO tbl_gb18030 (f1) VALUES ('���Ұ�2');
INSERT INTO tbl_gb18030 (f1) VALUES ('?0?0?3?3?5?32');

-- order by
SELECT * FROM tbl_gb18030 ORDER BY f1;
-- regular expression query
SELECT * FROM tbl_gb18030 WHERE f1 ~ '^��' ORDER BY f1;
SELECT * FROM tbl_gb18030 WHERE f1 ~ '^?0?0' ORDER BY f1;

-- query encoding length
SELECT OCTET_LENGTH(f1) FROM tbl_gb18030 ORDER BY f1;

-- test redistribution
CREATE TABLE tbl_gb18030_re(i int, name_varchar varchar(32), name_name name);
INSERT INTO tbl_gb18030_re(i, name_varchar, name_name) SELECT length(f1), f1, f1::name from tbl_gb18030;
SELECT * FROM tbl_gb18030_re ORDER BY name_varchar;
SELECT * FROM tbl_gb18030_re ORDER BY name_name;
SELECT * FROM tbl_gb18030_re WHERE name_varchar ~ '^��' ORDER BY name_varchar;
SELECT * FROM tbl_gb18030_re WHERE name_varchar ~ '^?0?0' ORDER BY name_varchar;
SELECT * FROM tbl_gb18030_re WHERE name_name ~ '^��' ORDER BY name_name;
SELECT * FROM tbl_gb18030_re WHERE name_name ~ '^?0?0' ORDER BY name_name;
DROP TABLE tbl_gb18030_re;
