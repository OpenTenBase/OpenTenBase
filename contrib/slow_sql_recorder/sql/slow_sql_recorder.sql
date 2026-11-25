CREATE EXTENSION slow_sql_recorder;
SET slow_sql_recorder.ssl_switch = TRUE;
SET slow_sql_recorder.min_query_duration = 0;

--
-- Setup
--
-- 创建表
CREATE TABLE example_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    age INT
);

-- 插入数据
INSERT INTO example_table (name, age) VALUES
    ('John', 25),
    ('Jane', 30),
    ('Bob', 22),
    ('Alice', 28),
    ('Charlie', 35),
    ('Diana', 27),
    ('Eve', 32),
    ('Frank', 40),
    ('Grace', 29),
    ('Henry', 33);
CREATE TABLE t1 (a int, b text);
INSERT INTO t1 VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc');

CREATE TABLE t2 (x int, y text);
INSERT INTO t2 VALUES (1, 'xxx'), (2, 'yyy'), (3, 'zzz');

CREATE TABLE t3 (s int, t text);
INSERT INTO t3 VALUES (1, 'sss'), (2, 'ttt'), (3, 'uuu');

CREATE TABLE t4 (m int, n text);
INSERT INTO t4 VALUES (1, 'mmm'), (2, 'nnn'), (3, 'ooo');

CREATE TABLE t5 (e text, f text, g text);

---
-- partitioned table parent
CREATE TABLE t1p (o int, p text, q text) PARTITION BY RANGE (o);

-- partitioned table children
CREATE TABLE t1p_ones PARTITION OF t1p FOR VALUES FROM ('0') TO ('10');
CREATE TABLE t1p_tens PARTITION OF t1p FOR VALUES FROM ('10') TO ('100');

---

CREATE TABLE customer (cid int primary key, cname text, ccredit text);
INSERT INTO customer VALUES (1, 'Taro',   '1111-2222-3333-4444'),
                            (2, 'Hanako', '5555-6666-7777-8888');
CREATE FUNCTION customer_credit(int) RETURNS text
    AS 'SELECT regexp_replace(ccredit, ''-[0-9]+$'', ''-????'') FROM customer WHERE cid = $1'
    LANGUAGE sql;

SELECT objtype, objname, label FROM pg_seclabels
    WHERE provider = 'selinux'
     AND  objtype in ('table', 'column')
     AND  objname in ('t1', 't2', 't3', 't4',
                      't5', 't5.e', 't5.f', 't5.g',
                      't1p', 't1p.o', 't1p.p', 't1p.q',
                      't1p_ones', 't1p_ones.o', 't1p_ones.p', 't1p_ones.q',
                      't1p_tens', 't1p_tens.o', 't1p_tens.p', 't1p_tens.q')
ORDER BY objname COLLATE "C";

CREATE SCHEMA my_schema_1;
CREATE TABLE my_schema_1.ts1 (a int, b text);
CREATE TABLE my_schema_1.pts1 (o int, p text) PARTITION BY RANGE (o);
CREATE TABLE my_schema_1.pts1_ones PARTITION OF my_schema_1.pts1 FOR VALUES FROM ('0') to ('10');

CREATE SCHEMA my_schema_2;
CREATE TABLE my_schema_2.ts2 (x int, y text);
CREATE TABLE my_schema_2.pts2 (o int, p text) PARTITION BY RANGE (o);
CREATE TABLE my_schema_2.pts2_tens PARTITION OF my_schema_2.pts2 FOR VALUES FROM ('10') to ('100');


-- Hardwired Rules
UPDATE pg_attribute SET attisdropped = true
    WHERE attrelid = 't5'::regclass AND attname = 'f';	-- failed

--
-- Simple DML statements
--
-- @SECURITY-CONTEXT=unconfined_u:unconfined_r:sepgsql_regtest_user_t:s0

SELECT * FROM t1;			-- ok
SELECT * FROM t2;			-- ok
SELECT * FROM t3;			-- ok
SELECT * FROM t4;			-- failed
SELECT * FROM t5;			-- failed

---
-- partitioned table parent
SELECT * FROM t1p;			-- failed
SELECT o,p FROM t1p;		-- ok
--partitioned table children
SELECT * FROM t1p_ones;			-- failed
SELECT o FROM t1p_ones;			-- ok
SELECT o,p FROM t1p_ones;		-- ok
SELECT * FROM t1p_tens;			-- failed
SELECT o FROM t1p_tens;			-- ok
SELECT o,p FROM t1p_tens;		-- ok
---

SELECT * FROM customer;									-- failed
SELECT cid, cname, customer_credit(cid) FROM customer;	-- ok

SELECT count(*) FROM t5;					-- ok
SELECT count(*) FROM t5 WHERE g IS NULL;	-- failed

---
-- partitioned table parent
SELECT count(*) FROM t1p;					-- ok
SELECT count(*) FROM t1p WHERE q IS NULL;	-- failed
-- partitioned table children
SELECT count(*) FROM t1p_ones;					-- ok
SELECT count(*) FROM t1p_ones WHERE q IS NULL;	-- failed
SELECT count(*) FROM t1p_tens;					-- ok
SELECT count(*) FROM t1p_tens WHERE q IS NULL;	-- failed