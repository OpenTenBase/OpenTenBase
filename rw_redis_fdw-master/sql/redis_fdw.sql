-- ===================================================================
-- create FDW objects
-- ===================================================================

CREATE EXTENSION redis_fdw;

CREATE SERVER localredis FOREIGN DATA WRAPPER redis_fdw;
CREATE USER MAPPING FOR public SERVER localredis;

-- ===================================================================
-- create objects used through FDW loopback server
-- ===================================================================

-- STRING
CREATE FOREIGN TABLE rft_str(
	key    TEXT,
	sval   TEXT,     -- relabeled field
	expiry INT
) SERVER localredis
  OPTIONS (tabletype 'string', keyprefix 'rftc_', database '1');
ALTER FOREIGN TABLE rft_str ALTER COLUMN sval OPTIONS (ADD redis 'value');

-- HASH
CREATE FOREIGN TABLE rft_hash(
	key    TEXT,
	field  TEXT,
	value  TEXT,
	expiry INT
) SERVER localredis
  OPTIONS (tabletype 'hash', keyprefix 'rfth_', database '1');


-- MHASH
CREATE FOREIGN TABLE rft_mhash(
	key    TEXT,
	field  TEXT[],
	value  TEXT[],
	expiry INT
) SERVER localredis
  OPTIONS (tabletype 'mhash', keyprefix 'rfth_', database '1');


-- LIST
CREATE FOREIGN TABLE rft_list(
	key    TEXT,
	value  TEXT,
	"index" INT,
	expiry INT
) SERVER localredis
  OPTIONS (tabletype 'list', keyprefix 'rftl_', database '1');


-- SET
CREATE FOREIGN TABLE rft_set(
	key    TEXT,
	member TEXT,
	expiry INT
) SERVER localredis
  OPTIONS (tabletype 'set', keyprefix 'rfts_', database '1');

-- ZSET
CREATE FOREIGN TABLE rft_zset(
	key     TEXT,
	member  TEXT,
	score   INT,
	"index" INT,
	expiry  INT
) SERVER localredis
  OPTIONS (tabletype 'zset', keyprefix 'rftz_', database '1');

-- TTL
CREATE FOREIGN TABLE rft_ttl(
	key    TEXT,
	expiry INT
) SERVER localredis
  OPTIONS (tabletype 'ttl', database '1');

-- LEN
CREATE FOREIGN TABLE rft_len(
	key       TEXT,
	tabletype TEXT,
	len       INT,
	expiry    INT
) SERVER localredis
  OPTIONS (tabletype 'len', database '1');

-- PUBLISH
CREATE FOREIGN TABLE rft_pub(
	channel   TEXT,
	message   TEXT,
	len       INT
) SERVER localredis
  OPTIONS (tabletype 'publish', database '1');

-- KEYS
CREATE FOREIGN TABLE rft_keys(
	key       TEXT
) SERVER localredis
  OPTIONS (tabletype 'keys', database '1');


-- ===================================================================
-- simple insert
-- ===================================================================

-- STRING
INSERT INTO rft_str (key, sval) VALUES ('strkey', 'strval');
INSERT INTO rft_str (key, sval, expiry) VALUES ('strkey2', 'has-expiry', 30);

SELECT * FROM rft_str WHERE key = 'strkey';
SELECT * FROM rft_str WHERE key = 'strkey2';

UPDATE rft_str SET sval = (SELECT 'updated-strval'::TEXT) WHERE key = 'strkey';
SELECT * FROM rft_str WHERE key = 'strkey';
SELECT * FROM rft_str WHERE key = 'strkey2';

UPDATE rft_str SET sval = 'updated-strval2' WHERE key = (SELECT 'strkey'::TEXT);
SELECT * FROM rft_str WHERE key = 'strkey';

-- HASH
INSERT INTO rft_hash (key, field, value) VALUES ('hkey', 'f1', 'v1');
INSERT INTO rft_hash (key, field, value, expiry) VALUES ('hkey', 'f2', 'v2', 10);
INSERT INTO rft_hash (key, field, value, expiry) VALUES ('hkey', 'f4', 'v4', 10);
INSERT INTO rft_hash (key, field, value, expiry) VALUES ('hkey2', 'f2', 'v2', 10);

SELECT * FROM rft_hash WHERE key = 'hkey';
SELECT * FROM rft_hash WHERE key = 'hkey' AND field = 'f1';
SELECT * FROM rft_hash WHERE key = 'hkey' AND field = 'f4';
SELECT * FROM rft_hash WHERE key = 'hkey2' AND field = 'f2';

--    non-exist
SELECT * FROM rft_hash WHERE key = 'hkey' AND field = 'f3';

UPDATE rft_hash SET value = 'v1-updated' WHERE key = 'hkey' AND field = 'f1'
       RETURNING *;
SELECT * FROM rft_hash WHERE key = (SELECT 'hkey'::TEXT);

-- MHASH/HMSET
SELECT * FROM rft_mhash WHERE key = 'hkey';

-- *** The following insert is expected to fail because
--     insertion not allowed into mhash
INSERT INTO rft_mhash (key, field, value) VALUES ('hkey', '{foo}', '{fee}');

-- SET
INSERT INTO rft_set (key, member) VALUES ('skey', 'member1');
INSERT INTO rft_set (key, member) VALUES ('skey', 'member2');
INSERT INTO rft_set (key, member) VALUES ('skey', 'member3');
INSERT INTO rft_set (key, member) VALUES ('skey', 'member4') RETURNING *;

SELECT * FROM rft_set WHERE key = 'skey' ORDER BY member;

-- LIST
INSERT INTO rft_list (key, value, "index") VALUES ('lkey', 'idx0', 0);
INSERT INTO rft_list (key, value, "index") VALUES ('lkey', 'idx1', 1);
INSERT INTO rft_list (key, value, "index") VALUES ('lkey', 'idx2', 2);
INSERT INTO rft_list (key, value, "index") VALUES ('lkey', 'idx3', 3);

SELECT * FROM rft_list WHERE key = 'lkey';

UPDATE rft_list SET value = 'updated-idx2' WHERE index = 1 and key = 'lkey';

SELECT * FROM rft_list WHERE key = 'lkey';

DELETE FROM rft_list WHERE key = 'lkey' AND value = 'idx3';

SELECT * FROM rft_list WHERE key = 'lkey';

--    delete non-existent value
DELETE FROM rft_list WHERE key = 'lkey' AND value = 'some-value' RETURNING *;
SELECT * FROM rft_list WHERE key = 'lkey';

-- ZSET
INSERT INTO rft_zset (key, member, score) VALUES ('zkey', 'member1', 1);
INSERT INTO rft_zset (key, member, score) VALUES ('zkey', 'member2', 2);
INSERT INTO rft_zset (key, member, score) VALUES ('zkey', 'member3', 3);

SELECT * FROM rft_zset WHERE key = 'zkey';

SELECT * FROM rft_zset WHERE key = 'zkey' AND member = 'member2';

SELECT * FROM rft_zset WHERE key = 'zkey' AND index = 1;
SELECT * FROM rft_zset WHERE key = 'zkey' AND index > 1 AND index <= 3;
SELECT * FROM rft_zset WHERE key = 'zkey' AND index > 0 AND index < 3;

SELECT * FROM rft_zset WHERE key = 'zkey' AND score > 1;

-- TTL
SELECT * FROM rft_ttl WHERE key = 'rftz_zkey';
UPDATE rft_ttl SET expiry = 3 WHERE key = 'rftz_zkey';
SELECT * FROM rft_ttl WHERE key = 'rftz_zkey';

-- LEN
SELECT * FROM rft_len WHERE key = 'rftz_zkey' AND tabletype = 'zset';
SELECT * FROM rft_len WHERE key = '*';

-- PUBLISH
-- only SELECT and INSERT permitted
INSERT INTO rft_pub VALUES('chan', 'message') RETURNING *;
SELECT * FROM rft_pub WHERE channel = 'chan';

-- update will fail as it is not supported for "publish"
UPDATE rft_pub SET message = 'something' WHERE channel = 'chan';

-- delete will fail as it is not supported for "publish"
DELETE FROM rft_pub WHERE channel = 'chan';

-- list keys
SELECT * FROM rft_keys;

-- list keys with pattern
SELECT * FROM rft_keys WHERE key = 'rftz*';

-- ===================================================================
-- delete
-- ===================================================================

DELETE FROM rft_hash WHERE key = 'hkey' AND field = 'f2';
SELECT * FROM rft_hash WHERE key = 'hkey';
SELECT * FROM rft_hash WHERE key = 'hkey' AND field = 'f2';

-- show table in mhash format
SELECT * FROM rft_mhash WHERE key = 'hkey';


-- delete entire key
DELETE FROM rft_hash WHERE key = 'hkey';
SELECT * FROM rft_hash WHERE key = 'hkey';

-- delete list item at index
DELETE FROM rft_list WHERE key = 'lkey' AND index = 2;
SELECT * FROM rft_list WHERE key = 'lkey';

-- delete first list item
DELETE FROM rft_list WHERE key = 'lkey' AND index = 0;
SELECT * FROM rft_list WHERE key = 'lkey';

DELETE FROM rft_set WHERE key = 'skey' AND member = 'member2';
SELECT * FROM rft_set WHERE key = 'skey' ORDER BY member;
SELECT * FROM rft_set WHERE key = 'skey' AND member = 'member3';

DELETE FROM rft_zset WHERE key = 'zkey' AND member = 'member3';
SELECT * FROM rft_zset WHERE key = 'zkey';

-- ===================================================================
-- cleanup
-- ===================================================================

-- delete all keys
DELETE FROM rft_str WHERE key = 'strkey';
DELETE FROM rft_str WHERE key = 'strkey2';

DELETE FROM rft_hash WHERE key = 'hkey';
DELETE FROM rft_hash WHERE key = 'hkey2';

DELETE FROM rft_list WHERE key = 'lkey';
SELECT * FROM rft_list WHERE key = 'lkey';

DELETE FROM rft_zset WHERE key = 'zkey';
SELECT * FROM rft_zset WHERE key = 'zkey';

-- list remaining keys

SELECT * FROM rft_keys;

DROP FOREIGN TABLE rft_str;
DROP FOREIGN TABLE rft_hash;
DROP FOREIGN TABLE rft_mhash;
DROP FOREIGN TABLE rft_list;
DROP FOREIGN TABLE rft_set;
DROP FOREIGN TABLE rft_zset;
DROP FOREIGN TABLE rft_ttl;
DROP FOREIGN TABLE rft_len;
DROP FOREIGN TABLE rft_pub;
DROP FOREIGN TABLE rft_keys;
