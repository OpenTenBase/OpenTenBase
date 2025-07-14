CREATE EXTENSION redis_fdw;

CREATE SERVER localredis FOREIGN DATA WRAPPER redis_fdw;
CREATE USER MAPPING FOR public SERVER localredis;

CREATE FOREIGN TABLE rft_str(
	key    TEXT,
	value  TEXT,
	expiry INT
) SERVER localredis
  OPTIONS (tabletype 'string');

CREATE TEMP TABLE bt(
	k	TEXT,
	v	TEXT
);

COPY bt (k, v) FROM STDIN;
key1	value-1
key2	value-2
key3	value-3
key4	value-4
key5	value-5
key6	value-6
key7	value-7
key8	value-8
key9	value-9
\.

INSERT INTO rft_str(key, value)
SELECT * FROM bt
RETURNING *;

SELECT * FROM rft_str WHERE key = 'key3';

DELETE FROM rft_str WHERE key = 'key1';
DELETE FROM rft_str WHERE key = 'key2';
DELETE FROM rft_str WHERE key = 'key3';
DELETE FROM rft_str WHERE key = 'key4';
DELETE FROM rft_str WHERE key = 'key5';
DELETE FROM rft_str WHERE key = 'key6';
DELETE FROM rft_str WHERE key = 'key7';
DELETE FROM rft_str WHERE key = 'key8';
DELETE FROM rft_str WHERE key = 'key9';

DROP TABLE bt;
DROP FOREIGN TABLE rft_str;
