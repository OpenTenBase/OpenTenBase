\c contrib_regression
--
-- HMAC-MD5
--

SELECT encode(hmac(
'Hi There',
decode('0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b', 'hex'),
'sha1'), 'hex');

-- 2
SELECT encode(hmac(
'Jefe',
'what do ya want for nothing?',
'sha1'), 'hex');

-- 3
SELECT encode(hmac(
decode('dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd', 'hex'),
decode('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex'),
'sha1'), 'hex');

-- 4
SELECT encode(hmac(
decode('cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd', 'hex'),
decode('0102030405060708090a0b0c0d0e0f10111213141516171819', 'hex'),
'sha1'), 'hex');

-- 5
SELECT encode(hmac(
'Test With Truncation',
decode('0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c', 'hex'),
'sha1'), 'hex');

-- 6
SELECT encode(hmac(
'Test Using Larger Than Block-Size Key - Hash Key First',
decode('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex'),
'sha1'), 'hex');

-- 7
SELECT encode(hmac(
'Test Using Larger Than Block-Size Key and Larger Than One Block-Size Data',
decode('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex'),
'sha1'), 'hex');
\c contrib_regression_ora
--
-- HMAC-MD5
--

SELECT encode(hmac(
'Hi There',
pg_catalog.decode('0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b', 'hex'),
'sha1'), 'hex');

-- 2
SELECT encode(hmac(
'Jefe',
'what do ya want for nothing?',
'sha1'), 'hex');

-- 3
SELECT encode(hmac(
pg_catalog.decode('dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd', 'hex'),
pg_catalog.decode('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex'),
'sha1'), 'hex');

-- 4
SELECT encode(hmac(
pg_catalog.decode('cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd', 'hex'),
pg_catalog.decode('0102030405060708090a0b0c0d0e0f10111213141516171819', 'hex'),
'sha1'), 'hex');

-- 5
SELECT encode(hmac(
'Test With Truncation',
pg_catalog.decode('0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c', 'hex'),
'sha1'), 'hex');

-- 6
SELECT encode(hmac(
'Test Using Larger Than Block-Size Key - Hash Key First',
pg_catalog.decode('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex'),
'sha1'), 'hex');

-- 7
SELECT encode(hmac(
'Test Using Larger Than Block-Size Key and Larger Than One Block-Size Data',
pg_catalog.decode('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex'),
'sha1'), 'hex');
