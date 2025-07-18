-- Test script for HDFS FDW in OpenTenBase
-- This script demonstrates how to use the HDFS foreign data wrapper

-- Enable debug logging
SET log_min_messages = DEBUG1;
SET client_min_messages = DEBUG1;

-- Create the extension (this should be done after building and installing)
CREATE EXTENSION IF NOT EXISTS hdfs_fdw;

-- Verify the extension is loaded
SELECT fdwname, fdwhandler, fdwvalidator FROM pg_foreign_data_wrapper WHERE fdwname = 'hdfs_fdw';

-- Create a server that connects to Hive/Spark
-- Adjust host, port, and dbname according to your Hive/Spark setup
CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw 
OPTIONS (
    host 'localhost', 
    port '10000', 
    dbname 'default',
    client_type '0',  -- 0 for HIVESERVER2, 1 for SPARKSERVER
    auth_type '1'     -- 1 for NOSASL, 2 for LDAP
);

-- Create user mapping
CREATE USER MAPPING FOR current_user SERVER hdfs_server 
OPTIONS (
    username 'hive', 
    password ''
);

-- Create a foreign table
-- This assumes you have a table 'test_table' in Hive with these columns
CREATE FOREIGN TABLE hdfs_test_table (
    id integer,
    name text,
    value double precision,
    created_date date
) SERVER hdfs_server 
OPTIONS (
    table_name 'test_table'
);

-- Test basic queries
SELECT * FROM hdfs_test_table;
SELECT count(*) FROM hdfs_test_table;
SELECT id, name FROM hdfs_test_table LIMIT 10;

-- Test with WHERE clause (will be executed locally for now)
SELECT * FROM hdfs_test_table WHERE id > 100;

-- Test with aggregation (will be executed locally for now)
SELECT name, COUNT(*) as cnt, AVG(value) as avg_value 
FROM hdfs_test_table 
GROUP BY name;

-- Test joining with local tables
-- First create a local table
CREATE TABLE local_users (
    id integer PRIMARY KEY,
    username text
);

INSERT INTO local_users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');

-- Then join
SELECT l.username, h.name, h.value
FROM local_users l
JOIN hdfs_test_table h ON l.id = h.id;

-- Clean up (optional)
DROP FOREIGN TABLE IF EXISTS hdfs_test_table;
DROP USER MAPPING IF EXISTS FOR current_user SERVER hdfs_server;
DROP SERVER IF EXISTS hdfs_server CASCADE;
DROP EXTENSION IF EXISTS hdfs_fdw CASCADE;