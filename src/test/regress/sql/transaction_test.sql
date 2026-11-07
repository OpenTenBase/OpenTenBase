SELECT node_name FROM pgxc_node WHERE node_type = 'D';
-- Please modify the following variables according to the actual cluster node names
-- Only two nodes were selected for the test.
\set node1 'dn001'
\set node2 'dn002'
-- a test case for distributed transactions
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id bigint, name text, num int) DISTRIBUTE BY SHARD(id);

-- insert
INSERT INTO test_table VALUES 
(1, 'A', 100),(2, 'B', 110),(3, 'C', 120),(4, 'D', 130),
(5, 'E', 140),(6, 'F', 150),(7, 'G', 160),(8, 'H', 170),
(9, 'I', 180),(10, 'J', 190);

-- select
SELECT * FROM test_table ORDER BY id;
-- select by shard(remind:dn001,dn002 are the name of datanodes)
EXECUTE DIRECT ON (:node1) 'SELECT * FROM test_table';
EXECUTE DIRECT ON (:node2) 'SELECT * FROM test_table';

-- update
UPDATE test_table SET num = 129 WHERE num < 130;
SELECT * FROM test_table ORDER BY id;

-- delete
DELETE FROM test_table WHERE num < 130;
SELECT * FROM test_table ORDER BY id;

-- distributed transaction
BEGIN;
-- Insert data at the node1,node2
INSERT INTO test_table VALUES (12, 'Node1', 1001);
INSERT INTO test_table VALUES (14, 'Node2', 1002);
UPDATE test_table SET num = num + 10 WHERE id = 12;
UPDATE test_table SET num = num + 20 WHERE id = 14;
COMMIT;
-- Query separately at two nodes to verify data consistency
EXECUTE DIRECT ON (:node1) 'SELECT * FROM test_table WHERE id IN (12,14) ORDER BY id';
EXECUTE DIRECT ON (:node2) 'SELECT * FROM test_table WHERE id IN (12,14) ORDER BY id';

-- Query separately at two nodes to verify data consistency
SELECT * FROM test_table WHERE id IN (12,14) ORDER BY id;

-- transaction control
BEGIN;
INSERT INTO test_table VALUES (1, 'A', 100);
SELECT * FROM test_table ORDER BY id;
-- update (Simulated transfer)
UPDATE test_table SET num = num - 200 WHERE id = 4;
-- check update
SELECT num FROM test_table WHERE id = 4 \gset
SELECT (num < 0) AS is_negative FROM test_table WHERE id = 4\gset

-- Simulate conditional branching. If num is less than 0, roll back; otherwise, continue the update
\if :is_negative
    ROLLBACK;
    BEGIN;
\else
    UPDATE test_table SET num = num + 200 WHERE id = 6;
\endif
SELECT * FROM test_table ORDER BY id;
COMMIT;

-- savepoint test
BEGIN;
INSERT INTO test_table VALUES (2, 'B', 200);
-- savepoint 
SAVEPOINT my_savepoint;
INSERT INTO test_table VALUES (3, 'C', 300);
SELECT * FROM test_table ORDER BY id;
ROLLBACK TO SAVEPOINT my_savepoint;
SELECT * FROM test_table ORDER BY id;
COMMIT;

--Unique Constraint Conflict Test
ALTER TABLE test_table ADD CONSTRAINT unique_id UNIQUE(id);
BEGIN;
--An error should be reported
INSERT INTO test_table VALUES (10, 'Duplicate', 999); 
COMMIT;
-- The verification data has not been inserted
SELECT * FROM test_table WHERE name = 'Duplicate';

-- Transaction isolation level test
-- 1. Dirty read test (needs to be executed in two sessions, this is just an example)
\echo '-- Test dirty reads (to be executed in two sessions) --'
\echo '-- Session 1 --'
\echo 'BEGIN;'
\echo 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
\echo 'SHOW transaction_isolation;'
-- Update the data but do not submit
\echo 'UPDATE test_table SET num = 0 WHERE id = 2;'
\echo '--Connection 1 updated id=2 num to 0, but did not commit'
\echo '-- Session 2 --'
\echo 'BEGIN;'
\echo 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
\echo 'SHOW transaction_isolation;'
\echo 'SELECT * FROM test_table WHERE id = 2; -- Unsubmitted changes should not be visible'
\echo '-- Session 1 --'
\echo 'ROLLBACK;'
\echo '-- Session 2 --'
\echo 'SELECT * FROM test_table WHERE id = 2;'
\echo 'COMMIT;'

-- 2. Non-repetitive reading test
\echo '-- Test non-repetitive reads (needs to be executed in two sessions) --'
\echo '-- Session 1 --'
\echo 'BEGIN;'
\echo 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED;'
\echo 'SHOW transaction_isolation;'
\echo 'SELECT * FROM test_table ORDER BY id;'

\echo '-- Session 2 --'
\echo '--modified data and committed'
\echo 'BEGIN;'
\echo 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED;'
\echo 'UPDATE test_table SET num = 0 WHERE id = 2;'
\echo 'COMMIT;'
\echo '-- Session 1 --'
\echo '--Query the same data again'
\echo 'SELECT * FROM test_table ORDER BY id;'
\echo 'COMMIT;'


-- 3. Phantom read test
\echo '-- Test phantom reads (needs to be executed in two sessions) --'
\echo '-- Session 1 --'
\echo 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;'
\echo 'SHOW transaction_isolation;'
\echo 'SELECT * FROM test_table ORDER BY id;'
\echo 'SELECT * FROM test_table WHERE id > 8 ORDER BY id;'
\echo '-- Session 2 --'
\echo '--inserted new data and committed'
\echo 'BEGIN;'
\echo 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;'
\echo 'INSERT INTO test_table VALUES (11, ''K'', 200);'
\echo 'COMMIT;'
\echo '-- Session 1 --'
\echo '--Query the same data Again'
\echo 'SELECT * FROM test_table WHERE id > 8 ORDER BY id;'
\echo 'COMMIT;'

-- remove test data
SELECT * FROM test_table ORDER BY id;
DROP TABLE test_table;