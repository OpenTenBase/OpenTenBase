-- Load extension and create test objects
CREATE EXTENSION pg_dist_stat_views;
CREATE TABLE regress_lock_test (id int, val text) DISTRIBUTE BY SHARD(id);

-- Basic View Syntax Check
SELECT * FROM dist_pg_stat_activity LIMIT 0;
SELECT * FROM dist_pg_stat_activity_cn LIMIT 0;
SELECT * FROM dist_pg_locks_raw LIMIT 0;
SELECT * FROM dist_pg_locks LIMIT 0;
SELECT * FROM dist_pg_locks_all_info LIMIT 0;
SELECT * FROM dist_pg_stat_query_summary LIMIT 0;
SELECT * FROM dist_pg_stat_query_details LIMIT 0;
SELECT * FROM dist_pg_lock_waits_summary LIMIT 0;
SELECT * FROM dist_pg_lock_wait_chains LIMIT 0;
SELECT * FROM dist_pg_deadlocks LIMIT 0;

-- Simple Held Lock Verification
BEGIN;
LOCK TABLE regress_lock_test IN ACCESS EXCLUSIVE MODE;
SELECT count(*) FROM dist_pg_locks WHERE lock_target = 'regress_lock_test' AND granted;
ROLLBACK;

DROP TABLE regress_lock_test;
DROP EXTENSION pg_dist_stat_views;