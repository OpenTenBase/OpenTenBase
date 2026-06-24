--
-- MERGE
--
CREATE USER regress_merge_privs;
CREATE USER regress_merge_no_privs;
DROP TABLE IF EXISTS target;
DROP TABLE IF EXISTS source;
CREATE TABLE target (tid integer, balance integer, id oid)
  WITH (autovacuum_enabled=off);
CREATE TABLE source (sid integer, delta integer, id oid) -- no index
  WITH (autovacuum_enabled=off);
INSERT INTO target VALUES (1, 10);
INSERT INTO target VALUES (2, 20);
INSERT INTO target VALUES (3, 30);
SELECT t.ctid is not null as matched, t.*, s.* FROM source s FULL OUTER JOIN target t ON s.sid = t.tid ORDER BY t.tid, s.sid;

ALTER TABLE target OWNER TO regress_merge_privs;
ALTER TABLE source OWNER TO regress_merge_privs;

CREATE TABLE target2 (tid integer, balance integer)
  WITH (autovacuum_enabled=off);
CREATE TABLE source2 (sid integer, delta integer)
  WITH (autovacuum_enabled=off);

ALTER TABLE target2 OWNER TO regress_merge_no_privs;
ALTER TABLE source2 OWNER TO regress_merge_no_privs;

GRANT INSERT ON target TO regress_merge_no_privs;

SET SESSION AUTHORIZATION regress_merge_privs;

EXPLAIN (COSTS OFF)
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	DELETE;

--
-- Errors
--
MERGE INTO target t RANDOMWORD
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0;
-- MATCHED/INSERT error
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	INSERT DEFAULT VALUES;
-- incorrectly specifying INTO target
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT INTO target DEFAULT VALUES;
-- Multiple VALUES clause
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT VALUES (1,1), (2,2);
-- SELECT query for INSERT
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT SELECT (1, 1);
-- NOT MATCHED/UPDATE
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	UPDATE SET balance = 0;
-- UPDATE tablename
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE target SET balance = 0;
-- source and target names the same
MERGE INTO target
USING target
ON tid = tid
WHEN MATCHED THEN DO NOTHING;
-- used in a CTE
WITH foo AS (
  MERGE INTO target USING source ON (true)
  WHEN MATCHED THEN DELETE
) SELECT * FROM foo;
-- used in COPY
COPY (
  MERGE INTO target USING source ON (true)
  WHEN MATCHED THEN DELETE
) TO stdout;

-- unsupported relation types
-- view
CREATE VIEW tv AS SELECT * FROM target;
MERGE INTO tv t
USING source s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT DEFAULT VALUES;
DROP VIEW tv;

-- materialized view
CREATE MATERIALIZED VIEW mv AS SELECT * FROM target;
MERGE INTO mv t
USING source s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT DEFAULT VALUES;
DROP MATERIALIZED VIEW mv;

-- permissions

MERGE INTO target
USING source2
ON target.tid = source2.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0;

GRANT INSERT ON target TO regress_merge_no_privs;
SET SESSION AUTHORIZATION regress_merge_no_privs;

MERGE INTO target
USING source2
ON target.tid = source2.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0;

GRANT UPDATE ON target2 TO regress_merge_privs;
SET SESSION AUTHORIZATION regress_merge_privs;

MERGE INTO target2
USING source
ON target2.tid = source.sid
WHEN MATCHED THEN
	DELETE;

MERGE INTO target2
USING source
ON target2.tid = source.sid
WHEN NOT MATCHED THEN
	INSERT DEFAULT VALUES;

-- check if the target can be accessed from source relation subquery; we should
-- not be able to do so
MERGE INTO target t
USING (SELECT * FROM source WHERE t.tid > sid) s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT DEFAULT VALUES;

--
-- initial tests
--
-- zero rows in source has no effect
MERGE INTO target
USING source
ON target.tid = source.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0;

MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	DELETE;
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT DEFAULT VALUES;
ROLLBACK;

-- insert some non-matching source rows to work from
INSERT INTO source VALUES (4, 40);
SELECT * FROM source ORDER BY sid;
SELECT * FROM target ORDER BY tid;

MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	DO NOTHING;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	DELETE;
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT DEFAULT VALUES;
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- index plans
INSERT INTO target SELECT generate_series(1000,2500), 0;
ALTER TABLE target ADD PRIMARY KEY (tid);
ANALYZE target;

EXPLAIN (COSTS OFF)
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0;
EXPLAIN (COSTS OFF)
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	DELETE;
EXPLAIN (COSTS OFF)
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT VALUES (4, NULL);
DELETE FROM target WHERE tid > 100;
ANALYZE target;

-- insert some matching source rows to work from
INSERT INTO source VALUES (2, 5);
INSERT INTO source VALUES (3, 20);
SELECT * FROM source ORDER BY sid;
SELECT * FROM target ORDER BY tid;

-- equivalent of an UPDATE join
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0;
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- equivalent of a DELETE join
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	DELETE;
SELECT * FROM target ORDER BY tid;
ROLLBACK;

BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	DO NOTHING;
SELECT * FROM target ORDER BY tid;
ROLLBACK;

BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT VALUES (4, NULL);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- duplicate source row causes multiple target row update ERROR
INSERT INTO source VALUES (2, 5);
SELECT * FROM source ORDER BY sid;
SELECT * FROM target ORDER BY tid;
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0;
ROLLBACK;

BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	DELETE;
ROLLBACK;

-- remove duplicate MATCHED data from source data
DELETE FROM source WHERE sid = 2;
INSERT INTO source VALUES (2, 5);
SELECT * FROM source ORDER BY sid;
SELECT * FROM target ORDER BY tid;

-- duplicate source row on INSERT should fail because of target_pkey
INSERT INTO source VALUES (4, 40);
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
  INSERT VALUES (4, NULL);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- remove duplicate NOT MATCHED data from source data
DELETE FROM source WHERE sid = 4;
INSERT INTO source VALUES (4, 40);
SELECT * FROM source ORDER BY sid;
SELECT * FROM target ORDER BY tid;

-- remove constraints
alter table target drop CONSTRAINT target_pkey;
alter table target alter column tid drop not null;

-- multiple actions
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT VALUES (4, 4)
WHEN MATCHED THEN
	UPDATE SET balance = 0;
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- should be equivalent
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = 0
WHEN NOT MATCHED THEN
	INSERT VALUES (4, 4);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- column references
-- do a simple equivalent of an UPDATE join
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = t.balance + s.delta;
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- do a simple equivalent of an INSERT SELECT
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT VALUES (s.sid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- and again with duplicate source rows
INSERT INTO source VALUES (5, 50);
INSERT INTO source VALUES (5, 50);

-- do a simple equivalent of an INSERT SELECT
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
  INSERT VALUES (s.sid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- removing duplicate source rows
DELETE FROM source WHERE sid = 5;

-- and again with explicitly identified column list
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT (tid, balance) VALUES (s.sid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- and again with a subtle error: referring to non-existent target row for NOT MATCHED
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT (tid, balance) VALUES (t.tid, s.delta);

-- and again with a constant ON clause
BEGIN;
MERGE INTO target t
USING source AS s
ON (SELECT true)
WHEN NOT MATCHED THEN
	INSERT (tid, balance) VALUES (t.tid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- now the classic UPSERT
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = t.balance + s.delta
WHEN NOT MATCHED THEN
	INSERT VALUES (s.sid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- unreachable WHEN clause should ERROR
BEGIN;
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED THEN /* Terminal WHEN clause for MATCHED */
	DELETE
WHEN MATCHED THEN
	UPDATE SET balance = t.balance - s.delta;
ROLLBACK;

-- conditional WHEN clause
CREATE TABLE wq_target (tid integer not null, balance integer DEFAULT -1)
  WITH (autovacuum_enabled=off);
CREATE TABLE wq_source (balance integer, sid integer)
  WITH (autovacuum_enabled=off);

INSERT INTO wq_source (sid, balance) VALUES (1, 100);

BEGIN;
-- try a simple INSERT with default values first
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT (tid) VALUES (s.sid);
SELECT * FROM wq_target;
ROLLBACK;

-- this time with a FALSE condition
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN NOT MATCHED AND FALSE THEN
	INSERT (tid) VALUES (s.sid);
SELECT * FROM wq_target;

-- this time with an actual condition which returns false
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN NOT MATCHED AND s.balance <> 100 THEN
	INSERT (tid) VALUES (s.sid);
SELECT * FROM wq_target;

BEGIN;
-- and now with a condition which returns true
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN NOT MATCHED AND s.balance = 100 THEN
	INSERT (tid) VALUES (s.sid);
SELECT * FROM wq_target;
ROLLBACK;

-- conditions in the NOT MATCHED clause can only refer to source columns
BEGIN;
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN NOT MATCHED AND t.balance = 100 THEN
	INSERT (tid) VALUES (s.sid);
SELECT * FROM wq_target;
ROLLBACK;

MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN NOT MATCHED AND s.balance = 100 THEN
	INSERT (tid) VALUES (s.sid);
SELECT * FROM wq_target;

-- conditions in MATCHED clause can refer to both source and target
SELECT * FROM wq_source;
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN MATCHED AND s.balance = 100 THEN
	UPDATE SET balance = t.balance + s.balance;
SELECT * FROM wq_target;

MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN MATCHED AND t.balance = 100 THEN
	UPDATE SET balance = t.balance + s.balance;
SELECT * FROM wq_target;

-- check if AND works
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN MATCHED AND t.balance = 99 AND s.balance > 100 THEN
	UPDATE SET balance = t.balance + s.balance;
SELECT * FROM wq_target;

MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN MATCHED AND t.balance = 99 AND s.balance = 100 THEN
	UPDATE SET balance = t.balance + s.balance;
SELECT * FROM wq_target;

-- check if OR works
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN MATCHED AND t.balance = 99 OR s.balance > 100 THEN
	UPDATE SET balance = t.balance + s.balance;
SELECT * FROM wq_target;

MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN MATCHED AND t.balance = 199 OR s.balance > 100 THEN
	UPDATE SET balance = t.balance + s.balance;
SELECT * FROM wq_target;

-- check source-side whole-row references
BEGIN;
MERGE INTO wq_target t
USING wq_source s ON (t.tid = s.sid)
WHEN matched and t = s or t.tid = s.sid THEN
	UPDATE SET balance = t.balance + s.balance;
SELECT * FROM wq_target;
ROLLBACK;

-- check if subqueries work in the conditions?
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN MATCHED AND t.balance > (SELECT max(balance) FROM target) THEN
	UPDATE SET balance = t.balance + s.balance;

-- check if we can access system columns in the conditions
MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN MATCHED AND t.xmin = t.xmax THEN
	UPDATE SET balance = t.balance + s.balance;

MERGE INTO wq_target t
USING wq_source s ON t.tid = s.sid
WHEN MATCHED AND t.tableoid >= 0 THEN
	UPDATE SET balance = t.balance + s.balance;
SELECT * FROM wq_target;

DROP TABLE wq_target, wq_source;

-- test triggers
create or replace function merge_trigfunc () returns trigger
language plpgsql as
$$
DECLARE
	line text;
BEGIN
	SELECT INTO line format('%s %s %s trigger%s',
		TG_WHEN, TG_OP, TG_LEVEL, CASE
		WHEN TG_OP = 'INSERT' AND TG_LEVEL = 'ROW'
			THEN format(' row: %s', NEW)
		WHEN TG_OP = 'UPDATE' AND TG_LEVEL = 'ROW'
			THEN format(' row: %s -> %s', OLD, NEW)
		WHEN TG_OP = 'DELETE' AND TG_LEVEL = 'ROW'
			THEN format(' row: %s', OLD)
		END);

	RAISE NOTICE '%', line;
	IF (TG_WHEN = 'BEFORE' AND TG_LEVEL = 'ROW') THEN
		IF (TG_OP = 'DELETE') THEN
			RETURN OLD;
		ELSE
			RETURN NEW;
		END IF;
	ELSE
		RETURN NULL;
	END IF;
END;
$$;
CREATE TRIGGER merge_bsi BEFORE INSERT ON target FOR EACH STATEMENT EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_bsu BEFORE UPDATE ON target FOR EACH STATEMENT EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_bsd BEFORE DELETE ON target FOR EACH STATEMENT EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_asi AFTER INSERT ON target FOR EACH STATEMENT EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_asu AFTER UPDATE ON target FOR EACH STATEMENT EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_asd AFTER DELETE ON target FOR EACH STATEMENT EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_bri BEFORE INSERT ON target FOR EACH ROW EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_bru BEFORE UPDATE ON target FOR EACH ROW EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_brd BEFORE DELETE ON target FOR EACH ROW EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_ari AFTER INSERT ON target FOR EACH ROW EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_aru AFTER UPDATE ON target FOR EACH ROW EXECUTE PROCEDURE merge_trigfunc ();
CREATE TRIGGER merge_ard AFTER DELETE ON target FOR EACH ROW EXECUTE PROCEDURE merge_trigfunc ();

-- now the classic UPSERT, with a DELETE
BEGIN;
UPDATE target SET balance = 0 WHERE tid = 3;
--EXPLAIN (ANALYZE ON, COSTS OFF, SUMMARY OFF, TIMING OFF)
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED AND t.balance > s.delta THEN
	UPDATE SET balance = t.balance - s.delta
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
	INSERT VALUES (s.sid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- Test behavior of triggers that turn UPDATE/DELETE into no-ops
create or replace function skip_merge_op() returns trigger
language plpgsql as
$$
BEGIN
	RETURN NULL;
END;
$$;

SELECT * FROM target full outer join source on (sid = tid) order by 1,2;
create trigger merge_skip BEFORE INSERT OR UPDATE or DELETE
  ON target FOR EACH ROW EXECUTE FUNCTION skip_merge_op();
DO $$
DECLARE
  result integer;
BEGIN
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED AND s.sid = 3 THEN UPDATE SET balance = t.balance + s.delta
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (sid, delta);
IF FOUND THEN
  RAISE NOTICE 'Found';
ELSE
  RAISE NOTICE 'Not found';
END IF;
GET DIAGNOSTICS result := ROW_COUNT;
RAISE NOTICE 'ROW_COUNT = %', result;
END;
$$;
SELECT * FROM target FULL OUTER JOIN source ON (sid = tid) order by 1,2;
DROP TRIGGER merge_skip ON target;
DROP FUNCTION skip_merge_op();

-- test from PL/pgSQL
-- make sure MERGE INTO isn't interpreted to mean returning variables like SELECT INTO
BEGIN;
DO LANGUAGE plpgsql $$
BEGIN
MERGE INTO target t
USING source AS s
ON t.tid = s.sid
WHEN MATCHED AND t.balance > s.delta THEN
	UPDATE SET balance = t.balance - s.delta;
END;
$$;
ROLLBACK;

--source constants
BEGIN;
MERGE INTO target t
USING (SELECT 9 AS sid, 57 AS delta) AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT (tid, balance) VALUES (s.sid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

--source query
BEGIN;
MERGE INTO target t
USING (SELECT sid, delta FROM source WHERE delta > 0) AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT (tid, balance) VALUES (s.sid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

BEGIN;
MERGE INTO target t
USING (SELECT sid, delta as newname FROM source WHERE delta > 0) AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT (tid, balance) VALUES (s.sid, s.newname);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

--self-merge
BEGIN;
MERGE INTO target t1
USING target t2
ON t1.tid = t2.tid
WHEN MATCHED THEN
	UPDATE SET balance = t1.balance + t2.balance
WHEN NOT MATCHED THEN
	INSERT VALUES (t2.tid, t2.balance);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

BEGIN;
MERGE INTO target t
USING (SELECT tid as sid, balance as delta FROM target WHERE balance > 0) AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT (tid, balance) VALUES (s.sid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

BEGIN;
MERGE INTO target t
USING
(SELECT sid, max(delta) AS delta
 FROM source
 GROUP BY sid
 HAVING count(*) = 1
 ORDER BY sid ASC) AS s
ON t.tid = s.sid
WHEN NOT MATCHED THEN
	INSERT (tid, balance) VALUES (s.sid, s.delta);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- plpgsql parameters and results
BEGIN;
CREATE FUNCTION merge_func (p_id integer, p_bal integer)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
 result integer;
BEGIN
MERGE INTO target t
USING (SELECT p_id AS sid) AS s
ON t.tid = s.sid
WHEN MATCHED THEN
	UPDATE SET balance = t.balance - p_bal;
IF FOUND THEN
	GET DIAGNOSTICS result := ROW_COUNT;
END IF;
RETURN result;
END;
$$;
SELECT merge_func(3, 4);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- PREPARE
BEGIN;
prepare foom as merge into target t using (select 1 as sid) s on (t.tid = s.sid) when matched then update set balance = 1;
execute foom;
SELECT * FROM target ORDER BY tid;
ROLLBACK;

BEGIN;
PREPARE foom2 (integer, integer) AS
MERGE INTO target t
USING (SELECT 1) s
ON t.tid = $1
WHEN MATCHED THEN
UPDATE SET balance = $2;
--EXPLAIN (ANALYZE ON, COSTS OFF, SUMMARY OFF, TIMING OFF)
execute foom2 (1, 1);
SELECT * FROM target ORDER BY tid;
ROLLBACK;

-- subqueries in source relation

CREATE TABLE sq_target (tid integer NOT NULL, balance integer)
  WITH (autovacuum_enabled=off);
CREATE TABLE sq_source (delta integer, sid integer, balance integer DEFAULT 0)
  WITH (autovacuum_enabled=off);

INSERT INTO sq_target(tid, balance) VALUES (1,100), (2,200), (3,300);
INSERT INTO sq_source(sid, delta) VALUES (1,10), (2,20), (4,40);

BEGIN;
MERGE INTO sq_target t
USING (SELECT * FROM sq_source) s
ON tid = sid
WHEN MATCHED AND t.balance > delta THEN
	UPDATE SET balance = t.balance + delta;
SELECT * FROM sq_target order by 1;
ROLLBACK;

-- try a view
CREATE VIEW v AS SELECT * FROM sq_source WHERE sid < 2;

BEGIN;
MERGE INTO sq_target
USING v
ON tid = sid
WHEN MATCHED THEN
    UPDATE SET balance = v.balance + delta;
SELECT * FROM sq_target order by 1;
ROLLBACK;

-- ambiguous reference to a column
BEGIN;
MERGE INTO sq_target
USING v
ON tid = sid
WHEN MATCHED AND tid > 2 THEN
    UPDATE SET balance = balance + delta
WHEN NOT MATCHED THEN
	INSERT (balance, tid) VALUES (balance + delta, sid)
WHEN MATCHED AND tid < 2 THEN
	DELETE;
ROLLBACK;

BEGIN;
INSERT INTO sq_source (sid, balance, delta) VALUES (-1, -1, -10);
MERGE INTO sq_target t
USING v
ON tid = sid
WHEN MATCHED AND tid > 2 THEN
    UPDATE SET balance = t.balance + delta
WHEN NOT MATCHED THEN
	INSERT (balance, tid) VALUES (balance + delta, sid)
WHEN MATCHED AND tid < 2 THEN
	DELETE;
SELECT * FROM sq_target order by 1;
ROLLBACK;

-- CTEs
BEGIN;
INSERT INTO sq_source (sid, balance, delta) VALUES (-1, -1, -10);
WITH targq AS (
	SELECT * FROM v
)
MERGE INTO sq_target t
USING v
ON tid = sid
WHEN MATCHED AND tid > 2 THEN
    UPDATE SET balance = t.balance + delta
WHEN NOT MATCHED THEN
	INSERT (balance, tid) VALUES (balance + delta, sid)
WHEN MATCHED AND tid < 2 THEN
	DELETE;
ROLLBACK;

-- RETURNING
BEGIN;
INSERT INTO sq_source (sid, balance, delta) VALUES (-1, -1, -10);
MERGE INTO sq_target t
USING v
ON tid = sid
WHEN MATCHED AND tid > 2 THEN
    UPDATE SET balance = t.balance + delta
WHEN NOT MATCHED THEN
	INSERT (balance, tid) VALUES (balance + delta, sid)
WHEN MATCHED AND tid < 2 THEN
	DELETE
RETURNING *;
ROLLBACK;

-- EXPLAIN
CREATE TABLE ex_mtarget (a int, b int)
  WITH (autovacuum_enabled=off);
CREATE TABLE ex_msource (a int, b int)
  WITH (autovacuum_enabled=off);
INSERT INTO ex_mtarget SELECT i, i*10 FROM generate_series(1,100,2) i;
INSERT INTO ex_msource SELECT i, i*10 FROM generate_series(1,100,1) i;

CREATE FUNCTION explain_merge(query text) RETURNS SETOF text
LANGUAGE plpgsql AS
$$
DECLARE ln text;
BEGIN
    FOR ln IN
        EXECUTE 'explain (analyze, timing off, summary off, costs off) ' ||
		  query
    LOOP
        ln := regexp_replace(ln, '(Memory( Usage)?|Buckets|Batches): \S*',  '\1: xxx', 'g');
        RETURN NEXT ln;
    END LOOP;
END;
$$;

-- only updates
SELECT explain_merge('
MERGE INTO ex_mtarget t USING ex_msource s ON t.a = s.a
WHEN MATCHED THEN
	UPDATE SET b = t.b + 1');

-- only updates to selected tuples
SELECT explain_merge('
MERGE INTO ex_mtarget t USING ex_msource s ON t.a = s.a
WHEN MATCHED AND t.a < 10 THEN
	UPDATE SET b = t.b + 1');

-- updates + deletes
SELECT explain_merge('
MERGE INTO ex_mtarget t USING ex_msource s ON t.a = s.a
WHEN MATCHED AND t.a < 10 THEN
	UPDATE SET b = t.b + 1
WHEN MATCHED AND t.a >= 10 AND t.a <= 20 THEN
	DELETE');

-- only inserts
SELECT explain_merge('
MERGE INTO ex_mtarget t USING ex_msource s ON t.a = s.a
WHEN NOT MATCHED AND s.a < 10 THEN
	INSERT VALUES (a, b)');

-- all three
SELECT explain_merge('
MERGE INTO ex_mtarget t USING ex_msource s ON t.a = s.a
WHEN MATCHED AND t.a < 10 THEN
	UPDATE SET b = t.b + 1
WHEN MATCHED AND t.a >= 30 AND t.a <= 40 THEN
	DELETE
WHEN NOT MATCHED AND s.a < 20 THEN
	INSERT VALUES (a, b)');

-- nothing
SELECT explain_merge('
MERGE INTO ex_mtarget t USING ex_msource s ON t.a = s.a AND t.a < -1000
WHEN MATCHED AND t.a < 10 THEN
	DO NOTHING');

DROP TABLE ex_msource, ex_mtarget;
DROP FUNCTION explain_merge(text);

-- Subqueries
BEGIN;
MERGE INTO sq_target t
USING v
ON tid = sid
WHEN MATCHED THEN
    UPDATE SET balance = (SELECT count(*) FROM sq_target);
SELECT * FROM sq_target WHERE tid = 1;
ROLLBACK;

BEGIN;
MERGE INTO sq_target t
USING v
ON tid = sid
WHEN MATCHED AND (SELECT count(*) > 0 FROM sq_target) THEN
    UPDATE SET balance = 42;
SELECT * FROM sq_target WHERE tid = 1;
ROLLBACK;

BEGIN;
MERGE INTO sq_target t
USING v
ON tid = sid AND (SELECT count(*) > 0 FROM sq_target)
WHEN MATCHED THEN
    UPDATE SET balance = 42;
SELECT * FROM sq_target WHERE tid = 1;
ROLLBACK;

DROP TABLE sq_target, sq_source CASCADE;

CREATE TABLE pa_target (tid integer, balance float, val text)
	PARTITION BY LIST (tid);

CREATE TABLE part1 PARTITION OF pa_target FOR VALUES IN (1,4)
  WITH (autovacuum_enabled=off);
CREATE TABLE part2 PARTITION OF pa_target FOR VALUES IN (2,5,6)
  WITH (autovacuum_enabled=off);
CREATE TABLE part3 PARTITION OF pa_target FOR VALUES IN (3,8,9)
  WITH (autovacuum_enabled=off);
CREATE TABLE part4 PARTITION OF pa_target DEFAULT
  WITH (autovacuum_enabled=off);

CREATE TABLE pa_source (sid integer, delta float);
-- insert many rows to the source table
INSERT INTO pa_source SELECT id, id * 10  FROM generate_series(1,14) AS id;
-- insert a few rows in the target table (odd numbered tid)
INSERT INTO pa_target SELECT id, id * 100, 'initial' FROM generate_series(1,14,2) AS id;

-- try simple MERGE
BEGIN;
MERGE INTO pa_target t
  USING pa_source s
  ON t.tid = s.sid
  WHEN MATCHED THEN
    UPDATE SET balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');
SELECT * FROM pa_target ORDER BY tid, balance;
ROLLBACK;

-- same with a constant qual
BEGIN;
MERGE INTO pa_target t
  USING pa_source s
  ON t.tid = s.sid AND tid = 1
  WHEN MATCHED THEN
    UPDATE SET balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');
SELECT * FROM pa_target ORDER BY tid, balance;
ROLLBACK;

-- try updating the partition key column
BEGIN;
CREATE FUNCTION merge_func() RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  result integer;
BEGIN
MERGE INTO pa_target t
  USING pa_source s
  ON t.tid = s.sid
  WHEN MATCHED THEN
    UPDATE SET tid = tid + 1, balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');
IF FOUND THEN
  GET DIAGNOSTICS result := ROW_COUNT;
END IF;
RETURN result;
END;
$$;
SELECT merge_func();
SELECT * FROM pa_target ORDER BY tid, balance;
ROLLBACK;

DROP TABLE pa_target CASCADE;

-- The target table is partitioned in the same way, but this time by attaching
-- partitions which have columns in different order, dropped columns etc.
CREATE TABLE pa_target (tid integer, balance float, val text)
	PARTITION BY LIST (tid);

CREATE TABLE part1 (tid integer, balance float, val text)
  WITH (autovacuum_enabled=off);
CREATE TABLE part2 (tid integer, balance float, val text)
  WITH (autovacuum_enabled=off);
CREATE TABLE part3 (tid integer, balance float, val text)
  WITH (autovacuum_enabled=off);
CREATE TABLE part4 (tid integer, balance float, val text) 
  WITH (autovacuum_enabled=off);

ALTER TABLE pa_target ATTACH PARTITION part1 FOR VALUES IN (1,4);
ALTER TABLE pa_target ATTACH PARTITION part2 FOR VALUES IN (2,5,6);
ALTER TABLE pa_target ATTACH PARTITION part3 FOR VALUES IN (3,8,9);
ALTER TABLE pa_target ATTACH PARTITION part4 DEFAULT;

-- insert a few rows in the target table (odd numbered tid)
INSERT INTO pa_target SELECT id, id * 100, 'initial' FROM generate_series(1,14,2) AS id;

-- try simple MERGE
BEGIN;
MERGE INTO pa_target t
  USING pa_source s
  ON t.tid = s.sid
  WHEN MATCHED THEN
    UPDATE SET balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');
SELECT * FROM pa_target ORDER BY tid, balance;
ROLLBACK;

-- same with a constant qual
BEGIN;
MERGE INTO pa_target t
  USING pa_source s
  ON t.tid = s.sid AND tid IN (1, 5)
  WHEN MATCHED AND tid % 5 = 0 THEN DELETE
  WHEN MATCHED THEN
    UPDATE SET balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');
SELECT * FROM pa_target ORDER BY tid, balance;
ROLLBACK;

-- try updating the partition key column
BEGIN;
MERGE INTO pa_target t
  USING pa_source s
  ON t.tid = s.sid
  WHEN MATCHED THEN
    UPDATE SET tid = tid + 1, balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');
SELECT * FROM pa_target ORDER BY tid, balance;
ROLLBACK;

-- test RLS enforcement
BEGIN;
ALTER TABLE pa_target ENABLE ROW LEVEL SECURITY;
ALTER TABLE pa_target FORCE ROW LEVEL SECURITY;
CREATE POLICY pa_target_pol ON pa_target USING (tid != 0);
MERGE INTO pa_target t
  USING pa_source s
  ON t.tid = s.sid AND t.tid IN (1,2,3,4)
  WHEN MATCHED THEN
    UPDATE SET tid = tid - 1;
ROLLBACK;

DROP TABLE pa_source;
DROP TABLE pa_target CASCADE;

-- Sub-partitioning
CREATE TABLE pa_target (logts timestamp, tid integer, balance float, val text)
	PARTITION BY RANGE (logts);

CREATE TABLE part_m01 PARTITION OF pa_target
	FOR VALUES FROM ('2017-01-01') TO ('2017-02-01')
	PARTITION BY LIST (tid);
CREATE TABLE part_m01_odd PARTITION OF part_m01
	FOR VALUES IN (1,3,5,7,9) WITH (autovacuum_enabled=off);
CREATE TABLE part_m01_even PARTITION OF part_m01
	FOR VALUES IN (2,4,6,8) WITH (autovacuum_enabled=off);
CREATE TABLE part_m02 PARTITION OF pa_target
	FOR VALUES FROM ('2017-02-01') TO ('2017-03-01')
	PARTITION BY LIST (tid);
CREATE TABLE part_m02_odd PARTITION OF part_m02
	FOR VALUES IN (1,3,5,7,9) WITH (autovacuum_enabled=off);
CREATE TABLE part_m02_even PARTITION OF part_m02
	FOR VALUES IN (2,4,6,8) WITH (autovacuum_enabled=off);

CREATE TABLE pa_source (sid integer, delta float)
  WITH (autovacuum_enabled=off);
-- insert many rows to the source table
INSERT INTO pa_source SELECT id, id * 10  FROM generate_series(1,14) AS id;
-- insert a few rows in the target table (odd numbered tid)
INSERT INTO pa_target SELECT '2017-01-31', id, id * 100, 'initial' FROM generate_series(1,9,3) AS id;
INSERT INTO pa_target SELECT '2017-02-28', id, id * 100, 'initial' FROM generate_series(2,9,3) AS id;

-- try simple MERGE
BEGIN;
MERGE INTO pa_target t
  USING (SELECT '2017-01-15' AS slogts, * FROM pa_source WHERE sid < 10) s
  ON t.tid = s.sid
  WHEN MATCHED THEN
    UPDATE SET balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (slogts::timestamp, sid, delta, 'inserted by merge');
SELECT * FROM pa_target ORDER BY tid, balance;
ROLLBACK;

DROP TABLE pa_source;
DROP TABLE pa_target CASCADE;

-- Partitioned table with primary key

CREATE TABLE pa_target (tid integer PRIMARY KEY) PARTITION BY LIST (tid);
CREATE TABLE pa_targetp PARTITION OF pa_target DEFAULT;
CREATE TABLE pa_source (sid integer);

INSERT INTO pa_source VALUES (1), (2);

EXPLAIN (VERBOSE, COSTS OFF)
MERGE INTO pa_target t USING pa_source s ON t.tid = s.sid
  WHEN NOT MATCHED THEN INSERT VALUES (s.sid);

MERGE INTO pa_target t USING pa_source s ON t.tid = s.sid
  WHEN NOT MATCHED THEN INSERT VALUES (s.sid);

select * from pa_target order by 1;

-- Partition-less partitioned table
-- (the bug we are checking for appeared only if table had partitions before)

DROP TABLE pa_targetp;

EXPLAIN (VERBOSE, COSTS OFF, NODES off)
MERGE INTO pa_target t USING pa_source s ON t.tid = s.sid
  WHEN NOT MATCHED THEN INSERT VALUES (s.sid);

MERGE INTO pa_target t USING pa_source s ON t.tid = s.sid
  WHEN NOT MATCHED THEN INSERT VALUES (s.sid);

DROP TABLE pa_source;
DROP TABLE pa_target CASCADE;

-- some complex joins on the source side

CREATE TABLE cj_target (tid integer, balance float, val text)
  WITH (autovacuum_enabled=off);
CREATE TABLE cj_source1 (sid1 integer, scat integer, delta integer)
  WITH (autovacuum_enabled=off);
CREATE TABLE cj_source2 (sid2 integer, sval text)
  WITH (autovacuum_enabled=off);
INSERT INTO cj_source1 VALUES (1, 10, 100);
INSERT INTO cj_source1 VALUES (1, 20, 200);
INSERT INTO cj_source1 VALUES (2, 20, 300);
INSERT INTO cj_source1 VALUES (3, 10, 400);
INSERT INTO cj_source2 VALUES (1, 'initial source2');
INSERT INTO cj_source2 VALUES (2, 'initial source2');
INSERT INTO cj_source2 VALUES (3, 'initial source2');

-- source relation is an unaliased join
MERGE INTO cj_target t
USING cj_source1 s1
	INNER JOIN cj_source2 s2 ON sid1 = sid2
ON t.tid = sid1
WHEN NOT MATCHED THEN
	INSERT VALUES (sid1, delta, sval);

-- try accessing columns from either side of the source join
MERGE INTO cj_target t
USING cj_source2 s2
	INNER JOIN cj_source1 s1 ON sid1 = sid2 AND scat = 20
ON t.tid = sid1
WHEN NOT MATCHED THEN
	INSERT VALUES (sid2, delta, sval)
WHEN MATCHED THEN
	DELETE;

-- some simple expressions in INSERT targetlist
MERGE INTO cj_target t
USING cj_source2 s2
	INNER JOIN cj_source1 s1 ON sid1 = sid2
ON t.tid = sid1
WHEN NOT MATCHED THEN
	INSERT VALUES (sid2, delta + scat, sval)
WHEN MATCHED THEN
	UPDATE SET val = val || ' updated by merge';

MERGE INTO cj_target t
USING cj_source2 s2
	INNER JOIN cj_source1 s1 ON sid1 = sid2 AND scat = 20
ON t.tid = sid1
WHEN MATCHED THEN
	UPDATE SET val = val || ' ' || delta::text;

SELECT * FROM cj_target order by tid,balance;

ALTER TABLE cj_source1 RENAME COLUMN sid1 TO sid;
ALTER TABLE cj_source2 RENAME COLUMN sid2 TO sid;

TRUNCATE cj_target;

MERGE INTO cj_target t
USING cj_source1 s1
	INNER JOIN cj_source2 s2 ON s1.sid = s2.sid
ON t.tid = s1.sid
WHEN NOT MATCHED THEN
	INSERT VALUES (s2.sid, delta, sval);

DROP TABLE cj_source2, cj_source1, cj_target;

-- Function scans
CREATE TABLE fs_target (a int, b int, c text)
  WITH (autovacuum_enabled=off);
MERGE INTO fs_target t
USING generate_series(1,100,1) AS id
ON t.a = id
WHEN MATCHED THEN
	UPDATE SET b = b + id
WHEN NOT MATCHED THEN
	INSERT VALUES (id, -1);

MERGE INTO fs_target t
USING generate_series(1,100,2) AS id
ON t.a = id
WHEN MATCHED THEN
	UPDATE SET b = b + id, c = 'updated '|| id.*::text
WHEN NOT MATCHED THEN
	INSERT VALUES (id, -1, 'inserted ' || id.*::text);

SELECT count(*) FROM fs_target;
DROP TABLE fs_target;

-- SERIALIZABLE test
-- handled in isolation tests

-- Inheritance-based partitioning
CREATE TABLE measurement (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
) WITH (autovacuum_enabled=off);
CREATE TABLE measurement_y2006m02 (
    CHECK ( logdate >= DATE '2006-02-01' AND logdate < DATE '2006-03-01' )
) INHERITS (measurement) WITH (autovacuum_enabled=off);
CREATE TABLE measurement_y2006m03 (
    CHECK ( logdate >= DATE '2006-03-01' AND logdate < DATE '2006-04-01' )
) INHERITS (measurement) WITH (autovacuum_enabled=off);
CREATE TABLE measurement_y2007m01 (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
    CHECK ( logdate >= DATE '2007-01-01' AND logdate < DATE '2007-02-01')
) WITH (autovacuum_enabled=off);
ALTER TABLE measurement_y2007m01 INHERIT measurement;
INSERT INTO measurement VALUES (0, '2005-07-21', 5, 15);

set enable_datanode_row_triggers to on;
CREATE OR REPLACE FUNCTION measurement_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.logdate >= DATE '2006-02-01' AND
         NEW.logdate < DATE '2006-03-01' ) THEN
        INSERT INTO measurement_y2006m02 VALUES (NEW.*);
    ELSIF ( NEW.logdate >= DATE '2006-03-01' AND
            NEW.logdate < DATE '2006-04-01' ) THEN
        INSERT INTO measurement_y2006m03 VALUES (NEW.*);
    ELSIF ( NEW.logdate >= DATE '2007-01-01' AND
            NEW.logdate < DATE '2007-02-01' ) THEN
        INSERT INTO measurement_y2007m01 (city_id, logdate, peaktemp, unitsales)
            VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range.  Fix the measurement_insert_trigger() function!';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql ;
CREATE TRIGGER insert_measurement_trigger
    BEFORE INSERT ON measurement
    FOR EACH ROW EXECUTE PROCEDURE measurement_insert_trigger();
INSERT INTO measurement VALUES (1, '2006-02-10', 35, 10);
INSERT INTO measurement VALUES (1, '2006-02-16', 45, 20);
INSERT INTO measurement VALUES (1, '2006-03-17', 25, 10);
INSERT INTO measurement VALUES (1, '2006-03-27', 15, 40);
INSERT INTO measurement VALUES (1, '2007-01-15', 10, 10);
INSERT INTO measurement VALUES (1, '2007-01-17', 10, 10);

SELECT tableoid::regclass, * FROM measurement ORDER BY city_id, logdate;

CREATE TABLE new_measurement (LIKE measurement) WITH (autovacuum_enabled=off);
INSERT INTO new_measurement VALUES (0, '2005-07-21', 25, 20);
INSERT INTO new_measurement VALUES (1, '2006-03-01', 20, 10);
INSERT INTO new_measurement VALUES (1, '2006-02-16', 50, 10);
INSERT INTO new_measurement VALUES (2, '2006-02-10', 20, 20);
INSERT INTO new_measurement VALUES (1, '2006-03-27', NULL, NULL);
INSERT INTO new_measurement VALUES (1, '2007-01-17', NULL, NULL);
INSERT INTO new_measurement VALUES (1, '2007-01-15', 5, NULL);
INSERT INTO new_measurement VALUES (1, '2007-01-16', 10, 10);

DROP TRIGGER insert_measurement_trigger ON measurement;

BEGIN;
MERGE INTO ONLY measurement m
 USING new_measurement nm ON
      (m.city_id = nm.city_id and m.logdate=nm.logdate)
WHEN MATCHED AND nm.peaktemp IS NULL THEN DELETE
WHEN MATCHED THEN UPDATE
     SET peaktemp = greatest(m.peaktemp, nm.peaktemp),
        unitsales = m.unitsales + coalesce(nm.unitsales, 0)
WHEN NOT MATCHED THEN INSERT
     (city_id, logdate, peaktemp, unitsales)
   VALUES (city_id, logdate, peaktemp, unitsales);

SELECT tableoid::regclass, * FROM measurement ORDER BY city_id, logdate, peaktemp;
ROLLBACK;

MERGE into measurement m
 USING new_measurement nm ON
      (m.city_id = nm.city_id and m.logdate=nm.logdate)
WHEN MATCHED AND nm.peaktemp IS NULL THEN DELETE
WHEN MATCHED THEN UPDATE
     SET peaktemp = greatest(m.peaktemp, nm.peaktemp),
        unitsales = m.unitsales + coalesce(nm.unitsales, 0)
WHEN NOT MATCHED THEN INSERT
     (city_id, logdate, peaktemp, unitsales)
   VALUES (city_id, logdate, peaktemp, unitsales);

SELECT tableoid::regclass, * FROM measurement ORDER BY city_id, logdate;

BEGIN;
MERGE INTO new_measurement nm
 USING ONLY measurement m ON
      (nm.city_id = m.city_id and nm.logdate=m.logdate)
WHEN MATCHED THEN DELETE;

SELECT * FROM new_measurement ORDER BY city_id, logdate;
ROLLBACK;

MERGE INTO new_measurement nm
 USING measurement m ON
      (nm.city_id = m.city_id and nm.logdate=m.logdate)
WHEN MATCHED THEN DELETE;

SELECT * FROM new_measurement ORDER BY city_id, logdate;

DROP TABLE measurement, new_measurement CASCADE;
DROP FUNCTION measurement_insert_trigger();

-- prepare

RESET SESSION AUTHORIZATION;
DROP TABLE target, target2;
DROP TABLE source, source2;
DROP FUNCTION merge_trigfunc();
DROP USER regress_merge_privs;
DROP USER regress_merge_no_privs;

-- MERGE INTO (opentenbase_ora_enable_compitable on)
\c regression_ora
set enable_distribute_update to on;
--test update in merge into
create table test_d (a int, b int);
create table test_s (a int, b int);
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b;
select * from test_d order by a;
truncate table test_s;
insert into test_s values(generate_series(1,10),20);
merge into test_d d using test_s on(d.a=test_s.a) when matched then update set d.b=test_s.b;
select * from test_d order by a;
-- test when matched and not matched
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b);
select * from test_d order by a;
-- test when matched then delete
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b where test_s.a <= 8 delete where test_s.a = 8 when not matched then insert (a,b) VALUES (test_s.a, test_s.b) where test_s.a >= 3;
select * from test_d order by a;
-- fail for when matched tuple repeatedly
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
insert into test_s values(10,12);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b) where test_s.a >= 3;

-- test execute merge into in anonymous block for when matched and not matched
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
do 
$$
begin
  merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b);
end;
$$
;
select * from test_d order by a;
-- test execute merge into in anonymous block for when matched then delete
truncate table test_d;
truncate table test_s;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
do 
$$
begin
  merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b where test_s.a <= 8 delete where test_s.a = 8 when not matched then insert (a,b) VALUES (test_s.a, test_s.b) where test_s.a >= 3;
end;
$$
;
select * from test_d order by a;

drop table test_d;
drop table test_s;

-- test merge into for REPLICATION
create table test_d (a int, b int) DISTRIBUTE BY REPLICATION;
create table test_s (a int, b int) DISTRIBUTE BY REPLICATION;
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b);
select * from test_d order by a;

drop table test_d;
drop table test_s;
create table test_d (a int, b int) DISTRIBUTE BY REPLICATION;
create table test_s (a int, b int);
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b when not matched then insert (a,b) VALUES (test_s.a, test_s.b);
select * from test_d order by a;

-- test merge into REPLICATION with join on non-distribute key, ok
drop table test_d;
drop table test_s;
create table test_d (a int, b int, c int) DISTRIBUTE BY REPLICATION;
create table test_s (a int, b int, c int);
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
insert into test_s values(generate_series(1,10),generate_series(1,10),2);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;

-- test merge into join on non-distribute key, ok
drop table test_d;
drop table test_s;
create table test_d (a int, b int, c int);
create table test_s (a int, b int, c int);
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
insert into test_s values(generate_series(1,10),generate_series(1,10),2);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;
truncate test_d;
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
merge into test_d using test_s on(test_d.a=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;

-- test merge into join on non-distribute key, ok
drop table test_d;
drop table test_s;
create table test_d (a int, b int, c int);
create table test_s (a int, b int, c int) DISTRIBUTE BY REPLICATION;
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
insert into test_s values(generate_series(1,10),generate_series(1,10),2);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;
truncate test_d;
insert into test_d values(generate_series(6,10),generate_series(6,10),1);
merge into test_d using test_s on(test_d.a=test_s.b) when matched then update set test_d.c=test_s.c when not matched then insert (a,b,c) VALUES (test_s.a, test_s.b, test_s.c);
select * from test_d order by a;

-- clean table
drop table test_d;
drop table test_s;

-- test when matched with multi where conditions
create table test1(id int primary key,name varchar2(10));
insert into test1 values(1,'test11');
insert into test1 values(2,'test12');
insert into test1 values(3,'test13');
create table test2(id int primary key,name varchar2(10));
insert into test2 values(2,'test22');
insert into test2 values(3,'test23');
insert into test2 values(4,'test24');
insert into test2 values(5,'test25');
insert into test2 values(6,'test26');
MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.name = t2.name WHERE t.id >= 2 and t.name = 'test12'
    WHEN NOT MATCHED THEN INSERT (id,name) VALUES (t2.id, t2.name) WHERE t2.name = 'test25' and t2.id <= 5 ;
select * from test1 order by id;
--fail for violates unique constraint
merge into test1 a using test2 b on(a.id=b.id) when matched then update set a.id = 12;

truncate test1;
insert into test1 values(1,'test22');
insert into test1 values(3,'test23');
merge into test1 a using test2 b on(a.name=b.name) when matched then update set a.id = 12;

truncate test1;
insert into test1 values(1,'test11');
insert into test1 values(3,'test23');
merge into test1 a using test2 b on(a.name=b.name) when matched then update set a.id = 12;
select * from test1 order by id;
drop table test1;
drop table test2;
-- test array type for merge into
create table test_d(a int[3],b int);
create table test_s(a int[3],b int);
insert into test_d values('{1,2,3}',4);
insert into test_s values('{10,20,30}',4);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values('{11,21,31}',4);
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a=test_s.a;
select * from test_d;
drop table test_d;
drop table test_s;

create type newtype as(a int,b int);
create table test_d(a newtype, b int);
create table test_s(a newtype, b int);
insert into test_d values(ROW(1,2),3);
insert into test_s values(ROW(10,20),3);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values(ROW(11,12),3);
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values(ROW(22,22),3);
merge into test_d a using test_s on(a.b=test_s.b) when matched then update set a.a=21;
merge into test_d a using test_s on(a.b=test_s.b) when matched then update set a.b=22;
select * from test_d;
--fail
merge into test_d a using test_s on(a.b=test_s.b) when matched then update set a.a=test_s.a;
--must compatible with previous features, though not perfect
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a.a=test_s.b;
select * from test_d;
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a.a=test_s.b;
select * from test_d;
drop table test_s;
drop table test_d;
drop type newtype;

--merge into does not support view
create table empx(empno int,deptno int);
create table emp2 as select * from empx;
create view empx1 as select * from emp2 where deptno=20;
merge into empx1 a
using empx b
on (a.empno=b.empno)
when matched then
  update set a.sal=a.sal+100
when not matched then
  insert(empno,ename,sal,deptno) values (b.empno,b.ename,b.sal,b.deptno);
drop view empx1;

-- test partition table
create table test1(id int,name varchar2(10),age int) partition by list(name);
CREATE TABLE p_list_test1_201901 PARTITION OF test1 FOR VALUES in ('201901');
CREATE TABLE p_list_test1_201902 PARTITION OF test1 FOR VALUES in ('201902');
CREATE TABLE p_list_test1_201903 PARTITION OF test1 FOR VALUES in ('201903');

create unique index on p_list_test1_201901(id,name);
create unique index on p_list_test1_201902(id,name);
create unique index on p_list_test1_201903(id,name);

insert into test1(id,name,age) values(1,'201901',1);
insert into test1(id,name,age) values(2,'201902',2);
insert into test1(id,name,age) values(3,'201903',3);

create table test2(id int,name varchar2(10),age int) partition by list(name);
CREATE TABLE p_list_test2_201901 PARTITION OF test2 FOR VALUES in ('201901');
CREATE TABLE p_list_test2_201902 PARTITION OF test2 FOR VALUES in ('201902');
CREATE TABLE p_list_test2_201903 PARTITION OF test2 FOR VALUES in ('201903');
create unique index on p_list_test2_201901(id,name);
create unique index on p_list_test2_201902(id,name);
create unique index on p_list_test2_201903(id,name);
insert into test2 values(2,'201901',12);
insert into test2 values(3,'201902',13);
insert into test2 values(4,'201903',14);
insert into test2 values(5,'201901',15);
MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.name = t2.name WHERE t.id = t2.id
    WHEN NOT MATCHED THEN INSERT (id,name) VALUES (t2.id, t2.name) ;
select * from test1 order by id;

truncate table test1;
truncate table test2;
insert into test1(id,name,age) values(1,'201901',1);
insert into test1(id,name,age) values(2,'201902',2);
insert into test1(id,name,age) values(3,'201903',3);
insert into test2 values(2,'201901',12);
insert into test2 values(3,'201902',13);
insert into test2 values(4,'201903',14);
insert into test2 values(5,'201901',15);

MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.name = t2.name, t.age = t2.age WHERE t.id = t2.id
    WHEN NOT MATCHED THEN INSERT (id,name,age) VALUES (t2.id, t2.name, t2.age) ;
select * from test1 order by id;

update test2 set age = age + 100;
MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.age = t2.age WHERE t.id = t2.id
    WHEN NOT MATCHED THEN INSERT (id, age) VALUES (t2.id, t2.age) ;
select * from test1 order by id;

-- fail for when matched tuple repeatedly
truncate table test1;
truncate table test2;
insert into test1(id,name,age) values(1,'201901',1);
insert into test1(id,name,age) values(2,'201902',2);
insert into test1(id,name,age) values(3,'201903',3);
insert into test2 values(2,'201901',12);
insert into test2 values(3,'201902',13);
insert into test2 values(3,'201901',13);
MERGE INTO test1 t
USING (
  select * from test2
) t2 ON (t.id = t2.id)
WHEN MATCHED THEN UPDATE SET t.name = t2.name, t.age = t2.age WHERE t.id = t2.id
    WHEN NOT MATCHED THEN INSERT (id,name,age) VALUES (t2.id, t2.name, t2.age) ;

drop table test1;
drop table test2;

-- merge into update distribute key
DROP TABLE IF EXISTS test1;
create table test1(id int primary key,f2 int,name varchar2(10));
insert into test1 values(1,1,'test1');
insert into test1 values(2,2,'test1');
insert into test1 values(3,3,'test1');

DROP TABLE IF EXISTS test2;
create table test2(id int primary key,f2 int,name varchar2(10));
insert into test2 values(12,2,'test2');
insert into test2 values(13,3,'test2');
insert into test2 values(4,4,'test2');
insert into test2 values(5,5,'test2');
MERGE INTO test1 t
USING (
select * from test2
) t2 ON (t.f2  = t2.f2 )
WHEN MATCHED THEN UPDATE SET t.id=t2.id,t.name = t2.name WHERE t.f2 = t2.f2 
WHEN NOT MATCHED THEN INSERT (id,f2,name) VALUES (t2.id,t2.f2, t2.name) ;
select * from test1 order by id;
drop table test1;
drop table test2;

DROP TABLE IF EXISTS newproducts_row,products_row_multi;
CREATE TABLE newproducts_row
(
product_id INTEGER DEFAULT 0,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
);

INSERT INTO newproducts_row VALUES (1502, 'olympus camera', 'electrncs', 200);
INSERT INTO newproducts_row VALUES (1601, 'lamaze', 'toys', 200);
INSERT INTO newproducts_row VALUES (1666, 'harry potter', 'toys', 200);
INSERT INTO newproducts_row VALUES (1700, 'wait interface', 'books', 200);

CREATE TABLE products_row_multi
(
product_id INTEGER,
product_name VARCHAR(60),
category VARCHAR(60),
total INTEGER
);

INSERT INTO products_row_multi VALUES (1501, 'vivitar 35mm', 'electrncs', 100);
INSERT INTO products_row_multi VALUES (1502, 'olympus is50', 'electrncs', 100);
INSERT INTO products_row_multi VALUES (1600, 'play gym', 'toys', 100);
INSERT INTO products_row_multi VALUES (1601, 'lamaze', 'toys', 100);
INSERT INTO products_row_multi VALUES (1666, 'harry potter', 'dvd', 100);

ALTER TABLE products_row_multi DROP COLUMN product_name; 

MERGE INTO products_row_multi p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.category || 'ABC', np.total + 100);

SELECT * FROM products_row_multi ORDER BY product_id;
drop table newproducts_row,products_row_multi;

-- merge into with regexp expr
drop table if exists regexp_count_tb3 ;
create table regexp_count_tb3(id int,id1 varchar2(40),id2 varchar(40),id3 char(3),id4 char(40),id5 int,id6 char(1));
insert into regexp_count_tb3 values(10,'adbadrXXbcdfgdiec,ncdn','adbadrbcdfaagdiec,ncdn','a','adbadrbcdfgdiec,ncdn',1,'i'); 
insert into regexp_count_tb3 values(11,'adbadrabmmcdfBBBbb,ncdn','adbadrbcdfbbbbb,ncdn','B','adba1dr1bcd3fb3bb42b3b,ncdn',1,'c'); 
insert into regexp_count_tb3 values(12,'adbadriaannbcdfgdiec,ncdn','adbadrbCCCcdfgdiec,ncdn','c','adbad2rbcdf113gd2iec,2ncdn',2,'m'); 
insert into regexp_count_tb3 values(13,'adbadriaaabcdfedgdiec,ncdn','adbadrbdddcdfgdiec,ncdn','D','adba,d1rb,cdf,gd1i,ec,nc1dn',1,'n'); 
insert into regexp_count_tb3 values(14,'adb.xmdr,bcxmadf,gdi,ec,ncdn','AAdbdda,drb,cdf,gd,iec,ncdn',',','ad-ba-dr3bcd-fgd3-iec,n3cdn',3,'x'); 
insert into regexp_count_tb3 values(15,'ad-badaxxr-bcxxdccfg-die-c,','adba-drbbb-cdfgdi-ec-ncdn','-','adba#d123c#df321gda#ie33c#ncdn',1,'m'); 
insert into regexp_count_tb3 values(16,'a#adxmrdcbinca#dfingdiec#nc','adb#adqqrb#cdfgd#iec','#','a#',1,'i'); 
insert into regexp_count_tb3 values(17,null,null,null,null,null,null); 
--create table as 
drop table if exists regexp_count_tb5;
create table regexp_count_tb5 as select id,regexp_count(id1,'a',1) as id1 from regexp_count_tb3;

delete from regexp_count_tb5 where id = 10 or id=11;
select * from regexp_count_tb5 order by 1,2;

MERGE INTO regexp_count_tb5 t
USING (
  select * from regexp_count_tb3
) t2 ON (t.id = t2.id+1)
WHEN MATCHED THEN UPDATE SET t.id1 = regexp_count(t2.id1,'bc',1,'c')+4 where (t.id = t2.id+1)
WHEN NOT MATCHED THEN INSERT (id,id1) VALUES (t2.id, regexp_count(t2.id2,'bc',1,'c')) ;
select * from regexp_count_tb5 order by id, id1;

-- http://tapd.woa.com/20421696/bugtrace/bugs/view?bug_id=1020421696095179657
drop table if exists test_date_tb5;
create table test_date_tb5                      
(                                               
    id int primary key,                                     
    col1 numeric,                               
    col2 number,                                
    col3 int,                                
    col4 varchar2(30),                          
    col5 varchar(30),                           
    col6 char(30),                              
    col7 float ,
    col8 date                                 
);                                                                                     
insert into test_date_tb5  values(1,20200101.000+1,   20200102.000+1,   20200201+1,       20200201+1,       20200301+1,       20200302+1,       20200301+1,null); 
insert into test_date_tb5  values(2,   20200101.000+2,   20200102.000+2,   20200201+2,       20200201+2,       20200301+2,       20200302+2,       20200301+2,null); 
insert into test_date_tb5  values(3,   20200101.000+3,   20200102.000+3,   20200201+3,       20200201+3,       20200301+3,       20200302+3,       20200301+3,null); 
insert into test_date_tb5  values(4,   20200101.000+4,   20200102.000+4,   20200201+4,       20200201+4,       20200301+4,       20200302+4,       20200301+4,null); 
insert into test_date_tb5  values(5,   20200101.000+5,   20200102.000+5,   20200201+5,       20200201+5,       20200301+5,       20200302+5,       20200301+5,null); 
insert into test_date_tb5  values(6,   20200101.000+6,   20200102.000+6,   20200201+6,       20200201+6,       20200301+6,       20200302+6,       20200301+6,null); 
insert into test_date_tb5  values(7,   20200101.000+7,   20200102.000+7,   20200201+7,       20200201+7,       20200301+7,       20200302+7,       20200301+7,null); 
insert into test_date_tb5  values(8,   20200101.000+8,   20200102.000+8,   20200201+8,       20200201+8,       20200301+8,       20200302+8,       20200301+8,null); 
insert into test_date_tb5  values(9,   20200101.000+9,   20200102.000+9,   20200201+9,       20200201+9,       20200301+9,       20200302+9,       20200301+9,null); 
insert into test_date_tb5  values(10,   20200101.000+10,   20200102.000+10,   20200201+10,       20200201+10,       20200301+10,       20200302+10,       20200301+10,null); 
insert into test_date_tb5  values(11,   20200101.000+11,   20200102.000+11,   20200201+11,       20200201+11,       20200301+11,       20200302+11,       20200301+11,null); 
insert into test_date_tb5  values(12,   20200101.000+12,   20200102.000+12,   20200201+12,       20200201+12,       20200301+12,       20200302+12,       20200301+12,null);      
insert into test_date_tb5  values(13,   20200101.000+13,   20200102.000+13,   20200201+13,       20200201+13,       20200301+13,       20200302+13,       20200301+13,null); 
insert into test_date_tb5  values(14,   20200101.000+14,   20200102.000+14,   20200201+14,       20200201+14,       20200301+14,       20200302+14,       20200301+14,null); 
insert into test_date_tb5  values(15,   20200101.000+15,   20200102.000+15,   20200201+15,       20200201+15,       20200301+15,       20200302+15,       20200301+15,null); 
drop table if exists test_date_tb6;
create  table test_date_tb6 as select * from test_date_tb5;
merge into test_date_tb5 t1 
using  test_date_tb6 
on (t1.id = test_date_tb6.id+1)
WHEN MATCHED THEN
UPDATE SET t1.col8 = to_date(test_date_tb6.col2,'yyyymmdd'), t1.col3=cast(test_date_tb6.col4 as int) 
WHEN NOT MATCHED THEN
INSERT (id,col1,col2) VALUES (test_date_tb6.id+1,test_date_tb6.col2,test_date_tb6.col1) ; 
select * from test_date_tb5 order by id;

-- http://tapd.woa.com/20421696/bugtrace/bugs/view?bug_id=1020421696095179655
truncate test_date_tb5;
insert into test_date_tb5  values(1,20200101.000+1,   20200102.000+1,   20200201+1,       20200201+1,       20200301+1,       20200302+1,       20200301+1); 
insert into test_date_tb5  values(2,   20200101.000+2,   20200102.000+2,   20200201+2,       20200201+2,       20200301+2,       20200302+2,       20200301+2); 
insert into test_date_tb5  values(3,   20200101.000+3,   20200102.000+3,   20200201+3,       20200201+3,       20200301+3,       20200302+3,       20200301+3); 
insert into test_date_tb5  values(4,   20200101.000+4,   20200102.000+4,   20200201+4,       20200201+4,       20200301+4,       20200302+4,       20200301+4); 
insert into test_date_tb5  values(5,   20200101.000+5,   20200102.000+5,   20200201+5,       20200201+5,       20200301+5,       20200302+5,       20200301+5); 
insert into test_date_tb5  values(6,   20200101.000+6,   20200102.000+6,   20200201+6,       20200201+6,       20200301+6,       20200302+6,       20200301+6); 
insert into test_date_tb5  values(7,   20200101.000+7,   20200102.000+7,   20200201+7,       20200201+7,       20200301+7,       20200302+7,       20200301+7); 
insert into test_date_tb5  values(8,   20200101.000+8,   20200102.000+8,   20200201+8,       20200201+8,       20200301+8,       20200302+8,       20200301+8); 
insert into test_date_tb5  values(9,   20200101.000+9,   20200102.000+9,   20200201+9,       20200201+9,       20200301+9,       20200302+9,       20200301+9); 
insert into test_date_tb5  values(10,   20200101.000+10,   20200102.000+10,   20200201+10,       20200201+10,       20200301+10,       20200302+10,       20200301+10); 
insert into test_date_tb5  values(11,   20200101.000+11,   20200102.000+11,   20200201+11,       20200201+11,       20200301+11,       20200302+11,       20200301+11); 
insert into test_date_tb5  values(12,   20200101.000+12,   20200102.000+12,   20200201+12,       20200201+12,       20200301+12,       20200302+12,       20200301+12);      
insert into test_date_tb5  values(13,   20200101.000+13,   20200102.000+13,   20200201+13,       20200201+13,       20200301+13,       20200302+13,       20200301+13); 
insert into test_date_tb5  values(14,   20200101.000+14,   20200102.000+14,   20200201+14,       20200201+14,       20200301+14,       20200302+14,       20200301+14); 
insert into test_date_tb5  values(15,   20200101.000+15,   20200102.000+15,   20200201+15,       20200201+15,       20200301+15,       20200302+15,       20200301+15); 

merge into test_date_tb5 t1 
using  test_date_tb5 
on (t1.id = test_date_tb5.id+1)
WHEN MATCHED THEN
UPDATE SET t1.col8 = to_date(test_date_tb5.col2,'yyyymmdd'), t1.col3=cast(test_date_tb5.col4 as int) 
WHEN NOT MATCHED THEN
INSERT (id,col1,col2) VALUES (test_date_tb5.id+1,test_date_tb5.col2,test_date_tb5.col1) ;
select * from test_date_tb5 order by id;

--
-- cases for forbidden updating join on column
--
-- case 1: The source table and target table are the same
drop table if exists homo_table;
create table homo_table(f1 int, f2 text, f3 int) distribute by shard(f1);
insert into homo_table values (10, 'a', 15);
insert into homo_table values (15, 'a', 16);
-- error
merge into homo_table t using homo_table s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;

-- error
merge into homo_table t using homo_table s on (t.f1 = s.f1) when matched then update set f1 = f3;

-- case 2: the source is a subquery that contains the target table
drop table if exists homo_table_2;
create table homo_table_2(f1 int, f2 text, f3 int) distribute by shard(f1);
insert into homo_table_2 values (15, 'a', 15);

-- error
merge into homo_table t using (select * from homo_table s1 where f1 < 10) s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;

-- OK
begin;
merge into homo_table t using (select s1.f1, s1.f2, s2.f3 from homo_table s1 join homo_table_2 s2 on (s1.f3=s2.f3)) s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;
select * from homo_table order by f1;
rollback;

-- error
merge into homo_table t using (select s2.f1, s2.f2, s1.f3 from homo_table s1 join homo_table_2 s2 on (s1.f3=s2.f3)) s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;

with src as (select s2.f1, s2.f2, s1.f3 from homo_table s1 join homo_table_2 s2 on (s1.f3=s2.f3))
merge into homo_table t using src s on (t.f1 = s.f3) when matched then update set f3 = s.f1 + 10;

drop table if exists homo_part_list_20220813;
create table homo_part_list_20220813(f1 number(10),f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) partition by list ( f3 ) distribute by shard(f1) to group default_group;
create table homo_part_list_1_20220813 partition of homo_part_list_20220813 for values in ('f3 r0') partition by hash (f2);
create table homo_part_list_1_h1_20220813 partition of homo_part_list_1_20220813 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
create table homo_part_list_1_h2_20220813 partition of homo_part_list_1_20220813 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
create table homo_part_list_2_20220813 partition of homo_part_list_20220813 for values in ('f3 r1') partition by hash (f2);
create table homo_part_list_2_h1_20220813 partition of homo_part_list_2_20220813 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
create table homo_part_list_2_h2_20220813 partition of homo_part_list_2_20220813 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
create table homo_part_list_3_20220813 partition of homo_part_list_20220813 for values in ('f3 r2') partition by range (f2);

drop table if exists homo_part_interval_20220813;
create table homo_part_interval_20220813(f1 number(10) not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) partition by range(f5) begin(timestamp without time zone '2022-01-01') step(interval '1 day') partitions(10) distribute by shard(f1) to group default_group;

-- case 2: The source table is a child partition of the target table
merge into homo_part_list_20220813 t using homo_part_list_2_20220813 s on (t.f1 = s.f2) when matched then update set f2 = s.f1 + 10;

merge into homo_part_list_20220813 t using homo_part_list_2_h1_20220813 s on (t.f1 = s.f2) when matched then update set f2 = s.f1 + 10;

merge into homo_part_interval_20220813 t using homo_part_interval_20220813_part_0 s on (t.f1 = s.f2) when matched then update set f2 = s.f1 + 10;



create table test1(id int, info varchar2(32));
create table test2(id int, info varchar2(32));
merge into (select * from test2 where id is not null) t2 using (select * from test1 where id<4) t1 on (t2.id=t1.id) when matched then update set t2.info=t1.info;
drop table test1;
drop table test2;
create table tp (id int, name name ,age int, class text, CONSTRAINT tp_index PRIMARY KEY (id, class))PARTITION BY LIST (class) 
DISTRIBUTE BY SHARD (id);

create table tp_c1 partition of tp for values in ('c1');
create table tp_c2 partition of tp for values in ('c2');

merge into tp using (select 1 id, 'aa' name, 10 age, 'c10' class from dual) s on tp.class = s.class when matched then update set age = s.age;

merge into tp using (select 1 id, 'aa' name, 10 age, 'c10' class from dual) s on tp.class = s.class when matched then update set age = s.age when not matched then insert values(s.id, s.name, s.age, s.class);

-- 复制分区表，无子分区
drop table if exists t_merge_20220928;
create table t_merge_20220928 (id int, name name ,age int, class text, CONSTRAINT t_merge_20220928_index PRIMARY KEY (id, class))PARTITION BY LIST (class) DISTRIBUTE by replication;

merge into t_merge_20220928 using (select id+1 id,name,age,class from t_merge_20220928) s on t_merge_20220928.class = s.class when matched then update set age = s.age+100 when not matched then insert values(s.id, s.name, s.age, s.class);

select * from t_merge_20220928;
drop table t_merge_20220928;

-- shard分布分区表，无子分区
create table t_merge_20220928_2 (id int, name name ,age int, class text, CONSTRAINT t_merge_20220928_2_index PRIMARY KEY (id, class))PARTITION BY LIST (class) 
DISTRIBUTE by shard(id);

merge into t_merge_20220928_2 using (select id+1 id,name,age,class from t_merge_20220928_2) s on t_merge_20220928_2.class = s.class when matched then update set age = s.age+100 when not matched then insert values(s.id, s.name, s.age, s.class);

select * from t_merge_20220928_2;
drop table t_merge_20220928_2;
reset enable_distribute_update;

-- merge is independent of the enable_distribute_update parameter
set enable_distribute_update to off;
drop table if exists bug_merge_into_evaluate_1;
create table bug_merge_into_evaluate_1 (id number,name varchar2(20));
insert into bug_merge_into_evaluate_1 select a,'name2'||a from generate_series(1,1000) a;
insert into bug_merge_into_evaluate_1 select a,'name2'||a from generate_series(1,1000) a;
create index idx_bug_merge_into_evaluate_1_id on bug_merge_into_evaluate_1(id);
analyze bug_merge_into_evaluate_1;
select reltuples,relpages from pg_class where relname='BUG_MERGE_INTO_EVALUATE_1';
explain (costs off,nodes off,buffers off,summary off, buffers off) 
merge into bug_merge_into_evaluate_1 a 
using (select '1' as id ,'name1' as name from dual) b
on (a.id=b.id)
when matched then
    update set a.name='name1'
when not matched then
    insert (id,name) values('1','name1');
merge into bug_merge_into_evaluate_1 a 
using (select '1' as id ,'name1' as name from dual) b
on (a.id=b.id)
when matched then
    update set a.name='name1'
when not matched then
    insert (id,name) values('1','name1');
select count(1) from bug_merge_into_evaluate_1 where name='name1';
drop table bug_merge_into_evaluate_1;
create schema IF NOT EXISTS mergeview_with_check;
drop table if exists mergeview_with_check.ora_st_shard_20230310;

create table mergeview_with_check.ora_st_shard_20230310(f1 number(10) not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);

drop table if exists mergeview_with_check.ora_st_shard_20230310_t;
create table mergeview_with_check.ora_st_shard_20230310_t(f1 number(10) not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);

drop view if exists mergeview_with_check.ora_st_shard_20230310_v;

create view mergeview_with_check.ora_st_shard_20230310_v as select * from mergeview_with_check.ora_st_shard_20230310 where f1 < 10 ;

drop view if exists mergeview_with_check.ora_st_shard_20230310_v2;

create view mergeview_with_check.ora_st_shard_20230310_v2 as select * from mergeview_with_check.ora_st_shard_20230310 where f2 < 10 ;

drop table if exists mergeview_with_check.ora_st_shard_20230310_2;

create table mergeview_with_check.ora_st_shard_20230310_2(f1 number(10) not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) distribute by shard(f1);

drop view if exists mergeview_with_check.ora_st_shard_20230310_v_2;

create view mergeview_with_check.ora_st_shard_20230310_v_2 as select * from mergeview_with_check.ora_st_shard_20230310_2;

truncate mergeview_with_check.ora_st_shard_20230310_2;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-01 00:00:00' from generate_series(1,5) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-02 00:00:00' from generate_series(6,10) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-03 00:00:00' from generate_series(11,15) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-04 00:00:00' from generate_series(16,20) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-05 00:00:00' from generate_series(21,25) n;

insert into mergeview_with_check.ora_st_shard_20230310_2 select n, n, 'f3 r'|| floor(n/5),'f4 r' || n, timestamp '2022-01-06 00:00:00' from generate_series(26,30) n;

begin;
-- case 1.1 only not matched
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310_v_2 s on (t.f1 = s.f1) when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from mergeview_with_check.ora_st_shard_20230310_v2 order by f1, f2, f3, f4;
select * from mergeview_with_check.ora_st_shard_20230310 order by f1, f2, f3, f4;
-- case 1.2: matched & not matched
delete from mergeview_with_check.ora_st_shard_20230310_v2 t where MOD (f1, 2) = 0;
delete from mergeview_with_check.ora_st_shard_20230310 t where MOD (f1, 2) = 0;
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310_v_2 s on (t.f1 = s.f1) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
select * from mergeview_with_check.ora_st_shard_20230310_v2 order by f1, f2, f3, f4;
select * from mergeview_with_check.ora_st_shard_20230310 order by f1, f2, f3, f4;
-- case 1.3：matched delete & not matched
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310_v_2 s on (t.f1 = s.f1) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 delete where t.f2 = 31 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 <= 10;
select * from mergeview_with_check.ora_st_shard_20230310_v2 order by f1, f2, f3, f4;
select * from mergeview_with_check.ora_st_shard_20230310 order by f1, f2, f3, f4;
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310_v_2 s on (t.f1 = s.f1) when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
select * from mergeview_with_check.ora_st_shard_20230310_v2 order by f1, f2, f3, f4;
select * from mergeview_with_check.ora_st_shard_20230310 order by f1, f2, f3, f4;
-- case 1.2: matched & not matched
delete from mergeview_with_check.ora_st_shard_20230310_v2 t where MOD (f1, 2) = 0;
delete from mergeview_with_check.ora_st_shard_20230310 t where MOD (f1, 2) = 0;
-- 源表和目标视图引用同一基表
-- 1. 可更新
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using (select * from mergeview_with_check.ora_st_shard_20230310 where f2 < 10) s on (t.f1 = s.f1) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

begin;
-- 1. 不可更新：ORA-38104: Columns referenced in the ON Clause cannot be updated: "S"."F2"
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using (select * from mergeview_with_check.ora_st_shard_20230310 where f2 < 10) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

begin;
-- 1. 不可更新：ORA-38104: Columns referenced in the ON Clause cannot be updated: "S"."F2"
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using mergeview_with_check.ora_st_shard_20230310 s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

-- 源表来自多个表
begin;
-- 1. 不可更新：on条件中列指向同一基表
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using (select s1.* from mergeview_with_check.ora_st_shard_20230310 s1 join mergeview_with_check.ora_st_shard_20230310_t s2 on s1.f1 = s2.f1 where s1.f2 < 10) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

begin;
-- 2. 可更新
merge into mergeview_with_check.ora_st_shard_20230310_v2 t using (select s2.* from mergeview_with_check.ora_st_shard_20230310 s1 join mergeview_with_check.ora_st_shard_20230310_t s2 on s1.f1 = s2.f1 where s1.f2 < 10) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 1 where t.f1 <= 5 when not matched then insert values(s.f1 - 1, s.f2, s.f3, s.f4, s.f5) where s.f1 >= 25;
rollback;

create table mergeview_with_check.ora_st_shard_part_list_20220710(f1 number(10) not null,f2 integer,f3 varchar2(20),f4 varchar2(20), f5 timestamp without time zone) partition by list ( f3 ) distribute by shard(f1) to group default_group;
create table mergeview_with_check.ora_st_shard_part_list_1_20220710 partition of mergeview_with_check.ora_st_shard_part_list_20220710 for values in ('f3 r0');
create table mergeview_with_check.ora_st_shard_part_list_2_20220710 partition of mergeview_with_check.ora_st_shard_part_list_20220710 for values in ('f3 r1');
create table mergeview_with_check.ora_st_shard_part_list_3_20220710 partition of mergeview_with_check.ora_st_shard_part_list_20220710 for values in ('f3 r2');

begin;
merge into mergeview_with_check.ora_st_shard_part_list_1_20220710 t using mergeview_with_check.ora_st_shard_part_list_20220710 s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_20220710 t using mergeview_with_check.ora_st_shard_part_list_1_20220710 s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_20220710 t using (select s1.* from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_1_20220710 t using (select s1.* from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_1_20220710 t using (select s2.* from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_20220710 t using (select s2.f1 f1, s1.f2 f2, s1.f3, s1.f4, s1.f5 from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f1 and t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;

begin;
merge into mergeview_with_check.ora_st_shard_part_list_3_20220710 t using (select s1.* from mergeview_with_check.ora_st_shard_part_list_1_20220710 s1 join mergeview_with_check.ora_st_shard_part_list_2_20220710 s2 on s1.f3 = s2.f3) s on (t.f1 = s.f2) when matched then update set f2 = s.f2 + 2 where t.f1 >=25 when not matched then insert values(s.f1, s.f2, s.f3, s.f4, s.f5);
rollback;
drop schema if exists mergeview_with_check cascade;

\c regression
-- https://tapd.woa.com/20421696/prong/stories/view/1020421696883101603
drop sequence if exists tt;
drop sequence if exists tt11;
drop sequence if exists sqq;
drop table if exists t1; 
drop table if exists t11; 
drop table if exists t2; 
create sequence tt start 1;
create sequence tt11 start 1;
create sequence sqq start 1;

create table t2 (f1 int, f2 varchar(36), create_date pg_catalog.timestamp(6)) distribute by shard(f1) ;
create table t1 (f1 int, f2 varchar(36), create_date pg_catalog.timestamp(6)) partition by range (create_date) distribute by shard(f1) ;

create table t1_01 partition of t1 for values from ( '2023-01-01') to ( '2023-02-01');
create table t1_02 partition of t1 for values from ( '2023-02-01') to ( '2023-03-01');
create table t1_03 partition of t1 for values from ( '2023-03-01') to ( '2023-04-01');
create table t1_04 partition of t1 for values from ( '2023-04-01') to ( '2023-05-01');
create table t1_05 partition of t1 for values from ( '2023-05-01') to ( '2023-06-01');
create table t1_06 partition of t1 for values from ( '2023-06-01') to ( '2023-07-01');
create table t1_07 partition of t1 for values from ( '2023-07-01') to ( '2023-08-01');
create table t1_08 partition of t1 for values from ( '2023-08-01') to ( '2023-09-01');
create table t1_09 partition of t1 for values from ( '2023-09-01') to ( '2023-10-01');
create table t1_10 partition of t1 for values from ( '2023-10-01') to ( '2023-11-01');
create table t1_11 partition of t1 for values from ( '2023-11-01') to ( '2023-12-01');
create table t1_12 partition of t1 for values from ( '2023-12-01') to ( '2024-01-01');
create table t1_13 partition of t1 for values from ( '2024-01-01') to ( '2024-02-01');
create table t1_14 partition of t1 for values from ( '2024-02-01') to ( '2024-03-01');
create table t1_15 partition of t1 for values from ( '2024-03-01') to ( '2024-04-01');
create table t1_16 partition of t1 for values from ( '2024-04-01') to ( '2024-05-01');
create table t1_17 partition of t1 for values from ( '2024-05-01') to ( '2024-06-01');
create table t1_18 partition of t1 for values from ( '2024-06-01') to ( '2024-07-01');
create table t1_19 partition of t1 for values from ( '2024-07-01') to ( '2024-08-01');
create index t1_test001 on t1 (f1);
create index t1_test002 on t1 (create_date);


insert into t1 select nextval('tt'), currval('tt'),generate_series(date'2023-01-01',date'2024-07-30','1 hour') ;

insert into t2 select nextval('sqq'), currval('sqq') + 1, generate_series(date'2023-01-01',date'2024-07-30','1 hour') ;

merge into t1 using t2 on t1.f1 = t2.f1 when matched then update set t1.f2 = t2.f2 when not matched then insert(f1, f2, create_date) values (99, '999', '2023-05-01');

explain (costs off) merge into t1 using t2 on t1.f1 = t2.f1 when matched then update set t1.f2 = t2.f2 when not matched then insert(f1, f2, create_date) values (99, '999', '2023-05-01');

analyze t1;
analyze t2;
set enable_seqscan TO off;
set enable_bitmapscan TO off;
explain (costs off) select f2 from t1 t where create_date >= date'2023-04-01' and create_date < date'2023-04-02' order by f2;
select f2 from t1 t where create_date >= date'2023-04-01' and create_date < date'2023-04-02' order by f2;

select distinct f2 from t1 t where create_date >= date'2023-04-01' and create_date < date'2023-04-02' order by f2;
reset enable_seqscan;
reset enable_bitmapscan;

create user merge_r1;
grant all on t1 to merge_r1;
grant all on t2 to merge_r1;
grant all on t1_03 to merge_r1;

alter table t1 enable row level security ;
alter table t1_03 enable row level security ;

create policy p_t1 on t1 for insert to merge_r1 with check( create_date < '2023-03-20');
create policy p_t1_s on t1 for select to merge_r1 using( create_date < '2023-03-20');
create policy p_t1_03 on t1_03 for insert to merge_r1 with check( create_date < '2023-03-15');

set session authorization merge_r1;

merge into t1 using t2 on t1.f1 = t2.f1 when not matched then insert(f1, f2, create_date) values (99, '999', '2023-03-20');

merge into t1 using t2 on t1.f1 = t2.f1 when matched then update set t1.f2 = t2.f2;

merge into t1 using t2 on t1.f1 = t2.f1 when matched then update set t1.f2 = t2.f2 when not matched then insert(f1, f2, create_date) values (99, '999', '2023-03-16');

reset session authorization;
DROP POLICY p_t1 on t1;
DROP POLICY p_t1_s on t1;
DROP POLICY p_t1_03 on t1_03;
drop sequence if exists tt;
drop sequence if exists tt11;
drop sequence if exists sqq;
drop table if exists t1; 
drop table if exists t2; 
drop user merge_r1;

-- merge with dual
CREATE TABLE tlbb_server_log (
    sn varchar(50),
    group_id numeric(6,0),
    buy_time timestamp(0) without time zone,
    jewel_total numeric(10,0),
    cn varchar(80),
    ip varchar(20),
    weaponid varchar(200),
    put_date timestamp(0) without time zone
)
PARTITION BY RANGE (put_date)
DISTRIBUTE BY SHARD (sn);

CREATE TABLE tlbb_server_log_2019 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2019-01-01 00:00:00') TO ('2020-01-01 00:00:00');
CREATE TABLE tlbb_server_log_2020q1 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2020-04-01 00:00:00');
CREATE TABLE tlbb_server_log_2020q2 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2020-04-01 00:00:00') TO ('2020-07-01 00:00:00');
CREATE TABLE tlbb_server_log_2020q3 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2020-07-01 00:00:00') TO ('2020-10-01 00:00:00');
CREATE TABLE tlbb_server_log_2020q4 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2020-10-01 00:00:00') TO ('2021-01-01 00:00:00');

CREATE OR REPLACE FUNCTION to_number(a text) RETURNS NUMERIC AS
$$
SELECT to_number(a, '99999999999999999999');
$$ LANGUAGE SQL pushdown;

explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN NOT MATCHED AND U.GROUP_ID > 0 THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN NOT MATCHED AND U.GROUP_ID > 0  THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

select * from tlbb_server_log;
select * from tlbb_server_log_2020q1;
explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN MATCHED AND T.GROUP_ID > 0 THEN UPDATE SET PUT_DATE = TO_DATE('2020-10-29','YYYY-MM-DD')
WHEN NOT MATCHED AND U.GROUP_ID > 0 THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN MATCHED AND T.GROUP_ID > 0 THEN UPDATE SET PUT_DATE = TO_DATE('2020-10-29','YYYY-MM-DD')
WHEN NOT MATCHED AND U.GROUP_ID > 0 THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

select * from tlbb_server_log;
select * from tlbb_server_log_2020q4;

explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.BUY_TIME=U.BUY_TIME)
WHEN MATCHED AND T.GROUP_ID > 0 THEN UPDATE SET PUT_DATE = TO_DATE('2020-4-29','YYYY-MM-DD'), SN='0000000000'
WHEN NOT MATCHED AND U.GROUP_ID > 0 THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.BUY_TIME=U.BUY_TIME)
WHEN MATCHED AND T.GROUP_ID > 0 THEN UPDATE SET PUT_DATE = TO_DATE('2020-4-29','YYYY-MM-DD')
WHEN NOT MATCHED AND U.GROUP_ID > 0 THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

select * from tlbb_server_log;
select * from tlbb_server_log_2020q2;

truncate tlbb_server_log;
explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT  * from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED AND U.GROUP_ID > 0 THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

MERGE INTO TLBB_SERVER_LOG T
USING (SELECT  * from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED AND U.GROUP_ID > 0 THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

select * from tlbb_server_log;
select * from tlbb_server_log_2020q1;

truncate tlbb_server_log;
explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT  TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED AND U.GROUP_ID > 0 THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

MERGE INTO TLBB_SERVER_LOG T
USING (SELECT  TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED AND U.GROUP_ID > 0 THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE);

select * from tlbb_server_log;
select * from tlbb_server_log_2020q1;

drop table tlbb_server_log;
DROP FUNCTION to_number(text);

\c regression_ora
-- set nls_date_format = 'yyyy-mm-dd hh24:mi:ss';
-- set nls_timestamp_format = 'yyyy-mm-dd hh24:mi:ss';
CREATE TABLE tlbb_server_log (
    sn varchar2(50),
    group_id numeric(6,0),
    buy_time timestamp(0) without time zone,
    jewel_total numeric(10,0),
    cn varchar2(80),
    ip varchar2(20),
    weaponid varchar2(200),
    put_date timestamp(0) without time zone
)
PARTITION BY RANGE (put_date)
DISTRIBUTE BY SHARD (sn);

CREATE TABLE tlbb_server_log_2019 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2019-01-01 00:00:00') TO ('2020-01-01 00:00:00');
CREATE TABLE tlbb_server_log_2020q1 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2020-04-01 00:00:00');
CREATE TABLE tlbb_server_log_2020q2 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2020-04-01 00:00:00') TO ('2020-07-01 00:00:00');
CREATE TABLE tlbb_server_log_2020q3 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2020-07-01 00:00:00') TO ('2020-10-01 00:00:00');
CREATE TABLE tlbb_server_log_2020q4 PARTITION OF tlbb_server_log
FOR VALUES FROM ('2020-10-01 00:00:00') TO ('2021-01-01 00:00:00');

explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

select * from tlbb_server_log;
select * from tlbb_server_log_2020q1;
explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN MATCHED THEN UPDATE SET PUT_DATE = TO_DATE('2020-10-29','YYYY-MM-DD') WHERE T.GROUP_ID > 0
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME)
WHEN MATCHED THEN UPDATE SET PUT_DATE = TO_DATE('2020-10-29','YYYY-MM-DD') WHERE T.GROUP_ID > 0
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

select * from tlbb_server_log;
select * from tlbb_server_log_2020q4;

truncate tlbb_server_log;
explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT  * from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

MERGE INTO TLBB_SERVER_LOG T
USING (SELECT  * from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

select * from tlbb_server_log;
select * from tlbb_server_log_2020q1;

truncate tlbb_server_log;
explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT  TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

MERGE INTO TLBB_SERVER_LOG T
USING (SELECT  TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE from (select TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM DUAL) )U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

CREATE TABLE tlbb_server_src_s (
    sn varchar2(50),
    group_id numeric(6,0),
    buy_time timestamp(0) without time zone,
    jewel_total numeric(10,0),
    cn varchar2(80),
    ip varchar2(20),
    weaponid varchar2(200),
    put_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (sn);

CREATE TABLE tlbb_server_src_r (
    sn varchar2(50),
    group_id numeric(6,0),
    buy_time timestamp(0) without time zone,
    jewel_total numeric(10,0),
    cn varchar2(80),
    ip varchar2(20),
    weaponid varchar2(200),
    put_date timestamp(0) without time zone
)
DISTRIBUTE BY replication;

explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM tlbb_server_src_s) U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

explain (costs off, nodes off)
MERGE INTO TLBB_SERVER_LOG T
USING (SELECT TO_CHAR('003313087142937153390') AS SN, TO_NUMBER('33') AS GROUP_ID, TO_DATE('2020-03-29 14:29:37','YYYY-MM-DD HH24:MI:SS') AS BUY_TIME,
		TO_NUMBER('10') AS JEWEL_TOTAL, TO_CHAR('760966439') AS CN, TO_CHAR('123.165.59.109') AS IP, TO_CHAR('40004427') AS WEAPONID, TO_DATE('2020-03-29','YYYY-MM-DD') AS PUT_DATE
		FROM tlbb_server_src_r) U
ON (T.SN=U.SN AND T.BUY_TIME=U.BUY_TIME AND T.PUT_DATE=U.PUT_DATE)
WHEN NOT MATCHED THEN INSERT  (T.SN,T.GROUP_ID,T.BUY_TIME,T.JEWEL_TOTAL,T.CN,T.IP,T.WEAPONID,T.PUT_DATE) 
VALUES(U.SN, U.GROUP_ID, U.BUY_TIME, U.JEWEL_TOTAL, U.CN, U.IP, U.WEAPONID, U.PUT_DATE) WHERE U.GROUP_ID > 0 ;

select * from tlbb_server_log;
select * from tlbb_server_log_2020q1;

-- 1020421696886021453 merge into a table with trigger
set enable_datanode_row_triggers to true;
create table merge_trg_rel_20230808(id int, name varchar2(100), salary int);
create table del_merge_trg_rel_20230808(id int, name varchar2(100), salary int);
create table new_merge_trg_rel_20230808(id int, name varchar2(100), salary int);
create view merge_trg_view_20230808_v as select * from merge_trg_rel_20230808 where salary > 15000;
insert into merge_trg_rel_20230808 values (1, 'tom', 10000);
insert into merge_trg_rel_20230808 values (2, 'jim', 13000);
insert into merge_trg_rel_20230808 values (3, 'lucy', 15000);
insert into new_merge_trg_rel_20230808 values (1, 'Tom', 12000);
insert into new_merge_trg_rel_20230808 values (2, 'Jim', 15000);
insert into new_merge_trg_rel_20230808 values (3, 'Lucy', 17000);
insert into new_merge_trg_rel_20230808 values (4, 'Jack', 20000);
-- test before insert or update trigger
CREATE OR REPLACE FUNCTION upd_ins_trg_20230808_f()
RETURNS TRIGGER AS
BEGIN
INSERT INTO del_merge_trg_rel_20230808 (id, name, salary) VALUES (NEW.id, NEW.name, NEW.salary + 1000);
RETURN NEW;
END;
/
CREATE TRIGGER be_up_ins_trg_20230808
    BEFORE INSERT OR UPDATE ON merge_trg_rel_20230808
                         FOR EACH ROW
                         EXECUTE FUNCTION upd_ins_trg_20230808_f();
MERGE INTO merge_trg_rel_20230808 e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        UPDATE SET e.name = n.name, e.salary = n.salary
    WHEN NOT MATCHED THEN
        INSERT (id, name, salary) VALUES (n.id, n.name, n.salary);
select * from merge_trg_rel_20230808 order by 1, 2;
select * from new_merge_trg_rel_20230808 order by 1;
select * from del_merge_trg_rel_20230808;
truncate table del_merge_trg_rel_20230808;
truncate table merge_trg_rel_20230808;
drop TRIGGER be_up_ins_trg_20230808 on merge_trg_rel_20230808;
-- test after insert or update trigger
insert into merge_trg_rel_20230808 values (1, 'tom', 10000);
CREATE TRIGGER af_up_ins_trg_20230808
    AFTER INSERT OR UPDATE ON merge_trg_rel_20230808
                        FOR EACH ROW
                        EXECUTE FUNCTION upd_ins_trg_20230808_f();
MERGE INTO merge_trg_rel_20230808 e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        UPDATE SET e.name = n.name, e.salary = n.salary
    WHEN NOT MATCHED THEN
        INSERT (id, name, salary) VALUES (n.id, n.name, n.salary);
select * from merge_trg_rel_20230808 order by 1, 2;
select * from new_merge_trg_rel_20230808 order by 1;
select * from del_merge_trg_rel_20230808;
truncate table del_merge_trg_rel_20230808;
truncate table merge_trg_rel_20230808;
drop TRIGGER af_up_ins_trg_20230808 on merge_trg_rel_20230808;
-- test before delete trigger
insert into merge_trg_rel_20230808 values (1, 'tom', 10000);
insert into merge_trg_rel_20230808 values (2, 'jim', 13000);
insert into merge_trg_rel_20230808 values (3, 'lucy', 15000);
CREATE OR REPLACE FUNCTION del_trg_20230808_f()
RETURNS TRIGGER AS
BEGIN
INSERT INTO del_merge_trg_rel_20230808 (id, name, salary)
VALUES (OLD.id, OLD.name, OLD.salary);
RETURN OLD;
END;
/
CREATE TRIGGER bef_del_trg_20230808
    AFTER DELETE ON merge_trg_rel_20230808
    FOR EACH ROW
    EXECUTE FUNCTION del_trg_20230808_f();
MERGE INTO merge_trg_rel_20230808 e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        update set e.name = n.name, e.salary = n.salary delete where e.id = 1;
select * from merge_trg_rel_20230808 order by 1, 2;
select * from del_merge_trg_rel_20230808;
truncate table del_merge_trg_rel_20230808;
truncate table merge_trg_rel_20230808;
drop TRIGGER bef_del_trg_20230808 on merge_trg_rel_20230808;
-- test after delete trigger
insert into merge_trg_rel_20230808 values (1, 'tom', 10000);
insert into merge_trg_rel_20230808 values (2, 'jim', 13000);
insert into merge_trg_rel_20230808 values (3, 'lucy', 15000);
CREATE TRIGGER aft_del_trg_20230808
    AFTER DELETE ON merge_trg_rel_20230808
    FOR EACH ROW
    EXECUTE FUNCTION del_trg_20230808_f();
MERGE INTO merge_trg_rel_20230808 e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        update set e.name = n.name, e.salary = n.salary delete where e.id = 1;
select * from merge_trg_rel_20230808 order by 1, 2;
select * from del_merge_trg_rel_20230808;
truncate table del_merge_trg_rel_20230808;
drop TRIGGER aft_del_trg_20230808 on merge_trg_rel_20230808;
-- error: test instead of insert or update trigger on view
CREATE TRIGGER ins_of_upins_trg_20230808
    INSTEAD OF INSERT OR UPDATE ON merge_trg_view_20230808_v
                             FOR EACH ROW
                             EXECUTE FUNCTION upd_ins_trg_20230808_f();
MERGE INTO merge_trg_view_20230808_v e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        UPDATE SET e.name = n.name, e.salary = n.salary
    WHEN NOT MATCHED THEN
        INSERT (id, name, salary) VALUES (n.id, n.name, n.salary);
drop TRIGGER ins_of_upins_trg_20230808 on merge_trg_view_20230808_v;
-- error: test instead of delete trigger on view
CREATE TRIGGER ins_of_del_trg_20230808
    INSTEAD OF DELETE ON merge_trg_view_20230808_v
    FOR EACH ROW
    EXECUTE FUNCTION del_trg_20230808_f();
MERGE INTO merge_trg_view_20230808_v e
    USING new_merge_trg_rel_20230808 n
    ON (e.id = n.id)
    WHEN MATCHED THEN
        update set e.name = n.name, e.salary = n.salary delete where e.id = 1;
drop TRIGGER ins_of_del_trg_20230808 on merge_trg_view_20230808_v;
drop view merge_trg_view_20230808_v;
drop table merge_trg_rel_20230808;
drop table new_merge_trg_rel_20230808;
drop table del_merge_trg_rel_20230808;
drop function upd_ins_trg_20230808_f;
drop function del_trg_20230808_f;
reset enable_distribute_update;

\c regression
drop table if exists mt1;
drop table if exists mst1;
drop table if exists mst2;

create table mt1(c1 int, c2 int, c3 int[]);
create table mst1(c1 int, c2 int, c3 int[]);
create table mst2(c1 int, c2 int, c3 int[]);
insert into mst2 values(1,1,'{1,2}'::int[]);
insert into mst2 values(2,2,'{3,4}'::int[]);
insert into mst1 values(1,1,'{1,2}'::int[]);
insert into mst1 values(2,2,'{4,5}'::int[]);

explain (costs off)
merge into mt1 using mst1 on mt1.c2=mst1.c2 when not matched and cast(nullif((select max(c3) from mst2), cast(null as _int4)) as _int4) >= (select min(c3) from mst1) then insert values(c1, c2, c3);

merge into mt1 using mst1 on mt1.c2=mst1.c2 when not matched and cast(nullif((select max(c3) from mst2), cast(null as _int4)) as _int4) >= (select min(c3) from mst1) then insert values(c1, c2, c3);
select * from mt1;

drop table mt1;
drop table mst1;
drop table mst2;
set enable_fast_query_shipping=off;
drop table source1;
drop table mergetest1;
create table source1 (v1 varchar(20), v2 varchar(20), v3 varchar(20), v4 varchar(20), v5 varchar(20), v6 varchar(20));
insert into source1 values ('v','v','b','v','v','b');
create table mergetest1(v1 varchar(20), v2 varchar(20), v3 varchar(20), v4 varchar(20), v5 varchar(20));
merge into mergetest1 using source1 on mergetest1.v1=source1.v1 when matched then update set mergetest1.v2 = source1.v2 when not matched then insert values ('2','2','2','2','2');
alter table mergetest1 add column i1 varchar(20) default '44vvvv';
alter table mergetest1 add column i2 varchar(20) default '44vvvv';
alter table mergetest1 add column i3 varchar(20) default '44vvvv';
alter table mergetest1 add column i4 varchar(20) default '44vvv';
alter table mergetest1 add column i5 varchar(20) default '44vvv';
alter table mergetest1 add column i6 varchar(20) default '44vvv';
select * from mergetest1;

create extension if not exists pageinspect;
drop table source1;
drop table mergetest1;
create table source1 (v1 varchar(20), v2 varchar(20), v3 varchar(20), v4 varchar(20), v5 varchar(20), v6 varchar(20));
insert into source1 values ('v','v','b','v','v','b');
create table mergetest1(v1 varchar(20), v2 varchar(20), v3 varchar(20), v4 varchar(20), v5 varchar(20));
merge into mergetest1 using source1 on mergetest1.v1=source1.v1 when matched then update set mergetest1.v2 = source1.v2 when not matched then insert values ('2','2','2','2','2');
create table temp_check_merge(
    tab_name text,
    node_name text,
    m_ctid    tid,
    confirm   smallint
);
create or replace function check_merge_unavailable_data(tab_name text, invalid bool) returns setof temp_check_merge  as
$$
    declare
        r1 temp_check_merge%rowtype;
        nodes varchar(20)[];
        tmp_node varchar(20);
        sql_nodes text:='';
        sql_qblock text := '';
        sql_tmp text := '';
        page int;
    begin
        page = -1;
        sql_nodes = 'select node_name from pgxc_node where node_id in(select xc_node_id from ' || tab_name ||')';
        for tmp_node in execute sql_nodes
        loop
            sql_qblock = 'execute direct on (' || tmp_node || ') ''select distinct tp_arr[1] from (select pg_catalog.regexp_replace(pg_catalog.regexp_replace(ctid::text,''''\('''', ''''{''''), ''''\)'''',''''}'''')::int[] as tp_arr  from '|| tab_name || ') order by tp_arr[1];''';
            for page in execute sql_qblock
                    loop
                            if invalid then
                            sql_tmp = 'select * from store_invalid_data('''||tab_name||''','''||tmp_node||''','||page||')';
                            else
                            sql_tmp = 'select * from store_valid_data('''||tab_name||''','''||tmp_node||''','||page||')';
                            end if;
                            for r1 in execute sql_tmp
                                loop
                                    return next r1;
                                end loop;
                    end loop;
        end loop;
    end;
$$ language plpgsql;

create or replace function store_invalid_data(tab_name text, node_name text, page int) returns setof temp_check_merge as
$$
    declare
        r1 temp_check_merge%rowtype;
        sql_qattrnum text := '';
        sql_qvalid_tuple text := '';
        nattrs int;
        temp int;
        temp_ctid tid;
    begin
        sql_qattrnum  = 'select count(1) from pg_class, (select xmin,* from pg_attribute) pg_attribute  where relname ='''||tab_name||''' and pg_class.oid = pg_attribute.attrelid and attnum > 0;';

        for temp in execute sql_qattrnum
            loop
                nattrs = temp;
            end loop;
        --raise notice '%', nattrs;
        sql_qvalid_tuple = 'execute direct on ('||node_name||') ''select t_ctid as m_ctid from heap_page_items(get_raw_page(''''' ||tab_name||''''','||page||')) where (t_infomask2  & (16 ^ 3)::int)=0 and t_xmax=0 and ((t_infomask2 & 2047) - 5) > '||nattrs||''';';

        for temp_ctid in execute sql_qvalid_tuple
            loop
                r1.node_name = node_name;
                r1.tab_name=tab_name;
                r1.m_ctid = temp_ctid;
                r1.confirm = 0;
                return next r1;
            end loop;
    end;
$$ language plpgsql;

create or replace function store_valid_data(tab_name text, node_name text, page int) returns setof temp_check_merge as
$$
    declare
        r1 temp_check_merge%rowtype;
        sql_qattrnum text := '';
        sql_qvalid_tuple text := '';
        nattrs int;
        temp int;
        temp_ctid tid;
    begin
        sql_qattrnum  = 'select count(1) from pg_class, (select xmin,* from pg_attribute) pg_attribute  where relname ='''||tab_name||''' and pg_class.oid = pg_attribute.attrelid and attnum > 0;';

        for temp in execute sql_qattrnum
            loop
                nattrs = temp;
            end loop;
        --raise notice '%', nattrs;
        sql_qvalid_tuple = 'execute direct on ('||node_name||') ''select t_ctid as m_ctid from heap_page_items(get_raw_page(''''' ||tab_name||''''','||page||')) where (t_infomask2  & (16 ^ 3)::int)=0 and t_xmax=0 and ((t_infomask2 & 2047)) = '||nattrs||''';';

        for temp_ctid in execute sql_qvalid_tuple
            loop
                r1.node_name = node_name;
                r1.tab_name=tab_name;
                r1.m_ctid = temp_ctid;
                r1.confirm = 0;
                return next r1;
            end loop;
    end;
$$ language plpgsql;
select * from check_merge_unavailable_data('mergetest1', false);
drop function check_merge_unavailable_data;
drop function store_valid_data;
drop function store_invalid_data;
drop table temp_check_merge;
reset enable_fast_query_shipping;

drop table if exists mt1;
drop table if exists mt2;
drop table if exists mt3;
create table mt1(c0 int, c1 int, c2 int, c3 int);
create table mt2(c0 int, c1 int, c2 int, c3 int);
create table mt3(c0 int, c1 int, c2 int, c3 int);

insert into mt1 select n, n*10, n*100, n*1000 from generate_series(1, 10) n;
insert into mt2 select n, n*10, n*100, n*1000 from generate_series(5, 15) n;
insert into mt3 select n, n*10, n*100, n*1000 from generate_series(5, 10) n;

explain (costs off, nodes off)
merge into mt1 using (select c1,c2, c3 ,(select max(c3) from mt3 where mt3.c2=mt2.c2) cc3, (select max(c1) from mt3 where mt3.c2=mt2.c2) cc1 from mt2) mtt2 on mt1.c1 / 2 =mtt2.cc1 when matched and mtt2.cc1 < 100 then update set mt1.c2=mtt2.cc3 when not matched and mtt2.cc3 < 7000 then insert (c1, c3) values(mtt2.c1, mtt2.cc3 / 1000) when not matched and mtt2.cc3 < 9000 then insert (c1, c3) values(mtt2.c1, mtt2.cc3);

merge into mt1 using (select c1,c2, c3 ,(select max(c3) from mt3 where mt3.c2=mt2.c2) cc3, (select max(c1) from mt3 where mt3.c2=mt2.c2) cc1 from mt2) mtt2 on mt1.c1 / 2 =mtt2.cc1 when matched and mtt2.cc1 < 100 then update set mt1.c2=mtt2.cc3 when not matched and mtt2.cc3 < 7000 then insert (c1, c3) values(mtt2.c1, mtt2.cc3 / 1000) when not matched and mtt2.cc3 < 9000 then insert (c1, c3) values(mtt2.c1, mtt2.cc3);

select * from mt1 order by 1,2,3;
drop table if exists mt1;
drop table if exists mt2;
drop table if exists mt3;

drop table if exists mt2;
CREATE TABLE mt2 (
c0 int,
c1 int default null,
c2 varchar (2000),
c3 varchar (2000) default null,
c4 date,
c5 date default null,
c6 timestamp,
c7 timestamp default null,
c8 numeric,
c9 numeric default null)    PARTITION BY hash( c5) distribute by replication;
create TABLE mt2_p0 partition of mt2 for values with(modulus 5,remainder 0);
create TABLE mt2_p1 partition of mt2 for values with(modulus 5,remainder 1);
create TABLE mt2_p2 partition of mt2 for values with(modulus 5,remainder 2);
create TABLE mt2_p3 partition of mt2 for values with(modulus 5,remainder 3);
create TABLE mt2_p4 partition of mt2 for values with(modulus 5,remainder 4);
alter table mt2 alter column c0 drop not null;


drop table if exists mt1;
CREATE TABLE mt1 (
c0 int,
c1 int default null,
c2 varchar (2000),
c3 varchar (2000) default null,
c4 date,
c5 date default null,
c6 timestamp,
c7 timestamp default null,
c8 numeric,
c9 numeric default null)    PARTITION BY hash( c6) ;
create TABLE mt1_p0 partition of mt1 for values with(modulus 5,remainder 0);
create TABLE mt1_p1 partition of mt1 for values with(modulus 5,remainder 1);
create TABLE mt1_p2 partition of mt1 for values with(modulus 5,remainder 2);
create TABLE mt1_p3 partition of mt1 for values with(modulus 5,remainder 3);
create TABLE mt1_p4 partition of mt1 for values with(modulus 5,remainder 4);
alter table mt1 alter column c0 drop not null;

insert into mt2 select n, n, 'aa','bb', '2024-03-24'::date + INTERVAL '1 day', '2024-04-23'::date + INTERVAL '1 day' from generate_series(1, 10000) n;
analyze mt1;
analyze mt2;
begin;
explain (costs off, verbose)merge into mt1 t1 using mt2 t2 on ( ( NOT ( t1.c2 <= t1.c2 ) AND t1.c2 > 10 ) AND ( t1.c2 IS NULL ) AND t2.c0 = t1.c1 and t1.c0=t2.c0) when not matched and t2.c5 = t2.c4 then insert (c1) values(t2.c1);
merge into mt1 t1 using mt2 t2 on ( ( NOT ( t1.c2 <= t1.c2 ) AND t1.c2 > 10 ) AND ( t1.c2 IS NULL ) AND t2.c0 = t1.c1 and t1.c0=t2.c0) when not matched and t2.c5 = t2.c4 then insert (c1) values(t2.c1);
select * from mt1 order by 1;
rollback;
drop table if exists mt2;
drop table if exists mt1;

-- test merge into replication table
create table m_rep(i int, j int, k int) distribute by replication;
insert into m_rep select i,i,i from generate_series(1,100) t(i);
create table s_shard(i int, j int, k int) distribute by shard(i);
insert into s_shard select i,i,i from generate_series(50,150) t(i);
create table s_hash(i int, j int, k int) distribute by hash(i);
insert into s_hash select * from s_shard;
create table s_rep (i int, j int, k int) distribute by replication;
insert into s_rep select * from s_shard;

--step1 merge target is replication table,source table is shard
\set QUIET off
begin;
explain (verbose, costs off, nodes off, summary off)
merge into m_rep m using s_shard s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
merge into m_rep m using s_shard s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
select count(*) from m_rep;
select count(*) from m_rep where i=56;
select count(*) from m_rep where i = -1;
rollback;

--step2 merge target is replication table, source table is hash
begin;
explain (verbose, costs off, nodes off, summary off)
merge into m_rep m using s_hash s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
merge into m_rep m using s_hash s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
select count(*) from m_rep;
select count(*) from m_rep where i=56;
select count(*) from m_rep where i = -1;
rollback;

--step3 merge target is replication table, source table is replication
begin;
explain (verbose, costs off, nodes off, summary off)
merge into m_rep m using s_rep s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
merge into m_rep m using s_rep s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
select count(*) from m_rep;
select count(*) from m_rep where i=56;
select count(*) from m_rep where i = -1;
rollback;
\set QUIET on

--step4 merge target is replication partitioned table
create table m_rep_part(i int, j int, k int) partition by range(i) distribute by replication;
create table m_rep_part_1 partition of m_rep_part for values from (MINVALUE) to (30);
create table m_rep_part_2 partition of m_rep_part for values from (30) to (60);
create table m_rep_part_3 partition of m_rep_part for values from (60) to (MAXVALUE) partition by hash (i);
create table m_rep_part_3_1 partition of m_rep_part_3 for values with(modulus 3 ,remainder 0);
create table m_rep_part_3_2 partition of m_rep_part_3 for values with(modulus 3 ,remainder 1);
create table m_rep_part_3_3 partition of m_rep_part_3 for values with(modulus 3 ,remainder 2);
insert into m_rep_part select * from m_rep;

\set QUIET off
begin;
explain (verbose, costs off, nodes off, summary off)
merge into m_rep_part m using s_shard s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
merge into m_rep_part m using s_shard s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
select count(*) from m_rep_part;
select count(*) from m_rep_part where i=56;
select count(*) from m_rep_part where i = -1;
rollback;

begin;
explain (verbose, costs off, nodes off, summary off)
merge into m_rep_part m using s_hash s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
merge into m_rep_part m using s_hash s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
select count(*) from m_rep_part;
select count(*) from m_rep_part where i=56;
select count(*) from m_rep_part where i = -1;
rollback;

begin;
explain (verbose, costs off, nodes off, summary off)
merge into m_rep_part m using s_rep s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
merge into m_rep_part m using s_rep s on(m.i=s.i) when matched then update set m.j=s.j + 1,k=k+1 delete where s.i = 56 when not matched then insert values (-1,-1);
select count(*) from m_rep_part;
select count(*) from m_rep_part where i=56;
select count(*) from m_rep_part where i = -1;
rollback;
\set QUIET on
drop table m_rep_part cascade;
drop table m_rep;
drop table s_shard;
drop table s_hash;
drop table s_rep;

--test merge ambiguous colname
create table m_col(i int, j int, k int);
create table s_col(i int, j int, k int);
insert into m_col values (1,1,1);
insert into s_col values (1,10,10),(-1, -1, -1);

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1;
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 when not matched then insert values (-2,-2,-2);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 when not matched then insert values (s.i,s.j,-2);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 when not matched then insert values (s.i,-2,s.k);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 when not matched then insert values (s.i,s.j,s.k);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 where j=1 when not matched then insert values (s.i,s.j,s.k);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 where j=10 when not matched then insert values (s.i,s.j,s.k);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 where j=1 when not matched then insert values (s.i,-2,s.k);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 delete where j=2 when not matched then insert values (s.i,-2,s.k);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 delete where j=2 when not matched then insert values (s.i,s.j,s.k);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 delete where j=10 when not matched then insert values (s.i,s.j,s.k);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 when not matched then insert values (i,j,k);
select * from m_col;
rollback;

begin;
merge into m_col m using s_col s on (m.i=s.i) when matched then update set j=j+1,k=k+1 delete where j=10 when not matched then insert values (s.i,s.j,s.k) where i > 0;
select * from m_col;
rollback;

drop table m_col;
drop table s_col;

drop table if exists mgt1;
drop table if exists mgt2;
create table mgt1(c0 int, c1 int, c2 int);
create table mgt2(c0 int, c1 int, c2 int);
explain (costs off, nodes off) merge into mgt1 t1 using mgt2 t2 on t1.c1=t2.c1 
WHEN MATCHED THEN UPDATE SET c2 = t2.c2
WHEN NOT MATCHED THEN
INSERT DEFAULT VALUES;
drop table mgt1;
drop table mgt2;

drop table ttrep;
drop table tt1;
create sequence s1;
create table tt1 (t1 int, a int, b int) distribute by shard(t1);
insert into tt1 values (1,1,1);
insert into tt1 values (2,2,2);
insert into tt1 values (3,3,3);

create table ttrep (
a int default floor(random()*100) + floor(random()*100),
b int default (100 + floor(random()*100)),
c varchar(100) default floor(random()*100)::text) distribute by replication;

insert into ttrep(a, b) values (1, 1), (2, 2);

-- explain (verbose, costs off)
merge into ttrep using (select * from tt1 order by 1,2,3) tt1 on ttrep.a = tt1.t1
when matched then update set c = 'xixi_update'
when not matched then insert (a , c) values (tt1.a * 10000, 'xixi_insert');
drop table ttrep;
drop table tt1;
drop sequence s1;
