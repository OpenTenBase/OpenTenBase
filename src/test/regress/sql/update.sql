--
-- UPDATE syntax tests
--

CREATE TABLE update_test (
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
) DISTRIBUTE BY REPLICATION;

CREATE TABLE upsert_test (
    a   INT PRIMARY KEY,
    b   TEXT
) DISTRIBUTE BY REPLICATION;

INSERT INTO update_test VALUES (5, 10, 'foo');
INSERT INTO update_test(b, a) VALUES (15, 10);

SELECT * FROM update_test ORDER BY a, b, c;

UPDATE update_test SET a = DEFAULT, b = DEFAULT;

SELECT * FROM update_test  ORDER BY a, b, c;

-- aliases for the UPDATE target table
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;

SELECT * FROM update_test  ORDER BY a, b, c;

UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;

SELECT * FROM update_test  ORDER BY a, b, c;

--
-- Test VALUES in FROM
--

UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

SELECT * FROM update_test  ORDER BY a, b, c;

-- fail, wrong data type:
UPDATE update_test SET a = v.* FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--
-- Test multiple-set-clause syntax
--

INSERT INTO update_test SELECT a,b+1,c FROM update_test;
SELECT * FROM update_test;

UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';
SELECT * FROM update_test  ORDER BY a, b, c;
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;
SELECT * FROM update_test  ORDER BY a, b, c;
-- fail, multi assignment to same column:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;

-- uncorrelated sub-select:
UPDATE update_test
  SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
  WHERE a = 100 AND b = 20;
SELECT * FROM update_test order by 1;
-- correlated sub-select:
UPDATE update_test o
  SET (b,a) = (select a+1,b from update_test i
               where i.a=o.a and i.b=o.b and i.c is not distinct from o.c);
SELECT * FROM update_test order by 1;
-- fail, multiple rows supplied:
UPDATE update_test SET (b,a) = (select a+1,b from update_test);
-- set to null if no rows supplied:
UPDATE update_test SET (b,a) = (select a+1,b from update_test where a = 1000)
  WHERE a = 11;
SELECT * FROM update_test order by 1;
-- *-expansion should work in this context:
UPDATE update_test SET (a,b) = ROW(v.*) FROM (VALUES(21, 100)) AS v(i, j)
  WHERE update_test.a = v.i;
-- you might expect this to work, but syntactically it's not a RowExpr:
UPDATE update_test SET (a,b) = (v.*) FROM (VALUES(21, 101)) AS v(i, j)
  WHERE update_test.a = v.i;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;

-- Make sure that we can update to a TOASTed value.
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';
SELECT a, b, char_length(c) FROM update_test ORDER BY a;

-- Check multi-assignment with a Result node to handle a one-time filter.
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
SELECT a, b, char_length(c) FROM update_test;

-- Test ON CONFLICT DO UPDATE
INSERT INTO upsert_test VALUES(1, 'Boo');
-- uncorrelated  sub-select:
WITH aaa AS (SELECT 1 AS a, 'Foo' AS b) INSERT INTO upsert_test
  VALUES (1, 'Bar') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b, a FROM aaa) RETURNING *;
-- correlated sub-select:
INSERT INTO upsert_test VALUES (1, 'Baz') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Correlated', a from upsert_test i WHERE i.a = upsert_test.a)
  RETURNING *;
-- correlated sub-select (EXCLUDED.* alias):
INSERT INTO upsert_test VALUES (1, 'Bat') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING *;

DROP TABLE update_test;
DROP TABLE upsert_test;


---------------------------
-- UPDATE with row movement
---------------------------

-- When a partitioned table receives an UPDATE to the partitioned key and the
-- new values no longer meet the partition's bound, the row must be moved to
-- the correct partition for the new partition key (if one exists). We must
-- also ensure that updatable views on partitioned tables properly enforce any
-- WITH CHECK OPTION that is defined. The situation with triggers in this case
-- also requires thorough testing as partition key updates causing row
-- movement convert UPDATEs into DELETE+INSERT.

CREATE TABLE range_parted (
	a text,
	b bigint,
	c numeric,
	d int,
	e varchar
) PARTITION BY RANGE (a, b);

-- Create partitions intentionally in descending bound order, so as to test
-- that update-row-movement works with the leaf partitions not in bound order.
CREATE TABLE part_b_20_b_30 (a text, b bigint, c numeric, d int, e varchar);
ALTER TABLE range_parted ATTACH PARTITION part_b_20_b_30 FOR VALUES FROM ('b', 20) TO ('b', 30);
CREATE TABLE part_b_10_b_20 (a text, b bigint, c numeric, d int, e varchar) PARTITION BY RANGE (c);
CREATE TABLE part_b_1_b_10 PARTITION OF range_parted FOR VALUES FROM ('b', 1) TO ('b', 10);
ALTER TABLE range_parted ATTACH PARTITION part_b_10_b_20 FOR VALUES FROM ('b', 10) TO ('b', 20);
CREATE TABLE part_a_10_a_20 PARTITION OF range_parted FOR VALUES FROM ('a', 10) TO ('a', 20);
CREATE TABLE part_a_1_a_10 PARTITION OF range_parted FOR VALUES FROM ('a', 1) TO ('a', 10);

-- Check that partition-key UPDATE works sanely on a partitioned table that
-- does not have any child partitions.
UPDATE part_b_10_b_20 set b = b - 6;

-- Create some more partitions following the above pattern of descending bound
-- order, but let's make the situation a bit more complex by having the
-- attribute numbers of the columns vary from their parent partition.
CREATE TABLE part_c_100_200 (a text, b bigint, c numeric, d int, e varchar) PARTITION BY range (abs(d));
CREATE TABLE part_d_1_15 PARTITION OF part_c_100_200 FOR VALUES FROM (1) TO (15);
CREATE TABLE part_d_15_20 PARTITION OF part_c_100_200 FOR VALUES FROM (15) TO (20);

ALTER TABLE part_b_10_b_20 ATTACH PARTITION part_c_100_200 FOR VALUES FROM (100) TO (200);

CREATE TABLE part_c_1_100 (a text, b bigint, c numeric, d int, e varchar);
ALTER TABLE part_b_10_b_20 ATTACH PARTITION part_c_1_100 FOR VALUES FROM (1) TO (100);

\set init_range_parted 'truncate range_parted; insert into range_parted VALUES (''a'', 1, 1, 1), (''a'', 10, 200, 1), (''b'', 12, 96, 1), (''b'', 13, 97, 2), (''b'', 15, 105, 16), (''b'', 17, 105, 19)'
\set show_data 'select tableoid::regclass::text COLLATE "C" partname, * from range_parted ORDER BY 1, 2, 3, 4, 5, 6'
:init_range_parted;
:show_data;

-- The order of subplans should be in bound order
EXPLAIN (costs off) UPDATE range_parted set c = c - 50 WHERE c > 97;

-- fail, row movement happens only within the partition subtree.
UPDATE part_c_100_200 set c = c - 20, d = c WHERE c = 105;
-- fail, no partition key update, so no attempt to move tuple,
-- but "a = 'a'" violates partition constraint enforced by root partition)
UPDATE part_b_10_b_20 set a = 'a';
-- ok, partition key update, no constraint violation
UPDATE range_parted set d = d - 10 WHERE d > 10;
-- ok, no partition key update, no constraint violation
UPDATE range_parted set e = d;
-- No row found
UPDATE part_c_1_100 set c = c + 20 WHERE c = 98;
:show_data;

-- Common table needed for multiple test scenarios.
CREATE TABLE mintab(c1 int);
INSERT into mintab VALUES (120);

-- update partition key using updatable view.
CREATE VIEW upview AS SELECT * FROM range_parted WHERE (select c > c1 FROM mintab) WITH CHECK OPTION;
-- ok
UPDATE upview set c = 199 WHERE b = 4;
-- fail, check option violation
UPDATE upview set c = 120 WHERE b = 4;
-- fail, row movement with check option violation
UPDATE upview set a = 'b', b = 15, c = 120 WHERE b = 4;
-- ok, row movement, check option passes
UPDATE upview set a = 'b', b = 15 WHERE b = 4;

:show_data;

-- cleanup
DROP VIEW upview;

-- RETURNING having whole-row vars.
:init_range_parted;
UPDATE range_parted set c = 95 WHERE a = 'b' and b > 10 and c < 100 returning (range_parted), *;
:show_data;


-- Transition tables with update row movement
:init_range_parted;

CREATE FUNCTION trans_updatetrigfunc() RETURNS trigger LANGUAGE plpgsql AS
$$
  begin
    raise notice 'trigger = %, old table = %, new table = %',
                 TG_NAME,
                 (select string_agg(old_table::text, ', ' ORDER BY a) FROM old_table),
                 (select string_agg(new_table::text, ', ' ORDER BY a) FROM new_table);
    return null;
  end;
$$;

CREATE TRIGGER trans_updatetrig
  AFTER UPDATE ON range_parted REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
  FOR EACH STATEMENT EXECUTE PROCEDURE trans_updatetrigfunc();

UPDATE range_parted set c = (case when c = 96 then 110 else c + 1 end ) WHERE a = 'b' and b > 10 and c >= 96;
:show_data;
:init_range_parted;

-- Enabling OLD TABLE capture for both DELETE as well as UPDATE stmt triggers
-- should not cause DELETEd rows to be captured twice. Similar thing for
-- INSERT triggers and inserted rows.
CREATE TRIGGER trans_deletetrig
  AFTER DELETE ON range_parted REFERENCING OLD TABLE AS old_table
  FOR EACH STATEMENT EXECUTE PROCEDURE trans_updatetrigfunc();
CREATE TRIGGER trans_inserttrig
  AFTER INSERT ON range_parted REFERENCING NEW TABLE AS new_table
  FOR EACH STATEMENT EXECUTE PROCEDURE trans_updatetrigfunc();
UPDATE range_parted set c = c + 50 WHERE a = 'b' and b > 10 and c >= 96;
:show_data;
DROP TRIGGER trans_deletetrig ON range_parted;
DROP TRIGGER trans_inserttrig ON range_parted;
-- Don't drop trans_updatetrig yet. It is required below.

-- Test with transition tuple conversion happening for rows moved into the
-- new partition. This requires a trigger that references transition table
-- (we already have trans_updatetrig). For inserted rows, the conversion
-- is not usually needed, because the original tuple is already compatible with
-- the desired transition tuple format. But conversion happens when there is a
-- BR trigger because the trigger can change the inserted row. So install a
-- BR triggers on those child partitions where the rows will be moved.
CREATE FUNCTION func_parted_mod_b() RETURNS trigger AS $$
BEGIN
   NEW.b = NEW.b + 1;
   return NEW;
END $$ language plpgsql;
CREATE TRIGGER trig_c1_100 BEFORE UPDATE OR INSERT ON part_c_1_100
   FOR EACH ROW EXECUTE PROCEDURE func_parted_mod_b();
CREATE TRIGGER trig_d1_15 BEFORE UPDATE OR INSERT ON part_d_1_15
   FOR EACH ROW EXECUTE PROCEDURE func_parted_mod_b();
CREATE TRIGGER trig_d15_20 BEFORE UPDATE OR INSERT ON part_d_15_20
   FOR EACH ROW EXECUTE PROCEDURE func_parted_mod_b();
:init_range_parted;
UPDATE range_parted set c = (case when c = 96 then 110 else c + 1 end) WHERE a = 'b' and b > 10 and c >= 96;
:show_data;
:init_range_parted;
UPDATE range_parted set c = c + 50 WHERE a = 'b' and b > 10 and c >= 96;
:show_data;

-- Case where per-partition tuple conversion map array is allocated, but the
-- map is not required for the particular tuple that is routed, thanks to
-- matching table attributes of the partition and the target table.
:init_range_parted;
UPDATE range_parted set b = 15 WHERE b = 1;
:show_data;

DROP TRIGGER trans_updatetrig ON range_parted;
DROP TRIGGER trig_c1_100 ON part_c_1_100;
DROP TRIGGER trig_d1_15 ON part_d_1_15;
DROP TRIGGER trig_d15_20 ON part_d_15_20;
DROP FUNCTION func_parted_mod_b();

-- RLS policies with update-row-movement
-----------------------------------------

ALTER TABLE range_parted ENABLE ROW LEVEL SECURITY;
CREATE USER regress_range_parted_user;
GRANT ALL ON range_parted, mintab TO regress_range_parted_user;
CREATE POLICY seeall ON range_parted AS PERMISSIVE FOR SELECT USING (true);
CREATE POLICY policy_range_parted ON range_parted for UPDATE USING (true) WITH CHECK (c % 2 = 0);

:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
-- This should fail with RLS violation error while moving row from
-- part_a_10_a_20 to part_d_1_15, because we are setting 'c' to an odd number.
UPDATE range_parted set a = 'b', c = 151 WHERE a = 'a' and c = 200;

RESET SESSION AUTHORIZATION;
-- Create a trigger on part_d_1_15
CREATE FUNCTION func_d_1_15() RETURNS trigger AS $$
BEGIN
   NEW.c = NEW.c + 1; -- Make even numbers odd, or vice versa
   return NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER trig_d_1_15 BEFORE INSERT ON part_d_1_15
   FOR EACH ROW EXECUTE PROCEDURE func_d_1_15();

:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;

-- Here, RLS checks should succeed while moving row from part_a_10_a_20 to
-- part_d_1_15. Even though the UPDATE is setting 'c' to an odd number, the
-- trigger at the destination partition again makes it an even number.
UPDATE range_parted set a = 'b', c = 151 WHERE a = 'a' and c = 200;

RESET SESSION AUTHORIZATION;
:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
-- This should fail with RLS violation error. Even though the UPDATE is setting
-- 'c' to an even number, the trigger at the destination partition again makes
-- it an odd number.
UPDATE range_parted set a = 'b', c = 150 WHERE a = 'a' and c = 200;

-- Cleanup
RESET SESSION AUTHORIZATION;
DROP TRIGGER trig_d_1_15 ON part_d_1_15;
DROP FUNCTION func_d_1_15();

-- Policy expression contains SubPlan
RESET SESSION AUTHORIZATION;
:init_range_parted;
CREATE POLICY policy_range_parted_subplan on range_parted
    AS RESTRICTIVE for UPDATE USING (true)
    WITH CHECK ((SELECT range_parted.c <= c1 FROM mintab));
SET SESSION AUTHORIZATION regress_range_parted_user;
-- fail, mintab has row with c1 = 120
UPDATE range_parted set a = 'b', c = 122 WHERE a = 'a' and c = 200;
-- ok
UPDATE range_parted set a = 'b', c = 120 WHERE a = 'a' and c = 200;

-- RLS policy expression contains whole row.

RESET SESSION AUTHORIZATION;
:init_range_parted;
CREATE POLICY policy_range_parted_wholerow on range_parted AS RESTRICTIVE for UPDATE USING (true)
   WITH CHECK (range_parted = row('b', 10, 112, 1, NULL)::range_parted);
SET SESSION AUTHORIZATION regress_range_parted_user;
-- ok, should pass the RLS check
UPDATE range_parted set a = 'b', c = 112 WHERE a = 'a' and c = 200;
RESET SESSION AUTHORIZATION;
:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
-- fail, the whole row RLS check should fail
UPDATE range_parted set a = 'b', c = 116 WHERE a = 'a' and c = 200;

-- Cleanup
RESET SESSION AUTHORIZATION;
DROP POLICY policy_range_parted ON range_parted;
DROP POLICY policy_range_parted_subplan ON range_parted;
DROP POLICY policy_range_parted_wholerow ON range_parted;
REVOKE ALL ON range_parted, mintab FROM regress_range_parted_user;
DROP USER regress_range_parted_user;
DROP TABLE mintab;


-- statement triggers with update row movement
---------------------------------------------------

:init_range_parted;

CREATE FUNCTION trigfunc() returns trigger language plpgsql as
$$
  begin
    raise notice 'trigger = % fired on table % during %',
                 TG_NAME, TG_TABLE_NAME, TG_OP;
    return null;
  end;
$$;
-- Triggers on root partition
CREATE TRIGGER parent_delete_trig
  AFTER DELETE ON range_parted for each statement execute procedure trigfunc();
CREATE TRIGGER parent_update_trig
  AFTER UPDATE ON range_parted for each statement execute procedure trigfunc();
CREATE TRIGGER parent_insert_trig
  AFTER INSERT ON range_parted for each statement execute procedure trigfunc();

-- Triggers on leaf partition part_c_1_100
CREATE TRIGGER c1_delete_trig
  AFTER DELETE ON part_c_1_100 for each statement execute procedure trigfunc();
CREATE TRIGGER c1_update_trig
  AFTER UPDATE ON part_c_1_100 for each statement execute procedure trigfunc();
CREATE TRIGGER c1_insert_trig
  AFTER INSERT ON part_c_1_100 for each statement execute procedure trigfunc();

-- Triggers on leaf partition part_d_1_15
CREATE TRIGGER d1_delete_trig
  AFTER DELETE ON part_d_1_15 for each statement execute procedure trigfunc();
CREATE TRIGGER d1_update_trig
  AFTER UPDATE ON part_d_1_15 for each statement execute procedure trigfunc();
CREATE TRIGGER d1_insert_trig
  AFTER INSERT ON part_d_1_15 for each statement execute procedure trigfunc();
-- Triggers on leaf partition part_d_15_20
CREATE TRIGGER d15_delete_trig
  AFTER DELETE ON part_d_15_20 for each statement execute procedure trigfunc();
CREATE TRIGGER d15_update_trig
  AFTER UPDATE ON part_d_15_20 for each statement execute procedure trigfunc();
CREATE TRIGGER d15_insert_trig
  AFTER INSERT ON part_d_15_20 for each statement execute procedure trigfunc();

-- Move all rows from part_c_100_200 to part_c_1_100. None of the delete or
-- insert statement triggers should be fired.
UPDATE range_parted set c = c - 50 WHERE c > 97;
:show_data;

DROP TRIGGER parent_delete_trig ON range_parted;
DROP TRIGGER parent_update_trig ON range_parted;
DROP TRIGGER parent_insert_trig ON range_parted;
DROP TRIGGER c1_delete_trig ON part_c_1_100;
DROP TRIGGER c1_update_trig ON part_c_1_100;
DROP TRIGGER c1_insert_trig ON part_c_1_100;
DROP TRIGGER d1_delete_trig ON part_d_1_15;
DROP TRIGGER d1_update_trig ON part_d_1_15;
DROP TRIGGER d1_insert_trig ON part_d_1_15;
DROP TRIGGER d15_delete_trig ON part_d_15_20;
DROP TRIGGER d15_update_trig ON part_d_15_20;
DROP TRIGGER d15_insert_trig ON part_d_15_20;


-- Creating default partition for range
:init_range_parted;
create table part_def partition of range_parted default;
\d+ part_def
insert into range_parted values ('c', 9);
-- ok
update part_def set a = 'd' where a = 'c';
-- fail
update part_def set a = 'a' where a = 'd';

:show_data;

-- Update row movement from non-default to default partition.
-- fail, default partition is not under part_a_10_a_20;
UPDATE part_a_10_a_20 set a = 'ad' WHERE a = 'a';
-- ok
UPDATE range_parted set a = 'ad' WHERE a = 'a';
UPDATE range_parted set a = 'bd' WHERE a = 'b';
:show_data;
-- Update row movement from default to non-default partitions.
-- ok
UPDATE range_parted set a = 'a' WHERE a = 'ad';
UPDATE range_parted set a = 'b' WHERE a = 'bd';
:show_data;

-- Cleanup: range_parted no longer needed.
DROP TABLE range_parted;

CREATE TABLE list_parted (
	a text,
	b int
) PARTITION BY list (a);
CREATE TABLE list_part1  PARTITION OF list_parted for VALUES in ('a', 'b');
CREATE TABLE list_default PARTITION OF list_parted default;
INSERT into list_part1 VALUES ('a', 1);
INSERT into list_default VALUES ('d', 10);

-- fail
UPDATE list_default set a = 'a' WHERE a = 'd';
-- ok
UPDATE list_default set a = 'x' WHERE a = 'd';

DROP TABLE list_parted;

--------------
-- Some more update-partition-key test scenarios below. This time use list
-- partitions.
--------------

-- Setup for list partitions
CREATE TABLE list_parted (a numeric, b int, c int8) PARTITION BY list (a);
CREATE TABLE sub_parted PARTITION OF list_parted for VALUES in (1) PARTITION BY list (b);

CREATE TABLE sub_part1(b int, c int8, a numeric);
ALTER TABLE sub_parted ATTACH PARTITION sub_part1 for VALUES in (1);
CREATE TABLE sub_part2(b int, c int8, a numeric);
ALTER TABLE sub_parted ATTACH PARTITION sub_part2 for VALUES in (2);

CREATE TABLE list_part1(a numeric, b int, c int8);
ALTER TABLE list_parted ATTACH PARTITION list_part1 for VALUES in (2,3);

INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (3,6,60);
INSERT into sub_parted VALUES (1,1,60);
INSERT into sub_parted VALUES (1,2,10);

-- Test partition constraint violation when intermediate ancestor is used and
-- constraint is inherited from upper root.
UPDATE sub_parted set a = 2 WHERE c = 10;

-- Test update-partition-key, where the unpruned partitions do not have their
-- partition keys updated.
SELECT tableoid::regclass::text, * FROM list_parted WHERE a = 2 ORDER BY 1;
UPDATE list_parted set b = c + a WHERE a = 2;
SELECT tableoid::regclass::text, * FROM list_parted WHERE a = 2 ORDER BY 1;


-- Test the case where BR UPDATE triggers change the partition key.
CREATE FUNCTION func_parted_mod_b() returns trigger as $$
BEGIN
   NEW.b = 2; -- This is changing partition key column.
   return NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER parted_mod_b before update on sub_part1
   for each row execute procedure func_parted_mod_b();

SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;

-- This should do the tuple routing even though there is no explicit
-- partition-key update, because there is a trigger on sub_part1.
UPDATE list_parted set c = 70 WHERE b  = 1;
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;

DROP TRIGGER parted_mod_b ON sub_part1;

-- If BR DELETE trigger prevented DELETE from happening, we should also skip
-- the INSERT if that delete is part of UPDATE=>DELETE+INSERT.
CREATE OR REPLACE FUNCTION func_parted_mod_b() returns trigger as $$
BEGIN
   raise notice 'Trigger: Got OLD row %, but returning NULL', OLD;
   return NULL;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER trig_skip_delete before delete on sub_part2
   for each row execute procedure func_parted_mod_b();
UPDATE list_parted set b = 1 WHERE c = 70;
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;
-- Drop the trigger. Now the row should be moved.
DROP TRIGGER trig_skip_delete ON sub_part2;
UPDATE list_parted set b = 1 WHERE c = 70;
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;
DROP FUNCTION func_parted_mod_b();

-- UPDATE partition-key with FROM clause. If join produces multiple output
-- rows for the same row to be modified, we should tuple-route the row only
-- once. There should not be any rows inserted.
CREATE TABLE non_parted (id int);
INSERT into non_parted VALUES (1), (1), (1), (2), (2), (2), (3), (3), (3);
UPDATE list_parted t1 set a = 2 FROM non_parted t2 WHERE t1.a = t2.id and a = 1;
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;
DROP TABLE non_parted;

-- Cleanup: list_parted no longer needed.
DROP TABLE list_parted;

-- create custom operator class and hash function, for the same reason
-- explained in alter_table.sql
create or replace function dummy_hashint4(a int4, seed int8) returns int8 as
$$ begin return (a + seed); end; $$ language 'plpgsql' immutable;
create operator class custom_opclass for type int4 using hash as
operator 1 = , function 2 dummy_hashint4(int4, int8);

create table hash_parted (
	a int,
	b int
) partition by hash (a custom_opclass, b custom_opclass);
create table hpart1 partition of hash_parted for values with (modulus 2, remainder 1);
create table hpart2 partition of hash_parted for values with (modulus 4, remainder 2);
create table hpart3 partition of hash_parted for values with (modulus 8, remainder 0);
create table hpart4 partition of hash_parted for values with (modulus 8, remainder 4);
insert into hpart1 values (1, 1);
insert into hpart2 values (2, 5);
insert into hpart4 values (3, 4);

-- fail
update hpart1 set a = 3, b=4 where a = 1;
-- ok, row movement
update hash_parted set b = b - 1 where b = 1;
-- ok
update hash_parted set b = b + 8 where b = 1;

-- cleanup
drop table hash_parted;
drop operator class custom_opclass using hash;
drop function dummy_hashint4(a int4, seed int8);

-- test update pull up
create table test1 (                                         
    c11 integer,                                           
    c12 integer,                                                 
    c13 integer,                                                  
    c14 integer,                                                     
    c15 integer                                                       
);

create table test2 (                                         
    c21 integer,                                           
    c22 integer,                                                 
    c23 integer,                                                  
    c24 integer,                                                     
    c25 integer                                                       
);


create table test3 (                                         
    c31 bigint,                                           
    c32 bigint,                                                 
    c33 bigint,                                                  
    c34 bigint,                                                     
    c35 bigint                                                       
);


create table test4 (                                         
    c41 char(10),                                           
    c42 char(10),                                                 
    c43 char(10),                                                  
    c44 char(10),                                                     
    c45 char(10)                                                       
);

create table test5 (
    c51 char(10),
    c52 char(20),
    c53 char(30),
    c54 char(40),
    c55 char(50)
);

insert into test1
select  i,i,i,i,i
from generate_series(1, 10) as i;

insert into test2
select  i,i,i+1,i,i
from generate_series(1, 10) as i;

insert into test3
select  i,i,i+1,i,i
from generate_series(1, 10) as i;

insert into test4
select  i,i,i+1,i,i
from generate_series(1, 10) as i;

insert into test5
select  i,i,i+1,i,i
from generate_series(1, 10) as i;

explain (costs off) update test1
set c13=(select test2.c23 from test2
         where test1.c12=test2.c22
           and exists(select 1 from test3
                      where c32=c22));
                      
explain (costs off) update test1
set c13=(select (select 1 from test3 where c32=c22)
         from test2
         where test1.c12=test2.c22);

explain (costs off) update test1
set c13=(select c22
         from (select * from test2 where c12=c22) t2
         where test1.c12=t2.c22);

explain (costs off) update test1
set (c13, c14)= (select c23, c24
                 from test2 where c12=c22);

explain (costs off) update test1
set (c13)= (select c23
            from test2 where c12=c22);

explain (costs off) update test3
set (c33, c34)= (select c23, c24
                 from test2 where c32=c22);

explain (costs off) update test1
set (c12)=(select c22 from test2 where c13=c23), 
    (c14)=(select c24 from test2 where c13=c23);

explain (costs off) update test1
set (c12)=(select c22 from test2 where c13=c23),
    (c14,c15)=(select c24,c25 from test2 where c13=c23);

explain (costs off) update test1
set (c14,c15)=(select c24,c25 from test2 where c13=c23),
    (c12)=(select c22 from test2);

explain (costs off, nodes off)
update test4
set c42=(select c52||'u' from test5
                where c43=c53);

explain (costs off, nodes off)
update test4
set c42=(select c54 from test5
                where c43=c53);

explain (costs off, nodes off)
update test4
set (c42)=(select c52||'u' from test5
                where c43=c53);

explain (costs off, nodes off)
update test4
set (c42, c44)=(select c52||'u', c55 from test5
                where c43=c53);

explain (costs off, nodes off)
update test4
set (c42, c44)=(select c52||c53, c55 from test5
                where c43=c53);

explain (costs off) update test1
set c13=(select c13
         from (select * from test2 where c12=c22) t2
         where test1.c12=t2.c22);

explain (costs off) update test1
set (c13, c14)= (select c13, c24
                 from test2 where c12=c22);

drop table test1;
drop table test2;
drop table test3;
drop table test4;
drop table test5;

create table t1(f11 int, f12 int) distribute by replication;
create table t2(f21 int, f22 int);

explain (costs off) update t1 set f11=t2.f21 from t2 where f11=t2.f21;
explain (costs off) update t2 set f22=t1.f12 from t1 where f21=t1.f11;

drop table t1;
drop table t2;

DROP TABLE IF EXISTS test11;
create table test11(id int primary key, f2 int, name varchar(10)) distribute by shard(id);
insert into test11 values(3, 2, 'test1');

DROP TABLE IF EXISTS test22;
create table test22(id int, f2 int primary key, name varchar(10)) distribute by shard(f2);
insert into test22 values(2, 3, 'test2');

EXPLAIN (VERBOSE on, COSTS off)
MERGE INTO test11 t
USING (
select * from test22
) t2 ON (t.id = t2.f2)
WHEN MATCHED THEN UPDATE SET t.name = t2.name;

select * from test11 order by id;
MERGE INTO test11 t
USING (
select * from test22
) t2 ON (t.id = t2.f2)
WHEN MATCHED THEN UPDATE SET t.name = t2.name;
select * from test11 order by id;

DROP TABLE IF EXISTS test11;
create table test11(id int primary key, f2 int, name varchar(10)) distribute by shard(id);
insert into test11 values(2, 2, 'test1');
insert into test11 values(3, 2, 'test1');

DROP TABLE IF EXISTS test22;
create table test22(id int, f2 int primary key, name varchar(10)) distribute by shard(f2);
insert into test22 values(2, 3, 'test2');

EXPLAIN (VERBOSE on, COSTS off)
MERGE INTO test11 t
USING (
select * from test22
) t2 ON (t.id = t2.f2)
WHEN MATCHED THEN UPDATE SET t.name = t2.name
WHEN NOT MATCHED THEN INSERT VALUES(t2.id, t2.f2, t2.name);

select * from test11 order by id;
MERGE INTO test11 t
USING (
select * from test22
) t2 ON (t.id = t2.f2)
WHEN MATCHED THEN UPDATE SET t.name = t2.name
WHEN NOT MATCHED THEN INSERT VALUES(t2.id, t2.f2, t2.name);
select * from test11 order by id;

DROP TABLE IF EXISTS test11;
create table test11(id int primary key, f2 int, name varchar(10)) distribute by shard(id);
insert into test11 values(1, 1, 'test1');
insert into test11 values(3, 2, 'test1');

DROP TABLE IF EXISTS test22;
create table test22(id int, f2 int primary key, name varchar(10)) distribute by shard(f2);
insert into test22 values(0, 0, 'test2');
insert into test22 values(2, 3, 'test2');

EXPLAIN (VERBOSE on, COSTS off)
MERGE INTO test11 t
USING (
select * from test22
) t2 ON (t.id = t2.f2)
WHEN MATCHED THEN UPDATE SET t.name = t2.name 
WHEN NOT MATCHED THEN INSERT VALUES(id + 2, f2, name);

MERGE INTO test11 t
USING (
select * from test22
) t2 ON (t.id = t2.f2)
WHEN MATCHED THEN UPDATE SET t.name = t2.name 
WHEN NOT MATCHED THEN INSERT VALUES(id + 2, f2, name);
select * from test11 order by id;

DROP TABLE IF EXISTS test11;
DROP TABLE IF EXISTS test22;

------------------------
-- Test combocid bug
------------------------
CREATE TABLE exchange20230221(a int, b int, c int) DISTRIBUTE BY SHARD(a);

BEGIN;
    INSERT INTO exchange20230221 VALUES(101, 0, 1);
    INSERT INTO exchange20230221 VALUES(102, 0, 1);
    INSERT INTO exchange20230221 VALUES(103, 0, 1);
    INSERT INTO exchange20230221 VALUES(104, 0, 1);

    UPDATE exchange20230221 tt SET b =
    (
        SELECT SUM(C) FROM exchange20230221 WHERE tt.a >= exchange20230221.b
    );

    SELECT CTID, * FROM exchange20230221 ORDER BY a;

    UPDATE exchange20230221 tt SET b =
    (
        SELECT SUM(C) FROM exchange20230221 WHERE tt.a >= exchange20230221.b
    );

    SELECT CTID, * FROM exchange20230221 ORDER BY a;


    UPDATE exchange20230221 tt SET b =
    (
        SELECT SUM(C) FROM exchange20230221 WHERE tt.a >= exchange20230221.b
    );

    SELECT CTID, * FROM exchange20230221 ORDER BY a;

END;

SELECT CTID, * FROM exchange20230221 ORDER BY A;

EXPLAIN UPDATE exchange20230221 TT SET b =
(
        SELECT SUM(C) FROM exchange20230221 WHERE tt.a >= exchange20230221.b
);

drop table if exists t1 cascade;
CREATE TABLE IF NOT EXISTS t1(c0 boolean , c1 boolean  PRIMARY KEY);
INSERT INTO t1(c0, c1) VALUES(TRUE, FALSE);
-- need report error, distributed column can't be updated
UPDATE t1 SET c1=(t1.c0);
drop table t1;

CREATE TABLE numeric_t1 (
    c0 int,
    c1 int,
    c2 text,
    c3 text,
    c4 date,
    c5 date,
    c6 timestamp,
    c7 timestamp,
    c8 numeric,
    c9 numeric) PARTITION BY hash(c1) ;
create TABLE numeric_t1_p0 partition of numeric_t1 for values with(modulus 2,remainder 0);
create TABLE numeric_t1_p1 partition of numeric_t1 for values with(modulus 2,remainder 1);
alter table numeric_t1 alter column c0 drop not null;
CREATE INDEX idx_numeric_t1_c1 ON numeric_t1(c1);
CREATE INDEX idx_numeric_t1_c3 ON numeric_t1(c3);
CREATE INDEX idx_numeric_t1_c5 ON numeric_t1(c5);
CREATE INDEX idx_numeric_t1_c7 ON numeric_t1(c7);
CREATE INDEX idx_numeric_t1_c9 ON numeric_t1(c9);
INSERT INTO numeric_t1 VALUES (8, NULL, 'd', NULL, '1982-10-20', '2030-04-08 19:04:18', '2000-06-17 02:13:12.045113', '2035-05-01 04:41:48.021671', -5.97543228059052e+18, -1.23456789123457e+30) ;
INSERT INTO numeric_t1 VALUES (2, 8, 'sfqiutilrankiwcsswhajl', NULL, '2024-04-01 20:14:50', '2029-12-17 04:32:05', '2009-01-25 03:20:02.049928', NULL, 0.123456789123457, 1.23456789123457e+44);
INSERT INTO numeric_t1 VALUES (3, 6, NULL, 'foo', '2024-11-19', '1993-12-09', '2031-07-28 17:26:17', NULL, -1.23456789123457e-09, 0.123456789123457);
INSERT INTO numeric_t1 VALUES (NULL, 7, 'bar', 'fqiu', '2034-10-05 21:10:19', '2015-08-15 11:46:15', '2029-08-28 01:41:22.021862', '2014-10-16 12:24:57', 1.23456789123457e+25, 1.23456789123457e+43);
INSERT INTO numeric_t1 VALUES (1, 9, NULL, 'qiutilivqnqjgmnqkkbb', '1973-12-17 14:20:44', '1982-03-06 18:27:55', '2020-03-02 16:38:21.053405', '1993-06-28 18:16:39.019608', 6.30340576171875e+80, 1.23456789123457e-09);
INSERT INTO numeric_t1 VALUES (NULL, NULL, 'iutilrankiwwjl', 'utilrankiwwsjnqy', '1973-07-18 20:02:01', '1988-09-06', NULL, '2023-09-09', -1.23456789123457e+43, 5.07171630859375e+80);
INSERT INTO numeric_t1 VALUES (NULL, 8, 'tilranki', 'foo', '1987-04-18', '1997-08-13 21:02:30', '2022-08-11 22:10:03', '1987-07-16 01:35:03.017239', 1.23456789123457e+39, 1.23456789123457e+39);
INSERT INTO numeric_t1 VALUES (7, NULL, 'foo', 'ilrank', '2002-12-21', '1983-10-10', '1988-10-18 02:07:19.004820', '1985-08-28 17:42:50.006351', 1.23456789123457e+43, -1.23456789123457e+43);
INSERT INTO numeric_t1 VALUES (0, 1, 'lrankiwwsjnqymnlkyjmbtqd', 'bar', '1979-07-05', '2034-02-06 10:30:34', '1987-07-17 09:10:12', '2031-03-15 05:47:42', -1.23456789123457e+30, 1.23456789123457e+43);
INSERT INTO numeric_t1 VALUES (3, 0, 'bar', NULL, '1999-08-09 14:58:55', '2012-06-19 21:58:04', NULL, '1972-07-21 10:04:14.016456', -1.23456789123457e+43, -1.23456789123457e+30);

CREATE TABLE numeric_t2 (
    c0 int,
    c1 int,
    c2 text,
    c3 text,
    c4 date,
    c5 date,
    c6 timestamp,
    c7 timestamp,
    c8 numeric,
    c9 numeric)  PARTITION BY hash( c4) ;
create TABLE numeric_t2_p0 partition of numeric_t2 for values with(modulus 4,remainder 0);
create TABLE numeric_t2_p1 partition of numeric_t2 for values with(modulus 4,remainder 1);
create TABLE numeric_t2_p2 partition of numeric_t2 for values with(modulus 4,remainder 2);
create TABLE numeric_t2_p3 partition of numeric_t2 for values with(modulus 4,remainder 3);
alter table numeric_t2 alter column c0 drop not null;
CREATE INDEX idx_numeric_t2_c1 ON numeric_t2(c1);
CREATE INDEX idx_numeric_t2_c3 ON numeric_t2(c3);
CREATE INDEX idx_numeric_t2_c5 ON numeric_t2(c5);
CREATE INDEX idx_numeric_t2_c7 ON numeric_t2(c7);
CREATE INDEX idx_numeric_t2_c9 ON numeric_t2(c9);
INSERT INTO numeric_t2 VALUES (NULL, 0, NULL, NULL, '1993-09-02', '2004-10-19', '1983-03-05 17:05:58.001300', NULL, -0.123456789123457, 0.03564453125);
INSERT INTO numeric_t2 VALUES (NULL, NULL, NULL, 'bar', '2020-10-16', '2014-08-15', '2012-06-20 00:16:56.002271', NULL, 0.123456789123457, -1.23456789123457e+44);
INSERT INTO numeric_t2 VALUES (NULL, NULL, 'bar', 'bar', '1973-01-10', '1992-02-21 00:31:02', '2001-02-03 23:08:54.012154', '1995-04-02', 1.23456789123457e+25, -1.23456789123457e-09);
INSERT INTO numeric_t2 VALUES (2, NULL, NULL, NULL, '1990-12-14', '1976-05-18', '1990-02-04 02:55:04.062384', '1976-09-12', 1.23456789123457e+25, 1.23456789123457e+39);
INSERT INTO numeric_t2 VALUES (6, 1, 'jnqym', 'foo', '1994-11-17', '1992-12-19', '1983-09-16 20:25:16', '1993-06-11 20:11:55.034239', 1.23456789123457e-09, -1.23456789123457e-09);
INSERT INTO numeric_t2 VALUES (8, NULL, 'nqymnlkkuauxsr', 'bar', '2009-01-20', '2021-11-10 11:29:47', NULL, '1983-08-26 12:21:43', 1.23456789123457e+30, 8.26326089129473e+18);
INSERT INTO numeric_t2 VALUES (9, NULL, NULL, 'qymnlkyjmefaa', '2006-01-17 15:28:48', '2014-12-17 23:51:26', '2019-07-10 07:18:14.060949', NULL, -1.23456789123457e-09, 1.23456789123457e+44);
INSERT INTO numeric_t2 VALUES (NULL, NULL, 'ymnlkyjmtdoetzumapaqztok', 'foo', '1971-07-28 00:12:34', '2022-04-04', '1985-02-03 14:23:35.014738', '2005-06-20 17:13:35.057121', 5.98724365234375e+80, NULL);
INSERT INTO numeric_t2 VALUES (NULL, 7, 'mn', 'nlkyjmefrbtqdq', '1982-03-02', '1988-01-15 11:07:22', '1976-05-21 16:05:54.029950', '2032-12-15 08:54:03.030226', -1.23456789123457e+25, -1.23456789123457e+30);
INSERT INTO numeric_t2 VALUES (NULL, 2, 'foo', 'lkyjmefehbqurhplvrfwmpfvnnaicbnvlejrbtqdq', '1994-03-12', '1981-07-20 18:42:38', NULL, NULL, 1.23456789123457e-09, -1.23456789123457e+44) ;

-- update select, success
update numeric_t1 tt1 set (c4,c8)=(select c4,tt3.c8 from numeric_t2 tt2 where
        tt1.c7=tt2.c7 and tt1.c3=tt2.c3 and tt1.c4 IS NOT NULL AND ( ( NOT ( tt2.c3 IS NULL ) AND tt3.c0 = 4 )
        OR ( tt2.c0 = tt3.c1 ) AND tt1.c0 = tt1.c1 ) AND ( tt3.c5 >= '2005-11-16 12:05:19' ) ),
        (c3,c7)=(select c3,tt3.c7 from numeric_t1 tt2 where tt1.c7=tt2.c7 and tt1.c3=tt2.c3 and
        tt1.c0 = 6 AND tt2.c1 = tt1.c1 ) from numeric_t1 tt3 WHERE ( tt1.c8 IN ( tt3.c8) );