# Tests for the EvalPlanQual mechanism
#
# EvalPlanQual is used in READ COMMITTED isolation level to attempt to
# re-execute UPDATE and DELETE operations against rows that were updated
# by some concurrent transaction.

setup
{
 CREATE TABLE accounts (accountid text PRIMARY KEY, balance numeric not null);
}
setup
{
 INSERT INTO accounts VALUES ('checking', 600), ('savings', 600);
}
setup
{
 CREATE TABLE p (a int, b int, c int);
}
setup
{
 CREATE TABLE c1 () INHERITS (p);
}
setup
{
 CREATE TABLE c2 () INHERITS (p);
}
setup
{
 CREATE TABLE c3 () INHERITS (p);
}
setup
{
 INSERT INTO c1 SELECT 0, a / 3, a % 3 FROM generate_series(0, 9) a;
 INSERT INTO c2 SELECT 1, a / 3, a % 3 FROM generate_series(0, 9) a;
 INSERT INTO c3 SELECT 2, a / 3, a % 3 FROM generate_series(0, 9) a;
}
setup
{
 CREATE TABLE table_a (id integer, value text);
}
setup
{
 CREATE TABLE table_b (id integer, value text);
}
setup
{
 INSERT INTO table_a VALUES (1, 'tableAValue');
 INSERT INTO table_b VALUES (1, 'tableBValue');
}
setup
{ 
 CREATE TABLE parttbl (a int) PARTITION BY LIST (a);
 CREATE TABLE parttbl1 PARTITION OF parttbl FOR VALUES IN (1);
 INSERT INTO parttbl VALUES (1);
}

teardown
{
 DROP TABLE accounts, p, table_a, table_b CASCADE;
 DROP TABLE parttbl;
}

session "s1"
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
# wx1 then wx2 checks the basic case of re-fetching up-to-date values
step "wx1"	{ UPDATE accounts SET balance = balance - 200 WHERE accountid = 'checking'; }
# wy1 then wy2 checks the case where quals pass then fail
step "wy1"	{ UPDATE accounts SET balance = balance + 500 WHERE accountid = 'checking'; }
# upsert tests are to check writable-CTE cases
step "upsert1"	{
	WITH upsert AS
	  (UPDATE accounts SET balance = balance + 500
	   WHERE accountid = 'savings'
	   RETURNING accountid)
	INSERT INTO accounts SELECT 'savings', 500
	  WHERE NOT EXISTS (SELECT 1 FROM upsert);
}

# tests with table p check inheritance cases:
# readp1/writep1/readp2 tests a bug where nodeLockRows did the wrong thing
# when the first updated tuple was in a non-first child table.
# writep2/returningp1 tests a memory allocation issue

step "readp1"	{ SELECT tableoid::regclass, ctid, * FROM p WHERE b IN (0, 1) AND c = 0 FOR UPDATE; }
step "writep1"	{ UPDATE p SET b = -1 WHERE a = 1 AND b = 1 AND c = 0; }
step "writep2"	{ UPDATE p SET b = -b WHERE a = 1 AND c = 0; }
step "c1"	{ COMMIT; }

# these tests are meant to exercise EvalPlanQualFetchRowMarks,
# ie, handling non-locked tables in an EvalPlanQual recheck

step "partiallock"	{
	SELECT * FROM accounts a1, accounts a2
	  WHERE a1.accountid = a2.accountid
	  FOR UPDATE OF a1;
}
step "lockwithvalues"	{
	SELECT * FROM accounts a1, (values('checking'),('savings')) v(id)
	  WHERE a1.accountid = v.id
	  FOR UPDATE OF a1;
}

# these tests exercise EvalPlanQual with a SubLink sub-select (which should be
# unaffected by any EPQ recheck behavior in the outer query); cf bug #14034

step "updateforss"	{
	UPDATE table_a SET value = 'newTableAValue' WHERE id = 1;
	UPDATE table_b SET value = 'newTableBValue' WHERE id = 1;
}

# test for EPQ on a partitioned result table

step "simplepartupdate"    {
   update parttbl set a = a;
}

session "s2"
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step "wx2"	{ UPDATE accounts SET balance = balance + 450 WHERE accountid = 'checking'; }
step "wy2"	{ UPDATE accounts SET balance = balance + 1000 WHERE accountid = 'checking' AND balance < 1000; }
step "upsert2"	{
	WITH upsert AS
	  (UPDATE accounts SET balance = balance + 1234
	   WHERE accountid = 'savings'
	   RETURNING accountid)
	INSERT INTO accounts SELECT 'savings', 1234
	  WHERE NOT EXISTS (SELECT 1 FROM upsert);
}
step "readp2"	{ SELECT tableoid::regclass, ctid, * FROM p WHERE b IN (0, 1) AND c = 0 FOR UPDATE; }
step "returningp1" {
	WITH u AS ( UPDATE p SET b = b WHERE a > 0 RETURNING * )
	  SELECT * FROM u;
}
step "readforss"	{
	SELECT ta.id AS ta_id, ta.value AS ta_value,
		(SELECT ROW(tb.id, tb.value)
		 FROM table_b tb WHERE ta.id = tb.id) AS tb_row
	FROM table_a ta
	WHERE ta.id = 1 FOR UPDATE OF ta;
}
step "wrtwcte"	{ UPDATE table_a SET value = 'tableAValue2' WHERE id = 1; }
step "complexpartupdate"   {
   with u as (update parttbl set a = a returning parttbl.*)
   update parttbl set a = u.a from u;
}
step "c2"	{ COMMIT; }

session "s3"
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step "read"	{ SELECT * FROM accounts ORDER BY accountid; }

# this test exercises EvalPlanQual with a CTE, cf bug #14328
step "readwcte"	{
	WITH
	    cte1 AS (
	      SELECT id FROM table_b WHERE value = 'tableBValue'
	    ),
	    cte2 AS (
	      SELECT * FROM table_a
	      WHERE id = (SELECT id FROM cte1)
	      FOR UPDATE
	    )
	SELECT * FROM cte2;
}

teardown	{ COMMIT; }

permutation "wx1" "wx2" "c1" "c2" "read"
permutation "wy1" "wy2" "c1" "c2" "read"
permutation "upsert1" "upsert2" "c1" "c2" "read"
permutation "readp1" "writep1" "readp2" "c1" "c2"
permutation "writep2" "returningp1" "c1" "c2"
permutation "wx2" "partiallock" "c2" "c1" "read"
permutation "wx2" "lockwithvalues" "c2" "c1" "read"
permutation "updateforss" "readforss" "c1" "c2"
permutation "wrtwcte" "readwcte" "c1" "c2"

permutation "simplepartupdate" "complexpartupdate" "c1" "c2"
