# Read Skew
# RR
# In this scenario, there are three sessions: t1, t2, and t3.
# Session t1 attempts to calculate the total sum of all account balances,
# while t2 increases Bob's balance by 40 during this time and commits the transaction.
# Meanwhile, t3 reads the balances of Bob and Charlie.
# Due to concurrent operations, t1 may be influenced by the modification of Bob's balance by t2 during reading,
# leading to observing inconsistent results, known as Read Skew.

setup
{
	CREATE TABLE accounts (id SERIAL PRIMARY KEY, name TEXT, balance INT);
	INSERT INTO accounts (id, name, balance) VALUES (1, 'Alice', 60), (2, 'Bob', 80), (3, 'Charlie', 40);
}

teardown
{
	DROP TABLE accounts;
}

session t1
setup 		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step t1r1	{ SELECT SUM(balance) AS total FROM accounts; }
step t1c	{ COMMIT; }

session t2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step t2r1	{ SELECT * FROM accounts WHERE name = 'Alice'; }
step t2u1	{ UPDATE accounts SET balance = balance + 40 WHERE name = 'Bob'; }
step t2c	{ COMMIT; }

session t3
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step t3r1	{ SELECT * FROM accounts WHERE name = 'Bob'; }
step t3r2	{ SELECT * FROM accounts WHERE name = 'Charlie'; }
step t3c	{ COMMIT; }

permutation t1r1 t2r1 t2u1 t3r1 t3r2 t2c t3c t1r1 t1c

