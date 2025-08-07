# Phantom
# RR
# In this scenario, there are three sessions: s1, s2, and s3.
# Sessions s1 and s3 attempt to select orders with quantities greater than 15,
# while s2 inserts a new order. Due to concurrent operations,
# s1 and s3 may see different result sets, leading to the phenomenon of Phantom Read.

setup
{
	CREATE TABLE orders (id SERIAL PRIMARY KEY, item TEXT, quantity INT) with(orientation=column);
	INSERT INTO orders (id, item, quantity) VALUES (1, 'Item A', 10), (2, 'Item B', 20);
}

teardown
{
	DROP TABLE orders;
}

session s1
setup 		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s1r	{ SELECT * FROM orders WHERE quantity > 15; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s2i	{ INSERT INTO orders (item, quantity) VALUES ('Item C', 25); }
step s2c	{ COMMIT; }

session s3
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s3r1	{ SELECT * FROM orders WHERE quantity > 15; }
step s3r2	{ SELECT * FROM orders order by 1; }
step s3c	{ COMMIT; }

permutation s1r s2i s3r1 s3r2 s2c s3c s1r s1c
