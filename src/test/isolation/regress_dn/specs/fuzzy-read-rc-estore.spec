# Fuzzy Read
# RC
# In this scenario, there are three sessions: s1, s2, and s3.
# Sessions s1 and s3 attempt to read the total amount of all orders,
# while s2 modifies the amount of one order among them. Due to concurrent operations,
# s1 and s3 may see different total amounts, leading to the phenomenon of Fuzzy Read.

setup
{
	CREATE TABLE orders (id SERIAL PRIMARY KEY, total_amount DECIMAL) with(orientation=column);
	INSERT INTO orders (id, total_amount) VALUES (1, 100), (2, 200);
}

teardown
{
	DROP TABLE orders;
}

session s1
setup 		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step s1r	{ SELECT SUM(total_amount) AS total FROM orders; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step s2u	{ UPDATE orders SET total_amount = 150 WHERE id = 1; }
step s2c	{ COMMIT; }

session s3
setup		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step s3r	{ SELECT SUM(total_amount) AS total FROM orders; }
step s3c	{ COMMIT; }

permutation s1r s2u s3r s2c s3c s1r s1c
