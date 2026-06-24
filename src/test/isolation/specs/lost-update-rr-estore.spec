# Lost Update
# RR
# In this scenario, there are three sessions: s1, s2, and s3.
# Both s1 and s2 attempt to update the quantity of product X, but due to concurrent operations,
# s2 commits the changes while s1 continues updating based on the old value.
# This situation may lead to data loss in the updates (Lost Update).

setup
{
	CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, quantity INT) with(orientation=column);
	INSERT INTO products (id, name, quantity) VALUES (1, 'Product X', 100), (2, 'Product Y', 150);
}

teardown
{
	DROP TABLE products;
}

session s1
setup 		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s1u	{ UPDATE products SET quantity = quantity - 20 WHERE id = 1; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s2u	{ UPDATE products SET quantity = quantity - 30 WHERE id = 1; }
step s2c	{ COMMIT; }

session s3
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s3r	{ SELECT * FROM products WHERE id = 1; }
step s3c	{ COMMIT; }

permutation s1u s2u s3r s1c s2c s3c
