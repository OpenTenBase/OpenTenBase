# Dirty Write
# RC
# This scenario involves three sessions: s1, s2, and s3.
# Session s1 tries to modify the price of product A to 120,
# while s2 also attempts to change the price of product A to 200.
# In some cases, concurrent operations may lead to a phenomenon known as dirty write.

setup
{
	CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price DECIMAL) with(orientation=column);
	INSERT INTO products (id, name, price) VALUES (1, 'Product A', 100), (2, 'Product B', 150);
}

teardown
{
	DROP TABLE products;
}

session s1
setup 		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step s1u	{ UPDATE products SET price = 120 WHERE id = 1; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step s2u	{ UPDATE products SET price = 200 WHERE id = 1; }
step s2c	{ COMMIT; }

session s3
setup		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step s3s	{ SELECT name, price FROM products WHERE id = 1; }
step s3c	{ COMMIT; }

permutation s1u s2u s3s s1c s2c s3c

