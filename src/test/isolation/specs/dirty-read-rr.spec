# Dirty Read
# RR
# This scenario involves three sessions: s1, s2, and s3.
# Session s1 attempts to update Alice's salary to 5500, s2 tries to read Alice's information,
# while s3 attempts to delete Bob's record. In some cases,
# concurrent operations may lead to a phenomenon known as dirty read.

setup
{
	CREATE TABLE employee (id SERIAL PRIMARY KEY, name TEXT, salary DECIMAL);
	INSERT INTO employee (id, name, salary) VALUES (1, 'Alice', 5000), (2, 'Bob', 6000);
}

teardown
{
	DROP TABLE employee;
}

session s1
setup 		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s1u	{ UPDATE employee SET salary = 5500 WHERE id = 1; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s2r	{ SELECT * FROM employee WHERE id = 1; }
step s2c	{ COMMIT; }

session s3
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s3d	{ DELETE FROM employee WHERE id = 2; }
step s3c	{ COMMIT; }

permutation s2r s1u s3d s1c s3c s2c
