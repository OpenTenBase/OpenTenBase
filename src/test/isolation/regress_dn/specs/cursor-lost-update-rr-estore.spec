# Cursor Lost Updates1c
# RR
#In this scenario, we have three sessions: s1, s2, and s3.
# Both s1 and s2 declare a cursor to select data with book id 1 and then attempt to update the inventory.
# s2 commits the transaction after updating, while s1 updates the inventory afterward.
# In this situation, due to lost updates through cursors, data inconsistency may occur.

setup
{
    CREATE TABLE books (id SERIAL PRIMARY KEY, title TEXT, stock INT) with(orientation=column);
    INSERT INTO books (id, title, stock) VALUES (1, 'Book A', 10), (2, 'Book B', 20);
}

teardown
{
    DROP TABLE books;
}

session s1
setup       { BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s1d    { DECLARE cur1 CURSOR FOR SELECT * FROM books WHERE id = 1 FOR UPDATE; }
step s1u    { UPDATE books SET stock = stock - 2 WHERE id = 1; }
step s1c    { COMMIT; }

session s2
setup       { BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s2d    { DECLARE cur2 CURSOR FOR SELECT * FROM books WHERE id = 1 FOR UPDATE; }
step s2u    { UPDATE books SET stock = stock - 3 WHERE id = 1; }
step s2c    { COMMIT; }

session s3
setup       { BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s3r    { SELECT * FROM books WHERE id = 1; }
step s3c    { COMMIT; }

permutation s1d s2d s1u s2u s3r s1c s2c s3c