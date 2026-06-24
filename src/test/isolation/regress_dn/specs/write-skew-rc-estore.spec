# Write Skew
# RC
# Account x and account y each have $50, totaling $100.
# Assuming a constraint that the combined balance of accounts x and y should not be less than $60.
# Transaction 1 and Transaction 2, thinking they are not violating the constraint
# (each reads the balance of x and y respectively), proceed to withdraw $40 from account y and x.
# However, in reality, after these two transactions are completed, x + y = $20, breaking the constraint.

setup
{
    CREATE TABLE accounts (id TEXT PRIMARY KEY, amount INT) with(orientation=column);
    INSERT INTO accounts (id, amount) VALUES ('x', 50), ('y', 50);
}

teardown
{
    DROP TABLE accounts;
}

session t1
setup       { BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step t1r1   { SELECT amount FROM accounts WHERE id = 'x'; }
step t1r2   { SELECT (SELECT amount FROM accounts WHERE id = 'x') + (SELECT amount FROM accounts WHERE id = 'y') >= 60 AS check_constraint; }
step t1u1   { UPDATE accounts SET amount = amount - 40 WHERE id = 'y'; }
step t1c    { COMMIT; }

session t2
setup       { BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step t2r1   { SELECT amount FROM accounts WHERE id = 'y'; }
step t2r2   { SELECT (SELECT amount FROM accounts WHERE id = 'x') + (SELECT amount FROM accounts WHERE id = 'y') >= 60 AS check_constraint; }
step t2u1   { UPDATE accounts SET amount = amount - 40 WHERE id = 'x'; }
step t2c    { COMMIT; }

permutation t1r1 t1r2 t2r1 t2r2 t1u1 t2u1 t1c t2c t1r2 t2r2
