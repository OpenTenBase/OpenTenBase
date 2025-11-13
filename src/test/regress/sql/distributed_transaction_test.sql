-- local node
BEGIN;
INSERT INTO accounts (id, balance) VALUES (1, 1000);
UPDATE accounts SET balance = balance - 100 WHERE id = 1;

-- remote node
SELECT dblink_exec('dbname=remote_db',
    'BEGIN; INSERT INTO accounts (id, balance) VALUES (2, 1000);
     UPDATE accounts SET balance = balance + 100 WHERE id = 2;
     COMMIT;'
);

COMMIT;

SELECT * FROM accounts ORDER BY id;

-- test rollback
BEGIN;
UPDATE accounts SET balance = balance - 50 WHERE id = 1;
UPDATE accounts SET balance = balance + 50 WHERE id = 2;

ROLLBACK;

SELECT * FROM accounts ORDER BY id;