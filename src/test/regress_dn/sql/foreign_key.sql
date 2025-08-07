--
-- FOREIGN KEY
--
set datestyle = 'ISO, YMD';
--test foreign key constraints
CREATE TABLE users_main (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age SMALLINT NOT NULL,
    email VARCHAR(50) UNIQUE,
    phone VARCHAR(20) NOT NULL
);
CREATE TABLE orders_ref (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users_main (id) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO "users_main" (id, name, age, email, phone) SELECT i, 'User ' || i, i % 100, 'user' || i || '@example.com', '139' || i FROM generate_series(1, 10) AS s(i);
INSERT INTO "orders_ref" (id, user_id, amount, status, create_time, update_time) SELECT i, i + 1, (i * 10)%20000, 'created', now(), now()FROM generate_series(1,12) AS s(i);
drop table orders_ref;
drop table users_main;

-- MATCH FULL
--
-- First test, check and cascade
--
CREATE TABLE PKTABLE ( ptest1 int PRIMARY KEY, ptest2 text ) DISTRIBUTE BY REPLICATION;
CREATE TABLE FKTABLE ( ftest1 int REFERENCES PKTABLE MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE, ftest2 int );

-- Insert test data into PKTABLE
INSERT INTO PKTABLE VALUES (1, 'Test1');
INSERT INTO PKTABLE VALUES (2, 'Test2');
INSERT INTO PKTABLE VALUES (3, 'Test3');
INSERT INTO PKTABLE VALUES (4, 'Test4');
INSERT INTO PKTABLE VALUES (5, 'Test5');

-- Insert successful rows into FK TABLE
INSERT INTO FKTABLE VALUES (1, 2);
INSERT INTO FKTABLE VALUES (2, 3);
INSERT INTO FKTABLE VALUES (3, 4);
INSERT INTO FKTABLE VALUES (NULL, 1);

-- Insert a failed row into FK TABLE
INSERT INTO FKTABLE VALUES (100, 2);

-- Check FKTABLE
SELECT * FROM FKTABLE ORDER BY 1, 2;

-- Delete a row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=1;

-- Check FKTABLE for removal of matched row
SELECT * FROM FKTABLE ORDER BY 1, 2;

-- Update a row from PK TABLE
UPDATE PKTABLE SET ptest1=1 WHERE ptest1=2;

-- Check FKTABLE for update of matched row
SELECT * FROM FKTABLE ORDER BY 1, 2;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

--
-- check set NULL and table constraint on multiple columns
--
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 text, PRIMARY KEY(ptest1, ptest2) ) DISTRIBUTE BY REPLICATION;
CREATE TABLE FKTABLE ( ftest1 int, ftest2 int, ftest3 int, CONSTRAINT constrname FOREIGN KEY(ftest1, ftest2)
                       REFERENCES PKTABLE MATCH FULL ON DELETE SET NULL ON UPDATE SET NULL);

-- Test comments
COMMENT ON CONSTRAINT constrname_wrong ON FKTABLE IS 'fk constraint comment';
COMMENT ON CONSTRAINT constrname ON FKTABLE IS 'fk constraint comment';
COMMENT ON CONSTRAINT constrname ON FKTABLE IS NULL;

-- Insert test data into PKTABLE
INSERT INTO PKTABLE VALUES (1, 2, 'Test1');
INSERT INTO PKTABLE VALUES (1, 3, 'Test1-2');
INSERT INTO PKTABLE VALUES (2, 4, 'Test2');
INSERT INTO PKTABLE VALUES (3, 6, 'Test3');
INSERT INTO PKTABLE VALUES (4, 8, 'Test4');
INSERT INTO PKTABLE VALUES (5, 10, 'Test5');

-- Insert successful rows into FK TABLE
INSERT INTO FKTABLE VALUES (1, 2, 4);
INSERT INTO FKTABLE VALUES (1, 3, 5);
INSERT INTO FKTABLE VALUES (2, 4, 8);
INSERT INTO FKTABLE VALUES (3, 6, 12);
INSERT INTO FKTABLE VALUES (NULL, NULL, 0);

-- Insert failed rows into FK TABLE
INSERT INTO FKTABLE VALUES (100, 2, 4);
INSERT INTO FKTABLE VALUES (2, 2, 4);
INSERT INTO FKTABLE VALUES (NULL, 2, 4);
INSERT INTO FKTABLE VALUES (1, NULL, 4);

-- Check FKTABLE
SELECT * FROM FKTABLE ORDER BY 1, 2, 3;

-- Delete a row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=1 and ptest2=2;

-- Check FKTABLE for removal of matched row
SELECT * FROM FKTABLE ORDER BY 1, 2, 3;

-- Delete another row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=5 and ptest2=10;

-- Check FKTABLE (should be no change)
SELECT * FROM FKTABLE ORDER BY 1, 2, 3;

-- Update a row from PK TABLE
UPDATE PKTABLE SET ptest1=1 WHERE ptest1=2;

-- Check FKTABLE for update of matched row
SELECT * FROM FKTABLE ORDER BY 1, 2, 3;

-- Try altering the column type where foreign keys are involved
ALTER TABLE PKTABLE ALTER COLUMN ptest1 TYPE bigint;
ALTER TABLE FKTABLE ALTER COLUMN ftest1 TYPE bigint;
SELECT * FROM PKTABLE ORDER BY 1, 2, 3;
SELECT * FROM FKTABLE ORDER BY 1, 2, 3;

DROP TABLE PKTABLE CASCADE;
DROP TABLE FKTABLE;

--
-- check set default and table constraint on multiple columns
--
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 text, PRIMARY KEY(ptest1, ptest2) ) DISTRIBUTE BY REPLICATION;
CREATE TABLE FKTABLE ( ftest1 int DEFAULT -1, ftest2 int DEFAULT -2, ftest3 int, CONSTRAINT constrname2 FOREIGN KEY(ftest1, ftest2)
                       REFERENCES PKTABLE MATCH FULL ON DELETE SET DEFAULT ON UPDATE SET DEFAULT);

-- Insert a value in PKTABLE for default
INSERT INTO PKTABLE VALUES (-1, -2, 'The Default!');

-- Insert test data into PKTABLE
INSERT INTO PKTABLE VALUES (1, 2, 'Test1');
INSERT INTO PKTABLE VALUES (1, 3, 'Test1-2');
INSERT INTO PKTABLE VALUES (2, 4, 'Test2');
INSERT INTO PKTABLE VALUES (3, 6, 'Test3');
INSERT INTO PKTABLE VALUES (4, 8, 'Test4');
INSERT INTO PKTABLE VALUES (5, 10, 'Test5');

-- Insert successful rows into FK TABLE
INSERT INTO FKTABLE VALUES (1, 2, 4);
INSERT INTO FKTABLE VALUES (1, 3, 5);
INSERT INTO FKTABLE VALUES (2, 4, 8);
INSERT INTO FKTABLE VALUES (3, 6, 12);
INSERT INTO FKTABLE VALUES (NULL, NULL, 0);

-- Insert failed rows into FK TABLE
INSERT INTO FKTABLE VALUES (100, 2, 4);
INSERT INTO FKTABLE VALUES (2, 2, 4);
INSERT INTO FKTABLE VALUES (NULL, 2, 4);
INSERT INTO FKTABLE VALUES (1, NULL, 4);

-- Check FKTABLE
SELECT * FROM FKTABLE ORDER BY 1, 2, 3;

-- Delete a row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=1 and ptest2=2;

-- Check FKTABLE to check for removal
SELECT * FROM FKTABLE ORDER BY 1, 2, 3;

-- Delete another row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=5 and ptest2=10;

-- Check FKTABLE (should be no change)
SELECT * FROM FKTABLE ORDER BY 1, 2, 3;

-- Update a row from PK TABLE
UPDATE PKTABLE SET ptest1=1 WHERE ptest1=2;

-- Check FKTABLE for update of matched row
SELECT * FROM FKTABLE ORDER BY 1, 2, 3;

-- this should fail for lack of CASCADE
DROP TABLE PKTABLE;
DROP TABLE PKTABLE CASCADE;
DROP TABLE FKTABLE;


--
-- First test, check with no on delete or on update
--
CREATE TABLE PKTABLE ( ptest1 int PRIMARY KEY, ptest2 text ) DISTRIBUTE BY REPLICATION;
CREATE TABLE FKTABLE ( ftest1 int REFERENCES PKTABLE MATCH FULL, ftest2 int ) DISTRIBUTE BY REPLICATION;

-- Insert test data into PKTABLE
INSERT INTO PKTABLE VALUES (1, 'Test1');
INSERT INTO PKTABLE VALUES (2, 'Test2');
INSERT INTO PKTABLE VALUES (3, 'Test3');
INSERT INTO PKTABLE VALUES (4, 'Test4');
INSERT INTO PKTABLE VALUES (5, 'Test5');

-- Insert successful rows into FK TABLE
INSERT INTO FKTABLE VALUES (1, 2);
INSERT INTO FKTABLE VALUES (2, 3);
INSERT INTO FKTABLE VALUES (3, 4);
INSERT INTO FKTABLE VALUES (NULL, 1);

-- Insert a failed row into FK TABLE
INSERT INTO FKTABLE VALUES (100, 2);

-- Check FKTABLE
SELECT * FROM FKTABLE ORDER BY 1, 2;

-- Check PKTABLE
SELECT * FROM PKTABLE ORDER BY 1, 2;

-- Delete a row from PK TABLE (should fail)
DELETE FROM PKTABLE WHERE ptest1=1;

-- Delete a row from PK TABLE (should succeed)
DELETE FROM PKTABLE WHERE ptest1=5;

-- Check PKTABLE for deletes
SELECT * FROM PKTABLE ORDER BY 1, 2;

-- Update a row from PK TABLE (should fail)
UPDATE PKTABLE SET ptest1=0 WHERE ptest1=2;

-- Update a row from PK TABLE (should succeed)
UPDATE PKTABLE SET ptest1=0 WHERE ptest1=4;

-- Check PKTABLE for updates
SELECT * FROM PKTABLE ORDER BY 1, 2;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;


-- MATCH SIMPLE

-- Base test restricting update/delete
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 int, ptest4 text, PRIMARY KEY(ptest1, ptest2, ptest3) ) DISTRIBUTE BY REPLICATION;
CREATE TABLE FKTABLE ( ftest1 int, ftest2 int, ftest3 int, ftest4 int,  CONSTRAINT constrname3
			FOREIGN KEY(ftest1, ftest2, ftest3) REFERENCES PKTABLE) DISTRIBUTE BY REPLICATION;

-- Insert Primary Key values
INSERT INTO PKTABLE VALUES (1, 2, 3, 'test1');
INSERT INTO PKTABLE VALUES (1, 3, 3, 'test2');
INSERT INTO PKTABLE VALUES (2, 3, 4, 'test3');
INSERT INTO PKTABLE VALUES (2, 4, 5, 'test4');

-- Insert Foreign Key values
INSERT INTO FKTABLE VALUES (1, 2, 3, 1);
INSERT INTO FKTABLE VALUES (NULL, 2, 3, 2);
INSERT INTO FKTABLE VALUES (2, NULL, 3, 3);
INSERT INTO FKTABLE VALUES (NULL, 2, 7, 4);
INSERT INTO FKTABLE VALUES (NULL, 3, 4, 5);

-- Insert a failed values
INSERT INTO FKTABLE VALUES (1, 2, 7, 6);

-- Show FKTABLE
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to update something that should fail
UPDATE PKTABLE set ptest2=5 where ptest2=2;

-- Try to update something that should succeed
UPDATE PKTABLE set ptest1=1 WHERE ptest2=3;

-- Try to delete something that should fail
DELETE FROM PKTABLE where ptest1=1 and ptest2=2 and ptest3=3;

-- Try to delete something that should work
DELETE FROM PKTABLE where ptest1=2;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;

SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- cascade update/delete
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 int, ptest4 text, PRIMARY KEY(ptest1, ptest2, ptest3) ) DISTRIBUTE BY REPLICATION;
CREATE TABLE FKTABLE ( ftest1 int, ftest2 int, ftest3 int, ftest4 int,  CONSTRAINT constrname3
			FOREIGN KEY(ftest1, ftest2, ftest3) REFERENCES PKTABLE
			ON DELETE CASCADE ON UPDATE CASCADE);

-- Insert Primary Key values
INSERT INTO PKTABLE VALUES (1, 2, 3, 'test1');
INSERT INTO PKTABLE VALUES (1, 3, 3, 'test2');
INSERT INTO PKTABLE VALUES (2, 3, 4, 'test3');
INSERT INTO PKTABLE VALUES (2, 4, 5, 'test4');

-- Insert Foreign Key values
INSERT INTO FKTABLE VALUES (1, 2, 3, 1);
INSERT INTO FKTABLE VALUES (NULL, 2, 3, 2);
INSERT INTO FKTABLE VALUES (2, NULL, 3, 3);
INSERT INTO FKTABLE VALUES (NULL, 2, 7, 4);
INSERT INTO FKTABLE VALUES (NULL, 3, 4, 5);

-- Insert a failed values
INSERT INTO FKTABLE VALUES (1, 2, 7, 6);

-- Show FKTABLE
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to update something that will cascade
UPDATE PKTABLE set ptest2=5 where ptest2=2;

-- Try to update something that should not cascade
UPDATE PKTABLE set ptest1=1 WHERE ptest2=3;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to delete something that should cascade
DELETE FROM PKTABLE where ptest1=1 and ptest2=5 and ptest3=3;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to delete something that should not have a cascade
DELETE FROM PKTABLE where ptest1=2;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- set null update / set default delete
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 int, ptest4 text, PRIMARY KEY(ptest1, ptest2, ptest3) );
CREATE TABLE FKTABLE ( ftest1 int DEFAULT 0, ftest2 int, ftest3 int, ftest4 int,  CONSTRAINT constrname3
			FOREIGN KEY(ftest1, ftest2, ftest3) REFERENCES PKTABLE
			ON DELETE SET DEFAULT ON UPDATE SET NULL);

-- Insert Primary Key values
INSERT INTO PKTABLE VALUES (1, 2, 3, 'test1');
INSERT INTO PKTABLE VALUES (1, 3, 3, 'test2');
INSERT INTO PKTABLE VALUES (2, 3, 4, 'test3');
INSERT INTO PKTABLE VALUES (2, 4, 5, 'test4');

-- Insert Foreign Key values
INSERT INTO FKTABLE VALUES (1, 2, 3, 1);
INSERT INTO FKTABLE VALUES (2, 3, 4, 1);
INSERT INTO FKTABLE VALUES (NULL, 2, 3, 2);
INSERT INTO FKTABLE VALUES (2, NULL, 3, 3);
INSERT INTO FKTABLE VALUES (NULL, 2, 7, 4);
INSERT INTO FKTABLE VALUES (NULL, 3, 4, 5);

-- Insert a failed values
INSERT INTO FKTABLE VALUES (1, 2, 7, 6);

-- Show FKTABLE
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to update something that will set null
UPDATE PKTABLE set ptest2=5 where ptest2=2;

-- Try to update something that should not set null
UPDATE PKTABLE set ptest2=2 WHERE ptest2=3 and ptest1=1;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to delete something that should set default
DELETE FROM PKTABLE where ptest1=2 and ptest2=3 and ptest3=4;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to delete something that should not set default
DELETE FROM PKTABLE where ptest2=5;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- set default update / set null delete
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 int, ptest4 text, PRIMARY KEY(ptest1, ptest2, ptest3) ) DISTRIBUTE BY REPLICATION;
CREATE TABLE FKTABLE ( ftest1 int DEFAULT 0, ftest2 int DEFAULT -1, ftest3 int DEFAULT -2, ftest4 int, CONSTRAINT constrname3
			FOREIGN KEY(ftest1, ftest2, ftest3) REFERENCES PKTABLE
			ON DELETE SET NULL ON UPDATE SET DEFAULT) DISTRIBUTE BY REPLICATION;

-- Insert Primary Key values
INSERT INTO PKTABLE VALUES (1, 2, 3, 'test1');
INSERT INTO PKTABLE VALUES (1, 3, 3, 'test2');
INSERT INTO PKTABLE VALUES (2, 3, 4, 'test3');
INSERT INTO PKTABLE VALUES (2, 4, 5, 'test4');
INSERT INTO PKTABLE VALUES (2, -1, 5, 'test5');

-- Insert Foreign Key values
INSERT INTO FKTABLE VALUES (1, 2, 3, 1);
INSERT INTO FKTABLE VALUES (2, 3, 4, 1);
INSERT INTO FKTABLE VALUES (2, 4, 5, 1);
INSERT INTO FKTABLE VALUES (NULL, 2, 3, 2);
INSERT INTO FKTABLE VALUES (2, NULL, 3, 3);
INSERT INTO FKTABLE VALUES (NULL, 2, 7, 4);
INSERT INTO FKTABLE VALUES (NULL, 3, 4, 5);

-- Insert a failed values
INSERT INTO FKTABLE VALUES (1, 2, 7, 6);

-- Show FKTABLE
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to update something that will fail
UPDATE PKTABLE set ptest2=5 where ptest2=2;

-- Try to update something that will set default
UPDATE PKTABLE set ptest1=0, ptest2=-1, ptest3=-2 where ptest2=2;
UPDATE PKTABLE set ptest2=10 where ptest2=4;

-- Try to update something that should not set default
UPDATE PKTABLE set ptest2=2 WHERE ptest2=3 and ptest1=1;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to delete something that should set null
DELETE FROM PKTABLE where ptest1=2 and ptest2=3 and ptest3=4;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

-- Try to delete something that should not set null
DELETE FROM PKTABLE where ptest2=-1 and ptest3=5;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE ORDER BY 1, 2, 3,4;
SELECT * from FKTABLE ORDER BY 1, 2, 3,4;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

CREATE TABLE PKTABLE (ptest1 int PRIMARY KEY);
CREATE TABLE FKTABLE_FAIL1 ( ftest1 int, CONSTRAINT fkfail1 FOREIGN KEY (ftest2) REFERENCES PKTABLE);
CREATE TABLE FKTABLE_FAIL2 ( ftest1 int, CONSTRAINT fkfail1 FOREIGN KEY (ftest1) REFERENCES PKTABLE(ptest2));

DROP TABLE FKTABLE_FAIL1;
DROP TABLE FKTABLE_FAIL2;
DROP TABLE PKTABLE;

-- Test for referencing column number smaller than referenced constraint
CREATE TABLE PKTABLE (ptest1 int, ptest2 int, UNIQUE(ptest1, ptest2));
CREATE TABLE FKTABLE_FAIL1 (ftest1 int REFERENCES pktable(ptest1));

DROP TABLE FKTABLE_FAIL1;
DROP TABLE PKTABLE;

--
-- Tests for mismatched types
--
-- Basic one column, two table setup
CREATE TABLE PKTABLE (ptest1 int PRIMARY KEY) DISTRIBUTE BY REPLICATION;
INSERT INTO PKTABLE VALUES(42);
-- This next should fail, because int=inet does not exist
CREATE TABLE FKTABLE (ftest1 inet REFERENCES pktable);
-- This should also fail for the same reason, but here we
-- give the column name
CREATE TABLE FKTABLE (ftest1 inet REFERENCES pktable(ptest1)) DISTRIBUTE BY REPLICATION;
-- This should succeed, even though they are different types,
-- because int=int8 exists and is a member of the integer opfamily
CREATE TABLE FKTABLE (ftest1 int8 REFERENCES pktable) DISTRIBUTE BY REPLICATION;
-- Check it actually works
INSERT INTO FKTABLE VALUES(42);		-- should succeed
INSERT INTO FKTABLE VALUES(43);		-- should fail
UPDATE FKTABLE SET ftest1 = ftest1;	-- should succeed
UPDATE FKTABLE SET ftest1 = ftest1 + 1;	-- should fail
DROP TABLE FKTABLE;
-- This should fail, because we'd have to cast numeric to int which is
-- not an implicit coercion (or use numeric=numeric, but that's not part
-- of the integer opfamily)
CREATE TABLE FKTABLE (ftest1 numeric REFERENCES pktable) DISTRIBUTE BY REPLICATION;
DROP TABLE PKTABLE;
-- On the other hand, this should work because int implicitly promotes to
-- numeric, and we allow promotion on the FK side
CREATE TABLE PKTABLE (ptest1 numeric PRIMARY KEY) DISTRIBUTE BY REPLICATION;
INSERT INTO PKTABLE VALUES(42);
CREATE TABLE FKTABLE (ftest1 int REFERENCES pktable) DISTRIBUTE BY REPLICATION;
-- Check it actually works
INSERT INTO FKTABLE VALUES(42);		-- should succeed
INSERT INTO FKTABLE VALUES(43);		-- should fail
UPDATE FKTABLE SET ftest1 = ftest1;	-- should succeed
UPDATE FKTABLE SET ftest1 = ftest1 + 1;	-- should fail
DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- Two columns, two tables
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, PRIMARY KEY(ptest1, ptest2))  DISTRIBUTE BY REPLICATION;
-- This should fail, because we just chose really odd types
CREATE TABLE FKTABLE (ftest1 cidr, ftest2 timestamp, FOREIGN KEY(ftest1, ftest2) REFERENCES pktable) DISTRIBUTE BY REPLICATION;
-- Again, so should this...
CREATE TABLE FKTABLE (ftest1 cidr, ftest2 timestamp, FOREIGN KEY(ftest1, ftest2) REFERENCES pktable(ptest1, ptest2)) DISTRIBUTE BY REPLICATION;
-- This fails because we mixed up the column ordering
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest2, ftest1) REFERENCES pktable) DISTRIBUTE BY REPLICATION;
-- As does this...
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest2, ftest1) REFERENCES pktable(ptest1, ptest2)) DISTRIBUTE BY REPLICATION;
-- And again..
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest1, ftest2) REFERENCES pktable(ptest2, ptest1)) DISTRIBUTE BY REPLICATION;
-- This works...
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest2, ftest1) REFERENCES pktable(ptest2, ptest1)) DISTRIBUTE BY REPLICATION;
DROP TABLE FKTABLE;
-- As does this
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest1, ftest2) REFERENCES pktable(ptest1, ptest2)) DISTRIBUTE BY REPLICATION;
DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- Two columns, same table
-- Make sure this still works...
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest3,
ptest4) REFERENCES pktable(ptest1, ptest2));
DROP TABLE PKTABLE;
-- And this,
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest3,
ptest4) REFERENCES pktable);
DROP TABLE PKTABLE;
-- This shouldn't (mixed up columns)
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest3,
ptest4) REFERENCES pktable(ptest2, ptest1));
-- Nor should this... (same reason, we have 4,3 referencing 1,2 which mismatches types
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest4,
ptest3) REFERENCES pktable(ptest1, ptest2));
-- Not this one either... Same as the last one except we didn't defined the columns being referenced.
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest4,
ptest3) REFERENCES pktable);

--
-- Now some cases with inheritance
-- Basic 2 table case: 1 column of matching types.
create table pktable_base (base1 int not null) DISTRIBUTE BY REPLICATION;
create table pktable (ptest1 int, primary key(base1), unique(base1, ptest1)) inherits (pktable_base) DISTRIBUTE BY REPLICATION;
create table fktable (ftest1 int references pktable(base1));
-- now some ins, upd, del
insert into pktable(base1) values (1);
insert into pktable(base1) values (2);
--  let's insert a non-existent fktable value
insert into fktable(ftest1) values (3);
--  let's make a valid row for that
insert into pktable(base1) values (3);
insert into fktable(ftest1) values (3);
-- let's try removing a row that should fail from pktable
delete from pktable where base1>2;
-- okay, let's try updating all of the base1 values to *4
-- which should fail.
update pktable set base1=base1*4;
-- okay, let's try an update that should work.
update pktable set base1=base1*4 where base1<3;
-- and a delete that should work
delete from pktable where base1>3;
-- cleanup
drop table fktable;
delete from pktable;

-- Now 2 columns 2 tables, matching types
create table fktable (ftest1 int, ftest2 int, foreign key(ftest1, ftest2) references pktable(base1, ptest1));
-- now some ins, upd, del
insert into pktable(base1, ptest1) values (1, 1);
insert into pktable(base1, ptest1) values (2, 2);
--  let's insert a non-existent fktable value
insert into fktable(ftest1, ftest2) values (3, 1);
--  let's make a valid row for that
insert into pktable(base1,ptest1) values (3, 1);
insert into fktable(ftest1, ftest2) values (3, 1);
-- let's try removing a row that should fail from pktable
delete from pktable where base1>2;
-- okay, let's try updating all of the base1 values to *4
-- which should fail.
update pktable set base1=base1*4;
-- okay, let's try an update that should work.
update pktable set base1=base1*4 where base1<3;
-- and a delete that should work
delete from pktable where base1>3;
-- cleanup
drop table fktable;
drop table pktable;
drop table pktable_base;

-- Now we'll do one all in 1 table with 2 columns of matching types
create table pktable_base(base1 int not null, base2 int) DISTRIBUTE BY REPLICATION;
create table pktable(ptest1 int, ptest2 int, primary key(base1, ptest1), foreign key(base2, ptest2) references
                                             pktable(base1, ptest1)) inherits (pktable_base) DISTRIBUTE BY REPLICATION;
insert into pktable (base1, ptest1, base2, ptest2) values (1, 1, 1, 1);
insert into pktable (base1, ptest1, base2, ptest2) values (2, 1, 1, 1);
insert into pktable (base1, ptest1, base2, ptest2) values (2, 2, 2, 1);
insert into pktable (base1, ptest1, base2, ptest2) values (1, 3, 2, 2);
-- fails (3,2) isn't in base1, ptest1
insert into pktable (base1, ptest1, base2, ptest2) values (2, 3, 3, 2);
-- fails (2,2) is being referenced
delete from pktable where base1=2;
-- fails (1,1) is being referenced (twice)
update pktable set base1=3 where base1=1;
-- this sequence of two deletes will work, since after the first there will be no (2,*) references
delete from pktable where base2=2;
delete from pktable where base1=2;
drop table pktable;
drop table pktable_base;

-- 2 columns (2 tables), mismatched types
create table pktable_base(base1 int not null) DISTRIBUTE BY REPLICATION;
create table pktable(ptest1 inet, primary key(base1, ptest1)) inherits (pktable_base);
-- just generally bad types (with and without column references on the referenced table)
create table fktable(ftest1 cidr, ftest2 int[], foreign key (ftest1, ftest2) references pktable);
create table fktable(ftest1 cidr, ftest2 int[], foreign key (ftest1, ftest2) references pktable(base1, ptest1)) DISTRIBUTE BY REPLICATION;
-- let's mix up which columns reference which
create table fktable(ftest1 int, ftest2 inet, foreign key(ftest2, ftest1) references pktable) DISTRIBUTE BY REPLICATION;
create table fktable(ftest1 int, ftest2 inet, foreign key(ftest2, ftest1) references pktable(base1, ptest1)) DISTRIBUTE BY REPLICATION;
create table fktable(ftest1 int, ftest2 inet, foreign key(ftest1, ftest2) references pktable(ptest1, base1));
drop table pktable;
drop table pktable_base;

-- 2 columns (1 table), mismatched types
create table pktable_base(base1 int not null, base2 int);
create table pktable(ptest1 inet, ptest2 inet[], primary key(base1, ptest1), foreign key(base2, ptest2) references
                                             pktable(base1, ptest1)) inherits (pktable_base);
create table pktable(ptest1 inet, ptest2 inet, primary key(base1, ptest1), foreign key(base2, ptest2) references
                                             pktable(ptest1, base1)) inherits (pktable_base);
create table pktable(ptest1 inet, ptest2 inet, primary key(base1, ptest1), foreign key(ptest2, base2) references
                                             pktable(base1, ptest1)) inherits (pktable_base);
create table pktable(ptest1 inet, ptest2 inet, primary key(base1, ptest1), foreign key(ptest2, base2) references
                                             pktable(base1, ptest1)) inherits (pktable_base);
drop table pktable;
drop table pktable_base;

--
-- Deferrable constraints
--		(right now, only FOREIGN KEY constraints can be deferred)
--

-- deferrable, explicitly deferred
CREATE TABLE pktable (
	id		INT4 PRIMARY KEY,
	other	INT4
) DISTRIBUTE BY REPLICATION;

CREATE TABLE fktable (
	id		INT4 PRIMARY KEY,
	fk		INT4 REFERENCES pktable DEFERRABLE
) DISTRIBUTE BY REPLICATION;

-- default to immediate: should fail
INSERT INTO fktable VALUES (5, 10);

-- explicitly defer the constraint
BEGIN;

SET CONSTRAINTS ALL DEFERRED;

INSERT INTO fktable VALUES (10, 15);
INSERT INTO pktable VALUES (15, 0); -- make the FK insert valid

COMMIT;

DROP TABLE fktable, pktable;

-- deferrable, initially deferred
CREATE TABLE pktable (
	id		INT4 PRIMARY KEY,
	other	INT4
) DISTRIBUTE BY REPLICATION;

CREATE TABLE fktable (
	id		INT4 PRIMARY KEY,
	fk		INT4 REFERENCES pktable DEFERRABLE INITIALLY DEFERRED
) DISTRIBUTE BY REPLICATION;

-- default to deferred, should succeed
BEGIN;

INSERT INTO fktable VALUES (100, 200);
INSERT INTO pktable VALUES (200, 500); -- make the FK insert valid

COMMIT;

-- default to deferred, explicitly make immediate
BEGIN;

SET CONSTRAINTS ALL IMMEDIATE;

-- should fail
INSERT INTO fktable VALUES (500, 1000);

COMMIT;

DROP TABLE fktable, pktable;

-- tricky behavior: according to SQL99, if a deferred constraint is set
-- to 'immediate' mode, it should be checked for validity *immediately*,
-- not when the current transaction commits (i.e. the mode change applies
-- retroactively)
CREATE TABLE pktable (
	id		INT4 PRIMARY KEY,
	other	INT4
) DISTRIBUTE BY REPLICATION;

CREATE TABLE fktable (
	id		INT4 PRIMARY KEY,
	fk		INT4 REFERENCES pktable DEFERRABLE
) DISTRIBUTE BY REPLICATION;

BEGIN;

SET CONSTRAINTS ALL DEFERRED;

-- should succeed, for now
INSERT INTO fktable VALUES (1000, 2000);

-- should cause transaction abort, due to preceding error
SET CONSTRAINTS ALL IMMEDIATE;

INSERT INTO pktable VALUES (2000, 3); -- too late

COMMIT;

DROP TABLE fktable, pktable;

-- deferrable, initially deferred
CREATE TABLE pktable (
	id		INT4 PRIMARY KEY,
	other	INT4
) DISTRIBUTE BY REPLICATION;

CREATE TABLE fktable (
	id		INT4 PRIMARY KEY,
	fk		INT4 REFERENCES pktable DEFERRABLE INITIALLY DEFERRED
) DISTRIBUTE BY REPLICATION;

BEGIN;

-- no error here
INSERT INTO fktable VALUES (100, 200);

-- error here on commit
COMMIT;

DROP TABLE pktable, fktable;

-- test notice about expensive referential integrity checks,
-- where the index cannot be used because of type incompatibilities.
CREATE TEMP TABLE pktable (
        id1     INT4 PRIMARY KEY,
        id2     VARCHAR(4) UNIQUE,
        id3     REAL UNIQUE,
        UNIQUE(id1, id2, id3)
) DISTRIBUTE BY REPLICATION;

CREATE TEMP TABLE fktable (
        x1      INT4 REFERENCES pktable(id1),
        x2      VARCHAR(4) REFERENCES pktable(id2),
        x3      REAL REFERENCES pktable(id3),
        x4      TEXT,
        x5      INT2
) DISTRIBUTE BY REPLICATION;

-- check individual constraints with alter table.

-- should fail

-- varchar does not promote to real
ALTER TABLE fktable ADD CONSTRAINT fk_2_3
FOREIGN KEY (x2) REFERENCES pktable(id3);

-- nor to int4
ALTER TABLE fktable ADD CONSTRAINT fk_2_1
FOREIGN KEY (x2) REFERENCES pktable(id1);

-- real does not promote to int4
ALTER TABLE fktable ADD CONSTRAINT fk_3_1
FOREIGN KEY (x3) REFERENCES pktable(id1);

-- int4 does not promote to text
ALTER TABLE fktable ADD CONSTRAINT fk_1_2
FOREIGN KEY (x1) REFERENCES pktable(id2);

-- should succeed

-- int4 promotes to real
ALTER TABLE fktable ADD CONSTRAINT fk_1_3
FOREIGN KEY (x1) REFERENCES pktable(id3);

-- text is compatible with varchar
ALTER TABLE fktable ADD CONSTRAINT fk_4_2
FOREIGN KEY (x4) REFERENCES pktable(id2);

-- int2 is part of integer opfamily as of 8.0
ALTER TABLE fktable ADD CONSTRAINT fk_5_1
FOREIGN KEY (x5) REFERENCES pktable(id1);

-- check multikey cases, especially out-of-order column lists

-- these should work

ALTER TABLE fktable ADD CONSTRAINT fk_123_123
FOREIGN KEY (x1,x2,x3) REFERENCES pktable(id1,id2,id3);

ALTER TABLE fktable ADD CONSTRAINT fk_213_213
FOREIGN KEY (x2,x1,x3) REFERENCES pktable(id2,id1,id3);

ALTER TABLE fktable ADD CONSTRAINT fk_253_213
FOREIGN KEY (x2,x5,x3) REFERENCES pktable(id2,id1,id3);

-- these should fail

ALTER TABLE fktable ADD CONSTRAINT fk_123_231
FOREIGN KEY (x1,x2,x3) REFERENCES pktable(id2,id3,id1);

ALTER TABLE fktable ADD CONSTRAINT fk_241_132
FOREIGN KEY (x2,x4,x1) REFERENCES pktable(id1,id3,id2);

DROP TABLE pktable, fktable;

-- test a tricky case: we can elide firing the FK check trigger during
-- an UPDATE if the UPDATE did not change the foreign key
-- field. However, we can't do this if our transaction was the one that
-- created the updated row and the trigger is deferred, since our UPDATE
-- will have invalidated the original newly-inserted tuple, and therefore
-- cause the on-INSERT RI trigger not to be fired.

CREATE TEMP TABLE pktable (
    id int primary key,
    other int
) DISTRIBUTE BY REPLICATION;

CREATE TEMP TABLE fktable (
    id int primary key,
    fk int references pktable deferrable initially deferred
) DISTRIBUTE BY REPLICATION;

INSERT INTO pktable VALUES (5, 10);

BEGIN;

-- doesn't match PK, but no error yet
INSERT INTO fktable VALUES (0, 20);

-- don't change FK
UPDATE fktable SET id = id + 1;

-- should catch error from initial INSERT
COMMIT;

-- check same case when insert is in a different subtransaction than update

BEGIN;

-- doesn't match PK, but no error yet
INSERT INTO fktable VALUES (0, 20);

-- UPDATE will be in a subxact
SAVEPOINT savept1;

-- don't change FK
UPDATE fktable SET id = id + 1;

-- should catch error from initial INSERT
COMMIT;

BEGIN;

-- INSERT will be in a subxact
SAVEPOINT savept1;

-- doesn't match PK, but no error yet
INSERT INTO fktable VALUES (0, 20);

RELEASE SAVEPOINT savept1;

-- don't change FK
UPDATE fktable SET id = id + 1;

-- should catch error from initial INSERT
COMMIT;

BEGIN;

-- doesn't match PK, but no error yet
INSERT INTO fktable VALUES (0, 20);

-- UPDATE will be in a subxact
SAVEPOINT savept1;

-- don't change FK
UPDATE fktable SET id = id + 1;

-- Roll back the UPDATE
ROLLBACK TO savept1;

-- should catch error from initial INSERT
COMMIT;

--
-- check ALTER CONSTRAINT
--

INSERT INTO fktable VALUES (1, 5);

ALTER TABLE fktable ALTER CONSTRAINT fktable_fk_fkey DEFERRABLE INITIALLY IMMEDIATE;

BEGIN;

-- doesn't match FK, should throw error now
UPDATE pktable SET id = 10 WHERE id = 5;

COMMIT;

BEGIN;

-- doesn't match PK, should throw error now
INSERT INTO fktable VALUES (0, 20);

COMMIT;

-- try additional syntax
ALTER TABLE fktable ALTER CONSTRAINT fktable_fk_fkey NOT DEFERRABLE;
-- illegal option
ALTER TABLE fktable ALTER CONSTRAINT fktable_fk_fkey NOT DEFERRABLE INITIALLY DEFERRED;

-- test order of firing of FK triggers when several RI-induced changes need to
-- be made to the same row.  This was broken by subtransaction-related
-- changes in 8.0.

CREATE TEMP TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL
) DISTRIBUTE BY REPLICATION;

INSERT INTO users VALUES (1, 'Jozko');
INSERT INTO users VALUES (2, 'Ferko');
INSERT INTO users VALUES (3, 'Samko');

CREATE TEMP TABLE tasks (
  id INT PRIMARY KEY,
  owner INT REFERENCES users ON UPDATE CASCADE ON DELETE SET NULL,
  worker INT REFERENCES users ON UPDATE CASCADE ON DELETE SET NULL,
  checked_by INT REFERENCES users ON UPDATE CASCADE ON DELETE SET NULL
) DISTRIBUTE BY REPLICATION;

INSERT INTO tasks VALUES (1,1,NULL,NULL);
INSERT INTO tasks VALUES (2,2,2,NULL);
INSERT INTO tasks VALUES (3,3,3,3);

SELECT * FROM tasks ORDER BY 1, 2, 3,4;

UPDATE users SET id = 4 WHERE id = 3;

SELECT * FROM tasks ORDER BY 1, 2, 3,4;

DELETE FROM users WHERE id = 4;

SELECT * FROM tasks ORDER BY 1, 2, 3,4;

-- could fail with only 2 changes to make, if row was already updated
BEGIN;
UPDATE tasks set id=id WHERE id=2;
SELECT * FROM tasks ORDER BY 1, 2, 3,4;
DELETE FROM users WHERE id = 2;
SELECT * FROM tasks ORDER BY 1, 2, 3,4;
COMMIT;

--
-- Test self-referential FK with CASCADE (bug #6268)
--
create temp table selfref (
    a int primary key,
    b int,
    foreign key (b) references selfref (a)
        on update cascade on delete cascade
) DISTRIBUTE BY REPLICATION;

insert into selfref (a, b)
values
    (0, 0),
    (1, 1);

begin;
    update selfref set a = 123 where a = 0;
    select a, b from selfref;
    update selfref set a = 456 where a = 123;
    select a, b from selfref;
commit;

--
-- Test that SET DEFAULT actions recognize updates to default values
--
create temp table defp (f1 int primary key);
create temp table defc (f1 int default 0
                        references defp on delete set default);
insert into defp values (0), (1), (2);
insert into defc values (2);
select * from defc;
delete from defp where f1 = 2;
select * from defc;
delete from defp where f1 = 0; -- fail
alter table defc alter column f1 set default 1;
delete from defp where f1 = 0;
select * from defc;
delete from defp where f1 = 1; -- fail

--
-- Test the difference between NO ACTION and RESTRICT
--
create temp table pp (f1 int primary key);
create temp table cc (f1 int references pp on update no action);
insert into pp values(12);
insert into pp values(11);
update pp set f1=f1+1;
insert into cc values(13);
update pp set f1=f1+1;
update pp set f1=f1+1; -- fail
drop table pp, cc;

create temp table pp (f1 int primary key);
create temp table cc (f1 int references pp on update restrict);
insert into pp values(12);
insert into pp values(11);
update pp set f1=f1+1;
insert into cc values(13);
update pp set f1=f1+1; -- fail
drop table pp, cc;

--
-- Test interaction of foreign-key optimization with rules (bug #14219)
--
create temp table t1 (a integer primary key, b text);
create temp table t2 (a integer, b integer references t1) distribute by hash (b);
create rule r1 as on delete to t1 do delete from t2 where t2.b = old.a;

explain (costs off) delete from t1 where a = 1;
delete from t1 where a = 1;

drop rule r1 on t1;

explain (costs off, nodes off) delete from t1 where a = 1;
delete from t1 where a = 1;
--
-- Test deferred FK check on a tuple deleted by a rolled-back subtransaction
--
create table pktable2(f1 int primary key);
create table fktable2(f1 int references pktable2 deferrable initially deferred);
insert into pktable2 values(1);

-- Since subtransactions are not supported in XL, these tests make no sense

-- begin;
-- insert into fktable2 values(1);
-- savepoint x;
-- delete from fktable2;
-- rollback to x;
-- commit;
-- 
-- begin;
-- insert into fktable2 values(2);
-- savepoint x;
-- delete from fktable2;
-- rollback to x;
-- commit; -- fail

--
-- Test that we prevent dropping FK constraint with pending trigger events
--
begin;
insert into fktable2 values(2);
alter table fktable2 drop constraint fktable2_f1_fkey;
commit;

begin;
delete from pktable2 where f1 = 1;
alter table fktable2 drop constraint fktable2_f1_fkey;
commit;

drop table pktable2, fktable2;

-- Altering a type referenced by a foreign key needs to drop/recreate the FK.
-- Ensure that works.
CREATE TABLE fk_notpartitioned_pk (a INT, PRIMARY KEY(a), CHECK (a > 0));
CREATE TABLE fk_partitioned_fk (a INT REFERENCES fk_notpartitioned_pk(a) PRIMARY KEY) PARTITION BY RANGE(a);
CREATE TABLE fk_partitioned_fk_1 PARTITION OF fk_partitioned_fk FOR VALUES FROM (MINVALUE) TO (MAXVALUE);
INSERT INTO fk_notpartitioned_pk VALUES (1);
INSERT INTO fk_partitioned_fk VALUES (1);
ALTER TABLE fk_notpartitioned_pk ALTER COLUMN a TYPE bigint;
DELETE FROM fk_notpartitioned_pk WHERE a = 1;
DROP TABLE fk_notpartitioned_pk, fk_partitioned_fk;

-- ensure we check partitions are "not used" when dropping constraints
CREATE SCHEMA fkpart8
  CREATE TABLE tbl1(f1 int PRIMARY KEY)
  CREATE TABLE tbl2(f1 int REFERENCES tbl1 DEFERRABLE INITIALLY DEFERRED) PARTITION BY RANGE(f1)
  CREATE TABLE tbl2_p1 PARTITION OF tbl2 FOR VALUES FROM (minvalue) TO (maxvalue);
INSERT INTO fkpart8.tbl1 VALUES(1);
BEGIN;
INSERT INTO fkpart8.tbl2 VALUES(1);
ALTER TABLE fkpart8.tbl2 DROP CONSTRAINT tbl2_f1_fkey;
COMMIT;
DROP SCHEMA fkpart8 CASCADE;

CREATE TABLE IF NOT EXISTS rep_tbl_foreign(
col_int int not null,
col_smallint smallint,
col_bigint bigint check (col_bigint > 0),
col_nemeric numeric(5,5),
col_double double precision,
constraint pk_foreign_col_int primary key (col_int,col_double)
) DISTRIBUTE BY REPLICATION;

CREATE TABLE IF NOT EXISTS rep_tbl_constraint(
col_int int not null,
col_smallint smallint,
col_bigint bigint check (col_bigint > 0),
col_nemeric numeric(5,5),
col_double double precision,
col_varchar varchar(50) not null default 'default values',
col_text text,
col_timestamp timestamp,
col_date date,
col_boolean boolean,
col_bigserial bigserial,
constraint pk_constraint_col_int primary key (col_int),
constraint un_constraint_col_int_bigserial unique (col_int,col_bigserial),
constraint fk_constraint_col_int_double foreign key(col_int,col_double) references rep_tbl_foreign(col_int,col_double)
);

DROP TABLE IF EXISTS rep_tbl_foreign;
CREATE TABLE IF NOT EXISTS rep_tbl_foreign(
col_int int not null,
col_smallint smallint,
col_bigint bigint check (col_bigint > 0),
col_nemeric numeric(5,5),
col_double double precision,
constraint pk_foreign_col_int primary key (col_int,col_double)
);

DROP TABLE IF EXISTS rep_tbl_constraint;
CREATE TABLE IF NOT EXISTS rep_tbl_constraint(
col_int int not null,
col_smallint smallint,
col_bigint bigint check (col_bigint > 0),
col_nemeric numeric(5,5),
col_double double precision,
col_varchar varchar(50) not null default 'default values',
col_text text,
col_timestamp timestamp,
col_date date,
col_boolean boolean,
col_bigserial bigserial,
constraint pk_constraint_col_int primary key (col_int),
constraint un_constraint_col_int_bigserial unique (col_int,col_bigserial),
constraint fk_constraint_col_int_double foreign key(col_int,col_double) references rep_tbl_foreign(col_int,col_double)
);

-- Test inconsistency of distribution keys
drop table if exists t_copy_tri_20220622_tab1;
create table t_copy_tri_20220622_tab1(id int,id1 int,id2 date);
insert into t_copy_tri_20220622_tab1 values(1,1,'2020-12-20');
insert into t_copy_tri_20220622_tab1 values(2,2,NULL);
insert into t_copy_tri_20220622_tab1 values(3,3,'2021-12-20');
insert into t_copy_tri_20220622_tab1 values(4,4,null);
insert into t_copy_tri_20220622_tab1 values(5,5,'2022-12-20');
drop table if exists t_copy_tri_from_20220622_tb4;
create table t_copy_tri_from_20220622_tb4 as select * from t_copy_tri_20220622_tab1;
alter table t_copy_tri_from_20220622_tb4 add constraint ck1 primary key(id);

drop table if exists t_copy_tri_from_20220622_tb3;
create table t_copy_tri_from_20220622_tb3
(f1 int, id integer default 10 not null, id1 int, id2 date,
foreign key (id1) references t_copy_tri_from_20220622_tb4(id)
);

alter table t_copy_tri_from_20220622_tb3 drop column id;
insert into t_copy_tri_from_20220622_tb3(f1,id1,id2) values(4,1,'2020-07-20'),(5,1,'2020-07-20');
select * from t_copy_tri_from_20220622_tb3 order by 1,2,3;

drop table t_copy_tri_from_20220622_tb3;
drop table t_copy_tri_from_20220622_tb4;
drop table t_copy_tri_20220622_tab1;

--create and drop foreign key
drop table if exists t_foreignkey_ref_itself; 
CREATE TABLE t_foreignkey_ref_itself ( --ERROR
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 FOREIGN KEY (item_id) REFERENCES t_foreignkey_ref_itself (so_id)
);

CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id),
 FOREIGN KEY (item_id) REFERENCES t_foreignkey_ref_itself (so_id)
);
\d+ t_foreignkey_ref_itself
alter table t_foreignkey_ref_itself drop constraint t_foreignkey_ref_itself_item_id_fkey;

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (item_id),
 FOREIGN KEY (item_id) REFERENCES t_foreignkey_ref_itself (item_id)
);
\d+ t_foreignkey_ref_itself
alter table t_foreignkey_ref_itself drop constraint t_foreignkey_ref_itself_item_id_fkey;

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself ( --ERROR in distribute
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id),
 FOREIGN KEY (item_id) REFERENCES t_foreignkey_ref_itself (so_id)
) distribute By replication;

CREATE TABLE t_foreignkey_ref_itself ( --ERROR
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id),
 FOREIGN KEY (item_id) REFERENCES t_foreignkey_ref_itself (so_id)
) partition by range (so_id) distribute By shard(so_id);

drop table if exists t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id),
 FOREIGN KEY (item_id) REFERENCES t_foreignkey_ref_itself (so_id)
) distribute By shard(so_id);
\d+ t_foreignkey_ref_itself
alter table t_foreignkey_ref_itself drop constraint t_foreignkey_ref_itself_item_id_fkey;

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself ( --ERROR
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id, qty),
 FOREIGN KEY (item_id) REFERENCES t_foreignkey_ref_itself (so_id, qty)
) distribute By shard(so_id);

CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id, qty),
 FOREIGN KEY (item_id, product_id) REFERENCES t_foreignkey_ref_itself (so_id, qty)
) distribute By shard(so_id);
\d+ t_foreignkey_ref_itself

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id, qty),
 FOREIGN KEY (item_id, product_id) REFERENCES t_foreignkey_ref_itself (so_id, qty),
 FOREIGN KEY (product_id, item_id) REFERENCES t_foreignkey_ref_itself (so_id, qty)
) distribute By shard(so_id);

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric
) distribute By shard(so_id);
alter table t_foreignkey_ref_itself add constraint t_foreignkey_ref_itself_item_id_fkey foreign key(item_id) REFERENCES _foreignkey_ref_itself (so_id); --ERROR
alter table t_foreignkey_ref_itself add primary key (so_id);
alter table t_foreignkey_ref_itself add constraint t_foreignkey_ref_itself_item_id_fkey foreign key(item_id) REFERENCES _foreignkey_ref_itself (so_id);
alter table t_foreignkey_ref_itself add constraint t_foreignkey_ref_itself_product_id_fkey foreign key(product_id) REFERENCES _foreignkey_ref_itself (so_id);
\d+ t_foreignkey_ref_itself
alter table t_foreignkey_ref_itself drop constraint t_foreignkey_ref_itself_item_id_fkey;
alter table t_foreignkey_ref_itself drop constraint t_foreignkey_ref_itself_product_id_fkey;

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself ( --ERROR
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id, qty)
) distribute By shard(so_id);
alter table t_foreignkey_ref_itself add constraint t_foreignkey_ref_itself_item_id_fkey foreign key(item_id) REFERENCES _foreignkey_ref_itself (so_id);
alter table t_foreignkey_ref_itself drop constraint t_foreignkey_ref_itself_item_id_fkey;

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id, qty)
) distribute By shard(so_id);
alter table t_foreignkey_ref_itself add constraint t_foreignkey_ref_itself_item_id_fkey foreign key(item_id, product_id) REFERENCES _foreignkey_ref_itself (so_id, qty);
alter table t_foreignkey_ref_itself drop constraint t_foreignkey_ref_itself_item_id_fkey;

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id, qty)
) distribute By replication;
alter table t_foreignkey_ref_itself add constraint t_foreignkey_ref_itself_item_id_fkey foreign key(item_id, product_id) REFERENCES _foreignkey_ref_itself (so_id, qty);--ERROR

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id)
) partition by range (so_id) distribute By shard(so_id);
alter table t_foreignkey_ref_itself add constraint t_foreignkey_ref_itself_item_id_fkey foreign key(item_id) REFERENCES _foreignkey_ref_itself (so_id);--ERROR

drop table t_foreignkey_ref_itself;
CREATE TABLE t_foreignkey_ref_itself (
 so_id int4,
 item_id int4,
 product_id int4,
 qty int4,
 net_price numeric,
 PRIMARY KEY (so_id)
) distribute By shard(so_id);
alter table t_foreignkey_ref_itself add constraint t_foreignkey_ref_itself_item_id_fkey foreign key(item_id) REFERENCES _foreignkey_ref_itself (so_id);

--insert/update/delete check
insert into t_foreignkey_ref_itself select t,t,t,t,t from generate_series(1,10) as t;
select * from t_foreignkey_ref_itself order by so_id;

insert into t_foreignkey_ref_itself(so_id, item_id, product_id) values (11,12,13);--ERROR
insert into t_foreignkey_ref_itself(so_id, item_id, product_id) values (11,12,11);--ERROR
insert into t_foreignkey_ref_itself(so_id, item_id, product_id) values (11,11,12);
select * from t_foreignkey_ref_itself order by so_id;

update t_foreignkey_ref_itself set item_id = 100 where item_id = 1;--ERROR
update t_foreignkey_ref_itself set item_id = 15 where item_id = 10;--ERROR
update t_foreignkey_ref_itself set item_id = 5 where item_id = 10;
select * from t_foreignkey_ref_itself order by so_id;

delete from t_foreignkey_ref_itself where so_id = 5;--ERROR
delete from t_foreignkey_ref_itself where so_id = 1;
delete from t_foreignkey_ref_itself where so_id = 10;
delete from t_foreignkey_ref_itself where so_id = 5;
select * from t_foreignkey_ref_itself order by so_id;

--insert on conflict
insert into t_foreignkey_ref_itself values(2,12,12,12,12) on conflict(so_id) do UPDATE SET item_id = 13;--ERROR
insert into t_foreignkey_ref_itself values(2,12,12,12,12) on conflict(so_id) do UPDATE SET item_id = 8;
select * from t_foreignkey_ref_itself order by so_id;

drop table if exists t_foreignkey_ref_itself;
