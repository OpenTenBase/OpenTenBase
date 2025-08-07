CALL nonexistent();  -- error

CREATE FUNCTION testfunc1(a int) RETURNS int LANGUAGE SQL AS $$ SELECT a $$;

CREATE TABLE cp_test (a int, b text);

CREATE PROCEDURE ptest1(x text)
LANGUAGE SQL
AS $$
INSERT INTO cp_test VALUES (1, x);
$$;

\df ptest1
SELECT pg_get_functiondef('ptest1'::regproc);

-- show only normal functions
\dfn public.*test*1

-- show only procedures
\dfp public.*test*1

SELECT ptest1('x');  -- error
CALL ptest1('a');  -- ok
CALL ptest1('xy' || 'zzy');  -- ok, constant-folded arg
CALL ptest1(substring(random()::numeric(20,15)::text, 1, 1));  -- ok, volatile arg

SELECT * FROM cp_test ORDER BY b COLLATE "C";


CREATE PROCEDURE ptest2()
LANGUAGE SQL
AS $$
SELECT 5;
$$;

CALL ptest2();


-- nested CALL
TRUNCATE cp_test;

CREATE PROCEDURE ptest3(y text)
LANGUAGE SQL
AS $$
CALL ptest1(y);
CALL ptest1($1);
$$;

CALL ptest3('b');

SELECT * FROM cp_test;


-- output arguments

CREATE PROCEDURE ptest4a(INOUT a int, INOUT b int)
LANGUAGE SQL
AS $$
SELECT 1, 2;
$$;

CALL ptest4a(NULL, NULL);

CREATE PROCEDURE ptest4b(INOUT b int, INOUT a int)
LANGUAGE SQL
AS $$
CALL ptest4a(a, b);  -- error, not supported
$$;

DROP PROCEDURE ptest4a;


-- named and default parameters

CREATE OR REPLACE PROCEDURE ptest5(a int, b text, c int default 100)
LANGUAGE SQL
AS $$
INSERT INTO cp_test VALUES(a, b);
INSERT INTO cp_test VALUES(c, b);
$$;

TRUNCATE cp_test;

CALL ptest5(10, 'Hello', 20);
CALL ptest5(10, 'Hello');
CALL ptest5(10, b => 'Hello');
CALL ptest5(b => 'Hello', a => 10);

SELECT * FROM cp_test;


-- polymorphic types

CREATE PROCEDURE ptest6(a int, b anyelement)
LANGUAGE SQL
AS $$
SELECT NULL::int;
$$;

CALL ptest6(1, 2);


-- collation assignment

CREATE PROCEDURE ptest7(a text, b text)
LANGUAGE SQL
AS $$
SELECT a = b;
$$;

CALL ptest7(least('a', 'b'), 'a');


-- various error cases, adaptation for LIGHTWEIGHT_ORA. We support calling functions and will not report an error in this case.
CALL sum(1);

CREATE PROCEDURE ptestx() LANGUAGE SQL WINDOW AS $$ INSERT INTO cp_test VALUES (1, 'a') $$;
CREATE PROCEDURE ptestx() LANGUAGE SQL STRICT AS $$ INSERT INTO cp_test VALUES (1, 'a') $$;
CREATE PROCEDURE ptestx(OUT a int) LANGUAGE SQL AS $$ INSERT INTO cp_test VALUES (1, 'a') $$;

ALTER PROCEDURE ptest1(text) STRICT;
ALTER FUNCTION ptest1(text) VOLATILE;  -- error: not a function
ALTER PROCEDURE testfunc1(int) VOLATILE;  -- error: not a procedure
ALTER PROCEDURE nonexistent() VOLATILE;

DROP FUNCTION ptest1(text);  -- error: not a function
DROP PROCEDURE testfunc1(int);  -- error: not a procedure
DROP PROCEDURE nonexistent();


-- privileges

CREATE USER regress_user1;
GRANT INSERT ON cp_test TO regress_user1;
REVOKE EXECUTE ON PROCEDURE ptest1(text) FROM PUBLIC;
SET ROLE regress_user1;
CALL ptest1('a');  -- error
RESET ROLE;
GRANT EXECUTE ON PROCEDURE ptest1(text) TO regress_user1;
SET ROLE regress_user1;
CALL ptest1('a');  -- ok
RESET ROLE;


-- ROUTINE syntax

ALTER ROUTINE testfunc1(int) RENAME TO testfunc1a;
ALTER ROUTINE testfunc1a RENAME TO testfunc1;

ALTER ROUTINE ptest1(text) RENAME TO ptest1a;
ALTER ROUTINE ptest1a RENAME TO ptest1;

DROP ROUTINE testfunc1(int);


-- cleanup

DROP PROCEDURE ptest1;
DROP PROCEDURE ptest2;

DROP TABLE cp_test;

DROP USER regress_user1;

--
-- test opentenbase_ora/LIGHTWEIGHT_ORA output function package
--
SET SERVEROUTPUT = ON;
CALL enable(1999);
CALL enable(1000001);
CALL enable(2000);
CALL put_line('hello world!');
CALL put('hello world again... ');
CALL put(null);
CALL put('and again!!');
CALL new_line();
CALL disable();
CALL put_line('goodbye world!!!');
CALL enable();
CALL put_line('goodbye world!!!');
CALL put_line(null);
RESET SERVEROUTPUT;
