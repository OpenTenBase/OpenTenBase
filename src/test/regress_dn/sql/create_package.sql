\c regression_ora
--
-- CREATE_FUNCTION_2
--
CREATE PACKAGE pkghobby as 
$p$
    FUNCTION hobbies(person) RETURNS setof hobbies_r;
    FUNCTION hobby_construct(text, text) RETURNS hobbies_r;
$p$;

CREATE PACKAGE BODY pkghobby as
$p$
    FUNCTION hobbies(person)
        RETURNS setof hobbies_r
        AS 'select * from hobbies_r where person = $1.name'
        LANGUAGE SQL;

    FUNCTION hobby_construct(text, text)
        RETURNS hobbies_r
        AS 'select $1 as name, $2 as hobby'
        LANGUAGE SQL;
$p$;

-- cannot drop function which defined in package
DROP FUNCTION pkghobby.hobbies;

SELECT pkghobby.hobby_construct('n1', 'l2');
DROP PACKAGE BODY pkghobby;
SELECT pkghobby.hobby_construct('n1', 'l2');
DROP PACKAGE pkghobby;

CREATE PACKAGE body as
$p$
    FUNCTION hobbies(person) RETURNS setof hobbies_r;
    FUNCTION hobby_construct(text, text) RETURNS hobbies_r;
$p$;

CREATE PACKAGE BODY body as
$p$
    FUNCTION hobbies(person)
        RETURNS setof hobbies_r
        AS 'select * from hobbies_r where person = $1.name'
        LANGUAGE SQL;

    FUNCTION hobby_construct(text, text)
        RETURNS hobbies_r
        AS 'select $1 as name, $2 as hobby'
        LANGUAGE SQL;
$p$;

-- create package body before create package
CREATE PACKAGE BODY pkghobby as
$p$
    FUNCTION hobbies(person)
        RETURNS setof hobbies_r
        AS 'select * from hobbies_r where person = $1.name'
        LANGUAGE SQL;

    FUNCTION hobby_construct(text, text)
        RETURNS hobbies_r
        AS 'select $1 as name, $2 as hobby'
        LANGUAGE SQL;
$p$;

CREATE PACKAGE pkghobby as
$p$
    FUNCTION hobbies(person) RETURNS setof hobbies_r;
    FUNCTION hobby_construct(text, text) RETURNS hobbies_r;
$p$;

-- create package body which fucntion is not declare in package header
CREATE PACKAGE BODY pkghobby as
$p$
    FUNCTION hobbies(person)
        RETURNS setof hobbies_r
        AS 'select * from hobbies_r where person = $1.name'
        LANGUAGE SQL;

    FUNCTION hobby_construct4other(text, text)
        RETURNS hobbies_r
        AS 'select $1 as name, $2 as hobby'
        LANGUAGE SQL;
$p$;

CREATE PACKAGE BODY pkghobby as
$p$
    FUNCTION hobbies(person)
        RETURNS setof hobbies_r
        AS 'select * from hobbies_r where person = $1.name'
        LANGUAGE SQL;

    FUNCTION hobby_construct(text, text)
        RETURNS hobbies_r
        AS 'select $1 as name, $2 as hobby'
        LANGUAGE SQL;
$p$;

-- replace
CREATE OR REPLACE PACKAGE pkghobby as
$p$
    FUNCTION hobbies(person) RETURNS setof hobbies_r;
    FUNCTION hobby_construct4other(text, text) RETURNS hobbies_r;
$p$;

CREATE OR REPLACE PACKAGE BODY pkghobby as
$p$
    FUNCTION hobbies(person)
        RETURNS setof hobbies_r
        AS 'select * from hobbies_r where person = $1.name'
        LANGUAGE SQL;

    FUNCTION hobby_construct4other(text, text)
        RETURNS hobbies_r
        AS 'select $1 as name, $2 as hobby'
        LANGUAGE SQL;
$p$;

SELECT pkghobby.hobby_construct4other('n1', 'l2');
-- test multi-line comments
CREATE OR REPLACE PACKAGE test AS
-- multi
-- line
-- comments
END test;
/

CREATE OR REPLACE PACKAGE BODY test AS
-- multi
-- line
-- comments
	var1 varchar2(4096) := 'test';
	function func return varchar2 is
    var2 INTEGER;
  begin 
    var2 = 12;
  return gstr;
  end func;
END test;
/

/*
 * test end the procedures named unreserved keywords in 
 * format of 'END {unreserved_keyword};'
 */
 -- log
CREATE OR REPLACE PACKAGE PLOG IS 
PROCEDURE log();
END PLOG;
/

CREATE OR REPLACE PACKAGE BODY PLOG IS 
PROCEDURE log() is
BEGIN
    raise notice 'function:log';
END log;
END PLOG;
/
call plog.log();

-- error
CREATE OR REPLACE PACKAGE PLOG IS 
PROCEDURE error();
END PLOG;
/

CREATE OR REPLACE PACKAGE BODY PLOG IS 
PROCEDURE error() IS
BEGIN
    raise notice 'function:error';
END error;
END PLOG;
/
call plog.error();

-- debug
CREATE OR REPLACE PACKAGE PLOG IS 
PROCEDURE debug();
END PLOG;
/

CREATE OR REPLACE PACKAGE BODY PLOG IS 
PROCEDURE debug() IS
BEGIN
    raise notice 'function:debug';
END debug;
END PLOG;
/
call plog.debug();