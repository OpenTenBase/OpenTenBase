\c opentenbase_ora_package_function_regression_ora
\set ECHO none
SET client_min_messages = warning;
SET DATESTYLE TO ISO;
SET client_encoding = utf8;
\pset null '<NULL>'
\set ECHO all

DROP FUNCTION dbms_output_test();
DROP TABLE dbms_output_test;

-- CALL dbms_output.DISABLE [0]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
SELECT dbms_output_test(); -- bug during testing
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.PUT_LINE [1]
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff1	VARCHAR(20) := 'orafce';
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.PUT_LINE ('ORAFCE');
	CALL dbms_output.PUT_LINE (buff1);
	CALL dbms_output.PUT ('ABC');
	CALL dbms_output.PUT_LINE ('');
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.PUT_LINE [2]
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.PUT_LINE ('ORA
F
CE');
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.PUT [1]
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff1	VARCHAR(20) := 'ora';
	buff2	VARCHAR(20) := 'f';
	buff3	VARCHAR(20) := 'ce';
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.PUT ('ORA');
	CALL dbms_output.PUT ('F');
	CALL dbms_output.PUT ('CE');
	CALL dbms_output.PUT_LINE ('');
	CALL dbms_output.PUT ('ABC');
	CALL dbms_output.PUT_LINE ('');
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.PUT [2]
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.PUT ('ORA
F
CE');
	CALL dbms_output.PUT_LINE ('');
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINE [1]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
SELECT * FROM dbms_output_test order by buff;
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINE [2]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 3');
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINE [3]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.PUT ('ORA');
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINE [4]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.NEW_LINE();
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINE [5]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1
');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINE [6]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORA
F
CE');
	CALL dbms_output.GET_LINE(buff, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINES [1]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	buff1	VARCHAR2(20);
	buff2	VARCHAR2(20);
	buff3	VARCHAR2(20);
	stts	INTEGER := 10;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 3');
	CALL dbms_output.GET_LINES(lines, stts);
	buff1 = lines[1];
	buff2 = lines[2];
	buff3 = lines[3];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff1, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff2, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff3, stts);
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINES [2]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	buff1	VARCHAR2(20);
	buff2	VARCHAR2(20);
	stts	INTEGER := 2;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 3');
	CALL dbms_output.GET_LINES(lines, stts);
	buff1 = lines[1];
	buff2 = lines[2];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff1, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff2, stts);
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINES [3]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER := 1;
	lines   VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 3');
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINES [4]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER := 1;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.PUT ('ORA');
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINES [5]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER := 1;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.NEW_LINE();
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.GET_LINES [6]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER := 1;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORA
F
CE');
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.NEW_LINE [1]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff1	VARCHAR2(20);
	buff2	VARCHAR2(20);
	stts	INTEGER := 10;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT ('ORA');
	CALL dbms_output.NEW_LINE();
	CALL dbms_output.PUT ('FCE');
	CALL dbms_output.NEW_LINE();
	CALL dbms_output.GET_LINES(lines, stts);
	buff1 = lines[1];
	buff2 = lines[2];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff1, stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff2, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.NEW_LINE [2]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(3000), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff1	VARCHAR(3000);
	stts	INTEGER := 10;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.ENABLE(2000);
	FOR j IN 1..1999 LOOP
		CALL dbms_output.PUT ('A');
	END LOOP;
	CALL dbms_output.NEW_LINE();
	CALL dbms_output.GET_LINES(lines, stts);
	buff1 = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff1, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.DISABLE [1]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.GET_LINE(buff,stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);

	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.DISABLE();
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
	CALL dbms_output.ENABLE();
	CALL dbms_output.GET_LINE(buff,stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 3');
	CALL dbms_output.DISABLE();
	CALL dbms_output.GET_LINE(buff,stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.ENABLE();

	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.DISABLE();
	CALL dbms_output.PUT ('ORAFCE TEST 4');
	CALL dbms_output.ENABLE();
	CALL dbms_output.NEW_LINE();
	CALL dbms_output.GET_LINE(buff,stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);

	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.PUT ('ORAFCE TEST 5');
	CALL dbms_output.DISABLE();
	CALL dbms_output.NEW_LINE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.GET_LINE(buff,stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);

	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.DISABLE [2]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER := 10;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.DISABLE();
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.GET_LINES(lines,stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.ENABLE [1]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	status	INTEGER;
	num		INTEGER := 2000;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.ENABLE(2000);
	CALL dbms_output.PUT ('ORAFCE TEST 1');
	CALL dbms_output.NEW_LINE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.ENABLE [2]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.PUT ('ORAFCE TEST 2');
	CALL dbms_output.NEW_LINE();
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.ENABLE [3]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER := 10;
	lines	VARCHAR2[];
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.GET_LINES(lines, stts);
	buff = lines[1];
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);

END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.ENABLE [4]
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	FOR j IN 1..2000 LOOP
		CALL dbms_output.PUT ('A');
	END LOOP;
	CALL dbms_output.NEW_LINE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.ENABLE [5]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE(NULL);
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.GET_LINE(buff,stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- CALL dbms_output.ENABLE [6]
CREATE TABLE dbms_output_test (id serial,buff VARCHAR(20), status INTEGER);
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
DECLARE
	buff	VARCHAR2(20);
	stts	INTEGER;
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
	CALL dbms_output.ENABLE();
	CALL dbms_output.GET_LINE(buff,stts);
	INSERT INTO dbms_output_test(buff,status) VALUES (buff, stts);
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP TABLE dbms_output_test;
DROP FUNCTION dbms_output_test();

-- SERVEROUTPUT [1]
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
BEGIn
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 2');
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

-- SERVEROUTPUT [2]
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.PUT ('ORAFCE TEST 1');
	CALL dbms_output.NEW_LINE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.PUT ('ORAFCE TEST 2');
	CALL dbms_output.NEW_LINE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

-- SERVEROUTPUT [3]
CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('f');
	CALL dbms_output.DISABLE();
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

CREATE FUNCTION dbms_output_test() RETURNS VOID AS $$
BEGIN
	CALL dbms_output.DISABLE();
	CALL dbms_output.ENABLE();
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.PUT_LINE ('ORAFCE TEST 1');
END;
$$ LANGUAGE default_plsql;
SELECT dbms_output_test();
DROP FUNCTION dbms_output_test();

create or replace procedure dbms_output_putline_reg_test_type() as
$$
declare
	v_int integer := 1000000;
	v_float float := 11111111.222222;
	v_number number := 9876543221;
	v_numeric numeric(10, 2) := 123456.78;
begin
	CALL dbms_output.SERVEROUTPUT ('t');
	CALL dbms_output.put_line(v_int);
	CALL dbms_output.put_line(v_float);
	CALL dbms_output.put_line(v_number);
	CALL dbms_output.put_line(v_numeric);
end;
$$LANGUAGE default_plsql;
call dbms_output_putline_reg_test_type();
DROP procedure dbms_output_putline_reg_test_type;

create or replace procedure dbms_output_putline_reg_test_loop() as
$$
declare
i integer;
begin
	for i in 1..5 loop		
		CALL dbms_output.put_line('A');
	END LOOP;
	CALL dbms_output.new_line();
end;
$$LANGUAGE default_plsql;
call dbms_output_putline_reg_test_loop();
DROP  procedure dbms_output_putline_reg_test_loop;

-- Common users also have operation rights
declare
a int:=1;
BEGIN
CALL dbms_output.put_line('id='||a);
END;
/
drop user if exists a1;
create user a1 PASSWORD 'OpenTenBase123@';
alter role a1 with login;
declare
a int:=1;
BEGIN
CALL dbms_output.put_line('id='||a);
END;
/
reset role;
drop user a1;

drop table if exists dn_print_tbl;
create table dn_print_tbl(id int, name varchar2(20), score numeric(4,1));
insert into dn_print_tbl values(1, '�| �~I', 80.5);
insert into dn_print_tbl values(2, 'aaa', 90.5);
insert into dn_print_tbl values(3, 'bbb', 70.5);
drop table  dn_print_tbl;

declare
  number_result number := null;
  bytea_result bytea := null;
  blob_result blob := null;
  clob_result clob := null;
  raw_result raw := null;
  long_result long raw:= null;
  float_result float8 := null;
  bi_result binary_integer := null;
  pls_result pls_integer := null;
  int_result int := null;
  bif_result binary_float := null;
  bid_result binary_double := null;
  varc_result varchar2(200) := null;
  text_result text := null;
  date_result date := null;
  timestamp_result timestamp := null;
  timestamptz_result timestamptz := null;
  interval_result interval := null;
  rowid_result rowid := null;
BEGIN
  CALL dbms_output.PUT(123);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(number_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(bytea_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(blob_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(float_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(bi_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(pls_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(int_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(clob_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(raw_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(long_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(bif_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(bid_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(text_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(varc_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(date_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(timestamp_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(timestamptz_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(interval_result);
  CALL dbms_output.PUT_LINE(' ');
  CALL dbms_output.PUT(rowid_result);
  CALL dbms_output.PUT_LINE(' ');

  CALL dbms_output.PUT_LINE(123);
  CALL dbms_output.PUT_LINE(number_result);
  CALL dbms_output.PUT_LINE(bytea_result);
  CALL dbms_output.PUT_LINE(blob_result);
  CALL dbms_output.PUT_LINE(float_result);
  CALL dbms_output.PUT_LINE(bi_result);
  CALL dbms_output.PUT_LINE(pls_result);
  CALL dbms_output.PUT_LINE(int_result);
  CALL dbms_output.PUT_LINE(clob_result);
  CALL dbms_output.PUT_LINE(raw_result);
  CALL dbms_output.PUT_LINE(long_result);
  CALL dbms_output.PUT_LINE(bif_result);
  CALL dbms_output.PUT_LINE(bid_result);
  CALL dbms_output.PUT_LINE(text_result);
  CALL dbms_output.PUT_LINE(varc_result);
  CALL dbms_output.PUT_LINE(date_result);
  CALL dbms_output.PUT_LINE(timestamp_result);
  CALL dbms_output.PUT_LINE(timestamptz_result);
  CALL dbms_output.PUT_LINE(interval_result);
  CALL dbms_output.PUT_LINE(rowid_result);
END;
/
