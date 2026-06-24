/*
 * PostgreSQL System Views
 *
 * Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * src/backend/catalog/system_views_pg.sql
 *
 * Note: this file is read in single-user -j mode, which means that the
 * command terminator is semicolon-newline-newline; whenever the backend
 * sees that, it stops and executes what it's got.  If you write a lot of
 * statements without empty lines between, they'll all get quoted to you
 * in any error message about one of them, so don't do that.  Also, you
 * cannot write a semicolon immediately followed by an empty line in a
 * string literal (including a function body!) or a multiline comment.
 */

CREATE SCHEMA dbms_random;

CREATE FUNCTION dbms_random.value()
    RETURNS numeric AS $$
BEGIN
RETURN random()::numeric;
END;
$$ LANGUAGE default_plsql;

CREATE FUNCTION dbms_random.value(low numeric, high numeric)
    RETURNS numeric AS $$
BEGIN
RETURN (random() * (high - low) + low)::numeric;
END;
$$ LANGUAGE default_plsql;

CREATE PROCEDURE dbms_random.seed(val INT) AS $$
BEGIN
    PERFORM setseed(val / power(2, 31));
END;
$$ LANGUAGE default_plsql NOT PUSHDOWN;

CREATE PROCEDURE dbms_random.seed(val TEXT) AS $$
DECLARE
l_ascii NUMERIC;
BEGIN
    IF val = '''' OR val IS NULL THEN
        RETURN;
ELSE
SELECT SUM(ascii(id)) / (127 * count(*))
INTO l_ascii
FROM unnest(string_to_array(val, '''')) AS id;
IF l_ascii <= 1 THEN
            PERFORM setseed(l_ascii);
END IF;
END IF;
END;
$$ LANGUAGE default_plsql NOT PUSHDOWN;
GRANT USAGE ON SCHEMA dbms_random TO PUBLIC;