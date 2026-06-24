-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.21'" to load this file. \quit

/*
 * ATTENTION ! ! ! Always define the procedure version of the dbms_output package
 * after the latest default version of the opentenbase_ora_package_function.
 * It means that when we need to make a new version of opentenbase_ora_package_function,
 * we can put the content here and move the definition of dbms_output to the next version file.
 */

-- redefine dbms_output
drop package dbms_output;
create package dbms_output as
$p$
	PROCEDURE enable(IN buffer_size integer default 20000);
    PROCEDURE disable();
    PROCEDURE serveroutput(IN bool);
    PROCEDURE put(IN a text);
    PROCEDURE put(IN a long);
    PROCEDURE put(IN a rowid);
    PROCEDURE put(IN a raw);
    PROCEDURE put(IN a integer);
    PROCEDURE put(IN a pls_integer);
    PROCEDURE put(IN a simple_integer);
    PROCEDURE put(IN a binary_integer);
    PROCEDURE put(IN a double precision);
    PROCEDURE put(IN a numeric);
    PROCEDURE put(IN a bytea);
    PROCEDURE put(IN a blob);
    PROCEDURE put(IN a clob);
    PROCEDURE put_line(IN a text);
    PROCEDURE put_line(IN a long);
    PROCEDURE put_line(IN a rowid);
    PROCEDURE put_line(IN a raw);
    PROCEDURE put_line(IN a integer);
    PROCEDURE put_line(IN a pls_integer);
    PROCEDURE put_line(IN a simple_integer);
    PROCEDURE put_line(IN a binary_integer);
    PROCEDURE put_line(IN a double precision);
    PROCEDURE put_line(IN a numeric);
    PROCEDURE put_line(IN a bytea);
    PROCEDURE put_line(IN a blob);
    PROCEDURE put_line(IN a clob);
    PROCEDURE new_line();
    PROCEDURE get_line(OUT line varchar2, OUT status int4);
    PROCEDURE get_lines(OUT lines varchar2[], INOUT numlines int4);
    FUNCTION byte2text(IN a bytea) RETURNS text;
$p$;

create  package body dbms_output as
$p$
PROCEDURE enable(IN buffer_size int4 default 20000)
   AS 'MODULE_PATHNAME','dbms_output_enable' LANGUAGE C;
PROCEDURE disable()
   AS 'MODULE_PATHNAME','dbms_output_disable' LANGUAGE C;
PROCEDURE serveroutput(IN bool)
   AS 'MODULE_PATHNAME','dbms_output_serveroutput' LANGUAGE C;
PROCEDURE put(IN a text)
   AS 'MODULE_PATHNAME','dbms_output_put' LANGUAGE C;
PROCEDURE put(IN a long)
   AS 'MODULE_PATHNAME','dbms_output_put_long' LANGUAGE C;
PROCEDURE put(IN a rowid)
   AS 'MODULE_PATHNAME','dbms_output_put_rowid' LANGUAGE C;
PROCEDURE put(IN a raw)
   AS 'MODULE_PATHNAME','dbms_output_put_raw' LANGUAGE C;
PROCEDURE put(IN a integer)
   AS 'MODULE_PATHNAME','dbms_output_put_int' LANGUAGE C;

PROCEDURE put(IN a pls_integer)
   AS 'MODULE_PATHNAME','dbms_output_put_pls_int' LANGUAGE C;

PROCEDURE put(IN a simple_integer)
   AS 'MODULE_PATHNAME','dbms_output_put_simple_int' LANGUAGE C;

PROCEDURE put(IN a binary_integer)
   AS 'MODULE_PATHNAME','dbms_output_put_binary_int' LANGUAGE C;

PROCEDURE put(IN a double precision)
   AS 'MODULE_PATHNAME','dbms_output_put_float8' LANGUAGE C;

PROCEDURE put(IN a numeric)
   AS 'MODULE_PATHNAME','dbms_output_put_numeric' LANGUAGE C;

PROCEDURE put(IN a bytea)
   AS 'MODULE_PATHNAME','dbms_output_put_bytea' LANGUAGE C;

PROCEDURE put(IN a blob)
   AS 'MODULE_PATHNAME','dbms_output_put_blob' LANGUAGE C;

PROCEDURE put(IN a clob)
   AS 'MODULE_PATHNAME','dbms_output_put_clob' LANGUAGE C;

PROCEDURE put_line(IN a text)
   AS 'MODULE_PATHNAME','dbms_output_put_line' LANGUAGE C;
PROCEDURE put_line(IN a long)
   AS 'MODULE_PATHNAME','dbms_output_put_line_long' LANGUAGE C;
PROCEDURE put_line(IN a rowid)
   AS 'MODULE_PATHNAME','dbms_output_put_line_rowid' LANGUAGE C;
PROCEDURE put_line(IN a raw)
   AS 'MODULE_PATHNAME','dbms_output_put_line_raw' LANGUAGE C;
PROCEDURE put_line(IN a integer)
   AS 'MODULE_PATHNAME','dbms_output_put_line_int' LANGUAGE C;

PROCEDURE put_line(IN a pls_integer)
   AS 'MODULE_PATHNAME','dbms_output_put_line_pls_int' LANGUAGE C;

PROCEDURE put_line(IN a simple_integer)
   AS 'MODULE_PATHNAME','dbms_output_put_line_simple_int' LANGUAGE C;

PROCEDURE put_line(IN a binary_integer)
   AS 'MODULE_PATHNAME','dbms_output_put_line_binary_int' LANGUAGE C;

PROCEDURE put_line(IN a double precision)
   AS 'MODULE_PATHNAME','dbms_output_put_line_float8' LANGUAGE C;

PROCEDURE put_line(IN a numeric)
   AS 'MODULE_PATHNAME','dbms_output_put_line_numeric' LANGUAGE C;

PROCEDURE put_line(IN a bytea)
   AS 'MODULE_PATHNAME','dbms_output_put_line_bytea' LANGUAGE C;

PROCEDURE put_line(IN a blob)
   AS 'MODULE_PATHNAME','dbms_output_put_line_blob' LANGUAGE C;

PROCEDURE put_line(IN a clob)
   AS 'MODULE_PATHNAME','dbms_output_put_line_clob' LANGUAGE C;

PROCEDURE new_line()
   AS 'MODULE_PATHNAME','dbms_output_new_line' LANGUAGE C;
PROCEDURE get_line(OUT line varchar2, OUT status int4)
   AS 'MODULE_PATHNAME','dbms_output_get_line' LANGUAGE C;
PROCEDURE get_lines(OUT lines varchar2[], INOUT numlines int4)
   AS 'MODULE_PATHNAME','dbms_output_get_lines' LANGUAGE C;

function byte2text(IN a bytea) RETURNS text
   AS 'MODULE_PATHNAME','dbms_output_byte2text'
   LANGUAGE C IMMUTABLE STRICT;
$p$;
grant usage on schema dbms_output to public;
