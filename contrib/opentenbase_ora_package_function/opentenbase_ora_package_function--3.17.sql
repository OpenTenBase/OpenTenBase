/* opentenbase_ora_package_function--3.14.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION opentenbase_ora_package_function" to load this file. \quit

/*
 * Map pls_integer to INT4.
 */
-- Support PLS_INTEGER
CREATE TYPE pls_integer;
CREATE FUNCTION pls_integer_in(cstring)
	RETURNS pls_integer
	AS 'int4in'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE FUNCTION pls_integer_out(pls_integer)
	RETURNS cstring
	AS 'int4out'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE FUNCTION pls_integer_recv(internal,oid,int4)
	RETURNS pls_integer
	AS 'int4recv'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE FUNCTION pls_integer_send(pls_integer)
	RETURNS bytea
	AS 'int4send'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE TYPE pls_integer (
	input = pls_integer_in,
	output = pls_integer_out,
	send = pls_integer_send,
	receive = pls_integer_recv,
	like = int4
);

CREATE CAST (pls_integer AS int) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (int AS pls_integer) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (pls_integer AS float8) WITH INOUT AS IMPLICIT; -- For usage of float operators
CREATE CAST (pls_integer AS numeric) WITH INOUT AS IMPLICIT; -- For usage of numeric operators

CREATE FUNCTION plsint_um(pls_integer)
RETURNS pls_integer
AS 'int4um'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;
CREATE OPERATOR - (
	rightarg = pls_integer,
	function = plsint_um);
	
--function to_nclob
CREATE FUNCTION to_nclob(varchar) RETURNS nclob
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::nclob $$;

CREATE FUNCTION to_nclob(text) RETURNS nclob
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::nclob $$;

CREATE FUNCTION to_nclob(int) RETURNS nclob
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::nclob $$;

CREATE FUNCTION to_nclob(nclob) RETURNS nclob
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::nclob $$;

CREATE FUNCTION to_nclob(float4) RETURNS nclob
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::nclob $$;

CREATE FUNCTION to_nclob(float8) RETURNS nclob
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::nclob $$;


CREATE FUNCTION to_nclob(numeric) RETURNS nclob
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::nclob $$;


CREATE FUNCTION to_nclob(int8) RETURNS nclob
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::nclob $$;

CREATE FUNCTION to_nclob(long) RETURNS nclob
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::nclob $$;

--function to_binary_float
CREATE FUNCTION to_binary_float(varchar) RETURNS binary_float
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_float $$;

CREATE FUNCTION to_binary_float(text) RETURNS binary_float
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_float $$;

CREATE FUNCTION to_binary_float(float4) RETURNS binary_float
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_float $$;

CREATE FUNCTION to_binary_float(float8) RETURNS binary_float
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_float $$;


CREATE FUNCTION to_binary_float(numeric) RETURNS binary_float
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_float $$;


CREATE FUNCTION to_binary_float(text, text)
RETURNS binary_float
AS 'MODULE_PATHNAME','to_binary_float'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

--function to_binary_double
CREATE FUNCTION to_binary_double(varchar) RETURNS binary_double
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_double $$;

CREATE FUNCTION to_binary_double(text) RETURNS binary_double
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_double $$;

CREATE FUNCTION to_binary_double(float4) RETURNS binary_double
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_double $$;

CREATE FUNCTION to_binary_double(float8) RETURNS binary_double
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_double $$;


CREATE FUNCTION to_binary_double(numeric) RETURNS binary_double
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT $1::binary_double $$;

CREATE FUNCTION to_binary_double(text, text)
RETURNS binary_double
AS 'MODULE_PATHNAME','to_binary_double'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;


/*CREATE FUNCTION userenv(param varchar2)
RETURNS varchar2
AS 'MODULE_PATHNAME', 'orafce_userenv'
LANGUAGE c VOLATILE PARALLEL SAFE STRICT;*/

/*exists cast rule*/
--CREATE CAST (bytea AS blob) WITHOUT FUNCTION AS IMPLICIT;

create package dbms_output as
$p$
	FUNCTION enable(IN buffer_size integer) returns void;
    FUNCTION enable() returns void;
    FUNCTION disable() RETURNS void;
    FUNCTION serveroutput(IN bool) RETURNS void;
    FUNCTION put(IN a text) RETURNS void;
    FUNCTION put(IN a long) RETURNS void;
    FUNCTION put(IN a rowid) RETURNS void;
    FUNCTION put(IN a raw) RETURNS void;
    FUNCTION put(IN a integer) RETURNS void;
    FUNCTION put(IN a pls_integer) RETURNS void;
    --FUNCTION put(IN a float) RETURNS void;
    FUNCTION put(IN a numeric) RETURNS void;
    FUNCTION put(IN a bytea) RETURNS void;
    FUNCTION put(IN a blob) RETURNS void;
    FUNCTION put(IN a clob) RETURNS void;
    FUNCTION put_line(IN a text) RETURNS void;
    FUNCTION put_line(IN a long) RETURNS void;
    FUNCTION put_line(IN a rowid) RETURNS void;
    FUNCTION put_line(IN a raw) RETURNS void;
    FUNCTION put_line(IN a integer) RETURNS void;
    FUNCTION put_line(IN a pls_integer) RETURNS void;	
    --FUNCTION put_line(IN a float) RETURNS void;
    FUNCTION put_line(IN a numeric) RETURNS void;
    FUNCTION put_line(IN a bytea) RETURNS void;
    FUNCTION put_line(IN a blob) RETURNS void;
    FUNCTION put_line(IN a clob) RETURNS void;	
    FUNCTION new_line() RETURNS void;
    FUNCTION get_line(OUT line text, OUT status int4);
    FUNCTION get_lines(OUT lines text[], INOUT numlines int4);
    FUNCTION byte2text(IN a bytea) RETURNS text;
$p$;

create  package body dbms_output as
$p$
FUNCTION enable(IN buffer_size int4) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_enable'
    LANGUAGE C IMMUTABLE STRICT;
FUNCTION enable() RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_enable_default'
    LANGUAGE C IMMUTABLE STRICT;
FUNCTION disable() RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_disable'
    LANGUAGE C IMMUTABLE STRICT;
function serveroutput(IN bool) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_serveroutput'
    LANGUAGE C IMMUTABLE STRICT;
function put(IN a text) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put'
    LANGUAGE C VOLATILE STRICT;
function put(IN a long) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_long'
    LANGUAGE C VOLATILE;
function put(IN a rowid) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_rowid'
    LANGUAGE C VOLATILE;
function put(IN a raw) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_raw'
    LANGUAGE C VOLATILE;	
function put(IN a integer) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_int'
    LANGUAGE C VOLATILE STRICT;

function put(IN a pls_integer) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_pls_int'
    LANGUAGE C VOLATILE STRICT;

/*function put(IN a float) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_float8'
    LANGUAGE C VOLATILE STRICT;*/

function put(IN a numeric) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_numeric'
    LANGUAGE C VOLATILE STRICT;

function put(IN a bytea) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_bytea'
    LANGUAGE C VOLATILE STRICT;
	
function put(IN a blob) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_blob'
    LANGUAGE C VOLATILE STRICT;

function put(IN a clob) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_clob'
    LANGUAGE C VOLATILE STRICT;
	
function put_line(IN a text) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line'
    LANGUAGE C VOLATILE STRICT;
function put_line(IN a long) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_long'
    LANGUAGE C VOLATILE STRICT;
function put_line(IN a rowid) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_rowid'
    LANGUAGE C VOLATILE STRICT;
function put_line(IN a raw) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_raw'
    LANGUAGE C VOLATILE STRICT;
function put_line(IN a integer) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_int'
    LANGUAGE C VOLATILE STRICT;

function put_line(IN a pls_integer) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_pls_int'
    LANGUAGE C VOLATILE STRICT;

/*function put_line(IN a float) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_float8'
    LANGUAGE C VOLATILE STRICT;*/

function put_line(IN a numeric) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_numeric'
    LANGUAGE C VOLATILE STRICT;

function put_line(IN a bytea) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_bytea'
    LANGUAGE C VOLATILE STRICT;

function put_line(IN a blob) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_blob'
    LANGUAGE C VOLATILE STRICT;

function put_line(IN a clob) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_clob'
    LANGUAGE C VOLATILE STRICT;

function new_line() RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_new_line'
   LANGUAGE C VOLATILE STRICT;
function get_line(OUT line text, OUT status int4)
   AS 'MODULE_PATHNAME','dbms_output_get_line'
   LANGUAGE C IMMUTABLE STRICT;
function get_lines(OUT lines text[], INOUT numlines int4)
   AS 'MODULE_PATHNAME','dbms_output_get_lines'
   LANGUAGE C IMMUTABLE STRICT;

function byte2text(IN a bytea) RETURNS text
   AS 'MODULE_PATHNAME','dbms_output_byte2text'
   LANGUAGE C IMMUTABLE STRICT;
$p$;
GRANT USAGE ON SCHEMA DBMS_OUTPUT TO PUBLIC;

-- package dbms_assert
create package dbms_assert as
$p$
    FUNCTION enquote_literal(str varchar) RETURNS varchar;
    FUNCTION enquote_name(str varchar, loweralize boolean)RETURNS varchar;
    FUNCTION enquote_name(str varchar) RETURNS varchar;
    FUNCTION noop(str varchar) RETURNS varchar;
    FUNCTION schema_name(str varchar) RETURNS varchar;
    FUNCTION sql_object_name(str varchar) RETURNS varchar;
    FUNCTION simple_sql_name(str varchar) RETURNS varchar;
    FUNCTION qualified_sql_name(str varchar) RETURNS varchar;
$p$;

create package body dbms_assert as
$p$
  FUNCTION enquote_literal(str varchar)
  RETURNS varchar
  AS 'MODULE_PATHNAME','dbms_assert_enquote_literal'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION enquote_name(str varchar, loweralize boolean)
  RETURNS varchar
  AS 'MODULE_PATHNAME','dbms_assert_enquote_name'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION dbms_assert.enquote_name(str varchar)
  RETURNS varchar
  AS 'SELECT dbms_assert.enquote_name($1, false)'
  LANGUAGE SQL IMMUTABLE STRICT;
  FUNCTION noop(str varchar)
  RETURNS varchar
  AS 'MODULE_PATHNAME','dbms_assert_noop'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION schema_name(str varchar)
  RETURNS varchar
  AS 'MODULE_PATHNAME','dbms_assert_schema_name'
  LANGUAGE C IMMUTABLE;
  FUNCTION sql_object_name(str varchar)
  RETURNS varchar
  AS 'MODULE_PATHNAME','dbms_assert_object_name'
  LANGUAGE C IMMUTABLE;
  FUNCTION simple_sql_name(str varchar)
  RETURNS varchar
  AS 'MODULE_PATHNAME','dbms_assert_simple_sql_name'
  LANGUAGE C IMMUTABLE;
  FUNCTION qualified_sql_name(str varchar)
  RETURNS varchar
  AS 'MODULE_PATHNAME','dbms_assert_qualified_sql_name'
  LANGUAGE C IMMUTABLE;
$p$;
GRANT USAGE ON SCHEMA DBMS_ASSERT TO PUBLIC;

create package dbms_alert as
$p$
	FUNCTION register(name text) RETURNS void;
	FUNCTION remove(name text) RETURNS void;
	FUNCTION removeall() RETURNS void;
	FUNCTION _signal(name text, message text) RETURNS void;
	FUNCTION waitany(OUT name text, OUT message text, OUT status integer, timeout float8) RETURNS record;
	FUNCTION waitone(name text, OUT message text, OUT status integer, timeout float8) RETURNS record;
	FUNCTION set_defaults(sensitivity float8) RETURNS void;
	FUNCTION defered_signal() RETURNS trigger;
	FUNCTION signal(_event text, _message text) RETURNS void;
$p$;

create package body dbms_alert as
$p$
	FUNCTION register(name text)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_alert_register'
	LANGUAGE C VOLATILE STRICT;

	FUNCTION remove(name text)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_alert_remove'
	LANGUAGE C VOLATILE STRICT;

	FUNCTION removeall()
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_alert_removeall'
	LANGUAGE C VOLATILE;

	FUNCTION _signal(name text, message text)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_alert_signal'
	LANGUAGE C VOLATILE;

	FUNCTION waitany(OUT name text, OUT message text, OUT status integer, timeout float8)
	RETURNS record
	AS 'MODULE_PATHNAME','dbms_alert_waitany'
	LANGUAGE C VOLATILE;

	FUNCTION waitone(name text, OUT message text, OUT status integer, timeout float8)
	RETURNS record
	AS 'MODULE_PATHNAME','dbms_alert_waitone'
	LANGUAGE C VOLATILE;

	FUNCTION set_defaults(sensitivity float8)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_alert_set_defaults'
	LANGUAGE C VOLATILE;

	FUNCTION defered_signal()
	RETURNS trigger
	AS 'MODULE_PATHNAME','dbms_alert_defered_signal'
	LANGUAGE C SECURITY DEFINER;

	FUNCTION signal(_event text, _message text)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_alert_signal'
	LANGUAGE C SECURITY DEFINER;
$p$;
GRANT USAGE ON SCHEMA DBMS_ALERT TO PUBLIC;

create package dbms_pipe as
$p$
	FUNCTION pack_message(text) RETURNS void;
    FUNCTION unpack_message_text() RETURNS text;
    FUNCTION receive_message(text, int) RETURNS int;
    FUNCTION receive_message(text) RETURNS int;
    FUNCTION send_message(text, int, int) RETURNS int;
	FUNCTION send_message(text, int) RETURNS int;
	FUNCTION send_message(text) RETURNS int;
	FUNCTION unique_session_name() RETURNS varchar;
	FUNCTION __list_pipes() RETURNS SETOF RECORD;
    FUNCTION next_item_type() RETURNS int;
	FUNCTION create_pipe(text, int, bool) RETURNS void;
	FUNCTION create_pipe(text, int) RETURNS void;
	FUNCTION create_pipe(text)RETURNS void;
	FUNCTION reset_buffer()RETURNS void;
	FUNCTION purge(text)RETURNS void;
	FUNCTION remove_pipe(text) RETURNS void;
	FUNCTION pack_message(date) RETURNS void;
	FUNCTION unpack_message_date() RETURNS date;
	FUNCTION pack_message(timestamp with time zone) RETURNS void;
	FUNCTION unpack_message_timestamp() RETURNS timestamp with time zone;
	FUNCTION pack_message(numeric) RETURNS void;
	FUNCTION unpack_message_number() RETURNS numeric;
	FUNCTION pack_message(integer) RETURNS void;
	FUNCTION pack_message(bigint) RETURNS void;
	FUNCTION pack_message(bytea) RETURNS void;
	FUNCTION unpack_message_bytea() RETURNS bytea;
	FUNCTION pack_message(record) RETURNS void;
	FUNCTION unpack_message_record() RETURNS record;
$p$;

CREATE VIEW dbms_pipe.db_pipes
AS SELECT * FROM dbms_pipe.__list_pipes() AS (Name varchar, Items int, Size int, "limit" int, "private" bool, "owner" varchar);

create  package body dbms_pipe as
$p$
	FUNCTION pack_message(text)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_pack_message_text'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION unpack_message_text()
	RETURNS text
	AS 'MODULE_PATHNAME','dbms_pipe_unpack_message_text'
	LANGUAGE C VOLATILE;
	FUNCTION receive_message(text, int)
	RETURNS int
	AS 'MODULE_PATHNAME','dbms_pipe_receive_message'
	LANGUAGE C VOLATILE;
	FUNCTION receive_message(text)
	RETURNS int
	AS $$SELECT dbms_pipe.receive_message($1,NULL::int);$$
	LANGUAGE SQL VOLATILE;
	FUNCTION send_message(text, int, int)
	RETURNS int
	AS 'MODULE_PATHNAME','dbms_pipe_send_message'
	LANGUAGE C VOLATILE;
	FUNCTION send_message(text, int)
	RETURNS int
	AS $$SELECT dbms_pipe.send_message($1,$2,NULL);$$
	LANGUAGE SQL VOLATILE;
	FUNCTION send_message(text)
	RETURNS int
	AS $$SELECT dbms_pipe.send_message($1,NULL,NULL);$$
	LANGUAGE SQL VOLATILE;
	FUNCTION unique_session_name()
	RETURNS varchar
	AS 'MODULE_PATHNAME','dbms_pipe_unique_session_name'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION __list_pipes()
	RETURNS SETOF RECORD
	AS 'MODULE_PATHNAME','dbms_pipe_list_pipes'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION next_item_type()
	RETURNS int
	AS 'MODULE_PATHNAME','dbms_pipe_next_item_type'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION create_pipe(text, int, bool)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_create_pipe'
	LANGUAGE C VOLATILE;
	FUNCTION create_pipe(text, int)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_create_pipe_2'
	LANGUAGE C VOLATILE;
	FUNCTION create_pipe(text)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_create_pipe_1'
	LANGUAGE C VOLATILE;
	FUNCTION reset_buffer()
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_reset_buffer'
	LANGUAGE C VOLATILE;
	FUNCTION purge(text)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_purge'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION remove_pipe(text)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_remove_pipe'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION pack_message(date)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_pack_message_date'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION unpack_message_date()
	RETURNS date
	AS 'MODULE_PATHNAME','dbms_pipe_unpack_message_date'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION pack_message(timestamp with time zone)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_pack_message_timestamp'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION unpack_message_timestamp()
	RETURNS timestamp with time zone
	AS 'MODULE_PATHNAME','dbms_pipe_unpack_message_timestamp'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION pack_message(numeric)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_pack_message_number'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION unpack_message_number()
	RETURNS numeric
	AS 'MODULE_PATHNAME','dbms_pipe_unpack_message_number'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION pack_message(integer)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_pack_message_integer'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION pack_message(bigint)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_pack_message_bigint'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION pack_message(bytea)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_pack_message_bytea'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION unpack_message_bytea()
	RETURNS bytea
	AS 'MODULE_PATHNAME','dbms_pipe_unpack_message_bytea'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION pack_message(record)
	RETURNS void
	AS 'MODULE_PATHNAME','dbms_pipe_pack_message_record'
	LANGUAGE C VOLATILE STRICT;
	FUNCTION unpack_message_record()
	RETURNS record
	AS 'MODULE_PATHNAME','dbms_pipe_unpack_message_record'
	LANGUAGE C VOLATILE STRICT;
$p$;
GRANT USAGE ON SCHEMA DBMS_PIPE TO PUBLIC;

create package dbms_random as
$p$
  FUNCTION initialize(int) RETURNS void;
  FUNCTION normal() RETURNS double precision;
  FUNCTION random() RETURNS integer;
  FUNCTION seed(integer) RETURNS void;
  FUNCTION seed(text) RETURNS void;
  FUNCTION string(opt text, len int) RETURNS text;
  FUNCTION terminate() RETURNS void;
  FUNCTION value(low double precision, high double precision) RETURNS double precision;
  FUNCTION value() RETURNS double precision;
$p$;

create package body dbms_random as
$p$
  FUNCTION initialize(int)
  RETURNS void
  AS 'MODULE_PATHNAME','dbms_random_initialize'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION normal()
  RETURNS double precision
  AS 'MODULE_PATHNAME','dbms_random_normal'
  LANGUAGE C VOLATILE;
  FUNCTION random()
  RETURNS integer
  AS 'MODULE_PATHNAME','dbms_random_random'
  LANGUAGE C VOLATILE;
  FUNCTION seed(integer)
  RETURNS void
  AS 'MODULE_PATHNAME','dbms_random_seed_int'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION seed(text)
  RETURNS void
  AS 'MODULE_PATHNAME','dbms_random_seed_varchar'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION string(opt text, len int)
  RETURNS text
  AS 'MODULE_PATHNAME','dbms_random_string'
  LANGUAGE C IMMUTABLE;
  FUNCTION terminate()
  RETURNS void
  AS 'MODULE_PATHNAME','dbms_random_terminate'
  LANGUAGE C IMMUTABLE;
  FUNCTION value(low double precision, high double precision)
  RETURNS double precision
  AS 'MODULE_PATHNAME','dbms_random_value_range'
  LANGUAGE C STRICT VOLATILE;
  FUNCTION value()
  RETURNS double precision
  AS 'MODULE_PATHNAME','dbms_random_value'
  LANGUAGE C VOLATILE;
$p$;
GRANT USAGE ON SCHEMA DBMS_RANDOM TO PUBLIC;

-- package dbms_utility
create package dbms_utility as
$p$
  FUNCTION format_call_stack(text) RETURNS text;
  FUNCTION format_call_stack() RETURNS text;
  FUNCTION get_hash_value(VARCHAR2, NUMBER, NUMBER) RETURNS NUMBER;
$p$;

create package body dbms_utility as
$p$
  FUNCTION dbms_utility.format_call_stack(text)
  RETURNS text
  AS 'MODULE_PATHNAME','dbms_utility_format_call_stack1'
  LANGUAGE C STRICT VOLATILE;

  FUNCTION dbms_utility.format_call_stack()
  RETURNS text
  AS 'MODULE_PATHNAME','dbms_utility_format_call_stack0'
  LANGUAGE C VOLATILE;
  
  FUNCTION dbms_utility.get_hash_value(name VARCHAR2, base NUMBER, hash_size NUMBER)
  RETURNS NUMBER
  AS 'MODULE_PATHNAME','get_hash_value_ora'
  LANGUAGE C IMMUTABLE;
$p$;
GRANT USAGE ON SCHEMA DBMS_UTILITY TO PUBLIC;

-- plunit package
create package plunit as
$p$
    FUNCTION assert_true(condition boolean)RETURNS void;
    FUNCTION assert_true(condition boolean, message varchar)RETURNS void;
    FUNCTION assert_false(condition boolean)RETURNS void;
    FUNCTION assert_false(condition boolean, message varchar)RETURNS void;
    FUNCTION assert_null(actual anyelement)RETURNS void;
    FUNCTION assert_null(actual anyelement, message varchar)RETURNS void;
    FUNCTION assert_not_null(actual anyelement)RETURNS void;
    FUNCTION assert_not_null(actual anyelement, message varchar)RETURNS void;
    FUNCTION assert_equals(expected anyelement, actual anyelement)RETURNS void;
    FUNCTION assert_equals(expected anyelement, actual anyelement, message varchar)RETURNS void;
    FUNCTION assert_equals(expected double precision, actual double precision, "range" double precision)RETURNS void;
    FUNCTION assert_equals(expected double precision, actual double precision, "range" double precision, message varchar)RETURNS void;
    FUNCTION assert_not_equals(expected anyelement, actual anyelement)RETURNS void;
    FUNCTION assert_not_equals(expected anyelement, actual anyelement, message varchar)RETURNS void;
    FUNCTION assert_not_equals(expected double precision, actual double precision, "range" double precision)RETURNS void;
    FUNCTION assert_not_equals(expected double precision, actual double precision, "range" double precision, message varchar)RETURNS void;
    FUNCTION fail()RETURNS void;
    FUNCTION fail(message varchar)RETURNS void;
$p$;

create package body plunit as
$p$
    FUNCTION assert_true(condition boolean)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_true'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_true(condition boolean, message varchar)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_true_message'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_false(condition boolean)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_false'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_false(condition boolean, message varchar)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_false_message'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_null(actual anyelement)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_null'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_null(actual anyelement, message varchar)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_null_message'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_not_null(actual anyelement)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_not_null'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_not_null(actual anyelement, message varchar)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_not_null_message'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_equals(expected anyelement, actual anyelement)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_equals'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_equals(expected anyelement, actual anyelement, message varchar)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_equals_message'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_equals(expected double precision, actual double precision, "range" double precision)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_equals_range'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_equals(expected double precision, actual double precision, "range" double precision, message varchar)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_equals_range_message'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_not_equals(expected anyelement, actual anyelement)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_not_equals'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_not_equals(expected anyelement, actual anyelement, message varchar)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_not_equals_message'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_not_equals(expected double precision, actual double precision, "range" double precision)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_not_equals_range'
    LANGUAGE C IMMUTABLE;
    FUNCTION assert_not_equals(expected double precision, actual double precision, "range" double precision, message varchar)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_assert_not_equals_range_message'
    LANGUAGE C IMMUTABLE;
    FUNCTION fail()
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_fail'
    LANGUAGE C IMMUTABLE;
    FUNCTION fail(message varchar)
    RETURNS void
    AS 'MODULE_PATHNAME','plunit_fail_message'
    LANGUAGE C IMMUTABLE;
$p$;
GRANT USAGE ON SCHEMA PLUNIT TO PUBLIC;

--package plvchr
create package plvchr as
$p$
    FUNCTION nth(str text, n int)RETURNS text;
    FUNCTION first(str text)RETURNS varchar;
    FUNCTION last(str text)RETURNS varchar;
    FUNCTION _is_kind(str text, kind int)RETURNS bool;
    FUNCTION _is_kind(c int, kind int)RETURNS bool;
    FUNCTION is_blank(c int)RETURNS BOOL;
    FUNCTION is_blank(c text)RETURNS BOOL;
    FUNCTION is_digit(c int)RETURNS BOOL;
    FUNCTION is_digit(c text)RETURNS BOOL;
    FUNCTION is_quote(c int)RETURNS BOOL;
    FUNCTION is_quote(c text)RETURNS BOOL;
    FUNCTION is_other(c int)RETURNS BOOL;
    FUNCTION is_other(c text)RETURNS BOOL;
    FUNCTION is_letter(c int)RETURNS BOOL;
    FUNCTION is_letter(c text)RETURNS BOOL;
    FUNCTION char_name(c text)RETURNS varchar;
    FUNCTION quoted1(str text)RETURNS varchar;
    FUNCTION quoted2(str text)RETURNS varchar;
    FUNCTION stripped(str text, char_in text)RETURNS varchar;
$p$;

create package body plvchr as
$p$
    FUNCTION nth(str text, n int)
    RETURNS text
    AS 'MODULE_PATHNAME','plvchr_nth'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION first(str text)
    RETURNS varchar
    AS 'MODULE_PATHNAME','plvchr_first'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION last(str text)
    RETURNS varchar
    AS 'MODULE_PATHNAME','plvchr_last'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION _is_kind(str text, kind int)
    RETURNS bool
    AS 'MODULE_PATHNAME','plvchr_is_kind_a'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION _is_kind(c int, kind int)
    RETURNS bool
    AS 'MODULE_PATHNAME','plvchr_is_kind_i'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION is_blank(c int)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 1);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_blank(c text)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 1);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_digit(c int)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 2);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_digit(c text)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 2);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_quote(c int)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 3);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_quote(c text)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 3);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_other(c int)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 4);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_other(c text)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 4);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_letter(c int)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 5);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_letter(c text)
    RETURNS BOOL
    AS $$ SELECT plvchr._is_kind($1, 5);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION char_name(c text)
    RETURNS varchar
    AS 'MODULE_PATHNAME','plvchr_char_name'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION quoted1(str text)
    RETURNS varchar
    AS $$SELECT ''''||$1||'''';$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION quoted2(str text)
    RETURNS varchar
    AS $$SELECT '"'||$1||'"';$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION stripped(str text, char_in text)
    RETURNS varchar
    AS $$ SELECT TRANSLATE($1, 'A'||$2, 'A'); $$
    LANGUAGE SQL IMMUTABLE STRICT;
$p$;
GRANT USAGE ON SCHEMA PLVCHR TO PUBLIC;

--package plvlex
create package plvlex as
$p$
   FUNCTION tokens(IN str text, IN skip_spaces bool, IN qualified_names bool,OUT pos int, OUT token text, OUT code int, OUT class text, OUT separator text, OUT mod text)RETURNS SETOF RECORD;
$p$;

create package body plvlex as
$p$
    FUNCTION tokens(IN str text, IN skip_spaces bool, IN qualified_names bool,OUT pos int, OUT token text, OUT code int, OUT class text, OUT separator text, OUT mod text)
    RETURNS SETOF RECORD
    AS 'MODULE_PATHNAME','plvlex_tokens'
    LANGUAGE C IMMUTABLE STRICT;
$p$;
GRANT USAGE ON SCHEMA PLVLEX TO PUBLIC;

--package plvstr
create package plvstr as
$p$
    FUNCTION rvrs(str text, "start" int, _end int)
    RETURNS text;
    FUNCTION rvrs(str text, "start" int)
    RETURNS text;
    FUNCTION rvrs(str text)
    RETURNS text;
    FUNCTION normalize(str text)
    RETURNS varchar;
    FUNCTION is_prefix(str text, prefix text, cs bool)
    RETURNS bool;
    FUNCTION is_prefix(str text, prefix text)
    RETURNS bool;
    FUNCTION is_prefix(str int, prefix int)
    RETURNS bool;
    FUNCTION is_prefix(str bigint, prefix bigint)
    RETURNS bool;
    FUNCTION substr(str text, "start" int, len int)
    RETURNS varchar;
    FUNCTION substr(str text, "start" int)
    RETURNS varchar;
    FUNCTION instr(str text, patt text, "start" int, nth int)
    RETURNS int;
    FUNCTION instr(str text, patt text, "start" int)
    RETURNS int;
    FUNCTION instr(str text, patt text)
    RETURNS int;
    FUNCTION lpart(str text, div text, "start" int, nth int, all_if_notfound bool)
    RETURNS text;
    FUNCTION lpart(str text, div text, "start" int, nth int)
    RETURNS text;
    FUNCTION lpart(str text, div text, "start" int)
    RETURNS text;
    FUNCTION lpart(str text, div text)
    RETURNS text;
    FUNCTION rpart(str text, div text, "start" int, nth int, all_if_notfound bool)
    RETURNS text;
    FUNCTION rpart(str text, div text, "start" int, nth int)
    RETURNS text;
    FUNCTION rpart(str text, div text, "start" int)
    RETURNS text;
    FUNCTION rpart(str text, div text)
    RETURNS text;
    FUNCTION lstrip(str text, substr text, num int)
    RETURNS text;
    FUNCTION lstrip(str text, substr text)
    RETURNS text;
    FUNCTION rstrip(str text, substr text, num int)
    RETURNS text;
    FUNCTION rstrip(str text, substr text)
    RETURNS text;
    FUNCTION swap(str text, replace text, "start" int, length int)
    RETURNS text;
    FUNCTION swap(str text, replace text)
    RETURNS text;
    FUNCTION betwn(str text, "start" int, _end int, inclusive bool)
    RETURNS text;
    FUNCTION betwn(str text, "start" int, _end int)
    RETURNS text;
    FUNCTION betwn(str text, "start" text, _end text, startnth int, endnth int, inclusive bool, gotoend bool)
    RETURNS text;
    FUNCTION betwn(str text, "start" text, _end text)
    RETURNS text;
    FUNCTION betwn(str text, "start" text, _end text, startnth int, endnth int)
    RETURNS text;
    FUNCTION left(str text, n int)
    RETURNS varchar;
    FUNCTION right(str text, n int)
    RETURNS varchar;
$p$;

create package body plvstr as
$p$
    FUNCTION rvrs(str text, "start" int, _end int)
    RETURNS text
    AS 'MODULE_PATHNAME','plvstr_rvrs'
    LANGUAGE C IMMUTABLE;

    FUNCTION rvrs(str text, "start" int)
    RETURNS text
    AS $$ SELECT plvstr.rvrs($1,$2,NULL);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION rvrs(str text)
    RETURNS text
    AS $$ SELECT plvstr.rvrs($1,1,NULL);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION normalize(str text)
    RETURNS varchar
    AS 'MODULE_PATHNAME','plvstr_normalize'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION is_prefix(str text, prefix text, cs bool)
    RETURNS bool
    AS 'MODULE_PATHNAME','plvstr_is_prefix_text'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION is_prefix(str text, prefix text)
    RETURNS bool
    AS $$ SELECT plvstr.is_prefix($1,$2,true);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION is_prefix(str int, prefix int)
    RETURNS bool
    AS 'MODULE_PATHNAME','plvstr_is_prefix_int'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION is_prefix(str bigint, prefix bigint)
    RETURNS bool
    AS 'MODULE_PATHNAME','plvstr_is_prefix_int64'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION substr(str text, "start" int, len int)
    RETURNS varchar
    AS 'MODULE_PATHNAME','plvstr_substr3'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION substr(str text, "start" int)
    RETURNS varchar
    AS 'MODULE_PATHNAME','plvstr_substr2'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION instr(str text, patt text, "start" int, nth int)
    RETURNS int
    AS 'MODULE_PATHNAME','plvstr_instr4'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION instr(str text, patt text, "start" int)
    RETURNS int
    AS 'MODULE_PATHNAME','plvstr_instr3'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION instr(str text, patt text)
    RETURNS int
    AS 'MODULE_PATHNAME','plvstr_instr2'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION lpart(str text, div text, "start" int, nth int, all_if_notfound bool)
    RETURNS text
    AS 'MODULE_PATHNAME','plvstr_lpart'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION lpart(str text, div text, "start" int, nth int)
    RETURNS text
    AS $$ SELECT plvstr.lpart($1,$2, $3, $4, false); $$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION lpart(str text, div text, "start" int)
    RETURNS text
    AS $$ SELECT plvstr.lpart($1,$2, $3, 1, false); $$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION lpart(str text, div text)
    RETURNS text
    AS $$ SELECT plvstr.lpart($1,$2, 1, 1, false); $$
    LANGUAGE SQL IMMUTABLE STRICT;
    FUNCTION rpart(str text, div text, "start" int, nth int, all_if_notfound bool)
    RETURNS text
    AS 'MODULE_PATHNAME','plvstr_rpart'
    LANGUAGE C IMMUTABLE STRICT;
    FUNCTION rpart(str text, div text, "start" int, nth int)
    RETURNS text
    AS $$ SELECT plvstr.rpart($1,$2, $3, $4, false); $$
    LANGUAGE SQL IMMUTABLE STRICT;
    FUNCTION rpart(str text, div text, "start" int)
    RETURNS text
    AS $$ SELECT plvstr.rpart($1,$2, $3, 1, false); $$
    LANGUAGE SQL IMMUTABLE STRICT;
    FUNCTION rpart(str text, div text)
    RETURNS text
    AS $$ SELECT plvstr.rpart($1,$2, 1, 1, false); $$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION lstrip(str text, substr text, num int)
    RETURNS text
    AS 'MODULE_PATHNAME','plvstr_lstrip'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION lstrip(str text, substr text)
    RETURNS text
    AS $$ SELECT plvstr.lstrip($1, $2, 1); $$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION rstrip(str text, substr text, num int)
    RETURNS text
    AS 'MODULE_PATHNAME','plvstr_rstrip'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION rstrip(str text, substr text)
    RETURNS text
    AS $$ SELECT plvstr.rstrip($1, $2, 1); $$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION swap(str text, replace text, "start" int, length int)
    RETURNS text
    AS 'MODULE_PATHNAME','plvstr_swap'
    LANGUAGE C IMMUTABLE;

    FUNCTION swap(str text, replace text)
    RETURNS text
    AS $$ SELECT plvstr.swap($1,$2,1, NULL);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION betwn(str text, "start" int, _end int, inclusive bool)
    RETURNS text
    AS 'MODULE_PATHNAME','plvstr_betwn_i'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION betwn(str text, "start" int, _end int)
    RETURNS text
    AS $$ SELECT plvstr.betwn($1,$2,$3,true);$$
    LANGUAGE SQL IMMUTABLE STRICT;

    FUNCTION betwn(str text, "start" text, _end text, startnth int, endnth int, inclusive bool, gotoend bool)
    RETURNS text
    AS 'MODULE_PATHNAME','plvstr_betwn_c'
    LANGUAGE C IMMUTABLE;

    FUNCTION betwn(str text, "start" text, _end text)
    RETURNS text
    AS $$ SELECT plvstr.betwn($1,$2,$3,1,1,true,false);$$
    LANGUAGE SQL IMMUTABLE;

    FUNCTION betwn(str text, "start" text, _end text, startnth int, endnth int)
    RETURNS text
    AS $$ SELECT plvstr.betwn($1,$2,$3,$4,$5,true,false);$$
    LANGUAGE SQL IMMUTABLE;

    FUNCTION left(str text, n int)
    RETURNS varchar
    AS 'MODULE_PATHNAME', 'plvstr_left'
    LANGUAGE C IMMUTABLE STRICT;

    FUNCTION right(str text, n int)
    RETURNS varchar
    AS 'MODULE_PATHNAME','plvstr_right'
    LANGUAGE C IMMUTABLE STRICT;
$p$;
GRANT USAGE ON SCHEMA PLVSTR TO PUBLIC;

--package plvdate
create package plvdate as
$p$
    FUNCTION add_bizdays(date, int)
    RETURNS date;
    FUNCTION nearest_bizday(date)
    RETURNS date;
    FUNCTION next_bizday(date)
    RETURNS date;
    FUNCTION bizdays_between(date, date)
    RETURNS int;
    FUNCTION prev_bizday(date)
    RETURNS date;
    FUNCTION isbizday(date)
    RETURNS bool;
    FUNCTION set_nonbizday(text)
    RETURNS void;
    FUNCTION unset_nonbizday(text)
    RETURNS void;
    FUNCTION set_nonbizday(date, bool)
    RETURNS void;
    FUNCTION unset_nonbizday(date, bool)
    RETURNS void;
    FUNCTION set_nonbizday(date)
    RETURNS bool;
    FUNCTION unset_nonbizday(date)
    RETURNS bool;
    FUNCTION use_easter(bool)
    RETURNS void;
    FUNCTION use_easter()
    RETURNS bool;
    FUNCTION unuse_easter()
    RETURNS bool;
    FUNCTION using_easter()
    RETURNS bool;
    FUNCTION use_great_friday(bool)
    RETURNS void;
    FUNCTION use_great_friday()
    RETURNS bool;
    FUNCTION unuse_great_friday()
    RETURNS bool;
    FUNCTION using_great_friday()
    RETURNS bool;
    FUNCTION include_start(bool)
    RETURNS void;
    FUNCTION include_start()
    RETURNS bool;
    FUNCTION noinclude_start()
    RETURNS bool;
    FUNCTION including_start()
    RETURNS bool;
    FUNCTION version()
    RETURNS cstring;
    FUNCTION default_holidays(text)
    RETURNS void;
    FUNCTION days_inmonth(date)
    RETURNS integer;
    FUNCTION isleapyear(date)
    RETURNS bool;
$p$;

create package body plvdate as
$p$
    FUNCTION add_bizdays(date, int)
    RETURNS date
    AS 'MODULE_PATHNAME','plvdate_add_bizdays'
    LANGUAGE C IMMUTABLE STRICT;
    FUNCTION nearest_bizday(date)
    RETURNS date
    AS 'MODULE_PATHNAME','plvdate_nearest_bizday'
    LANGUAGE C IMMUTABLE STRICT;
    FUNCTION next_bizday(date)
    RETURNS date
    AS 'MODULE_PATHNAME','plvdate_next_bizday'
    LANGUAGE C IMMUTABLE STRICT;
    FUNCTION bizdays_between(date, date)
    RETURNS int
    AS 'MODULE_PATHNAME','plvdate_bizdays_between'
    LANGUAGE C IMMUTABLE STRICT;
    FUNCTION prev_bizday(date)
    RETURNS date
    AS 'MODULE_PATHNAME','plvdate_prev_bizday'
    LANGUAGE C IMMUTABLE STRICT;
    FUNCTION isbizday(date)
    RETURNS bool
    AS 'MODULE_PATHNAME','plvdate_isbizday'
    LANGUAGE C IMMUTABLE STRICT;
    FUNCTION set_nonbizday(text)
    RETURNS void
    AS 'MODULE_PATHNAME','plvdate_set_nonbizday_dow'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION unset_nonbizday(text)
    RETURNS void
    AS 'MODULE_PATHNAME','plvdate_unset_nonbizday_dow'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION set_nonbizday(date, bool)
    RETURNS void
    AS 'MODULE_PATHNAME','plvdate_set_nonbizday_day'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION unset_nonbizday(date, bool)
    RETURNS void
    AS 'MODULE_PATHNAME','plvdate_unset_nonbizday_day'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION set_nonbizday(date)
    RETURNS bool
    AS $$SELECT plvdate.set_nonbizday($1, false); SELECT NULL::boolean;$$
    LANGUAGE SQL VOLATILE STRICT;
    FUNCTION unset_nonbizday(date)
    RETURNS bool
    AS $$SELECT plvdate.unset_nonbizday($1, false); SELECT NULL::boolean;$$
    LANGUAGE SQL VOLATILE STRICT;
    FUNCTION use_easter(bool)
    RETURNS void
    AS 'MODULE_PATHNAME','plvdate_use_easter'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION use_easter()
    RETURNS bool
    AS $$SELECT plvdate.use_easter(true); SELECT NULL::boolean;$$
    LANGUAGE SQL VOLATILE STRICT;
    FUNCTION unuse_easter()
    RETURNS bool
    AS $$SELECT plvdate.use_easter(false); SELECT NULL::boolean;$$
    LANGUAGE SQL VOLATILE STRICT;
    FUNCTION using_easter()
    RETURNS bool
    AS 'MODULE_PATHNAME','plvdate_using_easter'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION use_great_friday(bool)
    RETURNS void
    AS 'MODULE_PATHNAME','plvdate_use_great_friday'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION use_great_friday()
    RETURNS bool
    AS $$SELECT plvdate.use_great_friday(true); SELECT NULL::boolean;$$
    LANGUAGE SQL VOLATILE STRICT;
    FUNCTION unuse_great_friday()
    RETURNS bool
    AS $$SELECT plvdate.use_great_friday(false); SELECT NULL::boolean;$$
    LANGUAGE SQL VOLATILE STRICT;
    FUNCTION using_great_friday()
    RETURNS bool
    AS 'MODULE_PATHNAME','plvdate_using_great_friday'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION include_start(bool)
    RETURNS void
    AS 'MODULE_PATHNAME','plvdate_include_start'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION include_start()
    RETURNS bool
    AS $$SELECT plvdate.include_start(true); SELECT NULL::boolean;$$
    LANGUAGE SQL VOLATILE STRICT;
    FUNCTION noinclude_start()
    RETURNS bool
    AS $$SELECT plvdate.include_start(false); SELECT NULL::boolean;$$
    LANGUAGE SQL VOLATILE STRICT;
    FUNCTION including_start()
    RETURNS bool
    AS 'MODULE_PATHNAME','plvdate_including_start'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION version()
    RETURNS cstring
    AS 'MODULE_PATHNAME','plvdate_version'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION default_holidays(text)
    RETURNS void
    AS 'MODULE_PATHNAME','plvdate_default_holidays'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION days_inmonth(date)
    RETURNS integer
    AS 'MODULE_PATHNAME','plvdate_days_inmonth'
    LANGUAGE C VOLATILE STRICT;
    FUNCTION isleapyear(date)
    RETURNS bool
    AS 'MODULE_PATHNAME','plvdate_isleapyear'
    LANGUAGE C VOLATILE STRICT;
$p$;
GRANT USAGE ON SCHEMA PLVDATE TO PUBLIC;

--package plvsubst
create package plvsubst as
$p$
     FUNCTION string(template_in text, values_in text[], subst text)
    RETURNS text;

    FUNCTION string(template_in text, values_in text[])
    RETURNS text;

    FUNCTION string(template_in text, vals_in text, delim_in text, subst_in text)
    RETURNS text;

    FUNCTION string(template_in text, vals_in text)
    RETURNS text;

    FUNCTION string(template_in text, vals_in text, delim_in text)
    RETURNS text;

    FUNCTION setsubst(str text)
    RETURNS void;

    FUNCTION setsubst()
    RETURNS void;

    FUNCTION subst()
    RETURNS text;
$p$;

create package body plvsubst as
$p$
    FUNCTION string(template_in text, values_in text[], subst text)
    RETURNS text
    AS 'MODULE_PATHNAME','plvsubst_string_array'
    LANGUAGE C IMMUTABLE;

    FUNCTION string(template_in text, values_in text[])
    RETURNS text
    AS $$SELECT plvsubst.string($1,$2, NULL);$$
    LANGUAGE SQL STRICT VOLATILE;

    FUNCTION string(template_in text, vals_in text, delim_in text, subst_in text)
    RETURNS text
    AS 'MODULE_PATHNAME','plvsubst_string_string'
    LANGUAGE C IMMUTABLE;

    FUNCTION string(template_in text, vals_in text)
    RETURNS text
    AS 'MODULE_PATHNAME','plvsubst_string_string'
    LANGUAGE C IMMUTABLE;

    FUNCTION string(template_in text, vals_in text, delim_in text)
    RETURNS text
    AS 'MODULE_PATHNAME','plvsubst_string_string'
    LANGUAGE C IMMUTABLE;

    FUNCTION setsubst(str text)
    RETURNS void
    AS 'MODULE_PATHNAME','plvsubst_setsubst'
    LANGUAGE C STRICT VOLATILE;

    FUNCTION setsubst()
    RETURNS void
    AS 'MODULE_PATHNAME','plvsubst_setsubst_default'
    LANGUAGE C STRICT VOLATILE;

    FUNCTION subst()
    RETURNS text
    AS 'MODULE_PATHNAME','plvsubst_subst'
    LANGUAGE C STRICT VOLATILE;

$p$;
GRANT USAGE ON SCHEMA PLVSUBST TO PUBLIC;

--package utl_file
create package utl_file as
$p$
    FUNCTION fopen(location text, filename text, open_mode text, max_linesize integer, encoding name)
    RETURNS integer;
    FUNCTION fopen(location text, filename text, open_mode text, max_linesize integer)
    RETURNS integer;
    FUNCTION fopen(location text, filename text, open_mode text)
    RETURNS integer;
    FUNCTION is_open(file integer)
    RETURNS bool;
    PROCEDURE get_line(file integer, OUT buffer text);
    PROCEDURE get_line(file integer, OUT buffer text, len integer);
    PROCEDURE get_nextline(file integer, OUT buffer text);
    PROCEDURE put(file integer, buffer text);
    PROCEDURE put(file integer, buffer anyelement);
    PROCEDURE new_line(file integer);
    PROCEDURE new_line(file integer, lines int);
    PROCEDURE put_line(file integer, buffer text);
    PROCEDURE put_line(file integer, buffer text, autoflush bool);
    PROCEDURE putf(file integer, format text, arg1 text, arg2 text, arg3 text, arg4 text, arg5 text);
    PROCEDURE putf(file integer, format text, arg1 text, arg2 text, arg3 text, arg4 text);
    PROCEDURE putf(file integer, format text, arg1 text, arg2 text, arg3 text);
    PROCEDURE putf(file integer, format text, arg1 text, arg2 text);
    PROCEDURE putf(file integer, format text, arg1 text);
    PROCEDURE putf(file integer, format text);
    PROCEDURE fflush(file integer);
    PROCEDURE fclose(file integer);
    PROCEDURE fclose_all();
    PROCEDURE fremove(location text, filename text);
    PROCEDURE frename(location text, filename text, dest_dir text, dest_file text, overwrite boolean);
    PROCEDURE frename(location text, filename text, dest_dir text, dest_file text);
    PROCEDURE fcopy(src_location text, src_filename text, dest_location text, dest_filename text);
    PROCEDURE fcopy(src_location text, src_filename text, dest_location text, dest_filename text, start_line integer);
    PROCEDURE fcopy(src_location text, src_filename text, dest_location text, dest_filename text, start_line integer, end_line integer);
    PROCEDURE fgetattr(location text, filename text, OUT fexists boolean, OUT file_length bigint, OUT blocksize integer);
    FUNCTION tmpdir()
    RETURNS text;
    PROCEDURE put_line(file integer, buffer anyelement);
    PROCEDURE put_line(file integer, buffer anyelement, autoflush bool);
$p$;

create package body utl_file as
$p$
    FUNCTION fopen(location text, filename text, open_mode text, max_linesize integer, encoding name)
    RETURNS integer
    AS 'MODULE_PATHNAME','utl_file_fopen'
    LANGUAGE C VOLATILE;
    FUNCTION fopen(location text, filename text, open_mode text, max_linesize integer)
    RETURNS integer
    AS 'MODULE_PATHNAME','utl_file_fopen'
    LANGUAGE C VOLATILE;
    FUNCTION fopen(location text, filename text, open_mode text)
    RETURNS integer
    AS $$SELECT utl_file.fopen($1, $2, $3, 1024); $$
    LANGUAGE SQL VOLATILE;
    FUNCTION is_open(file integer)
    RETURNS bool
    AS 'MODULE_PATHNAME','utl_file_is_open'
    LANGUAGE C VOLATILE;
    PROCEDURE get_line(file integer, OUT buffer text)
    AS 'MODULE_PATHNAME','utl_file_get_line'
    LANGUAGE C;
    PROCEDURE get_line(file integer, OUT buffer text, len integer)
    AS 'MODULE_PATHNAME','utl_file_get_line'
    LANGUAGE C;
    PROCEDURE get_nextline(file integer, OUT buffer text)
    AS 'MODULE_PATHNAME','utl_file_get_nextline'
    LANGUAGE C;
    PROCEDURE put(file integer, buffer text)
    AS 'MODULE_PATHNAME','utl_file_put'
    LANGUAGE C;
    PROCEDURE put(file integer, buffer anyelement)
    AS $$SELECT utl_file.put($1, $2::text); $$
    LANGUAGE SQL;
    PROCEDURE new_line(file integer)
    AS 'MODULE_PATHNAME','utl_file_new_line'
    LANGUAGE C;
    PROCEDURE new_line(file integer, lines int)
    AS 'MODULE_PATHNAME','utl_file_new_line'
    LANGUAGE C;
    PROCEDURE put_line(file integer, buffer text)
    AS 'MODULE_PATHNAME','utl_file_put_line'
    LANGUAGE C;
    PROCEDURE put_line(file integer, buffer text, autoflush bool)
    AS 'MODULE_PATHNAME','utl_file_put_line'
    LANGUAGE C;
    PROCEDURE putf(file integer, format text, arg1 text, arg2 text, arg3 text, arg4 text, arg5 text)
    AS 'MODULE_PATHNAME','utl_file_putf'
    LANGUAGE C;
    PROCEDURE putf(file integer, format text, arg1 text, arg2 text, arg3 text, arg4 text)
    AS $$call utl_file.putf($1, $2, $3, $4, $5, $6, NULL); $$
    LANGUAGE SQL;
    PROCEDURE putf(file integer, format text, arg1 text, arg2 text, arg3 text)
    AS $$call utl_file.putf($1, $2, $3, $4, $5, NULL, NULL); $$
    LANGUAGE SQL;
    PROCEDURE putf(file integer, format text, arg1 text, arg2 text)
    AS $$call utl_file.putf($1, $2, $3, $4, NULL, NULL, NULL); $$
    LANGUAGE SQL;
    PROCEDURE putf(file integer, format text, arg1 text)
    AS $$call utl_file.putf($1, $2, $3, NULL, NULL, NULL, NULL); $$
    LANGUAGE SQL;
    PROCEDURE putf(file integer, format text)
    AS $$call utl_file.putf($1, $2, NULL, NULL, NULL, NULL, NULL); $$
    LANGUAGE SQL;
    PROCEDURE fflush(file integer)
    AS 'MODULE_PATHNAME','utl_file_fflush'
    LANGUAGE C;
    PROCEDURE fclose(file integer)
    AS 'MODULE_PATHNAME','utl_file_fclose'
    LANGUAGE C;
    PROCEDURE fclose_all()
    AS 'MODULE_PATHNAME','utl_file_fclose_all'
    LANGUAGE C;
    PROCEDURE fremove(location text, filename text)
    AS 'MODULE_PATHNAME','utl_file_fremove'
    LANGUAGE C;
    PROCEDURE frename(location text, filename text, dest_dir text, dest_file text, overwrite boolean)
    AS 'MODULE_PATHNAME','utl_file_frename'
    LANGUAGE C;
    PROCEDURE frename(location text, filename text, dest_dir text, dest_file text)
    AS $$call utl_file.frename($1, $2, $3, $4, false);$$
    LANGUAGE SQL;
    PROCEDURE fcopy(src_location text, src_filename text, dest_location text, dest_filename text)
    AS 'MODULE_PATHNAME','utl_file_fcopy'
    LANGUAGE C;
    PROCEDURE fcopy(src_location text, src_filename text, dest_location text, dest_filename text, start_line integer)
    AS 'MODULE_PATHNAME','utl_file_fcopy'
    LANGUAGE C;
    PROCEDURE fcopy(src_location text, src_filename text, dest_location text, dest_filename text, start_line integer, end_line integer)
    AS 'MODULE_PATHNAME','utl_file_fcopy'
    LANGUAGE C;
    PROCEDURE fgetattr(location text, filename text, OUT fexists boolean, OUT file_length bigint, OUT blocksize integer)
    AS 'MODULE_PATHNAME','utl_file_fgetattr'
    LANGUAGE C;
    FUNCTION tmpdir()
    RETURNS text
    AS 'MODULE_PATHNAME','utl_file_tmpdir'
    LANGUAGE C VOLATILE;
    PROCEDURE put_line(file integer, buffer anyelement)
    AS $$call utl_file.put_line($1, $2::text); $$
    LANGUAGE SQL;
    PROCEDURE put_line(file integer, buffer anyelement, autoflush bool)
    AS $$call utl_file.put_line($1, $2::text, true); $$
    LANGUAGE SQL;
$p$;
GRANT USAGE ON SCHEMA UTL_FILE TO PUBLIC;

/* carry all safe directories */
CREATE TABLE utl_file.utl_file_dir(dir text, dirname text unique);
REVOKE ALL ON utl_file.utl_file_dir FROM PUBLIC;

/* allow only read on utl_file.utl_file_dir to unprivileged users */
GRANT SELECT ON TABLE utl_file.utl_file_dir TO PUBLIC;

/* dbms_stats package */
create package dbms_stats as
$p$
    PROCEDURE gather_table_stats (ownname text, tabname text, partname text default null, estimate_percent numeric default null,
        block_sample bool default false , method_opt text default null , degree numeric default null , granularity text default null ,
        cascade bool default true, stattab text default null , statid text default null , statown text default null , no_invalidate bool default false, force bool default true);
    PROCEDURE GATHER_DATABASE_STATS ( estimate_percent numeric DEFAULT NULL, block_sample bool DEFAULT FALSE, method_opt text DEFAULT NULL, degree numeric DEFAULT NULL,
        granularity text DEFAULT NULL,  cascade bool DEFAULT TRUE, stattab text DEFAULT NULL,  statid text DEFAULT NULL, options text DEFAULT 'GATHER',
        statown text DEFAULT NULL, gather_sys BOOLEAN  DEFAULT TRUE, no_invalidate BOOLEAN DEFAULT FALSE, obj_filter_list text DEFAULT NULL);
    PROCEDURE GET_TABLE_STATS (ownname text, tabname text, partname text DEFAULT NULL, stattab text DEFAULT NULL, statid text DEFAULT NULL, numrows INOUT numeric default 0,
        numblks INOUT numeric default 0, avgrlen INOUT numeric default 0, statown text DEFAULT NULL, cachedblk INOUT numeric default 0, cachehit INOUT numeric default 0);
    PROCEDURE GET_COLUMN_STATS (ownname text, tabname text, colname text, partname text DEFAULT NULL, stattab text DEFAULT NULL,
        statid text DEFAULT NULL, distcnt INOUT int DEFAULT 0, density INOUT numeric DEFAULT 0, nullcnt INOUT numeric DEFAULT 0,
        srec INOUT text DEFAULT null, avgclen INOUT numeric DEFAULT 0, statown text DEFAULT NULL);
    PROCEDURE GET_INDEX_STATS (ownname text, indname text, partname text DEFAULT NULL, stattab text DEFAULT NULL, statid text DEFAULT NULL, numrows INOUT numeric DEFAULT 0,
        numlblks INOUT numeric DEFAULT 0, numdist INOUT numeric DEFAULT 0, avglblk INOUT numeric DEFAULT 0, avgdblk INOUT numeric DEFAULT 0, clstfct INOUT numeric DEFAULT 0,
        indlevel INOUT numeric DEFAULT 0, statown text DEFAULT NULL, cachedblk INOUT numeric DEFAULT 0, cachehit INOUT numeric DEFAULT 0);
$p$;

create or replace package body dbms_stats as
$p$
    -- GATHER_TABLE_STATS
    PROCEDURE GATHER_TABLE_STATS (ownname text, tabname text, partname text default null, estimate_percent numeric default null,
        block_sample bool default false , method_opt text default null , degree numeric default null , granularity text default null ,
        cascade bool default true, stattab text default null , statid text default null , statown text default null , no_invalidate bool default false, force bool default true) AS $$
    DECLARE
        l_cur_user text;
        l_cur_user_is_superuser bool;
        l_tablename text;
        l_tableowner text;
    BEGIN
        IF partname IS NOT NULL THEN
            l_tablename := partname;
        ELSE
            l_tablename := tabname;
        END IF;

        ownname = ownname;
        BEGIN
            SELECT * INTO l_cur_user FROM current_user;
            SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;
            SELECT tableowner INTO l_tableowner FROM pg_tables WHERE tablename = l_tablename;
         /*
          * Add this exception to catch 'no data found' for SELECT INTO. Else the rest
          * statements are not executed.
          */
        EXCEPTION WHEN no_data_found THEN
            NULL;/* May be not found */
        END;

        IF l_tableowner IS NULL or l_tableowner != ownname THEN
            RAISE EXCEPTION 'table % does not exist', l_tablename;
        ELSEIF l_cur_user_is_superuser = true or l_cur_user = ownname THEN
            EXECUTE format('analyze %s', l_tablename);
        ELSE
            RAISE EXCEPTION 'User % has no privilege to analyze table %', l_cur_user, l_tablename;
        END IF;
    END;
    $$ LANGUAGE default_plsql;

    -- GATHER_DATABASE_STATS
    PROCEDURE GATHER_DATABASE_STATS (estimate_percent numeric DEFAULT NULL, block_sample bool DEFAULT FALSE, method_opt text DEFAULT NULL, degree numeric DEFAULT NULL,
        granularity text DEFAULT NULL,  cascade bool DEFAULT TRUE, stattab text DEFAULT NULL,  statid text DEFAULT NULL, options text DEFAULT 'GATHER',
        statown text DEFAULT NULL, gather_sys BOOLEAN  DEFAULT TRUE, no_invalidate BOOLEAN DEFAULT FALSE, obj_filter_list text DEFAULT NULL) AS $$
    DECLARE
        l_cur_user text;
        l_cur_user_is_superuser bool;
        r record;
    BEGIN
        SELECT * INTO l_cur_user FROM current_user;
        SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;

        IF l_cur_user_is_superuser = true THEN
	        analyze;
	    ELSE
	        FOR r IN
	            select tablename FROM pg_tables where tableowner = l_cur_user
            LOOP
                EXECUTE format('analyze %s', r.tablename);
            END LOOP;
        END IF;
    END;
    $$ LANGUAGE default_plsql;

    -- GET_TABLE_STATS
    CREATE OR REPLACE PROCEDURE GET_TABLE_STATS (ownname text, tabname text, partname text DEFAULT NULL, stattab text DEFAULT NULL, statid text DEFAULT NULL, numrows INOUT numeric default 0,
        numblks INOUT numeric default 0, avgrlen INOUT numeric default 0, statown text DEFAULT NULL, cachedblk INOUT numeric default 0, cachehit INOUT numeric default 0) AS $$
    DECLARE
        l_tableowner text;
        l_tablename text;
        l_cur_user text;
        l_cur_user_is_superuser bool;
    BEGIN
        IF partname IS NOT NULL THEN
            l_tablename := partname;
        ELSE
            l_tablename := tabname;
        END IF;

        ownname = ownname;
        BEGIN
            SELECT * INTO l_cur_user FROM current_user;
            SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;
            SELECT tableowner INTO l_tableowner FROM pg_tables WHERE tablename = l_tablename;
        /*
         * Add this exception to catch 'no data found' for SELECT INTO. Else the rest
         * statements are not executed.
         */
        EXCEPTION WHEN no_data_found THEN
            NULL;/* May be not found */
        END;

        IF l_tableowner IS NULL or l_tableowner != ownname THEN
            RAISE EXCEPTION 'table % does not exist', l_tablename;
        ELSEIF l_cur_user_is_superuser = true or l_cur_user = ownname THEN
            select COALESCE(reltuples,0), COALESCE(relpages,0) INTO numrows, numblks from pg_class where relname = l_tablename and relkind = 'r';
            select COALESCE(sum(COALESCE(avg_width,0)),0) INTO avgrlen from pg_stats where tablename = l_tablename;
            select COALESCE(heap_blks_hit,0)/COALESCE(NULLIF(heap_blks_hit + heap_blks_read,0), 1) INTO cachehit from pg_statio_user_tables where relname = l_tablename;
            cachedblk := cachehit * numblks;
        ELSE
            RAISE EXCEPTION 'User % has no privilege to analyze table %', l_cur_user, l_tablename;
        END IF;
    END;
    $$ LANGUAGE default_plsql;

    -- GET_COLUMN_STATS
    CREATE OR REPLACE PROCEDURE GET_COLUMN_STATS (ownname text, tabname text, colname text, partname text DEFAULT NULL, stattab text DEFAULT NULL,
        statid text DEFAULT NULL, distcnt INOUT int DEFAULT 0, density INOUT numeric DEFAULT 0, nullcnt INOUT numeric DEFAULT 0,
        srec INOUT text DEFAULT null, avgclen INOUT numeric DEFAULT 0, statown text DEFAULT NULL) AS $$
    DECLARE
        l_tablename text;
        l_tableowner text;
        l_cnt int;
        l_numrows int;
        l_distinct numeric;
        l_cur_user text;
        l_cur_user_is_superuser bool;
    BEGIN
        IF partname IS NOT NULL THEN
            l_tablename := partname;
        ELSE
            l_tablename := tabname;
        END IF;

        ownname = ownname;
        BEGIN
            SELECT * INTO l_cur_user FROM current_user;
            SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;
            SELECT tableowner INTO l_tableowner FROM pg_tables WHERE tablename = l_tablename;
        /*
         * Add this exception to catch 'no data found' for SELECT INTO. Else the rest
         * statements are not executed.
         */
        EXCEPTION WHEN no_data_found THEN
            NULL;/* May be not found */
        END;

        IF l_tableowner IS NULL or l_tableowner != ownname THEN
            RAISE EXCEPTION 'table % does not exist', l_tablename;
        ELSEIF l_cur_user_is_superuser = true or l_cur_user = ownname THEN
            select COALESCE(count(1),0) into l_cnt from pg_stats where tablename = l_tablename and attname = colname;
            IF l_cnt = 1 THEN
                select COALESCE(reltuples, 0) INTO l_numrows from pg_class where relname = l_tablename;
                select COALESCE(n_distinct,0) INTO l_distinct from pg_stats where tablename = l_tablename and attname = colname;
                IF l_distinct < 0 THEN
                    distcnt := ABS(l_distinct) * l_numrows;
                    density := 1.0/distcnt::numeric;
                ELSEIF l_distinct > 0 THEN
                    distcnt := l_distinct::int;
                    density := 1.0/distcnt::numeric;
                ELSE
                    density := 0;
                    distcnt := 0;
                END IF;
                select COALESCE(reltuples * null_frac, 0) INTO nullcnt from pg_class c, pg_stats s where c.relname = s.tablename and s.tablename = l_tablename and s.attname = colname;
                select COALESCE(avg_width, 0) INTO avgclen from pg_stats where tablename = l_tablename and attname = colname;
            ELSE
                RAISE EXCEPTION 'column % does not exist', colname;
            END IF;
        ELSE
            RAISE EXCEPTION 'User % has no privilege to analyze table %', l_cur_user, l_tablename;
        END IF;
    END;
    $$ LANGUAGE default_plsql;

    -- GET_INDEX_STATS
    CREATE OR REPLACE PROCEDURE GET_INDEX_STATS (ownname text, indname text, partname text DEFAULT NULL, stattab text DEFAULT NULL, statid text DEFAULT NULL, numrows INOUT numeric DEFAULT 0,
        numlblks INOUT numeric DEFAULT 0, numdist INOUT numeric DEFAULT 0, avglblk INOUT numeric DEFAULT 0, avgdblk INOUT numeric DEFAULT 0, clstfct INOUT numeric DEFAULT 0,
        indlevel INOUT numeric DEFAULT 0, statown text DEFAULT NULL, cachedblk INOUT numeric DEFAULT 0, cachehit INOUT numeric DEFAULT 0) AS $$
    DECLARE
        l_tableowner text;
        l_indexname text;
        r record;
        l_cur_user text;
        l_cur_user_is_superuser bool;
        distcnt int;
    BEGIN
        numdist := 1;
        IF partname IS NOT NULL THEN
            l_indexname := partname;
        ELSE
            l_indexname := indname;
        END IF;

        SELECT * INTO l_cur_user FROM current_user;
        SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;
        select tableowner INTO l_tableowner from pg_indexes ix, pg_tables t where ix.tablename = t.tablename and ix.indexname = l_indexname;

        IF l_tableowner IS NULL or l_tableowner != ownname THEN
            RAISE EXCEPTION 'index % does not exist', l_indexname;
        ELSEIF l_cur_user_is_superuser = true or l_cur_user = ownname THEN
            select COALESCE(reltuples,0), COALESCE(relpages,0) INTO numrows, numlblks from pg_class where relname = l_indexname and relkind = 'i';
            FOR r IN select COALESCE(s.n_distinct,0) n_distinct, COALESCE(s.avg_width,0) avg_width from
                            pg_class t,
                            pg_class i,
                            pg_index ix,
                            pg_attribute a,
                            pg_stats s
                        where
                            t.oid = ix.indrelid
                            and ix.indexrelid = i.oid
                            and a.attrelid = t.oid
                            and a.attnum = ANY(ix.indkey)
                            and i.relkind = 'i'
                            and i.relname = l_indexname
                            and s.tablename = t.relname
                            and s.attname = a.attname
            LOOP
                IF r.n_distinct < 0 THEN
                    distcnt := ABS(r.n_distinct) * numrows;
                ELSEIF r.n_distinct > 0 THEN
                    distcnt := r.n_distinct::int;
                ELSE
                    distcnt := 0;
                END IF;
                -- numdist/avglblk
                numdist := numdist * COALESCE(NULLIF(distcnt,0), 1);
                avglblk := avglblk + r.avg_width;
            END LOOP;
            numdist := LEAST(numrows, numdist);

            -- avgdblk
            SELECT COALESCE(sum(COALESCE(s.avg_width, 0)),0) INTO avgdblk FROM
                            pg_stats s,
                            pg_class t,
                            pg_class i,
                            pg_index ix
                        WHERE
                            i.relname = l_indexname
                            and i.relkind = 'i'
                            and i.oid = ix.indexrelid
                            and t.oid = ix.indrelid
                            and t.relname = s.tablename;
            select COALESCE(idx_blks_hit/COALESCE(NULLIF(idx_blks_hit + idx_blks_read,0), 1), 0) INTO cachehit from pg_statio_user_indexes where indexrelname = l_indexname;
            cachedblk := cachehit * numlblks;
        ELSE
            RAISE EXCEPTION 'User % has no privilege to analyze index %', l_cur_user, l_indexname;
        END IF;
    END;
    $$ LANGUAGE default_plsql;
$p$;
GRANT USAGE ON SCHEMA DBMS_STATS TO PUBLIC;

------------------------------------------------------------------------------------------------------------------------
--dbms_lob package define 
------------------------------------------------------------------------------------------------------------------------
create package dbms_lob as
$p$
	FUNCTION COMPARE(IN lob_1 BLOB, IN lob_2 BLOB, IN amount INTEGER default 2147483647, 
					IN offset_1 INTEGER default 1,IN offset_2 INTEGER default 1) RETURNS INTEGER;
	FUNCTION COMPARE(IN lob_1 CLOB, IN lob_2 CLOB, IN amount INTEGER default 2147483647, 
					IN offset_1 INTEGER default 1,IN offset_2 INTEGER default 1) RETURNS INTEGER;
    FUNCTION OPEN(IN lob BFILE, IN mode VARCHAR2) RETURNS void;
    FUNCTION GETLENGTH(IN lob BFILE) RETURNS BIGINT;
    FUNCTION READ(IN lob BFILE, IN amount BIGINT, IN seek_offet BIGINT, OUT buffer bytea);
    FUNCTION CLOSE(IN lob BFILE) RETURNS void;
    FUNCTION FILECLOSEALL() RETURNS void;
$p$;

create  package body dbms_lob as
$p$
FUNCTION COMPARE(IN lob_1 BLOB, IN lob_2 BLOB, IN amount INTEGER default 2147483647, 
				IN offset_1 INTEGER default 1,IN offset_2 INTEGER default 1) RETURNS INTEGER 
AS 'MODULE_PATHNAME','dbms_lob_compare_blob'
    LANGUAGE C IMMUTABLE STRICT;
FUNCTION COMPARE(IN lob_1 CLOB, IN lob_2 CLOB, IN amount INTEGER default 2147483647, 
				IN offset_1 INTEGER default 1,IN offset_2 INTEGER default 1) RETURNS INTEGER 
AS 'MODULE_PATHNAME','dbms_lob_compare_clob'
    LANGUAGE C IMMUTABLE STRICT;
FUNCTION OPEN(IN lob BFILE, IN mode VARCHAR2) RETURNS void
AS 'MODULE_PATHNAME','dbms_lob_open_bfile'
    LANGUAGE C IMMUTABLE STRICT;
FUNCTION GETLENGTH(IN lob BFILE) RETURNS BIGINT
AS 'MODULE_PATHNAME','dbms_lob_getlength'
    LANGUAGE C IMMUTABLE STRICT;
FUNCTION READ(IN lob BFILE, IN amount BIGINT, IN seek_offet BIGINT, OUT buffer bytea)
AS 'MODULE_PATHNAME','dbms_lob_read'
    LANGUAGE C IMMUTABLE STRICT;
FUNCTION CLOSE(IN lob BFILE) RETURNS void
AS 'MODULE_PATHNAME','dbms_lob_close'
    LANGUAGE C IMMUTABLE STRICT;
FUNCTION FILECLOSEALL() RETURNS void
AS 'MODULE_PATHNAME','dbms_lob_closeall'
    LANGUAGE C IMMUTABLE STRICT;
$p$;
GRANT USAGE ON SCHEMA DBMS_LOB TO PUBLIC;

------------------------------------------------------------------------------------------------------------------------
--dbms_metadata package define 
------------------------------------------------------------------------------------------------------------------------
create package dbms_metadata as
$p$
	FUNCTION GET_DDL (object_type IN VARCHAR2, 
					  name 		  IN VARCHAR2, 
					  schema_name IN VARCHAR2 DEFAULT NULL,
					  version     IN VARCHAR2 DEFAULT 'COMPATIBLE',
					  model       IN VARCHAR2 DEFAULT 'OPENTENBASE_ORA',
					  transform   IN VARCHAR2 DEFAULT 'DDL') RETURNS CLOB;
	
	FUNCTION SESSION_TRANSFORM() RETURNS NUMBER;
					  
	PROCEDURE SET_TRANSFORM_PARAM (transform_handle   IN NUMBER,
								   name               IN VARCHAR2,
								   value              IN VARCHAR2,
								   object_type        IN VARCHAR2 DEFAULT NULL);

	PROCEDURE SET_TRANSFORM_PARAM (transform_handle   IN NUMBER,
								   name               IN VARCHAR2,
								   value			  IN BOOLEAN DEFAULT TRUE,
								   object_type        IN VARCHAR2 DEFAULT NULL);

	PROCEDURE SET_TRANSFORM_PARAM (transform_handle   IN NUMBER,
								   name               IN VARCHAR2,
								   value              IN NUMBER,
								   object_type        IN VARCHAR2 DEFAULT NULL);
								   
  
$p$;

create  package body dbms_metadata as
$p$
FUNCTION GET_DDL (object_type IN VARCHAR2, 
		  name 	  IN VARCHAR2, 
		  schema_name IN VARCHAR2 DEFAULT NULL,
		  version     IN VARCHAR2 DEFAULT 'COMPATIBLE',
		  model       IN VARCHAR2 DEFAULT 'OPENTENBASE_ORA',
		  transform   IN VARCHAR2 DEFAULT 'DDL') RETURNS CLOB
AS 'MODULE_PATHNAME','dbms_metadata_get_ddl'
    LANGUAGE C IMMUTABLE; 

FUNCTION SESSION_TRANSFORM () RETURNS NUMBER
AS 'MODULE_PATHNAME','dbms_metadata_session_transform'
    LANGUAGE C IMMUTABLE;

PROCEDURE SET_TRANSFORM_PARAM (transform_handle   IN NUMBER,
			   name               IN VARCHAR2,
								   value              IN VARCHAR2,
								   object_type        IN VARCHAR2 DEFAULT NULL)
AS 'MODULE_PATHNAME','dbms_md_set_transform_param_p3char'
    LANGUAGE C;

PROCEDURE SET_TRANSFORM_PARAM (transform_handle   IN NUMBER,
								   name               IN VARCHAR2,
								   value              IN BOOLEAN DEFAULT TRUE,
								   object_type        IN VARCHAR2 DEFAULT NULL)
AS 'MODULE_PATHNAME','dbms_md_set_transform_param_p3bool'
    LANGUAGE C;

PROCEDURE SET_TRANSFORM_PARAM (transform_handle   IN NUMBER,
								   name               IN VARCHAR2,
								   value              IN NUMBER,
								   object_type        IN VARCHAR2 DEFAULT NULL)
AS 'MODULE_PATHNAME','dbms_md_set_transform_param_p3number'
    LANGUAGE C;

$p$;
GRANT USAGE ON SCHEMA DBMS_METADATA TO PUBLIC;

-- Since PACKAGE feature does not support to CREATE AGGREGATE, use schema instead.
CREATE schema wmsys;
CREATE AGGREGATE wmsys.wm_concat(
	sfunc1 = wm_concat_text_transfn, FINALFUNC = list_agg_finalfn, basetype = text, stype1 = internal
);
CREATE AGGREGATE wmsys.wm_concat(
	sfunc1 = wm_concat_int_transfn, FINALFUNC = list_agg_finalfn, basetype = int, stype1 = internal
);
CREATE AGGREGATE wmsys.wm_concat(
	sfunc1 = wm_concat_numeric_transfn, FINALFUNC = list_agg_finalfn, basetype = numeric, stype1 = internal
);
GRANT USAGE ON SCHEMA WMSYS TO PUBLIC;

CREATE FUNCTION pg_catalog.median4_transfn(internal, real)
RETURNS internal
AS 'MODULE_PATHNAME','orafce_median4_transfn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pg_catalog.median4_finalfn(internal)
RETURNS real
AS 'MODULE_PATHNAME','orafce_median4_finalfn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pg_catalog.median8_transfn(internal, double precision)
RETURNS internal
AS 'MODULE_PATHNAME','orafce_median8_transfn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pg_catalog.median8_finalfn(internal)
RETURNS double precision
AS 'MODULE_PATHNAME','orafce_median8_finalfn'
LANGUAGE C IMMUTABLE;

CREATE AGGREGATE pg_catalog.median(real) (
  SFUNC=pg_catalog.median4_transfn,
  STYPE=internal,
  FINALFUNC=pg_catalog.median4_finalfn
);

CREATE AGGREGATE pg_catalog.median(double precision) (
  SFUNC=pg_catalog.median8_transfn,
  STYPE=internal,
  FINALFUNC=pg_catalog.median8_finalfn
);

/* adapt opentenbase_ora empty_clob ()*/
CREATE OR REPLACE FUNCTION pg_catalog.empty_clob()
RETURNS clob
AS 'MODULE_PATHNAME','get_empty_clob'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- Support SIMPLE_INTEGER
/*
 * SIMPLE_INTEGER will not raise exception when overflow, therefore will rewrite
 * the some operators: + - * / neg(-). Other operator will reuse pls_integer's.
 * 
 * NB: power() in OpenTenBase_Ora (^ in OpenTenBase) are not applied on SIMPLE_INTEGER. That
 * is to say simple_integer will converted to integer to calculate and exception
 * will be raised if over/under flow.
 */
CREATE TYPE simple_integer;
CREATE FUNCTION simple_integer_in(cstring)
	RETURNS simple_integer
	AS 'int4in'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE FUNCTION simple_integer_out(simple_integer)
	RETURNS cstring
	AS 'int4out'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE FUNCTION simple_integer_recv(internal,oid,int4)
	RETURNS simple_integer
	AS 'int4recv'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE FUNCTION simple_integer_send(simple_integer)
	RETURNS bytea
	AS 'int4send'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE TYPE simple_integer (
	input = simple_integer_in,
	output = simple_integer_out,
	send = simple_integer_send,
	receive = simple_integer_recv,
	like = int4
);

CREATE CAST (simple_integer AS int) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (int AS simple_integer) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (simple_integer AS float8) WITH INOUT AS IMPLICIT; -- For usage of float operators
CREATE CAST (simple_integer AS numeric) WITH INOUT AS IMPLICIT; -- For usage of numeric operators

CREATE FUNCTION simpint_int4_pl(simple_integer, int4)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_pl'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_pl(int4, simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_pl'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int4_mi(simple_integer, int4)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_mi'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_mi(int4, simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_mi'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int4_ml(simple_integer, int4)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_ml'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_ml(int4, simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_ml'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int4_div(simple_integer, int4)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_div'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_div(int4, simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_div'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_um(simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_um'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int2_pl(simple_integer, int2)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int2_pl'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int2_simpint_pl(int2, simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int2_pl'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int2_mi(simple_integer, int2)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int2_mi'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int2_simpint_mi(int2, simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int2_mi'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int2_ml(simple_integer, int2)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int2_ml'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int2_simpint_ml(int2, simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int2_ml'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int2_div(simple_integer, int2)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int2_div'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int2_simpint_div(int2, simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int2_div'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR + (
	leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_pl);
CREATE OPERATOR + (
	leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_pl);

CREATE OPERATOR - (
	leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_mi);
CREATE OPERATOR - (
	leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_mi);

CREATE OPERATOR * (
	leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_ml);
CREATE OPERATOR * (
	leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_ml);

CREATE OPERATOR / (
	leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_div);
CREATE OPERATOR / (
	leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_div);

CREATE OPERATOR - (
	rightarg = simple_integer,
	function = simpint_um);

CREATE OPERATOR + (
	leftarg = simple_integer,
	rightarg = int2,
	function = simpint_int2_pl);
CREATE OPERATOR + (
	leftarg = int2,
	rightarg = simple_integer,
	function = int2_simpint_pl);

CREATE OPERATOR - (
	leftarg = simple_integer,
	rightarg = int2,
	function = simpint_int2_mi);
CREATE OPERATOR - (
	leftarg = int2,
	rightarg = simple_integer,
	function = int2_simpint_mi);

CREATE OPERATOR * (
	leftarg = simple_integer,
	rightarg = int2,
	function = simpint_int2_ml);
CREATE OPERATOR * (
	leftarg = int2,
	rightarg = simple_integer,
	function = int2_simpint_ml);

CREATE OPERATOR / (
	leftarg = simple_integer,
	rightarg = int2,
	function = simpint_int2_div);
CREATE OPERATOR / (
	leftarg = int2,
	rightarg = simple_integer,
	function = int2_simpint_div);

-- package utl_raw
--CREATE CAST (bytea AS raw) WITHOUT FUNCTION AS IMPLICIT;
create package utl_raw as
$p$
	FUNCTION cast_to_raw(str varchar2) RETURNS raw;
    FUNCTION cast_to_varchar2(str raw) RETURNS varchar2;
    FUNCTION cast_to_varchar2(str text) RETURNS varchar2;
    FUNCTION length(str raw) RETURNS int;
    FUNCTION bit_and(str1 raw, str2 raw) RETURNS raw;
    FUNCTION bit_or(str1 raw, str2 raw) RETURNS raw;
    FUNCTION bit_xor(str1 raw, str2 raw) RETURNS raw;
    FUNCTION bit_complement(str1 raw) RETURNS raw;
    FUNCTION reverse(str raw) RETURNS raw;
    FUNCTION compare(str1 raw, str2 raw) RETURNS int;
    FUNCTION substr(str raw, pos INTEGER, len INTEGER) RETURNS raw;
    FUNCTION substr(str raw, pos INTEGER) RETURNS raw;
    FUNCTION copies(str raw, n NUMBER) RETURNS raw;
    FUNCTION concat(r1 raw DEFAULT NULL, r2 raw DEFAULT NULL, r3 raw DEFAULT NULL, r4 raw DEFAULT NULL, r5 raw DEFAULT NULL, r6 raw DEFAULT NULL, r7 raw DEFAULT NULL, r8 raw DEFAULT NULL, r9 raw DEFAULT NULL, r10 raw DEFAULT NULL, r11 raw DEFAULT NULL, r12 raw DEFAULT NULL) RETURNS raw;
	
    FUNCTION cast_to_varchar2(str long raw) RETURNS varchar2;
$p$;

create package body utl_raw as
$p$
  FUNCTION cast_to_raw(str varchar2)
  RETURNS raw
  AS 'MODULE_PATHNAME','cast_to_raw'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION cast_to_varchar2(str raw)
  RETURNS varchar2
  AS 'MODULE_PATHNAME','cast_to_varchar2'
  LANGUAGE C IMMUTABLE;
  FUNCTION cast_to_varchar2(str text)
  RETURNS varchar2
  AS 'MODULE_PATHNAME','cast_text_to_varchar2'
  LANGUAGE C IMMUTABLE;
  FUNCTION length(str raw)
  RETURNS int
  AS 'MODULE_PATHNAME','raw_strlen'
  LANGUAGE C IMMUTABLE;
  FUNCTION bit_and(str1 raw, str2 raw)
  RETURNS raw
  AS 'MODULE_PATHNAME','raw_bitand'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION bit_or(str1 raw, str2 raw)
  RETURNS raw
  AS 'MODULE_PATHNAME','raw_bitor'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION bit_xor(str1 raw, str2 raw)
  RETURNS raw
  AS 'MODULE_PATHNAME','raw_bitxor'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION bit_complement(str1 raw)
  RETURNS raw
  AS 'MODULE_PATHNAME','raw_bitcomplement'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION reverse(str raw)
  RETURNS raw
  AS 'MODULE_PATHNAME','raw_reverse'
  LANGUAGE C IMMUTABLE;
  FUNCTION compare(str1 raw, str2 raw)
  RETURNS int
  AS 'MODULE_PATHNAME','raw_compare'
  LANGUAGE C IMMUTABLE;
  FUNCTION substr(str raw, pos INTEGER, len INTEGER)
  RETURNS raw
  AS 'MODULE_PATHNAME','raw_substr'
  LANGUAGE C IMMUTABLE;
  FUNCTION substr(str raw, pos INTEGER)
  RETURNS raw
  AS 'MODULE_PATHNAME','raw_substr'
  LANGUAGE C IMMUTABLE;
  FUNCTION copies(str raw, n NUMBER)
  RETURNS raw
  AS 'MODULE_PATHNAME','raw_copies'
  LANGUAGE C IMMUTABLE;
  FUNCTION concat(r1 raw DEFAULT NULL, r2 raw DEFAULT NULL, r3 raw DEFAULT NULL, r4 raw DEFAULT NULL, r5 raw DEFAULT NULL, r6 raw DEFAULT NULL, r7 raw DEFAULT NULL, r8 raw DEFAULT NULL, r9 raw DEFAULT NULL, r10 raw DEFAULT NULL, r11 raw DEFAULT NULL, r12 raw DEFAULT NULL)
  RETURNS raw
  AS 'MODULE_PATHNAME','raw_concat'
  LANGUAGE C IMMUTABLE;
  
  FUNCTION cast_to_varchar2(str long raw)
  RETURNS varchar2
  AS 'MODULE_PATHNAME','cast_to_varchar2'
  LANGUAGE C IMMUTABLE;
$p$;
GRANT USAGE ON SCHEMA UTL_RAW TO PUBLIC;

create package DBMS_SESSION as
$p$
    FUNCTION unique_session_id() RETURNS varchar2;
$p$;

create package body DBMS_SESSION as
$p$
    FUNCTION unique_session_id()
    RETURNS varchar2
    AS 'MODULE_PATHNAME','dbms_session_unique_session_id'
    LANGUAGE C IMMUTABLE;
$p$;
GRANT USAGE ON SCHEMA DBMS_SESSION TO PUBLIC;
