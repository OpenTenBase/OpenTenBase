/* orafce--3.12.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION orafce" to load this file. \quit

create package dbms_output as
$p$
	FUNCTION enable(IN buffer_size integer) returns void;
    FUNCTION enable() returns void;
    FUNCTION disable() RETURNS void;
    FUNCTION serveroutput(IN bool) RETURNS void;
    FUNCTION put(IN a text) RETURNS void;
    FUNCTION put_line(IN a text) RETURNS void;
    FUNCTION new_line() RETURNS void;
    FUNCTION get_line(OUT line text, OUT status int4);
    FUNCTION get_lines(OUT lines text[], INOUT numlines int4);
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
    LANGUAGE C IMMUTABLE STRICT;
function put_line(IN a text) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line'
    LANGUAGE C IMMUTABLE STRICT;
function new_line() RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_new_line'    
   LANGUAGE C IMMUTABLE STRICT;
function get_line(OUT line text, OUT status int4)
   AS 'MODULE_PATHNAME','dbms_output_get_line'    
   LANGUAGE C IMMUTABLE STRICT;
function get_lines(OUT lines text[], INOUT numlines int4)
   AS 'MODULE_PATHNAME','dbms_output_get_lines'    
   LANGUAGE C IMMUTABLE STRICT;
$p$;

-- package dbms_assert
create package dbms_assert as
$p$
    FUNCTION enquote_literal(str varchar) RETURNS varchar;
    FUNCTION enquote_name(str varchar, loweralize boolean)RETURNS varchar;
    FUNCTION enquote_name(str varchar) RETURNS varchar;
    FUNCTION noop(str varchar) RETURNS varchar;
    FUNCTION schema_name(str varchar) RETURNS varchar;
    FUNCTION object_name(str varchar) RETURNS varchar;
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
  AS 'SELECT dbms_assert.enquote_name($1, true)'
  LANGUAGE SQL IMMUTABLE STRICT;
  FUNCTION noop(str varchar)
  RETURNS varchar
  AS 'MODULE_PATHNAME','dbms_assert_noop'
  LANGUAGE C IMMUTABLE STRICT;
  FUNCTION schema_name(str varchar)
  RETURNS varchar
  AS 'MODULE_PATHNAME','dbms_assert_schema_name'
  LANGUAGE C IMMUTABLE;
  FUNCTION object_name(str varchar)
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

-- package dbms_utility
create package dbms_utility as
$p$
  FUNCTION format_call_stack(text) RETURNS text;
  FUNCTION format_call_stack() RETURNS text;
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
$p$;

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
    FUNCTION get_line(file integer, OUT buffer text);
    FUNCTION get_line(file integer, OUT buffer text, len integer);
    FUNCTION get_nextline(file integer, OUT buffer text);
    FUNCTION put(file integer, buffer text)
    RETURNS bool;
    FUNCTION put(file integer, buffer anyelement)
    RETURNS bool;
    FUNCTION new_line(file integer)
    RETURNS bool;
    FUNCTION new_line(file integer, lines int)
    RETURNS bool;
    FUNCTION put_line(file integer, buffer text)
    RETURNS bool;
    FUNCTION put_line(file integer, buffer text, autoflush bool)
    RETURNS bool;
    FUNCTION putf(file integer, format text, arg1 text, arg2 text, arg3 text, arg4 text, arg5 text)
    RETURNS bool;
    FUNCTION putf(file integer, format text, arg1 text, arg2 text, arg3 text, arg4 text)
    RETURNS bool;
    FUNCTION putf(file integer, format text, arg1 text, arg2 text, arg3 text)
    RETURNS bool;
    FUNCTION putf(file integer, format text, arg1 text, arg2 text)
    RETURNS bool;
    FUNCTION putf(file integer, format text, arg1 text)
    RETURNS bool;
    FUNCTION putf(file integer, format text)
    RETURNS bool;
    FUNCTION fflush(file integer)
    RETURNS void;
    FUNCTION fclose(file integer)
    RETURNS integer;
    FUNCTION fclose_all()
    RETURNS void;
    FUNCTION fremove(location text, filename text)
    RETURNS void;
    FUNCTION frename(location text, filename text, dest_dir text, dest_file text, overwrite boolean)
    RETURNS void;
    FUNCTION frename(location text, filename text, dest_dir text, dest_file text)
    RETURNS void;
    FUNCTION fcopy(src_location text, src_filename text, dest_location text, dest_filename text)
    RETURNS void;
    FUNCTION fcopy(src_location text, src_filename text, dest_location text, dest_filename text, start_line integer)
    RETURNS void;
    FUNCTION fcopy(src_location text, src_filename text, dest_location text, dest_filename text, start_line integer, end_line integer)
    RETURNS void;
    FUNCTION fgetattr(location text, filename text, OUT fexists boolean, OUT file_length bigint, OUT blocksize integer);
    FUNCTION tmpdir()
    RETURNS text;
    FUNCTION put_line(file integer, buffer anyelement)
    RETURNS bool;
    FUNCTION put_line(file integer, buffer anyelement, autoflush bool)
    RETURNS bool;
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
    FUNCTION get_line(file integer, OUT buffer text)
    AS 'MODULE_PATHNAME','utl_file_get_line'
    LANGUAGE C VOLATILE;
    FUNCTION get_line(file integer, OUT buffer text, len integer)
    AS 'MODULE_PATHNAME','utl_file_get_line'
    LANGUAGE C VOLATILE;
    FUNCTION get_nextline(file integer, OUT buffer text)
    AS 'MODULE_PATHNAME','utl_file_get_nextline'
    LANGUAGE C VOLATILE;
    FUNCTION put(file integer, buffer text)
    RETURNS bool
    AS 'MODULE_PATHNAME','utl_file_put'
    LANGUAGE C VOLATILE;
    FUNCTION put(file integer, buffer anyelement)
    RETURNS bool
    AS $$SELECT utl_file.put($1, $2::text); $$
    LANGUAGE SQL VOLATILE;
    FUNCTION new_line(file integer)
    RETURNS bool
    AS 'MODULE_PATHNAME','utl_file_new_line'
    LANGUAGE C VOLATILE;
    FUNCTION new_line(file integer, lines int)
    RETURNS bool
    AS 'MODULE_PATHNAME','utl_file_new_line'
    LANGUAGE C VOLATILE;
    FUNCTION put_line(file integer, buffer text)
    RETURNS bool
    AS 'MODULE_PATHNAME','utl_file_put_line'
    LANGUAGE C VOLATILE;
    FUNCTION put_line(file integer, buffer text, autoflush bool)
    RETURNS bool
    AS 'MODULE_PATHNAME','utl_file_put_line'
    LANGUAGE C VOLATILE;
    FUNCTION putf(file integer, format text, arg1 text, arg2 text, arg3 text, arg4 text, arg5 text)
    RETURNS bool
    AS 'MODULE_PATHNAME','utl_file_putf'
    LANGUAGE C VOLATILE;
    FUNCTION putf(file integer, format text, arg1 text, arg2 text, arg3 text, arg4 text)
    RETURNS bool
    AS $$SELECT utl_file.putf($1, $2, $3, $4, $5, $6, NULL); $$
    LANGUAGE SQL VOLATILE;
    FUNCTION putf(file integer, format text, arg1 text, arg2 text, arg3 text)
    RETURNS bool
    AS $$SELECT utl_file.putf($1, $2, $3, $4, $5, NULL, NULL); $$
    LANGUAGE SQL VOLATILE;
    FUNCTION putf(file integer, format text, arg1 text, arg2 text)
    RETURNS bool
    AS $$SELECT utl_file.putf($1, $2, $3, $4, NULL, NULL, NULL); $$
    LANGUAGE SQL VOLATILE;
    FUNCTION putf(file integer, format text, arg1 text)
    RETURNS bool
    AS $$SELECT utl_file.putf($1, $2, $3, NULL, NULL, NULL, NULL); $$
    LANGUAGE SQL VOLATILE;
    FUNCTION putf(file integer, format text)
    RETURNS bool
    AS $$SELECT utl_file.putf($1, $2, NULL, NULL, NULL, NULL, NULL); $$
    LANGUAGE SQL VOLATILE;
    FUNCTION fflush(file integer)
    RETURNS void
    AS 'MODULE_PATHNAME','utl_file_fflush'
    LANGUAGE C VOLATILE;
    FUNCTION fclose(file integer)
    RETURNS integer
    AS 'MODULE_PATHNAME','utl_file_fclose'
    LANGUAGE C VOLATILE;
    FUNCTION fclose_all()
    RETURNS void
    AS 'MODULE_PATHNAME','utl_file_fclose_all'
    LANGUAGE C VOLATILE;
    FUNCTION fremove(location text, filename text)
    RETURNS void
    AS 'MODULE_PATHNAME','utl_file_fremove'
    LANGUAGE C VOLATILE;
    FUNCTION frename(location text, filename text, dest_dir text, dest_file text, overwrite boolean)
    RETURNS void
    AS 'MODULE_PATHNAME','utl_file_frename'
    LANGUAGE C VOLATILE;
    FUNCTION frename(location text, filename text, dest_dir text, dest_file text)
    RETURNS void
    AS $$SELECT utl_file.frename($1, $2, $3, $4, false);$$
    LANGUAGE SQL VOLATILE;
    FUNCTION fcopy(src_location text, src_filename text, dest_location text, dest_filename text)
    RETURNS void
    AS 'MODULE_PATHNAME','utl_file_fcopy'
    LANGUAGE C VOLATILE;
    FUNCTION fcopy(src_location text, src_filename text, dest_location text, dest_filename text, start_line integer)
    RETURNS void
    AS 'MODULE_PATHNAME','utl_file_fcopy'
    LANGUAGE C VOLATILE;
    FUNCTION fcopy(src_location text, src_filename text, dest_location text, dest_filename text, start_line integer, end_line integer)
    RETURNS void
    AS 'MODULE_PATHNAME','utl_file_fcopy'
    LANGUAGE C VOLATILE;
    FUNCTION fgetattr(location text, filename text, OUT fexists boolean, OUT file_length bigint, OUT blocksize integer)
    AS 'MODULE_PATHNAME','utl_file_fgetattr'
    LANGUAGE C VOLATILE;
    FUNCTION tmpdir()
    RETURNS text
    AS 'MODULE_PATHNAME','utl_file_tmpdir'
    LANGUAGE C VOLATILE;
    FUNCTION put_line(file integer, buffer anyelement)
    RETURNS bool
    AS $$SELECT utl_file.put_line($1, $2::text); $$
    LANGUAGE SQL VOLATILE;
    FUNCTION put_line(file integer, buffer anyelement, autoflush bool)
    RETURNS bool
    AS $$SELECT utl_file.put_line($1, $2::text, true); $$
    LANGUAGE SQL VOLATILE;
$p$;

/* carry all safe directories */
CREATE TABLE utl_file.utl_file_dir(dir text, dirname text unique);
REVOKE ALL ON utl_file.utl_file_dir FROM PUBLIC;

/* allow only read on utl_file.utl_file_dir to unprivileged users */
GRANT SELECT ON TABLE utl_file.utl_file_dir TO PUBLIC;
