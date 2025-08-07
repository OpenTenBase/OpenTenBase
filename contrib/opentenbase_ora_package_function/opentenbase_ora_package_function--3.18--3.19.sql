-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.19'" to load this file. \quit
-- support regexp_instr
CREATE OR REPLACE FUNCTION regexp_instr(str text, patt text)
RETURNS int
AS 'MODULE_PATHNAME','orcl_regexp_instr_2'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION regexp_instr(str text, patt text, pos int)
RETURNS int
AS 'MODULE_PATHNAME','orcl_regexp_instr_3'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION regexp_instr(str text, patt text, pos int, occurrence int)
RETURNS int
AS 'MODULE_PATHNAME','orcl_regexp_instr_4'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION regexp_instr(str text, patt text, pos int, occurrence int, return_opt int)
RETURNS int
AS 'MODULE_PATHNAME','orcl_regexp_instr_5'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION regexp_instr(str text, patt text, pos int, occurrence int, return_opt int, match_param text)
RETURNS int
AS 'MODULE_PATHNAME','orcl_regexp_instr_6'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION regexp_instr(str text, patt text, pos int, occurrence int, return_opt int, match_param text, subexpr int)
RETURNS int
AS 'MODULE_PATHNAME','orcl_regexp_instr_7'
LANGUAGE C IMMUTABLE STRICT;
/* OpenTenBase_Ora asciistr()*/
CREATE OR REPLACE FUNCTION pg_catalog.asciistr(text)
RETURNS text
AS 'MODULE_PATHNAME','ora_asciistr'
LANGUAGE C STABLE PARALLEL SAFE STRICT;

DROP PACKAGE dbms_lob;
CREATE PACKAGE dbms_lob AS
$P$
	FUNCTION COMPARE(IN lob_1 BLOB, IN lob_2 BLOB, IN amount INTEGER default 2147483647,
					IN offset_1 INTEGER default 1,IN offset_2 INTEGER default 1) RETURNS INTEGER;
	FUNCTION COMPARE(IN lob_1 CLOB, IN lob_2 CLOB, IN amount INTEGER default 2147483647,
					IN offset_1 INTEGER default 1,IN offset_2 INTEGER default 1) RETURNS INTEGER;
	FUNCTION OPEN(IN lob BFILE, IN mode VARCHAR2) RETURNS void;
	FUNCTION GETLENGTH(IN lob BFILE) RETURNS BIGINT;
	FUNCTION GETLENGTH(IN lob CLOB) RETURNS INT;
	FUNCTION GETLENGTH(IN lob BLOB) RETURNS INT;
	FUNCTION SUBSTR (lob_loc IN BLOB, amount IN INTEGER default 32767, offset_1 IN INTEGER default 1) RETURNS RAW;
	FUNCTION SUBSTR (lob_loc IN CLOB, amount IN INTEGER default 32767, offset_1 IN INTEGER default 1) RETURNS VARCHAR2;
	FUNCTION READ_INNER(IN lob BFILE, IN amount BIGINT, IN seek_offet BIGINT) RETURNS raw;
	PROCEDURE READ(IN lob BFILE, IN amount BIGINT, IN seek_offet BIGINT, OUT buffer raw);
	FUNCTION CLOSE(IN lob BFILE) RETURNS void;
	FUNCTION FILECLOSEALL() RETURNS void;
	PROCEDURE CREATETEMPORARY(lob_loc IN OUT BLOB, cache IN BOOLEAN, dur IN PLS_INTEGER := 10);
	PROCEDURE CREATETEMPORARY(lob_loc IN OUT CLOB, cache IN BOOLEAN, dur IN PLS_INTEGER := 10);
	PROCEDURE FREETEMPORARY(lob_loc IN OUT BLOB);
	PROCEDURE FREETEMPORARY(lob_loc IN OUT CLOB);
$P$;

CREATE PACKAGE BODY dbms_lob AS
$P$
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

	FUNCTION GETLENGTH(IN lob CLOB)
	RETURNS INT
	AS 'textlen'
	LANGUAGE internal STRICT IMMUTABLE;

	FUNCTION GETLENGTH(IN lob BLOB)
	RETURNS INT
	AS 'byteaoctetlen'
	LANGUAGE internal STRICT IMMUTABLE;

	FUNCTION SUBSTR (lob_loc IN BLOB, amount IN INTEGER default 32767, offset_1 IN INTEGER default 1) RETURNS RAW
	AS  'MODULE_PATHNAME','dbms_lob_substr_blob'
	LANGUAGE C IMMUTABLE STRICT;

	FUNCTION SUBSTR (lob_loc IN CLOB, amount IN INTEGER default 32767, offset_1 IN INTEGER default 1) RETURNS VARCHAR2
	AS  'MODULE_PATHNAME','dbms_lob_substr_clob'
	LANGUAGE C IMMUTABLE STRICT;

	FUNCTION READ_INNER(IN lob BFILE, IN amount BIGINT, IN seek_offet BIGINT) RETURNS raw
		AS 'MODULE_PATHNAME','dbms_lob_read'
		LANGUAGE C IMMUTABLE STRICT;
	
	PROCEDURE READ(IN lob BFILE, IN amount BIGINT, IN seek_offet BIGINT, OUT buffer raw)
		AS 'begin select dbms_lob.READ_INNER(lob, amount, seek_offet) into buffer; end;'
		language default_plsql;
	FUNCTION CLOSE(IN lob BFILE) RETURNS void
		AS 'MODULE_PATHNAME','dbms_lob_close'
		LANGUAGE C IMMUTABLE STRICT;

	FUNCTION FILECLOSEALL() RETURNS void
		AS 'MODULE_PATHNAME','dbms_lob_closeall'
		LANGUAGE C IMMUTABLE STRICT;
/*
 * Using the CREATETEMPORARY syntax, we can specify the life cycle of LOB, 
 * but in the new version of OpenTenBase_Ora(10+), the life cycle of the LOB is determined 
 * by the scope(i.e. the PL/SQL block) where the LOB is declared. When leave the 
 * scope of declaration, LOB will be released automatically("SQL semantics for LOB").
 * So the procedure here only provide syntax interfaces.
 */
	PROCEDURE CREATETEMPORARY(lob_loc IN OUT BLOB, cache IN BOOLEAN, dur IN PLS_INTEGER := 10)
		AS $$ BEGIN NULL; END; $$
		LANGUAGE default_plsql;

	PROCEDURE CREATETEMPORARY(lob_loc IN OUT CLOB, cache IN BOOLEAN, dur IN PLS_INTEGER := 10)
		AS $$ BEGIN NULL; END; $$
		LANGUAGE default_plsql;

	PROCEDURE FREETEMPORARY(lob_loc IN OUT BLOB)
		AS $$ BEGIN NULL; END; $$
		LANGUAGE default_plsql;

	PROCEDURE FREETEMPORARY(lob_loc IN OUT CLOB)
		AS $$ BEGIN NULL; END; $$
		LANGUAGE default_plsql;
$P$;
GRANT USAGE ON SCHEMA DBMS_LOB TO PUBLIC;

-- redefine dbms_utility
drop package dbms_utility;
-- package dbms_utility
create package dbms_utility as
$p$
  FUNCTION format_call_stack(text) RETURNS text;
  FUNCTION format_call_stack() RETURNS text;
  FUNCTION get_hash_value(VARCHAR2, NUMBER, NUMBER) RETURNS NUMBER;
  FUNCTION get_time() RETURNS NUMBER;
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

  FUNCTION dbms_utility.get_time()
  RETURNS NUMBER
  AS 'MODULE_PATHNAME','dbms_utility_get_time'
  LANGUAGE C VOLATILE;
$p$;

GRANT USAGE ON SCHEMA DBMS_UTILITY TO PUBLIC;

create package dbms_lock as
$p$
  PROCEDURE sleep(seconds IN NUMBER);
$p$;

create package body dbms_lock as
$p$
  PROCEDURE sleep(seconds IN NUMBER)
  AS $$SELECT pg_sleep(seconds); $$
  LANGUAGE SQL;
$p$;

GRANT USAGE ON SCHEMA DBMS_LOCK TO PUBLIC;

-- redefine DBMS_SESSION
drop package DBMS_SESSION;
create package DBMS_SESSION as
$p$
    FUNCTION unique_session_id() RETURNS varchar2;
    PROCEDURE reset_package();
    PROCEDURE free_unused_user_memory();
$p$;

create package body DBMS_SESSION as
$p$
    FUNCTION unique_session_id()
    RETURNS varchar2
    AS 'MODULE_PATHNAME','dbms_session_unique_session_id'
    LANGUAGE C IMMUTABLE;
    PROCEDURE reset_package()
    AS 'MODULE_PATHNAME','dbms_session_reset_package'
    LANGUAGE C;
    PROCEDURE free_unused_user_memory()
    AS 'MODULE_PATHNAME','dbms_session_free_unused_user_memory'
    LANGUAGE C;
$p$;

GRANT USAGE ON SCHEMA DBMS_SESSION TO PUBLIC;
DROP PACKAGE dbms_output;

/*
 * Support Operator on pls_integer Type
 */

-- a = b
CREATE FUNCTION plsint_eq(pls_integer,pls_integer)
RETURNS bool
AS 'int4eq'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_eq(pls_integer,int4)
RETURNS bool
AS 'int4eq'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_eq(int4,pls_integer)
RETURNS bool
AS 'int4eq'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR = (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_eq);

CREATE OPERATOR = (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_eq);

CREATE OPERATOR = (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_eq);

-- a <> b
CREATE FUNCTION plsint_ne(pls_integer,pls_integer)
RETURNS bool
AS 'int4ne'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_ne(pls_integer,int4)
RETURNS bool
AS 'int4ne'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_ne(int4,pls_integer)
RETURNS bool
AS 'int4ne'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR <> (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_ne);

CREATE OPERATOR <> (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_ne);

CREATE OPERATOR <> (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_ne);

-- a < b
CREATE FUNCTION plsint_lt(pls_integer,pls_integer)
RETURNS bool
AS 'int4lt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_lt(pls_integer,int4)
RETURNS bool
AS 'int4lt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_lt(int4,pls_integer)
RETURNS bool
AS 'int4lt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR < (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_lt);

CREATE OPERATOR < (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_lt);

CREATE OPERATOR < (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_lt);

-- a <= b
CREATE FUNCTION plsint_le(pls_integer,pls_integer)
RETURNS bool
AS 'int4le'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_le(pls_integer,int4)
RETURNS bool
AS 'int4le'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_le(int4,pls_integer)
RETURNS bool
AS 'int4le'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR <= (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_le);

CREATE OPERATOR <= (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_le);

CREATE OPERATOR <= (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_le);

-- a > b
CREATE FUNCTION plsint_gt(pls_integer,pls_integer)
RETURNS bool
AS 'int4gt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_gt(pls_integer,int4)
RETURNS bool
AS 'int4gt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_gt(int4,pls_integer)
RETURNS bool
AS 'int4gt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR > (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_gt);

CREATE OPERATOR > (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_gt);

CREATE OPERATOR > (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_gt);

-- a >= b
CREATE FUNCTION plsint_ge(pls_integer,pls_integer)
RETURNS bool
AS 'int4ge'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_ge(pls_integer,int4)
RETURNS bool
AS 'int4ge'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_ge(int4,pls_integer)
RETURNS bool
AS 'int4ge'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR >= (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_ge);

CREATE OPERATOR >= (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_ge);

CREATE OPERATOR >= (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_ge);

-- +a
CREATE FUNCTION plsint_up(pls_integer)
RETURNS pls_integer
AS 'int4up'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR + (
	rightarg = pls_integer,
	function = plsint_up);

-- a + b
CREATE FUNCTION plsint_pl(pls_integer,pls_integer)
RETURNS pls_integer
AS 'int4pl'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_pl(pls_integer,int4)
RETURNS pls_integer
AS 'int4pl'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_pl(int4,pls_integer)
RETURNS pls_integer
AS 'int4pl'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR + (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_pl);

CREATE OPERATOR + (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_pl);

CREATE OPERATOR + (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_pl);

-- a - b
CREATE FUNCTION plsint_mi(pls_integer,pls_integer)
RETURNS pls_integer
AS 'int4mi'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_mi(pls_integer,int4)
RETURNS pls_integer
AS 'int4mi'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_mi(int4,pls_integer)
RETURNS pls_integer
AS 'int4mi'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR - (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_mi);

CREATE OPERATOR - (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_mi);

CREATE OPERATOR - (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_mi);

-- a * b
CREATE FUNCTION plsint_mul(pls_integer,pls_integer)
RETURNS pls_integer
AS 'int4mul'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_mul(pls_integer,int4)
RETURNS pls_integer
AS 'int4mul'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_mul(int4,pls_integer)
RETURNS pls_integer
AS 'int4mul'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR * (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_mul);

CREATE OPERATOR * (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_mul);

CREATE OPERATOR * (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_mul);

-- a / b
CREATE FUNCTION plsint_div(pls_integer,pls_integer)
RETURNS pls_integer
AS 'int4div'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION plsint_int4_div(pls_integer,int4)
RETURNS pls_integer
AS 'int4div'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_plsint_div(int4,pls_integer)
RETURNS pls_integer
AS 'int4div'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR / (
   leftarg = pls_integer,
	rightarg = pls_integer,
	function = plsint_div);

CREATE OPERATOR / (
   leftarg = pls_integer,
	rightarg = int4,
	function = plsint_int4_div);

CREATE OPERATOR / (
   leftarg = int4,
	rightarg = pls_integer,
	function = int4_plsint_div);

/*
 * Support Operator on simple_integer Type
 */

-- a = b
CREATE FUNCTION simpint_eq(simple_integer,simple_integer)
RETURNS bool
AS 'int4eq'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int4_eq(simple_integer,int4)
RETURNS bool
AS 'int4eq'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_eq(int4,simple_integer)
RETURNS bool
AS 'int4eq'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_eq(pls_integer,simple_integer)
RETURNS bool
AS 'int4eq'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_eq(simple_integer,pls_integer)
RETURNS bool
AS 'int4eq'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR = (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_eq);

CREATE OPERATOR = (
   leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_eq);

CREATE OPERATOR = (
   leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_eq);

CREATE OPERATOR = (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_eq);

CREATE OPERATOR = (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_eq);

-- a <> b
CREATE FUNCTION simpint_ne(simple_integer,simple_integer)
RETURNS bool
AS 'int4ne'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int4_ne(simple_integer,int4)
RETURNS bool
AS 'int4ne'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_ne(int4,simple_integer)
RETURNS bool
AS 'int4ne'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_ne(pls_integer,simple_integer)
RETURNS bool
AS 'int4ne'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_ne(simple_integer,pls_integer)
RETURNS bool
AS 'int4ne'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR <> (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_ne);

CREATE OPERATOR <> (
   leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_ne);

CREATE OPERATOR <> (
   leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_ne);

CREATE OPERATOR <> (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_ne);

CREATE OPERATOR <> (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_ne);

-- a < b
CREATE FUNCTION simpint_lt(simple_integer,simple_integer)
RETURNS bool
AS 'int4lt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int4_lt(simple_integer,int4)
RETURNS bool
AS 'int4lt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_lt(int4,simple_integer)
RETURNS bool
AS 'int4lt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_lt(pls_integer,simple_integer)
RETURNS bool
AS 'int4lt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_lt(simple_integer,pls_integer)
RETURNS bool
AS 'int4lt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR < (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_lt);

CREATE OPERATOR < (
   leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_lt);

CREATE OPERATOR < (
   leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_lt);

CREATE OPERATOR < (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_lt);

CREATE OPERATOR < (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_lt);

-- a <= b
CREATE FUNCTION simpint_le(simple_integer,simple_integer)
RETURNS bool
AS 'int4le'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int4_le(simple_integer,int4)
RETURNS bool
AS 'int4le'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_le(int4,simple_integer)
RETURNS bool
AS 'int4le'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_le(pls_integer,simple_integer)
RETURNS bool
AS 'int4le'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_le(simple_integer,pls_integer)
RETURNS bool
AS 'int4le'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR <= (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_le);

CREATE OPERATOR <= (
   leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_le);

CREATE OPERATOR <= (
   leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_le);

CREATE OPERATOR <= (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_le);

CREATE OPERATOR <= (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_le);

-- a > b
CREATE FUNCTION simpint_gt(simple_integer,simple_integer)
RETURNS bool
AS 'int4gt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int4_gt(simple_integer,int4)
RETURNS bool
AS 'int4gt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_gt(int4,simple_integer)
RETURNS bool
AS 'int4gt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_gt(pls_integer,simple_integer)
RETURNS bool
AS 'int4gt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_gt(simple_integer,pls_integer)
RETURNS bool
AS 'int4gt'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR > (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_gt);

CREATE OPERATOR > (
   leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_gt);

CREATE OPERATOR > (
   leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_gt);

CREATE OPERATOR > (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_gt);

CREATE OPERATOR > (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_gt);

-- a >= b
CREATE FUNCTION simpint_ge(simple_integer,simple_integer)
RETURNS bool
AS 'int4ge'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simpint_int4_ge(simple_integer,int4)
RETURNS bool
AS 'int4ge'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION int4_simpint_ge(int4,simple_integer)
RETURNS bool
AS 'int4ge'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_ge(pls_integer,simple_integer)
RETURNS bool
AS 'int4ge'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_ge(simple_integer,pls_integer)
RETURNS bool
AS 'int4ge'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR >= (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_ge);

CREATE OPERATOR >= (
   leftarg = simple_integer,
	rightarg = int4,
	function = simpint_int4_ge);

CREATE OPERATOR >= (
   leftarg = int4,
	rightarg = simple_integer,
	function = int4_simpint_ge);

CREATE OPERATOR >= (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_ge);

CREATE OPERATOR >= (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_ge);

-- +a
CREATE FUNCTION simpint_up(simple_integer)
RETURNS simple_integer
AS 'int4up'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR + (
	rightarg = simple_integer,
	function = simpint_up);

-- a + b
CREATE FUNCTION simpint_pl(simple_integer,simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_pl'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_pl(pls_integer,simple_integer)
RETURNS pls_integer
AS 'int4pl'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_pl(simple_integer,pls_integer)
RETURNS pls_integer
AS 'int4pl'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR + (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_pl);

CREATE OPERATOR + (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_pl);

CREATE OPERATOR + (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_pl);

-- a - b
CREATE FUNCTION simpint_mi(simple_integer,simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_mi'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_mi(pls_integer,simple_integer)
RETURNS pls_integer
AS 'int4mi'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_mi(simple_integer,pls_integer)
RETURNS pls_integer
AS 'int4mi'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR - (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_mi);

CREATE OPERATOR - (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_mi);

CREATE OPERATOR - (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_mi);

-- a * b
CREATE FUNCTION simpint_mul(simple_integer,simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_ml'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_mul(pls_integer,simple_integer)
RETURNS pls_integer
AS 'int4mul'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_mul(simple_integer,pls_integer)
RETURNS pls_integer
AS 'int4mul'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR * (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_mul);

CREATE OPERATOR * (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_mul);

CREATE OPERATOR * (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_mul);

-- a / b
CREATE FUNCTION simpint_div(simple_integer,simple_integer)
RETURNS simple_integer
AS 'MODULE_PATHNAME','simpint_int4_div'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION pls_simpint_div(pls_integer,simple_integer)
RETURNS pls_integer
AS 'int4div'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_plsint_div(simple_integer,pls_integer)
RETURNS pls_integer
AS 'int4div'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR / (
   leftarg = simple_integer,
	rightarg = simple_integer,
	function = simpint_div);

CREATE OPERATOR / (
   leftarg = pls_integer,
	rightarg = simple_integer,
	function = pls_simpint_div);

CREATE OPERATOR / (
   leftarg = simple_integer,
	rightarg = pls_integer,
	function = simp_plsint_div);
/*
 * Map binary_integer to INT4.
 */
-- Support binary_integer
CREATE TYPE binary_integer;

CREATE FUNCTION binary_integer_in(cstring)
	RETURNS binary_integer
	AS 'int4in'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE FUNCTION binary_integer_out(binary_integer)
	RETURNS cstring
	AS 'int4out'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE FUNCTION binary_integer_recv(internal,oid,int4)
	RETURNS binary_integer
	AS 'int4recv'
	LANGUAGE internal STRICT IMMUTABLE;
CREATE FUNCTION binary_integer_send(binary_integer)
	RETURNS bytea
	AS 'int4send'
	LANGUAGE internal STRICT IMMUTABLE;

CREATE TYPE binary_integer (
	input = binary_integer_in,
	output = binary_integer_out,
	send = binary_integer_send,
	receive = binary_integer_recv,
   category = 'N',
	like = int4
);

CREATE CAST (binary_integer AS int) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (int AS binary_integer) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (binary_integer AS float8) WITH INOUT AS IMPLICIT; -- For usage of float operators
CREATE CAST (binary_integer AS numeric) WITH INOUT AS IMPLICIT; -- For usage of numeric operators

-- a + b
CREATE FUNCTION bin_simpint_pl(binary_integer,simple_integer)
RETURNS binary_integer
AS 'int4pl'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_binint_pl(simple_integer,binary_integer)
RETURNS binary_integer
AS 'int4pl'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR + (
   leftarg = binary_integer,
	rightarg = simple_integer,
	function = bin_simpint_pl);

CREATE OPERATOR + (
   leftarg = simple_integer,
	rightarg = binary_integer,
	function = simp_binint_pl);

-- a - b
CREATE FUNCTION bin_simpint_mi(binary_integer,simple_integer)
RETURNS binary_integer
AS 'int4mi'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_binint_mi(simple_integer,binary_integer)
RETURNS binary_integer
AS 'int4mi'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR - (
   leftarg = binary_integer,
	rightarg = simple_integer,
	function = bin_simpint_mi);

CREATE OPERATOR - (
   leftarg = simple_integer,
	rightarg = binary_integer,
	function = simp_binint_mi);

-- a * b
CREATE FUNCTION bin_simpint_mul(binary_integer,simple_integer)
RETURNS binary_integer
AS 'int4mul'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_binint_mul(simple_integer,binary_integer)
RETURNS binary_integer
AS 'int4mul'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR * (
   leftarg = binary_integer,
	rightarg = simple_integer,
	function = bin_simpint_mul);

CREATE OPERATOR * (
   leftarg = simple_integer,
	rightarg = binary_integer,
	function = simp_binint_mul);

-- a / b
CREATE FUNCTION bin_simpint_div(binary_integer,simple_integer)
RETURNS binary_integer
AS 'int4div'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION simp_binint_div(simple_integer,binary_integer)
RETURNS binary_integer
AS 'int4div'
LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR / (
   leftarg = binary_integer,
	rightarg = simple_integer,
	function = bin_simpint_div);

CREATE OPERATOR / (
   leftarg = simple_integer,
	rightarg = binary_integer,
	function = simp_binint_div);

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
    FUNCTION put(IN a simple_integer) RETURNS void;
    FUNCTION put(IN a binary_integer) RETURNS void;
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
    FUNCTION put_line(IN a simple_integer) RETURNS void;
    FUNCTION put_line(IN a binary_integer) RETURNS void;	
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

function put(IN a simple_integer) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_simple_int'
    LANGUAGE C VOLATILE STRICT;

function put(IN a binary_integer) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_binary_int'
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

function put_line(IN a simple_integer) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_simple_int'
    LANGUAGE C VOLATILE STRICT;

function put_line(IN a binary_integer) RETURNS void
   AS 'MODULE_PATHNAME','dbms_output_put_line_binary_int'
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
/*
 * utl_file: add type FILE_TYPE
 */
CREATE DOMAIN utl_file.FILE_TYPE AS INT;


-- define opentenbase_ora bitand function
-- bitand(numeric,numeric)
create or replace function opentenbase_ora.bitand(numeric,numeric)
returns numeric
language sql strict PARALLEL SAFE IMMUTABLE
as $$
select ($1::int8 & $2::int8)::numeric ;
$$;

-- bitand(text,numeric)
create or replace function opentenbase_ora.bitand(text,numeric)
returns numeric
language sql strict PARALLEL SAFE IMMUTABLE
as $$
select ($1::int8 & $2::int8)::numeric ;
$$;

-- bitand(numeric,text)
create or replace function opentenbase_ora.bitand(numeric,text)
returns numeric
language sql strict PARALLEL SAFE IMMUTABLE
as $$
select ($1::int8 & $2::int8)::numeric ;
$$;

-- bitand(text,text)
create or replace function opentenbase_ora.bitand(text,text)
returns numeric
language sql strict PARALLEL SAFE IMMUTABLE
as $$
select ($1::int8 & $2::int8)::numeric ;
$$;

--support OpenTenBase_Ora xxx_sequences
create or replace function _pg_seq_get_last_value(relname name) returns number AS
$$
declare
 plsql_block varchar2(256);
 res number := -1;
begin
 plsql_block := 'select last_value from ' || relname || ';';
 execute plsql_block into res;
 return res;
end;
$$language default_plsql;

CREATE OR REPLACE VIEW dba_sequences AS
SELECT cast(upper(pg_get_userbyid(relowner)) AS varchar2(128)) AS SEQUENCE_OWNER,
       cast(upper(relname) AS varchar2(128)) AS SEQUENCE_NAME,
       cast(seqmin AS number) AS MIN_VALUE,
       cast(seqmax AS number) AS MAX_VALUE,
       cast(seqincrement AS number) AS INCREMENT_BY,
       CASE WHEN seqcycle = true THEN cast('Y' as varchar2(1)) 
       ELSE cast('N' as varchar2(1)) 
       END AS CYCLE_FLAG,
       CASE WHEN seqoptions&1 = 1 THEN cast('Y' as varchar2(1))
       ELSE cast('N' as varchar2(1))
       END AS ORDER_FLAG,
       cast(seqcache AS number) AS CACHE_SIZE,
       CASE WHEN pg_has_role(m.relowner, 'USAGE')
       OR has_sequence_privilege(m.oid, 'SELECT, UPDATE, USAGE')
       OR has_any_column_privilege(m.oid, 'SELECT, INSERT, UPDATE, REFERENCES') THEN
            cast(_pg_seq_get_last_value(relname) AS number)
       ELSE
            cast(0 AS number)
       END AS LAST_NUMBER
       from pg_catalog.pg_ora_sequence s
       INNER JOIN
       (SELECT oid, relname, relowner from pg_catalog.pg_class) m
       ON s.seqrelid = m.oid;

CREATE OR REPLACE VIEW all_sequences AS
SELECT cast(upper(pg_get_userbyid(relowner)) AS varchar2(128)) AS SEQUENCE_OWNER,
       cast(upper(relname) AS varchar2(128)) AS SEQUENCE_NAME,
       cast(seqmin AS number) AS MIN_VALUE,
       cast(seqmax AS number) AS MAX_VALUE,
       cast(seqincrement AS number) AS INCREMENT_BY,
       CASE WHEN seqcycle = true THEN cast('Y' as varchar2(1))
       ELSE cast('N' as varchar2(1))
       END AS CYCLE_FLAG,
       CASE WHEN seqoptions&1 = 1 THEN cast('Y' as varchar2(1))
       ELSE cast('N' as varchar2(1))
       END AS ORDER_FLAG,
       cast(seqcache AS number) AS CACHE_SIZE,
       cast(_pg_seq_get_last_value(relname) AS number) AS LAST_NUMBER
       from pg_catalog.pg_ora_sequence s
       INNER JOIN
       (SELECT oid, relname, relowner from pg_catalog.pg_class) m
       ON s.seqrelid = m.oid
       where pg_has_role(m.relowner, 'USAGE')
             OR has_sequence_privilege(m.oid, 'SELECT, UPDATE, USAGE')
             OR has_any_column_privilege(m.oid, 'SELECT, INSERT, UPDATE, REFERENCES');

CREATE OR REPLACE VIEW user_sequences AS
SELECT cast(upper(relname) AS varchar2(128)) AS SEQUENCE_NAME,
       cast(seqmin AS number) AS MIN_VALUE,
       cast(seqmax AS number) AS MAX_VALUE,
       cast(seqincrement AS number) AS INCREMENT_BY,
       CASE WHEN seqcycle = true THEN cast('Y' as varchar2(1))
       ELSE cast('N' as varchar2(1))
       END AS CYCLE_FLAG,
       CASE WHEN seqoptions&1 = 1 THEN cast('Y' as varchar2(1))
       ELSE cast('N' as varchar2(1))
       END AS ORDER_FLAG,
       cast(seqcache AS number) AS CACHE_SIZE,
       cast(_pg_seq_get_last_value(relname) AS number) AS LAST_NUMBER
       from pg_catalog.pg_ora_sequence s
       INNER JOIN
       (SELECT oid, relname, relowner from pg_catalog.pg_class) m
       ON s.seqrelid = m.oid
       where pg_get_userbyid(m.relowner) = current_user;

GRANT SELECT ON dba_sequences TO PUBLIC;
GRANT SELECT ON all_sequences TO PUBLIC;
GRANT SELECT ON user_sequences TO PUBLIC;

--support OpenTenBase_Ora xxx_views
CREATE OR REPLACE VIEW dba_views AS
SELECT cast(upper(viewowner) AS varchar2(128)) AS OWNER,
       cast(upper(viewname) AS varchar2(128)) AS VIEW_NAME,
       cast(char_length(definition) AS number) AS TEXT_LENGTH,
       cast(definition AS long) AS TEXT,
       cast(definition AS varchar2(4000)) AS TEXT_VC,
       cast(0 AS number) AS TYPE_TEXT_LENGTH,
       cast(NULL AS varchar2(4000)) AS TYPE_TEXT,
       cast(0 AS number) AS OID_TEXT_LENGTH,
       cast(NULL AS varchar2(4000)) AS OID_TEXT,
       cast(NULL AS varchar2(128)) AS VIEW_TYPE_OWNER,
       cast(NULL AS varchar2(128)) AS VIEW_TYPE,
       cast(NULL AS varchar2(128)) AS SUPERVIEW_NAME,
       cast('N' AS varchar2(1)) AS EIDTIONING_VIEW,
       cast('N' AS varchar2(1)) AS READ_ONLY
FROM pg_catalog.pg_views;

CREATE OR REPLACE VIEW all_views AS
SELECT cast(upper(viewowner) AS varchar2(128)) AS OWNER,
       cast(upper(viewname) AS varchar2(128)) AS VIEW_NAME,
       cast(char_length(definition) AS number) AS TEXT_LENGTH,
       cast(definition AS long) AS TEXT,
       cast(definition AS varchar2(4000)) AS TEXT_VC,
       cast(0 AS number) AS TYPE_TEXT_LENGTH,
       cast(NULL AS varchar2(4000)) AS TYPE_TEXT,
       cast(0 AS number) AS OID_TEXT_LENGTH,
       cast(NULL AS varchar2(4000)) AS OID_TEXT,
       cast(NULL AS varchar2(128)) AS VIEW_TYPE_OWNER,
       cast(NULL AS varchar2(128)) AS VIEW_TYPE,
       cast(NULL AS varchar2(128)) AS SUPERVIEW_NAME,
       cast('N' AS varchar2(1)) AS EIDTIONING_VIEW,
       cast('N' AS varchar2(1)) AS READ_ONLY
       FROM 
       pg_catalog.pg_views v
       INNER JOIN pg_catalog.pg_class c
       on v.viewname = c.relname
       and c.relnamespace = (SELECT oid from pg_catalog.pg_namespace
                            where nspname = v.schemaname)
       where pg_has_role(c.relowner, 'USAGE')
             OR has_schema_privilege(c.oid, 'CREATE, USAGE')
             OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES');

CREATE OR REPLACE VIEW user_views AS
SELECT cast(upper(viewowner) AS varchar2(128)) AS OWNER,
       cast(upper(viewname) AS varchar2(128)) AS VIEW_NAME,
       cast(char_length(definition) AS number) AS TEXT_LENGTH,
       cast(definition AS long) AS TEXT,
       cast(definition AS varchar2(4000)) AS TEXT_VC,
       cast(0 AS number) AS TYPE_TEXT_LENGTH,
       cast(NULL AS varchar2(4000)) AS TYPE_TEXT,
       cast(0 AS number) AS OID_TEXT_LENGTH,
       cast(NULL AS varchar2(4000)) AS OID_TEXT,
       cast(NULL AS varchar2(128)) AS VIEW_TYPE_OWNER,
       cast(NULL AS varchar2(128)) AS VIEW_TYPE,
       cast(NULL AS varchar2(128)) AS SUPERVIEW_NAME,
       cast('N' AS varchar2(1)) AS EIDTIONING_VIEW,
       cast('N' AS varchar2(1)) AS READ_ONLY
       FROM pg_catalog.pg_views v
       INNER JOIN pg_catalog.pg_class c
       on v.viewname = c.relname
       and c.relnamespace = (SELECT oid from pg_catalog.pg_namespace
                            where nspname = v.schemaname)
       where pg_get_userbyid(c.relowner) = current_user;

GRANT SELECT ON dba_views TO PUBLIC;
GRANT SELECT ON all_views TO PUBLIC;
GRANT SELECT ON user_views TO PUBLIC;

CREATE OR REPLACE VIEW all_tables AS
 select cast(OWNER as varchar2(128)) AS OWNER,
		SCHEMA_NAME,
	    cast(TABLE_NAME as varchar2(128)) AS TABLE_NAME,
		cast(TABLESPACE_NAME as varchar2(30)) AS TABLESPACE_NAME,
		cast(CLUSTER_NAME as varchar2(128)) AS CLUSTER_NAME,
		cast(IOT_NAME as varchar2(128)) AS IOT_NAME,
		cast(STATUS as varchar2(8)) AS STATUS,
		cast(PCT_FREE as number) AS PCT_FREE,
		cast(PCT_USED as number) AS PCT_USED,
		cast(INI_TRANS as number) AS INI_TRANS,
		cast(MAX_TRANS as number) AS MAX_TRANS,
		cast(INITIAL_EXTENT as number) AS INITIAL_EXTENT,
		cast(NEXT_EXTENT as number) AS NEXT_EXTENT,
		cast(MIN_EXTENTS as number) AS MIN_EXTENTS,
		cast(MAX_EXTENTS as number) AS MAX_EXTENTS,
		cast(PCT_INCREASE as number) AS PCT_INCREASE,
		cast(FREELISTS as number) AS FREELISTS,
		cast(FREELIST_GROUPS as number) AS FREELIST_GROUPS,
		cast(LOGGING as varchar2(3)) AS LOGGING,
		cast(BACKED_UP as varchar2(1)) AS BACKED_UP,
		cast(NUM_ROWS as number) AS NUM_ROWS,
		cast(BLOCKS as number) AS BLOCKS,
		cast(EMPTY_BLOCKS as number) AS EMPTY_BLOCKS,
		cast(AVG_SPACE as number) AS AVG_SPACE,
		cast(CHAIN_CNT as number) AS CHAIN_CNT,
		cast(AVG_ROW_LEN as number) AS AVG_ROW_LEN,
		cast(AVG_SPACE_FREELIST_BLOCKS as number) AS AVG_SPACE_FREELIST_BLOCKS,
		cast(NUM_FREELIST_BLOCKS as number) AS NUM_FREELIST_BLOCKS,
		cast(DEGREE as varchar2(10)) AS DEGREE,
		cast(INSTANCES as varchar2(10)) AS INSTANCES,
		cast(CACHE as varchar2(5)) AS CACHE,
		cast(TABLE_LOCK as varchar2(8)) AS TABLE_LOCK,
		cast(SAMPLE_SIZE as number) AS SAMPLE_SIZE,
		cast(LAST_ANALYZED as pg_catalog.date) AS LAST_ANALYZED,
		cast(PARTITIONED as varchar2(3)) AS PARTITIONED,
		cast(IOT_TYPE as varchar2(12)) AS IOT_TYPE,
		cast(TEMPORARY as varchar2(1)) AS TEMPORARY,
		cast(SECONDARY as varchar2(1)) AS SECONDARY,
		cast(NESTED as varchar2(3)) AS NESTED,
		cast(BUFFER_POOL as varchar2(7)) AS BUFFER_POOL,
		cast(FLASH_CACHE as varchar2(7)) AS FLASH_CACHE,
		cast(CELL_FLASH_CACHE as varchar2(7)) AS CELL_FLASH_CACHE,
		cast(ROW_MOVEMENT as varchar2(8)) AS ROW_MOVEMENT,
		cast(GLOBAL_STATS as varchar2(3)) AS GLOBAL_STATS,
		cast(USER_STATS as varchar2(3)) AS USER_STATS,
		cast(DURATION as varchar2(15)) AS DURATION,
		cast(SKIP_CORRUPT as varchar2(8)) AS SKIP_CORRUPT,
		cast(MONITORING as varchar2(3)) AS MONITORING,
		cast(CLUSTER_OWNER as varchar2(128)) AS CLUSTER_OWNER,
		cast(DEPENDENCIES as varchar2(8)) AS DEPENDENCIES,
		cast(COMPRESSION as varchar2(8)) AS COMPRESSION,
		cast(COMPRESS_FOR as varchar2(30)) AS COMPRESS_FOR,
		cast(DROPPED as varchar2(3)) AS DROPPED,
		cast(READ_ONLY as varchar2(3)) AS READ_ONLY,
		cast(SEGMENT_CREATED as varchar2(3)) AS SEGMENT_CREATED,
		cast(RESULT_CACHE as varchar2(7)) AS RESULT_CACHE
	from ( select (SELECT pg_authid.rolname
           FROM pg_authid
          WHERE pg_authid.oid = c.relowner) AS OWNER, n.nspname as SCHEMA_NAME, c.relname as TABLE_NAME, t.spcname as TABLESPACE_NAME,
			NULL as CLUSTER_NAME, NULL as IOT_NAME, 'VALID' as STATUS, NULL as PCT_FREE, NULL as PCT_USED, 
			NULL as INI_TRANS, NULL as MAX_TRANS, NULL as INITIAL_EXTENT, NULL as NEXT_EXTENT, NULL as MIN_EXTENTS, 
			NULL as MAX_EXTENTS, NULL as PCT_INCREASE, NULL as FREELISTS, NULL as FREELIST_GROUPS,
			case when c.relpersistence = 'p' then 'YES' else 'NO' end as LOGGING, NULL as BACKED_UP,
			c.reltuples as NUM_ROWS, c.relpages as BLOCKS,
			NULL as EMPTY_BLOCKS, NULL as AVG_SPACE, NULL as CHAIN_CNT, NULL as AVG_ROW_LEN, NULL as AVG_SPACE_FREELIST_BLOCKS, 
			NULL as NUM_FREELIST_BLOCKS, NULL as DEGREE, NULL as INSTANCES, NULL as CACHE, NULL as TABLE_LOCK, NULL as SAMPLE_SIZE,
			s.last_analyze as LAST_ANALYZED, 
			case when c.relkind = 'p' then 'YES' else 'NO' end as PARTITIONED, NULL as IOT_TYPE, 
			case when c.relpersistence in ('t','p') then 'Y' else 'N' end as TEMPORARY, NULL as SECONDARY, NULL as NESTED,
			NULL as BUFFER_POOL, NULL as FLASH_CACHE, NULL as CELL_FLASH_CACHE, NULL as ROW_MOVEMENT, NULL as GLOBAL_STATS,
			NULL as USER_STATS, NULL as DURATION, NULL as SKIP_CORRUPT, NULL as MONITORING, NULL as CLUSTER_OWNER,
			NULL as DEPENDENCIES, NULL as COMPRESSION, NULL as COMPRESS_FOR, NULL as DROPPED, NULL as READ_ONLY,
			NULL as SEGMENT_CREATED, NULL as RESULT_CACHE
			from pg_class c left join pg_namespace n on n.oid = c.relnamespace
							left join pg_tablespace t on t.oid = c.reltablespace
							left join pg_stat_all_tables s on s.relid = c.oid
			where (NOT pg_is_other_temp_schema(n.oid)) AND c.relkind IN ('r', 'f', 'p') 
			AND (pg_has_role(c.relowner, 'USAGE') OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
				OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES'))
		  );
GRANT SELECT ON all_tables TO PUBLIC;

CREATE OR REPLACE VIEW dba_tables AS
 select cast(OWNER as varchar2(128)) AS OWNER,
		SCHEMA_NAME,
	    cast(TABLE_NAME as varchar2(128)) AS TABLE_NAME,
		cast(TABLESPACE_NAME as varchar2(30)) AS TABLESPACE_NAME,
		cast(CLUSTER_NAME as varchar2(128)) AS CLUSTER_NAME,
		cast(IOT_NAME as varchar2(128)) AS IOT_NAME,
		cast(STATUS as varchar2(8)) AS STATUS,
		cast(PCT_FREE as number) AS PCT_FREE,
		cast(PCT_USED as number) AS PCT_USED,
		cast(INI_TRANS as number) AS INI_TRANS,
		cast(MAX_TRANS as number) AS MAX_TRANS,
		cast(INITIAL_EXTENT as number) AS INITIAL_EXTENT,
		cast(NEXT_EXTENT as number) AS NEXT_EXTENT,
		cast(MIN_EXTENTS as number) AS MIN_EXTENTS,
		cast(MAX_EXTENTS as number) AS MAX_EXTENTS,
		cast(PCT_INCREASE as number) AS PCT_INCREASE,
		cast(FREELISTS as number) AS FREELISTS,
		cast(FREELIST_GROUPS as number) AS FREELIST_GROUPS,
		cast(LOGGING as varchar2(3)) AS LOGGING,
		cast(BACKED_UP as varchar2(1)) AS BACKED_UP,
		cast(NUM_ROWS as number) AS NUM_ROWS,
		cast(BLOCKS as number) AS BLOCKS,
		cast(EMPTY_BLOCKS as number) AS EMPTY_BLOCKS,
		cast(AVG_SPACE as number) AS AVG_SPACE,
		cast(CHAIN_CNT as number) AS CHAIN_CNT,
		cast(AVG_ROW_LEN as number) AS AVG_ROW_LEN,
		cast(AVG_SPACE_FREELIST_BLOCKS as number) AS AVG_SPACE_FREELIST_BLOCKS,
		cast(NUM_FREELIST_BLOCKS as number) AS NUM_FREELIST_BLOCKS,
		cast(DEGREE as varchar2(10)) AS DEGREE,
		cast(INSTANCES as varchar2(10)) AS INSTANCES,
		cast(CACHE as varchar2(5)) AS CACHE,
		cast(TABLE_LOCK as varchar2(8)) AS TABLE_LOCK,
		cast(SAMPLE_SIZE as number) AS SAMPLE_SIZE,
		cast(LAST_ANALYZED as pg_catalog.date) AS LAST_ANALYZED,
		cast(PARTITIONED as varchar2(3)) AS PARTITIONED,
		cast(IOT_TYPE as varchar2(12)) AS IOT_TYPE,
		cast(TEMPORARY as varchar2(1)) AS TEMPORARY,
		cast(SECONDARY as varchar2(1)) AS SECONDARY,
		cast(NESTED as varchar2(3)) AS NESTED,
		cast(BUFFER_POOL as varchar2(7)) AS BUFFER_POOL,
		cast(FLASH_CACHE as varchar2(7)) AS FLASH_CACHE,
		cast(CELL_FLASH_CACHE as varchar2(7)) AS CELL_FLASH_CACHE,
		cast(ROW_MOVEMENT as varchar2(8)) AS ROW_MOVEMENT,
		cast(GLOBAL_STATS as varchar2(3)) AS GLOBAL_STATS,
		cast(USER_STATS as varchar2(3)) AS USER_STATS,
		cast(DURATION as varchar2(15)) AS DURATION,
		cast(SKIP_CORRUPT as varchar2(8)) AS SKIP_CORRUPT,
		cast(MONITORING as varchar2(3)) AS MONITORING,
		cast(CLUSTER_OWNER as varchar2(128)) AS CLUSTER_OWNER,
		cast(DEPENDENCIES as varchar2(8)) AS DEPENDENCIES,
		cast(COMPRESSION as varchar2(8)) AS COMPRESSION,
		cast(COMPRESS_FOR as varchar2(30)) AS COMPRESS_FOR,
		cast(DROPPED as varchar2(3)) AS DROPPED,
		cast(READ_ONLY as varchar2(3)) AS READ_ONLY,
		cast(SEGMENT_CREATED as varchar2(3)) AS SEGMENT_CREATED,
		cast(RESULT_CACHE as varchar2(7)) AS RESULT_CACHE
	from ( select (SELECT pg_authid.rolname
           FROM pg_authid
          WHERE pg_authid.oid = c.relowner) AS OWNER, n.nspname as SCHEMA_NAME, c.relname as TABLE_NAME, t.spcname as TABLESPACE_NAME,
			NULL as CLUSTER_NAME, NULL as IOT_NAME, 'VALID' as STATUS, NULL as PCT_FREE, NULL as PCT_USED, 
			NULL as INI_TRANS, NULL as MAX_TRANS, NULL as INITIAL_EXTENT, NULL as NEXT_EXTENT, NULL as MIN_EXTENTS, 
			NULL as MAX_EXTENTS, NULL as PCT_INCREASE, NULL as FREELISTS, NULL as FREELIST_GROUPS,
			case when c.relpersistence = 'p' then 'YES' else 'NO' end as LOGGING, NULL as BACKED_UP,
			c.reltuples as NUM_ROWS, c.relpages as BLOCKS,
			NULL as EMPTY_BLOCKS, NULL as AVG_SPACE, NULL as CHAIN_CNT, NULL as AVG_ROW_LEN, NULL as AVG_SPACE_FREELIST_BLOCKS, 
			NULL as NUM_FREELIST_BLOCKS, NULL as DEGREE, NULL as INSTANCES, NULL as CACHE, NULL as TABLE_LOCK, NULL as SAMPLE_SIZE,
			s.last_analyze as LAST_ANALYZED, 
			case when c.relkind = 'p' then 'YES' else 'NO' end as PARTITIONED, NULL as IOT_TYPE, 
			case when c.relpersistence in ('t','p') then 'Y' else 'N' end as TEMPORARY, NULL as SECONDARY, NULL as NESTED,
			NULL as BUFFER_POOL, NULL as FLASH_CACHE, NULL as CELL_FLASH_CACHE, NULL as ROW_MOVEMENT, NULL as GLOBAL_STATS,
			NULL as USER_STATS, NULL as DURATION, NULL as SKIP_CORRUPT, NULL as MONITORING, NULL as CLUSTER_OWNER,
			NULL as DEPENDENCIES, NULL as COMPRESSION, NULL as COMPRESS_FOR, NULL as DROPPED, NULL as READ_ONLY,
			NULL as SEGMENT_CREATED, NULL as RESULT_CACHE
			from pg_class c left join pg_namespace n on n.oid = c.relnamespace
							left join pg_tablespace t on t.oid = c.reltablespace
							left join pg_stat_all_tables s on s.relid = c.oid
			where (NOT pg_is_other_temp_schema(n.oid)) AND c.relkind IN ('r', 'f', 'p') 
		  );
GRANT SELECT ON dba_tables TO PUBLIC;

CREATE OR REPLACE VIEW user_tables AS
  select SCHEMA_NAME,
	    cast(TABLE_NAME as varchar2(128)) AS TABLE_NAME,
		cast(TABLESPACE_NAME as varchar2(30)) AS TABLESPACE_NAME,
		cast(CLUSTER_NAME as varchar2(128)) AS CLUSTER_NAME,
		cast(IOT_NAME as varchar2(128)) AS IOT_NAME,
		cast(STATUS as varchar2(8)) AS STATUS,
		cast(PCT_FREE as number) AS PCT_FREE,
		cast(PCT_USED as number) AS PCT_USED,
		cast(INI_TRANS as number) AS INI_TRANS,
		cast(MAX_TRANS as number) AS MAX_TRANS,
		cast(INITIAL_EXTENT as number) AS INITIAL_EXTENT,
		cast(NEXT_EXTENT as number) AS NEXT_EXTENT,
		cast(MIN_EXTENTS as number) AS MIN_EXTENTS,
		cast(MAX_EXTENTS as number) AS MAX_EXTENTS,
		cast(PCT_INCREASE as number) AS PCT_INCREASE,
		cast(FREELISTS as number) AS FREELISTS,
		cast(FREELIST_GROUPS as number) AS FREELIST_GROUPS,
		cast(LOGGING as varchar2(3)) AS LOGGING,
		cast(BACKED_UP as varchar2(1)) AS BACKED_UP,
		cast(NUM_ROWS as number) AS NUM_ROWS,
		cast(BLOCKS as number) AS BLOCKS,
		cast(EMPTY_BLOCKS as number) AS EMPTY_BLOCKS,
		cast(AVG_SPACE as number) AS AVG_SPACE,
		cast(CHAIN_CNT as number) AS CHAIN_CNT,
		cast(AVG_ROW_LEN as number) AS AVG_ROW_LEN,
		cast(AVG_SPACE_FREELIST_BLOCKS as number) AS AVG_SPACE_FREELIST_BLOCKS,
		cast(NUM_FREELIST_BLOCKS as number) AS NUM_FREELIST_BLOCKS,
		cast(DEGREE as varchar2(10)) AS DEGREE,
		cast(INSTANCES as varchar2(10)) AS INSTANCES,
		cast(CACHE as varchar2(5)) AS CACHE,
		cast(TABLE_LOCK as varchar2(8)) AS TABLE_LOCK,
		cast(SAMPLE_SIZE as number) AS SAMPLE_SIZE,
		cast(LAST_ANALYZED as pg_catalog.date) AS LAST_ANALYZED,
		cast(PARTITIONED as varchar2(3)) AS PARTITIONED,
		cast(IOT_TYPE as varchar2(12)) AS IOT_TYPE,
		cast(TEMPORARY as varchar2(1)) AS TEMPORARY,
		cast(SECONDARY as varchar2(1)) AS SECONDARY,
		cast(NESTED as varchar2(3)) AS NESTED,
		cast(BUFFER_POOL as varchar2(7)) AS BUFFER_POOL,
		cast(FLASH_CACHE as varchar2(7)) AS FLASH_CACHE,
		cast(CELL_FLASH_CACHE as varchar2(7)) AS CELL_FLASH_CACHE,
		cast(ROW_MOVEMENT as varchar2(8)) AS ROW_MOVEMENT,
		cast(GLOBAL_STATS as varchar2(3)) AS GLOBAL_STATS,
		cast(USER_STATS as varchar2(3)) AS USER_STATS,
		cast(DURATION as varchar2(15)) AS DURATION,
		cast(SKIP_CORRUPT as varchar2(8)) AS SKIP_CORRUPT,
		cast(MONITORING as varchar2(3)) AS MONITORING,
		cast(CLUSTER_OWNER as varchar2(128)) AS CLUSTER_OWNER,
		cast(DEPENDENCIES as varchar2(8)) AS DEPENDENCIES,
		cast(COMPRESSION as varchar2(8)) AS COMPRESSION,
		cast(COMPRESS_FOR as varchar2(30)) AS COMPRESS_FOR,
		cast(DROPPED as varchar2(3)) AS DROPPED,
		cast(READ_ONLY as varchar2(3)) AS READ_ONLY,
		cast(SEGMENT_CREATED as varchar2(3)) AS SEGMENT_CREATED,
		cast(RESULT_CACHE as varchar2(7)) AS RESULT_CACHE
	from (select n.nspname as SCHEMA_NAME, c.relname as TABLE_NAME, t.spcname as TABLESPACE_NAME,
			NULL as CLUSTER_NAME, NULL as IOT_NAME, 'VALID' as STATUS, NULL as PCT_FREE, NULL as PCT_USED, 
			NULL as INI_TRANS, NULL as MAX_TRANS, NULL as INITIAL_EXTENT, NULL as NEXT_EXTENT, NULL as MIN_EXTENTS, 
			NULL as MAX_EXTENTS, NULL as PCT_INCREASE, NULL as FREELISTS, NULL as FREELIST_GROUPS,
			case when c.relpersistence = 'p' then 'YES' else 'NO' end as LOGGING, NULL as BACKED_UP,
			c.reltuples as NUM_ROWS, c.relpages as BLOCKS,
			NULL as EMPTY_BLOCKS, NULL as AVG_SPACE, NULL as CHAIN_CNT, NULL as AVG_ROW_LEN, NULL as AVG_SPACE_FREELIST_BLOCKS, 
			NULL as NUM_FREELIST_BLOCKS, NULL as DEGREE, NULL as INSTANCES, NULL as CACHE, NULL as TABLE_LOCK, NULL as SAMPLE_SIZE,
			s.last_analyze as LAST_ANALYZED, 
			case when c.relkind = 'p' then 'YES' else 'NO' end as PARTITIONED, NULL as IOT_TYPE, 
			case when c.relpersistence in ('t', 'g') then 'Y' else 'N' end as TEMPORARY, NULL as SECONDARY, NULL as NESTED,
			NULL as BUFFER_POOL, NULL as FLASH_CACHE, NULL as CELL_FLASH_CACHE, NULL as ROW_MOVEMENT, NULL as GLOBAL_STATS,
			NULL as USER_STATS, NULL as DURATION, NULL as SKIP_CORRUPT, NULL as MONITORING, NULL as CLUSTER_OWNER,
			NULL as DEPENDENCIES, NULL as COMPRESSION, NULL as COMPRESS_FOR, NULL as DROPPED, NULL as READ_ONLY,
			NULL as SEGMENT_CREATED, NULL as RESULT_CACHE
			from pg_class c left join pg_namespace n on n.oid = c.relnamespace
							left join pg_tablespace t on t.oid = c.reltablespace
							left join pg_stat_all_tables s on s.relid = c.oid
			where (NOT pg_is_other_temp_schema(n.oid)) AND c.relkind IN ('r', 'f', 'p') 
				AND pg_get_userbyid(c.relowner) = current_user
		  );
GRANT SELECT ON user_tables TO PUBLIC;

CREATE OR REPLACE VIEW all_tab_comments AS
select cast(OWNER as varchar(128)) AS OWNER,
	   cast(SCHEMA_NAME as varchar(128)) AS SCHEMA_NAME,
	   cast(TABLE_NAME as varchar(128)) AS TABLE_NAME,
	   cast(TABLE_TYPE as varchar(11)) AS TABLE_TYPE,
	   cast(COMMENTS as varchar(4000)) AS COMMENTS
 from (select (SELECT pg_authid.rolname
           FROM pg_authid
          WHERE pg_authid.oid = c.relowner) AS OWNER, nc.nspname as SCHEMA_NAME, c.relname as TABLE_NAME, 
			case when c.relkind = 'v' THEN 'VIEW' 
				else 'TABLE' end as TABLE_TYPE,
			obj_description(c.oid, 'pg_class') as COMMENTS 
		from pg_class c left JOIN pg_namespace nc ON c.relnamespace = nc.oid
		where (NOT pg_is_other_temp_schema(nc.oid)) AND c.relkind IN ('r', 'v', 'f', 'p') 
				AND (pg_has_role(c.relowner, 'USAGE') OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
				OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES')));
GRANT SELECT ON all_tab_comments TO PUBLIC;	

CREATE OR REPLACE VIEW dba_tab_comments AS
select cast(OWNER as varchar(128)) AS OWNER,
	   cast(SCHEMA_NAME as varchar(128)) AS SCHEMA_NAME,
	   cast(TABLE_NAME as varchar(128)) AS TABLE_NAME,
	   cast(TABLE_TYPE as varchar(11)) AS TABLE_TYPE,
	   cast(COMMENTS as varchar(4000)) AS COMMENTS
 from (select (SELECT pg_authid.rolname
           FROM pg_authid
          WHERE pg_authid.oid = c.relowner) AS OWNER, nc.nspname as SCHEMA_NAME, c.relname as TABLE_NAME, 
			case when c.relkind = 'v' THEN 'VIEW' 
				else 'TABLE' end as TABLE_TYPE,
			obj_description(c.oid, 'pg_class') as COMMENTS 
		from pg_class c left JOIN pg_namespace nc ON c.relnamespace = nc.oid
		where (NOT pg_is_other_temp_schema(nc.oid)) AND c.relkind IN ('r', 'v', 'f', 'p'));
GRANT SELECT ON dba_tab_comments TO PUBLIC;	

CREATE OR REPLACE VIEW user_tab_comments AS
select cast(SCHEMA_NAME as varchar(128)) AS SCHEMA_NAME,
	   cast(TABLE_NAME as varchar(128)) AS TABLE_NAME,
	   cast(TABLE_TYPE as varchar(11)) AS TABLE_TYPE,
	   cast(COMMENTS as varchar(4000)) AS COMMENTS
 from ( select nc.nspname as SCHEMA_NAME, c.relname as TABLE_NAME, 
			case when c.relkind = 'v' THEN 'VIEW' 
				else 'TABLE' end as TABLE_TYPE,
			obj_description(c.oid, 'pg_class') as COMMENTS 
		from pg_class c left JOIN pg_namespace nc ON c.relnamespace = nc.oid
		where (NOT pg_is_other_temp_schema(nc.oid)) AND c.relkind IN ('r', 'v', 'f', 'p') 
				AND pg_get_userbyid(c.relowner) = current_user);
GRANT SELECT ON user_tab_comments TO PUBLIC;
