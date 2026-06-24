-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.23'" to load this file. \quit

CREATE FUNCTION opentenbase_ora.to_binary_float_ext(arg text, defExpr text) RETURNS binary_float
AS 'MODULE_PATHNAME','to_binary_float_def_on_conv_err'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.to_binary_float_ext(arg text, defExpr text, fmt text) RETURNS binary_float
AS 'MODULE_PATHNAME','to_binary_float_def_on_conv_err_with_fmt'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.to_binary_float_ext(arg numeric, defExpr text, fmt text) RETURNS binary_float
AS 'MODULE_PATHNAME','to_binary_float_def_on_conv_err_with_fmt_error'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.to_binary_float(arg text, fmt text, nls text) RETURNS binary_float
AS 'MODULE_PATHNAME','to_binary_float_with_fmt_nls'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION opentenbase_ora.to_binary_float_ext(arg text, defExpr text, fmt text, nls text) RETURNS binary_float
AS 'MODULE_PATHNAME','to_binary_float_def_on_conv_err_with_fmt_nls'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.to_binary_double_ext(arg text, defExpr text) RETURNS binary_double
AS 'MODULE_PATHNAME','to_binary_double_def_on_conv_err'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.to_binary_double_ext(arg text, defExpr text, fmt text) RETURNS binary_double
AS 'MODULE_PATHNAME','to_binary_double_def_on_conv_err_with_fmt'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.to_binary_double(arg text, fmt text, nls text) RETURNS binary_double
AS 'MODULE_PATHNAME','to_binary_double_with_fmt_nls'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE FUNCTION opentenbase_ora.to_binary_double_ext(arg text, defExpr text, fmt text, nls text) RETURNS binary_double
AS 'MODULE_PATHNAME','to_binary_double_def_on_conv_err_with_fmt_nls'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.chr(numeric) RETURNS varchar2
AS 'MODULE_PATHNAME','orcl_chr_1'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION opentenbase_ora.chr(numeric, varchar2) RETURNS nvarchar2
AS 'MODULE_PATHNAME','orcl_chr_2'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION opentenbase_ora.soundex(varchar2) RETURNS varchar2
AS 'MODULE_PATHNAME','orcl_soundex'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION opentenbase_ora.soundex(nvarchar2) RETURNS nvarchar2
AS 'MODULE_PATHNAME','orcl_soundex'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION opentenbase_ora.soundex(text) returns varchar2
as 'select opentenbase_ora.soundex($1::varchar2);'
LANGUAGE SQL IMMUTABLE STRICT;

CREATE FUNCTION opentenbase_ora.soundex(rowid) RETURNS varchar2
AS 'MODULE_PATHNAME','orcl_soundex'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION opentenbase_ora.soundex(long) returns varchar2
as 'select opentenbase_ora.soundex($1::varchar2);'
LANGUAGE SQL IMMUTABLE STRICT;

-- function to_char
CREATE FUNCTION opentenbase_ora.to_char(arg numeric, fmt text, nls text) RETURNS text
AS 'MODULE_PATHNAME','number_to_char_fmt_nls'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- function to_number
CREATE FUNCTION opentenbase_ora.to_number_ext(arg text, defExpr text) RETURNS numeric
AS 'MODULE_PATHNAME','numeric_to_number_def_on_conv_err'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.to_number_ext(arg text, defExpr text, fmt text) RETURNS numeric
AS 'MODULE_PATHNAME','numeric_to_number_def_on_conv_err_fmt'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.to_number_ext(arg text, defExpr text, fmt text, nls text) RETURNS numeric
AS 'MODULE_PATHNAME','numeric_to_number_def_on_conv_err_fmt_nls'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION opentenbase_ora.to_number(arg text, fmt text, nls text) RETURNS numeric
AS 'MODULE_PATHNAME','numeric_to_number_fmt_nls'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- function bin_to_num
CREATE OR REPLACE FUNCTION opentenbase_ora.bin_to_num(VARIADIC numeric []) RETURNS numeric
LANGUAGE default_plsql
AS
DECLARE
    i integer := 1;
    args numeric[] := $1;
    args_modify integer[] = '{}';
BEGIN
    WHILE i <= array_length(args, 1)
    LOOP
        IF args[i] is NULL THEN
            RAISE EXCEPTION 'illegal argument for function';
        END IF;
        args_modify := array_append(args_modify, cast(floor(args[i]) as integer));
        i := i + 1;
    END LOOP;
    RETURN int8(replace(array_to_string(args_modify, ','), ',')::varbit)::numeric;
END;
;

-- to_char(datetime)
CREATE FUNCTION opentenbase_ora.to_char(arg character, fmt text, nls text) RETURNS text
AS 'MODULE_PATHNAME','orcl_character_to_char_with_fmt_nls'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
