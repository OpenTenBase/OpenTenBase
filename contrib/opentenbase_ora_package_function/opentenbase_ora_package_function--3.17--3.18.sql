-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.18'" to load this file. \quit

--CREATE CAST (bytea AS long raw) WITHOUT FUNCTION AS IMPLICIT;

CREATE OR REPLACE VIEW all_arguments AS
SELECT
    cast(upper(OWNER) as varchar2(128)) AS OWNER,
    cast(upper(proname) as varchar2(128)) AS object_name,
    cast(pkgname as varchar2(128)) AS PACKAGE_NAME,
    cast(cast(poid as integer) as number) AS OBJECT_ID,
    cast(NULL as varchar2(40)) AS OVERLOAD,
    cast(subpid as number) AS SUBPROGRAM_ID,
    cast(upper(allargnames) as varchar2(128)) AS ARGUMENT_NAME,
    cast(CASE WHEN prokind = 'p' THEN
        id
    ELSE
        id - 1
    END as number) AS position,
    cast(id as number) AS sequence,
    cast(0 as number) AS data_level,
    cast(upper(allargtypes::regtype::text) as varchar2(30)) DATA_TYPE,
    cast(CASE WHEN pg_get_function_arg_default (poid, id) IS NULL THEN
        0
    ELSE
        1
    END as varchar2(1)) AS DEFAULTED,
    pg_get_function_arg_default (poid, id) AS DEFAULT_VALUE,
    cast(CASE WHEN allargmodes = 'i' THEN
        'IN'
    WHEN allargmodes = 'o' THEN
        'OUT'
    WHEN allargmodes = 'b' THEN
        'INOUT'
    WHEN allargmodes IS NULL THEN
        'IN'
    END as varchar2(9)) AS IN_OUT,
    cast(CASE WHEN y.typlen = - 1 THEN
        NULL
    ELSE
        y.typlen
    END as number) AS data_length,
    cast(NULL as number) AS data_precision,
    cast(NULL as number) AS data_scale,
    cast(CASE WHEN allargtypes IN (1700, 700, 701, 20, 21, 23) THEN
        10
    ELSE
        NULL
    END as number) AS radix,
    cast(NULL as varchar2(44)) AS CHARACTER_SET_NAME,
    cast(NULL as varchar2(128)) AS type_owner,
    cast(NULL as varchar2(128)) AS type_name,
    cast(NULL as varchar2(128)) AS type_subname,
    cast(NULL as varchar2(128)) AS type_link,
	cast(NULL as varchar2(7)) AS TYPE_OBJECT_TYPE,
    cast(upper(allargtypes::regtype::text) as varchar2(128)) AS pls_type,
    cast(NULL as number) AS char_length,
    cast('0' as varchar2(1)) AS char_used
FROM (
    SELECT
        f.oid AS poid,
        p.pkgname AS pkgname,
        pronamespace::regnamespace::text AS namespace,
        proname,
        proowner::regrole::text AS owner,
        array_position(p.pkgfunc, f.oid) AS subpid,
        prokind,
        unnest(
            CASE WHEN prokind = 'p' THEN
                proargtypes
            ELSE
                array_prepend(prorettype, cast(proargtypes AS oid[]))
            END) allargtypes,
        unnest(
            CASE WHEN prokind = 'p' THEN
                proargnames
            ELSE
                array_prepend(NULL, proargnames)
            END) allargnames,
        unnest(
            CASE WHEN prokind = 'p' THEN
                proargmodes
            ELSE
                array_prepend('o', proargmodes)
            END) allargmodes,
        generate_series(1, CASE WHEN prokind = 'p' THEN
                array_length(proargtypes, 1)
            ELSE
                array_length(proargtypes, 1) + 1
            END) AS id
    FROM
        pg_catalog.pg_proc f
    LEFT JOIN pg_catalog.pg_package p ON f.pronamespace = p.pkgcrrspndingns where pg_catalog.pg_function_is_visible(f.oid)) t
    JOIN pg_catalog.pg_type y ON t.allargtypes = y.oid;

GRANT SELECT ON all_arguments TO PUBLIC;

-- OpenTenBase_Ora compatible instrb() functions
CREATE FUNCTION instrb(str text, patt text)
RETURNS int
AS 'MODULE_PATHNAME','plvstr_instrb2'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION instrb(str text, patt text, "start" int, nth int)
RETURNS int
AS 'MODULE_PATHNAME','plvstr_instrb4'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION instrb(str text, patt text, "start" int)
RETURNS int
AS 'MODULE_PATHNAME','plvstr_instrb3'
LANGUAGE C IMMUTABLE STRICT;

-- regexp_count(numeric, numeric)
/*create or replace function opentenbase_ora.regexp_count(numeric, numeric) returns integer
   as 'select regexp_count($1::text, $2::text);'
   language sql immutable parallel safe strict cost 1;

create or replace function opentenbase_ora.regexp_count(numeric, numeric, integer) returns integer
   as 'select regexp_count($1::text, $2::text, $3);'
   language sql immutable parallel safe strict cost 1;

create or replace function opentenbase_ora.regexp_count(numeric, numeric, integer, text) returns integer
   as 'select regexp_count($1::text, $2::text, $3, $4);'
   language sql immutable parallel safe strict cost 1;

-- regexp_count(text, numeric)
create or replace function opentenbase_ora.regexp_count(text, numeric) returns integer
   as 'select regexp_count($1, $2::text);'
   language sql immutable parallel safe strict cost 1;

create or replace function opentenbase_ora.regexp_count(text, numeric, integer) returns integer
   as 'select regexp_count($1, $2::text, $3);'
   language sql immutable parallel safe strict cost 1;

create or replace function opentenbase_ora.regexp_count(text, numeric, integer, text) returns integer
   as 'select regexp_count($1, $2::text, $3, $4);'
   language sql immutable parallel safe strict cost 1;

-- regexp_count(numeric, text)
create or replace function opentenbase_ora.regexp_count(numeric, text) returns integer
   as 'select regexp_count($1::text, $2);'
   language sql immutable parallel safe strict cost 1;

create or replace function opentenbase_ora.regexp_count(numeric, text, integer) returns integer
   as 'select regexp_count($1::text, $2, $3);'
   language sql immutable parallel safe strict cost 1;

create or replace function opentenbase_ora.regexp_count(numeric, text, integer, text) returns integer
   as 'select regexp_count($1::text, $2, $3, $4);'
   language sql immutable parallel safe strict cost 1;*/

create or replace function sys_guid()
RETURNS raw
AS 'MODULE_PATHNAME','orafce_sys_guid'
LANGUAGE C VOLATILE PARALLEL SAFE STRICT;

-- chr(text)
create or replace function pg_catalog.chr(text) returns text
as 'select pg_catalog.chr($1::integer);'
language sql immutable parallel safe strict cost 1;

/*create or replace package dbms_date as
$$
    function date_eq(date1 opentenbase_ora.date, date2 opentenbase_ora.date) returns bool;
    function date_ne(date1 opentenbase_ora.date, date2 opentenbase_ora.date) returns bool;
    function date_lt(date1 opentenbase_ora.date, date2 opentenbase_ora.date) returns bool;
    function date_le(date1 opentenbase_ora.date, date2 opentenbase_ora.date) returns bool;
    function date_gt(date1 opentenbase_ora.date, date2 opentenbase_ora.date) returns bool;
    function date_ge(date1 opentenbase_ora.date, date2 opentenbase_ora.date) returns bool;
    function date_larger(date1 opentenbase_ora.date, date2 opentenbase_ora.date) returns opentenbase_ora.date;
    function date_smaller(date1 opentenbase_ora.date, date2 opentenbase_ora.date) returns opentenbase_ora.date;
    function date_cmp(date1 opentenbase_ora.date, date2 opentenbase_ora.date) returns INT;
    function date_hash(dt opentenbase_ora.date) returns INT;
$$;
    
create or replace package body dbms_date as
$pkg_dbms_date_body$
    function date_eq(date1 opentenbase_ora.date, date2 opentenbase_ora.date)
        returns bool
    as 'timestamp_eq'
        language internal STRICT IMMUTABLE;
    function date_ne(date1 opentenbase_ora.date, date2 opentenbase_ora.date)
        returns bool
    as 'timestamp_ne'
        language internal STRICT IMMUTABLE;
    function date_lt(date1 opentenbase_ora.date, date2 opentenbase_ora.date)
        returns bool
    as 'timestamp_lt'
        language internal STRICT IMMUTABLE;
    function date_le(date1 opentenbase_ora.date, date2 opentenbase_ora.date)
        returns bool
    as 'timestamp_le'
        language internal STRICT IMMUTABLE;
    function date_gt(date1 opentenbase_ora.date, date2 opentenbase_ora.date)
        returns bool
    as 'timestamp_gt'
        language internal STRICT IMMUTABLE;
    function date_ge(date1 opentenbase_ora.date, date2 opentenbase_ora.date)
        returns bool
    as 'timestamp_ge'
        language internal STRICT IMMUTABLE;
    function date_larger(date1 opentenbase_ora.date, date2 opentenbase_ora.date)
        returns opentenbase_ora.date
    as 'timestamp_larger'
        language internal STRICT IMMUTABLE;
    function date_smaller(date1 opentenbase_ora.date, date2 opentenbase_ora.date)
        returns opentenbase_ora.date
    as 'timestamp_smaller'
        language internal STRICT IMMUTABLE;
    function date_cmp(date1 opentenbase_ora.date, date2 opentenbase_ora.date)
        returns INT
    as 'timestamp_cmp'
        language internal STRICT IMMUTABLE;
    function date_hash(dt opentenbase_ora.date)
        returns INT
    as 'timestamp_hash'
        language internal STRICT IMMUTABLE;
$pkg_dbms_date_body$;

create OPERATOR = (
    leftarg = opentenbase_ora.date,
    rightarg = opentenbase_ora.date,
    commutator = =,
    negator = <>,
    function = dbms_date.date_eq,
    HASHES, MERGES);

create OPERATOR <> (
    leftarg = opentenbase_ora.date,
    rightarg = opentenbase_ora.date,
    commutator = <>,
    negator = =,
    function = dbms_date.date_ne);

create OPERATOR < (
    leftarg = opentenbase_ora.date,
    rightarg = opentenbase_ora.date,
    commutator = >,
    negator = >=,
    function = dbms_date.date_lt);

create OPERATOR > (
    leftarg = opentenbase_ora.date,
    rightarg = opentenbase_ora.date,
    commutator = <,
    negator = <=,
    function = dbms_date.date_gt);

create OPERATOR <= (
    leftarg = opentenbase_ora.date,
    rightarg = opentenbase_ora.date,
    commutator = >=,
    negator = >,
    function = dbms_date.date_le);

create OPERATOR >= (
    leftarg = opentenbase_ora.date,
    rightarg = opentenbase_ora.date,
    commutator = <=,
    negator = <,
    function = dbms_date.date_ge);

create OPERATOR FAMILY date_orcl_bt_ops using btree;
create OPERATOR CLASS date_orcl_ops
default for type opentenbase_ora.date using btree FAMILY date_orcl_bt_ops as
    OPERATOR 1 < ,
    OPERATOR 2 <= ,
    OPERATOR 3 = ,
    OPERATOR 4 >= ,
    OPERATOR 5 > ,
    function 1 dbms_date.date_cmp(date1 opentenbase_ora.date, date2 opentenbase_ora.date);

create OPERATOR FAMILY date_orcl_hash_ops using hash;
create OPERATOR CLASS date_orcl_ops
default for type opentenbase_ora.date using hash FAMILY date_orcl_hash_ops as
    OPERATOR 1 =,
    function 1 dbms_date.date_hash(dt opentenbase_ora.date);*/
