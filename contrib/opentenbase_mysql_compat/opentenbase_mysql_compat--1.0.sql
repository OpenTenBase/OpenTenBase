/* contrib/opentenbase_mysql_compat/opentenbase_mysql_compat--1.0.sql */

-- String functions
CREATE OR REPLACE FUNCTION IFNULL(anyelement, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'mysql_ifnull'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION CONCAT_WS(text, VARIADIC "any")
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_concat_ws'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION FIND_IN_SET(text, text)
RETURNS integer
AS 'MODULE_PATHNAME', 'mysql_find_in_set'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION ELT(integer, VARIADIC "any")
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_elt'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION FIELD(text, VARIADIC "any")
RETURNS integer
AS 'MODULE_PATHNAME', 'mysql_field'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION "INSERT"(text, integer, integer, text)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_insert_func'
LANGUAGE C IMMUTABLE STRICT;

-- Date/time functions
CREATE OR REPLACE FUNCTION DATE_FORMAT(timestamp, text)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_date_format'
LANGUAGE C STABLE STRICT;

CREATE OR REPLACE FUNCTION STR_TO_DATE(text, text)
RETURNS timestamp
AS 'MODULE_PATHNAME', 'mysql_str_to_date'
LANGUAGE C STABLE STRICT;

CREATE OR REPLACE FUNCTION DATEDIFF(date, date)
RETURNS integer
AS 'MODULE_PATHNAME', 'mysql_datediff'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION TIMESTAMPDIFF(text, timestamp, timestamp)
RETURNS bigint
AS 'MODULE_PATHNAME', 'mysql_timestampdiff'
LANGUAGE C STABLE STRICT;

CREATE OR REPLACE FUNCTION TIMESTAMPADD(text, integer, timestamp)
RETURNS timestamp
AS 'MODULE_PATHNAME', 'mysql_timestampadd'
LANGUAGE C STABLE STRICT;

CREATE OR REPLACE FUNCTION LAST_DAY(date)
RETURNS date
AS 'MODULE_PATHNAME', 'mysql_last_day'
LANGUAGE C IMMUTABLE STRICT;

-- Math functions
CREATE OR REPLACE FUNCTION TRUNCATE(numeric, integer)
RETURNS numeric
AS 'MODULE_PATHNAME', 'mysql_truncate_num'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION FORMAT(numeric, integer)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_format_num'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION FORMAT(numeric, integer, text)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_format_num'
LANGUAGE C IMMUTABLE STRICT;

-- Flow control functions
CREATE OR REPLACE FUNCTION "IF"(boolean, anyelement, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'mysql_if_func'
LANGUAGE C IMMUTABLE;

-- Aggregate functions
CREATE OR REPLACE FUNCTION group_concat_transfn(internal, text)
RETURNS internal
AS 'MODULE_PATHNAME', 'mysql_group_concat_transfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION group_concat_transfn(internal, text, text)
RETURNS internal
AS 'MODULE_PATHNAME', 'mysql_group_concat_transfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION group_concat_finalfn(internal)
RETURNS text
AS 'MODULE_PATHNAME', 'mysql_group_concat_finalfn'
LANGUAGE C IMMUTABLE;

CREATE AGGREGATE GROUP_CONCAT (text) (
    SFUNC = group_concat_transfn,
    STYPE = internal,
    FINALFUNC = group_concat_finalfn
);

CREATE AGGREGATE GROUP_CONCAT (text, text) (
    SFUNC = group_concat_transfn,
    STYPE = internal,
    FINALFUNC = group_concat_finalfn
);
