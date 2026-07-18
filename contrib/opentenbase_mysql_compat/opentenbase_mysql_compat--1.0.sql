/*-------------------------------------------------------------------------
 *
 * opentenbase_mysql_compat--1.0.sql
 *    MySQL-compatible functions intended to reduce migration rewrites.
 *
 * Copyright (c) 2026 OpenTenBase Contributors
 *
 * This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
 * in the repository root for details.
 *
 *-------------------------------------------------------------------------
 */

\echo Use "CREATE EXTENSION opentenbase_mysql_compat" to load this file. \quit

/*
 * The extension is deliberately implemented in SQL and PL/pgSQL.  Keeping
 * the compatibility layer outside the backend avoids parser changes and
 * lets administrators opt in per database.  Callers should qualify names
 * with mysql., or add mysql to search_path during a migration window.
 */

CREATE SCHEMA mysql;

/* -------------------------------------------------------------------------
 * Polymorphic flow-control functions
 * -------------------------------------------------------------------------
 */

CREATE FUNCTION mysql.ifnull(anyelement, anyelement)
RETURNS anyelement
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT COALESCE($1, $2)';

CREATE FUNCTION mysql.nullif_mysql(anyelement, anyelement)
RETURNS anyelement
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT NULLIF($1, $2)';

CREATE FUNCTION mysql.mysql_if(boolean, anyelement, anyelement)
RETURNS anyelement
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT CASE WHEN $1 THEN $2 ELSE $3 END';

CREATE FUNCTION mysql.if_true(boolean, anyelement, anyelement)
RETURNS anyelement
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT CASE WHEN COALESCE($1, false) THEN $2 ELSE $3 END';

/* -------------------------------------------------------------------------
 * String functions
 * -------------------------------------------------------------------------
 */

CREATE FUNCTION mysql.concat(VARIADIC items text[])
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    value text;
    result text := '';
BEGIN
    IF items IS NULL THEN
        RETURN NULL;
    END IF;

    FOREACH value IN ARRAY items LOOP
        IF value IS NULL THEN
            RETURN NULL;
        END IF;
        result := result || value;
    END LOOP;

    RETURN result;
END;
$$;

CREATE FUNCTION mysql.concat_ws(separator text, VARIADIC items text[])
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    value text;
    result text := '';
    have_value boolean := false;
BEGIN
    IF separator IS NULL THEN
        RETURN NULL;
    END IF;

    IF items IS NULL THEN
        RETURN '';
    END IF;

    FOREACH value IN ARRAY items LOOP
        IF value IS NULL THEN
            CONTINUE;
        END IF;
        IF have_value THEN
            result := result || separator;
        END IF;
        result := result || value;
        have_value := true;
    END LOOP;

    RETURN result;
END;
$$;

CREATE FUNCTION mysql.elt(item_index integer, VARIADIC items text[])
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN $1 IS NULL OR $2 IS NULL OR $1 < 1 OR $1 > cardinality($2)
            THEN NULL
        ELSE $2[$1]
    END
$$;

CREATE FUNCTION mysql.field(needle text, VARIADIC items text[])
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    index integer;
BEGIN
    IF needle IS NULL OR items IS NULL THEN
        RETURN 0;
    END IF;

    FOR index IN 1 .. cardinality(items) LOOP
        IF items[index] = needle THEN
            RETURN index;
        END IF;
    END LOOP;

    RETURN 0;
END;
$$;

CREATE FUNCTION mysql.find_in_set(needle text, comma_list text)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN $1 IS NULL OR $2 IS NULL THEN NULL
        WHEN strpos($1, ',') > 0 THEN 0
        ELSE COALESCE(array_position(string_to_array($2, ','), $1), 0)
    END
$$;

CREATE FUNCTION mysql.insert_string(source text, start_position integer,
                                    characters integer, replacement text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN $1 IS NULL OR $2 IS NULL OR $3 IS NULL OR $4 IS NULL THEN NULL
        WHEN $2 < 1 OR $2 > char_length($1) THEN $1
        WHEN $3 < 0 THEN $1
        ELSE overlay($1 placing $4 from $2 for $3)
    END
$$;

CREATE FUNCTION mysql.substring_index(source text, delimiter text,
                                      occurrence integer)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    parts text[];
    part_count integer;
BEGIN
    IF source IS NULL OR delimiter IS NULL OR occurrence IS NULL THEN
        RETURN NULL;
    END IF;
    IF delimiter = '' OR occurrence = 0 THEN
        RETURN '';
    END IF;

    parts := string_to_array(source, delimiter);
    part_count := cardinality(parts);

    IF abs(occurrence) >= part_count THEN
        RETURN source;
    ELSIF occurrence > 0 THEN
        RETURN array_to_string(parts[1:occurrence], delimiter);
    ELSE
        RETURN array_to_string(parts[(part_count + occurrence + 1):part_count],
                               delimiter);
    END IF;
END;
$$;

CREATE FUNCTION mysql.make_set(bits bigint, VARIADIC items text[])
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    index integer;
    selected text[] := ARRAY[]::text[];
BEGIN
    IF bits IS NULL OR items IS NULL THEN
        RETURN NULL;
    END IF;

    FOR index IN 1 .. LEAST(cardinality(items), 63) LOOP
        IF (bits & (1::bigint << (index - 1))) <> 0
           AND items[index] IS NOT NULL THEN
            selected := array_append(selected, items[index]);
        END IF;
    END LOOP;

    RETURN array_to_string(selected, ',');
END;
$$;

CREATE FUNCTION mysql.export_set(bits bigint, on_value text, off_value text,
                                 separator text DEFAULT ',',
                                 bit_count integer DEFAULT 64)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    index integer;
    result text := '';
BEGIN
    IF bits IS NULL OR on_value IS NULL OR off_value IS NULL
       OR separator IS NULL OR bit_count IS NULL THEN
        RETURN NULL;
    END IF;

    bit_count := GREATEST(0, LEAST(bit_count, 64));
    FOR index IN 0 .. bit_count - 1 LOOP
        IF index > 0 THEN
            result := result || separator;
        END IF;
        IF (bits & (1::bigint << index)) <> 0 THEN
            result := result || on_value;
        ELSE
            result := result || off_value;
        END IF;
    END LOOP;

    RETURN result;
END;
$$;

CREATE FUNCTION mysql.space(character_count integer)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT CASE WHEN $1 IS NULL THEN NULL ELSE repeat('' '', GREATEST($1, 0)) END';

CREATE FUNCTION mysql.strcmp(left_value text, right_value text)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN $1 IS NULL OR $2 IS NULL THEN NULL
        WHEN $1 < $2 THEN -1
        WHEN $1 > $2 THEN 1
        ELSE 0
    END
$$;

CREATE FUNCTION mysql.quote(source text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
BEGIN
    IF source IS NULL THEN
        RETURN 'NULL';
    END IF;

    RETURN '''' ||
           replace(
             replace(
               replace(source, E'\\', E'\\\\'),
               E'\032', E'\\Z'),
             '''', E'\\''') ||
           '''';
END;
$$;

CREATE FUNCTION mysql.unhex(source text)
RETURNS bytea
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
BEGIN
    IF source IS NULL THEN
        RETURN NULL;
    END IF;
    IF source !~ '^[0-9A-Fa-f]*$' THEN
        RETURN NULL;
    END IF;
    IF length(source) % 2 = 1 THEN
        source := '0' || source;
    END IF;
    RETURN decode(source, 'hex');
END;
$$;

CREATE FUNCTION mysql.hex(source bytea)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT upper(encode($1, ''hex''))';

CREATE FUNCTION mysql.hex(source text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT upper(encode(convert_to($1, ''UTF8''), ''hex''))';

CREATE FUNCTION mysql._digit_value(digit text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    code integer := ascii(upper(digit));
BEGIN
    IF code BETWEEN ascii('0') AND ascii('9') THEN
        RETURN code - ascii('0');
    ELSIF code BETWEEN ascii('A') AND ascii('Z') THEN
        RETURN code - ascii('A') + 10;
    END IF;
    RETURN -1;
END;
$$;

CREATE FUNCTION mysql._digit_character(value integer)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN $1 BETWEEN 0 AND 9 THEN chr(ascii('0') + $1)
        WHEN $1 BETWEEN 10 AND 35 THEN chr(ascii('A') + $1 - 10)
        ELSE NULL
    END
$$;

CREATE FUNCTION mysql.conv(source text, from_base integer, to_base integer)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    input_base integer := abs(from_base);
    output_base integer := abs(to_base);
    source_index integer := 1;
    digit integer;
    accumulator numeric := 0;
    negative boolean := false;
    result text := '';
    remainder integer;
BEGIN
    IF source IS NULL OR from_base IS NULL OR to_base IS NULL THEN
        RETURN NULL;
    END IF;
    IF input_base < 2 OR input_base > 36
       OR output_base < 2 OR output_base > 36 THEN
        RETURN NULL;
    END IF;

    source := btrim(source);
    IF left(source, 1) IN ('+', '-') THEN
        negative := left(source, 1) = '-';
        source_index := 2;
    END IF;

    WHILE source_index <= char_length(source) LOOP
        digit := mysql._digit_value(substr(source, source_index, 1));
        EXIT WHEN digit < 0 OR digit >= input_base;
        accumulator := accumulator * input_base + digit;
        source_index := source_index + 1;
    END LOOP;

    IF source_index = 1 OR (source_index = 2 AND left(source, 1) IN ('+', '-')) THEN
        RETURN '0';
    END IF;
    IF accumulator = 0 THEN
        RETURN '0';
    END IF;

    WHILE accumulator > 0 LOOP
        remainder := mod(accumulator, output_base)::integer;
        result := mysql._digit_character(remainder) || result;
        accumulator := trunc(accumulator / output_base);
    END LOOP;

    IF negative AND to_base < 0 THEN
        result := '-' || result;
    END IF;
    RETURN result;
END;
$$;

CREATE FUNCTION mysql.bin(value bigint)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT mysql.conv($1::text, 10, 2)';

CREATE FUNCTION mysql.oct(value bigint)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT mysql.conv($1::text, 10, 8)';

/* -------------------------------------------------------------------------
 * Numeric functions
 * -------------------------------------------------------------------------
 */

CREATE FUNCTION mysql.truncate(value numeric, decimal_places integer)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN $1 IS NULL OR $2 IS NULL THEN NULL
        WHEN $2 >= 0 THEN trunc($1, $2)
        ELSE trunc($1 / power(10::numeric, -$2)) * power(10::numeric, -$2)
    END
$$;

CREATE FUNCTION mysql.format(value numeric, decimal_places integer)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    places integer;
    pattern text;
BEGIN
    IF value IS NULL OR decimal_places IS NULL THEN
        RETURN NULL;
    END IF;

    places := GREATEST(decimal_places, 0);
    pattern := 'FM999G999G999G999G999G999G990';
    IF places > 0 THEN
        pattern := pattern || 'D' || repeat('0', places);
    END IF;
    RETURN to_char(round(value, places), pattern);
END;
$$;

CREATE FUNCTION mysql.sign(value numeric)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE WHEN $1 IS NULL THEN NULL
                WHEN $1 < 0 THEN -1
                WHEN $1 > 0 THEN 1
                ELSE 0 END
$$;

CREATE FUNCTION mysql.log2(value double precision)
RETURNS double precision
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT CASE WHEN $1 > 0 THEN ln($1) / ln(2.0) ELSE NULL END';

CREATE FUNCTION mysql.log10(value double precision)
RETURNS double precision
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT CASE WHEN $1 > 0 THEN log($1) ELSE NULL END';

CREATE FUNCTION mysql.radians(value double precision)
RETURNS double precision
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT $1 * pi() / 180.0';

CREATE FUNCTION mysql.degrees(value double precision)
RETURNS double precision
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT $1 * 180.0 / pi()';

/* -------------------------------------------------------------------------
 * Date and time functions
 * -------------------------------------------------------------------------
 */

CREATE FUNCTION mysql.datediff(end_date date, start_date date)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT $1 - $2';

CREATE FUNCTION mysql.last_day(value date)
RETURNS date
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT (date_trunc('month', $1::timestamp) + interval '1 month - 1 day')::date
$$;

CREATE FUNCTION mysql.dayofweek(value date)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT extract(dow FROM $1)::integer + 1';

CREATE FUNCTION mysql.weekday(value date)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT extract(isodow FROM $1)::integer - 1';

CREATE FUNCTION mysql.dayofyear(value date)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT extract(doy FROM $1)::integer';

CREATE FUNCTION mysql.quarter(value date)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT extract(quarter FROM $1)::integer';

CREATE FUNCTION mysql.weekofyear(value date)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT extract(week FROM $1)::integer';

CREATE FUNCTION mysql.unix_timestamp(value timestamp with time zone DEFAULT now())
RETURNS numeric
LANGUAGE sql
STABLE
PARALLEL SAFE
AS 'SELECT extract(epoch FROM $1)::numeric';

CREATE FUNCTION mysql.from_unixtime(value numeric)
RETURNS timestamp with time zone
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT to_timestamp($1::double precision)';

CREATE FUNCTION mysql.timestampdiff(unit_name text, start_value timestamp,
                                    end_value timestamp)
RETURNS bigint
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    seconds numeric;
    months integer;
BEGIN
    IF unit_name IS NULL OR start_value IS NULL OR end_value IS NULL THEN
        RETURN NULL;
    END IF;

    seconds := extract(epoch FROM end_value - start_value);
    CASE upper(unit_name)
        WHEN 'MICROSECOND' THEN RETURN trunc(seconds * 1000000)::bigint;
        WHEN 'SECOND' THEN RETURN trunc(seconds)::bigint;
        WHEN 'MINUTE' THEN RETURN trunc(seconds / 60)::bigint;
        WHEN 'HOUR' THEN RETURN trunc(seconds / 3600)::bigint;
        WHEN 'DAY' THEN RETURN trunc(seconds / 86400)::bigint;
        WHEN 'WEEK' THEN RETURN trunc(seconds / 604800)::bigint;
        WHEN 'MONTH' THEN
            months := (extract(year FROM end_value)::integer -
                       extract(year FROM start_value)::integer) * 12 +
                      extract(month FROM end_value)::integer -
                      extract(month FROM start_value)::integer;
            IF end_value < start_value + make_interval(months => months) THEN
                months := months - 1;
            END IF;
            RETURN months;
        WHEN 'QUARTER' THEN
            RETURN mysql.timestampdiff('MONTH', start_value, end_value) / 3;
        WHEN 'YEAR' THEN
            RETURN mysql.timestampdiff('MONTH', start_value, end_value) / 12;
        ELSE
            RAISE EXCEPTION 'unsupported TIMESTAMPDIFF unit: %', unit_name
                USING ERRCODE = '22023';
    END CASE;
END;
$$;

CREATE FUNCTION mysql.timestampadd(unit_name text, interval_count bigint,
                                   source timestamp)
RETURNS timestamp
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
BEGIN
    IF unit_name IS NULL OR interval_count IS NULL OR source IS NULL THEN
        RETURN NULL;
    END IF;

    CASE upper(unit_name)
        WHEN 'MICROSECOND' THEN
            RETURN source + interval_count * interval '1 microsecond';
        WHEN 'SECOND' THEN
            RETURN source + interval_count * interval '1 second';
        WHEN 'MINUTE' THEN
            RETURN source + interval_count * interval '1 minute';
        WHEN 'HOUR' THEN
            RETURN source + interval_count * interval '1 hour';
        WHEN 'DAY' THEN
            RETURN source + interval_count * interval '1 day';
        WHEN 'WEEK' THEN
            RETURN source + interval_count * interval '1 week';
        WHEN 'MONTH' THEN
            RETURN source + make_interval(months => interval_count::integer);
        WHEN 'QUARTER' THEN
            RETURN source + make_interval(months => (interval_count * 3)::integer);
        WHEN 'YEAR' THEN
            RETURN source + make_interval(years => interval_count::integer);
        ELSE
            RAISE EXCEPTION 'unsupported TIMESTAMPADD unit: %', unit_name
                USING ERRCODE = '22023';
    END CASE;
END;
$$;

CREATE FUNCTION mysql.adddate(source date, day_count integer)
RETURNS date
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT $1 + $2';

CREATE FUNCTION mysql.subdate(source date, day_count integer)
RETURNS date
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT $1 - $2';

CREATE FUNCTION mysql.period_add(period_value integer, month_count integer)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    year_value integer;
    month_value integer;
    absolute_month integer;
BEGIN
    IF period_value IS NULL OR month_count IS NULL THEN
        RETURN NULL;
    END IF;
    year_value := period_value / 100;
    month_value := period_value % 100;
    IF month_value NOT BETWEEN 1 AND 12 THEN
        RETURN NULL;
    END IF;
    IF year_value BETWEEN 0 AND 69 THEN
        year_value := year_value + 2000;
    ELSIF year_value BETWEEN 70 AND 99 THEN
        year_value := year_value + 1900;
    END IF;
    absolute_month := year_value * 12 + month_value - 1 + month_count;
    RETURN (absolute_month / 12) * 100 + (absolute_month % 12) + 1;
END;
$$;

CREATE FUNCTION mysql.period_diff(first_period integer, second_period integer)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    first_normalized integer;
    second_normalized integer;
BEGIN
    IF first_period IS NULL OR second_period IS NULL THEN
        RETURN NULL;
    END IF;
    first_normalized := mysql.period_add(first_period, 0);
    second_normalized := mysql.period_add(second_period, 0);
    IF first_normalized IS NULL OR second_normalized IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN ((first_normalized / 100) * 12 + first_normalized % 100) -
           ((second_normalized / 100) * 12 + second_normalized % 100);
END;
$$;

CREATE FUNCTION mysql.date_format(source timestamp, format_string text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    index integer := 1;
    token text;
    result text := '';
BEGIN
    IF source IS NULL OR format_string IS NULL THEN
        RETURN NULL;
    END IF;

    WHILE index <= char_length(format_string) LOOP
        IF substr(format_string, index, 1) <> '%' THEN
            result := result || substr(format_string, index, 1);
            index := index + 1;
            CONTINUE;
        END IF;

        index := index + 1;
        IF index > char_length(format_string) THEN
            result := result || '%';
            EXIT;
        END IF;
        token := substr(format_string, index, 1);
        CASE token
            WHEN '%' THEN result := result || '%';
            WHEN 'Y' THEN result := result || to_char(source, 'YYYY');
            WHEN 'y' THEN result := result || to_char(source, 'YY');
            WHEN 'M' THEN result := result || to_char(source, 'FMMonth');
            WHEN 'b' THEN result := result || to_char(source, 'Mon');
            WHEN 'm' THEN result := result || to_char(source, 'MM');
            WHEN 'c' THEN result := result || to_char(source, 'FMMM');
            WHEN 'D' THEN
                result := result || extract(day FROM source)::integer::text ||
                    CASE
                        WHEN extract(day FROM source)::integer BETWEEN 11 AND 13
                            THEN 'th'
                        WHEN extract(day FROM source)::integer % 10 = 1 THEN 'st'
                        WHEN extract(day FROM source)::integer % 10 = 2 THEN 'nd'
                        WHEN extract(day FROM source)::integer % 10 = 3 THEN 'rd'
                        ELSE 'th'
                    END;
            WHEN 'd' THEN result := result || to_char(source, 'DD');
            WHEN 'e' THEN result := result || to_char(source, 'FMDD');
            WHEN 'j' THEN result := result || to_char(source, 'DDD');
            WHEN 'H' THEN result := result || to_char(source, 'HH24');
            WHEN 'k' THEN result := result || to_char(source, 'FMHH24');
            WHEN 'h' THEN result := result || to_char(source, 'HH12');
            WHEN 'I' THEN result := result || to_char(source, 'HH12');
            WHEN 'l' THEN result := result || to_char(source, 'FMHH12');
            WHEN 'i' THEN result := result || to_char(source, 'MI');
            WHEN 's' THEN result := result || to_char(source, 'SS');
            WHEN 'S' THEN result := result || to_char(source, 'SS');
            WHEN 'f' THEN result := result || to_char(source, 'US');
            WHEN 'p' THEN result := result || to_char(source, 'AM');
            WHEN 'r' THEN result := result || to_char(source, 'HH12:MI:SS AM');
            WHEN 'T' THEN result := result || to_char(source, 'HH24:MI:SS');
            WHEN 'W' THEN result := result || to_char(source, 'FMDay');
            WHEN 'a' THEN result := result || to_char(source, 'Dy');
            WHEN 'w' THEN
                result := result || extract(dow FROM source)::integer::text;
            WHEN 'U' THEN result := result || to_char(source, 'WW');
            WHEN 'u' THEN result := result || to_char(source, 'IW');
            WHEN 'V' THEN result := result || to_char(source, 'IW');
            WHEN 'v' THEN result := result || to_char(source, 'IW');
            WHEN 'X' THEN result := result || to_char(source, 'IYYY');
            WHEN 'x' THEN result := result || to_char(source, 'IYYY');
            ELSE result := result || token;
        END CASE;
        index := index + 1;
    END LOOP;

    RETURN result;
END;
$$;

CREATE FUNCTION mysql._str_to_date_format(format_string text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
BEGIN
    format_string := replace(format_string, '%Y', 'YYYY');
    format_string := replace(format_string, '%y', 'YY');
    format_string := replace(format_string, '%M', 'Month');
    format_string := replace(format_string, '%b', 'Mon');
    format_string := replace(format_string, '%m', 'MM');
    format_string := replace(format_string, '%c', 'MM');
    format_string := replace(format_string, '%d', 'DD');
    format_string := replace(format_string, '%e', 'DD');
    format_string := replace(format_string, '%j', 'DDD');
    format_string := replace(format_string, '%H', 'HH24');
    format_string := replace(format_string, '%k', 'HH24');
    format_string := replace(format_string, '%h', 'HH12');
    format_string := replace(format_string, '%I', 'HH12');
    format_string := replace(format_string, '%l', 'HH12');
    format_string := replace(format_string, '%i', 'MI');
    format_string := replace(format_string, '%s', 'SS');
    format_string := replace(format_string, '%S', 'SS');
    format_string := replace(format_string, '%f', 'US');
    format_string := replace(format_string, '%p', 'AM');
    format_string := replace(format_string, '%T', 'HH24:MI:SS');
    format_string := replace(format_string, '%r', 'HH12:MI:SS AM');
    RETURN format_string;
END;
$$;

CREATE FUNCTION mysql.str_to_date(source text, format_string text)
RETURNS timestamp
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
BEGIN
    IF source IS NULL OR format_string IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN to_timestamp(source, mysql._str_to_date_format(format_string))::timestamp;
EXCEPTION
    WHEN invalid_datetime_format OR datetime_field_overflow THEN
        RETURN NULL;
END;
$$;

/* -------------------------------------------------------------------------
 * Aggregate helpers
 * -------------------------------------------------------------------------
 */

CREATE FUNCTION mysql._group_concat_transition(state text, value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN $2 IS NULL THEN $1
        WHEN $1 IS NULL THEN $2
        ELSE $1 || ',' || $2
    END
$$;

CREATE AGGREGATE mysql.group_concat(text) (
    SFUNC = mysql._group_concat_transition,
    STYPE = text,
    PARALLEL = SAFE
);

CREATE FUNCTION mysql._bit_xor_transition(state bigint, value bigint)
RETURNS bigint
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT COALESCE($1, 0) # COALESCE($2, 0)';

CREATE AGGREGATE mysql.bit_xor(bigint) (
    SFUNC = mysql._bit_xor_transition,
    STYPE = bigint,
    INITCOND = '0',
    PARALLEL = SAFE
);

CREATE FUNCTION mysql._first_nonnull(state anyelement, value anyelement)
RETURNS anyelement
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS 'SELECT COALESCE($1, $2)';

CREATE AGGREGATE mysql.any_value(anyelement) (
    SFUNC = mysql._first_nonnull,
    STYPE = anyelement,
    PARALLEL = SAFE
);

COMMENT ON SCHEMA mysql IS
    'Opt-in MySQL function compatibility layer for OpenTenBase migrations';

COMMENT ON FUNCTION mysql.concat(VARIADIC text[]) IS
    'MySQL CONCAT semantics: returns NULL if any argument is NULL';

COMMENT ON FUNCTION mysql.concat_ws(text, VARIADIC text[]) IS
    'MySQL CONCAT_WS semantics: skips NULL values after the separator';

COMMENT ON FUNCTION mysql.timestampdiff(text, timestamp, timestamp) IS
    'Returns complete unit boundaries between two timestamps';

COMMENT ON FUNCTION mysql.date_format(timestamp, text) IS
    'Formats a timestamp using common MySQL percent tokens';
