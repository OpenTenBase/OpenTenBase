/*-------------------------------------------------------------------------
 * Copyright (c) 2026 OpenTenBase Contributors
 *
 * This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
 * in the repository root for details.
 *-------------------------------------------------------------------------
 */

CREATE EXTENSION opentenbase_mysql_compat;

DO $test$
DECLARE
    text_result text;
    integer_result integer;
    bigint_result bigint;
    numeric_result numeric;
    timestamp_result timestamp;
BEGIN
    IF mysql.ifnull(NULL::integer, 7) <> 7 THEN
        RAISE EXCEPTION 'ifnull NULL case failed';
    END IF;
    IF mysql.ifnull(4, 7) <> 4 THEN
        RAISE EXCEPTION 'ifnull value case failed';
    END IF;
    IF mysql.nullif_mysql(4, 4) IS NOT NULL THEN
        RAISE EXCEPTION 'nullif equal case failed';
    END IF;
    IF mysql.mysql_if(true, 'yes'::text, 'no'::text) <> 'yes' THEN
        RAISE EXCEPTION 'mysql_if true case failed';
    END IF;
    IF mysql.if_true(NULL, 'yes'::text, 'no'::text) <> 'no' THEN
        RAISE EXCEPTION 'if_true NULL case failed';
    END IF;

    IF mysql.concat('a', 'b', 'c') <> 'abc' THEN
        RAISE EXCEPTION 'concat normal case failed';
    END IF;
    IF mysql.concat('a', NULL, 'c') IS NOT NULL THEN
        RAISE EXCEPTION 'concat NULL case failed';
    END IF;
    IF mysql.concat_ws('-', 'a', NULL, 'c') <> 'a-c' THEN
        RAISE EXCEPTION 'concat_ws NULL skipping failed';
    END IF;
    IF mysql.elt(2, 'red', 'green', 'blue') <> 'green' THEN
        RAISE EXCEPTION 'elt selection failed';
    END IF;
    IF mysql.elt(0, 'red', 'green') IS NOT NULL THEN
        RAISE EXCEPTION 'elt boundary failed';
    END IF;
    IF mysql.field('green', 'red', 'green', 'blue') <> 2 THEN
        RAISE EXCEPTION 'field match failed';
    END IF;
    IF mysql.field('missing', 'red', 'green') <> 0 THEN
        RAISE EXCEPTION 'field miss failed';
    END IF;
    IF mysql.find_in_set('b', 'a,b,c') <> 2 THEN
        RAISE EXCEPTION 'find_in_set match failed';
    END IF;
    IF mysql.find_in_set('x', 'a,b,c') <> 0 THEN
        RAISE EXCEPTION 'find_in_set miss failed';
    END IF;
    IF mysql.insert_string('Quadratic', 3, 4, 'What') <> 'QuWhattic' THEN
        RAISE EXCEPTION 'insert_string replacement failed';
    END IF;
    IF mysql.insert_string('abc', 9, 1, 'x') <> 'abc' THEN
        RAISE EXCEPTION 'insert_string boundary failed';
    END IF;
    IF mysql.substring_index('www.mysql.com', '.', 2) <> 'www.mysql' THEN
        RAISE EXCEPTION 'substring_index positive failed';
    END IF;
    IF mysql.substring_index('www.mysql.com', '.', -2) <> 'mysql.com' THEN
        RAISE EXCEPTION 'substring_index negative failed';
    END IF;
    IF mysql.make_set(5, 'a', 'b', 'c') <> 'a,c' THEN
        RAISE EXCEPTION 'make_set failed';
    END IF;
    IF mysql.export_set(5, 'Y', 'N', '', 4) <> 'YNYN' THEN
        RAISE EXCEPTION 'export_set failed';
    END IF;
    IF char_length(mysql.space(5)) <> 5 THEN
        RAISE EXCEPTION 'space failed';
    END IF;
    IF mysql.strcmp('a', 'b') <> -1 OR mysql.strcmp('b', 'b') <> 0 THEN
        RAISE EXCEPTION 'strcmp failed';
    END IF;
    IF mysql.unhex('4D7953514C') <> convert_to('MySQL', 'UTF8') THEN
        RAISE EXCEPTION 'unhex failed';
    END IF;
    IF mysql.unhex('not-hex') IS NOT NULL THEN
        RAISE EXCEPTION 'unhex invalid input failed';
    END IF;
    IF mysql.hex('MySQL') <> '4D7953514C' THEN
        RAISE EXCEPTION 'hex failed';
    END IF;
    IF mysql.conv('FF', 16, 10) <> '255' THEN
        RAISE EXCEPTION 'conv hexadecimal failed';
    END IF;
    IF mysql.conv('101010', 2, 36) <> '16' THEN
        RAISE EXCEPTION 'conv binary failed';
    END IF;
    IF mysql.bin(10) <> '1010' OR mysql.oct(10) <> '12' THEN
        RAISE EXCEPTION 'bin or oct failed';
    END IF;

    IF mysql.truncate(123.4567, 2) <> 123.45 THEN
        RAISE EXCEPTION 'truncate positive places failed';
    END IF;
    IF mysql.truncate(123.4567, -2) <> 100 THEN
        RAISE EXCEPTION 'truncate negative places failed';
    END IF;
    IF mysql.sign(-1.5) <> -1 OR mysql.sign(0) <> 0 OR mysql.sign(2) <> 1 THEN
        RAISE EXCEPTION 'sign failed';
    END IF;
    IF abs(mysql.log2(8) - 3) > 0.0000001 THEN
        RAISE EXCEPTION 'log2 failed';
    END IF;
    IF mysql.log10(-1) IS NOT NULL THEN
        RAISE EXCEPTION 'log10 domain handling failed';
    END IF;
    IF abs(mysql.degrees(pi()) - 180) > 0.0000001 THEN
        RAISE EXCEPTION 'degrees failed';
    END IF;
    IF abs(mysql.radians(180) - pi()) > 0.0000001 THEN
        RAISE EXCEPTION 'radians failed';
    END IF;

    IF mysql.datediff(DATE '2026-03-10', DATE '2026-03-01') <> 9 THEN
        RAISE EXCEPTION 'datediff failed';
    END IF;
    IF mysql.last_day(DATE '2024-02-11') <> DATE '2024-02-29' THEN
        RAISE EXCEPTION 'last_day leap year failed';
    END IF;
    IF mysql.dayofweek(DATE '2026-07-19') <> 1 THEN
        RAISE EXCEPTION 'dayofweek failed';
    END IF;
    IF mysql.weekday(DATE '2026-07-20') <> 0 THEN
        RAISE EXCEPTION 'weekday failed';
    END IF;
    IF mysql.dayofyear(DATE '2024-12-31') <> 366 THEN
        RAISE EXCEPTION 'dayofyear failed';
    END IF;
    IF mysql.quarter(DATE '2026-10-01') <> 4 THEN
        RAISE EXCEPTION 'quarter failed';
    END IF;
    IF mysql.timestampdiff('SECOND', TIMESTAMP '2026-01-01 00:00:00',
                           TIMESTAMP '2026-01-01 00:01:05') <> 65 THEN
        RAISE EXCEPTION 'timestampdiff seconds failed';
    END IF;
    IF mysql.timestampdiff('MONTH', TIMESTAMP '2025-01-31',
                           TIMESTAMP '2025-03-30') <> 1 THEN
        RAISE EXCEPTION 'timestampdiff complete months failed';
    END IF;
    IF mysql.timestampadd('DAY', 3, TIMESTAMP '2026-01-01') <>
       TIMESTAMP '2026-01-04' THEN
        RAISE EXCEPTION 'timestampadd day failed';
    END IF;
    IF mysql.timestampadd('QUARTER', 1, TIMESTAMP '2026-01-31') <>
       TIMESTAMP '2026-04-30' THEN
        RAISE EXCEPTION 'timestampadd quarter failed';
    END IF;
    IF mysql.adddate(DATE '2026-01-01', 2) <> DATE '2026-01-03' THEN
        RAISE EXCEPTION 'adddate failed';
    END IF;
    IF mysql.subdate(DATE '2026-01-03', 2) <> DATE '2026-01-01' THEN
        RAISE EXCEPTION 'subdate failed';
    END IF;
    IF mysql.period_add(202601, 2) <> 202603 THEN
        RAISE EXCEPTION 'period_add failed';
    END IF;
    IF mysql.period_diff(202603, 202512) <> 3 THEN
        RAISE EXCEPTION 'period_diff failed';
    END IF;
    IF mysql.date_format(TIMESTAMP '2026-07-18 13:04:05.123456',
                         '%Y-%m-%d %H:%i:%s.%f') <>
       '2026-07-18 13:04:05.123456' THEN
        RAISE EXCEPTION 'date_format failed';
    END IF;
    IF mysql.date_format(TIMESTAMP '2026-01-21', '%D') <> '21st' THEN
        RAISE EXCEPTION 'date_format ordinal failed';
    END IF;
    IF mysql.str_to_date('2026-07-18 13:04:05',
                         '%Y-%m-%d %H:%i:%s') <>
       TIMESTAMP '2026-07-18 13:04:05' THEN
        RAISE EXCEPTION 'str_to_date failed';
    END IF;
    IF mysql.str_to_date('not a date', '%Y-%m-%d') IS NOT NULL THEN
        RAISE EXCEPTION 'str_to_date invalid input failed';
    END IF;

    SELECT mysql.group_concat(value ORDER BY value)
    INTO text_result
    FROM (VALUES ('c'::text), ('a'), (NULL), ('b')) AS input(value);
    IF text_result <> 'a,b,c' THEN
        RAISE EXCEPTION 'group_concat failed: %', text_result;
    END IF;

    SELECT mysql.bit_xor(value)
    INTO bigint_result
    FROM (VALUES (1::bigint), (3::bigint), (7::bigint)) AS input(value);
    IF bigint_result <> 5 THEN
        RAISE EXCEPTION 'bit_xor failed: %', bigint_result;
    END IF;

    SELECT mysql.any_value(value)
    INTO integer_result
    FROM (VALUES (NULL::integer), (4), (9)) AS input(value);
    IF integer_result <> 4 THEN
        RAISE EXCEPTION 'any_value failed: %', integer_result;
    END IF;
END;
$test$;

DROP EXTENSION opentenbase_mysql_compat;
