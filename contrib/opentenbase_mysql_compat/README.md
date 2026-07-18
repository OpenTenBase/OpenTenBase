<!--
Copyright (c) 2026 OpenTenBase Contributors

This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
in the repository root for details.
-->

# opentenbase_mysql_compat

`opentenbase_mysql_compat` is an opt-in compatibility extension for teams
migrating SQL from MySQL to OpenTenBase.  It provides commonly used MySQL
scalar and aggregate functions without changing the OpenTenBase parser or the
semantics of built-in PostgreSQL functions.

The functions live in the `mysql` schema.  Applications can either qualify
calls explicitly (`mysql.find_in_set(...)`) or add `mysql` to `search_path`
for a controlled migration period.

## Design goals

- Preserve MySQL NULL and out-of-range behavior where it commonly differs
  from PostgreSQL.
- Keep the compatibility layer isolated and removable with `DROP EXTENSION`.
- Avoid network access, background workers, superuser-only hooks, and external
  dependencies.
- Make every helper immutable or stable when its result permits it, so the
  optimizer can fold constants and use the functions in expression indexes.
- Cover migration-heavy string, date/time, numeric, flow-control, and aggregate
  functions with deterministic regression assertions.

## Build and install

The extension is part of the top-level `contrib` build:

```sh
./configure --prefix=/path/to/opentenbase
make -C contrib/opentenbase_mysql_compat
make -C contrib/opentenbase_mysql_compat install
```

It can also be built against an installed OpenTenBase tree with PGXS:

```sh
cd contrib/opentenbase_mysql_compat
make USE_PGXS=1 PG_CONFIG=/path/to/pg_config
make USE_PGXS=1 PG_CONFIG=/path/to/pg_config install
```

Enable it in each database that needs the compatibility layer:

```sql
CREATE EXTENSION opentenbase_mysql_compat;
```

No shared library preload or server restart is required.

## Function catalog

### Flow control

| Function | Behavior |
| --- | --- |
| `mysql.ifnull(a, b)` | Returns `b` when `a` is NULL. |
| `mysql.nullif_mysql(a, b)` | MySQL-compatible alias for `NULLIF`. |
| `mysql.mysql_if(test, a, b)` | Chooses `a` or `b`; a NULL condition follows SQL CASE behavior. |
| `mysql.if_true(test, a, b)` | Treats a NULL condition as false explicitly. |

The arguments of polymorphic functions must resolve to the same OpenTenBase
type.  Cast untyped NULL literals when necessary:

```sql
SELECT mysql.ifnull(NULL::integer, 42);
```

### Strings and base conversion

| Function | Notes |
| --- | --- |
| `mysql.concat(VARIADIC text[])` | Returns NULL when any value is NULL. |
| `mysql.concat_ws(separator, VARIADIC text[])` | Skips NULL values. |
| `mysql.elt(n, VARIADIC text[])` | One-based selection; invalid indexes return NULL. |
| `mysql.field(value, VARIADIC text[])` | One-based lookup; returns zero when absent. |
| `mysql.find_in_set(value, csv)` | Finds a value in a comma-separated list. |
| `mysql.insert_string(text, pos, len, replacement)` | Replaces a one-based substring. |
| `mysql.substring_index(text, delimiter, count)` | Keeps fields from the left or right. |
| `mysql.make_set(bits, VARIADIC text[])` | Selects non-NULL values using a bit mask. |
| `mysql.export_set(bits, on, off, separator, count)` | Renders a bit mask as labels. |
| `mysql.space(count)` | Returns a string containing `count` spaces. |
| `mysql.strcmp(left, right)` | Returns -1, 0, or 1. |
| `mysql.quote(text)` | Escapes a value for a MySQL-style quoted literal. |
| `mysql.hex(text)` / `mysql.hex(bytea)` | Produces uppercase hexadecimal text. |
| `mysql.unhex(text)` | Decodes hexadecimal text; invalid input returns NULL. |
| `mysql.conv(text, from_base, to_base)` | Converts bases from 2 through 36. |
| `mysql.bin(bigint)` | Converts decimal to base 2. |
| `mysql.oct(bigint)` | Converts decimal to base 8. |

`mysql.conv` accepts an optional leading sign and consumes valid digits until
the first invalid digit, matching the migration behavior expected by MySQL
applications.  Negative output is requested with a negative destination base.
The implementation uses `numeric` internally so conversion is not restricted
to signed 64-bit intermediate values.

### Numeric functions

| Function | Notes |
| --- | --- |
| `mysql.truncate(value, places)` | Truncates toward zero, including negative places. |
| `mysql.format(value, places)` | Rounds and emits grouped decimal text. |
| `mysql.sign(value)` | Returns -1, 0, or 1. |
| `mysql.log2(value)` | Returns NULL for non-positive input. |
| `mysql.log10(value)` | Returns NULL for non-positive input. |
| `mysql.radians(value)` | Converts degrees to radians. |
| `mysql.degrees(value)` | Converts radians to degrees. |

Formatting follows the active locale's numeric grouping characters, as
OpenTenBase `to_char` does.  Applications requiring a fixed presentation
locale should set `lc_numeric` for their session.

### Date and time

| Function | Notes |
| --- | --- |
| `mysql.datediff(end, start)` | Counts calendar days and ignores time. |
| `mysql.last_day(date)` | Returns the final day of the month. |
| `mysql.dayofweek(date)` | Sunday is 1 and Saturday is 7. |
| `mysql.weekday(date)` | Monday is 0 and Sunday is 6. |
| `mysql.dayofyear(date)` | Returns 1 through 366. |
| `mysql.quarter(date)` | Returns 1 through 4. |
| `mysql.weekofyear(date)` | Returns the ISO week number. |
| `mysql.unix_timestamp(ts)` | Returns seconds from the Unix epoch. |
| `mysql.from_unixtime(seconds)` | Converts epoch seconds to timestamptz. |
| `mysql.timestampdiff(unit, start, end)` | Counts complete requested units. |
| `mysql.timestampadd(unit, count, ts)` | Adds the requested units. |
| `mysql.adddate(date, days)` | Adds calendar days. |
| `mysql.subdate(date, days)` | Subtracts calendar days. |
| `mysql.period_add(period, months)` | Adds months to a `YYYYMM` or `YYMM` period. |
| `mysql.period_diff(a, b)` | Returns the month difference between periods. |
| `mysql.date_format(ts, format)` | Formats common MySQL percent tokens. |
| `mysql.str_to_date(text, format)` | Parses common MySQL date/time formats. |

Supported interval units are `MICROSECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`,
`WEEK`, `MONTH`, `QUARTER`, and `YEAR`.  An unsupported unit raises SQLSTATE
`22023` instead of silently returning a misleading result.

`date_format` supports these tokens:

| Tokens | Meaning |
| --- | --- |
| `%Y`, `%y` | Four- and two-digit years. |
| `%M`, `%b`, `%m`, `%c` | Month name, abbreviation, padded and unpadded number. |
| `%D`, `%d`, `%e`, `%j` | Ordinal day, day of month, and day of year. |
| `%H`, `%k`, `%h`, `%I`, `%l` | 24-hour and 12-hour representations. |
| `%i`, `%s`, `%S`, `%f`, `%p` | Minute, second, microsecond, and AM/PM. |
| `%r`, `%T` | 12-hour and 24-hour complete times. |
| `%W`, `%a`, `%w` | Weekday name, abbreviation, and Sunday-based index. |
| `%U`, `%u`, `%V`, `%v`, `%X`, `%x` | Week and ISO week-year forms. |
| `%%` | Literal percent sign. |

`str_to_date` supports the common numeric, named-month, and time tokens.  It
returns NULL for invalid date text rather than exposing a backend exception.

### Aggregates

| Aggregate | Behavior |
| --- | --- |
| `mysql.group_concat(text)` | Concatenates non-NULL values with commas. |
| `mysql.bit_xor(bigint)` | Computes bitwise XOR and returns zero for no values. |
| `mysql.any_value(anyelement)` | Returns the first non-NULL value observed. |

OpenTenBase aggregate syntax can still control input order and duplicates:

```sql
SELECT mysql.group_concat(DISTINCT name ORDER BY name)
FROM customer;
```

## Migration example

Original MySQL query:

```sql
SELECT
    IFNULL(region, 'unknown') AS region,
    GROUP_CONCAT(DISTINCT customer_name ORDER BY customer_name) AS customers,
    DATE_FORMAT(MAX(created_at), '%Y-%m-%d') AS latest_day
FROM orders
GROUP BY IFNULL(region, 'unknown');
```

OpenTenBase query with this extension:

```sql
SELECT
    mysql.ifnull(region, 'unknown') AS region,
    mysql.group_concat(DISTINCT customer_name ORDER BY customer_name) AS customers,
    mysql.date_format(MAX(created_at), '%Y-%m-%d') AS latest_day
FROM orders
GROUP BY mysql.ifnull(region, 'unknown');
```

## Deliberate boundaries

This extension does not change parser syntax, implicit cast precedence,
collation rules, SQL modes, or storage behavior.  It does not emulate MySQL
functions whose semantics require those global changes.  String comparison
uses the collation of the arguments, and timestamp results follow the session
time zone where a `timestamptz` is involved.

The compatibility schema is not added to `search_path` automatically.  This
avoids shadowing built-in functions in databases that do not need MySQL
compatibility.

## Test

Run the extension regression test from a configured source tree:

```sh
make -C contrib/opentenbase_mysql_compat check
```

The test installs the extension, executes deterministic assertions across all
function families (including NULL and boundary cases), checks aggregates, and
then removes the extension.
