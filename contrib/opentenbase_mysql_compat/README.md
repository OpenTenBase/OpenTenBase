# opentenbase_mysql_compat

`opentenbase_mysql_compat` is an OpenTenBase extension that implements the
most widely used MySQL built-in functions with MySQL semantics, so that
SQL written for MySQL can run on OpenTenBase with minimal rewriting.  It
is a standalone `contrib` plugin: no kernel changes are required.

It implements Module 4 ("MySQL Compatible Function Extension") of the
OpenTenBase comprehensive optimization roadmap
([issue #230](https://github.com/OpenTenBase/OpenTenBase/issues/230)).

## Provided functions

### String functions

| Function | Signature | Notes |
|---|---|---|
| `concat` | `concat(VARIADIC "any") -> text` | Returns NULL if any argument is NULL (unlike PostgreSQL `concat`); non-text arguments are converted implicitly |
| `concat_ws` | `concat_ws(text, VARIADIC "any") -> text` | NULL separator yields NULL; NULL arguments are skipped |
| `find_in_set` | `find_in_set(text, text) -> int` | 1-based position in a comma-separated list, 0 if absent |
| `elt` | `elt(int, VARIADIC text) -> text` | N-th element of the list |
| `field` | `field(text, VARIADIC text) -> int` | 1-based index of the needle, 0 if absent |
| `insert` | `"insert"(text, int, int, text) -> text` | Replace a substring; `INSERT` is a reserved word, so quote the name or use `mysql_insert()` |
| `substring_index` | `substring_index(text, text, int) -> text` | Part before/after the N-th occurrence of the delimiter |
| `locate` | `locate(text, text [, int]) -> int` | 1-based case-insensitive position, 0 if absent |
| `instr` | `instr(text, text) -> int` | `locate(substr, str)` with swapped arguments |
| `reverse` | `reverse(text) -> text` | Multibyte aware |
| `space` | `space(int) -> text` | |
| `unhex` | `unhex(text) -> bytea` | NULL for odd-length or invalid input |

### Date and time functions

| Function | Signature | Notes |
|---|---|---|
| `date_format` | `date_format(timestamp \| date, text) -> text` | MySQL `%` format specifiers (English month/day names) |
| `str_to_date` | `str_to_date(text, text) -> timestamp` | Inverse of `date_format` for the common specifiers |
| `datediff` | `datediff(timestamp \| date, timestamp \| date) -> int` | Difference of the date parts in days (`a - b`) |
| `timestampdiff` | `timestampdiff(text, timestamp, timestamp) -> bigint` | Units: MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR |
| `timestampadd` | `timestampadd(text, bigint, timestamp) -> timestamp` | Same units; month arithmetic clamps like MySQL (Jan 31 + 1 month = Feb 28) |
| `last_day` | `last_day(timestamp \| date) -> date` | Last day of the month |
| `quarter` | `quarter(timestamp \| date) -> int` | |
| `dayofweek` | `dayofweek(timestamp) -> int` | 1 = Sunday … 7 = Saturday |
| `weekday` | `weekday(timestamp) -> int` | 0 = Monday … 6 = Sunday |
| `dayofmonth`, `dayofyear`, `weekofyear` | `(timestamp) -> int` | `weekofyear` is ISO-8601, matching MySQL |

### Math functions

| Function | Signature | Notes |
|---|---|---|
| `format` | `format(numeric \| double precision \| int, int) -> text` | Rounds and adds `,` thousands separators |
| `truncate` | `"truncate"(numeric \| double precision \| int, int) -> numeric` | MySQL truncation; `TRUNCATE` is a reserved word, quote the name or use `mysql_truncate()` / built-in `trunc()` |
| `bin`, `oct` | `(bigint) -> text` | Radix conversion; negatives use two's-complement 64-bit rendering |

### Flow control

| Function | Signature | Notes |
|---|---|---|
| `if` | `if(boolean, anyelement, anyelement) -> anyelement` | Both branches must have the same type |
| `ifnull` | `ifnull(anyelement, anyelement) -> anyelement` | Two-argument `coalesce` |
| `isnull` | `isnull("any") -> int` | 1 when the argument is NULL, else 0 |

### Aggregates

| Function | Signature | Notes |
|---|---|---|
| `group_concat` | `group_concat(anyelement [, text]) -> text` | Joins non-NULL values with `,` or the given separator; use aggregate `ORDER BY` for ordering |

## Installation

Build and install from the OpenTenBase source tree:

```sh
./configure ...            # as documented for OpenTenBase
make -C contrib/opentenbase_mysql_compat
make -C contrib/opentenbase_mysql_compat install
```

Then in the target database:

```sql
CREATE EXTENSION opentenbase_mysql_compat;
```

The extension is relocatable.  To have the MySQL versions of functions
that also exist in PostgreSQL (`concat`, `concat_ws`) take precedence,
install it into its own schema and put that schema in front of
`pg_catalog` in `search_path`:

```sql
CREATE SCHEMA mysql_compat;
CREATE EXTENSION opentenbase_mysql_compat SCHEMA mysql_compat;
SET search_path = mysql_compat, pg_catalog, public;
```

## Migration tutorial

Consider this MySQL query set:

```sql
-- MySQL
SELECT CONCAT(first_name, ' ', last_name)          AS full_name,
       IFNULL(nickname, first_name)                AS display_name,
       FORMAT(salary, 2)                           AS salary_fmt,
       IF(salary > 5000, 'high', 'low')            AS band,
       DATE_FORMAT(hired_at, '%Y-%m-%d')           AS hired,
       TIMESTAMPDIFF(YEAR, hired_at, NOW())        AS tenure_years
FROM employees;

SELECT dept, GROUP_CONCAT(name ORDER BY name SEPARATOR '; ')
FROM employees GROUP BY dept;

SELECT DATEDIFF('2020-01-02', '2020-01-01');
SELECT STR_TO_DATE('01,5,2013', '%d,%m,%Y');
```

The OpenTenBase equivalent with this extension:

```sql
-- OpenTenBase
SELECT concat(first_name, ' ', last_name)          AS full_name,
       ifnull(nickname, first_name)                AS display_name,
       format(salary, 2)                           AS salary_fmt,
       if(salary > 5000, 'high', 'low')            AS band,
       date_format(hired_at, '%Y-%m-%d')           AS hired,
       timestampdiff('YEAR', hired_at, now()::timestamp) AS tenure_years
FROM employees;

SELECT dept, group_concat(name, '; ' ORDER BY name)
FROM employees GROUP BY dept;

SELECT datediff('2020-01-02'::timestamp, '2020-01-01'::timestamp);
SELECT str_to_date('01,5,2013', '%d,%m,%Y');
```

Mechanical rewrite rules:

1. **Function names** map 1:1, in lowercase, once the extension is on
   `search_path`.
2. `GROUP_CONCAT(x SEPARATOR s)` becomes `group_concat(x, s)`.
3. `GROUP_CONCAT(x ORDER BY y)` keeps the aggregate `ORDER BY` syntax.
4. Timestamp units are passed as a string:
   `TIMESTAMPDIFF(YEAR, a, b)` → `timestampdiff('YEAR', a, b)`.
5. String literals compared with dates should be cast explicitly
   (`'2020-01-02'::timestamp`) — MySQL parses strings as dates
   implicitly, PostgreSQL requires a cast.
6. `INSERT()` and `TRUNCATE()` collide with SQL reserved words:
   write `"insert"(...)` / `"truncate"(...)`, or use the unquoted
   aliases `mysql_insert()` / `mysql_truncate()`.

## Known differences from MySQL

* Case-insensitive matching (`locate`, `instr`, `field`,
  `find_in_set`) covers the ASCII range; non-ASCII characters compare
  byte-exactly.  Adjust the collation in the query if needed.
* `str_to_date` always returns `timestamp`.  MySQL sometimes returns a
  `date`; cast the result (`::date`) where a plain date is required.
  Date fields absent from the format default to `0001-01-01` because
  PostgreSQL has no zero date.
* `concat` renders `bytea` arguments with the PostgreSQL bytea output
  (hex) representation.
* MySQL's `GROUP_CONCAT` ignores an ORDER BY/SUFFIX grammar; the
  aggregate standard syntax (`group_concat(x ORDER BY x)`) is used
  instead.
* Week format specifiers (`%U`, `%u`, `%V`, `%v`, `%X`, `%x`) follow
  the MySQL definitions; parsing them back with `str_to_date` is not
  supported (matching MySQL's own restrictions on `%U/%u/%V/%v`).

## Testing

With a running OpenTenBase instance:

```sh
make -C contrib/opentenbase_mysql_compat installcheck
```

## License

Same license as OpenTenBase (BSD 3-Clause), see the repository
`LICENSE.txt`.
