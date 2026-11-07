# sql-explainer

A dependency-free, single-file HTML teaching aid that explains SQL examples
from the OpenTenBase documentation **without connecting to a database**.

## What it does

Paste a statement into the left pane, or select any fragment of it. The right
pane shows:

- what the statement or keyword does, and a step-by-step reading order
- the grammar form
- the exact source location in `doc/src/sgml/`
- the expected result (derived by reading, never executed)
- a short exercise with a collapsible answer

It is deliberately a *reading* aid: no SQL is executed, no network access is
made, and no running cluster is required. That makes it safe to hand to
beginners who have not installed OpenTenBase yet.

## Sources

All examples are taken from the documentation shipped in this repository:

| Example | Source |
| --- | --- |
| `CREATE TABLE products` | `doc/src/sgml/ddl.sgml:109-113` |
| `INSERT INTO products` | `doc/src/sgml/dml.sgml:62, 94-98` |
| `SELECT * FROM table1` | `doc/src/sgml/queries.sgml:41, 50-51` |
| `DISTRIBUTE BY HASH` | `doc/src/sgml/start.sgml:364`, `ddl.sgml:176-177` |
| `UPDATE ... WHERE` | `doc/src/sgml/dml.sgml:178, 180-181` |

The explainer is written in Chinese; rule names and source paths are kept in
their original form so readers can cross-check them against the docs.

## Usage

Open `sql_explainer.html` in any modern browser. No build step, no server,
no dependencies.

## Tests

```sh
cd contrib/sql-explainer
node test.js
```

`test.js` parses the rule table out of the HTML and checks that each statement
form resolves to the intended rule, including statements split across lines
or separated by tabs. It also guards against the column-detection regex
regressing to a literal backslash match.

## License

BSD 3-Clause. See `LICENSE.txt` at the repository root.
