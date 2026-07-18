<!--
Copyright (c) 2026 OpenTenBase Contributors

This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
in the repository root for details.
-->

# OpenTenBase distributed observer

`opentenbase_observer` collects the same read-only operational snapshot from
every configured CoordinatorNode and DataNode, stores a portable JSON result,
applies explainable diagnostic rules, exports Prometheus text, and compares
counter rates between snapshots.

It is designed for fast incident triage and lightweight monitoring bootstrap:

- one standard-library Python script;
- existing `psql` authentication and TLS behavior;
- no server extension, daemon, HTTP listener, or third-party package;
- bounded parallel node collection;
- offline analysis and reproducible reports;
- deterministic JSON and Prometheus output.

## Install

The tool participates in the top-level `contrib` install:

```sh
./configure --prefix=/opt/opentenbase
make -C contrib/opentenbase_observer
make -C contrib/opentenbase_observer install
```

This installs `opentenbase_observer` in the configured OpenTenBase `bindir`.
It can also run directly:

```sh
python3 contrib/opentenbase_observer/opentenbase_observer.py --help
```

Python 3.8 or newer and a compatible `psql` executable are required.  The
collector does not accept passwords in configuration or command-line options.
Use `.pgpass`, `PGPASSFILE`, a service file, certificates, or another normal
libpq authentication mechanism.

## Configuration

Create a JSON file with explicit node endpoints:

```json
{
  "version": 1,
  "nodes": [
    {
      "name": "cn1",
      "host": "10.10.0.11",
      "port": 15432,
      "database": "postgres",
      "user": "observer",
      "role": "coordinator",
      "sslmode": "require",
      "connect_timeout": 5
    },
    {
      "name": "dn1",
      "host": "10.10.0.21",
      "port": 15432,
      "database": "postgres",
      "user": "observer",
      "role": "datanode"
    },
    {
      "name": "dn2",
      "host": "10.10.0.22",
      "port": 15432,
      "database": "postgres",
      "user": "observer",
      "role": "datanode"
    }
  ]
}
```

Supported roles are `coordinator`, `datanode`, `gtm`, and `standalone`.
Supported `sslmode` values match libpq: `disable`, `allow`, `prefer`, `require`,
`verify-ca`, and `verify-full`.

Validate before collection:

```sh
opentenbase_observer check-config observer.json
```

Validation rejects duplicate node names, invalid roles and ports, unknown TLS
modes, control characters in connection fields, malformed threshold values,
and unknown threshold keys.

## Least-privilege database user

The query reads `pg_stat_activity`, `pg_locks`, `pg_stat_database`,
`pg_stat_bgwriter`, `pg_database`, and selected settings.  Configure a login
that can see the required catalog fields according to the OpenTenBase version
and local security policy.  Do not grant write privileges for this tool.

On PostgreSQL-derived versions that restrict other sessions' query text, grant
the built-in monitoring role supported by that version, or accept that query
text fields will be hidden.  The collector itself issues only `SET
statement_timeout` and one catalog `SELECT` statement.

## Collect a distributed snapshot

```sh
opentenbase_observer collect observer.json \
  --output snapshots/2026-07-18T030000Z.json \
  --jobs 8 \
  --timeout 15 \
  --statement-timeout-ms 10000
```

`--timeout` bounds the complete `psql` process for one node.  The SQL statement
timeout is a second, server-side bound.  `PGCONNECT_TIMEOUT`, `PGSSLMODE`, and
`PGAPPNAME=opentenbase_observer` are set independently for each invocation.

The command exits `0` when every node succeeds and `2` when collection finishes
with one or more unavailable nodes.  A snapshot is written in both cases so
the outage evidence is retained.  Configuration and local filesystem errors
exit `1`.

Each successful node includes 30+ metrics across these groups:

- node reachability and collection duration;
- server version and maximum connections;
- total, active, and idle-in-transaction sessions;
- longest active query and oldest transaction age;
- total locks and waiting locks;
- commits, rollbacks, deadlocks, and tuple activity;
- blocks read and buffer hits;
- temporary files and bytes;
- total database bytes;
- timed and requested checkpoints;
- checkpoint write and sync time;
- checkpoint, clean, backend, and allocated buffer counters.

The snapshot also contains bounded event records for active queries older than
five seconds and ungranted locks.  Query whitespace is normalized and query
text is limited to 2,000 characters to keep incident snapshots manageable.

## Analyze health

```sh
opentenbase_observer analyze snapshots/latest.json
```

Rules produce a severity, stable code, node, summary, numerical evidence, and
an actionable recommendation.  Current rules cover:

- unreachable nodes;
- warning and critical connection pressure;
- sessions idle in transaction;
- waiting lock requests;
- warning and critical long-running queries;
- warning and critical old transactions;
- low cumulative buffer cache hit ratio;
- high rollback ratio after a minimum sample size.

Use the result in automation:

```sh
opentenbase_observer analyze snapshots/latest.json --fail-on critical
opentenbase_observer analyze snapshots/latest.json --fail-on warning
```

`--fail-on` returns status `2` when the selected severity is present.  The
default `never` always returns `0` after a valid analysis and leaves policy to
the caller.

### Threshold overrides

Add an optional object to the collector config:

```json
{
  "thresholds": {
    "connection_warning": 0.75,
    "connection_critical": 0.90,
    "idle_in_transaction_warning": 1,
    "waiting_locks_warning": 1,
    "longest_query_warning_seconds": 20,
    "longest_query_critical_seconds": 180,
    "oldest_transaction_warning_seconds": 60,
    "oldest_transaction_critical_seconds": 600,
    "cache_hit_warning": 0.95,
    "rollback_ratio_warning": 0.10,
    "rollback_ratio_minimum_transactions": 100
  }
}
```

Then analyze with those values:

```sh
opentenbase_observer analyze snapshots/latest.json --config observer.json
```

The analyzer deliberately calls buffer hit and rollback values "cumulative".
For operational rates, compare snapshots rather than interpreting a lifetime
counter as a recent incident.

## Compare snapshots

```sh
opentenbase_observer diff snapshots/before.json snapshots/after.json
```

The report contains elapsed seconds, added and removed nodes, and matched metric
values.  Gauge metrics receive a delta.  The collector's known cumulative
transaction, I/O, tuple, temporary-file, and checkpoint metrics also receive a
per-second rate.  A negative counter delta is marked `counter_reset` and no
misleading rate is calculated.  Connection and lock totals are gauges despite
their names and therefore never receive a counter rate.

This makes it possible to calculate recent commit, rollback, deadlock, block,
temporary-file, tuple, and checkpoint rates without running a permanent
exporter.

## Prometheus export

Render a stored snapshot using the Prometheus text exposition format:

```sh
opentenbase_observer prometheus snapshots/latest.json \
  > /var/lib/node_exporter/textfile_collector/opentenbase.prom
```

Every collected metric receives `node` and `role` labels.  Failed nodes receive
`opentenbase_up 0`.  Collection duration and finding counts by severity are
added by the tool.

For the node_exporter textfile collector, write to a temporary file and rename
it so Prometheus never reads partial content:

```sh
tmp=/var/lib/node_exporter/textfile_collector/opentenbase.prom.tmp
dst=/var/lib/node_exporter/textfile_collector/opentenbase.prom
opentenbase_observer prometheus snapshots/latest.json > "$tmp" && mv "$tmp" "$dst"
```

Pass `--no-findings` when only raw collected metrics are desired.

## Snapshot safety and portability

Stored snapshots are validated before analysis, diff, or export.  Validation
rejects:

- unknown format versions;
- timestamps without a timezone;
- duplicate node names;
- unknown node roles;
- non-boolean availability values;
- malformed metric names and label names;
- non-string labels;
- boolean, non-numeric, NaN, and infinite metric values.

Snapshots are written to a temporary sibling, flushed, and atomically renamed.
JSON keys and node results are sorted for stable review and archival diffs.

## Security notes

- Configuration never contains a password field.
- Connection values are passed as separate `psql` arguments, not through a
  shell command string.
- `psql -X` ignores user startup scripts.
- `ON_ERROR_STOP` prevents partial query results from looking successful.
- Client and server timeouts bound failed collection.
- Error text is truncated before entering a snapshot.
- No SQL text from configuration is executed.
- Prometheus label values escape backslashes, quotes, and newlines.

Snapshots can include query text, usernames, database names, endpoints, and
operational counters.  Protect them as production diagnostic data.

## Tests

```sh
python3 -m unittest discover \
  -s contrib/opentenbase_observer/tests \
  -v
```

or:

```sh
make -C contrib/opentenbase_observer check
```

The standard-library test suite covers configuration validation, JSON-line
parsing, subprocess isolation, timeout and authentication failures, snapshot
validation, every diagnostic rule and boundary, Prometheus escaping, counter
resets and rates, atomic output, and CLI exit-code policy.
