# otb-health — one-shot health check for an OpenTenBase cluster

`otb-health.sh` runs a complete "is this cluster actually usable?" check in one
command, instead of making you remember four separate ones.

It answers the four questions that actually matter after a deploy:

1. Are the GTM / Coordinator / Datanode processes running?
2. Are the nodes registered in `pgxc_node` (not just running)?
3. Does a node group exist so distributed DDL can be routed?
4. Can you create a sharded table, insert, and read it back?

## Why this exists

During a from-source deployment of OpenTenBase v5.0 on Ubuntu 24.04, every one of
those four things failed at least once *while the processes looked healthy*:

- `ps aux` showed all three nodes up, but `pgxc_node` was missing `dn001`, so
  every distributed `CREATE TABLE` failed with `default group not defined`.
- `pgxc_ctl monitor all` printed `Running` for everything, but the Coordinator
  had silently skipped node registration during `init all` because of a broken
  `libpq` — so the cluster was "up" and unusable at the same time.

Checking only the process list is not enough. Hence this script.

## Usage

```bash
bash otb_health.sh              # full check, includes a DDL/DML smoke test
bash otb_health.sh --no-write   # read-only: skip the create/insert/drop test
```

Defaults (override by exporting the variable):

| Variable      | Default                          |
| ------------- | -------------------------------- |
| `PG_HOME`     | `/usr/local/opentenbase`         |
| `CN_PORT`     | `11003`                          |
| `DB_USER`     | `opentenbase`                    |
| `DB_NAME`     | `postgres`                       |
| `PGXC_CONF`   | `$HOME/pgxc_ctl/pgxc_ctl.conf`   |

## What "healthy" looks like

- Section [1]: every node reports `Running`.
- Section [2]: three rows — `gtm`, `cn001`, `dn001` — each with the correct host
  and port. A row of `localhost:5432` means node registration never ran.
- Section [3]: at least one group (normally `default_group`).
- Section [4]: the table is created, two rows are returned, then dropped.

If [2] is empty or shows `localhost:5432`, the cluster was initialized with a
broken client library; see the note below.

## Exit codes

`otb_health.sh` is safe to use from cron, CI, or a Kubernetes probe:

| Code | Meaning |
| ---- | ------- |
| `0`  | Every check that has failure detection ran and succeeded. |
| `1`  | A psql connection error, a SQL execution error, or one of the checked failures was detected. |

The summary line printed at the end depends on the mode, and is emitted only on
the `0` path:

| Mode | Final line |
| ---- | ---------- |
| write | `PASS: read-only catalog checks (pgxc_node, pgxc_group) and the DDL/DML smoke test succeeded.` |
| `--no-write` | `PASS: read-only catalog checks (pgxc_node, pgxc_group) succeeded; write capability NOT verified (--no-write).` |

Every failure prints `FAIL: stage=<name> psql_exit=<status>` on stderr and exits
`1`; `<status>` is the status of that `psql` invocation, so the failure can be
classified without re-running the check — `2` bad connection, `3` SQL error
under `ON_ERROR_STOP`, `127` `psql` not found. The stage names are
`node-registration`, `node-groups`, and `ddl-dml`.

SQL statements are run with `ON_ERROR_STOP=1` (and `-X`, so a `psqlrc` cannot
turn it back off), so a failing statement inside the DDL/DML block is reported
through the exit code instead of being swallowed.

### What the exit code does *not* cover yet

- Section [1] only **displays** `pgxc_ctl monitor all`; a node that reports
  `not running` / `Stopped` does not fail the run on its own.
- Section [3] fails when the `pgxc_group` query errors, but an **empty** result
  (zero rows) is still reported as green.
- Node counts, group counts, and replica/HA state are not validated.

Treat a `0` as "these four checks passed", not as "every possible cluster
fault is ruled out".

## Data safety: the fixed test table

The write test is **not read-only**. It uses a fixed table name in whatever
database `DB_NAME` selects:

```sql
drop table if exists otb_health_check;
create table otb_health_check (…) distribute by shard(id) to group default_group;
insert into otb_health_check values (1,'ok'),(2,'good');
select * from otb_health_check order by id;
drop table otb_health_check;
```

Plan for these consequences before running it:

- **The leading `DROP TABLE IF EXISTS` is destructive.** If the database already
  contains a user table named `otb_health_check`, it is dropped before the
  smoke test starts — silently and without a backup.
- **Point `DB_NAME` at a dedicated test database.** Do not run the write test
  against a database that holds production or business data.
- **On production, use `--no-write`.** That path skips the whole
  create/insert/select/drop sequence and only runs the two read-only catalog
  queries.
- **A failed run can leave the table behind.** If a statement fails, the
  connection drops, or the process is killed between `CREATE` and `DROP`, the
  table stays in the database. Check for it and remove it manually with
  `drop table if exists otb_health_check;` (it may be a distributed table).

## Tests

`t/test_exit_codes.sh` is a behavioural suite: it runs the health check against
mock `psql` / `pgxc_ctl` commands injected through `PATH`, so no cluster,
container, or network is needed.

```bash
bash t/test_exit_codes.sh

# reverse check: run the same suite against an older/other revision
TARGET=/path/to/other/otb_health.sh bash t/test_exit_codes.sh
```

What it pins down:

- exit code and `PASS:` summary for every path (exactly one summary on success,
  none on any failure), including that the `--no-write` summary says the write
  capability was **not** verified;
- the reported `psql_exit=<status>` per stage, so a swallowed failure is caught;
- **psql call counts**, so a broken fail-fast — executing later SQL after an
  earlier check already failed — is detected;
- **`-X`**: a `psqlrc` containing `\set ON_ERROR_STOP off` must not be able to
  silence SQL errors again;
- missing `psql`, missing `PGXC_CONF`, and the `--no-write` skip path.

The mock's psql semantics (a script error returns `0` without `ON_ERROR_STOP`)
are asserted directly, so dropping the flag or `-X` fails the suite. This is
still **not** a substitute for a real database integration test.

## Deployment notes that cost real debugging time

These are the failures hit while deploying v5.0 from source on Ubuntu 24.04.
They are listed here because none of them produce an obvious error message at
the point where they happen.

1. **`libpq.so.5` must point at `libpq.so.5.10`, not `5.13`.** A wrong symlink
   breaks `psql` at connect time with `undefined symbol: PQSqlMode`, crashes the
   Coordinator's pooler on `PQconnectdbParallel`, and — worst of all — makes
   `pgxc_ctl init all` skip node registration *silently*. Fix the symlink, then
   `clean all` + `init all` again.
2. **Put `PATH`/`LD_LIBRARY_PATH` at the *top* of `~/.bashrc`.** `pgxc_ctl` runs
   its commands over non-interactive ssh; Ubuntu's default `.bashrc` returns
   early for non-interactive shells, so exports appended at the end never load
   and you get `initdb: command not found`.
3. **v5 requires `coordForwardPorts` / `datanodeForwardPorts`.** Older (v2-era)
   config templates omit them and `pgxc_ctl` refuses to start with a
   "count mismatch" error.
4. **A failed `init all` leaves non-empty data directories**, and the next
   `init all` then prints `Skip initialization` and a misleading `Done`. Run
   `clean all` first.
5. **The cluster does not start on boot.** After a reboot, run
   `pgxc_ctl -c ~/pgxc_ctl/pgxc_ctl.conf` and issue `start all`.
6. **Ubuntu 24.04 has no `libldap_r-2.4.so.2`.** Symlink the 2.5 library if the
   build pulled in LDAP.
7. **After a fresh `init all`, create the node group** before any distributed
   table; otherwise DDL fails with `default group not defined`.

`pgxc_ctl` can also be driven non-interactively, which is handy for scripts and
for remote automation:

```bash
printf 'start all\nmonitor all\n' | pgxc_ctl -c ~/pgxc_ctl/pgxc_ctl.conf
```

## Scope

Verified on a single-machine minimal cluster (1 GTM + 1 CN + 1 DN),
OpenTenBase v5.0, Ubuntu 24.04. Multi-node and HA topologies are untested here.
