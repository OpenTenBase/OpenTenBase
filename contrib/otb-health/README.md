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

## Exit codes

- `0` — all required checks passed. Sections skipped on purpose (`--no-write`,
  missing `PGXC_CONF`) are marked in the output and are not counted as checks.
- `1` — at least one required check failed: psql unreachable **or** a SQL
  statement failed. The psql invocations use `-v ON_ERROR_STOP=1`, without
  which psql exits 0 even when statements fail; the success banner is only
  printed after every required check has passed.

Note: the smoke test writes a fixed table `otb_health_check` in the target
database (`DROP TABLE IF EXISTS` first, `DROP TABLE` last), so point
`DB_NAME` at a dedicated test database, not at one holding production data.

## Tests

`tests/test_otb_health.sh` is a self-contained black-box contract test suite;
it needs no running cluster. A stub `psql` is installed into a temporary
`PG_HOME`, and it only simulates a SQL failure when it sees
`-v ON_ERROR_STOP=1`, so a regression that drops that flag fails the suite.

```bash
bash tests/test_otb_health.sh
```

## What "healthy" looks like

- Section [1]: every node reports `Running`.
- Section [2]: three rows — `gtm`, `cn001`, `dn001` — each with the correct host
  and port. A row of `localhost:5432` means node registration never ran.
- Section [3]: at least one group (normally `default_group`).
- Section [4]: the table is created, two rows are returned, then dropped.

If [2] is empty or shows `localhost:5432`, the cluster was initialized with a
broken client library; see the note below.

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
