# OpenTenBase Upgrade / Downgrade Guide

This guide covers version migration between OpenTenBase v2.6.0 and v5.0,
using `pg_dump` and `pg_restore` for data-safe migration.

## Overview

OpenTenBase is PostgreSQL-compatible, so standard PostgreSQL migration
tools work:

| Tool | Purpose |
|------|---------|
| `pg_dump` | Export database schema + data |
| `pg_restore` | Import database from dump |
| `pg_dumpall` | Export entire cluster (globals + all databases) |

The general principle: dump from the source version, restore into the
target version. This works across major version boundaries because the
dump is logical (SQL text), not binary.

## Prerequisites

- Both source and target OpenTenBase clusters running
- Network access between clusters
- `pg_dump` from the TARGET version (newer tool handles older servers)
- Sufficient disk space for dump files (2x database size recommended)

```bash
# Verify both clusters are accessible
psql -h <source_coordinator> -c "SELECT version();"
psql -h <target_coordinator> -c "SELECT version();"
```

## Upgrade: v2.6.0 → v5.0

### Step 1: Dump Globals (roles, tablespaces)

```bash
pg_dumpall -h <source_coordinator> -p <port> -U opentenbase \
  --globals-only --no-role-passwords \
  -f globals.sql
```

### Step 2: Dump Schema Only

```bash
pg_dump -h <source_coordinator> -p <port> -U opentenbase \
  -d <database_name> --schema-only --no-owner --no-acl \
  -f schema.sql
```

### Step 3: Dump Data Only

```bash
pg_dump -h <source_coordinator> -p <port> -U opentenbase \
  -d <database_name> --data-only --no-owner \
  --format=custom -f data.dump
```

### Step 4: Restore Globals to Target

```bash
psql -h <target_coordinator> -p <port> -U opentenbase \
  -d postgres -f globals.sql
```

### Step 5: Create Database on Target

```bash
psql -h <target_coordinator> -p <port> -U opentenbase \
  -c "CREATE DATABASE <database_name>;"
```

### Step 6: Restore Schema

```bash
psql -h <target_coordinator> -p <port> -U opentenbase \
  -d <database_name> -f schema.sql
```

### Step 7: Restore Data

```bash
pg_restore -h <target_coordinator> -p <port> -U opentenbase \
  -d <database_name> --no-owner --no-acl \
  -j 4 data.dump
```

> `-j 4` enables parallel restore using 4 worker processes

### Step 8: Verify

```sql
-- Compare row counts
SELECT schemaname, tablename, n_live_tup
FROM pg_stat_user_tables
ORDER BY schemaname, tablename;
```

## Downgrade: v5.0 → v2.6.0

### Compatibility Notes

v5.0 may introduce SQL features not available in v2.6.0. Before downgrading:

1. Audit schema for v5.0-specific features (new data types, functions)
2. Check distribution key compatibility
3. Export using v2.6.0's `pg_dump` when possible

### Procedure

Same as upgrade, but with additional verification:

```bash
# Use the OLDER pg_dump (v2.6.0) to avoid new-feature issues
/path/to/v2.6.0/bin/pg_dump -h <source_coordinator> \
  -d <database_name> --format=custom -f data.dump
```

### v5.0-Specific Features to Check

| Feature | v2.6.0 Compatible? | Mitigation |
|---------|-------------------|------------|
| New SQL functions | ❌ | Replace with v2.6.0 equivalents |
| Enhanced distribution strategies | ⚠️ | Review DISTRIBUTE BY clauses |
| Extended data types | ❌ | Cast to v2.6.0 types before dump |

## Automated Verification Script

```bash
#!/bin/bash
# verify_migration.sh — compare row counts before and after migration

SOURCE=$1
TARGET=$2
DATABASE=$3

echo "=== Row Count Comparison: $DATABASE ==="
psql -h $SOURCE -d $DATABASE -tAc \
  "SELECT schemaname||'.'||tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')" \
  | while read tbl; do
    src=$(psql -h $SOURCE -d $DATABASE -tAc "SELECT count(*) FROM $tbl")
    tgt=$(psql -h $TARGET -d $DATABASE -tAc "SELECT count(*) FROM $tbl")
    if [ "$src" != "$tgt" ]; then
      echo "MISMATCH: $tbl — source=$src target=$tgt"
    else
      echo "OK: $tbl ($src rows)"
    fi
  done
```

## Rollback Plan

If migration fails:

1. Keep the source cluster running until verification passes
2. Dump from target if partial migration occurred (for analysis)
3. Drop target database and retry from Step 1
4. As last resort, fail over back to source cluster

## Common Issues

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `pg_dump` connection refused | Coordinator not running | `opentenbase_ctl status -c config.ini` |
| Distribution key error on restore | Different node count | Recreate tables with correct DISTRIBUTE BY |
| Out of disk space during dump | Temp files exceed available space | Use `--compress=9` or dump to external storage |
| Permission denied on restore | Missing roles | Run `globals.sql` first, then `GRANT` statements |

## References

- [PostgreSQL pg_dump Documentation](https://www.postgresql.org/docs/current/app-pgdump.html)
- [OpenTenBase README](https://github.com/OpenTenBase/OpenTenBase)
