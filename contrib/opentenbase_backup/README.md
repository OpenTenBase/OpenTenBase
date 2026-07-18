<!--
Copyright (c) 2026 OpenTenBase Contributors

This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
in the repository root for details.
-->

# OpenTenBase backup manifest and incremental restore tool

`opentenbase_backup` makes filesystem backups easier to trust and cheaper to
retain.  It scans a completed OpenTenBase backup, records a SHA-256 manifest,
materializes changed-file-only delta layers, verifies the complete parent
chain, and restores the effective snapshot with atomic file replacement.

The tool is deliberately separate from live backup orchestration.  Use
`pg_basebackup`, `pg_dump`, `opentenbase_ctl`, or an existing coordinated node
backup procedure to produce a consistent directory first.  Run this tool only
after writers have stopped modifying that directory.

## Capabilities

- Parallel SHA-256 hashing with change-during-scan detection.
- Portable POSIX manifest paths on both Linux and Windows.
- Full manifests and incremental parent comparisons.
- Added, changed, unchanged, and deleted file states.
- Changed-file-only delta directories with deletion tombstones.
- Parent manifest digest pinning and cycle/depth protection.
- Optional HMAC-SHA256 manifest authentication.
- End-to-end verification before restore.
- Strict verification mode for unexpected files.
- Atomic manifest writes and atomic restored-file replacement.
- Symlink, traversal, duplicate path, special-file, and malformed JSON checks.
- WAL, configuration, and data file classification in summary reports.
- JSON output suitable for automation and audit logs.
- Standard-library-only Python implementation.

## Install

The tool participates in the top-level `contrib` install and is copied to
`$(bindir)/opentenbase_backup`:

```sh
./configure --prefix=/opt/opentenbase
make -C contrib/opentenbase_backup
make -C contrib/opentenbase_backup install
```

It can also run directly from the source tree:

```sh
python3 contrib/opentenbase_backup/opentenbase_backup.py --help
```

Python 3.8 or newer is required.  There are no third-party runtime packages.

## Create a full manifest

Assume `/backup/full-001` is a completed, quiesced backup:

```sh
opentenbase_backup manifest /backup/full-001 \
  --output /backup/manifests/full-001.json \
  --jobs 8
```

The output manifest contains:

- format and version identifiers;
- creation time and content hash algorithm;
- a path to the layer's payload directory;
- sorted file records with size, mtime, mode, SHA-256, kind, and state;
- counts and bytes by incremental state;
- counts by data, WAL, and configuration kind.

The output path may be inside the backup root, but keeping manifests in a
separate protected directory makes retention and authentication simpler.  If
the output is inside the root, the tool excludes the manifest and its temporary
write files from the scan automatically.

Use repeatable relative glob exclusions for known non-backup artifacts:

```sh
opentenbase_backup manifest /backup/full-001 \
  --output /backup/manifests/full-001.json \
  --exclude 'logs/*' \
  --exclude '*.pid'
```

Exclusions are case-sensitive and apply to normalized relative paths.

## Authenticate a manifest

Content hashes detect backup corruption.  An HMAC additionally detects an
attacker changing both a file and its recorded digest.  Store the key outside
the backup and restrict its permissions:

```sh
umask 077
openssl rand 32 > /secure/opentenbase-backup.hmac

opentenbase_backup manifest /backup/full-001 \
  --output /backup/manifests/full-001.json \
  --hmac-key-file /secure/opentenbase-backup.hmac
```

Every later command that reads a signed manifest must receive the same key.
The key bytes are never written to the manifest or standard output.

## Create an incremental delta

Produce a new complete consistent backup directory, then compare it with the
previous manifest:

```sh
opentenbase_backup delta /backup/full-002 \
  --parent /backup/manifests/full-001.json \
  --output-directory /backup/deltas/delta-002 \
  --jobs 8
```

The delta directory contains:

```text
delta-002/
├── opentenbase-backup-manifest.json
└── payload/
    └── changed and added files only
```

The manifest still describes the complete current file set.  Unchanged records
retain their digest and point logically to an earlier layer.  Deleted records
are stored as tombstones.  The `payload` directory contains only added and
changed files, so its retained size represents the physical incremental cost.

Create later deltas by using the newest delta manifest as the parent:

```sh
opentenbase_backup delta /backup/full-003 \
  --parent /backup/deltas/delta-002/opentenbase-backup-manifest.json \
  --output-directory /backup/deltas/delta-003 \
  --jobs 8
```

Do not delete a base or intermediate delta while a retained child references
it.  `inspect` prints the complete dependency chain.

## Verify before retention or restore

Verify the newest manifest and every parent layer:

```sh
opentenbase_backup verify \
  /backup/deltas/delta-003/opentenbase-backup-manifest.json \
  --jobs 8
```

Successful output:

```json
{
  "issues": [],
  "ok": true
}
```

A mismatch exits with status `2` and returns structured issues.  Examples of
issue codes are `missing`, `size`, `sha256`, `type`, and `extra`.  A malformed
manifest, missing key, parent digest mismatch, or filesystem error exits with
status `1` and a concise message on standard error.

Use `--strict` to report files physically present in a layer but absent from
its manifest:

```sh
opentenbase_backup verify /backup/manifests/full-001.json --strict
```

`--no-chain` limits payload verification to the newest layer.  Parent links
and the parent digest are still parsed so an invalid chain is never silently
treated as valid.  Full-chain verification is the safe default.

## Inspect retention dependencies

```sh
opentenbase_backup inspect \
  /backup/deltas/delta-003/opentenbase-backup-manifest.json
```

The JSON report lists each manifest, its payload directory, creation time,
summary, and the effective final file and byte totals.  Backup retention jobs
can use this output to avoid removing a referenced layer.

## Restore

Restore the final logical snapshot from the complete chain:

```sh
opentenbase_backup restore \
  /backup/deltas/delta-003/opentenbase-backup-manifest.json \
  /restore/opentenbase-data \
  --jobs 8
```

The restore algorithm performs these steps:

1. Load every manifest from newest to base.
2. Reject cycles, excessive depth, malformed records, and parent digest drift.
3. Authenticate each signed manifest.
4. Verify each physical payload file by size and SHA-256.
5. Merge added, changed, unchanged, and deleted records into an effective plan.
6. Copy files to temporary siblings in the destination.
7. Apply recorded permission bits where supported.
8. Atomically replace each final path.
9. Hash every restored file again before reporting success.

Existing destination files cause failure.  Use `--overwrite` only when the
destination is intentionally disposable:

```sh
opentenbase_backup restore newest.json /restore/data --overwrite
```

`--skip-verify` skips the separate preflight pass, but each restored file is
still hashed after copy.  Skipping preflight can leave a partially materialized
destination when a later source file is corrupt, so it is intended only for
specialized workflows that already performed and recorded verification.

The restored directory is a filesystem snapshot.  Starting a database from it
still requires the same recovery configuration, WAL availability, ownership,
and cluster coordination as the original backup method.

## Safety model

Manifest paths are always relative.  The loader rejects:

- absolute POSIX paths;
- Windows drive-qualified paths;
- `.` and `..` components;
- empty paths and NUL bytes;
- duplicate or unsorted records;
- invalid states, kinds, sizes, modes, and digests;
- symlinks by default;
- non-regular filesystem entries;
- parent cycles and chains longer than 128 layers.

`--follow-symlinks` is available for controlled source layouts.  Directory
links must still resolve below the scanned root.  Restore destinations never
accept manifest traversal.

The scanner checks size and nanosecond mtime before and after hashing.  If a
file changes, manifest creation fails instead of recording an inconsistent
snapshot.  This is an additional guard, not a substitute for obtaining a
database-consistent backup first.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Command completed; verification found no issues. |
| `1` | Usage, manifest, authentication, chain, or filesystem error. |
| `2` | Verification completed and found content issues. |

## Tests

Run the standard-library unit suite:

```sh
python3 -m unittest discover \
  -s contrib/opentenbase_backup/tests \
  -v
```

Or use the contrib target:

```sh
make -C contrib/opentenbase_backup check
```

The suite covers path traversal, record validation, hashing, exclusions,
symlink policy, full manifests, HMAC authentication, corruption detection,
strict verification, all incremental states, deletion tombstones, multi-layer
restore, overwrite policy, atomic writes, and CLI exit codes.
