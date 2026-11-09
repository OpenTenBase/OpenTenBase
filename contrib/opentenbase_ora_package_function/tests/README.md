<!--
Copyright (c) 2026 OpenTenBase Authors
Licensed under the BSD 3-Clause License. See LICENSE.txt in the repository root.
-->

# Compile wrapper regression tests

Run from the repository root with Python 3.10+ and Bash available.

```bash
python3 contrib/opentenbase_ora_package_function/tests/test_compile_script.py
```

Set `BASH` to a Bash executable if it is outside `PATH`. Each test creates a
temporary source-tree fixture and supplies simulated `make` and `getconf`
commands. The make replacement checks the working directory, records arguments
and invocation order, and can fail a selected stage with a chosen exit status.

The suite checks the three invocation directories, a source path containing
spaces, missing configuration, each failing make stage, and CPU-count fallback.
It also checks that the wrapper delegates configured installation directories
to the existing in-tree Makefile instead of reconstructing `PG_CONFIG` from
saved configure arguments.

## Increment over PR #273

Reference revision is `1f41379b6f27fd8f91009ca4704156c68c275dce` from
[PR #273](https://github.com/OpenTenBase/OpenTenBase/pull/273).
The script-relative working directory, readable-configuration check, sequential
clean/build/install stages, and original failing exit status come from that PR.

This supplement removes the configure-argument parser and validates the CPU
count. The extension's default in-tree Makefile already includes
`src/Makefile.global`, which defines `prefix`, `exec_prefix`, and `bindir`.
No explicit `--prefix` argument or installed `pg_config` is required by this
wrapper to select those directories.

| Test boundary | PR #273 reference | Supplement |
| --- | --- | --- |
| Explicit prefix, repository/contrib/unrelated working directory | Pass | Pass |
| No explicit prefix in saved configure arguments | Rejects before make | Pass |
| Separate option and value, `--prefix /opt/otb` | Rejects before make | Pass |
| Custom exec-prefix/bindir | Injects prefix/bin/pg_config | Leaves directory configuration to make |
| Quoted prefix containing a space | Truncates injected PG_CONFIG path | Leaves saved arguments untouched |
| getconf succeeds with empty output | Passes bare `-j` | Uses `-j1` |
| getconf succeeds with zero/non-numeric output | Passes `-j0`/`-junknown` | Uses `-j1` |
| getconf fails | Uses one job | Uses one job |
| Missing Makefile.global | Stops before make | Stops before make |
| clean/build/install failure | Preserves status and stops | Preserves status and stops |

The same 16 tests were run on both revisions using Windows, Git Bash 4.4.23,
and Python 3.12.7. The reference passed 9 tests and failed 7. The supplement
passed all 16. Two failures in the reference are argument-delegation assertions
for custom/quoted paths. Those assertions describe wrapper behavior only.
They do not demonstrate an extension compilation failure in those configurations.
The three invalid-CPU cases are defensive fault injection.

To compare another copy of the script with the same tests, set `COMPILE_SCRIPT`.

```bash
COMPILE_SCRIPT=/path/to/reference.sh \
  python3 contrib/opentenbase_ora_package_function/tests/test_compile_script.py
```

The suite checks control flow and command arguments. Actual compilation,
installation, PGXS/VPATH builds, and database execution require separate
integration validation. In particular, the quoted-path fixture establishes
that this wrapper avoids truncation. Complete build-system support for paths
containing spaces is outside the evidence collected here.
