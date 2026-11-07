#!/bin/bash
# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 OpenTenBase Authors

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
workdir=$(mktemp -d)
cleanup() {
    rm -f -- "$workdir/bin/psql" "$workdir/calls" "$workdir/smoke.sql" "$workdir/output"
    rmdir -- "$workdir/bin" "$workdir"
}
trap cleanup EXIT
mkdir "$workdir/bin"

cat > "$workdir/bin/psql" <<'MOCK'
#!/bin/bash
set -euo pipefail
stage=smoke
stop_on_error=false
no_psqlrc=false
health_table=
for arg in "$@"; do
    case "$arg" in
        *'from pgxc_node'*) stage=nodes ;;
        *'from pgxc_group'*) stage=groups ;;
        ON_ERROR_STOP=1) stop_on_error=true ;;
        -X) no_psqlrc=true ;;
        health_table=*) health_table=${arg#*=} ;;
    esac
done
printf '%s\n' "$stage" >> "$FIXTURE/calls"

if [[ $stage == smoke ]]; then
    cat > "$FIXTURE/smoke.sql"
fi

if [[ $stage == "$FAIL_STAGE" ]]; then
    echo "ERROR: mock $stage failure" >&2
    if [[ $stage != smoke ]] || "$stop_on_error"; then
        exit 3
    fi
elif [[ $stage == smoke ]]; then
    "$stop_on_error" && "$no_psqlrc" && [[ $health_table =~ ^otb_health_check_[0-9]+_[0-9]+$ ]] || exit 2
fi
MOCK
chmod +x "$workdir/bin/psql"

failures=0
run_case() {
    local name=$1 fail_stage=$2 expected_status=$3 expected_calls=$4 message=$5
    shift 5
    local status=0
    : > "$workdir/calls"
    : > "$workdir/smoke.sql"
    PG_HOME="$workdir" PGXC_CONF="$workdir/missing.conf" FIXTURE="$workdir" \
        FAIL_STAGE="$fail_stage" bash "$script_dir/otb_health.sh" "$@" \
        > "$workdir/output" 2>&1 || status=$?
    if [[ $status == "$expected_status" && $(cat "$workdir/calls") == "$expected_calls" ]] &&
        grep -Fq -- "$message" "$workdir/output" &&
        { [[ $expected_status == 0 ]] || ! grep -Fq 'Requested SQL checks passed' "$workdir/output"; }; then
        printf 'PASS: %s\n' "$name"
    else
        printf 'FAIL: %s (exit %s, expected %s)\n' "$name" "$status" "$expected_status" >&2
        cat "$workdir/output" >&2
        failures=$((failures + 1))
    fi
}

run_case 'node registration failure stops the check' nodes 1 nodes 'node registration query failed'
run_case 'node group failure stops before smoke test' groups 1 $'nodes\ngroups' 'node group query failed'
run_case 'read-only mode also detects group failure' groups 1 $'nodes\ngroups' 'node group query failed' --no-write
run_case 'failed smoke SQL returns nonzero' smoke 1 $'nodes\ngroups\nsmoke' 'distributed DDL/DML smoke test failed'
run_case 'read-only mode skips all smoke SQL' none 0 $'nodes\ngroups' '(skipped: --no-write)' --no-write
run_case 'full check reports SQL success' none 0 $'nodes\ngroups\nsmoke' 'Requested SQL checks passed'

if grep -Fxq 'begin;' "$workdir/smoke.sql" &&
    grep -Fxq 'create table :"health_table"(id int, name text)' "$workdir/smoke.sql" &&
    grep -Fxq 'rollback;' "$workdir/smoke.sql" &&
    ! grep -iq 'drop table' "$workdir/smoke.sql"; then
    echo 'PASS: smoke SQL uses an isolated table name and rolls back'
else
    echo 'FAIL: smoke SQL must create the per-run table and roll back without DROP' >&2
    failures=$((failures + 1))
fi

[[ $failures == 0 ]]
