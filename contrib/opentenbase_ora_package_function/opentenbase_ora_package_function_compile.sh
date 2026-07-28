#!/usr/bin/env bash

set -uo pipefail

fail()
{
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) ||
    fail 'could not resolve the ORA package script directory'
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd) ||
    fail "could not resolve the repository root from: $SCRIPT_DIR"
PACKAGE_DIR=$SCRIPT_DIR
MAKEFILE_GLOBAL=$REPO_ROOT/src/Makefile.global

if [ ! -f "$MAKEFILE_GLOBAL" ] || [ ! -r "$MAKEFILE_GLOBAL" ]; then
    fail "Makefile.global is not a readable file: $MAKEFILE_GLOBAL"
fi

read_make_assignment()
{
    local assignment_name
    local assignment_value
    local status

    assignment_name=$1
    assignment_value=$(awk -v name="$assignment_name" '
        BEGIN {
            in_configured_block = 0
            configured_block_found = 0
            configured_block_closed = 0
            assignment_count = 0
            assignment_value = ""
        }

        !configured_block_found &&
        $0 ~ /^[[:space:]]*ifndef[[:space:]]+PGXS[[:space:]]*$/ {
            configured_block_found = 1
            in_configured_block = 1
            next
        }

        in_configured_block &&
        $0 ~ /^[[:space:]]*else[[:space:]]*#[[:space:]]*PGXS[[:space:]]+case[[:space:]]*$/ {
            configured_block_closed = 1
            in_configured_block = 0
            exit
        }

        in_configured_block {
            assignment_pattern = "^[[:space:]]*" name "[[:space:]]*:="
            if (match($0, assignment_pattern)) {
                value = substr($0, RLENGTH + 1)
                sub(/^[[:space:]]*/, "", value)
                sub(/[[:space:]]*$/, "", value)
                assignment_count++
                assignment_value = value
            }
        }

        END {
            if (!configured_block_found || !configured_block_closed)
                exit 2
            if (assignment_count != 1 || assignment_value == "")
                exit 3
            print assignment_value
        }
    ' "$MAKEFILE_GLOBAL")
    status=$?

    case "$status" in
        0)
            printf '%s\n' "$assignment_value"
            ;;
        2)
            printf 'ERROR: missing or unterminated configured ifndef PGXS block in: %s\n' \
                "$MAKEFILE_GLOBAL" >&2
            return 1
            ;;
        *)
            printf 'ERROR: expected exactly one nonempty %s := assignment in configured Makefile.global block: %s\n' \
                "$assignment_name" "$MAKEFILE_GLOBAL" >&2
            return 1
            ;;
    esac
}

replace_literal()
{
    local remaining
    local token
    local replacement
    local result
    local before

    remaining=$1
    token=$2
    replacement=$3
    result=

    while :; do
        case "$remaining" in
            *"$token"*)
                before=${remaining%%"$token"*}
                result=$result$before$replacement
                remaining=${remaining#*"$token"}
                ;;
            *)
                result=$result$remaining
                break
                ;;
        esac
    done

    printf '%s' "$result"
}

require_safe_absolute_path()
{
    local value_name
    local value

    value_name=$1
    value=$2
    case "$value" in
        ''|*'$'*|*'`'*)
            fail "$value_name must resolve to a safe absolute path: ${value:-<empty>}"
            ;;
        /*)
            ;;
        *)
            fail "$value_name must resolve to a safe absolute path: $value"
            ;;
    esac
}

prefix=$(read_make_assignment prefix) || exit $?
require_safe_absolute_path prefix "$prefix"

exec_prefix=$(read_make_assignment exec_prefix) || exit $?
exec_prefix=$(replace_literal "$exec_prefix" '${prefix}' "$prefix")
exec_prefix=$(replace_literal "$exec_prefix" '$(prefix)' "$prefix")
require_safe_absolute_path exec_prefix "$exec_prefix"

bindir=$(read_make_assignment bindir) || exit $?
bindir=$(replace_literal "$bindir" '${prefix}' "$prefix")
bindir=$(replace_literal "$bindir" '$(prefix)' "$prefix")
bindir=$(replace_literal "$bindir" '${exec_prefix}' "$exec_prefix")
bindir=$(replace_literal "$bindir" '$(exec_prefix)' "$exec_prefix")
require_safe_absolute_path bindir "$bindir"

if [ "${PG_CONFIG+x}" = x ]; then
    pg_config=$PG_CONFIG
    if [ -z "$pg_config" ] || [ "${pg_config#/}" = "$pg_config" ] ||
       [ ! -f "$pg_config" ] || [ ! -x "$pg_config" ]; then
        fail "PG_CONFIG must be a nonempty absolute executable regular file: ${pg_config:-<empty>}"
    fi
else
    pg_config=$bindir/pg_config
    if [ ! -f "$pg_config" ] || [ ! -x "$pg_config" ]; then
        fail "derived pg_config must be an executable regular file: $pg_config"
    fi
fi

is_positive_integer()
{
    case "$1" in
        ''|0*|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

if [ "${MAKE_JOBS+x}" = x ]; then
    if ! is_positive_integer "$MAKE_JOBS"; then
        fail "MAKE_JOBS must be a positive integer: ${MAKE_JOBS:-<empty>}"
    fi
    make_jobs=$MAKE_JOBS
else
    make_jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null)
    getconf_status=$?
    if [ "$getconf_status" -ne 0 ] || ! is_positive_integer "$make_jobs"; then
        make_jobs=1
    fi
fi

run_make_stage()
{
    local stage
    local status

    stage=$1
    shift

    if make -C "$PACKAGE_DIR" "PG_CONFIG=$pg_config" "$@"; then
        printf '=== opentenbase_ora_package_function_compile %s finished ===\n' \
            "$stage"
    else
        status=$?
        printf '=== opentenbase_ora_package_function_compile %s error(%d) ===\n' \
            "$stage" "$status" >&2
        return "$status"
    fi
}

run_make_stage clean clean || exit $?
run_make_stage make "-j$make_jobs" || exit $?
run_make_stage 'make install' install "-j$make_jobs" || exit $?
printf '%s\n' \
    '=== opentenbase_ora_package_function_compile compile is success ==='
