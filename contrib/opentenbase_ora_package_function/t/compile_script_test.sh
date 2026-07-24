#!/usr/bin/env bash

set -u

TEST_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PACKAGE_SOURCE_DIR=$(CDPATH= cd -- "$TEST_DIR/.." && pwd)
PRODUCTION_SCRIPT=$PACKAGE_SOURCE_DIR/opentenbase_ora_package_function_compile.sh
ORIGINAL_PATH=$PATH

TEMP_BASE=${TMPDIR:-/tmp}
TEMP_BASE=${TEMP_BASE%/}
FIXTURE_ROOT=$(mktemp -d "$TEMP_BASE/ora compile test.XXXXXX") || exit 1
REPO_DIR=$FIXTURE_ROOT/repo\ with\ spaces
PACKAGE_DIR=$REPO_DIR/contrib/opentenbase_ora_package_function
MAKEFILE_GLOBAL=$REPO_DIR/src/Makefile.global
INSTALL_PREFIX=$FIXTURE_ROOT/install\ prefix
DEFAULT_PG_CONFIG=$INSTALL_PREFIX/bin/pg_config
EXPLICIT_PG_CONFIG=$FIXTURE_ROOT/explicit\ tools/pg_config
FAKE_BIN=$FIXTURE_ROOT/fake\ tools
MAKE_LOG=$FIXTURE_ROOT/make.log
MAKE_COUNT_FILE=$FIXTURE_ROOT/make.count
GETCONF_LOG=$FIXTURE_ROOT/getconf.log
STDOUT_FILE=$FIXTURE_ROOT/stdout
STDERR_FILE=$FIXTURE_ROOT/stderr
UNRELATED_DIR=$FIXTURE_ROOT/unrelated\ caller
ASSERTION_COUNT=0
CURRENT_CASE=
CASE_STATUS=0
SCRIPT_INVOCATION=

cleanup()
{
    rm -rf -- "$FIXTURE_ROOT"
}
trap cleanup EXIT HUP INT TERM

fail_test()
{
    printf 'FAIL: %s: %s\n' "$CURRENT_CASE" "$1" >&2
    printf '%s\n' '--- stdout ---' >&2
    [ ! -f "$STDOUT_FILE" ] || cat "$STDOUT_FILE" >&2
    printf '%s\n' '--- stderr ---' >&2
    [ ! -f "$STDERR_FILE" ] || cat "$STDERR_FILE" >&2
    printf '%s\n' '--- make log ---' >&2
    [ ! -f "$MAKE_LOG" ] || cat "$MAKE_LOG" >&2
    exit 1
}

pass_assertion()
{
    ASSERTION_COUNT=$((ASSERTION_COUNT + 1))
}

assert_status()
{
    expected=$1
    if [ "$CASE_STATUS" -ne "$expected" ]; then
        fail_test "expected status $expected, got $CASE_STATUS"
    fi
    pass_assertion
}

assert_nonzero_status()
{
    if [ "$CASE_STATUS" -eq 0 ]; then
        fail_test 'expected a nonzero status'
    fi
    pass_assertion
}

assert_file_contains()
{
    file=$1
    expected=$2
    if ! grep -F -- "$expected" "$file" >/dev/null 2>&1; then
        fail_test "expected $file to contain: $expected"
    fi
    pass_assertion
}

assert_file_not_contains()
{
    file=$1
    unexpected=$2
    if grep -F -- "$unexpected" "$file" >/dev/null 2>&1; then
        fail_test "expected $file not to contain: $unexpected"
    fi
    pass_assertion
}

assert_file_line_count()
{
    file=$1
    expected_line=$2
    expected_count=$3
    actual_count=$(grep -Fxc -- "$expected_line" "$file" 2>/dev/null || :)
    if [ "$actual_count" != "$expected_count" ]; then
        fail_test "expected $file to contain line <$expected_line> $expected_count time(s), got $actual_count"
    fi
    pass_assertion
}

assert_file_matching_line_count()
{
    file=$1
    expected_text=$2
    expected_count=$3
    actual_count=$(grep -Fc -- "$expected_text" "$file" 2>/dev/null || :)
    if [ "$actual_count" != "$expected_count" ]; then
        fail_test "expected $file to contain $expected_count line(s) matching <$expected_text>, got $actual_count"
    fi
    pass_assertion
}

assert_file_empty()
{
    file=$1
    if [ -s "$file" ]; then
        fail_test "expected $file to be empty"
    fi
    pass_assertion
}

assert_file_absent()
{
    file=$1
    if [ -e "$file" ]; then
        fail_test "expected $file not to exist"
    fi
    pass_assertion
}

assert_success_banners()
{
    assert_file_contains "$STDOUT_FILE" 'compile clean finished'
    assert_file_contains "$STDOUT_FILE" 'compile make finished'
    assert_file_contains "$STDOUT_FILE" 'compile make install finished'
    assert_file_contains "$STDOUT_FILE" 'compile is success'
}

assert_success_banners_once()
{
    assert_file_line_count "$STDOUT_FILE" \
        '=== opentenbase_ora_package_function_compile clean finished ===' 1
    assert_file_line_count "$STDOUT_FILE" \
        '=== opentenbase_ora_package_function_compile make finished ===' 1
    assert_file_line_count "$STDOUT_FILE" \
        '=== opentenbase_ora_package_function_compile make install finished ===' 1
    assert_file_matching_line_count "$STDOUT_FILE" \
        'opentenbase_ora_package_function_compile compile is success' 1
}

assert_no_success_banners()
{
    assert_file_not_contains "$STDOUT_FILE" 'compile clean finished'
    assert_file_not_contains "$STDOUT_FILE" 'compile make finished'
    assert_file_not_contains "$STDOUT_FILE" 'compile make install finished'
    assert_file_not_contains "$STDOUT_FILE" 'compile is success'
    assert_file_not_contains "$STDERR_FILE" 'compile clean finished'
    assert_file_not_contains "$STDERR_FILE" 'compile make finished'
    assert_file_not_contains "$STDERR_FILE" 'compile make install finished'
    assert_file_not_contains "$STDERR_FILE" 'compile is success'
}

assert_prerequisite_failure()
{
    diagnostic=$1
    assert_nonzero_status
    assert_file_contains "$STDERR_FILE" "$diagnostic"
    assert_file_empty "$MAKE_LOG"
    assert_no_success_banners
}

assert_make_log()
{
    jobs=$1
    pg_config=$2
    expected=$(printf '%s\n' \
        'CALL' \
        'ARG=-C' \
        "ARG=$PACKAGE_DIR" \
        "ARG=PG_CONFIG=$pg_config" \
        'ARG=clean' \
        'CALL' \
        'ARG=-C' \
        "ARG=$PACKAGE_DIR" \
        "ARG=PG_CONFIG=$pg_config" \
        "ARG=-j$jobs" \
        'CALL' \
        'ARG=-C' \
        "ARG=$PACKAGE_DIR" \
        "ARG=PG_CONFIG=$pg_config" \
        'ARG=install' \
        "ARG=-j$jobs")
    actual=$(cat "$MAKE_LOG")
    if [ "$actual" != "$expected" ]; then
        fail_test 'make arguments or stage order did not match'
    fi
    pass_assertion
}

assert_make_call_count()
{
    expected=$1
    actual=$(cat "$MAKE_COUNT_FILE")
    if [ "$actual" != "$expected" ]; then
        fail_test "expected $expected make calls, got $actual"
    fi
    pass_assertion
}

assert_make_log_through_call()
{
    failing_call=$1
    expected=$(printf '%s\n' \
        'CALL' \
        'ARG=-C' \
        "ARG=$PACKAGE_DIR" \
        "ARG=PG_CONFIG=$DEFAULT_PG_CONFIG" \
        'ARG=clean')

    if [ "$failing_call" -ge 2 ]; then
        expected=$(printf '%s\n' "$expected" \
            'CALL' \
            'ARG=-C' \
            "ARG=$PACKAGE_DIR" \
            "ARG=PG_CONFIG=$DEFAULT_PG_CONFIG" \
            'ARG=-j3')
    fi
    if [ "$failing_call" -ge 3 ]; then
        expected=$(printf '%s\n' "$expected" \
            'CALL' \
            'ARG=-C' \
            "ARG=$PACKAGE_DIR" \
            "ARG=PG_CONFIG=$DEFAULT_PG_CONFIG" \
            'ARG=install' \
            'ARG=-j3')
    fi

    actual=$(cat "$MAKE_LOG")
    if [ "$actual" != "$expected" ]; then
        fail_test "make did not stop after failing call $failing_call"
    fi
    pass_assertion
}

assert_getconf_called_once()
{
    expected=$(printf '%s\n' 'CALL' 'ARG=_NPROCESSORS_ONLN')
    actual=$(cat "$GETCONF_LOG")
    if [ "$actual" != "$expected" ]; then
        fail_test 'getconf was not called exactly once with _NPROCESSORS_ONLN'
    fi
    pass_assertion
}

write_makefile()
{
    prefix_line=$1
    exec_prefix_line=$2
    bindir_line=$3
    extra_line=${4-}

    {
        printf '%s\n' 'ifndef PGXS'
        printf '%s\n' "$prefix_line"
        printf '%s\n' "$exec_prefix_line"
        printf '%s\n' "$bindir_line"
        [ -z "$extra_line" ] || printf '%s\n' "$extra_line"
        printf '%s\n' 'else # PGXS case'
        printf '%s\n' 'bindir := $(shell fake-pg-config --bindir)'
        printf '%s\n' 'endif # PGXS'
    } >"$MAKEFILE_GLOBAL"
}

write_default_makefile()
{
    write_makefile \
        "prefix := $INSTALL_PREFIX" \
        'exec_prefix := ${prefix}' \
        'bindir := ${exec_prefix}/bin'
}

reset_fixture()
{
    mkdir -p -- "$PACKAGE_DIR" "$REPO_DIR/src" "$INSTALL_PREFIX/bin" \
        "$(dirname -- "$EXPLICIT_PG_CONFIG")" "$FAKE_BIN" "$UNRELATED_DIR"
    cp -- "$PRODUCTION_SCRIPT" "$PACKAGE_DIR/opentenbase_ora_package_function_compile.sh"
    write_default_makefile
    printf '%s\n' '#!/usr/bin/env bash' 'exit 0' >"$DEFAULT_PG_CONFIG"
    printf '%s\n' '#!/usr/bin/env bash' 'exit 0' >"$EXPLICIT_PG_CONFIG"
    chmod 755 "$DEFAULT_PG_CONFIG" "$EXPLICIT_PG_CONFIG"
}

reset_case_state()
{
    : >"$MAKE_LOG"
    : >"$GETCONF_LOG"
    printf '%s\n' 0 >"$MAKE_COUNT_FILE"
    : >"$STDOUT_FILE"
    : >"$STDERR_FILE"
}

run_script()
{
    CURRENT_CASE=$1
    caller_dir=$2
    shift 2
    reset_case_state

    (
        env_name=
        while IFS='=' read -r env_name ignored_value; do
            case "$env_name" in
                FAKE_*) unset "$env_name" ;;
            esac
        done < <(env)
        unset CDPATH PG_CONFIG MAKE_JOBS

        export PATH="$FAKE_BIN:$ORIGINAL_PATH"
        export MAKE_LOG MAKE_COUNT_FILE GETCONF_LOG
        export FAKE_GETCONF_OUTPUT=3
        export FAKE_GETCONF_STATUS=0
        while [ "$#" -gt 0 ]; do
            export "$1"
            shift
        done

        cd -- "$caller_dir" || exit 98
        bash "$SCRIPT_INVOCATION"
    ) >"$STDOUT_FILE" 2>"$STDERR_FILE"
    CASE_STATUS=$?
}

make_success_case()
{
    case_name=$1
    caller_dir=$2
    jobs=$3
    pg_config=$4
    shift 4
    run_script "$case_name" "$caller_dir" "$@"
    assert_status 0
    assert_make_call_count 3
    assert_make_log "$jobs" "$pg_config"
    assert_success_banners
}

mkdir -p -- "$PACKAGE_DIR" "$REPO_DIR/src" "$FAKE_BIN" "$UNRELATED_DIR"

cat >"$FAKE_BIN/make" <<'FAKE_MAKE'
#!/usr/bin/env bash

{
    printf '%s\n' 'CALL'
    for arg in "$@"; do
        printf 'ARG=%s\n' "$arg"
    done
} >>"$MAKE_LOG"

count=$(cat "$MAKE_COUNT_FILE")
count=$((count + 1))
printf '%s\n' "$count" >"$MAKE_COUNT_FILE"

if [ -n "${FAKE_MAKE_FAIL_CALL-}" ] &&
   [ "$count" -eq "$FAKE_MAKE_FAIL_CALL" ]; then
    exit "${FAKE_MAKE_STATUS:-1}"
fi
exit 0
FAKE_MAKE

cat >"$FAKE_BIN/getconf" <<'FAKE_GETCONF'
#!/usr/bin/env bash

{
    printf '%s\n' 'CALL'
    for arg in "$@"; do
        printf 'ARG=%s\n' "$arg"
    done
} >>"$GETCONF_LOG"

if [ "$#" -ne 1 ] || [ "$1" != '_NPROCESSORS_ONLN' ]; then
    exit 97
fi
if [ "${FAKE_GETCONF_STATUS:-0}" -ne 0 ]; then
    exit "$FAKE_GETCONF_STATUS"
fi
printf '%s\n' "${FAKE_GETCONF_OUTPUT-}"
FAKE_GETCONF

chmod 755 "$FAKE_BIN/make" "$FAKE_BIN/getconf"

reset_fixture

SCRIPT_INVOCATION=$PACKAGE_DIR/opentenbase_ora_package_function_compile.sh

make_success_case \
    'repository root uses absolute paths and exact stage order' \
    "$REPO_DIR" 3 "$DEFAULT_PG_CONFIG" MAKE_JOBS=3
assert_file_empty "$GETCONF_LOG"
assert_success_banners_once

reset_fixture
make_success_case \
    'unrelated cwd uses script-relative paths' \
    "$UNRELATED_DIR" 3 "$DEFAULT_PG_CONFIG" MAKE_JOBS=3

reset_fixture
SCRIPT_INVOCATION=contrib/opentenbase_ora_package_function/opentenbase_ora_package_function_compile.sh
make_success_case \
    'relative invocation ignores CDPATH output' \
    "$REPO_DIR" 3 "$DEFAULT_PG_CONFIG" MAKE_JOBS=3 CDPATH=.
SCRIPT_INVOCATION=$PACKAGE_DIR/opentenbase_ora_package_function_compile.sh

reset_fixture
make_success_case \
    'explicit absolute PG_CONFIG works from unrelated cwd' \
    "$UNRELATED_DIR" 3 "$EXPLICIT_PG_CONFIG" \
    MAKE_JOBS=3 "PG_CONFIG=$EXPLICIT_PG_CONFIG"

reset_fixture
make_success_case \
    'valid getconf output supplies jobs' \
    "$REPO_DIR" 6 "$DEFAULT_PG_CONFIG" FAKE_GETCONF_OUTPUT=6
assert_getconf_called_once

for discovery_case in failure empty zero nonnumeric; do
    reset_fixture
    case "$discovery_case" in
        failure)
            discovery_environment='FAKE_GETCONF_STATUS=9'
            ;;
        empty)
            discovery_environment='FAKE_GETCONF_OUTPUT='
            ;;
        zero)
            discovery_environment='FAKE_GETCONF_OUTPUT=0'
            ;;
        nonnumeric)
            discovery_environment='FAKE_GETCONF_OUTPUT=many'
            ;;
    esac
    make_success_case \
        "invalid getconf $discovery_case falls back to one job" \
        "$REPO_DIR" 1 "$DEFAULT_PG_CONFIG" "$discovery_environment"
    assert_getconf_called_once
done

for jobs_value in '' 0 -2 many 01; do
    reset_fixture
    run_script "invalid explicit MAKE_JOBS <$jobs_value>" \
        "$REPO_DIR" "MAKE_JOBS=$jobs_value"
    assert_prerequisite_failure 'MAKE_JOBS must be a positive integer'
    assert_file_empty "$GETCONF_LOG"
done

reset_fixture
run_script 'empty explicit PG_CONFIG is rejected' \
    "$REPO_DIR" MAKE_JOBS=3 PG_CONFIG=
assert_prerequisite_failure 'PG_CONFIG must be a nonempty absolute executable regular file'

reset_fixture
run_script 'relative explicit PG_CONFIG is rejected' \
    "$REPO_DIR" MAKE_JOBS=3 PG_CONFIG=relative/pg_config
assert_prerequisite_failure 'PG_CONFIG must be a nonempty absolute executable regular file'

reset_fixture
missing_override=$FIXTURE_ROOT/missing\ pg_config
run_script 'missing explicit PG_CONFIG is rejected' \
    "$REPO_DIR" MAKE_JOBS=3 "PG_CONFIG=$missing_override"
assert_prerequisite_failure 'PG_CONFIG must be a nonempty absolute executable regular file'
assert_file_contains "$STDERR_FILE" "$missing_override"

reset_fixture
chmod 644 "$EXPLICIT_PG_CONFIG"
run_script 'non-executable explicit PG_CONFIG is rejected' \
    "$REPO_DIR" MAKE_JOBS=3 "PG_CONFIG=$EXPLICIT_PG_CONFIG"
assert_prerequisite_failure 'PG_CONFIG must be a nonempty absolute executable regular file'
assert_file_contains "$STDERR_FILE" "$EXPLICIT_PG_CONFIG"

reset_fixture
rm -f -- "$DEFAULT_PG_CONFIG"
run_script 'missing derived pg_config is rejected' "$REPO_DIR" MAKE_JOBS=3
assert_prerequisite_failure 'derived pg_config must be an executable regular file'
assert_file_contains "$STDERR_FILE" "$DEFAULT_PG_CONFIG"

reset_fixture
chmod 644 "$DEFAULT_PG_CONFIG"
run_script 'non-executable derived pg_config is rejected' "$REPO_DIR" MAKE_JOBS=3
assert_prerequisite_failure 'derived pg_config must be an executable regular file'
assert_file_contains "$STDERR_FILE" "$DEFAULT_PG_CONFIG"

reset_fixture
rm -f -- "$MAKEFILE_GLOBAL"
run_script 'missing Makefile.global is rejected' "$REPO_DIR" MAKE_JOBS=3
assert_prerequisite_failure 'Makefile.global is not a readable file'
assert_file_contains "$STDERR_FILE" "$MAKEFILE_GLOBAL"

reset_fixture
printf '%s\n' \
    "prefix := $INSTALL_PREFIX" \
    'exec_prefix := ${prefix}' \
    'bindir := ${exec_prefix}/bin' >"$MAKEFILE_GLOBAL"
run_script 'missing configured PGXS block is rejected' "$REPO_DIR" MAKE_JOBS=3
assert_prerequisite_failure 'configured ifndef PGXS block'

for assignment_name in prefix exec_prefix bindir; do
    reset_fixture
    case "$assignment_name" in
        prefix)
            write_makefile '# prefix is missing' \
                'exec_prefix := /absolute/exec' \
                'bindir := /absolute/bin'
            ;;
        exec_prefix)
            write_makefile "prefix := $INSTALL_PREFIX" \
                '# exec_prefix is missing' \
                'bindir := /absolute/bin'
            ;;
        bindir)
            write_makefile "prefix := $INSTALL_PREFIX" \
                'exec_prefix := ${prefix}' \
                '# bindir is missing'
            ;;
    esac
    run_script "missing $assignment_name assignment is rejected" \
        "$REPO_DIR" MAKE_JOBS=3
    assert_prerequisite_failure \
        "expected exactly one nonempty $assignment_name := assignment"

    reset_fixture
    case "$assignment_name" in
        prefix)
            write_makefile 'prefix :=' \
                'exec_prefix := /absolute/exec' \
                'bindir := /absolute/bin'
            ;;
        exec_prefix)
            write_makefile "prefix := $INSTALL_PREFIX" \
                'exec_prefix :=' \
                'bindir := /absolute/bin'
            ;;
        bindir)
            write_makefile "prefix := $INSTALL_PREFIX" \
                'exec_prefix := ${prefix}' \
                'bindir :='
            ;;
    esac
    run_script "empty $assignment_name assignment is rejected" \
        "$REPO_DIR" MAKE_JOBS=3
    assert_prerequisite_failure \
        "expected exactly one nonempty $assignment_name := assignment"

    reset_fixture
    case "$assignment_name" in
        prefix)
            write_makefile "prefix := $INSTALL_PREFIX" \
                'exec_prefix := /absolute/exec' \
                'bindir := /absolute/bin' \
                'prefix := /duplicate'
            ;;
        exec_prefix)
            write_makefile "prefix := $INSTALL_PREFIX" \
                'exec_prefix := ${prefix}' \
                'bindir := /absolute/bin' \
                'exec_prefix := /duplicate'
            ;;
        bindir)
            write_makefile "prefix := $INSTALL_PREFIX" \
                'exec_prefix := ${prefix}' \
                'bindir := ${exec_prefix}/bin' \
                'bindir := /duplicate'
            ;;
    esac
    run_script "duplicate $assignment_name assignment is rejected" \
        "$REPO_DIR" MAKE_JOBS=3
    assert_prerequisite_failure \
        "expected exactly one nonempty $assignment_name := assignment"

    reset_fixture
    case "$assignment_name" in
        prefix)
            write_makefile 'prefix := relative-prefix' \
                'exec_prefix := /absolute/exec' \
                'bindir := /absolute/bin'
            ;;
        exec_prefix)
            write_makefile "prefix := $INSTALL_PREFIX" \
                'exec_prefix := relative-exec' \
                'bindir := /absolute/bin'
            ;;
        bindir)
            write_makefile "prefix := $INSTALL_PREFIX" \
                'exec_prefix := ${prefix}' \
                'bindir := relative-bin'
            ;;
    esac
    run_script "relative $assignment_name assignment is rejected" \
        "$REPO_DIR" MAKE_JOBS=3
    assert_prerequisite_failure \
        "$assignment_name must resolve to a safe absolute path"
done

reset_fixture
alternate_exec_prefix=$FIXTURE_ROOT/custom\ exec
mkdir -p -- "$alternate_exec_prefix/bin"
cp -- "$DEFAULT_PG_CONFIG" "$alternate_exec_prefix/bin/pg_config"
write_makefile "prefix := $INSTALL_PREFIX" \
    "exec_prefix := $alternate_exec_prefix" \
    'bindir := ${exec_prefix}/bin'
make_success_case \
    'absolute custom exec_prefix is supported' \
    "$REPO_DIR" 3 "$alternate_exec_prefix/bin/pg_config" MAKE_JOBS=3

reset_fixture
alternate_bindir=$FIXTURE_ROOT/custom\ bindir
mkdir -p -- "$alternate_bindir"
cp -- "$DEFAULT_PG_CONFIG" "$alternate_bindir/pg_config"
write_makefile "prefix := $INSTALL_PREFIX" \
    'exec_prefix := ${prefix}' \
    "bindir := $alternate_bindir"
make_success_case \
    'absolute custom bindir is supported' \
    "$REPO_DIR" 3 "$alternate_bindir/pg_config" MAKE_JOBS=3

reset_fixture
write_makefile "prefix := $INSTALL_PREFIX" \
    'exec_prefix := $(prefix)' \
    'bindir := $(exec_prefix)/bin'
make_success_case \
    'parenthesized literal Make references are supported' \
    "$REPO_DIR" 3 "$DEFAULT_PG_CONFIG" MAKE_JOBS=3

reset_fixture
write_makefile 'prefix := $unknown' \
    'exec_prefix := /absolute/exec' \
    'bindir := /absolute/bin'
run_script 'unknown prefix reference is rejected' "$REPO_DIR" MAKE_JOBS=3
assert_prerequisite_failure 'prefix must resolve to a safe absolute path'

reset_fixture
write_makefile "prefix := $INSTALL_PREFIX" \
    'exec_prefix := ${unknown}' \
    'bindir := /absolute/bin'
run_script 'unknown exec_prefix reference is rejected' "$REPO_DIR" MAKE_JOBS=3
assert_prerequisite_failure 'exec_prefix must resolve to a safe absolute path'

reset_fixture
write_makefile "prefix := $INSTALL_PREFIX" \
    'exec_prefix := ${prefix}' \
    'bindir := ${unknown}/bin'
run_script 'unknown bindir reference is rejected' "$REPO_DIR" MAKE_JOBS=3
assert_prerequisite_failure 'bindir must resolve to a safe absolute path'

reset_fixture
command_marker=$FIXTURE_ROOT/command-substitution-ran
write_makefile "prefix := $INSTALL_PREFIX" \
    "exec_prefix := \$(touch $command_marker)" \
    'bindir := /absolute/bin'
run_script 'Make command substitution is rejected without execution' \
    "$REPO_DIR" MAKE_JOBS=3
assert_prerequisite_failure 'exec_prefix must resolve to a safe absolute path'
assert_file_absent "$command_marker"

reset_fixture
backtick_marker=$FIXTURE_ROOT/backtick-ran
write_makefile "prefix := $INSTALL_PREFIX" \
    'exec_prefix := ${prefix}' \
    "bindir := \`touch $backtick_marker\`"
run_script 'Make backticks are rejected without execution' "$REPO_DIR" MAKE_JOBS=3
assert_prerequisite_failure 'bindir must resolve to a safe absolute path'
assert_file_absent "$backtick_marker"

for failure_call in 1 2 3; do
    reset_fixture
    case "$failure_call" in
        1)
            failure_status=41
            failure_stage=clean
            ;;
        2)
            failure_status=42
            failure_stage=make
            ;;
        3)
            failure_status=43
            failure_stage='make install'
            ;;
    esac

    run_script "$failure_stage failure preserves status and stops later stages" \
        "$REPO_DIR" MAKE_JOBS=3 \
        "FAKE_MAKE_FAIL_CALL=$failure_call" \
        "FAKE_MAKE_STATUS=$failure_status"
    assert_status "$failure_status"
    assert_make_call_count "$failure_call"
    assert_make_log_through_call "$failure_call"
    assert_file_contains "$STDERR_FILE" \
        "compile $failure_stage error($failure_status)"
    assert_file_not_contains "$STDOUT_FILE" \
        "compile $failure_stage finished"
    assert_file_not_contains "$STDERR_FILE" \
        "compile $failure_stage finished"
    assert_file_not_contains "$STDOUT_FILE" 'compile is success'
    assert_file_not_contains "$STDERR_FILE" 'compile is success'
done

printf 'PASS: %d assertions\n' "$ASSERTION_COUNT"
