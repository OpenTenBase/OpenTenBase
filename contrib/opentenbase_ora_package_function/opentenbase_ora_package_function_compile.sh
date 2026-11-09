#!/usr/bin/env bash
#
# Build and install opentenbase_ora_package_function.
#
# Paths are resolved from this script's own location. The previous version read
# ../src/Makefile.global relative to the caller's working directory and then ran
# `cd opentenbase_ora_package_function` without checking the result, so it only
# worked when started from contrib/: from anywhere else the reads failed, the cd
# failed too, and the make commands then ran against whatever Makefile happened
# to be in the current directory.

set -u

script_dir=$(cd "$(dirname "$0")" && pwd) || exit 1
top_dir=$(cd "$script_dir/../.." && pwd) || exit 1

makefile_global=$top_dir/src/Makefile.global
if [ ! -f "$makefile_global" ]; then
    echo "cannot find $makefile_global; configure the tree first" >&2
    exit 1
fi

# configure_args carries the --prefix the tree was built with; pg_config lives
# there, and is not necessarily on PATH.
prefix_args=$(grep "configure_args" "$makefile_global" \
    | awk -F "configure_args =" '{ print $NF }' \
    | sed "s/'//g" \
    | awk -F "prefix=" '{ print $NF }' \
    | awk '{ print $1 }')

if [ -z "$prefix_args" ]; then
    echo "cannot read --prefix from $makefile_global" >&2
    exit 1
fi

pg_config=$prefix_args/bin/pg_config
if [ ! -x "$pg_config" ]; then
    echo "pg_config not found or not executable: $pg_config" >&2
    exit 1
fi
echo "pg_config: $pg_config"

# /proc/cpuinfo is Linux-only. The old expression gave 0 on macOS, and
# `make -j0` fails before building anything. nproc and getconf both work on
# Linux and macOS.
make_jobs=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)
case $make_jobs in
    ''|*[!0-9]*|0) make_jobs=1 ;;
esac
echo "make jobs: $make_jobs"

# Report the command's own status. The previous code printed $? from inside the
# else branch of `if [ $? -eq 0 ]`, which is the status of the test -- always 1
# -- and not the status make returned.
run_step() {
    local label=$1 status
    shift
    "$@"
    status=$?
    if [ "$status" -eq 0 ]; then
        echo "=== $label finished ==="
    else
        echo "=== $label error($status) ===" >&2
        exit "$status"
    fi
}

cd "$script_dir" || exit 1

run_step "clean" make PG_CONFIG="$pg_config" clean
run_step "make" make PG_CONFIG="$pg_config" -j"$make_jobs"
run_step "install" make PG_CONFIG="$pg_config" install -j"$make_jobs"

echo "=== opentenbase_ora_package_function_compile compile is success ==="
