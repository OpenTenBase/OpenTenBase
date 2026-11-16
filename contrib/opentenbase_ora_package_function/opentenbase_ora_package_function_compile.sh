#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
makefile_global="${script_dir}/../../src/Makefile.global"

if [[ ! -r "${makefile_global}" ]]; then
    echo "ERROR: ${makefile_global} is not readable. Run configure before compiling the extension." >&2
    exit 1
fi

configure_args=$(sed -n 's/^configure_args[[:space:]]*=[[:space:]]*//p' "${makefile_global}")
echo "configure_args:${configure_args}"

make_jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)

prefix_args=$(printf '%s\n' "${configure_args}" | tr -d "'" | sed -n 's/.*--prefix=\([^[:space:]]*\).*/\1/p')
if [[ -z "${prefix_args}" ]]; then
    echo "ERROR: configure_args in ${makefile_global} does not contain --prefix." >&2
    exit 1
fi
echo "prefix_args:${prefix_args}"

cd "${script_dir}"

run_make_step()
{
    local step=$1
    shift

    if "$@"; then
        echo "=== opentenbase_ora_package_function_compile ${step} finished ==="
    else
        local status=$?
        echo "=== opentenbase_ora_package_function_compile ${step} error(${status}) ===" >&2
        exit "${status}"
    fi
}

echo "make PG_CONFIG=${prefix_args}/bin/pg_config clean"
run_make_step clean make PG_CONFIG="${prefix_args}/bin/pg_config" clean
run_make_step make make PG_CONFIG="${prefix_args}/bin/pg_config" -j"${make_jobs}"
run_make_step "make install" make PG_CONFIG="${prefix_args}/bin/pg_config" install -j"${make_jobs}"

echo "=== opentenbase_ora_package_function_compile compile is success ==="
