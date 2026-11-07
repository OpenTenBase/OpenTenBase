#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
makefile_global="${script_dir}/../../src/Makefile.global"

if [[ ! -r "${makefile_global}" ]]; then
    echo "ERROR: ${makefile_global} is not readable. Run configure before compiling the extension." >&2
    exit 1
fi

make_jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null) || make_jobs=1
if [[ ! "${make_jobs}" =~ ^[1-9][0-9]*$ ]]; then
    make_jobs=1
fi

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

# The in-tree Makefile includes Makefile.global and already knows the configured
# installation directories. Re-parsing configure_args would lose defaults,
# separate option values, and quoting, and would ignore --bindir/--exec-prefix.
run_make_step clean make clean
run_make_step make make -j"${make_jobs}"
run_make_step "make install" make install -j"${make_jobs}"

echo "=== opentenbase_ora_package_function_compile compile is success ==="
