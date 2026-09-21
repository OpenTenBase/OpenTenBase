#!/bin/sh
# Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
# Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.

set -eu
src=$(CDPATH= cd -- "$(dirname -- "$0")/../src" && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/ctl-shell-selection.XXXXXX")
trap 'rm -rf "$tmp"' 0
trap 'exit 1' 1 2 3 15

# cstdint works around the existing bundled CLI header's missing include.
# Compile the real dispatcher and worker; mock only execute_command in the test.
${CXX:-c++} ${CXXFLAGS:-} -std=c++17 -include cstdint -ffunction-sections \
    -fdata-sections -I"$src" "$src/../tests/shell_selection_test.cpp" \
    "$src/cluster/cluster.cpp" "$src/node/node.cpp" "$src/log/log.cpp" \
    -Wl,--gc-sections -pthread -o "$tmp/shell_selection_test"
cd "$tmp"
if ./shell_selection_test > output.log; then
    tail -n 1 output.log
else
    cat output.log
    exit 1
fi
