#!/bin/sh
# Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
# Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.

set -eu

test_dir=$(mktemp -d "${TMPDIR:-/tmp}/opentenbase-ctl-file-test.XXXXXX")
trap 'rm -f "$test_dir/file_test" "$test_dir/input.conf"; rmdir "$test_dir"' 0
trap 'exit 1' 1 2 3 15

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

# Allow the usual compiler command and flags, including compiler wrappers.
${CXX:-c++} ${CXXFLAGS:-} -std=c++17 -Wall -Wextra \
    -I"$script_dir/../src/file" \
    "$script_dir/file_test.cpp" "$script_dir/../src/file/file.cpp" \
    -o "$test_dir/file_test"

"$test_dir/file_test" "$test_dir/input.conf"
