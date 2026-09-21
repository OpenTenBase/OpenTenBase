#!/bin/sh
# Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
# See the COPYRIGHT file for license information.
set -eu

test_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/opentenbase-cli-header.XXXXXX")
trap 'rm -rf "$build_dir"' EXIT HUP INT TERM

# Do not force-include any headers: this checks the CLI header's dependencies.
"${CXX:-c++}" -std=c++17 -Wall -Wextra -Werror -pedantic \
    -I"$test_dir/../src" "$test_dir/cli_header_test.cpp" \
    -o "$build_dir/cli_header_test"
"$build_dir/cli_header_test"
