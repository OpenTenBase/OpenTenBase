#!/bin/sh
# Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
# Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.

set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
test_dir=$(mktemp -d "${TMPDIR:-/tmp}/opentenbase-ctl-saved-path.XXXXXX")
trap 'rm -rf -- "$test_dir"' 0
trap 'exit 1' 1 2 3 15

# Link the production reader, discarding unrelated command entry points.
# cstdint compensates for an existing bundled CLI include on newer compilers.
${CXX:-c++} ${CXXFLAGS:-} -std=c++17 -include cstdint -Wall -Wextra \
    -ffunction-sections -fdata-sections -I"$script_dir/../src" \
    "$script_dir/saved_config_path_test.cpp" \
    "$script_dir/../src/command/command.cpp" \
    "$script_dir/../src/log/log.cpp" \
    -Wl,--gc-sections -o "$test_dir/saved_config_path_test"

# Never read or modify the user's real shell configuration.
HOME=$test_dir
export HOME
cd "$test_dir"
./saved_config_path_test
