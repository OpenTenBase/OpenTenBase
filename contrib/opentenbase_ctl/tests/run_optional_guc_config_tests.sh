#!/bin/sh
# Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
# Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.

set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
test_dir=$(mktemp -d "${TMPDIR:-/tmp}/opentenbase-ctl-optional-guc.XXXXXX")
trap 'rm -rf -- "$test_dir"' 0
trap 'exit 1' 1 2 3 15

# Include cstdint for the bundled CLI header on newer standard libraries.
# Discard unrelated CLI/network entry points; execute the real config builders.
${CXX:-c++} ${CXXFLAGS:-} -std=c++17 -include cstdint -Wall -Wextra \
    -ffunction-sections -fdata-sections -I"$script_dir/../src" \
    "$script_dir/optional_guc_config_test.cpp" \
    "$script_dir/../src/types/types.cpp" \
    "$script_dir/../src/config/config.cpp" \
    "$script_dir/../src/utils/utils.cpp" \
    "$script_dir/../src/file/file.cpp" \
    "$script_dir/../src/log/log.cpp" \
    -Wl,--gc-sections -o "$test_dir/optional_guc_config_test"

cd "$test_dir"
./optional_guc_config_test
