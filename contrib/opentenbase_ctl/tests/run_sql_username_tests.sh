#!/bin/sh
# Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
# Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.

set -eu
src=$(CDPATH= cd -- "$(dirname -- "$0")/../src" && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/ctl-sql-username.XXXXXX")
trap 'rm -rf "$tmp"' 0
trap 'exit 1' 1 2 3 15
mkdir "$tmp/bin"
cat > "$tmp/bin/psql" <<'EOF'
#!/bin/sh
printf '%s\n' "$@"
EOF
chmod +x "$tmp/bin/psql"
cat > "$tmp/bin/sudo" <<'EOF'
#!/bin/sh
[ "$1" = -u ] && [ "$2" = testuser ] || exit 1
shift 2
exec "$@"
EOF
chmod +x "$tmp/bin/sudo"
cat > "$tmp/bin/sshpass" <<'EOF'
#!/bin/sh
echo 'Unexpected SSH invocation in a local-only test' >&2
exit 1
EOF
chmod +x "$tmp/bin/sshpass"
PATH="$tmp/bin:$PATH"
export PATH

# cstdint works around the existing bundled CLI header's missing include.
# Keep the locality predicate as a call so the test can force the currently
# disabled local branch. All execution and command-building code stays real.
${CXX:-c++} ${CXXFLAGS:-} -std=c++17 -O0 -include cstdint -ffunction-sections \
    -fdata-sections -c "$src/ssh/remote_ssh.cpp" -o "$tmp/remote_ssh.o"
local_symbol=$(nm "$tmp/remote_ssh.o" | awk '$2 == "T" && $3 ~ /^_Z11is_local_ip/ { print $3 }')
[ -n "$local_symbol" ]
# A strong test predicate overrides this weak symbol, including calls from
# within remote_ssh.cpp; ordinary linker --wrap cannot intercept those calls.
objcopy --weaken-symbol="$local_symbol" "$tmp/remote_ssh.o"
# Compile production translation units; gc-sections drops unrelated commands.
${CXX:-c++} ${CXXFLAGS:-} -std=c++17 -include cstdint -ffunction-sections \
    -fdata-sections -I"$src" "$src/../tests/sql_username_test.cpp" \
    "$src/utils/utils.cpp" "$tmp/remote_ssh.o" "$src/log/log.cpp" \
    -Wl,--gc-sections -pthread -o "$tmp/sql_username_test"
# Isolate fixture execution from user environment paths and HOME expansion.
PATH="$tmp/bin:/usr/bin:/bin"
HOME="$tmp/fake-home"
export PATH HOME
cd "$tmp"
./sql_username_test "$tmp"
