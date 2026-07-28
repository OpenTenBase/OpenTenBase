#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

if [[ -n $(git status --porcelain=v1 --untracked-files=all) ]]; then
	echo "error: cleanup test must start from a clean worktree" >&2
	git status --short >&2
	exit 1
fi

build_jobs=${BUILD_JOBS:-2}
install_dir=${TEST_INSTALL_DIR:-"$repo_root/tmp_cleanup_install"}

./configure --prefix="$install_dir" "$@"
make -sj"$build_jobs"

build_status=$(git status --porcelain=v1 --untracked-files=all)
if [[ -n "$build_status" ]]; then
	echo "error: expected build outputs must not make git status dirty" >&2
	printf '%s\n' "$build_status" >&2
	exit 1
fi

make maintainer-clean

cleanup_status=$(git status --porcelain=v1 --untracked-files=all)
if [[ -n "$cleanup_status" ]]; then
	echo "error: make maintainer-clean did not restore a clean worktree" >&2
	printf '%s\n' "$cleanup_status" >&2
	exit 1
fi

generated_paths=(
	GNUmakefile
	config.log
	config.status
	src/backend/access/objfiles.txt
	src/backend/catalog/all.bki
	src/backend/catalog/all.description
	src/backend/catalog/all.shdescription
	src/backend/catalog/opentenbase_ora.bki
	src/backend/catalog/opentenbase_ora.description
	src/backend/catalog/opentenbase_ora.shdescription
	src/backend/opentenbase_ora/gram_ora.c
	src/backend/opentenbase_ora/gram_ora.h
	src/backend/opentenbase_ora/scan_ora.c
	src/bin/pg_license/libpglicense.a
	src/bin/pg_rewind/unittest/objfiles.txt
	src/fe_utils/psqlscan_ora.c
	src/include/opentenbase_ora/gram_ora.h
	src/pl/oraplsql/src/pl_gram.h
	src/pl/oraplsql/src/pl_gram.output
	src/pl/oraplsql/src/plerrcodes.h
	src/pl/plpgsql/src/pl_gram.output
)

leftovers=()
for path in "${generated_paths[@]}"; do
	if [[ -e "$path" || -L "$path" ]]; then
		leftovers+=("$path")
	fi
done

if (( ${#leftovers[@]} != 0 )); then
	echo "error: make maintainer-clean left generated files behind" >&2
	printf '  %s\n' "${leftovers[@]}" >&2
	exit 1
fi

echo "PASS: source build and maintainer cleanup leave a clean worktree"
