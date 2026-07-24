#!/bin/sh
set -eu
binary=${1:-./opentenbase_ctl}
set +e
output=$("$binary" --opentenbase-invalid-option 2>&1)
status=$?
set -e
if [ "$status" -ne 1 ]; then
 printf '%s\n' "$output" >&2
 printf '%s\n' "invalid CLI option exited with status $status, expected 1" >&2
 exit 1
fi
case "$output" in
 *--opentenbase-invalid-option*) ;;
 *)
  printf '%s\n' "$output" >&2
  printf '%s\n' 'CLI parse error did not identify the rejected option' >&2
  exit 1
  ;;
esac
