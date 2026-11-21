#!/bin/bash

# If get the TESTS variable, just exit
if [ -n "$TESTS" ]; then
  exit
fi

# Report a failed check even when its output is piped into tee below
set -o pipefail

current_dir=$(pwd)
rc=0
# install check the contrib first
rm -f ${current_dir}/regression_contrib.out
if [ -n "$MR_CHECK" ]; then
  make contribinstallcheck >> ${current_dir}/regression_contrib.out 2>&1 || rc=$?
else
  make contribinstallcheck | tee ${current_dir}/regression_contrib.out || rc=$?
fi

# isolation check
if [ -n "$MR_CHECK" ]; then
  make -C ../isolation installcheck_dn >> ${current_dir}/regression_contrib.out 2>&1 || rc=$?
else
  make -C ../isolation installcheck_dn | tee ${current_dir}/regression_contrib.out || rc=$?
fi

cd ${current_dir}
exit $rc
