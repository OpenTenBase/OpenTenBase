#!/bin/bash

# If get the TESTS variable, just exit
if [ -n "$TESTS" ]; then
  exit
fi

current_dir=$(pwd)
# install check the contrib first
rm -f ${current_dir}/regression_contrib.out
if [ -n "$MR_CHECK" ]; then
  make contribinstallcheck >> ${current_dir}/regression_contrib.out 2>&1
else
  make contribinstallcheck | tee ${current_dir}/regression_contrib.out
fi

# isolation check
if [ -n "$MR_CHECK" ]; then
  make -C ../isolation installcheck_dn >> ${current_dir}/regression_contrib.out 2>&1
else
  make -C ../isolation installcheck_dn | tee ${current_dir}/regression_contrib.out
fi

cd ${current_dir}
