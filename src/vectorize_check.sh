#!/bin/bash

if [ $# -ne 2 ]
then
	echo "Usage: $0 source log"
	exit 1
fi

source="$1"
log="$2"
has_error=0

declare -A vectorized_lines;

for mark in $(grep "loop vectorized" ${log} | awk -F ':' '{print $2}')
do
	vectorized_lines[$mark]=1
done

for mark in $(grep -n ivdep ${source} |awk -F ':' '{print $1+1}')
do
	if [ -z ${vectorized_lines[$mark]} ] 
	then 
		echo "Error: ${source}:${mark} failed to vectorized." >&2
		has_error=1
	fi
done

exit $has_error
