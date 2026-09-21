#!/usr/bin/env bash
set -e

# Resolve build contexts independently of the caller's working directory.
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

docker build -t opentenbasebase:1.0.0 "$script_dir/base"
docker build -t opentenbase:1.0.0 "$script_dir/host"
