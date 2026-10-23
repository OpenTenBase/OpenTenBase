#!/usr/bin/env bash
#
# Build the two images OpenTenBase ships in this directory.
#
# Each build runs in a subshell so a missing context cannot leave the script in
# the wrong directory: previously a failed `pushd` was ignored and `docker build`
# then ran against whatever directory happened to be current, producing a wrong
# image under the right tag.
set -euo pipefail

build_image() {
    local context=$1
    local tag=$2

    if [[ ! -d $context ]]; then
        echo "buildImage: missing build context '$context'" >&2
        return 1
    fi
    if [[ ! -f $context/Dockerfile ]]; then
        echo "buildImage: '$context' has no Dockerfile" >&2
        return 1
    fi

    echo "buildImage: building $tag from $context"
    ( cd "$context" && exec docker build -t "$tag" . )
}

build_image ./base opentenbasebase:1.0.0
build_image ./host opentenbase:1.0.0
