#!/bin/bash

echo "Building base image..."
pushd ./docker/base
docker build -t opentenbasebase:1.0.0 .
popd

echo "Building host image..."
docker build -f docker/host/Dockerfile -t opentenbase:1.0.0 .