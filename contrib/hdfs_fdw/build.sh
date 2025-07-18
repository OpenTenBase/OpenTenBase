#!/bin/bash

# Build script for HDFS FDW
# This script builds the HDFS FDW extension for OpenTenBase

set -e

echo "=== Building HDFS FDW for OpenTenBase ==="

# Check if we're in the correct directory
if [ ! -f "hdfs_fdw.c" ]; then
    echo "Error: Please run this script from the OpenTenBase/contrib/hdfs_fdw directory"
    exit 1
fi

# Auto-detect JAVA_HOME if not set
if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME not set. Attempting to auto-detect..."
    JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
    if [ ! -f "$JAVA_HOME/include/jni.h" ]; then
        echo "Error: Could not find Java development headers."
        echo "Please install OpenJDK development package:"
        echo "  sudo apt-get install openjdk-11-jdk"
        echo "Or manually set JAVA_HOME to your Java installation directory."
        exit 1
    fi
    echo "Auto-detected JAVA_HOME: $JAVA_HOME"
    export JAVA_HOME
fi

echo "Using JAVA_HOME: $JAVA_HOME"

# Build libhive first
echo "Building libhive..."
cd libhive
if [ -f "Makefile" ]; then
    make clean
    make
    echo "libhive built successfully (stub version)"
    echo "Note: To build with JNI support, run 'make jni' in libhive/ directory"
else
    echo "Error: libhive Makefile not found"
    exit 1
fi
cd ..

# Build the FDW
echo "Building HDFS FDW..."
make clean
make

echo "=== Build completed successfully ==="
echo "Extensions built:"
echo "  - libhive.so (Hive client library)"
echo "  - hdfs_fdw.so (PostgreSQL foreign data wrapper)"
echo ""
echo "To install: make install"
echo "To test: psql -d your_database -f test_hdfs_fdw.sql"