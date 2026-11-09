#!/bin/bash
tar -czf libs.tar.gz * || exit $?
echo "Libraries have been packaged into libs.tar.gz"
