#!/bin/sh

set -eu

./gradlew benchmarks
./bench/scp.sh fix-gateway-system-tests/build/libs/*-benchmarks.jar
