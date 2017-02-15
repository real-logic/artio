#!/bin/sh

set -eu

./gradlew benchmarks
./bench/up.sh fix-gateway-system-tests/build/libs/*-benchmarks.jar
