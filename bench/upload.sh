#!/bin/sh

set -eu

./gradlew benchmarks
./bench/up.sh artio-system-tests/build/libs/*-benchmarks.jar
