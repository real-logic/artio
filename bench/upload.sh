#!/bin/sh

set -eu

./gradlew benchmarks
scp -i $PEM fix-gateway-system-tests/build/libs/*-benchmarks.jar ubuntu@$EC2:bench/
