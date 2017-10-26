#!/bin/sh

set -eu

java \
  -verbose:gc \
  -XX:+PrintGCDetails \
  -cp artio-system-tests-*-benchmarks.jar \
  -Dfix.codecs.no_validation=true \
  -Dfix.benchmark.warmup=100000 \
  -Dfix.benchmark.messages=500000 \
  uk.co.real_logic.artio.system_benchmarks.FixBenchmarkClient

