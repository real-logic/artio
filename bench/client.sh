#!/bin/sh

set -eu

java \
  -cp fix-gateway-system-tests-*-benchmarks.jar \
  -Dfix.codecs.no_validation=true \
  -Dfix.benchmark.warmup=100000 \
  -Dfix.benchmark.messages=500000 \
  uk.co.real_logic.fix_gateway.system_benchmarks.FixBenchmarkClient

