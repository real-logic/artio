#!/bin/sh

set -eu

java \
  -cp fix-gateway-system-tests-*-benchmarks.jar \
  uk.co.real_logic.fix_gateway.system_benchmarks.FixBenchmarkClient

