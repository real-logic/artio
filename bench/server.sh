#!/bin/sh

set -eu

java \
  -XX:+UnlockCommercialFeatures \
  -XX:+FlightRecorder \
  -XX:FlightRecorderOptions=defaultrecording=true,disk=true,repository=/tmp,maxage=6h,settings=./ProfileWithoutSockets.jfc,dumponexit=true,dumponexitpath=dump.jfr \
  -cp fix-gateway-system-tests-*-benchmarks.jar \
  -Dfix.core.timing=true \
  uk.co.real_logic.fix_gateway.system_benchmarks.FixBenchmarkServer

