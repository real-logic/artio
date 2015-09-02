#!/bin/sh

set -eu

java \
  -XX:+UnlockCommercialFeatures \
  -XX:+FlightRecorder \
  -cp fix-gateway-system-tests-*-benchmarks.jar \
  -XX:StartFlightRecording=delay=10s,duration=40s,name=MyRecording,filename=dump.jfr,settings=./ProfileWithoutSockets.jfc \
  -Dfix.core.timing=true \
  -Dfix.codecs.no_validation=true \
  -Dfix.benchmark.warmup=15000 \
  -Dfix.benchmark.messages=500000 \
  uk.co.real_logic.fix_gateway.system_benchmarks.FixBenchmarkServer
