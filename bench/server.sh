#!/bin/sh

set -eu

java \
  -XX:+UnlockCommercialFeatures \
  -XX:+FlightRecorder \
  -cp fix-gateway-system-tests-*-benchmarks.jar \
  -XX:StartFlightRecording=delay=5s,duration=30s,name=MyRecording,filename=dump.jfr,settings=./ProfileWithoutSockets.jfc \
  -Dfix.core.timing=true \
  uk.co.real_logic.fix_gateway.system_benchmarks.FixBenchmarkServer
