#!/bin/sh

set -eu

TMP="/dev/shm/"

rm -rf "$TMP/aeron"*
rm -rf "$TMP/fix"*

java \
  -verbose:gc \
  -XX:+PrintGCDetails \
  -XX:+UnlockCommercialFeatures \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+DebugNonSafepoints \
  -XX:+FlightRecorder \
  -XX:-UseBiasedLocking \
  -cp artio-system-tests-*-benchmarks.jar \
  -XX:StartFlightRecording=delay=10s,duration=40s,name=MyRecording,filename=dump.jfr,settings=./ProfileWithoutSockets.jfc \
  -Dfix.core.timing=true \
  -Dfix.codecs.no_validation=true \
  -Dfix.benchmark.engine_idle=noop \
  -Dfix.core.receiver_buffer_size=1048576 \
  -Dfix.core.sender_socket_buffer_size=16777216 \
  -Dfix.core.receiver_socket_buffer_size=16777216 \
  uk.co.real_logic.artio.system_benchmarks.FixBenchmarkServer

