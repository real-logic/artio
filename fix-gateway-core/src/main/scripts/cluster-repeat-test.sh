#!/bin/sh

set -u

count=$1
params=$2

for i in $(seq 1 $count)
do
  echo $i
  ./gradlew $params -Dtest.single=ClusterReplicationTest --rerun-tasks :fix-gateway-core:test > "$i.out"
done

