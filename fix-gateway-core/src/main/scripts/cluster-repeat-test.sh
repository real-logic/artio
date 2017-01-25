#!/bin/sh

count=$1

for i in $(seq 1 $count)
do
  echo $i
  ./gradlew -Dtest.single=ClusterReplicationTest --rerun-tasks :fix-gateway-core:test > "$i.out"
done

