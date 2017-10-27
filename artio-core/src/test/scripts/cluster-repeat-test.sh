#!/bin/sh

set -u

count=$1
params=$2

for i in $(seq 1 $count)
do
  echo $i
  ./gradlew $params --rerun-tasks :fix-gateway-core:test --tests *ClusterReplicationTest > "$i.out"
  #./gradlew $params --rerun-tasks :fix-gateway-core:test --tests *ClusterReplicationTest.shouldEventuallyReplicateMessageWhenClusterReformed > "$i.out"
done

