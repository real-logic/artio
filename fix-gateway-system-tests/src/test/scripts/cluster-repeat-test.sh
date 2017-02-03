#!/bin/sh

set -u

count=$1
params=$2

for i in $(seq 1 $count)
do
  echo $i
  ./gradlew $params --rerun-tasks :fix-gateway-system-test:test --tests *ClusteredGatewaySystemTest > "$i.out"
done

