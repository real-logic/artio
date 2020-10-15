#!/bin/bash

set -eu

TAG_NAME="$1"

echo "Checking out requested tag: $TAG_NAME"
git checkout "$TAG_NAME"

echo "Checking correct Java version (1.8) in use"
java -version 2>&1 | grep '1.8'

echo "Building binary to upload"

./gradlew

echo "Uploading to maven repository"

./gradlew publishAllPublicationsToOssRepository

if [ -v AWS_ACCESS_KEY_ID ]
then
    echo "Rebuilding and uploading with iLink3"

   ./gradlew -Dfix.core.iLink3Enabled=true clean build test publishAllPublicationsToPrivateRepository
else
    echo "Not uploading to private repository: no AWS key set"
fi

