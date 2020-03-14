#!/bin/sh

set -eu

TAG_NAME="$1"

echo "Checking out requested tag: $TAG_NAME"

git checkout "$TAG_NAME"

echo "Building binary to upload"

./gradlew


echo "Uploading to maven repository"

./gradlew publish

