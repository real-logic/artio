#!/bin/sh

set -eu

scp -i $PEM $1 ubuntu@$EC2:fix-gateway/
