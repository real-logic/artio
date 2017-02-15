#!/bin/sh

set -eu

scp -i $PEM ubuntu@$EC2:fix-gateway/$1 .
