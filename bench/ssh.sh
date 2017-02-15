#!/bin/sh

ssh -o "StrictHostKeyChecking no" -i $PEM ubuntu@$EC2
