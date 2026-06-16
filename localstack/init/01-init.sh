#!/usr/bin/env bash
set -euo pipefail

awslocal s3 mb s3://partforge || true
awslocal sqs create-queue --queue-name partforge >/dev/null

