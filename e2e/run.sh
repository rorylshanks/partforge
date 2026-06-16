#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT/.e2e/clickhouse-data"
BIN="$ROOT/.e2e/partforge"
QUEUE_URL_HOST="http://localhost:4566/000000000000/partforge"
QUEUE_URL_DOCKER="http://localstack:4566/000000000000/partforge"
CH_HTTP_HOST="http://127.0.0.1:18123"
CH_HTTP_DOCKER="http://clickhouse:8123"
JOB_ID="e2e-job"

cd "$ROOT"

rm -rf "$ROOT/.e2e"
mkdir -p "$DATA_DIR"
chmod -R a+rwx "$ROOT/.e2e"

docker compose down --remove-orphans >/dev/null 2>&1 || true
CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose build worker
CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose up -d localstack clickhouse

for _ in $(seq 1 60); do
  if curl -fsS "$CH_HTTP_HOST/?query=SELECT%201" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$CH_HTTP_HOST/?query=SELECT%201" >/dev/null

for _ in $(seq 1 60); do
  if docker compose exec -T localstack awslocal sqs list-queues >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker compose exec -T localstack awslocal s3 mb s3://partforge >/dev/null 2>&1 || true
docker compose exec -T localstack awslocal sqs create-queue --queue-name partforge >/dev/null

docker compose exec -T clickhouse clickhouse-client --multiquery < e2e/sql/setup_and_freeze.sql

part_count="$(find "$DATA_DIR/shadow/e2e_freeze" -name checksums.txt | wc -l | tr -d ' ')"
if [[ "$part_count" == "0" ]]; then
  echo "no frozen parts found" >&2
  exit 1
fi

go build -o "$BIN" ./cmd/partforge

"$BIN" upload-freeze \
  -database=src \
  -table=events \
  -freeze=e2e_freeze \
  -shadow-dir="$DATA_DIR/shadow" \
  -destination-database=dst \
  -destination-table=events_new \
  -destination-schema-file=e2e/sql/destination.sql \
  -insert-select-file=e2e/sql/insert.sql \
  -clickhouse-url="$CH_HTTP_HOST" \
  -bucket=partforge \
  -prefix=e2e \
  -job-id="$JOB_ID" \
  -queue-url="$QUEUE_URL_HOST" \
  -s3-endpoint=http://localhost:4566 \
  -sqs-endpoint=http://localhost:4566

for _ in $(seq 1 "$part_count"); do
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    worker \
    -queue-url="$QUEUE_URL_DOCKER" \
    -s3-endpoint=http://localstack:4566 \
    -sqs-endpoint=http://localstack:4566 \
    -once
done

clickhouse_owner="$(stat -c '%u:%g' "$DATA_DIR")"
CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm --user "$clickhouse_owner" \
  -v "$DATA_DIR:/var/lib/clickhouse" \
  worker \
  import-finished \
  -database=dst \
  -table=events_new \
  -job-id="$JOB_ID" \
  -bucket=partforge \
  -prefix=e2e \
  -clickhouse-url="$CH_HTTP_DOCKER" \
  -s3-endpoint=http://localstack:4566

docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT id, name, amount_text, event_date, migrated FROM dst.events_new ORDER BY id FORMAT TSV" \
  > "$ROOT/.e2e/actual.tsv"

diff -u e2e/expected.tsv "$ROOT/.e2e/actual.tsv"
echo "e2e passed with $part_count frozen parts"
