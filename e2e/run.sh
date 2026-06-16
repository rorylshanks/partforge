#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT/.e2e/clickhouse-data"
STATE_TABLE="partforge"
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
  if docker compose exec -T localstack awslocal dynamodb list-tables >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker compose exec -T localstack awslocal s3 mb s3://partforge >/dev/null 2>&1 || true
docker compose exec -T localstack awslocal dynamodb create-table \
  --table-name "$STATE_TABLE" \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
    AttributeName=gsi1pk,AttributeType=S \
    AttributeName=gsi1sk,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --global-secondary-indexes \
    '[{"IndexName":"gsi1","KeySchema":[{"AttributeName":"gsi1pk","KeyType":"HASH"},{"AttributeName":"gsi1sk","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]' \
  --billing-mode PAY_PER_REQUEST >/dev/null 2>&1 || true

docker compose exec -T clickhouse clickhouse-client --multiquery < e2e/sql/setup_and_freeze.sql

clickhouse_owner="$(docker compose exec -T clickhouse stat -c '%u:%g' /var/lib/clickhouse)"

part_count="$(
  docker compose exec -T -u "$clickhouse_owner" clickhouse \
    find /var/lib/clickhouse -path "*/shadow/e2e_freeze/*/checksums.txt" |
    wc -l |
    tr -d ' '
)"
if [[ "$part_count" == "0" ]]; then
  echo "no frozen parts found" >&2
  exit 1
fi

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm --user "$clickhouse_owner" \
  --workdir /work \
  -v "$ROOT:/work:ro" \
  -v "$DATA_DIR:/var/lib/clickhouse" \
  worker \
  upload-freeze \
  -database=src \
  -table=events \
  -freeze=e2e_freeze \
  -destination-database=dst \
  -destination-table=events_new \
  -destination-schema-file=e2e/sql/destination.sql \
  -insert-select-file=e2e/sql/insert.sql \
  -clickhouse-url="$CH_HTTP_DOCKER" \
  -bucket=partforge \
  -prefix=e2e \
  -job-id="$JOB_ID" \
  -s3-endpoint=http://localstack:4566 \
  -dynamodb-endpoint=http://localstack:4566

for _ in $(seq 1 "$part_count"); do
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    worker \
    -s3-endpoint=http://localstack:4566 \
    -dynamodb-endpoint=http://localstack:4566 \
    -once
done

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm --user "$clickhouse_owner" \
  -v "$DATA_DIR:/var/lib/clickhouse" \
  worker \
  import-finished \
  -database=dst \
  -table=events_new \
  -job-id="$JOB_ID" \
  -clickhouse-url="$CH_HTTP_DOCKER" \
  -s3-endpoint=http://localstack:4566 \
  -dynamodb-endpoint=http://localstack:4566

docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT id, name, amount_text, event_date, migrated FROM dst.events_new ORDER BY id FORMAT TSV" \
  > "$ROOT/.e2e/actual.tsv"

diff -u e2e/expected.tsv "$ROOT/.e2e/actual.tsv"
echo "e2e passed with $part_count frozen parts"
