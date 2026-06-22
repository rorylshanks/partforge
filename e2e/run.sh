#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT/.e2e/clickhouse-data"
STATE_TABLE="partforge"
CH_HTTP_HOST="http://127.0.0.1:18123"
CH_HTTP_DOCKER="http://clickhouse:8123"
JOB_ID="e2e-job"

cd "$ROOT"

log_value() {
  local line="$1"
  local key="$2"
  printf '%s\n' "$line" |
    tr ' ' '\n' |
    sed -n "s/^${key}=//p" |
    tail -n 1 |
    tr -d '"'
}

require_uint() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "expected numeric $name in worker settings log, got ${value:-<empty>}" >&2
    exit 1
  fi
}

assert_worker_insert_memory_settings() {
  local log_file="$1"
  local line
  line="$(grep 'configured clickhouse resource settings' "$log_file" | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    echo "worker log $log_file did not contain configured clickhouse resource settings" >&2
    exit 1
  fi

  local cpus memory_bytes max_threads max_insert_threads max_memory_usage min_rows min_bytes
  cpus="$(log_value "$line" "cpus")"
  memory_bytes="$(log_value "$line" "memory_bytes_raw")"
  max_threads="$(log_value "$line" "max_threads")"
  max_insert_threads="$(log_value "$line" "max_insert_threads")"
  max_memory_usage="$(log_value "$line" "max_memory_usage_raw")"
  min_rows="$(log_value "$line" "min_insert_block_size_rows")"
  min_bytes="$(log_value "$line" "min_insert_block_size_bytes_raw")"

  require_uint "cpus" "$cpus"
  require_uint "memory_bytes" "$memory_bytes"
  require_uint "max_threads" "$max_threads"
  require_uint "max_insert_threads" "$max_insert_threads"
  require_uint "max_memory_usage" "$max_memory_usage"
  require_uint "min_insert_block_size_rows" "$min_rows"
  require_uint "min_insert_block_size_bytes" "$min_bytes"

  local cpu_threads memory_threads expected_threads
  if (( cpus < 4 )); then
    cpu_threads=1
  else
    cpu_threads=$((cpus / 4))
  fi
  local expected_max_memory expected_min_bytes expected_min_rows reserved_insert_block_memory
  expected_max_memory=$((memory_bytes * 70 / 100))
  memory_threads=$((expected_max_memory / (6 * 2 * 1024 * 1024 * 1024)))
  if (( memory_threads < 1 )); then
    memory_threads=1
  fi
  if (( memory_threads < cpu_threads )); then
    expected_threads=$memory_threads
  else
    expected_threads=$cpu_threads
  fi
  expected_min_bytes=$((expected_max_memory / (6 * expected_threads)))
  expected_min_rows=$((expected_min_bytes / 1024))
  if (( expected_min_rows < 8192 )); then
    expected_min_rows=8192
  elif (( expected_min_rows > 8388608 )); then
    expected_min_rows=8388608
  fi
  reserved_insert_block_memory=$((min_bytes * max_insert_threads * 3))

  if (( max_threads != expected_threads )); then
    echo "max_threads=$max_threads, expected $expected_threads from cpus=$cpus" >&2
    exit 1
  fi
  if (( max_insert_threads != expected_threads )); then
    echo "max_insert_threads=$max_insert_threads, expected $expected_threads from cpus=$cpus" >&2
    exit 1
  fi
  if (( max_memory_usage != expected_max_memory )); then
    echo "max_memory_usage=$max_memory_usage, expected $expected_max_memory from memory_bytes=$memory_bytes" >&2
    exit 1
  fi
  if (( min_rows != expected_min_rows )); then
    echo "min_insert_block_size_rows=$min_rows, expected $expected_min_rows" >&2
    exit 1
  fi
  if (( min_bytes != expected_min_bytes )); then
    echo "min_insert_block_size_bytes=$min_bytes, expected $expected_min_bytes" >&2
    exit 1
  fi
  if (( reserved_insert_block_memory > max_memory_usage / 2 )); then
    echo "modeled insert block memory budget $reserved_insert_block_memory exceeds half max_memory_usage $((max_memory_usage / 2))" >&2
    exit 1
  fi
}

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
  -destination-schema-file=e2e/sql/destination.sql \
  -insert-select-file=e2e/sql/insert.sql \
  -clickhouse-url="$CH_HTTP_DOCKER" \
  -bucket=partforge \
  -prefix=e2e \
  -job-id="$JOB_ID" \
  -s3-endpoint=http://localstack:4566 \
  -dynamodb-endpoint=http://localstack:4566

for i in $(seq 1 "$part_count"); do
  worker_log="$ROOT/.e2e/worker-${i}.log"
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    worker \
    -s3-endpoint=http://localstack:4566 \
    -dynamodb-endpoint=http://localstack:4566 \
    -once 2>&1 | tee "$worker_log"
  assert_worker_insert_memory_settings "$worker_log"
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

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  delete-job \
  -job-id="$JOB_ID" \
  -delete-s3 \
  -s3-endpoint=http://localstack:4566 \
  -dynamodb-endpoint=http://localstack:4566

echo "e2e passed with $part_count frozen parts"
