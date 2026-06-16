# PartForge

Distributed ClickHouse MergeTree part rewriting for large schema migrations.

The intended flow is:

1. Run `ALTER TABLE db.table FREEZE WITH NAME 'name'` on a ClickHouse node.
2. Run `partforge upload-freeze` against that node. It packages every frozen part with a manifest, uploads source artifacts to S3, and sends SQS work messages.
3. Run `partforge worker` in a container that also contains `clickhouse-server`. The worker starts ClickHouse locally, attaches one source part, runs the provided `INSERT INTO ... SELECT ...`, detaches produced destination parts, and uploads finished artifacts.
4. Run `partforge import-finished` near the destination ClickHouse node. It downloads finished artifacts one at a time, copies each produced part into the destination table's `detached` directory, and runs `ALTER TABLE ... ATTACH PART`. ClickHouse assigns the final active part names.

This is a part-level rewrite tool, not a generic distributed SQL engine. The insert-select must be valid when executed independently for each source part. Row-local schema migrations, casts, computed columns, filters, changed codecs, changed sort keys, and changed partitioning fit this model. Global transforms such as `GROUP BY`, `DISTINCT`, windows, and `ORDER BY ... LIMIT` do not.

## LocalStack

```sh
docker compose up -d localstack
```

LocalStack creates:

- S3 bucket: `partforge`
- SQS queue: `partforge`

Use these endpoint flags for local runs:

```sh
-s3-endpoint=http://localhost:4566
-sqs-endpoint=http://localhost:4566
-queue-url=http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/partforge
```

## Worker Container

The worker image is a single Ubuntu-based container with ClickHouse packages installed and the Go binary copied in from a builder stage. Its entrypoint is the Go worker binary, and the worker starts `clickhouse server` as a child process before consuming SQS messages. The default ClickHouse version is `26.3.10.60`.

```sh
docker compose build worker
docker compose up worker
```

## Metrics

`partforge worker` exposes Prometheus metrics on `:2112/metrics` by default. Use `-metrics-addr=""` to disable the endpoint, or `-metrics-addr` / `-metrics-path` to change where it listens.

Core worker metrics:

- `partforge_rows_read_total`
- `partforge_bytes_read_total`
- `partforge_rows_written_total`
- `partforge_bytes_written_total`
- `partforge_current_read_rows`
- `partforge_current_read_bytes`
- `partforge_current_written_rows`
- `partforge_current_written_bytes`
- `partforge_active_part_count`
- `partforge_active_part_rows`
- `partforge_active_part_bytes`
- `partforge_forges_started_total`
- `partforge_forges_completed_total`
- `partforge_forges_failed_total`

The read/write counters are updated live while the `INSERT SELECT` is running by polling ClickHouse `system.processes` for the rewrite query id. Source and destination active part gauges are measured from `system.parts` while those parts are attached in the worker.

## Example

```sh
partforge upload-freeze \
  -database=src_db \
  -table=events \
  -freeze=migration_001 \
  -destination-database=dst_db \
  -destination-table=events_new \
  -destination-schema-file=dest.sql \
  -insert-select-file=insert.sql \
  -bucket=partforge \
  -queue-url=http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/partforge \
  -s3-endpoint=http://localhost:4566 \
  -sqs-endpoint=http://localhost:4566
```

The destination schema file should contain a full `CREATE TABLE` statement. The insert file should contain a full statement such as:

```sql
INSERT INTO dst_db.events_new
SELECT
    *
FROM src_db.events
```

`Replicated*MergeTree` engines are normalized to their non-replicated `*MergeTree` equivalents inside the worker.

`import-finished` requires the destination table to be empty by default. This is intentional: attaching the same finished artifacts twice would duplicate data, and there is no exact transaction spanning S3 and ClickHouse. Use `-require-empty=false` only when importing into a table that you have verified manually.
