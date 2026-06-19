# PartForge

Distributed ClickHouse MergeTree part rewriting for large schema migrations.

The intended flow is:

1. Run `ALTER TABLE db.table FREEZE WITH NAME 'name'` on a ClickHouse node.
2. Run `partforge upload-freeze` on a host that can read the ClickHouse disk paths reported by `system.disks`. It writes a `manifest.json` into each frozen part directory, uploads the raw part directory to S3 with `s5cmd`, and registers `READY` part records in DynamoDB.
3. Run `partforge worker` in a container that also contains `clickhouse-server` and `s5cmd`. The worker starts ClickHouse locally, downloads one raw source part directory, attaches it, runs the provided `INSERT INTO ... SELECT ...`, freezes the produced destination parts, removes any existing finished artifact prefix for that part, and uploads one uncompressed tarball per produced destination part under the finished artifact prefix.
4. Run `partforge import-finished` near the destination ClickHouse node. It reads `FINISHED` part records from DynamoDB, downloads each finished artifact prefix one at a time, extracts the downloaded part tarballs, moves each extracted part into the destination table's `detached` directory, and runs `ALTER TABLE ... ATTACH PART`.

This is a part-level rewrite tool, not a generic distributed SQL engine. The insert-select must be valid when executed independently for each source part. Row-local schema migrations, casts, computed columns, filters, changed codecs, changed sort keys, and changed partitioning fit this model. Global transforms such as `GROUP BY`, `DISTINCT`, windows, and `ORDER BY ... LIMIT` do not.

## LocalStack

```sh
docker compose up -d localstack
```

LocalStack creates:

- S3 bucket: `partforge`
- DynamoDB table: `partforge`

Use these endpoint flags for local runs:

```sh
-s3-endpoint=http://localhost:4566
-dynamodb-endpoint=http://localhost:4566
```

The DynamoDB state table defaults to `partforge`. Use `-state-table` only to override it.

`partforge` uses `s5cmd` for S3 directory transfers. The worker image includes `s5cmd`; local binary runs need `s5cmd` available on `PATH` or passed with `-s5cmd-binary`.

## Config

Every command reads defaults from `/etc/partforge/config.json` when that file exists. Use `-config=/path/to/config.json` to override the location. CLI flags always take precedence over config values.

Top-level config keys apply to every command. Command-specific keys under `commands` override top-level keys for that command:

```json
{
  "s3_endpoint": "http://localhost:4566",
  "dynamodb_endpoint": "http://localhost:4566",
  "state_table": "partforge",
  "bucket": "partforge",
  "prefix": "partforge",
  "s5cmd_binary": "s5cmd",
  "commands": {
    "worker": {
      "metrics_addr": ":2112",
      "state_progress_interval": "15s",
      "shutdown_grace_period": "2m"
    },
    "import-finished": {
      "clickhouse_url": "http://clickhouse:8123"
    }
  }
}
```

Config keys may use either flag-style names such as `aws-region` or JSON-style names such as `aws_region`.

The DynamoDB region is resolved in order: `-aws-region`, JSON config, AWS environment/shared config, EC2 IMDS, then `us-east-1`.

ClickHouse connection settings are resolved in order: CLI flags, JSON config, `/etc/clickhouse-client/config.xml`, then built-in defaults.

## Worker Container

The worker image is a single Ubuntu-based container with ClickHouse packages, `s5cmd`, and the Go binary copied in from a builder stage. Its entrypoint is the Go worker binary, and the worker runs as root so it can create and write the resolved worker work directory on root-owned host mounts. The worker starts `clickhouse server` as a child process for each claimed `READY` part from DynamoDB. The default ClickHouse version is `26.3.10.60`.

Large worker data should live on the same local filesystem. In production, mount local NVMe at `/mnt/nvme` and set the worker `-work-dir` under that mount, for example `/mnt/nvme/partforge-work`. Each claimed part creates a unique `run-*` directory under `-work-dir`; ClickHouse data, temp files, logs, and pid file live under `run-*/clickhouse`, while downloaded source artifacts live under `run-*/scratch`. The run directory is removed after that part finishes and the child ClickHouse process has stopped. The worker moves source parts into ClickHouse `detached`, freezes produced destination parts, and uploads the frozen part directories with an `s5cmd` glob from `shadow/<freeze>/store/*/*/*`.

Workers run insert-select queries with the detected CPU count and an insert-heavy memory cap. After the insert completes, the worker applies memory-derived `merge_max_block_size` and `merge_max_block_size_bytes` settings plus a lower local `merge_selecting_sleep_ms` to the worker destination table, restarts its local ClickHouse child process with `background_pool_size` set to the larger of the detected CPU count and ClickHouse's minimum valid pool size for default MergeTree mutation settings, and then lets destination merges run. If no merge is active but more than one active output part remains, the worker keeps polling until merges have stayed idle and the active output part count has stayed unchanged for at least two minutes before treating merges as settled; `-merge-timeout` remains the hard ceiling. Use `upload-freeze -optimize-final` to record that this job's workers should run `OPTIMIZE TABLE ... FINAL` on their local worker destination table after each insert-select. Use `worker -optimize-final` to force that same local worker optimize for every job processed by that worker. PartForge never runs `OPTIMIZE FINAL` on the source/upload host.

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

Workers also persist a per-part progress heartbeat to DynamoDB every `15s` by default while a part is being processed, including stages such as S3 download and upload where rewrite counters may not change. Use `-state-progress-interval` to change the interval, or `-state-progress-interval=0` to disable these DynamoDB progress writes without disabling Prometheus metrics.

## Admin

List job IDs from the DynamoDB state table:

```sh
partforge list-jobs
```

Show progress, status counts, and failed part errors for one job:

```sh
partforge job-status \
  -job-id=job-123
```

Use `-json` on either command for machine-readable output. `job-status` also rolls up `IN_PROGRESS` parts by current rewrite stage, such as `insert_select`, `optimize_final`, and `wait_merges`. Use `job-status -parts` to include one row per part with the latest persisted rewrite counters and active part stats, or `job-status -details` to include each part's current rewrite stage and per-stage timings. `list-jobs` scans the state table, so admin IAM needs `dynamodb:Scan`; normal worker/import paths do not.

Retry one failed part:

```sh
partforge retry-failed \
  -job-id=job-123 \
  -part-id=part-abc
```

Retry every failed part in a job:

```sh
partforge retry-failed \
  -job-id=job-123 \
  -all
```

Retry failed parts and any stuck rewrite workers without resetting completed parts:

```sh
partforge retry-failed \
  -job-id=job-123 \
  -all \
  -include-in-progress
```

Retry only stale in-progress rewrite parts whose persisted progress has not changed for at least five minutes:

```sh
partforge retry-failed \
  -job-id=job-123 \
  -stale
```

Use `-stale-after=10m` to change the stale threshold. Parts with no persisted progress are ignored by `-stale`.

Failed rewrite parts are moved back to `READY`. Failed import parts are moved back to `FINISHED`, so `import-finished` retries the import stage instead of re-running the worker. With `-include-in-progress`, `IN_PROGRESS` rewrite parts are also moved back to `READY`. With `-stale`, only `IN_PROGRESS` rewrite parts whose `progress_updated_at` is older than the stale threshold are moved back to `READY`. `retry-failed` uses conditional updates and requires `dynamodb:UpdateItem`.

Force every part in a job back to `READY`, including parts that already succeeded:

```sh
partforge retry-failed \
  -job-id=job-123 \
  -all \
  -force
```

Force one specific part back to `READY`, regardless of its current state:

```sh
partforge retry-failed \
  -job-id=job-123 \
  -part-id=part-abc \
  -force
```

Use `-force` only when the selected part or whole job should be rewritten from the worker stage.

Delete one job's DynamoDB state rows:

```sh
partforge delete-job \
  -job-id=job-123
```

Also delete that job's S3 artifacts:

```sh
partforge delete-job \
  -job-id=job-123 \
  -delete-s3
```

`delete-job -delete-s3` derives the exact `s3://bucket/<prefix>/jobs/<job-id>/*` target from the job's recorded state rows, rejects S5CMD glob metacharacters in the generated bucket or prefix, deletes S3 before deleting DynamoDB state, and fails if the job has no state rows. It requires `dynamodb:Query` and `dynamodb:DeleteItem`; with `-delete-s3`, it also requires S3 list/delete permissions for the recorded job prefix.

## Example

```sh
partforge upload-freeze \
  -database=src_db \
  -table=events \
  -freeze=migration_001 \
  -destination-schema-file=dest.sql \
  -insert-select-file=insert.sql \
  -bucket=partforge \
  -s3-endpoint=http://localhost:4566 \
  -dynamodb-endpoint=http://localhost:4566
```

The destination schema file should contain the full `CREATE TABLE` statement used inside the worker, including the database-qualified table name. The insert file should contain a full statement that writes to that table, such as:

```sql
INSERT INTO dst_db.events_new
SELECT
    *
FROM src_db.events
```

Source-table `Replicated*MergeTree` engines are normalized to their non-replicated `*MergeTree` equivalents inside the worker. The destination schema is executed as provided.

`upload-freeze` discovers every ClickHouse disk from `system.disks`, scans each local disk's `shadow/<freeze>` directory, and includes the disk name in the part identity. S3-backed ClickHouse disks are rejected for now.

`upload-freeze` uploads multiple source parts concurrently with `-upload-concurrency` (default `0`, meaning the detected CPU count). Each upload runs its own `s5cmd` process. To avoid multiplying `s5cmd`'s default worker pool too aggressively, `-s5cmd-numworkers` defaults to auto-sizing from the effective upload concurrency. For example, with concurrency `8`, each process runs with `--numworkers 32`. Set `-s5cmd-numworkers` explicitly to tune per-process parallelism.

Part state is stored in DynamoDB. Workers claim work with conditional updates from `READY` to `IN_PROGRESS`; handled processing errors are written as `FAILED`; successful rewrites become `FINISHED`; and `import-finished` transitions parts through `IMPORTING` to `IMPORTED`. On SIGINT or SIGTERM, a worker stops claiming new work and gives the active part up to `-shutdown-grace-period` to finish; if the grace period expires, the worker cancels processing and conditionally returns its owned part to `READY`. If a worker process dies outside handled code, the part remains visible as `IN_PROGRESS` for manual inspection or reset.

Source part artifacts keep stable S3 prefixes. Finished artifacts contain one uncompressed tarball per produced ClickHouse part under their stable `finished/<part-id>/` prefixes. Before uploading a finished artifact, the worker removes any existing objects under that finished part prefix with `s5cmd rm` and then uploads the newly generated tarballs. The `finished_key` in DynamoDB is updated only after the replacement upload succeeds.

`import-finished` requires the destination table to be empty by default. This is intentional: attaching the same finished artifacts twice would duplicate data, and there is no exact transaction spanning S3 and ClickHouse. Use `-require-empty=false` only when importing into a table that you have verified manually.

For `import-finished`, the default scratch directory is created under the destination ClickHouse disk as `partforge-import-work`, so downloaded parts are on the same filesystem as the destination table's `detached` directory. If `-work-dir` is set explicitly, PartForge checks that it is on the same filesystem as `detached` before downloading anything and fails fast if it is not.
