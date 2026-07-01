# Setup

Requirements, configuration, and how to run the four stages by hand. For the high-level overview see the [README](../README.md); for a working example end to end, run `./e2e/run.sh`.

## Requirements

- **Go 1.24+** — to build the CLI (`go build -o partforge ./cmd/partforge`).
- **Docker + Docker Compose** — to build/run the worker image and the local stack.
- **`s5cmd`** on `PATH` for any command that moves S3 data as a local binary (`-s5cmd-binary` to override). The worker image bundles it.
- **A DynamoDB table** with a `gsi1` GSI and an **S3 bucket** — see [dynamodb.md](dynamodb.md). LocalStack provides both for local runs.
- **ClickHouse** — a source to freeze from and a destination to import into. The worker brings its own ClickHouse (default server version `26.3.10.60`, baked into the image).

## Local stack (LocalStack)

```sh
docker compose up -d localstack
```

This creates the `partforge` S3 bucket and DynamoDB table (via `localstack/init/`). Point commands at it with:

```
-s3-endpoint=http://localhost:4566
-dynamodb-endpoint=http://localhost:4566
-state-table=partforge
```

Against real AWS you omit the two endpoint flags; the SDK resolves the region and credentials from the environment.

## Configuration

Every command reads defaults from `/etc/partforge/config.json` when it exists; override the path with `-config`. **CLI flags always win.** Keys accept flag-style (`aws-region`) or JSON-style (`aws_region`) names. Top-level keys apply to all commands; keys under `commands.<name>` override them for that command.

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
      "state_progress_interval": "15s"
    },
    "import-finished": {
      "clickhouse_url": "http://clickhouse:8123"
    }
  }
}
```

Resolution order for the two settings that aren't plain flags:

- **DynamoDB region:** `-aws-region` → JSON config → AWS env/shared config → EC2 IMDS → `us-east-1`.
- **ClickHouse connection:** CLI flags → JSON config → `/etc/clickhouse-client/config.xml` → built-in defaults.

## Running the four stages by hand

### 1. Freeze the source table

On the source ClickHouse node:

```sh
clickhouse-client --query "ALTER TABLE src_db.events FREEZE WITH NAME 'migration_001'"
```

### 2. upload-freeze

Run where the ClickHouse disk paths from `system.disks` are readable. It scans each disk's `shadow/<freeze>` directory, writes a `manifest.json` into every frozen part, uploads the raw part directory to S3, and registers a `READY` row per part.

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

`dest.sql` holds the full, database-qualified `CREATE TABLE` the worker builds locally. `insert.sql` holds the full statement that writes into it:

```sql
INSERT INTO dst_db.events_new
SELECT
    id,
    name,
    toString(amount) AS amount_text,
    event_date,
    1 AS migrated
FROM src_db.events
```

It prints the derived `job-id` (or pass `-job-id` to set one). Source `Replicated*MergeTree` engines are normalized to plain `*MergeTree` inside the worker; the destination schema runs exactly as written. S3-backed ClickHouse disks are rejected — only local disks are handled.

`upload-freeze` uploads parts concurrently (`-upload-concurrency`, default = detected CPU count) and auto-sizes `s5cmd`'s worker pool per process (`-s5cmd-numworkers`).

### 3. worker

Runs in a container that also has `clickhouse-server` and `s5cmd`. Claims a `READY` part, starts a local ClickHouse, downloads and attaches the source part, runs your `INSERT ... SELECT`, freezes the produced destination parts, uploads one uncompressed tarball per part, and marks the row `COMPACT_READY`. When no rewrite work is left, workers opportunistically compact finished artifacts before promoting them to `FINISHED`.

```sh
partforge worker \
  -s3-endpoint=http://localhost:4566 \
  -dynamodb-endpoint=http://localhost:4566
```

Run as many workers as you like. See [operations.md](operations.md) for the worker flags (`-role`, `-work-dir`, compaction limits) and metrics.

### 4. import-finished

Run near the destination ClickHouse node. Downloads each `FINISHED` artifact, extracts the part tarballs into the destination table's `detached` directory, and runs `ALTER TABLE ... ATTACH PART`.

```sh
partforge import-finished \
  -database=dst_db \
  -table=events_new \
  -job-id=<job-id> \
  -clickhouse-url=http://destination:8123 \
  -s3-endpoint=http://localhost:4566 \
  -dynamodb-endpoint=http://localhost:4566
```

Notes:

- **The destination table must be empty by default** (`-require-empty=true`). Attaching the same artifacts twice duplicates data, and there is no transaction spanning S3 and ClickHouse. Use `-require-empty=false` only after verifying the target manually.
- `import-finished -part-id=<part-id>` imports a single finished part, for controlling import load. After the first attach the table is no longer empty, so subsequent single-part imports need `-require-empty=false`.
- The import work-dir must be on the **same filesystem** as the destination table's `detached` directory (parts are moved, not copied). Unset, it defaults to a directory on the destination ClickHouse disk; if `-work-dir` is set and fails that check, the command errors before downloading anything.

Track progress with `partforge list-jobs` and `partforge job-status -job-id=<id>` (see [operations.md](operations.md)).
