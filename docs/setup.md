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

## Running a migration by hand

### 1. Freeze the source table

On the source ClickHouse node:

```sh
clickhouse-client --query "ALTER TABLE src_db.events FREEZE WITH NAME 'migration_001'"
```

### 2. The two SQL files you write

The migration is defined entirely by two files you pass to `upload-freeze`. They are captured into each part's manifest and executed by the worker; almost every mistake in a job traces back to one of them, so it's worth getting the contract exact.

Inside the worker, for each source part, the sequence is:

1. Recreate the **source** table locally from the schema captured at `upload-freeze` time (`SHOW CREATE TABLE`), under its **original `db.table` name**, with `Replicated*MergeTree` normalized to plain `*MergeTree`. Then attach exactly one source part.
2. Run your **destination `CREATE TABLE` verbatim** to create the target table.
3. Run your **`INSERT ... SELECT` verbatim** as the rewrite query.

#### Destination schema (`-destination-schema-file`)

A single, **database-qualified** `CREATE TABLE`. Run verbatim.

- **Database-qualified name is required** — `CREATE TABLE dst_db.events_new (...)`, not `CREATE TABLE events_new (...)`. `upload-freeze` rejects an unqualified name.
- The `db.table` must **differ** from the source `db.table`; both tables coexist in the worker, and identical names fail with `source and destination table names must differ inside the worker`.
- **Use a plain `MergeTree`-family engine, not `Replicated*`.** The destination schema is *not* normalized — it runs as written — and this is a disposable single-node table, so a `Replicated*` engine (or anything expecting Keeper/cluster context) is wrong here.
- Everything else — columns, `ORDER BY`, `PARTITION BY`, per-column codecs, TTLs, table `SETTINGS` — is exactly the shape you want the new table to have. This is also the schema `import-finished` attaches into, so it must match the real destination table.

```sql
CREATE TABLE dst_db.events_new
(
    id UInt64,
    name String,
    amount_text String,          -- was UInt32 `amount`
    event_date Date,
    migrated UInt8               -- new column
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, id)        -- changed sort key
```

#### Insert-select (`-insert-select-file`)

A single `INSERT INTO <dest> SELECT ... FROM <source>` statement. Run verbatim, once per source part, with exactly one part attached to the source table.

- **`INSERT INTO`** must name the same `db.table` as the destination `CREATE TABLE`.
- **`FROM`** must name the original source `db.table` — the `-database`/`-table` you pass to `upload-freeze` (the worker recreates the source under that exact name).
- The `SELECT` list maps source columns to destination columns, in destination column order, same as any ClickHouse insert. This is where the actual transform lives: casts (`toString(amount)`), computed/added columns (`1 AS migrated`), dropped columns (just omit them), row filters (`WHERE ...`).
- It must be **correct per part**. Only one part is attached when it runs, so anything that needs to see the whole table — `GROUP BY`, `DISTINCT`, window functions, joins across sources, `ORDER BY ... LIMIT`, dedup/aggregation — will silently produce wrong output.

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

#### Settings

You don't need to hand-tune performance. The worker derives insert settings (memory cap, `max_threads` / `max_insert_threads`, insert block sizes) and merge settings from the container's detected CPU and memory, and applies the destination `default_compression_codec` (default `ZSTD(5)`, `-default-compression-codec`) before the insert. A query-level `SETTINGS` clause in your `INSERT ... SELECT` is passed through to ClickHouse if you have a specific need, but avoid overriding the memory/thread settings the worker manages. See [operations.md](operations.md) and [rewrite-flow.md](rewrite-flow.md).

### 3. upload-freeze

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

It prints the derived `job-id` (or pass `-job-id` to set one). S3-backed ClickHouse disks are rejected — only local disks are handled. It uploads parts concurrently (`-upload-concurrency`, default = detected CPU count) and auto-sizes `s5cmd`'s worker pool per process (`-s5cmd-numworkers`).

### 4. worker

The worker claims a `READY` part, starts a local ClickHouse, downloads and attaches the source part, runs your `INSERT ... SELECT`, freezes the produced destination parts, uploads one uncompressed tarball per part, and marks the row `COMPACT_READY`. When no rewrite work is left, workers opportunistically compact finished artifacts before promoting them to `FINISHED`.

**Run it via Docker.** Because the worker starts its own `clickhouse-server` and shells out to `s5cmd`, it needs both alongside the binary — the published image (`ghcr.io/<owner>/partforge`) bundles ClickHouse, `s5cmd`, and the binary, so it's the recommended way to run it. Running the bare binary is only practical if `clickhouse-server` and `s5cmd` are already on the host `PATH`.

```sh
docker run --rm \
  ghcr.io/<owner>/partforge:latest \
  worker \
  -s3-endpoint=http://localstack:4566 \
  -dynamodb-endpoint=http://localstack:4566
```

Against LocalStack, `docker compose up worker` runs the same image wired to the compose network. Run as many workers as you like. See [operations.md](operations.md) for the worker flags (`-role`, `-work-dir`, compaction limits) and metrics, and [deployment.md](deployment.md) for running them on ECS.

### 5. import-finished

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
