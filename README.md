# PartForge

Distributed ClickHouse MergeTree part rewriting for large schema migrations.

PartForge rewrites a large ClickHouse table into a new schema without loading the production cluster. Doing it in place - one giant `INSERT INTO new SELECT ... FROM old` or a mutation - competes with production queries for CPU, memory, and disk, and is hard to resume if it fails partway. PartForge instead freezes the source table's parts and rewrites each one on disposable workers:

- **Off-cluster** — the heavy `INSERT ... SELECT` runs in throwaway worker containers, each with its own local ClickHouse; the production cluster only does a cheap `FREEZE` up front and `ATTACH PART` at the end.
- **Parallel and horizontally scalable** — each part is an independent unit of work; add workers to go faster.
- **Resumable** — every part's state lives in DynamoDB, so an interrupted job picks up where it left off and failed parts can be retried.

## When to use it

Use it for row-local schema changes on tables too large to rewrite in place — anything expressible as an `INSERT INTO dest SELECT ... FROM source` that is correct when run **independently on each source part**:

- type changes and casts (`toString(amount)`, `CAST(...)`)
- added, dropped, or computed columns
- changed compression codecs
- changed `ORDER BY` / sort key or `PARTITION BY`
- row filters

**It is not a distributed SQL engine.** Transforms that must see rows across part boundaries do not fit and will produce wrong output: `GROUP BY`, `DISTINCT`, window functions, `ORDER BY ... LIMIT`, joins across sources, and dedup/aggregation.

## How it works

Four stages. State moves through DynamoDB; part data moves through S3 (via `s5cmd`).

```
FREEZE (you)           upload-freeze         worker                    import-finished
ALTER ... FREEZE  -->  scan + upload    -->  per-part rewrite in  -->  attach parts into
on source node         parts to S3;          local ClickHouse;         destination table
                       register READY        compact; FINISHED         (mark IMPORTED)
```

## Getting started

Two SQL files define your migration; everything else is mechanical. Write them, then run four commands.

### 1. Destination schema — `dest.sql`

A single, **database-qualified** `CREATE TABLE` for the new table. The worker runs it **verbatim** to create the target table in its local ClickHouse.

- The `db.table` name must **differ** from the source table's name.
- Use a plain `MergeTree`-family engine (**not** `Replicated*`) — this is a throwaway local table in the worker.
- Columns, `ORDER BY`, `PARTITION BY`, codecs, and table `SETTINGS` are whatever you want the new table to be.

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

### 2. Insert-select — `insert.sql`

A single `INSERT INTO <dest> SELECT ... FROM <source>` statement. The worker runs it **verbatim** against **one attached source part at a time**.

- `INSERT INTO` must name the same `db.table` as `dest.sql`.
- `FROM` must name the original source `db.table` (the `-database`/`-table` you pass to `upload-freeze`). The worker recreates the source table locally under that name, normalizing `Replicated*MergeTree` to plain `MergeTree`.
- The `SELECT` columns line up with the destination columns, exactly like a normal ClickHouse insert.
- It must be correct **per part** — see [When to use it](#when-to-use-it). No `GROUP BY`, `DISTINCT`, windows, joins, or `ORDER BY ... LIMIT`.

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

You don't hand-tune memory or thread settings — the worker derives insert and merge settings from the container's CPU and memory. A query-level `SETTINGS` clause is passed through if you genuinely need one.

### 3. Run the pipeline

Run the worker from the published image (`ghcr.io/<owner>/partforge`) — it starts its own `clickhouse-server` and shells out to `s5cmd`, both of which the image bundles. `upload-freeze` and `import-finished` use the `partforge` binary directly, near the ClickHouse nodes.

```sh
# a. Freeze the source table (on the source ClickHouse node)
clickhouse-client --query "ALTER TABLE src_db.events FREEZE WITH NAME 'migration_001'"

# b. Register parts — reads the source ClickHouse disks, uploads to S3
partforge upload-freeze \
  -database=src_db -table=events -freeze=migration_001 \
  -destination-schema-file=dest.sql -insert-select-file=insert.sql \
  -bucket=partforge

# c. Rewrite — run as many worker containers as you want
docker run --rm ghcr.io/posthog/partforge:latest worker

# d. Import the finished parts into the destination table
partforge import-finished -database=dst_db -table=events_new -job-id=<job-id>
```

`upload-freeze` prints the `job-id`. For LocalStack add `-s3-endpoint=http://localhost:4566 -dynamodb-endpoint=http://localhost:4566` to each command. Scale the rewrite by running more worker containers, ideally on ECS — see [docs/deployment.md](docs/deployment.md). Full flag reference, config, and per-stage detail are in **[docs/setup.md](docs/setup.md)**.

Part state lifecycle (tracked in DynamoDB, so a job is resumable):

```
READY -> IN_PROGRESS -> COMPACT_READY <-> COMPACTING -> FINISHED -> IMPORTING -> IMPORTED
                             (failures land in FAILED and can be retried)
```

## Documentation

- [docs/setup.md](docs/setup.md) — requirements, configuration, and running the four stages by hand
- [docs/dynamodb.md](docs/dynamodb.md) — the DynamoDB state table: schema, creation, and IAM
- [docs/deployment.md](docs/deployment.md) — running workers on ECS with IAM roles, and scaling
- [docs/operations.md](docs/operations.md) — worker flags, metrics, and admin/recovery commands
- [docs/development.md](docs/development.md) — building, testing, and project layout
- [docs/rewrite-flow.md](docs/rewrite-flow.md) — detailed rewrite, merge-wait, and compaction behavior
