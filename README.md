# PartForge

Distributed ClickHouse MergeTree part rewriting for large schema migrations.

## What it is

PartForge rewrites a large ClickHouse table into a new schema **off the production cluster**. Instead of running one giant `INSERT INTO new SELECT ... FROM old` (or a mutation) on the live cluster, it freezes the source table's parts, ships each frozen part to disposable workers that each run their own local ClickHouse, rewrites parts in parallel, optionally compacts the output, and attaches the finished parts into the destination table.

## The problem it solves

Rewriting a very large ClickHouse table in place is slow and disruptive: the rewrite competes with production queries for CPU, memory, and disk on the cluster you can least afford to overload, and a single long-running statement is hard to resume if it fails partway.

PartForge moves that work off the cluster and breaks it into independent, per-part units:

- **Off-cluster.** The heavy `INSERT ... SELECT` runs in throwaway worker containers, not on your production nodes. The cluster only does a cheap `FREEZE` at the start and `ATTACH PART` at the end.
- **Parallel and horizontally scalable.** Each part is an independent unit of work; add more workers to go faster.
- **Resumable.** Every part's state lives in DynamoDB, so a failed or interrupted job picks up where it left off, and individual failed parts can be retried.

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

Part state lifecycle:

```
READY -> IN_PROGRESS -> COMPACT_READY <-> COMPACTING -> FINISHED -> IMPORTING -> IMPORTED
                             (failures land in FAILED and can be retried)
```

1. **Freeze** the source table: `ALTER TABLE db.table FREEZE WITH NAME 'name'`.
2. **`upload-freeze`** scans `system.disks`, writes a manifest into each frozen part, uploads it to S3, and registers a `READY` row per part.
3. **`worker`** claims a part, rewrites it in a local ClickHouse using your `INSERT ... SELECT`, uploads the result, and — when no rewrite work is left — compacts finished artifacts into fewer, larger parts before promoting them to `FINISHED`.
4. **`import-finished`** attaches the finished parts into the destination table near the destination node.

## Try it

The end-to-end script runs the whole pipeline against LocalStack and a ClickHouse container and verifies the result. It's the fastest way to see PartForge work and doubles as a minimal example (the migration it runs lives in `e2e/sql/`):

```sh
./e2e/run.sh    # requires Docker
```

## Documentation

- [docs/setup.md](docs/setup.md) — requirements, configuration, and running the four stages by hand
- [docs/dynamodb.md](docs/dynamodb.md) — the DynamoDB state table: schema, creation, and IAM
- [docs/deployment.md](docs/deployment.md) — running workers on ECS with IAM roles, and scaling
- [docs/operations.md](docs/operations.md) — worker flags, metrics, and admin/recovery commands
- [docs/development.md](docs/development.md) — building, testing, and project layout
- [docs/rewrite-flow.md](docs/rewrite-flow.md) — detailed rewrite, merge-wait, and compaction behavior
