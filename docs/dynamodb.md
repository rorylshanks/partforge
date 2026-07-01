# DynamoDB state table

PartForge tracks every part of every job as a row in a single DynamoDB table. This is the source of truth for the pipeline: workers claim work with conditional updates, transition parts through their lifecycle, and record progress and compaction lineage here. Because all state lives in DynamoDB, jobs are resumable and can be driven by many workers at once.

The table holds no bulk data — only per-part metadata and pointers to the S3 artifacts. Part data itself lives in S3.

## Schema

- **Primary key:** `pk` (HASH, String) + `sk` (RANGE, String).
- **GSI `gsi1`:** `gsi1pk` (HASH, String) + `gsi1sk` (RANGE, String), projecting all attributes. Used to query jobs by status (`list-jobs`, `job-status`, and status-scoped claims), which keeps workers off full-table scans.

Default table name is `partforge`; override with `-state-table` (or `state_table` in config).

## Creating the table

`PAY_PER_REQUEST` billing is a good default — load is bursty and driven by how many workers run.

```sh
aws dynamodb create-table \
  --table-name partforge \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
    AttributeName=gsi1pk,AttributeType=S AttributeName=gsi1sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
  --global-secondary-indexes \
    '[{"IndexName":"gsi1","KeySchema":[{"AttributeName":"gsi1pk","KeyType":"HASH"},{"AttributeName":"gsi1sk","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]' \
  --billing-mode PAY_PER_REQUEST
```

For local development, LocalStack creates the same table automatically from `localstack/init/01-init.sh`.

## IAM

Grant access to the table **and its `gsi1` index** (`arn:aws:dynamodb:<region>:<account>:table/partforge` and `.../table/partforge/index/*`). The pipeline uses `Query`, `Scan`, `PutItem`, `UpdateItem`, `DeleteItem`, and `TransactWriteItems`.

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:Query",
    "dynamodb:Scan",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
    "dynamodb:TransactWriteItems"
  ],
  "Resource": [
    "arn:aws:dynamodb:us-east-1:123456789012:table/partforge",
    "arn:aws:dynamodb:us-east-1:123456789012:table/partforge/index/*"
  ]
}
```

If you want tighter, per-role policies:

- **`upload-freeze`** — `PutItem` (registers `READY` rows).
- **`worker`** — `Query`, `Scan`, `UpdateItem` (claim and transition parts), plus `TransactWriteItems`/`DeleteItem` for compaction lineage.
- **`import-finished`** — `Query`, `UpdateItem`.
- **Admin commands** — `Query` (including the `gsi1` index) plus `UpdateItem`, and `DeleteItem` for `reset-*`/`delete-*`. See [operations.md](operations.md).

Prefer an IAM role over static keys — see [deployment.md](deployment.md).
