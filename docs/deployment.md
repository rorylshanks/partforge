# Deployment

The worker image is published on every push to `main` to `ghcr.io/<owner>/partforge`, tagged with the short commit SHA and `latest`. CI also attaches static `linux/amd64` and `linux/arm64` CLI binaries to a GitHub release named after the SHA.

The image is a single Ubuntu container with `clickhouse-server`, `clickhouse-client`, `s5cmd`, and the Go binary. Its entrypoint is the binary and the default command is `worker`. It **runs as root** (so it can write its work directory on root-owned host mounts) and starts a local `clickhouse server` child process for each claimed part.

## Recommended: workers on ECS with an IAM task role

Run the workers as an ECS service and give the task an **IAM role** scoped to the S3 bucket and the DynamoDB table. This is the recommended setup:

- **No static credentials.** The AWS SDK picks up temporary credentials from the ECS task role via the container credentials endpoint. Do not bake access keys into the image or config.
- **Region resolves from the environment.** Set `AWS_REGION` (or `-aws-region`); otherwise it falls back through AWS config, IMDS, then `us-east-1`.
- **Scale by replicas.** More worker tasks = more parts in flight. There is no coordinator to scale — workers claim independently from DynamoDB.

### Task IAM policy

Combine the S3 and DynamoDB permissions. DynamoDB detail (including tighter per-role variants) is in [dynamodb.md](dynamodb.md).

```json
{
  "Version": "2012-10-17",
  "Statement": [
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
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::partforge"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::partforge/*"
    }
  ]
}
```

`s3:DeleteObject` is needed because workers replace finished-artifact prefixes (`s5cmd rm` then upload) and admin `-delete-s3` operations remove artifacts.

### Storage matters

Worker scratch (`-work-dir`) holds the local ClickHouse data plus downloaded source parts, and compaction transiently holds downloaded tarballs, extracted parts, merge output, and re-uploaded tarballs at once. It must be **fast local disk with enough headroom**:

- **EC2 launch type with instance-store NVMe** is best for large parts — mount the NVMe into the container and set `-work-dir` on it (e.g. `/mnt/nvme/partforge-work`).
- **Fargate** works for smaller jobs; raise the task's ephemeral storage and keep `-compact-max-bytes` well below it.

Each claimed part gets its own `run-*` directory that is removed when the part finishes.

### Splitting inserter and compactor

Run the rewrite and compaction stages as separate services to scale them independently:

- `worker -role=inserter` — rewrite only.
- `worker -role=compactor` — compaction only.
- `worker -role=all` (default) — rewrite first, compact when idle.

See [operations.md](operations.md) for the full flag set and metrics.

## Where the other commands run

`worker` is the only stage that belongs on ECS. The other two need local access to a ClickHouse node's disks and generally run there:

- **`upload-freeze`** must run where it can read the source ClickHouse data disks reported by `system.disks`.
- **`import-finished`** must run where its work-dir shares a filesystem with the destination table's `detached` directory (parts are moved, not copied).

Both still need the same S3 + DynamoDB access as the workers.
