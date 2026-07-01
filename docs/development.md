# Development

## Checks

CI (`.github/workflows/release.yml`) enforces formatting, tidy modules, vet, and tests. Run what CI runs before finishing a change:

```sh
gofmt -l .            # must print nothing
go mod tidy           # must not change go.mod / go.sum
go vet ./...
go test ./...
./e2e/run.sh          # requires Docker
```

`AGENTS.md` lists `go mod tidy && go test ./... && ./e2e/run.sh` as the required pre-commit check.

The e2e script stands up LocalStack + a ClickHouse container, builds the worker image, and runs the full pipeline against `e2e/sql/`, diffing the result against `e2e/expected.tsv`. It builds the image each run; set `PARTFORGE_E2E_SKIP_BUILD=1` to reuse an existing `partforge-worker:latest`.

## Build

```sh
go build -o partforge ./cmd/partforge          # local CLI
docker compose build worker                     # worker image
```

## Project layout

```
cmd/partforge/     CLI entrypoint — every subcommand, flag parsing, config resolution
internal/
  freeze/          discover ClickHouse disks; scan shadow/<freeze> for frozen parts
  manifest/        per-part manifest.json; job/part ID derivation
  artifact/        write manifests; build/extract part tarballs
  s3copy/          s5cmd wrapper for directory/glob transfers
  state/           DynamoDB state store — claims, transitions, compaction batches, admin ops
  chproc/          start/stop the local clickhouse-server child process
  chhttp/          ClickHouse HTTP client
  ddl/             CREATE TABLE normalization (Replicated* -> plain MergeTree)
  rewrite/         the worker: per-part processor + compactor
  resources/       CPU/memory detection -> ClickHouse insert & merge tuning
  parts/           import-finished: attach part tarballs into the destination table
  metrics/         Prometheus recorder + metrics HTTP server
  fileutil/        filesystem copy and directory stats
```

Where to change things:

- rewrite / merge-wait / compaction logic → `internal/rewrite`
- state transitions and admin operations → `internal/state`
- ClickHouse tuning heuristics → `internal/resources`
- CLI flags and command wiring → `cmd/partforge/main.go`

## Release

On push to `main`, CI builds and publishes the multi-arch worker image to `ghcr.io/<owner>/partforge` (tagged with the short commit SHA and `latest`) and attaches static `linux/amd64` and `linux/arm64` CLI binaries to a GitHub release named after the SHA. See [deployment.md](deployment.md).
