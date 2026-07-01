# AGENTS.md

## Verification

Before finishing any code change, always run:

```sh
go mod tidy
go test ./...
./e2e/run.sh
```

If any command cannot be run, report that explicitly with the reason.
