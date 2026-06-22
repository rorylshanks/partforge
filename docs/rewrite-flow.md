# Rewrite Flow

This document describes the current part rewrite procedure. There is one worker path; `optimize_final` is an optional step inside that path.

`OPTIMIZE TABLE ... FINAL` only runs inside the local worker ClickHouse process. PartForge never runs `OPTIMIZE FINAL` on the source/upload host or the final import host.

## Job-Level Flow

```mermaid
graph TD
    A[Source ClickHouse table is frozen] --> B[upload-freeze]
    B --> C[Scan frozen part directories]
    C --> D[Write manifest.json into each source part]
    D --> E[Upload raw source part prefixes to S3]
    D --> F[Create DynamoDB READY records]
    E --> G[worker claims READY part]
    F --> G
    G --> H[Rewrite part in local ClickHouse]
    H --> I[Upload finished destination part tarballs to S3]
    I --> J[Mark DynamoDB record FINISHED]
    J --> K[import-finished downloads finished artifacts]
    K --> L[Move parts into final table detached directory]
    L --> M[ALTER TABLE ... ATTACH PART]
    M --> N[Mark DynamoDB record IMPORTED]
```

`upload-freeze -optimize-final` stores `optimize_final: true` in each manifest and affects the derived job and part IDs. `worker -optimize-final` ignores the manifest option and enables the same optional optimize step for every part processed by that worker.

## Worker Part Flow

```mermaid
graph TD
    A[Claim READY part] --> B[Prepare run directory]
    B --> C[Download source artifact from S3]
    C --> D[Read manifest.json]
    D --> E[Create local source and destination tables]
    E --> F[Apply destination compression codec]
    F --> G[Move source part into detached]
    G --> H[ALTER TABLE source ATTACH PART]
    H --> I[Run INSERT INTO destination SELECT ... FROM source]
    I --> J[Apply destination merge settings]
    J --> K[Restart local ClickHouse with merge tuning]
    K --> L[Wait for destination merges]
    L --> M{optimize_final enabled and merges settled?}
    M -- Yes --> N[Run one OPTIMIZE TABLE destination FINAL]
    N --> O[Wait for destination merges again]
    M -- No --> P[Measure active destination parts]
    O --> P
    P --> Q{Any active destination parts?}
    Q -- No --> R[No frozen output parts]
    R --> W[Finished artifact upload fails]
    Q -- Yes --> S[ALTER TABLE destination FREEZE]
    S --> T[Build finished part tarballs]
    T --> U[Upload finished artifact prefix]
    U --> V[Mark part FINISHED]
```

The insert-select step has its own resource retry loop. If ClickHouse returns a retryable resource error such as memory pressure or too many threads, the worker halves `max_insert_threads` and, when present, `max_threads`; drops and recreates the destination table; reapplies only the destination compression codec; waits with a short backoff; and retries the insert-select. Destination merge settings are applied only after the insert-select succeeds.

## Destination Merge Settings

After a successful insert-select and before the ClickHouse restart, the worker applies these destination table settings:

- `merge_max_block_size`
- `merge_max_block_size_bytes`
- `merge_selecting_sleep_ms`
- `max_bytes_to_merge_at_max_space_in_pool`
- `max_bytes_to_merge_at_min_space_in_pool`
- `enable_vertical_merge_algorithm = 0`

The last setting forces horizontal merges.

## Merge Wait

```mermaid
graph TD
    A[Poll system.merges and system.parts] --> B{Merge inspection failed?}
    B -- Yes --> C[Warn and continue with current parts]
    B -- No --> D{No active destination merges?}
    D -- No --> E{Merge timeout reached?}
    E -- No --> A
    E -- Yes --> F[Stop waiting and continue with current parts]
    D -- Yes --> G{Active parts <= settle min OR no small-part debt?}
    G -- Yes --> H[Settled]
    G -- No --> I{Same part snapshot idle for settle min wait?}
    I -- No --> E
    I -- Yes --> H
```

If the merge wait times out or merge-wait inspection fails, that is not a rewrite failure. The worker logs the reason and continues with whatever active destination parts exist.

Small-part debt means the destination table has more than one active part and more than `-merge-small-part-max-count` active parts below `-merge-small-part-bytes`. The default small-part threshold is 1 GiB and the default allowed small-part count is 2.

## Optional optimize_final Step

There is no separate optimize-final path and no normal-path fallback. If `optimize_final` is enabled, the worker first waits for destination merges to settle. If that wait settles, it runs one local `OPTIMIZE TABLE ... FINAL`, then waits for destination merges again before measuring and freezing parts.

If the first merge wait does not settle or cannot inspect merge state, the worker skips `OPTIMIZE FINAL` and continues with the current destination parts. If the optimize request itself returns an error and the worker context was not canceled, the worker logs the error and still performs the second merge wait. ClickHouse may still have started merge work before the client saw an error.

The optimize request uses `send_timeout=0` and `receive_timeout=0`, and does not use `optimize_throw_if_noop=1`. It is not retried, it does not inspect `system.part_log`, and it does not require the table to end with one active part.

## What Gets Frozen

The worker freezes the active destination parts that exist after the merge wait, optional `OPTIMIZE FINAL`, and optional post-optimize merge wait. A single output source part may therefore produce one destination part or several destination parts.

The worker currently expects at least one frozen destination part to upload. If the insert-select writes no rows and no active destination parts exist, the rewrite reaches the no-output path and the finished artifact upload fails rather than marking an empty result finished.
