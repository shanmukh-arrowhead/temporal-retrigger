# retrigger

CLI to retrigger Temporal `post-processor-*` workflows by call ID. Wraps the `temporal` CLI for describe / start / reset, with batch support from a CSV.

## Prerequisites

- Rust (stable)
- [Temporal CLI](https://docs.temporal.io/cli)

## Setup

```sh
cp .env.example .env
# fill in TEMPORAL_API_KEY and TEMPORAL_ADDRESS
cargo build --release
```

`.env` is loaded automatically at startup. Vars can also be passed as CLI flags (`--api-key`, `--address`). TLS is always on. The namespace is derived from the address: `<namespace>.tmprl.cloud:7233` → `<namespace>`; anything else falls back to `default`.

### Available namespaces (account `cug4t`)

| Namespace                      | `TEMPORAL_ADDRESS`                              |
| ------------------------------ | ----------------------------------------------- |
| `common-workers-dev.cug4t`     | `common-workers-dev.cug4t.tmprl.cloud:7233`     |
| `common-workers-dev-in.cug4t`  | `common-workers-dev-in.cug4t.tmprl.cloud:7233`  |
| `common-workers-prod.cug4t`    | `common-workers-prod.cug4t.tmprl.cloud:7233`    |
| `common-workers-prod-in.cug4t` | `common-workers-prod-in.cug4t.tmprl.cloud:7233` |

## Commands

```sh
# Inspect a workflow's status and original input
cargo run --release -- validate <call_id>

# Start a fresh execution with the original input
# (overwrite_transcription + force_recompute_output_variables are forced true)
cargo run --release -- start <call_id>

# Reset an existing workflow to its first task
cargo run --release -- reset <call_id> --reason "why"

# Batch: start a new execution for every call_id in a CSV
cargo run --release -- batch --csv calls.csv --concurrency 50

# Batch dry-run: describe each workflow, don't start anything
cargo run --release -- batch --csv calls.csv --dry-run
```

The CSV is read by `csv_reader::read_call_ids` — one call ID per row. Workflow IDs are formed as `post-processor-<call_id>`.
