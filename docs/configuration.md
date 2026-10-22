# Configuration

Compose selects `testdata/<dataset>/transfer.json` with `DATASET=lineitem|acs|iot|icu` (default `iot`). The image includes all four schemas, metadata files, and small samples. Rebuild with `make up` after changing them. Full input and exported CSVs are excluded from the image.

## Shared transfer settings

| JSON field | IoT default | Meaning |
|---|---|---|
| `schema` | `public` | PostgreSQL schema on both sides |
| `table` | `iotm` | Table on both sides |
| `fields` | 85 ordered fields | Column name, row type, and slot width in bytes |
| `compression` | `none` | `none` or `snappy` |
| `buffer_size` | `65536` | Maximum batch payload bytes, excluding the 32-byte pool header |
| `pool_size` | `8` | Number of buffers per process, per transfer |
| `queue_capacity` | `2` | Number of buffer IDs per queue |

Column names and field order must match the database projection and sink columns. The source builds an explicit SELECT list from `fields`. The sink table must already exist. The harness prepares the selected dataset using its `schema.sql` and `dataset.json`.

Both peers must use the same ordered schema, including widths. The handshake checks compression and format and accepts the smaller buffer capacity; it does not exchange or validate schemas. Keep both sides configured together.

The source and sink support NULL and all [row scalar types](row-v1.md#field-encodings). Numeric and text fields need enough slot width for their actual byte representation. PostgreSQL enforces the destination column types and constraints; see the [sink adapter](target-v1.md).

### Buffer sizing

A buffer must fit at least one complete encoded row. Rows never span buffers.

The runtime requires:

```text
queue_capacity >= 1
pool_size >= 2 * queue_capacity + 4
```

For the current one-worker stages, this leaves space for queued buffers, active read/write operations, and the compression stage's input and output. Increasing queue capacity without increasing pool size can otherwise exhaust the pool while a stage waits for another buffer.

Each Snappy worker also allocates reusable scratch space sized for the codec's maximum encoded batch length.

## Compose overrides

Pass these to Make or export them in your shell. Direct Compose commands can also read a root `.env` file:

| Variable | Default | Purpose |
|---|---|---|
| `DATASET` | `iot` | Select Lineitem, ACS, IoT, or ICU |
| `COMPRESSION` | `none` | Override the JSON compression setting for both peers |
| `TRANSFER_TIMEOUT` | `1m` | Positive duration for each transfer; use a longer limit for full datasets |
| `CLIENT_PORT` | `8080` | Host port for the client HTTP API |
| `SOURCE_POSTGRES_PORT` | `5433` | Host port for the source database |
| `TARGET_POSTGRES_PORT` | `5434` | Host port for the sink database |
| `DATA_DIR` | `./data` | Host directory mounted read-only as `/data` by tooling |
| `FILE` | `<dataset>.csv` | Full CSV basename within `DATA_DIR` |

```sh
make up COMPRESSION=snappy CLIENT_PORT=8081
make seed
curl -X POST http://localhost:8081/transfer
```

Internal connections use Compose service names and container ports, so changing published ports does not change service-to-service connections. The checked-in Compose file sets the database URLs and configuration path explicitly; edit the service environment or use a Compose override file to change them.

The Makefile also accepts `COMPOSE` to select a Compose command, for example `make up COMPOSE='docker compose -p xdbc-dev'`. Use the same project name for subsequent commands.

## Running Go commands directly

The commands read the following environment variables:

| Variable | Used by | Default or requirement |
|---|---|---|
| `DATABASE_URL` | Server, client | Required; source URL for server, sink URL for client |
| `CONFIG_PATH` | Server, client, harness | `testdata/iot/transfer.json` outside Compose |
| `COMPRESSION` | Server, client, harness | JSON setting when unset |
| `TRANSFER_TIMEOUT` | Server, client, harness | `1m`; accepts positive durations such as `30m` |
| `LISTEN_ADDR` | Server | `:9000` for TCP transfers |
| `SERVER_ADDR` | Client | `localhost:9000` |
| `HTTP_ADDR` | Server, client | `:8080`; use different addresses when sharing a host |
| `SOURCE_DATABASE_URL` | Harness | Source connection URL |
| `TARGET_DATABASE_URL` | Harness | Sink connection URL |
| `CLIENT_URL` | Harness | `http://client:8080` |
| `SERVER_URL` | Harness E2E | `http://server:8080` |
| `DATA_FILE` | Harness load | `/data/<dataset>.csv` |

Run from the repository root so the harness can find its fixture files. For a local health probe, set `CLIENT_URL` to the process being checked, such as `CLIENT_URL=http://localhost:8080 go run ./cmd/harness health`.

`EXPORT_DIR` controls the host export directory (default `./exports`); it is a Make setting, not a container environment variable.

`make test-postgres` supplies three test connection variables using the published database ports:

- `XDBC_TEST_DATABASE_URL`: existing adapter tests.
- `XDBC_TEST_SOURCE_DATABASE_URL`: source for full scalar round trips.
- `XDBC_TEST_TARGET_DATABASE_URL`: sink for full scalar round trips.

The scalar tests exercise TCP and both compression modes, using binary and text source queries and different source/sink session time zones.

The harness reads `dataset.json` beside `CONFIG_PATH`. It records sample dimensions and full archive/header settings. Tool operations use `TRANSFER_TIMEOUT` plus one minute for setup and result handling; health probes remain short. Use the same `DATASET` for `up`, loading, transfer, verification, and export.
