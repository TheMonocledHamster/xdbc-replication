# Configuration

Compose selects `testdata/<dataset>/transfer.json` with `DATASET=lineitem|acs|iot|icu` (default `iot`). Each dataset also has a `transfer_multi.json` that runs several workers per stage; select it with `TRANSFER_CONFIG=transfer_multi` (see [Compose overrides](#compose-overrides)). The image includes all four schemas, metadata files, and small samples. Rebuild with `make up` after changing them. Full input and exported CSVs are excluded from the image.

## Shared transfer settings

| JSON field | IoT default | Meaning |
|---|---|---|
| `schema` | `public` | PostgreSQL schema on both sides |
| `table` | `iotm` | Table on both sides |
| `fields` | 85 ordered fields | Column name, row type, and slot width in bytes |
| `compression` | `snappy` | `none` or `snappy`; fixtures default to Snappy |
| `buffer_size` | `65536` | Maximum batch payload bytes, excluding the 32-byte pool header |
| `pool_size` | `8` | Number of buffers per process, per transfer |
| `queue_capacity` | `2` | Number of buffer IDs per queue |
| `workers` | all `1` | Optional per-stage worker counts; see [Workers](#workers) |

Column names and field order must match the database projection and sink columns. The source builds an explicit SELECT list from `fields`. The sink table must already exist. The harness prepares the selected dataset using its `schema.sql` and `dataset.json`.

Both peers must use the same ordered schema, including widths. The handshake checks compression and format and accepts the smaller buffer capacity; it does not exchange or validate schemas. Keep both sides configured together.

The source and sink support NULL and all [row scalar types](row-v1.md#field-encodings). Numeric and text fields need enough slot width for their actual byte representation. PostgreSQL enforces the destination column types and constraints; see the [sink adapter](target-v1.md).

### Workers

The optional `workers` object sets how many workers run in each stage. Every field defaults to `1`, so a configuration without it runs one worker per stage.

```json
"workers": {"read": 3, "compress": 3, "network": 2, "decompress": 3, "write": 3}
```

| Field | Runs in | What runs in parallel |
|---|---|---|
| `read` | Server | Source readers. Each scans its own range of the table's heap pages (by `ctid`) over its own PostgreSQL connection |
| `compress` | Server | Compression workers sharing one input queue |
| `network` | Both | TCP connections. Each has its own sender on the server and receiver on the client |
| `decompress` | Client | Decompression workers sharing one input queue |
| `write` | Client | Sink writers. Each commits whole batches on its own connection |

- A count of `0` or an omitted field means `1`; negative counts are rejected.
- `network` is a connection count that both peers must agree on, because the handshake does not carry it. Keep both processes on the same file. If the client opens fewer connections than the server expects, the server abandons that session after `TRANSFER_TIMEOUT` and keeps listening. Extra client connections would be accepted as the start of the next session.
- With `compression: none`, the `compress` and `decompress` counts have no effect, because that stage is bypassed.
- Batches are independent, so they are not delivered in order. With several workers, rows reach the sink in no defined order. `make verify` compares sorted output and is unaffected.

See [Parallel stages](architecture.md#parallel-stages) for how the stages share queues and how the source is partitioned.

### Buffer sizing

A buffer must fit at least one complete encoded row. Rows never span buffers.

The runtime requires:

```text
queue_capacity >= 1
pool_size >= 2 * queue_capacity + max(server, client)

server = read + 2 * compress + network
client = network + 2 * decompress + write
```

Beyond the two queues, each reader, sender, receiver, and writer holds one buffer, and each compression or decompression worker holds two: its input while it acquires an output. The server and client each allocate their own pool, but they share one configuration file, so the larger of the two requirements applies to both. With one worker per stage, this is `2 * queue_capacity + 4`. A smaller pool can deadlock, with every buffer held by a worker that is waiting for another, so `Validate` rejects it and reports the required size.

The minimum leaves no spare buffers. Raising `pool_size`, `queue_capacity`, or `buffer_size` above it lets stages work ahead of each other instead of waiting for a free buffer. Each process allocates up to `pool_size * (buffer_size + 32)` bytes.

Each Snappy worker also allocates reusable scratch space sized for the codec's maximum encoded batch length.

## Compose overrides

Pass these to Make or export them in your shell. Direct Compose commands can also read a root `.env` file:

| Variable | Default | Purpose |
|---|---|---|
| `DATASET` | `iot` | Select Lineitem, ACS, IoT, or ICU |
| `COMPRESSION` | `snappy` | Harness/Compose default; overrides the JSON setting for both peers |
| `TRANSFER_CONFIG` | `transfer` | Base name of the config in `testdata/<dataset>/`: `transfer` (one worker per stage) or `transfer_multi` (several workers per stage) |
| `TRANSFER_TIMEOUT` | `1m` | Positive duration for each transfer; use a longer limit for full datasets |
| `CLIENT_PORT` | `8080` | Host port for the client HTTP API |
| `CLIENT_BIND_IP` | `127.0.0.1` | Host interface for the client HTTP API |
| `SERVER_ADDR` | `server:9000` | Source TCP address reached by the client |
| `SERVER_BIND_IP` | `127.0.0.1` | Host interface for the source TCP listener |
| `SERVER_PORT` | `9000` | Published source TCP port |
| `SOURCE_POSTGRES_PORT` | `5433` | Host port for the source database |
| `TARGET_POSTGRES_PORT` | `5434` | Host port for the sink database |
| `SOURCE_DB_BIND_IP` | `127.0.0.1` | Host interface for source PostgreSQL |
| `TARGET_DB_BIND_IP` | `127.0.0.1` | Host interface for target PostgreSQL |
| `SOURCE_DATABASE_URL` | Source Compose database | Source connection URL for the server and harness |
| `TARGET_DATABASE_URL` | Target Compose database | Sink connection URL for the client and harness |
| `CLIENT_URL` | `http://client:8080` | Client HTTP URL reached by the harness |
| `SERVER_URL` | `http://server:8080` | Server health URL reached by harness E2E checks |
| `DATA_DIR` | `./data` | Host directory mounted read-only as `/data` by tooling |
| `FILE` | `<dataset>.csv` | Full CSV basename within `DATA_DIR` |

```sh
make up CLIENT_PORT=8081
make seed
curl -X POST http://localhost:8081/transfer
```

To run the multi-worker configuration, pass `TRANSFER_CONFIG` to every command that needs it, including `up`, which recreates the server and client with the new file:

```sh
make e2e DATASET=iot TRANSFER_CONFIG=transfer_multi
```

Local connections default to Compose service names and container ports, so changing published ports does not change service-to-service connections. Override `SERVER_ADDR` and the database URLs to use remote services. Bind IPs select interfaces on the Docker host; they do not change the container listeners. See the [two-host guide](multi-machine.md) for `up-source`, `up-target`, and native execution examples.

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
