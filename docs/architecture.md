# Architecture

The application transfers one configured table between two PostgreSQL 17 databases. `DATASET=lineitem|acs|iot|icu` selects the table and row schema; changing it with `make up` recreates both application processes. The server and client stay running; each transfer uses fresh connections, queues, and buffer pools.

```text
source-db → server → TCP → client → target-db
                          ↑
                     POST /transfer
```

## Services and code

| Service | Responsibility | Entrypoint |
|---|---|---|
| `source-db` | Holds the source table | PostgreSQL 17 |
| `target-db` | Holds the sink table | PostgreSQL 17 |
| `server` | Accepts sequential TCP sessions and reads the source | `cmd/server` |
| `client` | Accepts HTTP requests and writes the sink | `cmd/client` |
| `tools` | Loads, downloads, triggers, verifies, and exports the selected dataset; runs on demand | `cmd/harness` |

`internal/transfer` coordinates the stages. Source and sink behavior remains in `internal/postgres` and `internal/target`, so those adapters can be replaced separately from the control API.

## One transfer

1. The client accepts `POST /transfer` and reserves its single transfer slot.
2. It opens the sink, connects to the server, and negotiates transport settings.
3. It truncates the sink table after a successful handshake.
4. Both sides run their pipeline stages concurrently:

   ```text
   server: PostgreSQL read/row encoding → compression → TCP send
   client: TCP receive → decompression → row decoding/PostgreSQL COPY
   ```

5. The server sends END after successfully producing and sending all batches. The client requires END and waits for downstream writes to finish.
6. The client counts the sink rows, returns the result, and releases the transfer slot.

There is one worker per stage and one TCP connection. Uncompressed transfers bypass the codec. Snappy transfers may send individual batches uncompressed when compression does not reduce their size.

## HTTP API

The client exposes both endpoints. The server exposes only `/healthz` on its internal HTTP port.

| Request | Response |
|---|---|
| `GET /healthz` | `200` when its database is reachable; otherwise `503` |
| `POST /transfer` | `200` with `{"rows":128,"elapsed_ms":12}` after success |
| Concurrent `POST /transfer` | `409` with a JSON error |
| Failed transfer | `500` with a JSON error |
| Cancelled or timed-out transfer | `504` with a JSON error, if the caller is still connected |

`POST /transfer` takes no parameters and runs synchronously against the configured table. There is no job queue or transfer-history API. `elapsed_ms` includes setup and writing, but excludes the final row-count query and independent harness verification.

Readiness checks database connectivity, not whether fixture tables exist. Run `make seed` before a manual transfer. Each transfer defaults to a one-minute timeout (override with `TRANSFER_TIMEOUT`); disconnecting the HTTP caller cancels its active transfer.

## Ownership and failures

Stages exchange buffer IDs through bounded queues. A successful push transfers ownership to the next stage. A stage releases buffers it cannot forward; senders and sink writers release buffers after use.

The coordinator cancels the session on a stage error, waits for workers, and releases queued buffers. Producers close their own output queues. The original stage error is retained for reporting, and a cancelled source does not send a successful END.

The sink commits one transaction per batch. There is no whole-transfer rollback or automatic retry: a failure can leave partial rows. Both processes remain available, and the next request clears the sink before writing again. A server log saying the source was sent does not confirm that the sink committed; the client's response reports transfer completion.

## Harness

`make e2e DATASET=<name>` runs the selected 128-row sample through repeated transfers, failure recovery, and empty input. `make e2e-all` repeats this for all four datasets and both compression modes. CI runs the same eight combinations.

`make load` creates the selected tables if needed and transactionally replaces the source from a full local CSV. `make seed` uses the checked-in sample instead. The sink is cleared by the transfer itself.

`make fetch` explicitly downloads the author's archive, extracts the selected member, and converts native Lineitem pipe records to standard CSV. It preserves commas in comments and validates the gzip stream. Download and export files are published only after the command succeeds.

`make verify` streams canonical, sorted PostgreSQL CSV output from each database into SHA-256 digests. It compares counts and hashes across all columns without assuming a unique key or holding the relation in Go memory. `make export` streams the sink's same canonical CSV to the host. These operations are separate from transfer timing.

`make test-postgres` also checks the transfer runtime against a small scalar relation in two databases, including NULLs, numeric scale, floating-point wire bytes, wide and BC dates, and temporal infinity in both compression modes.
