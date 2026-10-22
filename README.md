# XDBC replication

A Go implementation of the transfer pipeline described in [Fast and Scalable Data Transfer Across Data Systems](https://dl.acm.org/doi/10.1145/3725294).

The current application copies one PostgreSQL table between two PostgreSQL 17 databases. A persistent server reads the source; a persistent client receives and writes the data. Transfers run one at a time, with optional Snappy compression.

## Quick start

Install Docker with Compose v2 and Make, then run from the repository root:

```sh
make e2e
make e2e COMPRESSION=snappy
```

Each command builds the application, starts the services, and runs the same checks:

1. Load the checked-in IoT sample: 128 rows and 85 columns.
2. Transfer twice and compare every column, including duplicate rows.
3. Force a sink write failure, then verify that another transfer succeeds.
4. Transfer an empty source, then restore and transfer the sample.

Both processes stay running. The final source and sink contain the verified sample. No dataset download is needed.

## Run a transfer yourself

```sh
make up        # build and start the databases, server, and client
make seed      # prepare both tables and load the source sample
make transfer  # replace the sink with the source contents
make verify    # compare source and sink
```

You can also trigger the client directly:

```sh
curl -X POST http://localhost:8080/transfer
# {"rows":128,"elapsed_ms":12}

curl http://localhost:8080/healthz
```

Every transfer clears the sink before writing. Batches commit separately, so a failed run may leave partial data. The next run clears it again. A second request during a transfer receives HTTP 409.

To change compression, run `make up COMPRESSION=snappy` or `make up COMPRESSION=none`. This recreates the application containers; database data is retained.

## Inspect and stop

```sh
docker compose ps
docker compose logs -f server client
make down
```

`make down` retains database volumes. To delete this project's stored database data explicitly:

```sh
docker compose down -v
```

Default local connections:

| Service | Address |
|---|---|
| Client HTTP API | `http://localhost:8080` |
| Source PostgreSQL | `localhost:5433` |
| Sink PostgreSQL | `localhost:5434` |

Both databases use database `xdbc_test`, user `postgres`, and password `postgres`. Published ports bind to localhost. See [configuration](docs/configuration.md) to change ports or run commands outside Compose.

## Tests

Go 1.26 is needed for local Go tests; the Compose workflow builds Go inside Docker.

```sh
make test           # race tests and go vet
make test-postgres  # adapters and scalar round trips against two databases
```

`make test-postgres` starts both PostgreSQL 17 databases. It tests the adapters and full scalar transfers over TCP, with and without Snappy. Coverage includes NULLs, exact numeric scale, floating-point special values, and wide dates. Direct Go test runs skip database tests unless their connection variables are set; the Make target supplies them.

CI runs the Go checks, live scalar round trips, and separate Compose IoT E2E jobs for uncompressed and Snappy transfers.

## Documentation

- [Architecture](docs/architecture.md): services, transfer lifecycle, HTTP API, and failure behavior.
- [Configuration](docs/configuration.md): shared schema, buffer settings, and environment variables.
- [Component guide](docs/README.md): row format, protocol, adapters, pools, and queues.
- [IoT fixture](testdata/iot/README.md): sample provenance and checksum.

Both database adapters support NULL and every [row scalar type](docs/row-v1.md#field-encodings): integers, floats, boolean, exact numeric, text/character fields, date, timestamp, and timestamptz. The IoT sample exercises a subset; the live scalar tests cover the full set. Parallel workers, CDC, baseline comparisons, and full-dataset benchmarks are future work.
