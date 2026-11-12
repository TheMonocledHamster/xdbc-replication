# XDBC replication

A Go implementation of the pipeline in [Fast and Scalable Data Transfer Across Data Systems](https://dl.acm.org/doi/10.1145/3725294).

The application copies a configured table from PostgreSQL 17 and writes it to either a PostgreSQL table or a CSV file. A persistent server reads the source; a persistent client writes the sink. Transfers run one at a time, with optional Snappy compression. The harness defaults to Snappy and the PostgreSQL sink.

## Datasets

Select a dataset with `DATASET`. The default is `iot`.

| Dataset | Table | Columns |
|---|---|---|
| `lineitem` | `lineitem_sf10` | 16 |
| `acs` | `ss13husallm` | 230 |
| `iot` | `iotm` | 85 |
| `icu` | `inputeventsm` | 26 |

Each has a checked-in 128-row real-data sample, a PostgreSQL schema, and a transfer configuration. Lineitem uses the PostgreSQL DDL's BIGINT, exact NUMERIC, and DATE types. See [dataset provenance and input formats](testdata/README.md).

## Quick start

Install Docker with Compose v2 and Make. Run from the repository root:

```sh
make e2e DATASET=acs
make e2e-all
```

`e2e-all` checks all four samples with Snappy. Each run loads the selected sample, verifies repeated transfers, forces a sink failure and checks recovery, tests an empty source, then leaves a verified sample behind. No dataset download is needed for these checks.

## Work interactively

```sh
make up DATASET=lineitem
make seed DATASET=lineitem
make transfer DATASET=lineitem
make verify DATASET=lineitem
make export DATASET=lineitem
```

The export is `exports/lineitem.csv`, with headers and deterministic row ordering. Use the same dataset in subsequent commands. `make up` switches both application processes to that dataset; tables for other datasets remain in the databases.

To use the XDBC CSV sink, start client with `SINK_TYPE=csv`. This will write the rows to `exports/acs.csv`. The harness loads that file into its target table to verify row counts and values. The CSV sink output has no header, matching the configured row schema order. 

You can trigger the active client directly:

```sh
curl -X POST http://localhost:8080/transfer
# {"rows":128,"elapsed_ms":12}
```

The API runs synchronously. A concurrent request receives HTTP 409. Each PostgreSQL transfer clears the selected sink table, then streams all batches through one COPY transaction. Failed transfers roll back all inserts; the preceding truncation remains committed, leaving the table empty.

## Full datasets

Download the selected archive explicitly, extract its data as CSV, and load it:

```sh
make fetch DATASET=acs TRANSFER_TIMEOUT=30m
make up DATASET=acs TRANSFER_TIMEOUT=30m
make load DATASET=acs TRANSFER_TIMEOUT=30m
make transfer DATASET=acs TRANSFER_TIMEOUT=30m
make verify DATASET=acs TRANSFER_TIMEOUT=30m
make export DATASET=acs TRANSFER_TIMEOUT=30m
```

Downloads go to `data/<dataset>.csv`; exports go to `exports/<dataset>.csv`. Downloads and exports use temporary `.part` files and rename them only after success. Full data stays outside Git and Docker images. Lineitem's much larger workload may need a longer timeout.

To use the existing full IoT file in `data/iot.csv`:

```sh
make up DATASET=iot TRANSFER_TIMEOUT=30m
make load DATASET=iot TRANSFER_TIMEOUT=30m
make transfer DATASET=iot TRANSFER_TIMEOUT=30m
make verify DATASET=iot TRANSFER_TIMEOUT=30m
make export DATASET=iot TRANSFER_TIMEOUT=30m
```

`make load` imports the complete file transactionally, with the header setting recorded for that dataset. The authors' full files are headerless; the sample CSVs include headers. Do not use `make seed` or `make e2e` to load full data: they replace the selected source with its small sample.

Verification compares row counts and SHA-256 digests of sorted PostgreSQL CSV output across all columns. Duplicates are retained. Sorting happens in PostgreSQL; Go streams both verification and export without holding the relation in memory.

## Inspect and stop

```sh
docker compose ps
docker compose logs -f server client
make down
```

Default host ports are client `8080`, source PostgreSQL `5433`, and sink PostgreSQL `5434`, all bound to localhost. Both databases use database `xdbc_test`, user `postgres`, and password `postgres`.

`make down` retains data. `docker compose down -v` explicitly removes this project's database volumes.

## Run on separate machines

Use `make up-source` on the source host and `make up-target` on the target host, with `SERVER_ADDR` pointing to the source IP and port. Bind the source transfer and database ports to the experiment interface. See the [two-host guide](docs/multi-machine.md) for complete Compose and native CloudLab commands.

## Tests and documentation

```sh
make test           # Go race tests and vet; requires Go 1.26
make test-postgres  # live adapter and scalar round-trip tests
```

CI runs these checks plus the four-dataset Snappy E2E matrix. Both adapters support NULL and all [row scalar types](docs/row-v1.md#field-encodings).

- [Architecture](docs/architecture.md): lifecycle, HTTP API, and failure behavior.
- [Configuration](docs/configuration.md): dataset selection, settings, and environment variables.
- [Two-host deployment](docs/multi-machine.md): separate source/target hosts, with or without containers.
- [Component guide](docs/README.md): protocol, codecs, adapters, pools, and queues.
- [CSV sink guide](docs/csv-sink-v1.md): CSV output format and client configuration.
- [Dataset guide](testdata/README.md): schemas, source archives, and fixture checksums.
