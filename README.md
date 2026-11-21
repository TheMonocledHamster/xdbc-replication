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

The quick start is a correctness check. It runs the full pipeline (server, client, both PostgreSQL databases) end to end on the 128-row sample of each dataset that ships with the repo, and verifies every transferred row against the source. It does not measure performance; for that, see [Benchmark (Fig. 9b)](#benchmark-fig-9b).

### 1. Install Docker, Compose v2, Make and Git

- **Mac:** install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Compose) and start it. Install Make and Git with `xcode-select --install`.
- **Ubuntu/Linux:** install Docker Engine and the Compose plugin from [Docker's apt repository](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository), plus `sudo apt-get install -y make git`. Then let your user run Docker without `sudo`:
  ```sh
  sudo usermod -aG docker $USER   # then log out and back in
  ```

Check: `docker compose version` and `make --version` both print a version.

### 2. Get the code

```sh
git clone https://github.com/TheMonocledHamster/xdbc-replication.git
cd xdbc-replication
```

### 3. Run the correctness check (about 5 minutes, no download)

```sh
make e2e DATASET=acs
make e2e-all
```

`e2e-all` checks all four datasets using the 128-row samples in the repo. Each run loads the sample, verifies repeated transfers, forces a sink failure and checks recovery, tests an empty source, then leaves a verified sample behind.

What you'll see:
- `verified <dataset>: 128 rows, ... SHA-256 ...` after each transfer.
- One `{"error": "... violates check constraint \"harness_reject\" ..."}` per dataset. **That's expected:** it's the forced sink failure, and the next transfer recovers.
- The run passed if `make` finishes without `make: *** ... Error`.

By default each stage (read, compress, send, receive, decompress, write) runs one worker. To run several per stage, select the multi-worker configuration:

```sh
make e2e DATASET=iot TRANSFER_CONFIG=transfer_multi
```

Worker counts, connection counts, and buffer sizing are described in [Configuration](docs/configuration.md#workers).

### 4. Run the benchmark

To time the transfers and reproduce Fig. 9b on the full datasets, see [Benchmark (Fig. 9b)](#benchmark-fig-9b).

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

## Benchmark (Fig. 9b)

Fig. 9b times Postgres → CSV for three transfer methods: XDBC, Postgres's built-in `postgres_fdw` (native) and `jdbc_fdw` (JDBC).

```sh
docker compose run --build --rm bench
```

That times iot only, with one worker per stage. To reproduce our final figure (all four datasets, several workers per stage, faithful JDBC):

```sh
docker compose run --build --rm -e DATASETS=all -e SINKS=csv -e TRANSFER_CONFIG=transfer_multi bench
docker compose run --build --rm -e DATASETS="acs iot icu" -e SINKS=csv -e TRANSFER_CONFIG=transfer_multi \
  -e JDBC=faithful -e JDBC_HEAP_MB=40000 bench
```

The first run downloads the full datasets (~15 GB of CSV; ~60 GB free disk needed). Faithful JDBC needs a 64 GB machine. Keep the machine awake; the full run takes hours. Results print as a table and are saved in `bench-results/`.

Results on one CloudLab xl170 node, original C++ artifact vs. our Go replication:
- [8 readers](https://claude.ai/artifact/LT7oPDtRSYtc62UjYX729j) (`transfer_multi`; matches the original's `--read-parallelism 8`)
- [3 readers](https://claude.ai/artifact/2b4dNbj2Kcgzk8qqm43GY8) (earlier setting, for comparison)

See [`bench/README.md`](bench/README.md) for options and the commands behind these results.

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
