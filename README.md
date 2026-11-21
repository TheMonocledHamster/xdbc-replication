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

## Reproduce Fig. 9b

Five steps, run in order. Steps 1–3 take about 10 minutes; step 4 takes a few hours.

**Machine:** 64 GB RAM (for JDBC), ~60 GB free disk, Mac or Linux. We used one CloudLab xl170 node (Ubuntu 20.04).

### 1. Install Docker, Compose v2, Make and Git

- **Mac:** install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Compose) and start it. In Docker Desktop → Settings → Resources, give Docker all memory and at least 60 GB disk. Then:
  ```sh
  xcode-select --install
  ```
- **Ubuntu:** install Docker Engine and the Compose plugin from [Docker's apt repository](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository), then:
  ```sh
  sudo apt-get install -y make git
  sudo usermod -aG docker $USER
  ```
  Log out and back in so the `docker` group applies.

Check that both print a version:

```sh
docker compose version
make --version
```

### 2. Get the code

```sh
git clone https://github.com/TheMonocledHamster/xdbc-replication.git
cd xdbc-replication
```

### 3. Run the correctness check (about 5 minutes, no download)

```sh
make e2e-all
```

This runs the full pipeline (server, client, both PostgreSQL databases) end to end on the 128-row sample of each dataset that ships with the repo, and verifies every transferred row against the source. It checks correctness only; it doesn't time anything.

It passed if `make` finishes without `make: *** ... Error`. Along the way you'll see:
- `verified <dataset>: 128 rows, ... SHA-256 ...` after each transfer.
- One `{"error": "... violates check constraint \"harness_reject\" ..."}` per dataset. **That's expected:** the check forces a sink failure on purpose, and the next transfer recovers.

### 4. Run the benchmark (a few hours)

```sh
docker compose run --build --rm -e DATASETS=lineitem -e SINKS=csv -e TRANSFER_CONFIG=transfer_multi bench
docker compose run --build --rm -e DATASETS="acs iot icu" -e SINKS=csv -e TRANSFER_CONFIG=transfer_multi -e JDBC=faithful -e JDBC_HEAP_MB=40000 bench
```

The first command times XDBC and native on lineitem. The second adds JDBC on the other three datasets; like the original, JDBC isn't run on lineitem, which Fig. 9b shows as "Timeout after 20m". Each command downloads the datasets it needs the first time (~15 GB of CSV in total). Keep the machine awake: on a Mac, put `caffeinate -i` before each command.

### 5. Read the results

Each command ends with a table of median seconds (3 rounds) per dataset. "Ours" is our Go XDBC; "Native" is `postgres_fdw`; "JDBC" is `jdbc_fdw`. Every timed run is also saved in `bench-results/<run id>.jsonl`.

Our results on one CloudLab xl170 node, next to the original C++ artifact:
- [8 readers](https://claude.ai/artifact/LT7oPDtRSYtc62UjYX729j): the current `transfer_multi` setting, matching the original's `--read-parallelism 8`.
- [3 readers](https://claude.ai/artifact/2b4dNbj2Kcgzk8qqm43GY8): an earlier setting, for comparison.

Options (other datasets, rounds, Postgres → Postgres, JDBC streaming for small machines) are in [`bench/README.md`](bench/README.md).

## Explore the data (optional)

Not needed for Fig. 9b. These commands run one XDBC transfer at a time so you can look at the data.

### On the 128-row samples

```sh
make up DATASET=lineitem         # start both databases, our server and client
make seed DATASET=lineitem       # load the 128-row sample into the source database
make transfer DATASET=lineitem   # copy it to the target database with XDBC
make verify DATASET=lineitem     # compare target to source (row count + SHA-256)
make export DATASET=lineitem     # write the target table to exports/lineitem.csv
```

To write a CSV file with XDBC instead of a target table, restart the client with the CSV sink, then transfer:

```sh
make up DATASET=lineitem SINK_TYPE=csv
make transfer DATASET=lineitem SINK_TYPE=csv   # writes exports/lineitem.csv (no header)
make verify DATASET=lineitem SINK_TYPE=csv
```

Both write `exports/lineitem.csv`, so the second replaces the first. To use several workers per stage, add `TRANSFER_CONFIG=transfer_multi` to every command ([details](docs/configuration.md#workers)). To start a transfer over HTTP instead of `make transfer`:

```sh
curl -X POST http://localhost:8080/transfer
# {"rows":128,"elapsed_ms":12}
```

### On a full dataset

```sh
make fetch DATASET=acs TRANSFER_TIMEOUT=30m      # download to data/acs.csv
make up DATASET=acs TRANSFER_TIMEOUT=30m
make load DATASET=acs TRANSFER_TIMEOUT=30m       # load the full file into the source database
make transfer DATASET=acs TRANSFER_TIMEOUT=30m
make verify DATASET=acs TRANSFER_TIMEOUT=30m
make export DATASET=acs TRANSFER_TIMEOUT=30m
```

Use `make load`, not `make seed`, for full data: `seed` and `e2e` replace the source table with the 128-row sample. Lineitem is 60M rows and may need a longer timeout (e.g. `TRANSFER_TIMEOUT=1h`).

### Inspect and stop

```sh
docker compose ps
docker compose logs -f server client
make down                 # stop; keeps the data
docker compose down -v    # stop and delete this project's databases
```

Default host ports are client `8080`, source PostgreSQL `5433`, and sink PostgreSQL `5434`, all bound to localhost. Both databases use database `xdbc_test`, user `postgres`, and password `postgres`. How transfers behave on failure and under concurrent requests is in [Architecture](docs/architecture.md).

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
