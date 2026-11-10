# Run on two machines

Use the same checkout, dataset configuration, and timeout on both hosts. The examples use Snappy. The source runs PostgreSQL and the server; the target runs PostgreSQL and the client. Only the client initiates the transfer connection.

The examples use source IP `10.10.1.1` and target IP `10.10.1.2` on a private experiment network. Substitute your assigned addresses. The target must reach source TCP port `9000`; the harness also needs source PostgreSQL port `5433` for loading and verification.

To run several workers per stage, set `workers` in the shared configuration, or export `TRANSFER_CONFIG=transfer_multi` alongside `DATASET` on both hosts. Both hosts must use the same configuration: `workers.network` is the number of connections the target opens to source port `9000`, which needs no additional ports, and the source expects exactly that many. See [Workers](configuration.md#workers).

## With Compose

On the source host, from the repository root:

```sh
export DATASET=iot COMPRESSION=snappy TRANSFER_TIMEOUT=30m
export SERVER_BIND_IP=10.10.1.1 SOURCE_DB_BIND_IP=10.10.1.1
make up-source
```

This starts only `source-db` and `server`. The server uses its local source database and publishes TCP port `9000` on the experiment interface.

On the target host, from the same checkout:

```sh
export DATASET=iot COMPRESSION=snappy TRANSFER_TIMEOUT=30m
export SERVER_ADDR=10.10.1.1:9000
export SOURCE_DATABASE_URL='postgres://postgres:postgres@10.10.1.1:5433/xdbc_test?sslmode=disable'
make up-target

make seed
make transfer
make verify
make export
```

`up-target` starts only `target-db` and `client`; it does not start a local server or source database. The source URL is used by the harness; the client writes to its local `target-db`. Target PostgreSQL and the HTTP API remain bound to localhost by default.

`seed` replaces the selected source with the checked-in 128-row sample. For full data, put `data/iot.csv` on the target host and use `make load` in place of `make seed`. The harness streams that file to source PostgreSQL. Exports are written on the target host to `exports/iot.csv`.

Trigger another transfer from the target host with:

```sh
curl --fail-with-body -X POST http://localhost:8080/transfer
```

To call the API from another experiment host, start the target with `CLIENT_BIND_IP=10.10.1.2` and use `http://10.10.1.2:8080/transfer`. Database bind addresses are independent of the API and transfer addresses. Keep the defaults on interfaces that do not need remote access.

Use `make down` on each host to stop its services while retaining database volumes. To switch datasets, rerun `up-source` and `up-target` with the same new `DATASET`, then load and transfer it. `make up` and `make e2e` start the entire local stack; use them for single-host checks.

CI checks this topology with `XDBC_TEST_HOST_IP=<host-ip> ./scripts/e2e-multi-machine.sh`. It creates two isolated Compose projects, transfers the sample twice through published host ports, verifies both results, and removes its own volumes afterward. The host IP must be reachable from containers; the test uses ports `19000`, `15433`, `15434`, and `18080`.

## Without containers

Install Go and PostgreSQL 17 on both hosts. Create a database on each host and supply reachable database URLs. PostgreSQL must listen on the chosen network interface and allow the harness host to connect. The examples below assume database `xdbc_test`, user/password `postgres`, and port `5432` on each experiment interface.

Build on both hosts from the repository root:

```sh
mkdir -p bin
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
go build -o bin/harness ./cmd/harness
export CONFIG_PATH=testdata/iot/transfer.json
export COMPRESSION=snappy TRANSFER_TIMEOUT=30m
```

On the source host:

```sh
DATABASE_URL='postgres://postgres:postgres@10.10.1.1:5432/xdbc_test?sslmode=disable' \
LISTEN_ADDR=10.10.1.1:9000 HTTP_ADDR=127.0.0.1:8081 \
./bin/server
```

On the target host:

```sh
DATABASE_URL='postgres://postgres:postgres@10.10.1.2:5432/xdbc_test?sslmode=disable' \
SERVER_ADDR=10.10.1.1:9000 HTTP_ADDR=127.0.0.1:8080 \
./bin/client
```

In a separate terminal on the target, with the same `CONFIG_PATH`, `COMPRESSION`, and `TRANSFER_TIMEOUT` exports:

```sh
export SOURCE_DATABASE_URL='postgres://postgres:postgres@10.10.1.1:5432/xdbc_test?sslmode=disable'
export TARGET_DATABASE_URL='postgres://postgres:postgres@10.10.1.2:5432/xdbc_test?sslmode=disable'
export CLIENT_URL=http://127.0.0.1:8080
./bin/harness seed
./bin/harness transfer
./bin/harness verify
mkdir -p exports
./bin/harness export > exports/iot.csv.part && mv exports/iot.csv.part exports/iot.csv
```

For a full CSV, replace `seed` with `DATA_FILE=data/iot.csv ./bin/harness load`. Run the harness from the repository root so it can find the schema and sample files. The server and client need only their configuration file and database URLs; neither depends on Compose DNS names when these addresses are set explicitly.
