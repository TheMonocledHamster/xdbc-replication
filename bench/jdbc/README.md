# JDBC baseline (issue #37)

Adds JDBC to target-db the same way the original authors did: pgspider `jdbc_fdw` +
PostgreSQL JDBC driver 42.7.4 (see `xdbc-sigmod-ari/postgres/Dockerfile:46`,
`pg_xdbc_fdw/docker/Dockerfile_client:96-130`, `pg_xdbc_fdw/experiments/setup_fdw.sql:37-50`).
The timed query is the original's: `COPY (select * from jdbc.<table>) TO '/tmp/out' WITH CSV HEADER`.

Deviations: PostgreSQL 17 (matches our target-db) and JDK 21 (JDK 8 isn't packaged for Debian trixie).

## Run it

```sh
# 1. build the faithful image (no streaming)
docker build -t xdbc-target-jdbc:pg17 bench/jdbc
# 2. swap target-db onto it (data volume is kept)
docker compose -f compose.yaml -f bench/jdbc/compose.jdbc.yaml up -d --no-deps target-db
# 3. load a dataset as usual (DATASET=iot docker compose run --rm tools load), then
docker compose exec -T target-db psql -U postgres -d xdbc_test < bench/jdbc/setup.sql
# 4. time it (original uses a 900 s statement timeout)
time docker compose exec -T target-db psql -U postgres -d xdbc_test \
  -c "SET statement_timeout = 900000" \
  -c "COPY (select * from jdbc.iotm) TO '/tmp/out' WITH CSV HEADER"
```

Switch back to the plain stack with `docker compose up -d --no-deps target-db`.

## Known problem: needs lots of RAM

`jdbc_fdw` never sets a fetch size or turns autocommit off, so the PostgreSQL driver
loads the **entire result set into Java memory** before returning a row. The JVM's default
heap is 1/4 of the machine's RAM. On a 16 GB Mac (12 GB to Docker, ~3 GB heap):

| dataset | result |
|---|---|
| iot | backend crashed after 49 s (JVM SIGSEGV in `jq_get_exception`) |
| acs | "remote server returned an error" after 259 s, heap pinned at ~3 GB |

Rough estimate: needs ~8 GB (icu), ~10 GB (acs), ~13 GB (iot) of heap, so a 32 GB+ machine.
To give Java more memory, add `maxheapsize` (MB) to the server, e.g.
`ALTER SERVER postgres_jdbc_server OPTIONS (ADD maxheapsize '16000');` (takes effect in a new session).

The original script never runs JDBC on lineitem: it records 0 and skips it
(`run_experiments_for_datasets.sh:124-133`), while the paper's figure shows "Timeout after 20m".

## Optional: streaming build (NOT faithful)

`STREAM=1` applies `stream.patch` (autocommit off + `setFetchSize(10000)`), so rows arrive
in chunks and memory stays small:

```sh
docker build --build-arg STREAM=1 -t xdbc-target-jdbc:pg17-stream bench/jdbc
JDBC_IMAGE=xdbc-target-jdbc:pg17-stream docker compose -f compose.yaml -f bench/jdbc/compose.jdbc.yaml up -d --no-deps target-db
```

Smoke test on the same 16 GB Mac, PG → CSV, one run each, all rows copied:
iot 122.4 s (5,833,099 rows, peak ~0.8 GB), acs 61.4 s (1,476,313), icu 59.9 s (8,978,893).
These are faster than the paper's JDBC (iot 435 s, acs 235 s, icu 210 s) partly *because*
of the patch, so label them as a deviation if used.
