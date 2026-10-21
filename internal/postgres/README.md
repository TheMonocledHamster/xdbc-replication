# PostgreSQL Reader

Go implementation of the PostgreSQL source reader for the XDBC replication project.

The reader supports nullable core scalar columns: SMALLINT, INTEGER, BIGINT,
BOOLEAN, REAL, DOUBLE PRECISION, NUMERIC/DECIMAL, DATE, TIMESTAMP, TIMESTAMPTZ,
and TEXT/VARCHAR/BPCHAR. NUMERIC is exact text with a caller-selected slot width;
temporal values retain PostgreSQL precision and infinity. Query column count and
OIDs must match the ordered layout. See [the row format](../../docs/row-v1.md)
for encodings, NULL APIs, type mappings, and the bitmap compatibility change.

## Prerequisites

- Go
- Docker Desktop (or Docker Engine with Compose)
- Make

## Running Tests

From the TOP LEVEL repository root, start PostgreSQL 17:

```bash
docker compose up -d
```

Run the reader tests:

```bash
make test-postgres
```

By default, the tests connect to PostgreSQL inside Docker at `localhost:5433`, using the `xdbc_test` database.

Each test creates its own temporary table and inserts test data, so no manual table creation or dataset import is required.

The suite also checks nullable mixed scalar rows in binary and text query modes,
buffer reuse, exact decimal scale, temporal infinity and wide date ranges,
schema rejection, and cancellation cleanup.

To see individual test results:

```bash
make test-postgres GOFLAGS=-v
```

## Running Tests with a Custom Port
The Makefile automatically detects PostgreSQL's Docker host port.

To use a different port:

```bash
POSTGRES_PORT=5544 docker compose up -d
make test-postgres
```

## Stopping PostgreSQL

```bash
docker compose down
```

The Docker volume preserves database data between restarts.

To remove the database and its stored data:

```bash
docker compose down -v
```

## Next Steps

Validate the reader against the authors' real IoT benchmark dataset using their original PostgreSQL schema.
