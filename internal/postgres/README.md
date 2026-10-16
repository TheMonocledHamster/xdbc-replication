# PostgreSQL Reader

Go implementation of the PostgreSQL source reader for the XDBC replication project.

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