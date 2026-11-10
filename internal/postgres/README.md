# PostgreSQL source adapter

`internal/postgres.Read` executes a query through `pgx`, encodes rows into pooled buffers, and pushes their IDs downstream. The coordinator owns the connection, pool, and output queue. The reader does not create tables or close the connection or queue.

## Reading a query

The query's column count and PostgreSQL type identifiers must match the configured row fields by position. Names are used for diagnostics. Type validation runs even for an empty result.

The reader fills complete rows, emits full batches and a final partial batch, and records sequence, row count, and used byte lengths. Empty input emits no data buffers. It clears reused rows before encoding them and releases any buffer it still owns on error.

## Supported values

| PostgreSQL type | Row type |
|---|---|
| SMALLINT, INTEGER, BIGINT | `SMALLINT`, `INT`, `BIGINT` |
| REAL, DOUBLE PRECISION | `REAL`, `DOUBLE` |
| BOOLEAN | `BOOLEAN` |
| TEXT, VARCHAR, BPCHAR | `STRING`, or `CHAR` for exactly one byte |
| NUMERIC / DECIMAL | `NUMERIC` as exact decimal text |
| DATE, TIMESTAMP, TIMESTAMPTZ | Matching temporal row type |

All source fields support NULL. Integer and floating-point types are not implicitly narrowed; unsupported PostgreSQL type identifiers fail with column context. SERIAL and identity columns use their underlying integer type.

Text and numeric slot widths count bytes. Oversized values fail instead of being truncated. Numeric values retain decimal scale, including zero's scale in binary query mode. Temporal values retain PostgreSQL precision and infinity; TIMESTAMP keeps civil clock fields, while TIMESTAMPTZ keeps the instant.

See the [row format](../../docs/row-v1.md) for exact encodings. The [sink](../../docs/target-v1.md) supports the same nullable scalar types. The IoT sample uses a subset; separate live scalar tests cover the full set.

## Tests

From the repository root:

```sh
make test-postgres
```

This starts both Compose PostgreSQL 17 databases and tests the adapters plus full scalar transfers. For direct Go test runs, `XDBC_TEST_DATABASE_URL` enables adapter tests; `XDBC_TEST_SOURCE_DATABASE_URL` and `XDBC_TEST_TARGET_DATABASE_URL` enable the two-database round trips. Tests skip when their required connection variables are unset.

The mandatory two-database checks run through `make e2e`; see the [project README](../../README.md).
