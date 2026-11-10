# PostgreSQL sink adapter

`internal/target` decodes uncompressed row batches and loads an existing PostgreSQL table through COPY. It uses `database/sql` with `lib/pq`.

## Supported values

The sink supports NULL and every [row scalar type](row-v1.md#field-encodings): SMALLINT, INT, BIGINT, REAL, DOUBLE, BOOLEAN, CHAR, STRING, NUMERIC, DATE, TIMESTAMP, and TIMESTAMPTZ.

NULL bitmap entries become SQL NULL, distinct from zero, false, and empty text. NUMERIC is passed as exact text, retaining its decimal scale without float conversion. Floating-point NaN, infinity, and signed zero are preserved.

Temporal decoding uses pgx codecs for PostgreSQL epochs, range checks, and infinity values, then passes database/sql values to lib/pq COPY. Finite dates and timestamps preserve wide year ranges, BC dates, and microseconds. TIMESTAMP retains civil time; TIMESTAMPTZ retains the instant independently of the sink session time zone. CHAR remains a single byte.

The table must already exist, with compatible column types and names matching the ordered field configuration. The adapter does not create tables or perform a database schema preflight. PostgreSQL rejects incompatible writes.

## API

| Function | Responsibility |
|---|---|
| `OpenPostgres(ctx, url)` | Open and verify a database connection |
| `New(db, config)` | Validate identifiers and row layout; prepare the COPY statement text |
| `WriteBatch(ctx, payload, count)` | Check payload size, decode rows, and COPY one batch in one transaction |
| `Consume(ctx, pool, input)` | Consume batches and release their buffers |

`WriteBatch` skips empty batches and rolls back a batch if decoding or COPY fails. Earlier committed batches remain in the table. There is no whole-transfer transaction.

`Consume` requires uncompressed buffers with matching payload and uncompressed lengths. It releases each popped buffer after success or failure. It does not close its input queue or manage network connections.

The [runtime](architecture.md) truncates the sink before a transfer, supplies decompressed buffers, and closes the input after upstream work finishes. After failure, the next run clears any partial data.

Run `make test` for validation, NULL decoding, and rollback tests. `make test-postgres` adds live adapter tests and full scalar round trips through TCP in both compression modes. `make e2e` checks the persistent commands with the real IoT sample.
