# PostgreSQL sink adapter

`internal/target` decodes uncompressed row batches and loads an existing PostgreSQL table through COPY. It uses native `pgx` binary `CopyFrom`.

## Supported values

The sink supports NULL and every [row scalar type](row-v1.md#field-encodings): SMALLINT, INT, BIGINT, REAL, DOUBLE, BOOLEAN, CHAR, STRING, NUMERIC, DATE, TIMESTAMP, and TIMESTAMPTZ.

NULL bitmap entries become SQL NULL, distinct from zero, false, and empty text. NUMERIC text is decoded to `pgtype.Numeric` for binary COPY, retaining decimal scale without float conversion. Floating-point NaN, infinity, and signed zero are preserved.

The PostgreSQL decoder produces COPY-ready values in a single column pass. Temporal values go directly from their binary row representation to typed pgx values, without intermediate CSV-formatting values. CSV keeps its separate formatting decoder.

Temporal decoding uses pgx codecs for PostgreSQL epochs, range checks, and infinity values, then supplies typed `pgtype` values to binary COPY. Finite dates and timestamps preserve wide year ranges, BC dates, and microseconds. TIMESTAMP retains civil time; TIMESTAMPTZ retains the instant independently of the sink session time zone. CHAR remains a single byte.

The table must already exist, with compatible column types and names matching the ordered field configuration. The adapter does not create tables or perform a database schema preflight. PostgreSQL rejects incompatible writes.

## API

| Function | Responsibility |
|---|---|
| `OpenPostgres(ctx, url)` | Open and verify a database connection |
| `New(db, config)` | Validate identifiers and row layout; record the binary COPY relation and columns |
| `WriteBatch(ctx, payload, count)` | Check payload size, decode rows, and COPY one batch in one transaction |
| `Consume(ctx, pool, input)` | Stream all batches through one COPY transaction and release their buffers |

`WriteBatch` is a standalone single-batch operation: it skips empty batches and rolls back if decoding or COPY fails. The transfer runtime uses `Consume`, which keeps one binary COPY operation and transaction open across all batches. It finishes COPY and commits only after clean input EOF; decoding, COPY, or cancellation errors roll back every inserted batch.

`Consume` requires uncompressed buffers with matching payload and uncompressed lengths. It releases each popped buffer after success or failure. It does not close its input queue or manage network connections.

The [runtime](architecture.md) truncates the sink before a transfer, supplies decompressed buffers, and closes the input after upstream work finishes. Truncation is committed separately before COPY begins, so a failed transfer leaves an empty target rather than restoring its previous contents.

Run `make test` for validation, NULL decoding, and rollback tests. `make test-postgres` adds live adapter tests and full scalar round trips through TCP in both compression modes. `make e2e` checks the persistent commands with the real IoT sample.
