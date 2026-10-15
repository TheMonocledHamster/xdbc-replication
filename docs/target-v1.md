# PostgreSQL Target V1

## Functionality

- `New(db, Config)` validates the destination and shared ordered row schema.
- `WriteBatch(ctx, payload, recordCount)` validates fixed-width tuples and loads a batch with PostgreSQL `COPY FROM STDIN` in one transaction.
- `Consume(ctx, pool, queue)` consumes uncompressed row-format buffers, writes them, and releases them for reuse.
- `OpenPostgres(ctx, connectionString)` opens and verifies a PostgreSQL connection using `lib/pq`.

## Assumptions

- The source and target use the same `internal/format/row` schema and field order.
- The target table already exists with compatible PostgreSQL columns. This package does not create tables.
- V1 supports `INT`, `DOUBLE`, `CHAR`, and fixed-width `STRING`; the row format has no NULL representation.
- Buffers must be decompressed before `Consume` receives them.
- The pipeline coordinator owns network setup, buffer-pool and queue creation, and closes the queue after receivers finish.

Tests cover schema and payload validation, COPY transaction behavior, and buffer release.
