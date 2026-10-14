# XDBC-Go PostgreSQL Target Implementation V1

## Functionality Provided

- `OpenPostgres(ctx, connectionString)` opens and verifies a PostgreSQL connection using `lib/pq`
- `New(db, Config)` configures the destination schema, table, and columns
- `WriteBatch(ctx, payload, recordCount)` decodes newline-delimited JSON rows and inserts each batch in a transaction
- `Consume(ctx, pool, queue)` writes received, uncompressed buffers and releases them for reuse
- `RunReceiver(ctx, conn, writer, config)` coordinates the network handshake, receive queue, and target writer

## MVP Assumptions

- Payload rows use newline-delimited JSON arrays, in destination column order
- Batches are uncompressed and use `FormatRow`
- The destination table already exists with matching columns and types
- Snappy decompression and table creation are not handled by this package yet

Tests cover batch validation, inserts and rollback with a fake database, buffer release, and a network-to-target transfer.
