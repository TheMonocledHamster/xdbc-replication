# CSV Sink V1

## Functionality

- `New(dst, Config)` creates a CSV sink for an `io.Writer`.
- `OpenFile(path, Config)` creates or replaces a CSV output file.
- `WriteBatch(payload, recordCount)` validates and writes fixed-width tuples as CSV records.
- `Consume(ctx, pool, queue)` consumes uncompressed row-format buffers, writes them, and releases them for reuse.
- `Close()` flushes output and closes files opened by `OpenFile`.

## Assumptions

- The sink and source use the same ordered `internal/format/row` schema.
- The sink supports `INT`, `DOUBLE`, `CHAR`, and fixed-width `STRING`; the row format does not represent NULL values.
- CSV fields are escaped using Go's standard `encoding/csv` writer. A header row is optional and uses the configured field names.
- `Consume` expects decompressed buffers. The pipeline coordinator owns network setup, queue closure, and sink shutdown.
- Use one consumer for a CSV output when buffer order must be preserved.

Tests cover CSV type formatting and escaping, malformed payload rejection, buffer release, output errors, and writing to a temporary file.
