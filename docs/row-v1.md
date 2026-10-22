# Fixed-width row format

`internal/format/row` encodes one row as a NULL bitmap followed by fixed-width
field slots in schema order. There is no alignment padding. Both peers configure
an identical ordered `[]row.Field{Name, Type, Size}`; the schema is not sent by the
network handshake. `Layout.Fields()` returns a defensive copy of that schema.

## NULL representation

The bitmap occupies `ceil(columnCount / 8)` bytes. Column `i` uses bit `i % 8`
of byte `i / 8` (least significant bit first): **1 is NULL**, 0 is non-NULL.
All columns can contain NULL. Unused bitmap bits are zero in initialized rows.
`RowSize()` includes the bitmap and all field slots.

- `PutNull(record, col)` sets the NULL bit and clears that column's slot.
- `IsNull(record, col)` checks validity independently of the stored value.
- Typed getters return `ErrNull` for NULL (`errors.Is(err, row.ErrNull)`).
- Successful typed writes clear their column's NULL bit. Rejected writes leave
  the entire record unchanged, including the bitmap.
- Clear each record before writing it, particularly when recycling a pooled
  buffer. A zeroed bitmap means all fields are non-NULL; write every column or
  explicitly call `PutNull` for it.

NULL is distinct from integer zero, false, empty string, floating-point NaN,
and NUMERIC NaN. NULL slots retain their fixed width, so row offsets and payload
validation do not depend on values.

## Scalar types

| Dtype constant | Type value | Size | Getter/setter Go value |
|---|---|---:|---|
| `Int16` | SMALLINT | 2 | `int16` |
| `Int32` | INT | 4 | `int32` |
| `Int64` | BIGINT | 8 | `int64` |
| `Real` | REAL | 4 | `float32` |
| `Double` | DOUBLE | 8 | `float64` |
| `Bool` | BOOLEAN | 1 | `bool` |
| `Char` | CHAR | 1 | `byte` |
| `String` | STRING | positive configured width | `string` |
| `Numeric` | NUMERIC | positive configured width | `string` |
| `Date` | DATE | 4 | `int32` days since 2000-01-01 |
| `Timestamp` | TIMESTAMP | 8 | `int64` wall-clock microseconds since 2000-01-01 |
| `TimestampTZ` | TIMESTAMPTZ | 8 | `int64` UTC-instant microseconds since 2000-01-01T00:00:00Z |

Use corresponding `PutInt64`/`GetInt64`, `PutNumeric`/`GetNumeric`, etc.
Integers and IEEE floating-point bits are little endian. BOOLEAN contains only
0 or 1; the getter rejects other bytes. Text slots are zero-padded, may fill
their entire width, and end at the first NUL when read. STRING and NUMERIC reject
embedded NUL and oversized values; their `Size` counts bytes, not characters.

NUMERIC stores exact decimal text with its fractional trailing zeros intact.
Decimal/scientific notation, `NaN`, `Infinity`, `+Infinity`, and `-Infinity` are
accepted. It never converts through float64, rounds, or truncates. The codec
validates numeric spelling; PostgreSQL owns precision/scale constraints. Set a
slot width large enough for the actual PostgreSQL output (including sign and
point). Unconstrained NUMERIC values that exceed that width cause a clear error.

`DateNegativeInfinity` and `DateInfinity` are int32 extrema;
`TimestampNegativeInfinity` and `TimestampInfinity` are int64 extrema, used for
both timestamp types. TIMESTAMP keeps civil clock fields; TIMESTAMPTZ keeps the
instant and does not store the original zone name or offset. No conversion uses
UnixNano, so dates outside its restricted year range remain supported.

## PostgreSQL source mapping

The source reader validates query column count and OIDs against the layout before
acquiring buffers, then scans reusable typed pgx destinations. Columns match by
position; schema names are used in diagnostics. SMALLINT, INTEGER, BIGINT, REAL,
DOUBLE PRECISION, BOOLEAN, NUMERIC/DECIMAL, DATE, TIMESTAMP, and TIMESTAMPTZ map to
their matching types without implicit narrowing or float/decimal conversion.
SERIAL and identity columns use their underlying integer OIDs.

TEXT, VARCHAR, and BPCHAR map to STRING. They can map to CHAR only when each
non-NULL value is exactly one byte. CHAR here is a byte slot, not a Unicode
character abstraction. Unsupported OIDs (including domains and arrays) fail with
column context, even for an empty result. No source type is silently converted
to text merely because a string field was configured.

The reader preserves NUMERIC scale in binary and text query modes. pgx v5's
binary zero-digit NUMERIC decoder discards zero's display scale; the adapter
restores that scale from the PostgreSQL wire header before exact text encoding.

## Buffer flow and compatibility

`RowsPerBuffer`, `RowAt`, and `ValidatePayload` include the bitmap. Rows never span
buffers. Before compression, `PayloadLen` and `UncompressedLen` equal
`RecordCount * RowSize()`. Consumers must decompress and validate the used
payload before taking borrowed row views.

This replaces the earlier bitmap-free row layout. **Existing encoded rows must
be re-encoded and both endpoints upgraded together.** `protocol.FormatRow` and
the network envelope remain unchanged; the handshake does not detect an old
row implementation.

Run `go test -race -cover -count=1 -timeout 60s ./...` for the full suite.
For live PostgreSQL tests, use the PostgreSQL 17 fixture and `make test-postgres`,
or supply `XDBC_TEST_DATABASE_URL` when running `go test ./internal/postgres/...`.
