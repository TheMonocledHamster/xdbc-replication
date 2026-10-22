# Fixed-width row format

`internal/format/row` stores each row as a NULL bitmap followed by fixed-width field slots, in schema order, with no alignment padding.

```text
row = NULL bitmap | field 0 | field 1 | ...
batch payload = row 0 | row 1 | ...
```

Build a `Layout` with `NewLayout([]row.Field{Name, Type, Size})`. Both peers must use the same ordered fields and widths; the handshake does not exchange schemas. `Fields()` returns a copy of the schema.

## Field encodings

| Go constant | Config type | Bytes | Getter/setter value |
|---|---|---|---|
| `Int16` | `SMALLINT` | 2 | `int16` |
| `Int32` | `INT` | 4 | `int32` |
| `Int64` | `BIGINT` | 8 | `int64` |
| `Real` | `REAL` | 4 | `float32` |
| `Double` | `DOUBLE` | 8 | `float64` |
| `Bool` | `BOOLEAN` | 1 | `bool` |
| `Char` | `CHAR` | 1 | `byte` |
| `String` | `STRING` | Configured positive width | `string` |
| `Numeric` | `NUMERIC` | Configured positive width | `string` |
| `Date` | `DATE` | 4 | `int32` days since 2000-01-01 |
| `Timestamp` | `TIMESTAMP` | 8 | `int64` wall-clock microseconds since 2000-01-01 |
| `TimestampTZ` | `TIMESTAMPTZ` | 8 | `int64` UTC microseconds since 2000-01-01T00:00:00Z |

Use the corresponding methods, such as `PutInt64`/`GetInt64`. Integers and IEEE floating-point bits use little endian. BOOLEAN stores only `0` or `1`; other bytes are rejected on read. CHAR represents a byte, not a Unicode character.

STRING and NUMERIC slots are zero-padded, may fill their entire width, and end at the first NUL when read. Their widths count bytes. Writes reject embedded NUL and oversized values without changing the record.

NUMERIC preserves exact decimal text, including fractional trailing zeros. Decimal and scientific notation, `NaN`, and signed `Infinity` are accepted. The codec validates spelling without converting through float64; PostgreSQL enforces database precision and scale constraints.

DATE uses int32 extrema for `DateNegativeInfinity` and `DateInfinity`. Both timestamp types use int64 extrema for `TimestampNegativeInfinity` and `TimestampInfinity`. TIMESTAMP preserves civil time; TIMESTAMPTZ preserves the instant, not the original zone or offset. Conversion does not depend on Unix nanoseconds.

## NULLs

The bitmap occupies `ceil(columnCount / 8)` bytes. For column `i`, bit `i % 8` of byte `i / 8` is **1 for NULL** and 0 otherwise. `RowSize()` includes the bitmap and every field slot, including NULL fields.

- `PutNull(record, col)` sets the NULL bit and clears that field's slot.
- `IsNull(record, col)` checks the bitmap independently of the stored value.
- Typed getters return `ErrNull` for NULL.
- Successful typed writes clear their field's NULL bit; rejected writes leave the record unchanged.

Clear each reused record, then write every field or explicitly mark it NULL. A cleared bitmap means all fields are non-NULL. NULL is distinct from zero, false, empty text, and NaN.

## Batches and borrowed memory

`RowsPerBuffer` includes bitmap space. Rows never span buffers. Before compression:

```text
PayloadLen = UncompressedLen = RecordCount * RowSize()
```

Consumers decompress first and use `ValidatePayload` to check byte length against row count. `RowAt` and `GetStringBytes` return borrowed views: stop using them when buffer ownership changes or the buffer is released.

The [source](../internal/postgres/README.md) and [sink](target-v1.md) both support every type above, including NULL. `make test-postgres` checks their round trip through TCP with both compression modes, including numeric scale and temporal edge cases.

The bitmap replaced an older bitmap-free layout without changing `FormatRow` or the wire version. Upgrade both peers and re-encode old payloads; the handshake cannot detect that difference.

Run `make test` for row codec tests and `make test-postgres` for source mapping tests.
