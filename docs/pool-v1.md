# Buffer pool

`internal/pool` allocates a fixed number of reusable buffers in one contiguous byte slice. Pipeline queues carry buffer IDs instead of copying payloads.

```go
p, err := pool.New(pool.Config{
    NumBuffers: 8,
    PayloadCap: 65536,
})
```

Each buffer contains a 32-byte header followed by `PayloadCap` bytes of payload storage. `PayloadCap()` excludes the header.

## Ownership

| Operation | Behavior |
|---|---|
| `Acquire(ctx)` | Wait for a free buffer; clear its header and return its ID |
| `Buffer(id)` | Borrow the full header-and-payload slice for an acquired ID |
| `Release(id)` | Return the buffer; reject invalid IDs and double release |

The caller owns a buffer until releasing it or successfully pushing its ID downstream. Borrowed slices become invalid for use after that ownership changes. The pool does not clear payload bytes on acquisition; row producers must initialize reused records.

Acquisition respects cancellation. Release does not take a context, so cleanup remains possible after cancellation. Ownership checks catch common mistakes; they do not make concurrent writes to one buffer safe.

## Batch header

`BufferHeader.Encode` and `DecodeHeader` use little-endian integers.

| Offset | Bytes | Field |
|---|---|---|
| 0 | 8 | `Seq`: batch sequence |
| 8 | 8 | `NextRecord`: cumulative row position after this batch |
| 16 | 4 | `RecordCount` |
| 20 | 4 | `PayloadLen`: bytes currently stored |
| 24 | 4 | `UncompressedLen`: row-payload bytes before compression |
| 28 | 1 | `Compression`: actual encoding ID |
| 29 | 1 | `Protocol`: pool metadata |
| 30 | 2 | Reserved fields |

Before compression, both lengths equal `RecordCount * RowSize()`. Compression can change the stored length; it preserves the original length and other metadata.

The [wire protocol](network-v1.md) carries this header inside a separate frame header. That outer header uses big endian.

See [buffer sizing](configuration.md#buffer-sizing) for the runtime's minimum pool size. Run `make test` for acquisition, reuse, header, and ownership checks.
