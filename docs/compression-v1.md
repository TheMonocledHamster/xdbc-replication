# Compression

`internal/compression` provides the Snappy codec and compression/decompression workers. The runtime supports two modes:

| Mode | Behavior |
|---|---|
| `none` (`0`) | Forward the original buffer ID without applying a codec |
| `snappy` (`1`) | Compress each batch; keep the original encoding if it does not shrink |

There is no `NoneCodec`: `ByID` currently constructs only the Snappy codec. The runtime handles `none` by bypassing the compression stage.

## Codec and stages

`Codec` exposes `ID`, `MaxEncodedLen`, `Compress`, and `Decompress`. Methods write into caller-provided storage and return the number of bytes written or an error. Snappy compression needs a destination large enough for `MaxEncodedLen`, even if the final result is smaller.

`StartCompressStage` consumes IDs from an input queue and forwards output IDs. Set `isDecompress` to select the direction. It waits for its workers and returns an error if a worker fails; the coordinator owns queue closure.

Compression uses one reusable scratch buffer per worker and a second pooled buffer for output. If the result is not smaller, it copies the original payload and marks it uncompressed. Decompression passes uncompressed batches through unchanged; Snappy batches are decoded and checked against `UncompressedLen`.

A successful output push transfers ownership downstream. The stage releases its input and any unforwarded output on failure. The [runtime pool budget](configuration.md#buffer-sizing) ensures a worker can acquire an output while holding its input.

## Batch metadata

- `Compression` describes the actual payload, even in a Snappy session.
- `PayloadLen` is the encoded length currently stored.
- `UncompressedLen` remains the original row-payload length.
- Successful decompression sets `Compression` to none and restores `PayloadLen`.

The [network layer](network-v1.md) accepts uncompressed fallback in Snappy sessions. It transports bytes; it does not apply codecs.

Run `make test` for codec, stage, and fallback tests, or `make e2e COMPRESSION=snappy` for the database-to-database check.
