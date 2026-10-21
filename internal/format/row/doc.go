// Package row encodes contiguous fixed-width rows without alignment padding.
// Each row starts with ceil(columnCount/8) NULL-bitmap bytes, followed by the
// ordered field slots. Column i uses bit i%8 of byte i/8; a set bit means NULL.
// A zeroed row starts with all values non-NULL. PutNull scrubs the field slot;
// successful typed writes clear its NULL bit and typed reads return ErrNull.
//
// INT/SMALLINT/BIGINT are little-endian signed integers. DOUBLE/REAL preserve
// IEEE 754 bits. BOOLEAN is 0 or 1; CHAR is one byte. STRING and NUMERIC widths
// count bytes: text is zero-padded and cannot contain embedded NUL. NUMERIC is
// exact decimal text, including scale, NaN and signed Infinity, never float64.
// Oversized text is rejected without changing the row.
//
// DATE stores int32 days since 2000-01-01. TIMESTAMP stores int64 wall-clock
// microseconds since 2000-01-01; TIMESTAMPTZ stores int64 UTC-instant microseconds
// since the same epoch. Signed extrema represent the named infinity sentinels.
//
// Layouts must be constructed with NewLayout. Both peers must configure the same
// ordered schema, including field widths. protocol.FormatRow identifies this
// format; the network handshake does not exchange schemas. The NULL bitmap
// replaces the earlier bitmap-free layout: upgrade both peers and re-encode old
// payloads. The outer network header and FormatRow ID are unchanged.
//
// Producers clear each row before filling it, including when reusing a pool
// buffer. RowsPerBuffer includes bitmap space; rows never span buffers. Before
// compression, set RecordCount and both PayloadLen and UncompressedLen to the
// actual row count and used payload length. Compression is a separate stage.
//
// Consumers decompress when needed, check the length against UncompressedLen,
// and call ValidatePayload with RecordCount. RowAt and GetStringBytes borrow
// memory until buffer ownership transfers or the buffer is released.
package row
