// Package row encodes and decodes contiguous fixed-widthrows without
// alignment padding. INT is a little-endian signed int32, DOUBLE contains
// little-endian IEEE 754 float64 bits, and CHAR is one byte. STRING widths count
// bytes: strings are zero-padded, may fill the entire field, and cannot contain
// embedded NUL. Decoders stop at the first NUL. Has no NULL representation currently.
//
// Layouts must be constructed with NewLayout. Both peers must configure the same
// ordered schema, including field widths. protocol.FormatRow identifies this
// format; the network handshake does not exchange schemas.
//
// Producers write complete rows directly into pool buffer payload memory after
// the pool header. Use RowsPerBuffer to determine capacity and RowAt on the used
// payload slice, excluding trailing unused capacity. Rows never span buffers.
// Before compression, set pool.BufferHeader.RecordCount to the encoded count and
// both PayloadLen and UncompressedLen to the used payload length. Compression is
// a separate stage, updating PayloadLen and Compression for the encoded payload.
//
// Consumers decompress when needed, verify the decoded byte length against
// UncompressedLen, and call ValidatePayload with RecordCount before accessing
// rows. RowAt and GetStringBytes borrow the buffer memory: retain ownership while
// using their views, and copy values that must survive release or transfer.
package row
