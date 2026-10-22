// Package row encodes fixed-width rows: a NULL bitmap followed by ordered field
// slots. Both peers must use the same schema and widths; the network handshake
// does not exchange schemas. See docs/row-v1.md for the byte encodings.
//
// Producers clear reused records before writing every field or marking it NULL.
// Typed writes clear the corresponding NULL bit; typed reads return ErrNull for
// NULL. STRING and NUMERIC widths count bytes, and oversized text is rejected.
//
// Rows never span buffers. Consumers decompress first, validate the payload
// length against the row count, then decode rows. RowAt and GetStringBytes return
// borrowed views valid only while the caller owns the underlying buffer.
//
// Both PostgreSQL adapters support the full nullable scalar type set.
package row
