package compression

import (
	"fmt"

	"github.com/TheMonocledHamster/xdbc-replication/internal/protocol"
	"github.com/golang/snappy"
)

type Compression = protocol.Compression

const (
	None   = protocol.CompressionNone
	Snappy = protocol.CompressionSnappy
)

type Codec interface {
	ID() Compression
	MaxEncodedLen(n int) int // Snappy already defines this!
	Compress(dst, src []byte) (int, error)
	Decompress(dst, src []byte) (int, error)
}

func ByID(id Compression) (Codec, error) {
	switch id {
	case None:
		return NoneCodec{}, nil
	case Snappy:
		return SnappyCodec{}, nil
	}
	return nil, fmt.Errorf("unknown compression type %d", id)
}

/*
********************
NONE / UNCOMPRESSED
********************
*/

type NoneCodec struct{}

func (NoneCodec) ID() Compression         { return None }
func (NoneCodec) MaxEncodedLen(n int) int { return n }

// Effectively just a fancy wrapper for a data copy
// Could make this some kind of buffer ID transfer thing, but too lazy
func (NoneCodec) Compress(dst, src []byte) (int, error) {
	// Make sure the provided dst buffer can accept the input
	if len(dst) < len(src) {
		return 0, fmt.Errorf("none compression codec: dst too small (%d < %d)", len(dst), len(src))
	}

	// Copy the data on over
	return copy(dst, src), nil
}

// Effectively just a fancy wrapper for a data copy
// Could make this some kind of buffer ID transfer thing, but too lazy
func (NoneCodec) Decompress(dst, src []byte) (int, error) { return NoneCodec{}.Compress(dst, src) }

/*
********************
SNAPPY
********************
*/

type SnappyCodec struct{}

func (SnappyCodec) ID() Compression         { return Snappy }
func (SnappyCodec) MaxEncodedLen(n int) int { return snappy.MaxEncodedLen(n) }

// Compresses some bytes via the Snappy compression library
func (SnappyCodec) Compress(dst, src []byte) (int, error) {
	// Have to ensure dst fits the data, otherwise snappy makes a new slice
	if requiredDstSize := snappy.MaxEncodedLen(len(src)); requiredDstSize < 0 || len(dst) < requiredDstSize {
		return 0, fmt.Errorf("snappy compression: dst too small (%d < %d)", len(dst), requiredDstSize)
	}

	// Take it away, Snappy!
	return len(snappy.Encode(dst, src)), nil
}

// Decompresses some bytes via the Snappy compression library
// WARNING: If there is an issue with the decompression process,
// such as having it accidentally decompress a non-snappy-encoded
// src, it will lead to undefined behavior
func (SnappyCodec) Decompress(dst, src []byte) (int, error) {
	decodedLength, err := snappy.DecodedLen(src)
	if err != nil { // Something went wrong getting the decoded length, somehow
		return 0, fmt.Errorf("snappy decompression: %w", err)
	}
	if decodedLength > len(dst) { // Decode would make a new slice, not ideal!
		return 0, fmt.Errorf("snappy decompression: decoded size %d exceeds dst %d", decodedLength, len(dst))
	}

	// Checks are good, let Snappy decompress src into dst
	decodedBytes, err := snappy.Decode(dst, src)
	if err != nil {
		return 0, fmt.Errorf("snappy decompression: %w", err)
	}

	return len(decodedBytes), nil
}
