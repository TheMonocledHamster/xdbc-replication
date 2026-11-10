package compression

import (
	"fmt"
	"io"

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
	case Snappy:
		return SnappyCodec{}, nil
	}
	return nil, fmt.Errorf("unknown compression type %d", id)
}

/*
********************
SNAPPY
********************
*/

type SnappyCodec struct{}

func (SnappyCodec) ID() Compression         { return Snappy }
func (SnappyCodec) MaxEncodedLen(n int) int { return snappy.MaxEncodedLen(n) }

// Compress writes Snappy bytes into dst. dst must fit MaxEncodedLen(len(src)).
func (SnappyCodec) Compress(dst, src []byte) (int, error) {
	// Have to ensure dst fits the data, otherwise snappy makes a new slice
	if requiredDstSize := snappy.MaxEncodedLen(len(src)); requiredDstSize < 0 || len(dst) < requiredDstSize {
		return 0, io.ErrShortBuffer
	}

	// Take it away, Snappy!
	return len(snappy.Encode(dst, src)), nil
}

// Decompress decodes Snappy bytes into dst. Invalid input and insufficient
// destination space return errors.
func (SnappyCodec) Decompress(dst, src []byte) (int, error) {
	decodedLength, err := snappy.DecodedLen(src)
	if err != nil { // Something went wrong getting the decoded length, somehow
		return 0, fmt.Errorf("snappy decompression: %w", err)
	}
	if decodedLength > len(dst) { // Decode would make a new slice, not ideal!
		return 0, io.ErrShortBuffer
	}

	// Checks are good, let Snappy decompress src into dst
	decodedBytes, err := snappy.Decode(dst, src)
	if err != nil {
		return 0, fmt.Errorf("snappy decompression: %w", err)
	}

	return len(decodedBytes), nil
}
