package compression

import (
	"errors"
	"fmt"

	"github.com/golang/snappy"
)

type Compression uint8

const (
	None   Compression = 0
	Snappy Compression = 1
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
func (NoneCodec) Compress(dst, src []byte) (int, error) {
	return -1, errors.New("Compress for NoneCodec not implemented!")
}
func (NoneCodec) Decompress(dst, src []byte) (int, error) {
	return -1, errors.New("Decompress for NoneCodec not implemented!")
}

/*
********************
SNAPPY
********************
*/

type SnappyCodec struct{}

func (SnappyCodec) ID() Compression         { return Snappy }
func (SnappyCodec) MaxEncodedLen(n int) int { return snappy.MaxEncodedLen(n) }
func (SnappyCodec) Compress(dst, src []byte) (int, error) {
	return -1, errors.New("Compress for SnappyCodec not implemented!")
}
func (SnappyCodec) Decompress(dst, src []byte) (int, error) {
	return -1, errors.New("Decompress for SnappyCodec not implemented!")
}
