package pool

import "encoding/binary"

// total size of all header elements
const HeaderSize = 32

const (
	offSeq         = 0
	offNextRecord  = 8
	offRecordCount = 16
	offPayloadLen  = 20
	offUncompLen   = 24
	offCompression = 28
	offProtocol    = 29
	offUnused1     = 30
	offUnused2     = 31
)

// Using little endian ordering for consistency in networking
var le = binary.LittleEndian

type BufferHeader struct {
	Seq             uint64
	NextRecord      uint64
	RecordCount     uint32
	PayloadLen      uint32
	UncompressedLen uint32
	Compression     uint8
	Protocol        uint8
	Unused1         uint8
	Unused2         uint8
}

// Add header information to a buffer
func (h BufferHeader) Encode(dst []byte) {
	// Quick way to check that memory is usable
	_ = dst[HeaderSize-1]

	// Values more than 1 byte need LE ordering
	le.PutUint64(dst[offSeq:], h.Seq)
	le.PutUint64(dst[offNextRecord:], h.NextRecord)
	le.PutUint32(dst[offRecordCount:], h.RecordCount)
	le.PutUint32(dst[offPayloadLen:], h.PayloadLen)
	le.PutUint32(dst[offUncompLen:], h.UncompressedLen)

	// Single byte values: no need to worry about LE ordering
	dst[offCompression] = h.Compression
	dst[offProtocol] = h.Protocol
	dst[offUnused1] = h.Unused1
	dst[offUnused2] = h.Unused2
}

// Gather header information from a buffer
func DecodeHeader(src []byte) BufferHeader {
	// Quick way to check that memory is usable
	_ = src[HeaderSize-1]

	return BufferHeader{
		Seq:             le.Uint64(src[offSeq:]),
		NextRecord:      le.Uint64(src[offNextRecord:]),
		RecordCount:     le.Uint32(src[offRecordCount:]),
		PayloadLen:      le.Uint32(src[offPayloadLen:]),
		UncompressedLen: le.Uint32(src[offUncompLen:]),
		Compression:     src[offCompression],
		Protocol:        src[offProtocol],
		Unused1:         src[offUnused1],
		Unused2:         src[offUnused2],
	}
}
