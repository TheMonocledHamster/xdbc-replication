package network

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/TheMonocledHamster/xdbc-replication/internal/protocol"
)

const (
	Magic      = "XDBC"
	Version    = 1
	HeaderSize = 16
)

type MessageType uint8

const (
	MessageHello    MessageType = 1
	MessageHelloAck MessageType = 2
	MessageData     MessageType = 3
	MessageEnd      MessageType = 4
	MessageError    MessageType = 5
)

type Compression = protocol.Compression

const (
	CompressionNone   = protocol.CompressionNone
	CompressionSnappy = protocol.CompressionSnappy
)

type Format = protocol.Format

const (
	FormatRow = protocol.FormatRow
)

type Header struct {
	Type        MessageType
	Compression Compression
	Format      Format
	PayloadSize uint32
	RecordCount uint32
}

// Encode writes the V1 wire header in network byte order. It preserves all
// message and encoding IDs; use Validate for the builtin V1 message rules.
func (h Header) Encode(dst []byte) error {
	if len(dst) < HeaderSize {
		return io.ErrShortBuffer
	}
	copy(dst[:4], Magic)
	dst[4], dst[5], dst[6], dst[7] = Version, byte(h.Type), byte(h.Compression), byte(h.Format)
	binary.BigEndian.PutUint32(dst[8:12], h.PayloadSize)
	binary.BigEndian.PutUint32(dst[12:16], h.RecordCount)
	return nil
}

// DecodeHeader checks the wire envelope, preserving IDs even when this runtime
// does not recognize them. Encoding support is negotiated by the pipeline.
func DecodeHeader(src []byte) (Header, error) {
	if len(src) < HeaderSize {
		return Header{}, io.ErrUnexpectedEOF
	}
	if string(src[:4]) != Magic {
		return Header{}, fmt.Errorf("invalid XDBC magic")
	}
	if src[4] != Version {
		return Header{}, fmt.Errorf("unsupported wire version %d", src[4])
	}
	h := Header{
		Type: MessageType(src[5]), Compression: Compression(src[6]), Format: Format(src[7]),
		PayloadSize: binary.BigEndian.Uint32(src[8:12]), RecordCount: binary.BigEndian.Uint32(src[12:16]),
	}
	return h, nil
}
