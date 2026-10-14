package protocol

type Compression uint8

const (
	CompressionNone   Compression = 0
	CompressionSnappy Compression = 1
)

type Format uint8

const (
	FormatRow Format = 0
)
