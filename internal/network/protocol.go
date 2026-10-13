package network

import "fmt"

const configSize = 6
const maxErrorSize = 1024

// Validate checks builtin V1 message semantics separately from wire encoding.
// DATA encoding IDs are opaque here: the negotiated Config determines which
// compression and format the pipeline supports. Unknown message types require
// a protocol handler and are rejected by the builtin transfer stages.
func (h Header) Validate() error {
	switch h.Type {
	case MessageData:
		return nil
	case MessageHello, MessageHelloAck:
		if h.PayloadSize != configSize {
			return fmt.Errorf("configuration frame requires %d bytes", configSize)
		}
	case MessageEnd:
		if h.PayloadSize != 0 {
			return fmt.Errorf("END frame must have no payload")
		}
	case MessageError:
		if h.PayloadSize > maxErrorSize {
			return fmt.Errorf("ERROR frame exceeds %d bytes", maxErrorSize)
		}
	default:
		return fmt.Errorf("unknown message type %d", h.Type)
	}
	if h.RecordCount != 0 || h.Compression != CompressionNone || h.Format != FormatRow {
		return fmt.Errorf("control frame contains data metadata")
	}
	return nil
}
