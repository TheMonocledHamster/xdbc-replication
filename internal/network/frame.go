package network

import (
	"fmt"
	"io"
)

func ReadHeader(r io.Reader) (Header, error) {
	var b [HeaderSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return Header{}, err
	}
	return DecodeHeader(b[:])
}

// WriteFrame handles partial writes without copying the payload.
func WriteFrame(w io.Writer, h Header, payload []byte) error {
	if uint64(len(payload)) != uint64(h.PayloadSize) {
		return fmt.Errorf("frame payload length does not match header")
	}
	var b [HeaderSize]byte
	if err := h.Encode(b[:]); err != nil {
		return err
	}
	if err := writeFull(w, b[:]); err != nil {
		return err
	}
	return writeFull(w, payload)
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if n < 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
