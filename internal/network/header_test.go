package network

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestHeaderMetadata(t *testing.T) {
	h := Header{
		Type:        MessageData,
		Compression: CompressionNone,
		Format:      FormatRow,
		PayloadSize: 5,
		RecordCount: 1,
	}

	if h.PayloadSize != 5 {
		t.Fatalf("expected payload size 5, got %d", h.PayloadSize)
	}

	if h.RecordCount != 1 {
		t.Fatalf("expected record count 1, got %d", h.RecordCount)
	}
}

func TestHeaderWireEncoding(t *testing.T) {
	h := Header{Type: MessageData, Compression: CompressionSnappy, PayloadSize: 0x01020304, RecordCount: 0x05060708}
	b := make([]byte, HeaderSize)
	if err := h.Encode(b); err != nil {
		t.Fatal(err)
	}
	want := []byte{'X', 'D', 'B', 'C', 1, 3, 1, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	if !bytes.Equal(b, want) {
		t.Fatalf("wire header = %x, want %x", b, want)
	}
	got, err := DecodeHeader(b)
	if err != nil || got != h {
		t.Fatalf("decoded header = %+v, %v", got, err)
	}
	if err := h.Encode(b[:HeaderSize-1]); !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("short destination: %v", err)
	}
}

func TestHeaderRejectsMalformedFrames(t *testing.T) {
	var valid [HeaderSize]byte
	if err := (Header{Type: MessageData}).Encode(valid[:]); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		offset int
		value  byte
	}{
		{"magic", 0, 'Y'}, {"version", 4, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := valid
			b[tc.offset] = tc.value
			if _, err := DecodeHeader(b[:]); err == nil {
				t.Fatal("accepted invalid header")
			}
		})
	}
	for n := 0; n < HeaderSize; n++ {
		if _, err := DecodeHeader(valid[:n]); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("length %d: %v", n, err)
		}
	}
	for _, h := range []Header{
		{Type: 0}, {Type: 99},
		{Type: MessageEnd, PayloadSize: 1}, {Type: MessageHello, RecordCount: 1},
		{Type: MessageHelloAck, Compression: CompressionSnappy},
		{Type: MessageHello, PayloadSize: configSize, Format: 99},
		{Type: MessageHelloAck, PayloadSize: configSize, RecordCount: 1},
		{Type: MessageError, PayloadSize: maxErrorSize + 1},
	} {
		if err := h.Validate(); err == nil {
			t.Fatalf("accepted invalid control header %+v", h)
		}
	}
}

func TestHeaderPreservesExtensionIDs(t *testing.T) {
	for _, typ := range []MessageType{MessageData, 200} {
		h := Header{Type: typ, Compression: 42, Format: 77, PayloadSize: 123, RecordCount: 4}
		var b [HeaderSize]byte
		if err := h.Encode(b[:]); err != nil {
			t.Fatal(err)
		}
		got, err := DecodeHeader(b[:])
		if err != nil || got != h {
			t.Fatalf("extension IDs changed: %+v, %v", got, err)
		}
		var frame bytes.Buffer
		if err := WriteFrame(&frame, h, make([]byte, h.PayloadSize)); err != nil {
			t.Fatal(err)
		}
		if got, err := ReadHeader(&frame); err != nil || got != h {
			t.Fatalf("frame extension IDs changed: %+v, %v", got, err)
		}
		if typ == MessageData {
			if err := got.Validate(); err != nil {
				t.Fatalf("DATA validation must defer encoding support to negotiation: %v", err)
			}
		} else if err := got.Validate(); err == nil {
			t.Fatal("builtin protocol accepted a message without a handler")
		}
	}
}

func TestValidControlHeaders(t *testing.T) {
	for _, h := range []Header{
		{Type: MessageHello, PayloadSize: configSize}, {Type: MessageHelloAck, PayloadSize: configSize},
		{Type: MessageEnd}, {Type: MessageError, PayloadSize: maxErrorSize},
	} {
		if err := h.Validate(); err != nil {
			t.Fatalf("valid control header %+v: %v", h, err)
		}
	}
}

type shortWriter struct{ bytes.Buffer }

func (w *shortWriter) Write(b []byte) (int, error) {
	return w.Buffer.Write(b[:min(3, len(b))])
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type shortReader struct{ io.Reader }

func (r shortReader) Read(b []byte) (int, error) { return r.Reader.Read(b[:min(2, len(b))]) }

func TestFramePartialIO(t *testing.T) {
	h := Header{Type: MessageData, PayloadSize: 7, RecordCount: 2}
	w := &shortWriter{}
	if err := WriteFrame(w, h, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	r := shortReader{&w.Buffer}
	got, err := ReadHeader(r)
	if err != nil || got != h {
		t.Fatalf("header: %+v, %v", got, err)
	}
	b := make([]byte, 7)
	if _, err := io.ReadFull(r, b); err != nil || string(b) != "payload" {
		t.Fatalf("payload: %q, %v", b, err)
	}
	if err := WriteFrame(zeroWriter{}, h, b); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero write: %v", err)
	}
	if err := WriteFrame(failingWriter{}, h, b); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("failed write: %v", err)
	}
	if err := WriteFrame(w, h, b[:6]); err == nil {
		t.Fatal("accepted mismatched payload")
	}
	if _, err := ReadHeader(bytes.NewReader(b)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated header: %v", err)
	}
}
