package pool

import (
	"testing"
)

// Shamelessly "reimagined" from Adithya's header test.
// Sorry, not sorry.

func TestBufferHeaderMetadata(t *testing.T) {
	h := BufferHeader{
		Seq:             1,
		NextRecord:      1,
		RecordCount:     2,
		PayloadLen:      4096,
		UncompressedLen: 4096,
		Compression:     0, // compression.None creates a loop w/ `compression/stage.go`
		Protocol:        1,
		Unused1:         0,
		Unused2:         0,
	}

	if h.RecordCount != 2 {
		t.Fatalf("expected record count 2, got %d", h.RecordCount)
	}

	if h.PayloadLen != 4096 {
		t.Fatalf("expected payload length 4096, got %d", h.PayloadLen)
	}
}
