package pool

import (
	"testing"

	"github.com/TheMonocledHamster/xdbc-replication/internal/compression"
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
		Compression:     uint8(compression.None),
		Protocol:        1,
		Unused1:         0,
		Unused2:         0,
	}

	if h.Compression != 0 {
		t.Fatalf("expected compression type 0 (None), got %d", h.Compression)
	}

	if h.PayloadLen != 4096 {
		t.Fatalf("expected payload length 4096, got %d", h.PayloadLen)
	}
}
