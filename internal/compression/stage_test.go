package compression

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

func TestStageCompressesFullBatchesAndFallsBack(t *testing.T) {
	for _, compressible := range []bool{true, false} {
		t.Run(map[bool]string{true: "snappy", false: "fallback"}[compressible], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			p, _ := pool.New(pool.Config{NumBuffers: 2, PayloadCap: 4096})
			input, _ := queue.New(1)
			compressed, _ := queue.New(1)
			output, _ := queue.New(1)
			original := bytes.Repeat([]byte{42}, 4096)
			if !compressible {
				if _, err := rand.Read(original); err != nil {
					t.Fatal(err)
				}
			}
			id, _ := p.Acquire(ctx)
			b, _ := p.Buffer(id)
			copy(b[pool.HeaderSize:], original)
			(pool.BufferHeader{PayloadLen: 4096, UncompressedLen: 4096, RecordCount: 1}).Encode(b)
			input.Push(ctx, id)
			close(input)
			if err := StartCompressStage(ctx, 1, input, compressed, p, false, SnappyCodec{}); err != nil {
				t.Fatal(err)
			}
			close(compressed)
			encodedID, err := compressed.Pop(ctx)
			if err != nil {
				t.Fatal(err)
			}
			encoded, _ := p.Buffer(encodedID)
			h := pool.DecodeHeader(encoded)
			if compressible && (h.Compression != uint8(Snappy) || h.PayloadLen >= 4096) {
				t.Fatalf("not compressed: %+v", h)
			}
			if !compressible && h.Compression != uint8(None) {
				t.Fatalf("expected fallback: %+v", h)
			}
			// Requeue the borrowed buffer to exercise the actual decompression stage.
			decodeInput, _ := queue.New(1)
			decodeInput.Push(ctx, encodedID)
			close(decodeInput)
			if err := StartCompressStage(ctx, 1, decodeInput, output, p, true, SnappyCodec{}); err != nil {
				t.Fatal(err)
			}
			decodedID, err := output.Pop(ctx)
			if err != nil {
				t.Fatal(err)
			}
			decoded, _ := p.Buffer(decodedID)
			if !bytes.Equal(decoded[pool.HeaderSize:], original) {
				t.Fatal("payload changed")
			}
			p.Release(decodedID)
			a, err := p.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			bID, err := p.Acquire(ctx)
			if err != nil {
				t.Fatal("buffer leaked:", err)
			}
			p.Release(a)
			p.Release(bID)
		})
	}
}
