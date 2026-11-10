package transfer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
)

func TestPipelineFailureReleasesQueuedBuffers(t *testing.T) {
	t.Setenv("COMPRESSION", "snappy")
	c, err := LoadConfig("../../testdata/iot/transfer.json")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := newPipeline(ctx, c, c.BufferSize)
	if err != nil {
		t.Fatal(err)
	}
	p.start(p.first, func(ctx context.Context) error {
		for i := 0; i < 100; i++ {
			id, err := p.pool.Acquire(ctx)
			if err != nil {
				return err
			}
			b, _ := p.pool.Buffer(id)
			(pool.BufferHeader{PayloadLen: 4096, UncompressedLen: 4096, RecordCount: 1}).Encode(b)
			clear(b[pool.HeaderSize : pool.HeaderSize+4096])
			if err := p.first.Push(ctx, id); err != nil {
				return errors.Join(err, p.pool.Release(id))
			}
		}
		return nil
	})
	p.start(p.second, func(context.Context) error { return p.encode(c, false) })
	failure := errors.New("sink failed")
	p.start(nil, func(ctx context.Context) error {
		id, err := p.second.Pop(ctx)
		if err != nil {
			return err
		}
		if err := p.pool.Release(id); err != nil {
			return err
		}
		return failure
	})
	if err := p.wait(); !errors.Is(err, failure) {
		t.Fatalf("lost original failure: %v", err)
	}
	var acquired []int
	for i := 0; i < c.PoolSize; i++ {
		id, err := p.pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("buffer leaked after cancellation: %v", err)
		}
		acquired = append(acquired, id)
	}
	for _, id := range acquired {
		if err := p.pool.Release(id); err != nil {
			t.Fatal(err)
		}
	}
}
