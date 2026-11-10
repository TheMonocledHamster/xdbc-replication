package transfer

import (
	"context"
	"errors"
	"io"
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

// Several workers of one stage must close the shared output exactly once, after
// all of them have finished.
func TestStartNClosesOutputAfterLastWorker(t *testing.T) {
	c, err := LoadConfig("../../testdata/iot/transfer.json")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := newPipeline(ctx, c, c.BufferSize)
	if err != nil {
		t.Fatal(err)
	}
	const workers, perWorker = 4, 50
	p.start(p.second, func(context.Context) error { return nil })
	p.startN(p.first, workers, func(ctx context.Context, w int) error {
		for i := 0; i < perWorker; i++ {
			if err := p.first.Push(ctx, w); err != nil {
				return err
			}
		}
		return nil
	})
	got := 0
	p.start(nil, func(ctx context.Context) error {
		for {
			if _, err := p.first.Pop(ctx); errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return err
			}
			got++
		}
	})
	if err := p.wait(); err != nil {
		t.Fatal(err)
	}
	if got != workers*perWorker {
		t.Fatalf("consumer saw %d items, want %d", got, workers*perWorker)
	}
}

// One failing worker must cancel its peers and surface its own error.
func TestStartNFailureCancelsPeers(t *testing.T) {
	c, err := LoadConfig("../../testdata/iot/transfer.json")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := newPipeline(ctx, c, c.BufferSize)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("worker failed")
	p.start(p.second, func(context.Context) error { return nil })
	p.startN(p.first, 3, func(ctx context.Context, w int) error {
		if w == 1 {
			return failure
		}
		<-ctx.Done() // peers block until the failure cancels the session
		return ctx.Err()
	})
	if err := p.wait(); !errors.Is(err, failure) {
		t.Fatalf("lost the original failure: %v", err)
	}
	if _, ok := <-p.first; ok {
		t.Fatal("output must be closed after all workers exit")
	}
}

// A multi-worker compress stage with the smallest legal pool must neither hang
// nor leak buffers.
func TestParallelCompressionTightPool(t *testing.T) {
	c, err := LoadConfig("../../testdata/iot/transfer.json")
	if err != nil {
		t.Fatal(err)
	}
	c.Compression = "snappy"
	c.Workers = Workers{Compress: 4}
	c.PoolSize = c.MinPool()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := newPipeline(ctx, c, c.BufferSize)
	if err != nil {
		t.Fatal(err)
	}
	const batches = 200
	p.start(p.first, func(ctx context.Context) error {
		for i := 0; i < batches; i++ {
			id, err := p.pool.Acquire(ctx)
			if err != nil {
				return err
			}
			b, _ := p.pool.Buffer(id)
			(pool.BufferHeader{Seq: uint64(i), PayloadLen: 4096, UncompressedLen: 4096, RecordCount: 1}).Encode(b)
			clear(b[pool.HeaderSize : pool.HeaderSize+4096])
			if err := p.first.Push(ctx, id); err != nil {
				return errors.Join(err, p.pool.Release(id))
			}
		}
		return nil
	})
	p.start(p.second, func(context.Context) error { return p.encode(c, false) })
	seen := map[uint64]bool{}
	p.start(nil, func(ctx context.Context) error {
		for {
			id, err := p.second.Pop(ctx)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			b, _ := p.pool.Buffer(id)
			seen[pool.DecodeHeader(b).Seq] = true
			if err := p.pool.Release(id); err != nil {
				return err
			}
		}
	})
	if err := p.wait(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != batches {
		t.Fatalf("got %d distinct batches, want %d", len(seen), batches)
	}
	for i := 0; i < c.PoolSize; i++ {
		if _, err := p.pool.Acquire(ctx); err != nil {
			t.Fatalf("buffer leaked: %v", err)
		}
	}
}
