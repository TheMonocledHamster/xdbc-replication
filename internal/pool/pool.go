package pool

import (
	"context"
	"errors"
	"sync/atomic"
)

type Config struct {
	NumBuffers int // number of logical buffers in the slab
	PayloadCap int // maximum data in each buffer in bytes
}

type Pool struct {
	cfg     Config        // Pool configuration
	slab    []byte        // A raw shared memory segment
	bufSize int           // Defines the size of an entire logical buffer
	free    chan int      // A list of free buffers set up in FIFO order
	used    []atomic.Bool // Optional, but might be nice to have for accounting
}

/*
******************
POOL INIT & INFO
******************
*/

// Create a contiguous memory segment/slab and all buffer handles.
// Usually used by a coordinating process on the server/client
func New(cfg Config) (*Pool, error) {
	// Basic argument checks
	if cfg.NumBuffers < 1 {
		return nil, errors.New("A pool can't have negative buffers. Duh")
	}
	if cfg.PayloadCap < 1 {
		return nil, errors.New("A pool's buffer's payload capacity can't be negative. Duh")
	}

	bufSize := HeaderSize + cfg.PayloadCap
	p := &Pool{
		cfg:     cfg,
		slab:    make([]byte, cfg.NumBuffers*bufSize),
		bufSize: bufSize,
		free:    make(chan int, cfg.NumBuffers),
		used:    make([]atomic.Bool, cfg.NumBuffers),
	}

	// Add all of the logical buffer IDs to the "free" list
	for i := 0; i < cfg.NumBuffers; i++ {
		p.free <- i
	}
	return p, nil
}

// Sanity check if the buffer ID is a valid ID for the pool
func (p *Pool) checkID(id int) error {
	if id < 0 || int(id) >= len(p.used) {
		return errors.New("Invalid buffer ID")
	}
	return nil
}

/*
******************
BUFFER HANDLING
******************
*/

// Buffer returns the slab-backed header and payload for an acquired buffer ID.
// The caller owns the bytes until it releases the ID or forwards it to a queue.
func (p *Pool) Buffer(id int) ([]byte, error) {
	if err := p.checkID(id); err != nil {
		return nil, err
	}
	start := id * p.bufSize
	return p.slab[start : start+p.bufSize : start+p.bufSize], nil
}

// PayloadCap is the maximum payload size of a logical buffer, excluding its header.
func (p *Pool) PayloadCap() int { return p.cfg.PayloadCap }

// Gets a free buffer from the pool
func (p *Pool) Acquire(ctx context.Context) (int, error) {
	// Verify surrounding context is ok
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// Block until we have a free buffer
	select {
	case id := <-p.free:
		buf, err := p.Buffer(id)
		if err != nil {
			return 0, err
		}
		// Header cleanup and accounting before providing the buffer to the worker
		clear(buf[:HeaderSize])
		p.used[id].Store(true)
		return id, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (p *Pool) Release(id int) error {
	// Make sure the buffer being released even exists
	iderr := p.checkID(id)
	if iderr != nil {
		return iderr
	}

	// Wait for the buffer ID to be loaded back into the free list
	select {
	case p.free <- id:
		p.used[id].Store(false)
		return nil
	default:
		return errors.New("Something bad happened in Release(), possibly a double release")
	}
}
