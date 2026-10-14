package queue

import (
	"context"
	"errors"
	"io"
)

// Development Note: A "poison pill" is equivalent to just closing
// a channel/queue in our Go-based version

type Queue chan int

func New(capacity int) (Queue, error) {
	if capacity < 1 {
		return nil, errors.New("Queue capacity must be >= 1. Duh.")
	}

	queue := Queue(make(chan int, capacity))
	return queue, nil
}

// Pushes a buffer ID into the queue to be used however a stage wants to
func (q Queue) Push(ctx context.Context, id int) error {
	// See if the surrounding context is closed already, no point in proceeding if so
	if err := ctx.Err(); err != nil {
		return err
	}

	// Wait for the buffer to be added to the queue
	select {
	case q <- id:
		return nil

	// If the surrounding context no longer needs the push, send an error
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Pops a buffer ID from the queue to be used however a stage wants to
func (q Queue) Pop(ctx context.Context) (int, error) {
	// Sanity check to see if surrounding context errored out
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// Wait for a buffer ID to be made available in the queue
	select {
	case entry, ok := <-q:
		// When queue is closed
		if !ok {
			return 0, io.EOF
		}
		return entry, nil

	// If the surrounding context no longer needs the pop, send an error
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
