package queue

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestInvalidQueueCapacity(t *testing.T) {
	_, err := New(-1)
	if err == nil {
		t.Fatal("Expected a fatal error initializing negative size queue, but returned successfully")
	}

	_, err2 := New(0)
	if err2 == nil {
		t.Fatal("Expected a fatal error initializing 0-size queue, but returned successfully")
	}
}

func TestClosedQueueDrainsThenEOF(t *testing.T) {
	ctx := context.Background()
	q, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Push(ctx, 7); err != nil {
		t.Fatal(err)
	}
	close(q)
	if id, err := q.Pop(ctx); err != nil || id != 7 {
		t.Fatalf("drain: %d, %v", id, err)
	}
	if _, err := q.Pop(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("closed queue: %v", err)
	}
}

func TestQueuePushPop(t *testing.T) {
	ctx := context.Background()
	q, err := New(1)
	if err != nil {
		t.Fatalf("Unexpected error from initializing the queue: %s", err)
	}

	pusherr := q.Push(ctx, 6400)
	if pusherr != nil {
		t.Fatalf("Unexpected error from pushing into the queue: %s", pusherr)
	}

	val, poperr := q.Pop(ctx)
	if poperr != nil {
		t.Fatalf("Unexpected error from popping from the queue: %s", poperr)
	}

	if val != 6400 {
		t.Fatalf("Expected %d to be returned, got %d", 6400, val)
	}
}
