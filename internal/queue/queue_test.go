package queue

import (
	"context"
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
