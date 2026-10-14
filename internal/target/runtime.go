package target

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/TheMonocledHamster/xdbc-replication/internal/network"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

// RuntimeConfig controls the target-side buffers and bounded handoff queue.
type RuntimeConfig struct {
	Network       network.Config
	NumBuffers    int
	QueueCapacity int
}

// RunReceiver coordinates one target-side connection from handshake through
// PostgreSQL insertion. The caller supplies an established connection to the
// source sender. 
func (writer *Writer) RunReceiver(ctx context.Context, conn net.Conn, cfg RuntimeConfig) error {
	if conn == nil {
		return errors.New("target: connection is nil")
	}
	if writer == nil || writer.db == nil {
		return errors.New("target: writer is not initialized")
	}
	// Currently only looking at uncompressed batches
	if cfg.Network.Compression != network.CompressionNone {
		return errors.New("target: MVP receiver requires uncompressed batches")
	}
	if cfg.Network.Format != network.FormatRow {
		return errors.New("target: MVP receiver requires row format")
	}
	if cfg.NumBuffers < 1 || cfg.QueueCapacity < 1 {
		return errors.New("target: buffer count and queue capacity must be positive")
	}
	defer conn.Close()

	accepted, err := network.ClientHandshake(ctx, conn, cfg.Network)
	if err != nil {
		return fmt.Errorf("target: negotiate transfer: %w", err)
	}
	
	// Create pool of reusable buffrs
	p, err := pool.New(pool.Config{NumBuffers: cfg.NumBuffers, PayloadCap: accepted.PayloadCap})
	if err != nil {
		return fmt.Errorf("target: create buffer pool: %w", err)
	}
	
	// Waiting line for IDs of filled buffers
	output, err := queue.New(cfg.QueueCapacity)
	if err != nil {
		return fmt.Errorf("target: create received-buffer queue: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	
	// To report when receiver + writer are done
	receiveDone := make(chan error, 1)
	consumeDone := make(chan error, 1)
	// Read batches from connection
	go func() { receiveDone <- network.Receive(runCtx, conn, p, output, accepted) }()
	// Takes buffer ids, reads batch, writes to postgres
	go func() { consumeDone <- writer.Consume(runCtx, p, output) }()

	var receiveErr, consumeErr error
	select {
	case receiveErr = <-receiveDone:
		close(output) // Close the queue
		consumeErr = <-consumeDone
	case consumeErr = <-consumeDone:
		// Should not happen unless db error. Stop socket I/O so
		// a blocked receiver cannot strand the coordinator or its buffers.
		if consumeErr == nil {
			consumeErr = errors.New("target: consumer stopped before receive completed")
		}
		cancel()
		_ = conn.Close()
		receiveErr = <-receiveDone
		close(output)
	}
	
	cancel()
	// Release those still waiting in queue
	if receiveErr != nil || consumeErr != nil {
		_ = conn.Close()
		for id := range output {
			if err := p.Release(id); err != nil {
				receiveErr = errors.Join(receiveErr, fmt.Errorf("target: release queued buffer %d: %w", id, err))
			}
		}
	}
	return errors.Join(receiveErr, consumeErr)
}
