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
// source sender. The function owns the connection for the duration of the run.
// It supports the MVP's uncompressed row format; Snappy requires a decompression
// stage before Writer.Consume.
func RunReceiver(ctx context.Context, conn net.Conn, writer *Writer, cfg RuntimeConfig) error {
	if conn == nil {
		return errors.New("target: connection is nil")
	}
	if writer == nil || writer.db == nil {
		return errors.New("target: writer is not initialized")
	}
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
	p, err := pool.New(pool.Config{NumBuffers: cfg.NumBuffers, PayloadCap: accepted.PayloadCap})
	if err != nil {
		return fmt.Errorf("target: create buffer pool: %w", err)
	}
	output, err := queue.New(cfg.QueueCapacity)
	if err != nil {
		return fmt.Errorf("target: create received-buffer queue: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	receiveDone := make(chan error, 1)
	consumeDone := make(chan error, 1)
	go func() { receiveDone <- network.Receive(runCtx, conn, p, output, accepted) }()
	go func() { consumeDone <- writer.Consume(runCtx, p, output) }()

	var receiveErr, consumeErr error
	select {
	case receiveErr = <-receiveDone:
		close(output)
		consumeErr = <-consumeDone
	case consumeErr = <-consumeDone:
		// A consumer can finish before END only when it fails. Stop socket I/O so
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
