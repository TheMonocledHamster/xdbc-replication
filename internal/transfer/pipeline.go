package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/compression"
	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/network"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/postgres"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
	"github.com/TheMonocledHamster/xdbc-replication/internal/target"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// pipeline owns a session's queues and pool. Producers close their own outputs;
// cancellation happens before closure on failure, so a failed source cannot END.
type pipeline struct {
	failure       sync.Once
	err           error
	ctx           context.Context
	cancel        context.CancelFunc
	group         *errgroup.Group
	pool          *pool.Pool
	first, second queue.Queue
}

func newPipeline(ctx context.Context, c Config, capacity int) (*pipeline, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)
	p, err := pool.New(pool.Config{NumBuffers: c.PoolSize, PayloadCap: capacity})
	if err != nil {
		cancel()
		return nil, err
	}
	a, _ := queue.New(c.QueueCapacity)
	b, _ := queue.New(c.QueueCapacity)
	return &pipeline{ctx: ctx, cancel: cancel, group: g, pool: p, first: a, second: b}, nil
}
func (p *pipeline) start(out queue.Queue, stage func(context.Context) error) {
	p.startN(out, 1, func(ctx context.Context, _ int) error { return stage(ctx) })
}

// Run n workers of one logical stage. The last worker to exit closes out.
func (p *pipeline) startN(out queue.Queue, n int, stage func(ctx context.Context, worker int) error) {
	var remaining atomic.Int32
	remaining.Store(int32(n))
	for i := 0; i < n; i++ {
		// Start a worker
		p.group.Go(func() error {
			err := stage(p.ctx, i)

			// If something's gone off, cancel the stage.
			if err != nil {
				p.failure.Do(func() { p.err = err })
				p.cancel()
			}

			// IF the last worker to exit, close the relevant queue
			// I don't anticipate scenarios where a worker exits early and triggers this
			if remaining.Add(-1) == 0 && out != nil {
				close(out)
			}
			return err
		})
	}
}
func (p *pipeline) wait() error {
	err := p.group.Wait()
	if p.err != nil {
		err = p.err
	}
	p.cancel()
	// No workers remain; queued IDs still belong to this session.
	for _, q := range []queue.Queue{p.first, p.second} {
		for id := range q {
			err = errors.Join(err, p.pool.Release(id))
		}
	}
	return err
}
func (p *pipeline) encode(c Config, decompress bool) error {
	if c.Compression == "none" {
		for {
			id, err := p.first.Pop(p.ctx)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := p.second.Push(p.ctx, id); err != nil {
				return errors.Join(err, p.pool.Release(id))
			}
		}
	}
	workers := c.Stages().Compress
	if decompress {
		workers = c.Stages().Decompress
	}
	codec, _ := compression.ByID(compression.Snappy)
	return compression.StartCompressStage(p.ctx, workers, p.first, p.second, p.pool, decompress, codec)
}

// ServeTransfer uses a fresh database connection and pipeline for each client.
func ServeTransfer(ctx context.Context, conn net.Conn, databaseURL string, c Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	defer conn.Close()
	db, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())
	cfg, err := network.ServerHandshake(ctx, conn, c.Network())
	if err != nil {
		return err
	}
	// The client sends no more bytes after HELLO. Its disconnect cancels reading
	// PostgreSQL too, including when the HTTP caller abandons the transfer.
	peerDone := make(chan struct{})
	go func() { defer close(peerDone); var b [1]byte; _, _ = conn.Read(b[:]); cancel() }()
	defer func() { conn.Close(); <-peerDone }()
	p, err := newPipeline(ctx, c, cfg.PayloadCap)
	if err != nil {
		return err
	}
	layout, _ := row.NewLayout(c.Fields)
	p.start(p.first, func(ctx context.Context) error { return postgres.Read(ctx, db, c.Query(), layout, p.pool, p.first) })
	p.start(p.second, func(context.Context) error { return p.encode(c, false) })
	p.start(nil, func(ctx context.Context) error { return network.Send(ctx, conn, p.pool, p.second, cfg) })
	return p.wait()
}

type Result struct {
	Rows      int64 `json:"rows"`
	ElapsedMS int64 `json:"elapsed_ms"`
}

// ReceiveTransfer replaces the sink using the current per-batch target writer.
// Earlier batches may remain if a later batch fails.
func ReceiveTransfer(ctx context.Context, address, databaseURL string, c Config) (Result, error) {
	var result Result
	started := time.Now()
	if err := c.Validate(); err != nil {
		return result, err
	}
	db, err := target.OpenPostgres(ctx, databaseURL)
	if err != nil {
		return result, err
	}
	defer db.Close()
	// Each write worker commits one batch per transaction on its own connection.
	db.SetMaxOpenConns(c.Stages().Write)
	writer, err := target.New(db, target.Config{Schema: c.Schema, Table: c.Table, Fields: c.Fields})
	if err != nil {
		return result, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	cfg, err := network.ClientHandshake(ctx, conn, c.Network())
	if err != nil {
		return result, err
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+c.Relation()); err != nil {
		return result, err
	}
	p, err := newPipeline(ctx, c, cfg.PayloadCap)
	if err != nil {
		return result, err
	}
	p.start(p.first, func(ctx context.Context) error { return network.Receive(ctx, conn, p.pool, p.first, cfg) })
	p.start(p.second, func(context.Context) error { return p.encode(c, true) })
	// Batches are independent transactions into one table, so writers share the
	// queue with no further partitioning; row order in the sink is not defined.
	p.startN(nil, c.Stages().Write, func(ctx context.Context, _ int) error { return writer.Consume(ctx, p.pool, p.second) })
	if err := p.wait(); err != nil {
		return result, fmt.Errorf("transfer: %w", err)
	}
	result.ElapsedMS = time.Since(started).Milliseconds()
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM "+c.Relation()).Scan(&result.Rows)
	return result, err
}
