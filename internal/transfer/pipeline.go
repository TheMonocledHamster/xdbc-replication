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

// handshakeAll runs one handshake per connection simultaneously, ensuring they all agree
func handshakeAll(ctx context.Context, conns []net.Conn,
	handshake func(context.Context, net.Conn) (network.Config, error)) (network.Config, error) {
	cfgs := make([]network.Config, len(conns))
	g, gctx := errgroup.WithContext(ctx)

	// Each connection does its handshake
	for i, conn := range conns {
		g.Go(func() (err error) { cfgs[i], err = handshake(gctx, conn); return err })
	}
	if err := g.Wait(); err != nil {
		return network.Config{}, err
	}

	// If any connection somehow negotiated a different config, we have a problem
	for _, cfg := range cfgs[1:] {
		if cfg != cfgs[0] {
			return network.Config{}, errors.New("connections negotiated different configurations")
		}
	}
	return cfgs[0], nil
}

func closeAll(conns []net.Conn) {
	for _, conn := range conns {
		conn.Close()
	}
}

// ServeTransfer uses a fresh database connection and pipeline for each client.
// conns are the session's TCP connections, one per network worker (see
// AcceptConns); ServeTransfer closes them.
func ServeTransfer(ctx context.Context, conns []net.Conn, databaseURL string, c Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { closeAll(conns) })
	defer stop()
	defer closeAll(conns)
	if len(conns) != c.Stages().Network {
		return fmt.Errorf("transfer: %d connections, but workers.network is %d", len(conns), c.Stages().Network)
	}
	db, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())
	cfg, err := handshakeAll(ctx, conns, func(ctx context.Context, conn net.Conn) (network.Config, error) {
		return network.ServerHandshake(ctx, conn, c.Network())
	})
	if err != nil {
		return err
	}
	// The client sends no more bytes after HELLO. Any disconnect cancels reading
	// PostgreSQL too, including when the HTTP caller abandons the transfer.
	var peers sync.WaitGroup
	for _, conn := range conns {
		peers.Add(1)
		go func() { defer peers.Done(); var b [1]byte; _, _ = conn.Read(b[:]); cancel() }()
	}
	defer func() { closeAll(conns); peers.Wait() }()
	p, err := newPipeline(ctx, c, cfg.PayloadCap)
	if err != nil {
		return err
	}
	layout, _ := row.NewLayout(c.Fields)
	// Parallel readers each scan a disjoint range of the table's heap pages
	readers := c.Stages().Read
	var pages int64
	if readers > 1 {
		err := db.QueryRow(ctx, "SELECT pg_relation_size($1::text::regclass) / current_setting('block_size')::bigint",
			c.Relation()).Scan(&pages)
		if err != nil {
			return fmt.Errorf("transfer: size source table: %w", err)
		}
	}
	seq := new(atomic.Uint64)
	p.startN(p.first, readers, func(ctx context.Context, i int) error {
		conn := db
		if i > 0 {
			var err error
			if conn, err = pgx.Connect(ctx, databaseURL); err != nil {
				return err
			}
			defer conn.Close(context.Background())
		}
		return postgres.ReadSeq(ctx, conn, c.PartitionQuery(i, readers, pages), layout, p.pool, p.first, seq)
	})
	p.start(p.second, func(context.Context) error { return p.encode(c, false) })
	// Every sender drains the shared queue and sends its own END on its connection.
	p.startN(nil, len(conns), func(ctx context.Context, i int) error {
		return network.Send(ctx, conns[i], p.pool, p.second, cfg)
	})
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
	// Dial every connection before beginning negotiations
	conns := make([]net.Conn, 0, c.Stages().Network)
	defer func() { closeAll(conns) }()
	for len(conns) < cap(conns) {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if err != nil {
			return result, err
		}
		conns = append(conns, conn)
	}
	stop := context.AfterFunc(ctx, func() { closeAll(conns) })
	defer stop()
	cfg, err := handshakeAll(ctx, conns, func(ctx context.Context, conn net.Conn) (network.Config, error) {
		return network.ClientHandshake(ctx, conn, c.Network())
	})
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
	// The coordinator closes p.first once every receiver wrapped up
	p.startN(p.first, len(conns), func(ctx context.Context, i int) error {
		return network.Receive(ctx, conns[i], p.pool, p.first, cfg)
	})
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
