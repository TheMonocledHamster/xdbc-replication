// Package target loads row-format XDBC batches into PostgreSQL.
package target

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
	"github.com/jackc/pgx/v5"
)

// Config describes the destination relation and ordered row schema.
type Config struct {
	Schema string
	Table  string
	Fields []row.Field
}

type database interface {
	Begin(context.Context) (pgx.Tx, error)
}

// Writer streams decoded rows through pgx binary COPY.
// A Writer and its connection must not be used concurrently.
type Writer struct {
	db       database
	relation pgx.Identifier
	columns  []string
	layout   *row.Layout
	fields   []row.Field
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// New validates identifiers and row layout for an existing destination table.
// The caller owns the connection.
func New(db database, cfg Config) (*Writer, error) {
	if db == nil {
		return nil, errors.New("target: database is nil")
	}
	if !identifier.MatchString(cfg.Table) {
		return nil, fmt.Errorf("target: invalid table name %q", cfg.Table)
	}
	if cfg.Schema != "" && !identifier.MatchString(cfg.Schema) {
		return nil, fmt.Errorf("target: invalid schema name %q", cfg.Schema)
	}
	layout, err := row.NewLayout(cfg.Fields)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(cfg.Fields))
	for i, field := range cfg.Fields {
		if !identifier.MatchString(field.Name) {
			return nil, fmt.Errorf("target: invalid column name %q", field.Name)
		}
		columns[i] = field.Name
	}
	relation := pgx.Identifier{cfg.Table}
	if cfg.Schema != "" {
		relation = pgx.Identifier{cfg.Schema, cfg.Table}
	}
	return &Writer{db: db, relation: relation, columns: columns, layout: layout, fields: append([]row.Field(nil), cfg.Fields...)}, nil
}

// WriteBatch performs a standalone COPY transaction for one batch.
// The transfer runtime uses Consume to keep one transaction across all batches.
func (w *Writer) WriteBatch(ctx context.Context, payload []byte, recordCount uint32) error {
	if w == nil || w.db == nil || w.layout == nil {
		return errors.New("target: writer is not initialized")
	}
	if err := w.layout.ValidatePayload(payload, recordCount); err != nil {
		return fmt.Errorf("target: invalid row payload: %w", err)
	}
	if recordCount == 0 {
		return nil
	}
	return w.copy(ctx, pgx.CopyFromSlice(int(recordCount), func(i int) ([]any, error) {
		return w.decodeRow(payload[i*w.layout.RowSize() : (i+1)*w.layout.RowSize()])
	}))
}

func (w *Writer) copy(ctx context.Context, source pgx.CopyFromSource) error {
	if w == nil || w.db == nil || w.layout == nil {
		return errors.New("target: writer is not initialized")
	}
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("target: begin COPY: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err := tx.CopyFrom(ctx, w.relation, w.columns, source); err != nil {
		return fmt.Errorf("target: COPY: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("target: commit COPY: %w", err)
	}
	return nil
}

// Consume keeps one binary COPY statement and transaction open until clean EOF.
// Any failure rolls back all inserted rows. Every popped buffer is released.
func (w *Writer) Consume(ctx context.Context, p *pool.Pool, input queue.Queue) (err error) {
	if p == nil {
		return errors.New("target: pool is nil")
	}
	if w == nil || w.layout == nil {
		return errors.New("target: writer is not initialized")
	}
	id := -1
	var payload []byte
	var index, count int
	release := func() error {
		if id < 0 {
			return nil
		}
		err := p.Release(id)
		id = -1
		return err
	}
	defer func() { err = errors.Join(err, release()) }()
	source := pgx.CopyFromFunc(func() ([]any, error) {
		for index == count {
			if err := release(); err != nil {
				return nil, err
			}
			next, err := input.Pop(ctx)
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			id = next
			payload, count, err = w.bufferPayload(p, id)
			if err != nil {
				return nil, err
			}
			index = 0
		}
		values, err := w.decodeRow(payload[index*w.layout.RowSize() : (index+1)*w.layout.RowSize()])
		index++
		return values, err
	})
	return w.copy(ctx, source)
}

func (w *Writer) bufferPayload(p *pool.Pool, id int) ([]byte, int, error) {
	b, err := p.Buffer(id)
	if err != nil {
		return nil, 0, fmt.Errorf("target: access buffer %d: %w", id, err)
	}
	if len(b) < pool.HeaderSize {
		return nil, 0, errors.New("target: buffer is shorter than its header")
	}
	h := pool.DecodeHeader(b)
	if uint64(h.PayloadLen) > uint64(p.PayloadCap()) {
		return nil, 0, fmt.Errorf("target: buffer %d payload length %d exceeds capacity %d", id, h.PayloadLen, p.PayloadCap())
	}
	if h.Compression != 0 {
		return nil, 0, fmt.Errorf("target: buffer %d is compressed; decompress it before writing", id)
	}
	if h.UncompressedLen != h.PayloadLen {
		return nil, 0, fmt.Errorf("target: buffer %d uncompressed length does not match payload length", id)
	}
	payload := b[pool.HeaderSize : pool.HeaderSize+int(h.PayloadLen)]
	if err := w.layout.ValidatePayload(payload, h.RecordCount); err != nil {
		return nil, 0, fmt.Errorf("target: invalid row payload: %w", err)
	}
	return payload, int(h.RecordCount), nil
}
