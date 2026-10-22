// Package target loads row-format XDBC batches into a PostgreSQL relation.
// The destination fields must match the ordered schema used to encode rows.
package target

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

// Config describes the destination relation and the ordered row schema. The
// field names are PostgreSQL column names; types and sizes define the incoming
// binary row layout shared with the source.
type Config struct {
	Schema string
	Table  string
	Fields []row.Field
}

// Writer copies decoded row-format batches into PostgreSQL using COPY FROM STDIN.
type Writer struct {
	db        *sql.DB
	statement string
	layout    *row.Layout
	fields    []row.Field
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// New constructs a writer for the requested destination. Schema may be empty
// to use PostgreSQL's search_path.
func New(db *sql.DB, cfg Config) (*Writer, error) {
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
		return nil, fmt.Errorf("target: invalid row schema: %w", err)
	}
	quotedColumns := make([]string, len(cfg.Fields))
	for i, field := range cfg.Fields {
		if !identifier.MatchString(field.Name) {
			return nil, fmt.Errorf("target: invalid column name %q", field.Name)
		}
		quotedColumns[i] = `"` + field.Name + `"`
	}
	relation := `"` + cfg.Table + `"`
	if cfg.Schema != "" {
		relation = `"` + cfg.Schema + `".` + relation
	}
	return &Writer{
		db: db, layout: layout, fields: append([]row.Field(nil), cfg.Fields...),
		statement: "COPY " + relation + " (" + strings.Join(quotedColumns, ", ") + ") FROM STDIN",
	}, nil
}

// WriteBatch validates and copies recordCount fixed-width rows as one
// transaction. A malformed payload or COPY error rolls the batch back.
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
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("target: begin transaction: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, w.statement)
	if err != nil {
		return fmt.Errorf("target: prepare COPY: %w", err)
	}
	for i := 0; i < int(recordCount); i++ {
		encoded, err := w.layout.RowAt(payload, i)
		if err != nil {
			_ = stmt.Close()
			return fmt.Errorf("target: read row %d: %w", i+1, err)
		}
		values, err := w.decodeRow(encoded)
		if err != nil {
			_ = stmt.Close()
			return fmt.Errorf("target: decode row %d: %w", i+1, err)
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			_ = stmt.Close()
			return fmt.Errorf("target: COPY row %d: %w", i+1, err)
		}
	}
	// lib/pq flushes COPY data when Exec is called with no arguments.
	if _, err := stmt.ExecContext(ctx); err != nil {
		_ = stmt.Close()
		return fmt.Errorf("target: finish COPY: %w", err)
	}
	if err := stmt.Close(); err != nil {
		return fmt.Errorf("target: close COPY: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("target: commit batch: %w", err)
	}
	return nil
}

// Consume reads buffer IDs from the downstream queue, writes each uncompressed
// row-format payload to PostgreSQL, and releases every buffer after processing.
func (w *Writer) Consume(ctx context.Context, p *pool.Pool, input queue.Queue) error {
	if p == nil {
		return errors.New("target: pool is nil")
	}
	for {
		// Pick new buffer id from queue
		id, err := input.Pop(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		// Read and write that batch
		if err := w.consumeBuffer(ctx, p, id); err != nil {
			return errors.Join(err, p.Release(id))
		}
		// Release buffer so new batch can take its space
		if err := p.Release(id); err != nil {
			return fmt.Errorf("target: release buffer %d: %w", id, err)
		}
	}
}

func (w *Writer) consumeBuffer(ctx context.Context, p *pool.Pool, id int) error {
	b, err := p.Buffer(id)
	if err != nil {
		return fmt.Errorf("target: access buffer %d: %w", id, err)
	}
	if len(b) < pool.HeaderSize {
		return errors.New("target: buffer is shorter than its header")
	}

	h := pool.DecodeHeader(b)
	if uint64(h.PayloadLen) > uint64(p.PayloadCap()) {
		return fmt.Errorf("target: buffer %d payload length %d exceeds capacity %d", id, h.PayloadLen, p.PayloadCap())
	}
	if h.Compression != 0 { // For now, not compressed
		return fmt.Errorf("target: buffer %d is compressed; decompress it before writing", id)
	}
	if h.UncompressedLen != h.PayloadLen {
		return fmt.Errorf("target: buffer %d uncompressed length does not match payload length", id)
	}

	end := pool.HeaderSize + int(h.PayloadLen)
	return w.WriteBatch(ctx, b[pool.HeaderSize:end], h.RecordCount)
}
