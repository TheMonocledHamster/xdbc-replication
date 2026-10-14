// Package target loads decoded XDBC batches into a PostgreSQL relation.
//
// The MVP payload format is newline-delimited JSON: each line is a JSON array
// containing one row's values, in the configured column order. JSON null maps
// to SQL NULL. 
package target

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

// Config describes the destination relation. Identifiers are validated and
// quoted before being included in SQL; values are always passed as parameters.
type Config struct {
	Schema  string
	Table   string
	Columns []string
}

// Writer inserts decoded row batches into PostgreSQL using database/sql.
// The caller supplies a PostgreSQL driver when opening the *sql.DB.
type Writer struct {
	db        *sql.DB
	statement string
	width     int
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
	if len(cfg.Columns) == 0 {
		return nil, errors.New("target: at least one column is required")
	}
	quotedColumns := make([]string, len(cfg.Columns))
	for i, column := range cfg.Columns {
		if !identifier.MatchString(column) {
			return nil, fmt.Errorf("target: invalid column name %q", column)
		}
		quotedColumns[i] = `"` + column + `"`
	}
	relation := `"` + cfg.Table + `"`
	if cfg.Schema != "" {
		relation = `"` + cfg.Schema + `".` + relation
	}
	placeholders := make([]string, len(cfg.Columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return &Writer{
		db: db, width: len(cfg.Columns),
		statement: "INSERT INTO " + relation + " (" + strings.Join(quotedColumns, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ")",
	}, nil
}

// Validate and insert recordCount rows from payload as a single transaction. 
// A malformed row or SQL error rolls the whole batch back.
func (w *Writer) WriteBatch(ctx context.Context, payload []byte, recordCount uint32) error {
	if w == nil || w.db == nil {
		return errors.New("target: writer is not initialized")
	}
	if recordCount == 0 {
		if len(bytes.TrimSpace(payload)) != 0 {
			return errors.New("target: non-empty payload has zero records")
		}
		return nil
	}
	rows := make([][]any, 0, recordCount)
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	
	// Scanner's default 64 KiB limit is too small for legitimate wide rows.
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			return errors.New("target: empty row in payload")
		}
		var row []any
		if err := json.Unmarshal(line, &row); err != nil {
			return fmt.Errorf("target: decode row %d: %w", len(rows)+1, err)
		}
		if len(row) != w.width {
			return fmt.Errorf("target: row %d has %d values; want %d", len(rows)+1, len(row), w.width)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("target: read payload: %w", err)
	}
	if uint64(len(rows)) != uint64(recordCount) {
		return fmt.Errorf("target: payload has %d rows; header declares %d", len(rows), recordCount)
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("target: begin transaction: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, w.statement)
	if err != nil {
		return fmt.Errorf("target: prepare insert: %w", err)
	}
	defer stmt.Close()
	for i, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return fmt.Errorf("target: insert row %d: %w", i+1, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("target: commit batch: %w", err)
	}
	return nil
}

// Consume reads buffer IDs from the downstream queue, writes each uncompressed
// row-format payload to PostgreSQL, and releases every buffer after processing.
// It returns successfully when the queue is closed and drained. Decompression
// and any format conversion must happen in an upstream pipeline stage.
func (w *Writer) Consume(ctx context.Context, p *pool.Pool, input queue.Queue) error {
	if p == nil {
		return errors.New("target: pool is nil")
	}
	for {
		id, err := input.Pop(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := w.consumeBuffer(ctx, p, id); err != nil {
			return errors.Join(err, p.Release(id))
		}
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
	if h.Compression != 0 {
		return fmt.Errorf("target: buffer %d is compressed; decompress it before writing", id)
	}
	if h.UncompressedLen != h.PayloadLen {
		return fmt.Errorf("target: buffer %d uncompressed length does not match payload length", id)
	}
	end := pool.HeaderSize + int(h.PayloadLen)
	return w.WriteBatch(ctx, b[pool.HeaderSize:end], h.RecordCount)
}
