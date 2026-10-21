// Package csvsink writes XDBC fixed-width row batches as CSV.
package csvsink

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

// Config describes the input row schema and CSV-specific output options.
// Fields uses the same row schema as the PostgreSQL target. Field names become
// CSV header cells when Header is true.
type Config struct {
	Fields []row.Field
	Header bool
}

// Writer converts row-format batches to CSV and writes them to dst. The Writer
// serializes calls to WriteBatch
type Writer struct {
	mu       sync.Mutex
	closer   io.Closer
	csv      *csv.Writer
	layout   *row.Layout
	fields   []row.Field
	closed   bool
	closeErr error
}

// New creates a CSV writer for dst
func New(dst io.Writer, cfg Config) (*Writer, error) {
	if dst == nil {
		return nil, errors.New("csvsink: destination is nil")
	}
	layout, err := row.NewLayout(cfg.Fields)
	if err != nil {
		return nil, fmt.Errorf("csvsink: invalid row schema: %w", err)
	}
	w := &Writer{
		csv: csv.NewWriter(dst), layout: layout,
		fields: append([]row.Field(nil), cfg.Fields...),
	}
	if cfg.Header {
		header := make([]string, len(cfg.Fields))
		for i, field := range cfg.Fields {
			header[i] = field.Name
		}
		if err := w.csv.Write(header); err != nil {
			return nil, fmt.Errorf("csvsink: write header: %w", err)
		}
		w.csv.Flush()
		if err := w.csv.Error(); err != nil {
			return nil, fmt.Errorf("csvsink: flush header: %w", err)
		}
	}
	return w, nil
}

// OpenFile creates or truncates path and returns a writer that owns the file.
func OpenFile(path string, cfg Config) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("csvsink: open %q: %w", path, err)
	}
	w, err := New(file, cfg)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	w.closer = file
	return w, nil
}

// WriteBatch validates and appends recordCount rows from the fixed-width row
// payload. Values are formatted as decimal numbers or strings and escaped by
// encoding/csv. A malformed payload is rejected before any batch rows are
// written.
func (w *Writer) WriteBatch(payload []byte, recordCount uint32) error {
	if w == nil || w.layout == nil {
		return errors.New("csvsink: writer is not initialized")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("csvsink: writer is closed")
	}
	if err := w.layout.ValidatePayload(payload, recordCount); err != nil {
		return fmt.Errorf("csvsink: invalid row payload: %w", err)
	}
	for i := 0; i < int(recordCount); i++ {
		encoded, err := w.layout.RowAt(payload, i)
		if err != nil {
			return fmt.Errorf("csvsink: read row %d: %w", i+1, err)
		}
		record, err := w.decodeRow(encoded)
		if err != nil {
			return fmt.Errorf("csvsink: decode row %d: %w", i+1, err)
		}
		if err := w.csv.Write(record); err != nil {
			return fmt.Errorf("csvsink: write row %d: %w", i+1, err)
		}
	}
	w.csv.Flush()
	if err := w.csv.Error(); err != nil {
		return fmt.Errorf("csvsink: flush batch: %w", err)
	}
	return nil
}

func (w *Writer) decodeRow(encoded []byte) ([]string, error) {
	record := make([]string, len(w.fields))
	for i, field := range w.fields {
		switch field.Type {
		case row.Int32:
			value, err := w.layout.GetInt32(encoded, i)
			if err != nil {
				return nil, err
			}
			record[i] = strconv.FormatInt(int64(value), 10)
		case row.Double:
			value, err := w.layout.GetDouble(encoded, i)
			if err != nil {
				return nil, err
			}
			record[i] = strconv.FormatFloat(value, 'g', -1, 64)
		case row.Char:
			value, err := w.layout.GetChar(encoded, i)
			if err != nil {
				return nil, err
			}
			record[i] = string([]byte{value})
		case row.String:
			value, err := w.layout.GetString(encoded, i)
			if err != nil {
				return nil, err
			}
			record[i] = value
		default:
			return nil, fmt.Errorf("unsupported field type %q", field.Type)
		}
	}
	return record, nil
}

// Consume reads buffer IDs from input, writes uncompressed row-format payloads
// to CSV, and releases each buffer after processing. The pipeline coordinator
// owns the pool and queue lifecycle.
func (w *Writer) Consume(ctx context.Context, p *pool.Pool, input queue.Queue) error {
	if p == nil {
		return errors.New("csvsink: pool is nil")
	}
	for {
		id, err := input.Pop(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := w.consumeBuffer(id, p); err != nil {
			return errors.Join(err, p.Release(id))
		}
		if err := p.Release(id); err != nil {
			return fmt.Errorf("csvsink: release buffer %d: %w", id, err)
		}
	}
}

func (w *Writer) consumeBuffer(id int, p *pool.Pool) error {
	b, err := p.Buffer(id)
	if err != nil {
		return fmt.Errorf("csvsink: access buffer %d: %w", id, err)
	}
	if len(b) < pool.HeaderSize {
		return errors.New("csvsink: buffer is shorter than its header")
	}
	h := pool.DecodeHeader(b)
	if uint64(h.PayloadLen) > uint64(p.PayloadCap()) {
		return fmt.Errorf("csvsink: buffer %d payload length %d exceeds capacity %d", id, h.PayloadLen, p.PayloadCap())
	}
	if h.Compression != 0 {
		return fmt.Errorf("csvsink: buffer %d is compressed; decompress it before writing", id)
	}
	if h.UncompressedLen != h.PayloadLen {
		return fmt.Errorf("csvsink: buffer %d uncompressed length does not match payload length", id)
	}
	end := pool.HeaderSize + int(h.PayloadLen)
	return w.WriteBatch(b[pool.HeaderSize:end], h.RecordCount)
}

// Close flushes pending CSV data and closes the destination only when it was
// opened by OpenFile. It is safe to call Close more than once.
func (w *Writer) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.csv == nil {
		return errors.New("csvsink: writer is not initialized")
	}
	if w.closed {
		return w.closeErr
	}
	w.csv.Flush()
	w.closeErr = w.csv.Error()
	if w.closer != nil {
		w.closeErr = errors.Join(w.closeErr, w.closer.Close())
	}
	w.closed = true
	return w.closeErr
}
