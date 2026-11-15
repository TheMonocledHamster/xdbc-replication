package target

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

// Config describes the ordered row schema and whether to emit a header.
type CSVConfig struct {
	Fields []row.Field
	Header bool
}

// CSVWriter decodes XDBC rows and writes CSV records to dst. NULL is represented by
// an unquoted empty field; an empty non-NULL string is written as "".
type CSVWriter struct {
	dst      io.Writer
	closer   io.Closer
	layout   *row.Layout
	fields   []row.Field
	rows     int64
	closed   bool
	closeErr error
}

// NewCSVWriter constructs a CSV writer that writes to dst.
func NewCSVWriter(dst io.Writer, cfg CSVConfig) (*CSVWriter, error) {
	if dst == nil {
		return nil, errors.New("target: CSV destination is nil")
	}
	layout, err := row.NewLayout(cfg.Fields)
	if err != nil {
		return nil, fmt.Errorf("target: invalid CSV row schema: %w", err)
	}
	w := &CSVWriter{dst: dst, layout: layout, fields: append([]row.Field(nil), cfg.Fields...)}
	if cfg.Header {
		header := make([]cell, len(cfg.Fields))
		for i, field := range cfg.Fields {
			header[i] = cell{text: field.Name}
		}
		if err := writeHeader(dst, header); err != nil {
			return nil, fmt.Errorf("target: write CSV header: %w", err)
		}
	}
	return w, nil
}

// OpenCSVFile creates or truncates path and returns a writer that owns the file.
func OpenCSVFile(path string, cfg CSVConfig) (*CSVWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("target: open CSV %q: %w", path, err)
	}
	w, err := NewCSVWriter(f, cfg)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	w.closer = f
	return w, nil
}

// WriteBatch validates and writes recordCount fixed-width rows.
func (w *CSVWriter) WriteBatch(payload []byte, recordCount uint32) error {
	if w == nil || w.layout == nil {
		return errors.New("target: CSV writer is not initialized")
	}
	if w.closed {
		return errors.New("target: CSV writer is closed")
	}
	if err := w.layout.ValidatePayload(payload, recordCount); err != nil {
		return fmt.Errorf("target: invalid CSV row payload: %w", err)
	}
	var batch bytes.Buffer
	encoder := bufio.NewWriter(&batch)
	for n := 0; n < int(recordCount); n++ {
		values, err := decodeRowValues(w.layout, w.fields, payload[n*w.layout.RowSize():(n+1)*w.layout.RowSize()])
		if err != nil {
			return fmt.Errorf("target: decode CSV row %d: %w", n+1, err)
		}
		record, err := w.formatRow(values)
		if err != nil {
			return fmt.Errorf("target: format CSV row %d: %w", n+1, err)
		}
		if err := writeRecord(encoder, record); err != nil {
			return fmt.Errorf("target: write CSV row %d: %w", n+1, err)
		}
	}
	if err := encoder.Flush(); err != nil {
		return fmt.Errorf("target: encode CSV batch: %w", err)
	}
	if err := writeAll(w.dst, batch.Bytes()); err != nil {
		return fmt.Errorf("target: write CSV batch: %w", err)
	}
	w.rows += int64(recordCount)
	return nil
}

func writeHeader(dst io.Writer, record []cell) error {
	var encoded bytes.Buffer
	w := bufio.NewWriter(&encoded)
	if err := writeRecord(w, record); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return writeAll(dst, encoded.Bytes())
}

func writeAll(dst io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := dst.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

type cell struct {
	text string
	null bool
}

func (w *CSVWriter) formatRow(values []any) ([]cell, error) {
	record := make([]cell, len(w.fields))
	for i, field := range w.fields {
		if values[i] == nil {
			record[i].null = true
			continue
		}
		text, err := formatValue(values[i], field.Type)
		if err != nil {
			return nil, fmt.Errorf("column %d (%s): %w", i, field.Name, err)
		}
		record[i].text = text
	}
	return record, nil
}

func formatValue(v any, dtype row.Dtype) (string, error) {
	switch x := v.(type) {
	case int16:
		return strconv.FormatInt(int64(x), 10), nil
	case int32:
		return strconv.FormatInt(int64(x), 10), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case float32:
		return formatFloat(float64(x), 32), nil
	case float64:
		return formatFloat(x, 64), nil
	case bool:
		return strconv.FormatBool(x), nil
	case string:
		return x, nil
	case time.Time:
		switch dtype {
		case row.Date:
			return formatDate(x), nil
		case row.Timestamp:
			return formatTimestamp(x, "2006-01-02 15:04:05.999999"), nil
		case row.TimestampTZ:
			return formatTimestamp(x, "2006-01-02 15:04:05.999999-07:00"), nil
		}
		return "", fmt.Errorf("cannot format temporal value for type %s", dtype)
	default:
		return "", fmt.Errorf("cannot format value of type %T", v)
	}
}

func formatDate(value time.Time) string {
	if value.Year() <= 0 {
		return fmt.Sprintf("%04d-%02d-%02d BC", 1-value.Year(), value.Month(), value.Day())
	}
	return value.Format("2006-01-02")
}

func formatTimestamp(value time.Time, layout string) string {
	formatted := value.Format(layout)
	if value.Year() <= 0 {
		return strings.Replace(formatted, fmt.Sprintf("%04d-", -value.Year()), fmt.Sprintf("%04d-", 1-value.Year()), 1) + " BC"
	}
	return formatted
}

func formatFloat(v float64, bits int) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "Infinity"
	case math.IsInf(v, -1):
		return "-Infinity"
	default:
		return strconv.FormatFloat(v, 'f', -1, bits)
	}
}

// writeRecord emits RFC 4180 escaping. Empty non-NULL values are quoted so
// PostgreSQL COPY CSV can distinguish them from NULL's unquoted empty field.
func writeRecord(out *bufio.Writer, record []cell) error {
	for i, value := range record {
		if i > 0 {
			if err := out.WriteByte(','); err != nil {
				return err
			}
		}
		if value.null {
			continue
		}
		quote := value.text == "" || strings.ContainsAny(value.text, ",\"\r\n")
		if quote {
			if err := out.WriteByte('"'); err != nil {
				return err
			}
		}
		for _, r := range value.text {
			if r == '"' && quote {
				if err := out.WriteByte('"'); err != nil {
					return err
				}
			}
			if _, err := out.WriteRune(r); err != nil {
				return err
			}
		}
		if quote {
			if err := out.WriteByte('"'); err != nil {
				return err
			}
		}
	}
	return out.WriteByte('\n')
}

// Consume writes uncompressed row-format batches and releases each pool buffer.
func (w *CSVWriter) Consume(ctx context.Context, p *pool.Pool, input queue.Queue) error {
	if p == nil {
		return errors.New("target: CSV pool is nil")
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
			return fmt.Errorf("target: release CSV buffer %d: %w", id, err)
		}
	}
}

func (w *CSVWriter) consumeBuffer(id int, p *pool.Pool) error {
	b, err := p.Buffer(id)
	if err != nil {
		return fmt.Errorf("target: access CSV buffer %d: %w", id, err)
	}
	if len(b) < pool.HeaderSize {
		return errors.New("target: CSV buffer is shorter than its header")
	}
	h := pool.DecodeHeader(b)
	if uint64(h.PayloadLen) > uint64(p.PayloadCap()) {
		return fmt.Errorf("target: CSV payload length %d exceeds capacity %d", h.PayloadLen, p.PayloadCap())
	}
	if h.Compression != 0 {
		return errors.New("target: compressed buffer reached CSV sink; decompress it before writing")
	}
	if h.UncompressedLen != h.PayloadLen {
		return errors.New("target: CSV uncompressed length does not match payload length")
	}
	end := pool.HeaderSize + int(h.PayloadLen)
	return w.WriteBatch(b[pool.HeaderSize:end], h.RecordCount)
}

// RowsWritten reports batches successfully written so far.
func (w *CSVWriter) RowsWritten() int64 {
	if w == nil {
		return 0
	}
	return w.rows
}

// Close finalizes output and closes only a file opened by OpenCSVFile.
func (w *CSVWriter) Close() error {
	if w == nil {
		return nil
	}
	if w.closed {
		return w.closeErr
	}
	if w.dst == nil {
		return errors.New("target: CSV writer is not initialized")
	}
	if w.closer != nil {
		w.closeErr = w.closer.Close()
	}
	w.closed = true
	return w.closeErr
}
