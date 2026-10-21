package csvsink

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

var csvTestFields = []row.Field{
	{Name: "id", Type: row.Int32, Size: 4},
	{Name: "ratio", Type: row.Double, Size: 8},
	{Name: "flag", Type: row.Char, Size: 1},
	{Name: "description", Type: row.String, Size: 24},
}

func csvTestLayout(t *testing.T) *row.Layout {
	t.Helper()
	layout, err := row.NewLayout(csvTestFields)
	if err != nil {
		t.Fatal(err)
	}
	return layout
}

func csvTestPayload(t *testing.T, id int32, ratio float64, flag byte, description string) []byte {
	t.Helper()
	layout := csvTestLayout(t)
	payload := make([]byte, layout.RowSize())
	if err := layout.PutInt32(payload, 0, id); err != nil {
		t.Fatal(err)
	}
	if err := layout.PutDouble(payload, 1, ratio); err != nil {
		t.Fatal(err)
	}
	if err := layout.PutChar(payload, 2, flag); err != nil {
		t.Fatal(err)
	}
	if err := layout.PutString(payload, 3, description); err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestNewValidatesDestinationAndSchema(t *testing.T) {
	if _, err := New(nil, Config{Fields: csvTestFields}); err == nil {
		t.Fatal("New() accepted a nil destination")
	}
	if _, err := New(io.Discard, Config{}); err == nil {
		t.Fatal("New() accepted an empty schema")
	}
	if _, err := New(io.Discard, Config{Fields: []row.Field{{Name: "id", Type: row.Int32, Size: 8}}}); err == nil {
		t.Fatal("New() accepted a schema with an invalid INT width")
	}
}

func TestWriteBatchEncodesTypesAndEscapesCSV(t *testing.T) {
	var output bytes.Buffer
	w, err := New(&output, Config{Fields: csvTestFields, Header: true})
	if err != nil {
		t.Fatal(err)
	}
	payload := csvTestPayload(t, -3, 2.5, 'Q', "Hello, \"world\"\n!")
	if err := w.WriteBatch(payload, 1); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	records, err := csv.NewReader(&output).ReadAll()
	if err != nil {
		t.Fatalf("parse generated CSV: %v\n%s", err, output.String())
	}
	want := [][]string{
		{"id", "ratio", "flag", "description"},
		{"-3", "2.5", "Q", "Hello, \"world\"\n!"},
	}
	if !reflect.DeepEqual(records, want) {
		t.Errorf("CSV records = %#v, want %#v", records, want)
	}
}

func TestWriteBatchRejectsMalformedPayloadWithoutWritingBatch(t *testing.T) {
	var output bytes.Buffer
	w, err := New(&output, Config{Fields: csvTestFields})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBatch([]byte{1, 2, 3}, 1); err == nil {
		t.Fatal("WriteBatch() accepted a partial tuple")
	}
	if output.Len() != 0 {
		t.Errorf("malformed batch wrote %q", output.String())
	}
}

func TestConsumeWritesAndReleasesBuffer(t *testing.T) {
	var output bytes.Buffer
	w, err := New(&output, Config{Fields: csvTestFields})
	if err != nil {
		t.Fatal(err)
	}
	payload := csvTestPayload(t, 42, 1.25, 'Y', "row")
	p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: len(payload)})
	if err != nil {
		t.Fatal(err)
	}
	id, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Buffer(id)
	if err != nil {
		t.Fatal(err)
	}
	(pool.BufferHeader{
		RecordCount: 1, PayloadLen: uint32(len(payload)), UncompressedLen: uint32(len(payload)),
	}).Encode(b[:pool.HeaderSize])
	copy(b[pool.HeaderSize:], payload)
	input, err := queue.New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := input.Push(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	close(input)
	if err := w.Consume(context.Background(), p, input); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if got, want := output.String(), "42,1.25,Y,row\n"; got != want {
		t.Errorf("CSV output = %q, want %q", got, want)
	}
	reused, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Consume did not release buffer: %v", err)
	}
	if reused != id {
		t.Errorf("reused buffer ID = %d, want %d", reused, id)
	}
	if err := p.Release(reused); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConsumeRejectsCompressedBufferAndReleasesIt(t *testing.T) {
	w, err := New(io.Discard, Config{Fields: csvTestFields})
	if err != nil {
		t.Fatal(err)
	}
	p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: 1})
	if err != nil {
		t.Fatal(err)
	}
	id, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Buffer(id)
	if err != nil {
		t.Fatal(err)
	}
	(pool.BufferHeader{PayloadLen: 1, UncompressedLen: 1, Compression: 1}).Encode(b[:pool.HeaderSize])
	input, err := queue.New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := input.Push(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	close(input)
	err = w.Consume(context.Background(), p, input)
	if err == nil || !strings.Contains(err.Error(), "compressed") {
		t.Fatalf("Consume() error = %v, want compressed-buffer error", err)
	}
	reused, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("buffer was not released: %v", err)
	}
	if reused != id {
		t.Errorf("reused buffer ID = %d, want %d", reused, id)
	}
	if err := p.Release(reused); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenFileWritesCSVAndClosesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rows.csv")
	w, err := OpenFile(path, Config{Fields: csvTestFields, Header: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBatch(csvTestPayload(t, 9, 3.5, 'N', "file output"), 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "id,ratio,flag,description\n9,3.5,N,file output\n"; got != want {
		t.Errorf("file = %q, want %q", got, want)
	}
	if err := w.WriteBatch(nil, 0); err == nil {
		t.Fatal("WriteBatch() succeeded after Close")
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestWriteBatchReturnsOutputError(t *testing.T) {
	writeErr := errors.New("disk write failed")
	w, err := New(failingWriter{writeErr}, Config{Fields: csvTestFields})
	if err != nil {
		t.Fatal(err)
	}
	err = w.WriteBatch(csvTestPayload(t, 1, 1, 'A', "x"), 1)
	if !errors.Is(err, writeErr) {
		t.Fatalf("WriteBatch() error = %v, want %v", err, writeErr)
	}
	if err := w.Close(); !errors.Is(err, writeErr) {
		t.Fatalf("Close() error = %v, want %v", err, writeErr)
	}
}
