package target

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/network"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

type fakeDB struct {
	mu        sync.Mutex
	statement string
	rows      [][]driver.Value
	begin     int
	commit    int
	rollback  int
	execErr   error
}

type fakeConnector struct{ state *fakeDB }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return fakeConn{state: c.state}, nil
}
func (c fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fake driver must be opened through its connector")
}

type fakeConn struct{ state *fakeDB }

func (c fakeConn) Prepare(query string) (driver.Stmt, error) {
	c.state.mu.Lock()
	c.state.statement = query
	c.state.mu.Unlock()
	return fakeStmt{state: c.state}, nil
}
func (fakeConn) Close() error { return nil }
func (c fakeConn) Begin() (driver.Tx, error) {
	c.state.mu.Lock()
	c.state.begin++
	c.state.mu.Unlock()
	return fakeTx{state: c.state}, nil
}

type fakeStmt struct{ state *fakeDB }

func (fakeStmt) Close() error  { return nil }
func (fakeStmt) NumInput() int { return -1 }
func (s fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.execErr != nil {
		return nil, s.state.execErr
	}
	s.state.rows = append(s.state.rows, append([]driver.Value(nil), args...))
	return driver.RowsAffected(1), nil
}
func (fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("queries are not expected in target writer tests")
}

type fakeTx struct{ state *fakeDB }

func (tx fakeTx) Commit() error {
	tx.state.mu.Lock()
	tx.state.commit++
	tx.state.mu.Unlock()
	return nil
}
func (tx fakeTx) Rollback() error {
	tx.state.mu.Lock()
	tx.state.rollback++
	tx.state.mu.Unlock()
	return nil
}

func newTestWriter(t *testing.T) (*Writer, *fakeDB) {
	t.Helper()
	state := &fakeDB{}
	db := sql.OpenDB(fakeConnector{state: state})
	t.Cleanup(func() { _ = db.Close() })
	writer, err := New(db, Config{
		Schema: "public", Table: "customers", Columns: []string{"id", "name"},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return writer, state
}

func TestNewValidatesDestinationIdentifiers(t *testing.T) {
	state := &fakeDB{}
	db := sql.OpenDB(fakeConnector{state: state})
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "invalid table", cfg: Config{Table: "customers; DROP TABLE x", Columns: []string{"id"}}},
		{name: "invalid schema", cfg: Config{Schema: "public.schema", Table: "customers", Columns: []string{"id"}}},
		{name: "invalid column", cfg: Config{Table: "customers", Columns: []string{"id, name"}}},
		{name: "no columns", cfg: Config{Table: "customers"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(db, tt.cfg); err == nil {
				t.Fatal("New() succeeded for invalid destination")
			}
		})
	}
	if _, err := New(nil, Config{Table: "customers", Columns: []string{"id"}}); err == nil {
		t.Fatal("New() succeeded with a nil database")
	}
}

func TestWriteBatchInsertsValidRowsInOneTransaction(t *testing.T) {
	writer, state := newTestWriter(t)
	payload := []byte("[1,\"Ava\"]\n[2,null]\n")
	if err := writer.WriteBatch(context.Background(), payload, 2); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	wantStatement := `INSERT INTO "public"."customers" ("id", "name") VALUES ($1, $2)`
	if state.statement != wantStatement {
		t.Errorf("statement = %q, want %q", state.statement, wantStatement)
	}
	wantRows := [][]driver.Value{{float64(1), "Ava"}, {float64(2), nil}}
	if !reflect.DeepEqual(state.rows, wantRows) {
		t.Errorf("inserted rows = %#v, want %#v", state.rows, wantRows)
	}
	if state.begin != 1 || state.commit != 1 || state.rollback != 0 {
		t.Errorf("transaction counts begin/commit/rollback = %d/%d/%d, want 1/1/0", state.begin, state.commit, state.rollback)
	}
}

func TestWriteBatchRejectsInvalidPayloadBeforeDatabaseWork(t *testing.T) {
	writer, state := newTestWriter(t)
	tests := []struct {
		name    string
		payload string
		count   uint32
	}{
		{name: "malformed JSON", payload: "[1,\"Ava\"", count: 1},
		{name: "wrong number of values", payload: "[1]\n", count: 1},
		{name: "wrong record count", payload: "[1,\"Ava\"]\n", count: 2},
		{name: "blank row", payload: "[1,\"Ava\"]\n\n", count: 2},
		{name: "payload with zero records", payload: "[1,\"Ava\"]", count: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := writer.WriteBatch(context.Background(), []byte(tt.payload), tt.count); err == nil {
				t.Fatal("WriteBatch() succeeded for invalid payload")
			}
		})
	}
	if state.begin != 0 {
		t.Errorf("database transactions started = %d, want 0", state.begin)
	}
}

func TestWriteBatchAcceptsEmptyBatch(t *testing.T) {
	writer, state := newTestWriter(t)
	if err := writer.WriteBatch(context.Background(), nil, 0); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	if state.begin != 0 {
		t.Errorf("database transactions started = %d, want 0", state.begin)
	}
}

func TestWriteBatchRollsBackOnInsertFailure(t *testing.T) {
	writer, state := newTestWriter(t)
	state.execErr = errors.New("simulated insert failure")
	err := writer.WriteBatch(context.Background(), []byte("[1,\"Ava\"]\n"), 1)
	if err == nil || !strings.Contains(err.Error(), "simulated insert failure") {
		t.Fatalf("WriteBatch() error = %v, want simulated insert failure", err)
	}
	if state.begin != 1 || state.commit != 0 || state.rollback != 1 {
		t.Errorf("transaction counts begin/commit/rollback = %d/%d/%d, want 1/0/1", state.begin, state.commit, state.rollback)
	}
}

func TestConsumeRejectsCompressedBatchAndReleasesBuffer(t *testing.T) {
	writer, _ := newTestWriter(t)
	p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: 64})
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
	payload := []byte("[1,\"Ava\"]\n")
	(pool.BufferHeader{
		RecordCount: 1, PayloadLen: uint32(len(payload)),
		UncompressedLen: uint32(len(payload)), Compression: 1,
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

	err = writer.Consume(context.Background(), p, input)
	if err == nil || !strings.Contains(err.Error(), "compressed") {
		t.Fatalf("Consume() error = %v, want compressed-buffer error", err)
	}

	reusedID, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() after failed consume: %v", err)
	}
	if reusedID != id {
		t.Errorf("reused buffer ID = %d, want %d", reusedID, id)
	}
	if err := p.Release(reusedID); err != nil {
		t.Fatal(err)
	}
}

func TestRunReceiverTransfersNetworkBatchIntoPostgres(t *testing.T) {
	writer, state := newTestWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := network.Config{
		PayloadCap: 128, Compression: network.CompressionNone, Format: network.FormatRow,
	}
	sourcePool, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: cfg.PayloadCap})
	if err != nil {
		t.Fatal(err)
	}
	bufferID, err := sourcePool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sourceBuffer, err := sourcePool.Buffer(bufferID)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("[7,\"Mina\"]\n")
	(pool.BufferHeader{
		Seq: 1, RecordCount: 1, PayloadLen: uint32(len(payload)),
		UncompressedLen: uint32(len(payload)),
	}).Encode(sourceBuffer[:pool.HeaderSize])
	copy(sourceBuffer[pool.HeaderSize:], payload)

	sourceQueue, err := queue.New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceQueue.Push(ctx, bufferID); err != nil {
		t.Fatal(err)
	}
	close(sourceQueue)

	sourceConn, targetConn := net.Pipe()
	sourceDone := make(chan error, 1)
	go func() {
		defer sourceConn.Close()
		accepted, err := network.ServerHandshake(ctx, sourceConn, cfg)
		if err == nil {
			err = network.Send(ctx, sourceConn, sourcePool, sourceQueue, accepted)
		}
		sourceDone <- err
	}()

	err = writer.RunReceiver(ctx, targetConn, RuntimeConfig{
		Network: cfg, NumBuffers: 2, QueueCapacity: 1,
	})
	if err != nil {
		t.Fatalf("RunReceiver() error = %v", err)
	}
	if err := <-sourceDone; err != nil {
		t.Fatalf("source sender error = %v", err)
	}
	wantRows := [][]driver.Value{{float64(7), "Mina"}}
	if !reflect.DeepEqual(state.rows, wantRows) {
		t.Errorf("inserted rows = %#v, want %#v", state.rows, wantRows)
	}
	if state.commit != 1 {
		t.Errorf("committed transactions = %d, want 1", state.commit)
	}
}

func TestRunReceiverRejectsUnsupportedMode(t *testing.T) {
	writer, _ := newTestWriter(t)
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	err := writer.RunReceiver(context.Background(), left, RuntimeConfig{
		Network: network.Config{
			PayloadCap: 128, Compression: network.CompressionSnappy, Format: network.FormatRow,
		},
		NumBuffers: 1, QueueCapacity: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "uncompressed") {
		t.Fatalf("RunReceiver() error = %v, want unsupported compression error", err)
	}
}
