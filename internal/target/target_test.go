package target

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	rowfmt "github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

var testFields = []rowfmt.Field{
	{Name: "id", Type: rowfmt.Int32, Size: 4},
	{Name: "name", Type: rowfmt.String, Size: 8},
}

type fakeDB struct {
	mu        sync.Mutex
	statement string
	rows      [][]driver.Value
	flushes   int
	begin     int
	commit    int
	rollback  int
	execErr   error
}

type fakeConnector struct{ state *fakeDB }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn{c.state}, nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type fakeConn struct{ state *fakeDB }

func (c fakeConn) Prepare(query string) (driver.Stmt, error) {
	c.state.mu.Lock()
	c.state.statement = query
	c.state.mu.Unlock()
	return fakeStmt{c.state}, nil
}
func (fakeConn) Close() error { return nil }
func (c fakeConn) Begin() (driver.Tx, error) {
	c.state.mu.Lock()
	c.state.begin++
	c.state.mu.Unlock()
	return fakeTx{c.state}, nil
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
	if len(args) > 0 {
		s.state.rows = append(s.state.rows, append([]driver.Value(nil), args...))
	} else {
		s.state.flushes++
	}
	return driver.RowsAffected(1), nil
}
func (fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("query not expected")
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
	db := sql.OpenDB(fakeConnector{state})
	t.Cleanup(func() { _ = db.Close() })
	w, err := New(db, Config{Schema: "public", Table: "customers", Fields: testFields})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return w, state
}

func encodedRows(t *testing.T, rows ...struct {
	id   int32
	name string
}) []byte {
	t.Helper()
	layout, err := rowfmt.NewLayout(testFields)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, len(rows)*layout.RowSize())
	for i, values := range rows {
		r, err := layout.RowAt(payload, i)
		if err != nil {
			t.Fatal(err)
		}
		if err := layout.PutInt32(r, 0, values.id); err != nil {
			t.Fatal(err)
		}
		if err := layout.PutString(r, 1, values.name); err != nil {
			t.Fatal(err)
		}
	}
	return payload
}

func TestNewValidatesDestinationAndSchema(t *testing.T) {
	state := &fakeDB{}
	db := sql.OpenDB(fakeConnector{state})
	t.Cleanup(func() { _ = db.Close() })
	for _, cfg := range []Config{
		{Table: "bad; DROP", Fields: testFields},
		{Schema: "bad.schema", Table: "t", Fields: testFields},
		{Table: "t", Fields: []rowfmt.Field{{Name: "bad name", Type: rowfmt.Int32, Size: 4}}},
		{Table: "t", Fields: []rowfmt.Field{{Name: "id", Type: rowfmt.Int32, Size: 8}}},
		{Table: "t"},
	} {
		if _, err := New(db, cfg); err == nil {
			t.Errorf("New(%+v) succeeded", cfg)
		}
	}
	if _, err := New(nil, Config{Table: "t", Fields: testFields}); err == nil {
		t.Fatal("New accepted nil database")
	}
}

func TestWriteBatchUsesCopyAndFixedWidthRows(t *testing.T) {
	w, state := newTestWriter(t)
	payload := encodedRows(t, struct {
		id   int32
		name string
	}{7, "Mina"}, struct {
		id   int32
		name string
	}{8, "Lee"})
	if err := w.WriteBatch(context.Background(), payload, 2); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	if want := `COPY "public"."customers" ("id", "name") FROM STDIN`; state.statement != want {
		t.Errorf("statement = %q, want %q", state.statement, want)
	}
	wantRows := [][]driver.Value{{int64(7), "Mina"}, {int64(8), "Lee"}}
	if !reflect.DeepEqual(state.rows, wantRows) {
		t.Errorf("copied rows = %#v, want %#v", state.rows, wantRows)
	}
	if state.flushes != 1 {
		t.Errorf("COPY flushes = %d, want 1", state.flushes)
	}
	if state.begin != 1 || state.commit != 1 || state.rollback != 0 {
		t.Errorf("transactions = %d/%d/%d, want 1/1/0", state.begin, state.commit, state.rollback)
	}
}

func TestWriteBatchRejectsInvalidPayloadBeforeTransaction(t *testing.T) {
	w, state := newTestWriter(t)
	valid := encodedRows(t, struct {
		id   int32
		name string
	}{1, "A"})
	cases := []struct {
		name    string
		payload []byte
		count   uint32
	}{
		{"partial row", valid[:len(valid)-1], 1},
		{"wrong count", valid, 2},
		{"zero count with bytes", valid, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := w.WriteBatch(context.Background(), tc.payload, tc.count); err == nil {
				t.Fatal("WriteBatch succeeded")
			}
		})
	}
	if state.begin != 0 {
		t.Errorf("transactions started = %d, want 0", state.begin)
	}
}

func TestWriteBatchEmptyDoesNotOpenTransaction(t *testing.T) {
	w, state := newTestWriter(t)
	if err := w.WriteBatch(context.Background(), nil, 0); err != nil {
		t.Fatal(err)
	}
	if state.begin != 0 {
		t.Errorf("transactions started = %d, want 0", state.begin)
	}
}

func TestWriteBatchRollsBackOnCopyFailure(t *testing.T) {
	w, state := newTestWriter(t)
	state.execErr = errors.New("simulated COPY failure")
	payload := encodedRows(t, struct {
		id   int32
		name string
	}{1, "A"})
	err := w.WriteBatch(context.Background(), payload, 1)
	if err == nil || !strings.Contains(err.Error(), "simulated COPY failure") {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	if state.begin != 1 || state.commit != 0 || state.rollback != 1 {
		t.Errorf("transactions = %d/%d/%d, want 1/0/1", state.begin, state.commit, state.rollback)
	}
}

func TestConsumeRejectsCompressedBatchAndReleasesBuffer(t *testing.T) {
	w, _ := newTestWriter(t)
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
	(pool.BufferHeader{RecordCount: 1, PayloadLen: 1, UncompressedLen: 1, Compression: 1}).Encode(b[:pool.HeaderSize])
	b[pool.HeaderSize] = 0
	q, err := queue.New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Push(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	close(q)
	err = w.Consume(context.Background(), p, q)
	if err == nil || !strings.Contains(err.Error(), "compressed") {
		t.Fatalf("Consume() error = %v", err)
	}
	reused, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("buffer was not released: %v", err)
	}
	if reused != id {
		t.Errorf("reused ID = %d, want %d", reused, id)
	}
	if err := p.Release(reused); err != nil {
		t.Fatal(err)
	}
}

func TestWriteBatchDistinguishesNullFromZero(t *testing.T) {
	w, state := newTestWriter(t)
	payload := encodedRows(t, struct {
		id   int32
		name string
	}{0, ""}, struct {
		id   int32
		name string
	}{9, "unused"})
	record, err := w.layout.RowAt(payload, 1)
	if err != nil {
		t.Fatal(err)
	}
	for col := range testFields {
		if err := w.layout.PutNull(record, col); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.WriteBatch(context.Background(), payload, 2); err != nil {
		t.Fatal(err)
	}
	if want := [][]driver.Value{{int64(0), ""}, {nil, nil}}; !reflect.DeepEqual(state.rows, want) {
		t.Fatalf("copied rows: %#v, want %#v", state.rows, want)
	}
}

func TestWriteBatchRollsBackMalformedScalar(t *testing.T) {
	state := &fakeDB{}
	db := sql.OpenDB(fakeConnector{state})
	defer db.Close()
	w, err := New(db, Config{Table: "t", Fields: []rowfmt.Field{{Name: "flag", Type: rowfmt.Bool, Size: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	// One bitmap byte, then an invalid non-NULL BOOLEAN slot.
	err = w.WriteBatch(context.Background(), []byte{0, 2}, 1)
	if err == nil || !strings.Contains(err.Error(), "column 0 (flag)") {
		t.Fatalf("missing field context: %v", err)
	}
	if state.rollback != 1 || state.commit != 0 {
		t.Fatalf("malformed scalar was not rolled back: %+v", state)
	}
}
