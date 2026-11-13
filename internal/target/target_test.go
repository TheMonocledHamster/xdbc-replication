package target

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"reflect"
	"strings"
	"testing"
	"time"

	rowfmt "github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

var testFields = []rowfmt.Field{
	{Name: "id", Type: rowfmt.Int32, Size: 4},
	{Name: "name", Type: rowfmt.String, Size: 8},
}

type fakeDB struct {
	statement                        string
	rows                             [][]any
	flushes, begin, commit, rollback int
	execErr                          error
}

func (db *fakeDB) Begin(context.Context) (pgx.Tx, error) {
	db.begin++
	return &fakeTx{state: db}, nil
}

type fakeTx struct {
	pgx.Tx
	state *fakeDB
	done  bool
}

func (tx *fakeTx) CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, source pgx.CopyFromSource) (int64, error) {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = pgx.Identifier{col}.Sanitize()
	}
	tx.state.statement = "COPY " + table.Sanitize() + " (" + strings.Join(quoted, ", ") + ") FROM STDIN"
	for source.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		values, err := source.Values()
		if err != nil {
			return 0, err
		}
		if tx.state.execErr != nil {
			return 0, tx.state.execErr
		}
		tx.state.rows = append(tx.state.rows, append([]any(nil), values...))
	}
	if err := source.Err(); err != nil {
		return 0, err
	}
	tx.state.flushes++
	return int64(len(tx.state.rows)), nil
}
func (tx *fakeTx) Commit(context.Context) error { tx.state.commit++; tx.done = true; return nil }
func (tx *fakeTx) Rollback(context.Context) error {
	if tx.done {
		return pgx.ErrTxClosed
	}
	tx.state.rollback++
	tx.done = true
	return nil
}

func newTestWriter(t *testing.T) (*Writer, *fakeDB) {
	t.Helper()
	state := &fakeDB{}
	db := state
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
	db := state
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
	wantRows := [][]any{{int32(7), "Mina"}, {int32(8), "Lee"}}
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
	if want := [][]any{{int32(0), ""}, {nil, nil}}; !reflect.DeepEqual(state.rows, want) {
		t.Fatalf("copied rows: %#v, want %#v", state.rows, want)
	}
}

func TestWriteBatchRollsBackMalformedScalar(t *testing.T) {
	state := &fakeDB{}
	db := state
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

func TestConsumeUsesOneTransactionAcrossBatches(t *testing.T) {
	for _, malformedLast := range []bool{false, true} {
		t.Run(fmt.Sprint("malformedLast=", malformedLast), func(t *testing.T) {
			w, state := newTestWriter(t)
			payload := encodedRows(t, struct {
				id   int32
				name string
			}{1, "A"})
			p, err := pool.New(pool.Config{NumBuffers: 3, PayloadCap: len(payload)})
			if err != nil {
				t.Fatal(err)
			}
			q, err := queue.New(3)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				id, err := p.Acquire(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				b, _ := p.Buffer(id)
				count := uint32(1)
				if malformedLast && i == 2 {
					count = 2
				}
				(pool.BufferHeader{RecordCount: count, PayloadLen: uint32(len(payload)), UncompressedLen: uint32(len(payload))}).Encode(b)
				copy(b[pool.HeaderSize:], payload)
				if err := q.Push(context.Background(), id); err != nil {
					t.Fatal(err)
				}
			}
			close(q)
			err = w.Consume(context.Background(), p, q)
			if malformedLast {
				if err == nil || state.begin != 1 || state.commit != 0 || state.rollback != 1 || state.flushes != 0 || len(state.rows) != 2 {
					t.Fatalf("late failure: err=%v, state=%+v", err, state)
				}
			} else if err != nil || state.begin != 1 || state.commit != 1 || state.rollback != 0 || state.flushes != 1 || len(state.rows) != 3 {
				t.Fatalf("success: err=%v, state=%+v", err, state)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for i := 0; i < 3; i++ {
				if _, err := p.Acquire(ctx); err != nil {
					t.Fatalf("buffer leaked: %v", err)
				}
			}
		})
	}
}

func TestConsumeCancellationRollsBack(t *testing.T) {
	w, state := newTestWriter(t)
	payload := encodedRows(t, struct {
		id   int32
		name string
	}{1, "A"})
	p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: len(payload)})
	if err != nil {
		t.Fatal(err)
	}
	q, err := queue.New(1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := p.Buffer(id)
	(pool.BufferHeader{RecordCount: 1, PayloadLen: uint32(len(payload)), UncompressedLen: uint32(len(payload))}).Encode(b)
	copy(b[pool.HeaderSize:], payload)
	if err := q.Push(ctx, id); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.Consume(ctx, p, q) }()
	// The buffer becomes available only after the first row has been consumed.
	recycled, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release(recycled)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not stop on cancellation")
	}
	if state.begin != 1 || state.commit != 0 || state.rollback != 1 || len(state.rows) != 1 {
		t.Fatalf("cancellation did not roll back: %+v", state)
	}
}

func TestConsumeEmptyInput(t *testing.T) {
	w, state := newTestWriter(t)
	p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: 64})
	if err != nil {
		t.Fatal(err)
	}
	q, err := queue.New(1)
	if err != nil {
		t.Fatal(err)
	}
	close(q)
	if err := w.Consume(context.Background(), p, q); err != nil {
		t.Fatal(err)
	}
	if state.begin != 1 || state.commit != 1 || len(state.rows) != 0 {
		t.Fatalf("empty input: %+v", state)
	}
}
