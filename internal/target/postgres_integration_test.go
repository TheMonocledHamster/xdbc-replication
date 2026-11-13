package target

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

// TestPostgresConsumeIntegration exercises the real pgx COPY path against
// PostgreSQL. Similar to source-reader integration tests, it runs only when
// XDBC_TEST_DATABASE_URL is set (for example by the repository's Makefile).
func TestPostgresConsumeIntegration(t *testing.T) {
	dsn := os.Getenv("XDBC_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set XDBC_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer db.Close(context.Background())

	// PostgreSQL temporary tables belong to one session.

	table := fmt.Sprintf("target_it_%d", time.Now().UnixNano())
	quotedTable := `"` + table + `"` // generated identifier contains only safe characters
	if _, err := db.Exec(ctx, "CREATE TEMP TABLE "+quotedTable+` (
		id INTEGER NOT NULL,
		name VARCHAR(8) NOT NULL
	)`); err != nil {
		t.Fatalf("create temporary destination table: %v", err)
	}
	fields := []row.Field{
		{Name: "id", Type: row.Int32, Size: 4},
		{Name: "name", Type: row.String, Size: 8},
	}
	writer, err := New(db, Config{Table: table, Fields: fields})
	if err != nil {
		t.Fatalf("create target writer: %v", err)
	}

	layout, err := row.NewLayout(fields)
	if err != nil {
		t.Fatalf("create row layout: %v", err)
	}
	payload := make([]byte, 2*layout.RowSize())
	for i, values := range []struct {
		id   int32
		name string
	}{{7, "Ada"}, {8, "Lin"}} {
		record, err := layout.RowAt(payload, i)
		if err != nil {
			t.Fatalf("get row %d: %v", i+1, err)
		}
		if err := layout.PutInt32(record, 0, values.id); err != nil {
			t.Fatalf("encode row %d id: %v", i+1, err)
		}
		if err := layout.PutString(record, 1, values.name); err != nil {
			t.Fatalf("encode row %d name: %v", i+1, err)
		}
	}

	p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: len(payload)})
	if err != nil {
		t.Fatalf("create buffer pool: %v", err)
	}
	id, err := p.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire buffer: %v", err)
	}
	b, err := p.Buffer(id)
	if err != nil {
		t.Fatalf("access buffer: %v", err)
	}
	(pool.BufferHeader{
		RecordCount: uint32(2), PayloadLen: uint32(len(payload)),
		UncompressedLen: uint32(len(payload)),
	}).Encode(b[:pool.HeaderSize])
	copy(b[pool.HeaderSize:], payload)

	input, err := queue.New(1)
	if err != nil {
		t.Fatalf("create input queue: %v", err)
	}
	if err := input.Push(ctx, id); err != nil {
		t.Fatalf("queue buffer: %v", err)
	}
	close(input)
	if err := writer.Consume(ctx, p, input); err != nil {
		t.Fatalf("consume buffer into PostgreSQL: %v", err)
	}

	rows, err := db.Query(ctx, "SELECT id, name FROM "+quotedTable+" ORDER BY id")
	if err != nil {
		t.Fatalf("query copied rows: %v", err)
	}
	defer rows.Close()
	want := []struct {
		id   int32
		name string
	}{{7, "Ada"}, {8, "Lin"}}
	for i, expected := range want {
		if !rows.Next() {
			t.Fatalf("row %d missing: %v", i+1, rows.Err())
		}
		var gotID int32
		var gotName string
		if err := rows.Scan(&gotID, &gotName); err != nil {
			t.Fatalf("scan row %d: %v", i+1, err)
		}
		if gotID != expected.id || gotName != expected.name {
			t.Errorf("row %d = (%d, %q), want (%d, %q)", i+1, gotID, gotName, expected.id, expected.name)
		}
	}
	if rows.Next() {
		var extraID int32
		var extraName string
		if err := rows.Scan(&extraID, &extraName); err != nil {
			t.Fatalf("scan unexpected row: %v", err)
		}
		t.Errorf("unexpected extra row (%d, %q)", extraID, extraName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate copied rows: %v", err)
	}

	// Consume should return the buffer to the pool after a successful write.
	reusedID, err := p.Acquire(ctx)
	if err != nil {
		t.Fatalf("buffer was not released after consume: %v", err)
	}
	if reusedID != id {
		t.Errorf("reused buffer ID = %d, want %d", reusedID, id)
	}
	if err := p.Release(reusedID); err != nil {
		t.Errorf("release reacquired buffer: %v", err)
	}
}

// A server-side error in a later batch must roll back earlier batches too.
func TestPostgresConsumeRollsBackEarlierBatches(t *testing.T) {
	dsn := os.Getenv("XDBC_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set XDBC_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	table := fmt.Sprintf("target_rollback_%d", time.Now().UnixNano())
	if _, err := db.Exec(ctx, "CREATE TEMP TABLE "+table+" (id INTEGER CHECK (id > 0), name TEXT)"); err != nil {
		t.Fatal(err)
	}
	w, err := New(db, Config{Table: table, Fields: testFields})
	if err != nil {
		t.Fatal(err)
	}
	// Enough data to force pgx to send COPY data before the final bad batch.
	good := encodedRows(t, struct {
		id   int32
		name string
	}{1, "A"})
	payload := bytes.Repeat(good, 20000)
	bad := encodedRows(t, struct {
		id   int32
		name string
	}{-1, "B"})
	p, err := pool.New(pool.Config{NumBuffers: 2, PayloadCap: len(payload)})
	if err != nil {
		t.Fatal(err)
	}
	q, err := queue.New(2)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{payload, bad} {
		id, err := p.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := p.Buffer(id)
		(pool.BufferHeader{RecordCount: uint32(len(data) / len(good)), PayloadLen: uint32(len(data)), UncompressedLen: uint32(len(data))}).Encode(b)
		copy(b[pool.HeaderSize:], data)
		if err := q.Push(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	close(q)
	if err := w.Consume(ctx, p, q); err == nil {
		t.Fatal("expected constraint failure")
	}
	var count int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("earlier batch committed: %d rows", count)
	}
	// A failed COPY must leave the connection usable for the next transfer.
	if err := w.WriteBatch(ctx, good, 1); err != nil {
		t.Fatal(err)
	}
}
