package target

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

// TestPostgresConsumeIntegration exercises the real lib/pq COPY path against
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
	defer db.Close()

	// PostgreSQL temporary tables belong to one session.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	table := fmt.Sprintf("target_it_%d", time.Now().UnixNano())
	quotedTable := `"` + table + `"` // generated identifier contains only safe characters
	if _, err := db.ExecContext(ctx, "CREATE TEMP TABLE "+quotedTable+` (
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

	rows, err := db.QueryContext(ctx, "SELECT id, name FROM "+quotedTable+" ORDER BY id")
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
