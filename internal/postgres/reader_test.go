package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
	"github.com/jackc/pgx/v5"
)

type testRow struct {
	id   int32
	name string
}

//CURRENT TESTS

func TestEmpty(t *testing.T) {
	// INPUT: 0 rows, 24-byte payloads.
	// EXPECTED OUTPUT: No queued buffers and no decoded rows.
	checkReader(t, nil, 24, nil)
}

func TestSmall001(t *testing.T) {
	// INPUT: (7, Ada), (8, Lin); 24-byte payloads.
	// EXPECTED OUTPUT: One buffer containing both rows.
	checkReader(t, []testRow{{7, "Ada"}, {8, "Lin"}}, 24, []int{2})
}

func TestSmall002(t *testing.T) {
	// INPUT: Five rows; 24-byte payloads.
	// EXPECTED OUTPUT: Three buffers containing 2, 2, and 1 rows.
	checkReader(t, []testRow{
		{1, "One"}, {2, "Two"}, {3, "Three"}, {4, "Four"}, {5, "Five"},
	}, 24, []int{2, 2, 1})
}

func TestSmall003(t *testing.T) {
	// INPUT: 20 rows, each 12 bytes.
	//        Test every payload capacity from 12 to 120 bytes.
	// EXPECTED OUTPUT: All 20 rows preserved, with correct buffer counts
	//                  and contents for every payload size.

	rows := make([]testRow, 20)
	for i := range rows {
		rows[i] = testRow{
			int32(i + 1),
			fmt.Sprintf("N%02d", i+1),
		}
	}

	// RUN: Test all 109 valid payload capacities.
	for payloadCap := 12; payloadCap <= 120; payloadCap++ {
		rowsPerBuffer := payloadCap / 12

		// EXPECTED: Number of rows in each output buffer.
		var expectedBuffers []int
		remaining := len(rows)

		for remaining > 0 {
			count := min(remaining, rowsPerBuffer)
			expectedBuffers = append(expectedBuffers, count)
			remaining -= count
		}

		// ASSERT: Verify buffer counts, headers, and decoded rows.
		checkReader(t, rows, payloadCap, expectedBuffers)
	}

	t.Log("PASS: all 109 payload sizes (12–120 bytes), 20 rows each")
}

//BOILERPLATE

// checkReader is shared test machinery. Individual tests only specify inputs
// and expected buffer counts; the helper checks every decoded value too.
func checkReader(t *testing.T, input []testRow, payloadCap int, expectedCounts []int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// INPUT: Connect to the configured PostgreSQL database and create a temporary test table.
	url := os.Getenv("XDBC_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("Set XDBC_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(ctx, `CREATE TEMP TABLE reader_test_data (
		id INTEGER NOT NULL, name VARCHAR(8) NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range input {
		if _, err := conn.Exec(ctx,
			"INSERT INTO reader_test_data (id, name) VALUES ($1, $2)", r.id, r.name,
		); err != nil {
			t.Fatal(err)
		}
	}

	layout, err := row.NewLayout([]row.Field{
		{Name: "id", Type: row.Int32, Size: 4},
		{Name: "name", Type: row.String, Size: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Only two buffers exist, so the five-row test must reuse one.
	p, err := pool.New(pool.Config{NumBuffers: 2, PayloadCap: payloadCap})
	if err != nil {
		t.Fatal(err)
	}
	q, err := queue.New(2)
	if err != nil {
		t.Fatal(err)
	}

	// RUN: Read concurrently with the test consumer; close the queue when done.
	readerDone := make(chan error, 1)
	go func() {
		readerDone <- Read(ctx, conn,
			"SELECT id, name FROM reader_test_data ORDER BY id", layout, p, q)
		close(q) // This test has exactly one producer and owns the queue.
	}()

	// ASSERT OUTPUT: Read each queued buffer, decode it and compare to input.
	rowIndex, bufferIndex := 0, 0
	for {
		id, err := q.Pop(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		buffer, err := p.Buffer(id)
		if err != nil {
			t.Fatal(err)
		}
		header := pool.DecodeHeader(buffer)
		if bufferIndex >= len(expectedCounts) {
			t.Fatalf("unexpected extra buffer %d", bufferIndex)
		}
		wantCount := expectedCounts[bufferIndex]
		wantBytes := wantCount * layout.RowSize()
		if int(header.RecordCount) != wantCount ||
			int(header.PayloadLen) != wantBytes ||
			int(header.UncompressedLen) != wantBytes {
			t.Fatalf("buffer %d: got count=%d payload=%d uncompressed=%d; want count=%d bytes=%d",
				bufferIndex, header.RecordCount, header.PayloadLen, header.UncompressedLen,
				wantCount, wantBytes)
		}
		payload := buffer[pool.HeaderSize : pool.HeaderSize+wantBytes]
		if err := layout.ValidatePayload(payload, header.RecordCount); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < wantCount; i++ {
			record, err := layout.RowAt(payload, i)
			if err != nil {
				t.Fatal(err)
			}
			gotID, err := layout.GetInt32(record, 0)
			if err != nil {
				t.Fatal(err)
			}
			gotName, err := layout.GetString(record, 1)
			if err != nil {
				t.Fatal(err)
			}
			if rowIndex >= len(input) || gotID != input[rowIndex].id || gotName != input[rowIndex].name {
				t.Fatalf("row %d: got (%d, %s), expected input %v",
					rowIndex, gotID, gotName, input)
			}
			rowIndex++
		}
		bufferIndex++
		if err := p.Release(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-readerDone; err != nil {
		t.Fatal(fmt.Errorf("reader failed: %w", err))
	}
	if rowIndex != len(input) || bufferIndex != len(expectedCounts) {
		t.Fatalf("got %d rows in %d buffers; want %d rows in %d buffers",
			rowIndex, bufferIndex, len(input), len(expectedCounts))
	}
}
