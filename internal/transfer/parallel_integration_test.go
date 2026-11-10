package transfer

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/jackc/pgx/v5"
)

// A table that spans many heap pages is split among several readers by ctid
// range. Every row must reach the sink exactly once, however the readers,
// compressors, connections and writers interleave.
func TestParallelTransferNoLossNoDuplicates(t *testing.T) {
	sourceURL, targetURL := os.Getenv("XDBC_TEST_SOURCE_DATABASE_URL"), os.Getenv("XDBC_TEST_TARGET_DATABASE_URL")
	if sourceURL == "" || targetURL == "" {
		t.Skip("set XDBC_TEST_SOURCE_DATABASE_URL and XDBC_TEST_TARGET_DATABASE_URL for two-database round trips")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	source, err := pgx.Connect(ctx, sourceURL)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close(context.Background())
	sink, err := pgx.Connect(ctx, targetURL)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close(context.Background())

	const rows = 20000
	const pad = 100
	schema := fmt.Sprintf("parallel_it_%d", time.Now().UnixNano())
	fields := []row.Field{{Name: "id", Type: row.Int32, Size: 4}, {Name: "s", Type: row.String, Size: pad}}
	layout, err := row.NewLayout(fields)
	if err != nil {
		t.Fatal(err)
	}
	base := Config{Schema: schema, Table: "t", Fields: fields, BufferSize: 20 * layout.RowSize(), QueueCapacity: 2,
		Workers: Workers{Read: 4, Compress: 3, Network: 3, Decompress: 3, Write: 3}}
	base.PoolSize = base.MinPool()
	for _, db := range []*pgx.Conn{source, sink} {
		if _, err := db.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()+`;
 CREATE TABLE `+base.Relation()+` (id integer, s text)`); err != nil {
			t.Fatal(err)
		}
		defer func(db *pgx.Conn) {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			db.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		}(db)
	}
	if _, err := source.Exec(ctx, fmt.Sprintf(`INSERT INTO %s SELECT g, repeat('x', %d) FROM generate_series(1, %d) g`,
		base.Relation(), pad, rows)); err != nil {
		t.Fatal(err)
	}
	// Make sure the partitioning has something to split.
	var pages int64
	if err := source.QueryRow(ctx, `SELECT pg_relation_size($1::text::regclass) / current_setting('block_size')::bigint`,
		base.Relation()).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if pages < int64(2*base.Workers.Read) {
		t.Fatalf("source has only %d pages; too few to exercise %d readers", pages, base.Workers.Read)
	}

	for _, compression := range []string{"none", "snappy"} {
		t.Run(compression, func(t *testing.T) {
			c := base
			c.Compression = compression
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			served := make(chan error, 1)
			go func() {
				conns, err := AcceptConns(listener, c.Stages().Network, 10*time.Second)
				if err != nil {
					served <- err
					return
				}
				served <- ServeTransfer(ctx, conns, sourceURL, c)
			}()
			result, err := ReceiveTransfer(ctx, listener.Addr().String(), targetURL, c)
			if err != nil {
				t.Fatal(err)
			}
			if err := <-served; err != nil {
				t.Fatal(err)
			}
			if result.Rows != rows {
				t.Fatalf("sink reported %d rows, want %d", result.Rows, rows)
			}
			var distinct, sum, damaged int64
			err = sink.QueryRow(ctx, fmt.Sprintf(
				`SELECT count(DISTINCT id), sum(id), count(*) FILTER (WHERE s <> repeat('x', %d)) FROM %s`, pad, c.Relation()),
			).Scan(&distinct, &sum, &damaged)
			if err != nil {
				t.Fatal(err)
			}
			if distinct != rows || sum != int64(rows)*(rows+1)/2 || damaged != 0 {
				t.Fatalf("sink has %d distinct ids, id sum %d, %d damaged rows; want %d, %d, 0",
					distinct, sum, damaged, rows, int64(rows)*(rows+1)/2)
			}
		})
	}
}
