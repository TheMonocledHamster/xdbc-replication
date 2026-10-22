package transfer

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/jackc/pgx/v5"
)

// This test uses the same source, TCP, compression, and sink runtime as the
// persistent commands. SQL text and floating-point wire bytes verify the result
// independently of the row codec, including numeric scale and signed zero.
func TestScalarRoundTrip(t *testing.T) {
	sourceURL, targetURL := os.Getenv("XDBC_TEST_SOURCE_DATABASE_URL"), os.Getenv("XDBC_TEST_TARGET_DATABASE_URL")
	if sourceURL == "" || targetURL == "" {
		t.Skip("set XDBC_TEST_SOURCE_DATABASE_URL and XDBC_TEST_TARGET_DATABASE_URL for two-database round trips")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	schema := fmt.Sprintf("scalar_it_%d", time.Now().UnixNano())
	fields := []row.Field{
		{Name: "id", Type: row.Int32, Size: 4}, {Name: "small", Type: row.Int16, Size: 2},
		{Name: "big", Type: row.Int64, Size: 8}, {Name: "n", Type: row.Numeric, Size: 64},
		{Name: "d", Type: row.Date, Size: 4}, {Name: "ts", Type: row.Timestamp, Size: 8},
		{Name: "tz", Type: row.TimestampTZ, Size: 8}, {Name: "b", Type: row.Bool, Size: 1},
		{Name: "r", Type: row.Real, Size: 4}, {Name: "f", Type: row.Double, Size: 8},
		{Name: "s", Type: row.String, Size: 32}, {Name: "c", Type: row.Char, Size: 1},
	}
	layout, err := row.NewLayout(fields)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Schema: schema, Table: "values", Fields: fields, BufferSize: 2 * layout.RowSize(), PoolSize: 8, QueueCapacity: 2}
	for _, db := range []*pgx.Conn{source, sink} {
		_, err := db.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()+`; CREATE TABLE `+config.Relation()+` (
   id integer, small smallint, big bigint, n numeric, d date, ts timestamp, tz timestamptz,
   b boolean, r real, f double precision, s text, c char(1))`)
		if err != nil {
			t.Fatal(err)
		}
		defer func(db *pgx.Conn) {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			db.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		}(db)
	}
	_, err = source.Exec(ctx, `INSERT INTO `+config.Relation()+` VALUES
  (1,-32768,9223372036854775807,123456789012345678901234567890.000100,'2000-01-01','2000-01-01','2000-01-01 00:00:00+02',false,'-0',1.5,'','A'),
  (2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
  (3,0,0,0.00,'1999-12-31','1999-12-31 23:59:59.999999','1999-12-31 23:59:59.999999+00',true,'NaN','Infinity','again','Z'),
  (4,32767,-9223372036854775808,'NaN','infinity','infinity','infinity',true,'Infinity','-Infinity','finite','B'),
  (5,-1,-1,'-Infinity','-infinity','-infinity','-infinity',false,1.25,'-0',E'tab\tline\nslash\\','C'),
  (6,1,1,'Infinity','600000-01-01','10000-01-01 01:02:03.123456','10000-01-01 01:02:03.123456+02',true,'-Infinity','NaN','é','D'),
  (7,0,0,-0.000,'0002-01-01 BC','0002-01-01 01:02:03.123456 BC','0002-01-01 01:02:03.123456+00 BC',false,0,0,'zero','E'),
  (0,0,0,0.00,'2000-01-01','2000-01-01','2000-01-01+00',false,0,0,'',' '),
  (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
  INSERT INTO `+config.Relation()+` SELECT * FROM `+config.Relation()+` WHERE id=1`)
	if err != nil {
		t.Fatal(err)
	}
	read := func(t *testing.T, db *pgx.Conn) [][]any {
		t.Helper()
		if _, err := db.Exec(ctx, "SET TIME ZONE 'UTC'"); err != nil {
			t.Fatal(err)
		}
		rows, err := db.Query(ctx, `SELECT row_to_json(v)::text, encode(float4send(r),'hex'), encode(float8send(f),'hex') FROM `+config.Relation()+` v ORDER BY id NULLS LAST`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var result [][]any
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				t.Fatal(err)
			}
			result = append(result, values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	want := read(t, source)
	withParams := func(raw string, params map[string]string) string {
		t.Helper()
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
		return u.String()
	}
	for _, mode := range []string{"cache_statement", "simple_protocol"} {
		for _, compression := range []string{"none", "snappy"} {
			t.Run(mode+"/"+compression, func(t *testing.T) {
				c := config
				c.Compression = compression
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				results := make(chan error, 2)
				sessionSourceURL := withParams(sourceURL, map[string]string{"default_query_exec_mode": mode, "timezone": "America/New_York"})
				sessionTargetURL := withParams(targetURL, map[string]string{"timezone": "Asia/Kolkata"})
				go func() {
					for i := 0; i < 2; i++ {
						conn, err := listener.Accept()
						if err != nil {
							results <- err
							return
						}
						results <- ServeTransfer(ctx, conn, sessionSourceURL, c)
					}
				}()
				for i := 0; i < 2; i++ {
					result, err := ReceiveTransfer(ctx, listener.Addr().String(), sessionTargetURL, c)
					if err != nil {
						t.Fatal(err)
					}
					if err := <-results; err != nil {
						t.Fatal(err)
					}
					if result.Rows != int64(len(want)) {
						t.Fatalf("row count: %d, want %d", result.Rows, len(want))
					}
					got := read(t, sink)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("round trip changed values:\ngot: %v\nwant: %v", got, want)
					}

				}
			})
		}
	}
}
