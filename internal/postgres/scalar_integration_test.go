package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
	"github.com/jackc/pgx/v5"
)

func scalarDatabase(t *testing.T, mode pgx.QueryExecMode) (context.Context, *pgx.Conn) {
	t.Helper()
	url := os.Getenv("XDBC_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("Set XDBC_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = mode
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return ctx, conn
}

func scalarRows(t *testing.T, ctx context.Context, conn *pgx.Conn, query string, layout *row.Layout) ([][]byte, []uint32) {
	t.Helper()
	p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: 2 * layout.RowSize()})
	if err != nil {
		t.Fatal(err)
	}
	q, err := queue.New(1)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- Read(ctx, conn, query, layout, p, q); close(q) }()
	var records [][]byte
	var counts []uint32
	for {
		id, err := q.Pop(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := p.Buffer(id)
		if err != nil {
			t.Fatal(err)
		}
		h := pool.DecodeHeader(b)
		if h.Seq != uint64(len(counts)) || h.NextRecord != uint64(len(records))+uint64(h.RecordCount) || h.PayloadLen != h.UncompressedLen {
			t.Fatalf("bad buffer header: %+v", h)
		}
		payload := b[pool.HeaderSize : pool.HeaderSize+int(h.PayloadLen)]
		if err := layout.ValidatePayload(payload, h.RecordCount); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(h.RecordCount); i++ {
			record, err := layout.RowAt(payload, i)
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, append([]byte(nil), record...))
		}
		counts = append(counts, h.RecordCount)
		if err := p.Release(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	id, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal("reader leaked buffer", err)
	}
	if err := p.Release(id); err != nil {
		t.Fatal(err)
	}
	return records, counts
}

func decodeScalar(l *row.Layout, b []byte, col int, dtype row.Dtype) (any, error) {
	switch dtype {
	case row.Int16:
		return l.GetInt16(b, col)
	case row.Int32:
		return l.GetInt32(b, col)
	case row.Int64:
		return l.GetInt64(b, col)
	case row.Bool:
		return l.GetBool(b, col)
	case row.Real:
		return l.GetReal(b, col)
	case row.Double:
		return l.GetDouble(b, col)
	case row.String:
		return l.GetString(b, col)
	case row.Char:
		return l.GetChar(b, col)
	case row.Numeric:
		return l.GetNumeric(b, col)
	case row.Date:
		return l.GetDate(b, col)
	case row.Timestamp:
		return l.GetTimestamp(b, col)
	case row.TimestampTZ:
		return l.GetTimestampTZ(b, col)
	}
	return nil, fmt.Errorf("unsupported test type %s", dtype)
}

func TestCoreScalarReader(t *testing.T) {
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeSimpleProtocol} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, conn := scalarDatabase(t, mode)
			if _, err := conn.Exec(ctx, `SET TIME ZONE 'America/New_York';
    CREATE TEMP TABLE scalar_rows (id integer, small smallint, big bigint, n numeric, d date,
      ts timestamp, tz timestamptz, b boolean, r real, f double precision, s text, c char(1));
    INSERT INTO scalar_rows VALUES
    (1,-32768,9223372036854775807,123456789012345678901234567890.000100,'2000-01-01','2000-01-01','2000-01-01 00:00:00+02',false,'-0',1.5,'','A'),
    (2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
    (3,0,0,0.00,'1999-12-31','1999-12-31 23:59:59.999999','1999-12-31 23:59:59.999999+00',true,'NaN','Infinity','again','Z'),
    (4,32767,-9223372036854775808,'NaN','infinity','infinity','infinity',true,'Infinity','-Infinity','finite','B'),
    (5,-1,-1,'-Infinity','-infinity','-infinity','-infinity',false,1.25,'-0','tail','C')`); err != nil {
				t.Fatal(err)
			}
			fields := []row.Field{{Name: "id", Type: row.Int32, Size: 4}, {Name: "small", Type: row.Int16, Size: 2}, {Name: "big", Type: row.Int64, Size: 8}, {Name: "n", Type: row.Numeric, Size: 64},
				{Name: "d", Type: row.Date, Size: 4}, {Name: "ts", Type: row.Timestamp, Size: 8}, {Name: "tz", Type: row.TimestampTZ, Size: 8}, {Name: "b", Type: row.Bool, Size: 1},
				{Name: "r", Type: row.Real, Size: 4}, {Name: "f", Type: row.Double, Size: 8}, {Name: "s", Type: row.String, Size: 8}, {Name: "c", Type: row.Char, Size: 1}}
			layout, err := row.NewLayout(fields)
			if err != nil {
				t.Fatal(err)
			}
			records, counts := scalarRows(t, ctx, conn, "SELECT * FROM scalar_rows ORDER BY id", layout)
			if !reflect.DeepEqual(counts, []uint32{2, 2, 1}) {
				t.Fatalf("buffers: %v", counts)
			}
			wants := [][]any{
				{int32(1), int16(-32768), int64(math.MaxInt64), "123456789012345678901234567890.000100", int32(0), int64(0), int64(-7200000000), false, math.Float32frombits(1 << 31), float64(1.5), "", byte('A')},
				{int32(2), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
				{int32(3), int16(0), int64(0), "0.00", int32(-1), int64(-1), int64(-1), true, float32(math.NaN()), math.Inf(1), "again", byte('Z')},
				{int32(4), int16(32767), int64(math.MinInt64), "NaN", row.DateInfinity, row.TimestampInfinity, row.TimestampInfinity, true, float32(math.Inf(1)), math.Inf(-1), "finite", byte('B')},
				{int32(5), int16(-1), int64(-1), "-Infinity", row.DateNegativeInfinity, row.TimestampNegativeInfinity, row.TimestampNegativeInfinity, false, float32(1.25), math.Copysign(0, -1), "tail", byte('C')},
			}
			if len(records) != len(wants) {
				t.Fatalf("rows: %d", len(records))
			}
			for i, b := range records {
				for col, field := range fields {
					null, err := layout.IsNull(b, col)
					if err != nil {
						t.Fatal(err)
					}
					want := wants[i][col]
					got, err := decodeScalar(layout, b, col, field.Type)
					if want == nil {
						if !null || !errors.Is(err, row.ErrNull) {
							t.Fatalf("row %d col %s NULL: %v %v", i, field.Name, got, err)
						}
						continue
					}
					if null || err != nil {
						t.Fatalf("row %d col %s: %v", i, field.Name, err)
					}
					equal := reflect.DeepEqual(got, want)
					switch v := want.(type) {
					case float32:
						g := got.(float32)
						equal = (math.IsNaN(float64(v)) && math.IsNaN(float64(g))) || math.Float32bits(v) == math.Float32bits(g)
					case float64:
						equal = math.Float64bits(v) == math.Float64bits(got.(float64))
					}
					if !equal {
						t.Fatalf("row %d col %s: %#v want %#v", i, field.Name, got, want)
					}
				}
			}
		})
	}
}

func TestReaderWideTemporalValues(t *testing.T) {
	ctx, conn := scalarDatabase(t, pgx.QueryExecModeCacheStatement)
	layout, err := row.NewLayout([]row.Field{{Name: "d", Type: row.Date, Size: 4}, {Name: "ts", Type: row.Timestamp, Size: 8}, {Name: "tz", Type: row.TimestampTZ, Size: 8}})
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := scalarRows(t, ctx, conn, `SELECT DATE '600000-01-01', TIMESTAMP '10000-01-01 01:02:03.123456', TIMESTAMPTZ '10000-01-01 01:02:03.123456+02'`, layout)
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	wantDate := (time.Date(600000, 1, 1, 0, 0, 0, 0, time.UTC).Unix() - epoch) / 86400
	wantTS := (time.Date(10000, 1, 1, 1, 2, 3, 0, time.UTC).Unix()-epoch)*1000000 + 123456
	if got, err := layout.GetDate(rows[0], 0); err != nil || int64(got) != wantDate {
		t.Fatal(got, wantDate, err)
	}
	if got, err := layout.GetTimestamp(rows[0], 1); err != nil || got != wantTS {
		t.Fatal(got, wantTS, err)
	}
	if got, err := layout.GetTimestampTZ(rows[0], 2); err != nil || got != wantTS-7200000000 {
		t.Fatal(got, wantTS, err)
	}
}

func TestReaderRejectsInvalidScalars(t *testing.T) {
	ctx, conn := scalarDatabase(t, pgx.QueryExecModeCacheStatement)
	for _, tc := range []struct {
		query   string
		field   row.Field
		message string
	}{
		{"SELECT 1::integer WHERE false", row.Field{Name: "v", Type: row.Int64, Size: 8}, "incompatible"},
		{"SELECT NULL::uuid WHERE false", row.Field{Name: "v", Type: row.String, Size: 40}, "incompatible"},
		{"SELECT 1::bigint,2::bigint WHERE false", row.Field{Name: "v", Type: row.Int64, Size: 8}, "columns"},
		{"SELECT 123456.00::numeric", row.Field{Name: "v", Type: row.Numeric, Size: 3}, "column 0 (v)"},
		{"SELECT 'é'::text", row.Field{Name: "v", Type: row.Char, Size: 1}, "one byte"},
	} {
		layout, err := row.NewLayout([]row.Field{tc.field})
		if err != nil {
			t.Fatal(err)
		}
		p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: layout.RowSize()})
		if err != nil {
			t.Fatal(err)
		}
		q, err := queue.New(1)
		if err != nil {
			t.Fatal(err)
		}
		err = Read(ctx, conn, tc.query, layout, p, q)
		if err == nil || !strings.Contains(err.Error(), tc.message) {
			t.Fatalf("%s: %v", tc.query, err)
		}
		if len(q) != 0 {
			t.Fatal("reader forwarded an invalid row")
		}
		checkCtx, cancel := context.WithTimeout(ctx, time.Second)
		id, err := p.Acquire(checkCtx)
		cancel()
		if err != nil {
			t.Fatal("buffer leaked", err)
		}
		if err := p.Release(id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReaderCancellationPreservesOwnership(t *testing.T) {
	ctx, conn := scalarDatabase(t, pgx.QueryExecModeCacheStatement)
	layout, err := row.NewLayout([]row.Field{{Name: "v", Type: row.Int32, Size: 4}})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"acquire", "push"} {
		t.Run(mode, func(t *testing.T) {
			p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: layout.RowSize()})
			if err != nil {
				t.Fatal(err)
			}
			q, err := queue.New(1)
			if err != nil {
				t.Fatal(err)
			}
			held := -1
			if mode == "acquire" {
				held, err = p.Acquire(ctx)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				q <- -1
			}
			short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			err = Read(short, conn, "SELECT 1::integer", layout, p, q)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("cancel %s: %v", mode, err)
			}
			if mode == "acquire" {
				if _, err := p.Buffer(held); err != nil {
					t.Fatalf("reader released a buffer owned by its consumer: %v", err)
				}
				if err := p.Release(held); err != nil {
					t.Fatal(err)
				}
			} else if id := <-q; id != -1 {
				t.Fatal("reader replaced a full queue entry")
			}
			id, err := p.Acquire(ctx)
			if err != nil {
				t.Fatal("reader leaked owned buffer", err)
			}
			if err := p.Release(id); err != nil {
				t.Fatal(err)
			}
		})
	}
}
