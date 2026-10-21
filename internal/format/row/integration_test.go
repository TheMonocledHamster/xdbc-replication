package row_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/TheMonocledHamster/xdbc-replication/internal/compression"
	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
)

func ExampleLayout() {
	if err := exampleRows(); err != nil {
		panic(err)
	}
	// Output:
	// 7 Ada
	// 8 Lin
}

func exampleRows() error {
	layout, err := row.NewLayout([]row.Field{{Name: "id", Type: row.Int32, Size: 4}, {Name: "name", Type: row.String, Size: 8}})
	if err != nil {
		return err
	}
	p, err := pool.New(pool.Config{NumBuffers: 1, PayloadCap: 2*layout.RowSize() + 1})
	if err != nil {
		return err
	}
	id, err := p.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer p.Release(id)
	buffer, err := p.Buffer(id)
	if err != nil {
		return err
	}
	count, err := layout.RowsPerBuffer(p.PayloadCap())
	if err != nil {
		return err
	}
	payload := buffer[pool.HeaderSize : pool.HeaderSize+count*layout.RowSize()]
	for i, name := range []string{"Ada", "Lin"} {
		record, err := layout.RowAt(payload, i)
		if err != nil {
			return err
		}
		if err := layout.PutInt32(record, 0, int32(i+7)); err != nil {
			return err
		}
		if err := layout.PutString(record, 1, name); err != nil {
			return err
		}
	}
	header := pool.BufferHeader{RecordCount: uint32(count), PayloadLen: uint32(len(payload)), UncompressedLen: uint32(len(payload))}
	header.Encode(buffer)
	// An uncompressed consumer uses only PayloadLen bytes, excluding spare capacity.
	header = pool.DecodeHeader(buffer)
	decoded := buffer[pool.HeaderSize : pool.HeaderSize+int(header.PayloadLen)]
	if len(decoded) != int(header.UncompressedLen) {
		return fmt.Errorf("decoded length mismatch")
	}
	if err := layout.ValidatePayload(decoded, header.RecordCount); err != nil {
		return err
	}
	for i := 0; i < int(header.RecordCount); i++ {
		record, err := layout.RowAt(decoded, i)
		if err != nil {
			return err
		}
		value, err := layout.GetInt32(record, 0)
		if err != nil {
			return err
		}
		name, err := layout.GetString(record, 1)
		if err != nil {
			return err
		}
		fmt.Println(value, name)
	}
	return nil
}

func TestPoolCompressionRoundTrip(t *testing.T) {
	layout, err := row.NewLayout([]row.Field{
		{Name: "id", Type: row.Int32, Size: 4}, {Name: "name", Type: row.String, Size: 8},
		{Name: "big", Type: row.Int64, Size: 8}, {Name: "decimal", Type: row.Numeric, Size: 48},
		{Name: "day", Type: row.Date, Size: 4}, {Name: "ts", Type: row.Timestamp, Size: 8},
		{Name: "tz", Type: row.TimestampTZ, Size: 8}, {Name: "bool", Type: row.Bool, Size: 1},
		{Name: "real", Type: row.Real, Size: 4}, {Name: "small", Type: row.Int16, Size: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, codec := range []compression.Codec{compression.NoneCodec{}, compression.SnappyCodec{}} {
		t.Run(fmt.Sprint(codec.ID()), func(t *testing.T) {
			const count = 3
			used := count * layout.RowSize()
			capacity := codec.MaxEncodedLen(used) + 5
			p, err := pool.New(pool.Config{NumBuffers: 3, PayloadCap: capacity})
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]int, 3)
			for i := range ids {
				ids[i], err = p.Acquire(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer p.Release(ids[i])
			}
			source, err := p.Buffer(ids[0])
			if err != nil {
				t.Fatal(err)
			}
			payload := source[pool.HeaderSize : pool.HeaderSize+used]
			for i := 0; i < count; i++ {
				record, err := layout.RowAt(payload, i)
				if err != nil {
					t.Fatal(err)
				}
				if err := layout.PutInt32(record, 0, int32(i-1)); err != nil {
					t.Fatal(err)
				}
				if err := layout.PutString(record, 1, fmt.Sprintf("row%d", i)); err != nil {
					t.Fatal(err)
				}
				writes := []func() error{
					func() error { return layout.PutInt64(record, 2, math.MaxInt64) },
					func() error { return layout.PutNumeric(record, 3, "9007199254740993.000100") },
					func() error { return layout.PutDate(record, 4, -1) },
					func() error { return layout.PutTimestamp(record, 5, -100001) },
					func() error { return layout.PutTimestampTZ(record, 6, row.TimestampInfinity) },
					func() error { return layout.PutBool(record, 7, false) },
					func() error { return layout.PutReal(record, 8, math.Float32frombits(0x7fc00042)) },
					func() error { return layout.PutInt16(record, 9, math.MinInt16) },
				}
				for j, write := range writes {
					var err error
					if i == 1 {
						err = layout.PutNull(record, j+2)
					} else {
						err = write()
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			original := pool.BufferHeader{Seq: 9, NextRecord: 12, RecordCount: count, PayloadLen: uint32(used), UncompressedLen: uint32(used)}
			original.Encode(source)
			encoded, err := p.Buffer(ids[1])
			if err != nil {
				t.Fatal(err)
			}
			n, err := codec.Compress(encoded[pool.HeaderSize:], payload)
			if err != nil {
				t.Fatal(err)
			}
			header := pool.DecodeHeader(source)
			header.PayloadLen = uint32(n)
			header.Compression = uint8(codec.ID())
			header.Encode(encoded)
			wireHeader := pool.DecodeHeader(encoded)
			if wireHeader.RecordCount != count || wireHeader.UncompressedLen != uint32(used) || wireHeader.PayloadLen != uint32(n) || wireHeader.Compression != uint8(codec.ID()) || wireHeader.Seq != original.Seq || wireHeader.NextRecord != original.NextRecord {
				t.Fatalf("metadata: %+v", wireHeader)
			}
			decodedBuffer, err := p.Buffer(ids[2])
			if err != nil {
				t.Fatal(err)
			}
			decoder, err := compression.ByID(compression.Compression(wireHeader.Compression))
			if err != nil {
				t.Fatal(err)
			}
			decodedLen, err := decoder.Decompress(decodedBuffer[pool.HeaderSize:], encoded[pool.HeaderSize:pool.HeaderSize+int(wireHeader.PayloadLen)])
			if err != nil {
				t.Fatal(err)
			}
			if uint64(decodedLen) != uint64(wireHeader.UncompressedLen) {
				t.Fatal("decoded length mismatch")
			}
			decoded := decodedBuffer[pool.HeaderSize : pool.HeaderSize+decodedLen]
			if !bytes.Equal(decoded, payload) {
				t.Fatal("compression changed nullable scalar bytes")
			}
			if err := layout.ValidatePayload(decoded, wireHeader.RecordCount); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i++ {
				record, err := layout.RowAt(decoded, i)
				if err != nil {
					t.Fatal(err)
				}
				value, err := layout.GetInt32(record, 0)
				if err != nil || value != int32(i-1) {
					t.Fatal(value, err)
				}
				name, err := layout.GetString(record, 1)
				if err != nil || name != fmt.Sprintf("row%d", i) {
					t.Fatal(name, err)
				}
			}
		})
	}
}
