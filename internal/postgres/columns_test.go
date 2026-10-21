package postgres

import (
	"bytes"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestColumnSchemaValidation(t *testing.T) {
	layout, err := row.NewLayout([]row.Field{{Name: "id", Type: row.Int64, Size: 8}})
	if err != nil {
		t.Fatal(err)
	}
	for _, descs := range [][]pgconn.FieldDescription{
		nil, {{DataTypeOID: pgtype.Int4OID}}, {{DataTypeOID: pgtype.UUIDOID}},
		{{DataTypeOID: pgtype.Int8OID}, {DataTypeOID: pgtype.Int8OID}},
	} {
		if _, _, err := prepareColumns(layout, descs); err == nil {
			t.Fatalf("accepted incompatible columns: %+v", descs)
		}
	}
	fields := layout.Fields()
	fields[0].Type = row.String
	if layout.Fields()[0].Type != row.Int64 {
		t.Fatal("layout schema mutated")
	}
}

func TestTemporalAdapters(t *testing.T) {
	// A fixed offset distinguishes wall-clock TIMESTAMP from UTC TIMESTAMPTZ.
	zone := time.FixedZone("plus02", 7200)
	tests := []struct {
		dtype row.Dtype
		size  int
		oid   uint32
		value any
		want  int64
	}{
		{row.Date, 4, pgtype.DateOID, pgtype.Date{Time: time.Date(2000, 1, 1, 0, 0, 0, 0, zone), Valid: true}, 0},
		{row.Date, 4, pgtype.DateOID, pgtype.Date{Time: time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true}, -1},
		{row.Date, 4, pgtype.DateOID, pgtype.Date{Time: time.Date(2000, 2, 29, 0, 0, 0, 0, time.UTC), Valid: true}, 59},
		{row.Date, 4, pgtype.DateOID, pgtype.Date{InfinityModifier: pgtype.Infinity, Valid: true}, int64(row.DateInfinity)},
		{row.Date, 4, pgtype.DateOID, pgtype.Date{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, int64(row.DateNegativeInfinity)},
		{row.Timestamp, 8, pgtype.TimestampOID, pgtype.Timestamp{Time: time.Date(2000, 1, 1, 0, 0, 0, 123456000, zone), Valid: true}, 123456},
		{row.TimestampTZ, 8, pgtype.TimestamptzOID, pgtype.Timestamptz{Time: time.Date(2000, 1, 1, 0, 0, 0, 123456000, zone), Valid: true}, -7200000000 + 123456},
		{row.Timestamp, 8, pgtype.TimestampOID, pgtype.Timestamp{Time: time.Date(1999, 12, 31, 23, 59, 59, 999999000, time.UTC), Valid: true}, -1},
		{row.Timestamp, 8, pgtype.TimestampOID, pgtype.Timestamp{InfinityModifier: pgtype.Infinity, Valid: true}, row.TimestampInfinity},
		{row.TimestampTZ, 8, pgtype.TimestamptzOID, pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, row.TimestampNegativeInfinity},
	}
	for _, tc := range tests {
		layout, err := row.NewLayout([]row.Field{{Name: "v", Type: tc.dtype, Size: tc.size}})
		if err != nil {
			t.Fatal(err)
		}
		adapters, _, err := prepareColumns(layout, []pgconn.FieldDescription{{DataTypeOID: tc.oid}})
		if err != nil {
			t.Fatal(err)
		}
		switch dst := adapters[0].destination.(type) {
		case *pgtype.Date:
			*dst = tc.value.(pgtype.Date)
		case *pgtype.Timestamp:
			*dst = tc.value.(pgtype.Timestamp)
		case *pgtype.Timestamptz:
			*dst = tc.value.(pgtype.Timestamptz)
		}
		b := make([]byte, layout.RowSize())
		if err := adapters[0].encode(b); err != nil {
			t.Fatal(err)
		}
		var got int64
		switch tc.dtype {
		case row.Date:
			v, e := layout.GetDate(b, 0)
			got, err = int64(v), e
		case row.Timestamp:
			got, err = layout.GetTimestamp(b, 0)
		case row.TimestampTZ:
			got, err = layout.GetTimestampTZ(b, 0)
		}
		if err != nil || got != tc.want {
			t.Fatalf("%s %+v: %d, want %d (%v)", tc.dtype, tc.value, got, tc.want, err)
		}
	}
}

func TestNumericAndCharAdapters(t *testing.T) {
	layout, err := row.NewLayout([]row.Field{{Name: "n", Type: row.Numeric, Size: 64}, {Name: "c", Type: row.Char, Size: 1}})
	if err != nil {
		t.Fatal(err)
	}
	adapters, _, err := prepareColumns(layout, []pgconn.FieldDescription{{DataTypeOID: pgtype.NumericOID}, {DataTypeOID: pgtype.BPCharOID}})
	if err != nil {
		t.Fatal(err)
	}
	n := adapters[0].destination.(*pgtype.Numeric)
	c := adapters[1].destination.(*pgtype.Text)
	b := make([]byte, layout.RowSize())
	for _, text := range []string{"123456789012345678901234567890.000100", "0.00", "-0.0001", "NaN", "Infinity", "-Infinity"} {
		if err := n.Scan(text); err != nil {
			t.Fatal(err)
		}
		if err := adapters[0].encode(b); err != nil {
			t.Fatal(err)
		}
		if got, err := layout.GetNumeric(b, 0); err != nil || got != text {
			t.Fatalf("numeric: %q want %q (%v)", got, text, err)
		}
	}
	*n = pgtype.Numeric{}
	if err := adapters[0].encode(b); err != nil {
		t.Fatal(err)
	}
	if null, err := layout.IsNull(b, 0); err != nil || !null {
		t.Fatal("invalid Numeric did not encode NULL")
	}
	for _, value := range []string{"", "ab", "é"} {
		*c = pgtype.Text{String: value, Valid: true}
		before := bytes.Clone(b)
		if err := adapters[1].encode(b); err == nil || !bytes.Equal(b, before) {
			t.Fatalf("invalid single-byte CHAR: %q", value)
		}
	}
	*c = pgtype.Text{String: "Z", Valid: true}
	if err := adapters[1].encode(b); err != nil {
		t.Fatal(err)
	}
	if got, err := layout.GetChar(b, 1); err != nil || got != 'Z' {
		t.Fatal(got, err)
	}
}

func TestBinaryNumericZeroScale(t *testing.T) {
	layout, err := row.NewLayout([]row.Field{{Name: "n", Type: row.Numeric, Size: 8}})
	if err != nil {
		t.Fatal(err)
	}
	adapters, dest, err := prepareColumns(layout, []pgconn.FieldDescription{{DataTypeOID: pgtype.NumericOID, Format: pgtype.BinaryFormatCode}})
	if err != nil {
		t.Fatal(err)
	}
	// ndigits=0, weight=0, positive sign, dscale=2: PostgreSQL's 0.00.
	raw := []byte{0, 0, 0, 0, 0, 0, 0, 2}
	if err := pgtype.NewMap().Scan(pgtype.NumericOID, pgtype.BinaryFormatCode, raw, dest[0]); err != nil {
		t.Fatal(err)
	}
	adapters[0].postScan(raw)
	b := make([]byte, layout.RowSize())
	if err := adapters[0].encode(b); err != nil {
		t.Fatal(err)
	}
	if got, err := layout.GetNumeric(b, 0); err != nil || got != "0.00" {
		t.Fatalf("binary numeric scale: %q %v", got, err)
	}
}
