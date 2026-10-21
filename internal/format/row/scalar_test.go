package row

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestExtendedScalarWireBytes(t *testing.T) {
	l := mustLayout(t, []Field{{"s", Int16, 2}, {"b", Int64, 8}, {"d", Date, 4}, {"ts", Timestamp, 8}, {"bool", Bool, 1}, {"r", Real, 4}})
	b := make([]byte, l.RowSize())
	for _, err := range []error{l.PutInt16(b, 0, -2), l.PutInt64(b, 1, 0x0102030405060708), l.PutDate(b, 2, -1), l.PutTimestamp(b, 3, 1), l.PutBool(b, 4, true), l.PutReal(b, 5, 1.5)} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := hex.EncodeToString(b); got != "00feff0807060504030201ffffffff0100000000000000010000c03f" {
		t.Fatal(got)
	}
}

func TestRejectedWritesKeepNull(t *testing.T) {
	l := mustLayout(t, []Field{{"s", String, 2}, {"n", Numeric, 4}})
	b := make([]byte, l.RowSize())
	for col := range l.fields {
		if err := l.PutNull(b, col); err != nil {
			t.Fatal(err)
		}
	}
	before := bytes.Clone(b)
	for _, write := range []func() error{
		func() error { return l.PutString(b, 0, "long") },
		func() error { return l.PutString(b, 0, "\x00") },
		func() error { return l.PutNumeric(b, 1, "12345") },
		func() error { return l.PutNumeric(b, 1, "bad") },
		func() error { return l.PutInt64(b, 0, 1) },
		func() error { return l.PutString(b[:len(b)-1], 0, "x") },
	} {
		if err := write(); err == nil || !bytes.Equal(b, before) {
			t.Fatalf("invalid write modified a NULL row: %v", err)
		}
	}
}

func mustLayout(t *testing.T, fields []Field) *Layout {
	t.Helper()
	l, err := NewLayout(fields)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestNullableScalars(t *testing.T) {
	// Twelve columns exercise both bitmap bytes and all scalar accessors.
	tests := []struct {
		dtype Dtype
		size  int
		put   func(*Layout, []byte, int) error
		get   func(*Layout, []byte, int) error
	}{
		{Int16, 2, func(l *Layout, b []byte, c int) error { return l.PutInt16(b, c, 0) }, func(l *Layout, b []byte, c int) error { _, e := l.GetInt16(b, c); return e }},
		{Int32, 4, func(l *Layout, b []byte, c int) error { return l.PutInt32(b, c, 0) }, func(l *Layout, b []byte, c int) error { _, e := l.GetInt32(b, c); return e }},
		{Int64, 8, func(l *Layout, b []byte, c int) error { return l.PutInt64(b, c, 0) }, func(l *Layout, b []byte, c int) error { _, e := l.GetInt64(b, c); return e }},
		{Bool, 1, func(l *Layout, b []byte, c int) error { return l.PutBool(b, c, false) }, func(l *Layout, b []byte, c int) error { _, e := l.GetBool(b, c); return e }},
		{Real, 4, func(l *Layout, b []byte, c int) error { return l.PutReal(b, c, float32(math.NaN())) }, func(l *Layout, b []byte, c int) error { _, e := l.GetReal(b, c); return e }},
		{Double, 8, func(l *Layout, b []byte, c int) error { return l.PutDouble(b, c, math.NaN()) }, func(l *Layout, b []byte, c int) error { _, e := l.GetDouble(b, c); return e }},
		{Char, 1, func(l *Layout, b []byte, c int) error { return l.PutChar(b, c, 0) }, func(l *Layout, b []byte, c int) error { _, e := l.GetChar(b, c); return e }},
		{String, 4, func(l *Layout, b []byte, c int) error { return l.PutString(b, c, "") }, func(l *Layout, b []byte, c int) error { _, e := l.GetStringBytes(b, c); return e }},
		{Numeric, 8, func(l *Layout, b []byte, c int) error { return l.PutNumeric(b, c, "0.00") }, func(l *Layout, b []byte, c int) error { _, e := l.GetNumeric(b, c); return e }},
		{Date, 4, func(l *Layout, b []byte, c int) error { return l.PutDate(b, c, 0) }, func(l *Layout, b []byte, c int) error { _, e := l.GetDate(b, c); return e }},
		{Timestamp, 8, func(l *Layout, b []byte, c int) error { return l.PutTimestamp(b, c, 0) }, func(l *Layout, b []byte, c int) error { _, e := l.GetTimestamp(b, c); return e }},
		{TimestampTZ, 8, func(l *Layout, b []byte, c int) error { return l.PutTimestampTZ(b, c, 0) }, func(l *Layout, b []byte, c int) error { _, e := l.GetTimestampTZ(b, c); return e }},
	}
	fields := make([]Field, len(tests))
	for i, tc := range tests {
		fields[i] = Field{fmt.Sprint(i), tc.dtype, tc.size}
	}
	l := mustLayout(t, fields)
	b := make([]byte, l.RowSize())
	for i, tc := range tests {
		// NULL must scrub stale bytes, even across the bitmap byte boundary.
		slot, _ := l.columnSlot(b, i)
		for j := range slot {
			slot[j] = 0xff
		}
		if err := l.PutNull(b, i); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(slot, make([]byte, len(slot))) {
			t.Fatal("NULL did not clear slot")
		}
		if err := tc.get(l, b, i); !errors.Is(err, ErrNull) {
			t.Fatalf("%s NULL getter: %v", tc.dtype, err)
		}
	}
	if b[0] != 0xff || b[1] != 0x0f {
		t.Fatalf("bitmap: %x", b[:2])
	}
	for i, tc := range tests {
		if err := tc.put(l, b, i); err != nil {
			t.Fatal(err)
		}
		if null, err := l.IsNull(b, i); err != nil || null {
			t.Fatalf("%s stayed NULL: %v", tc.dtype, err)
		}
		if err := tc.get(l, b, i); err != nil {
			t.Fatal(err)
		}
	}
	if b[0] != 0 || b[1] != 0 {
		t.Fatal("writes did not clear bitmap")
	}
	// Setting another column must not change its neighbors' validity.
	if err := l.PutNull(b, 8); err != nil {
		t.Fatal(err)
	}
	if b[0] != 0 || b[1] != 1 {
		t.Fatal("NULL bit leaked to a neighbor")
	}
	for _, col := range []int{-1, len(fields)} {
		before := bytes.Clone(b)
		if err := l.PutNull(b, col); err == nil {
			t.Fatal("invalid NULL write")
		}
		if _, err := l.IsNull(b, col); err == nil {
			t.Fatal("invalid NULL read")
		}
		if !bytes.Equal(before, b) {
			t.Fatal("invalid write changed bitmap")
		}
	}
	if err := l.PutNull(b[:len(b)-1], 0); err == nil {
		t.Fatal("short row accepted")
	}
	if _, err := l.IsNull(b[:len(b)-1], 0); err == nil {
		t.Fatal("short row accepted")
	}
}

func TestExtendedScalarLimits(t *testing.T) {
	l := mustLayout(t, []Field{{"small", Int16, 2}, {"large", Int64, 8}, {"real", Real, 4}, {"bool", Bool, 1}, {"date", Date, 4}, {"ts", Timestamp, 8}, {"tz", TimestampTZ, 8}})
	b := make([]byte, l.RowSize())
	for _, v := range []int16{math.MinInt16, -1, 0, math.MaxInt16} {
		if err := l.PutInt16(b, 0, v); err != nil {
			t.Fatal(err)
		}
		if got, err := l.GetInt16(b, 0); err != nil || got != v {
			t.Fatalf("int16: %d %v", got, err)
		}
	}
	for _, v := range []int64{math.MinInt64, -1, 0, math.MaxInt64} {
		if err := l.PutInt64(b, 1, v); err != nil {
			t.Fatal(err)
		}
		if got, err := l.GetInt64(b, 1); err != nil || got != v {
			t.Fatalf("int64: %d %v", got, err)
		}
		if err := l.PutTimestamp(b, 5, v); err != nil {
			t.Fatal(err)
		}
		if got, err := l.GetTimestamp(b, 5); err != nil || got != v {
			t.Fatalf("timestamp: %d %v", got, err)
		}
		if err := l.PutTimestampTZ(b, 6, v); err != nil {
			t.Fatal(err)
		}
		if got, err := l.GetTimestampTZ(b, 6); err != nil || got != v {
			t.Fatalf("timestamptz: %d %v", got, err)
		}
	}
	for _, v := range []int32{DateNegativeInfinity, -1, 0, 60, DateInfinity} {
		if err := l.PutDate(b, 4, v); err != nil {
			t.Fatal(err)
		}
		if got, err := l.GetDate(b, 4); err != nil || got != v {
			t.Fatalf("date: %d %v", got, err)
		}
	}
	for _, bits := range []uint32{0, 1, 1 << 31, 0x7f800000, 0xff800000, 0x7fc00042, math.Float32bits(math.MaxFloat32)} {
		if err := l.PutReal(b, 2, math.Float32frombits(bits)); err != nil {
			t.Fatal(err)
		}
		got, err := l.GetReal(b, 2)
		if err != nil || math.Float32bits(got) != bits {
			t.Fatalf("real bits: %x %v", math.Float32bits(got), err)
		}
	}
	for _, v := range []bool{true, false} {
		if err := l.PutBool(b, 3, v); err != nil {
			t.Fatal(err)
		}
		if got, err := l.GetBool(b, 3); err != nil || got != v {
			t.Fatalf("bool: %v %v", got, err)
		}
	}
	b[l.offsets[3]] = 2
	if _, err := l.GetBool(b, 3); err == nil {
		t.Fatal("malformed boolean accepted")
	}
	for _, dtype := range []Dtype{Int16, Int64, Bool, Real, Date, Timestamp, TimestampTZ} {
		if _, err := NewLayout([]Field{{"bad", dtype, 3}}); err == nil {
			t.Fatalf("invalid %s width accepted", dtype)
		}
	}
	fields := l.Fields()
	fields[0].Name = "modified"
	if l.Fields()[0].Name != "small" {
		t.Fatal("Fields aliases schema")
	}
}

func TestNumericExactText(t *testing.T) {
	l := mustLayout(t, []Field{{"decimal", Numeric, 64}})
	b := make([]byte, l.RowSize())
	for _, v := range []string{"123456789012345678901234567890.000100", "-0.0000", "0.00", "1e-30", ".001", "+42", "NaN", "Infinity", "-Infinity", "+Infinity"} {
		if err := l.PutNumeric(b, 0, v); err != nil {
			t.Fatal(err)
		}
		if got, err := l.GetNumeric(b, 0); err != nil || got != v {
			t.Fatalf("numeric: %q %v", got, err)
		}
	}
	for _, v := range []string{"", ".", "1.2.3", "0x12", "1e", " 1", "1\x00", string(bytes.Repeat([]byte{'9'}, 65))} {
		if err := l.PutNull(b, 0); err != nil {
			t.Fatal(err)
		}
		before := bytes.Clone(b)
		if err := l.PutNumeric(b, 0, v); err == nil || !bytes.Equal(before, b) {
			t.Fatalf("invalid numeric changed row: %q", v)
		}
	}
	if err := l.PutNull(b, 0); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(b)
	if err := l.PutString(b, 0, "abc"); err == nil || !bytes.Equal(b, before) {
		t.Fatal("wrong type changed row")
	}
	if _, err := NewLayout([]Field{{"bad", Numeric, 0}}); err == nil {
		t.Fatal("zero-width numeric accepted")
	}
	clear(b)
	copy(b[1:], "invalid")
	if _, err := l.GetNumeric(b, 0); err == nil {
		t.Fatal("malformed numeric accepted")
	}
}
