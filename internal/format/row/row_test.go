package row

import (
	"bytes"
	"encoding/hex"
	"math"
	"reflect"
	"testing"
)

func testLayout(t *testing.T) *Layout {
	t.Helper()
	l, err := NewLayout([]Field{{"id", Int32, 4}, {"value", Double, 8}, {"flag", Char, 1}, {"name", String, 4}})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestSchema(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	cases := [][]Field{nil, {{"", Char, 1}}, {{"x", Char, 1}, {"x", Char, 1}}, {{"x", "unknown", 1}}, {{"x", Int32, 3}}, {{"x", Double, 7}}, {{"x", Char, 2}}, {{"x", String, 0}}, {{"x", String, -1}}, {{"x", String, maxInt}, {"y", Char, 1}}}
	for _, fields := range cases {
		if _, err := NewLayout(fields); err == nil {
			t.Errorf("accepted invalid schema: %v", fields)
		}
	}
	l := testLayout(t)
	if l.RowSize() != 18 || !reflect.DeepEqual(l.offsets, []int{1, 5, 13, 14}) {
		t.Fatalf("layout: %+v", l)
	}
	fields := []Field{{"x", Int32, 4}}
	copyLayout, err := NewLayout(fields)
	if err != nil {
		t.Fatal(err)
	}
	fields[0] = Field{"changed", String, 99}
	if copyLayout.fields[0] != (Field{"x", Int32, 4}) {
		t.Fatal("schema aliases input")
	}
}

func TestExactBytes(t *testing.T) {
	l := testLayout(t)
	b := make([]byte, l.RowSize())
	for _, err := range []error{l.PutInt32(b, 0, -2), l.PutDouble(b, 1, 1.5), l.PutChar(b, 2, 0xff), l.PutString(b, 3, "ab")} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := hex.EncodeToString(b); got != "00feffffff000000000000f83fff61620000" {
		t.Fatal(got)
	}
	v, e := l.GetInt32(b, 0)
	if e != nil || v != -2 {
		t.Fatal(v, e)
	}
	d, e := l.GetDouble(b, 1)
	if e != nil || d != 1.5 {
		t.Fatal(d, e)
	}
	c, e := l.GetChar(b, 2)
	if e != nil || c != 0xff {
		t.Fatal(c, e)
	}
}

func TestNumericBits(t *testing.T) {
	l := testLayout(t)
	b := make([]byte, l.RowSize())
	for _, v := range []int32{math.MinInt32, -1, 0, math.MaxInt32} {
		if err := l.PutInt32(b, 0, v); err != nil {
			t.Fatal(err)
		}
		got, err := l.GetInt32(b, 0)
		if err != nil || got != v {
			t.Fatal(got, err)
		}
	}
	for _, bits := range []uint64{0, 1, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, math.Float64bits(math.MaxFloat64)} {
		if err := l.PutDouble(b, 1, math.Float64frombits(bits)); err != nil {
			t.Fatal(err)
		}
		got, err := l.GetDouble(b, 1)
		if err != nil || math.Float64bits(got) != bits {
			t.Fatalf("bits %x: %x %v", bits, math.Float64bits(got), err)
		}
	}
}

func TestStringsAndViews(t *testing.T) {
	l := testLayout(t)
	b := make([]byte, l.RowSize())
	for _, value := range []string{"abcd", "a", "", "éé"} {
		if err := l.PutString(b, 3, value); err != nil {
			t.Fatal(err)
		}
		got, err := l.GetString(b, 3)
		if err != nil || got != value {
			t.Fatal(got, err)
		}
		view, err := l.GetStringBytes(b, 3)
		if err != nil || string(view) != value || cap(view) != len(view) {
			t.Fatal(view, err)
		}
		if !bytes.Equal(b[l.offsets[3]+len(value):], make([]byte, 4-len(value))) {
			t.Fatal("stale padding")
		}
	}
	before := bytes.Clone(b)
	for _, value := range []string{"abcde", "a\x00b", "ééé"} {
		if err := l.PutString(b, 3, value); err == nil || !bytes.Equal(b, before) {
			t.Fatal("invalid write changed row", value)
		}
	}
	if err := l.PutString(b, 3, "ab"); err != nil {
		t.Fatal(err)
	}
	owned, _ := l.GetString(b, 3)
	view, _ := l.GetStringBytes(b, 3)
	view[0] = 'z'
	if b[l.offsets[3]] != 'z' || owned != "ab" {
		t.Fatal("ownership semantics")
	}
}

func TestInvalidAccess(t *testing.T) {
	l := testLayout(t)
	b := make([]byte, l.RowSize())
	for _, tc := range []struct {
		b   []byte
		col int
		typ Dtype
	}{{b[:l.RowSize()-1], 0, Int32}, {append(bytes.Clone(b), 0), 0, Int32}, {b, -1, Int32}, {b, 4, Int32}, {b, 1, Int32}} {
		if _, err := l.slot(tc.b, tc.col, tc.typ); err == nil {
			t.Fatal("accepted invalid access")
		}
	}
	errors := []error{}
	errors = append(errors, l.PutInt32(b, 1, 0), l.PutDouble(b, 0, 0), l.PutChar(b, 0, 0), l.PutString(b, 0, ""))
	_, e := l.GetInt32(b, 1)
	errors = append(errors, e)
	_, e = l.GetDouble(b, 0)
	errors = append(errors, e)
	_, e = l.GetChar(b, 0)
	errors = append(errors, e)
	_, e = l.GetString(b, 0)
	errors = append(errors, e)
	_, e = l.GetStringBytes(b, 0)
	errors = append(errors, e)
	for _, e := range errors {
		if e == nil {
			t.Fatal("missing type error")
		}
	}
}

func TestPayload(t *testing.T) {
	l := testLayout(t)
	for _, cap := range []int{-1, 0, l.RowSize() - 1} {
		if _, err := l.RowsPerBuffer(cap); err == nil {
			t.Fatal("accepted capacity", cap)
		}
	}
	for _, tc := range []struct{ cap, want int }{{l.RowSize(), 1}, {2*l.RowSize() - 1, 1}, {2 * l.RowSize(), 2}, {2*l.RowSize() + 1, 2}} {
		got, err := l.RowsPerBuffer(tc.cap)
		if err != nil || got != tc.want {
			t.Fatal(got, err)
		}
	}
	b := make([]byte, 2*l.RowSize(), 2*l.RowSize()+6)
	if err := l.ValidatePayload(b, 2); err != nil {
		t.Fatal(err)
	}
	if err := l.ValidatePayload(nil, 0); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		b []byte
		n uint32
	}{{b[:len(b)-1], 2}, {b, 1}, {nil, 1}, {b, math.MaxUint32}} {
		if err := l.ValidatePayload(tc.b, tc.n); err == nil {
			t.Fatal("accepted invalid payload")
		}
	}
	for _, index := range []int{-1, 2, math.MaxInt} {
		if _, err := l.RowAt(b, index); err == nil {
			t.Fatal("accepted index", index)
		}
	}
	if _, err := l.RowAt(nil, 0); err == nil {
		t.Fatal("accepted empty row")
	}
	if _, err := l.RowAt(b[:len(b)-1], 0); err == nil {
		t.Fatal("accepted partial row")
	}
	view, err := l.RowAt(b, 1)
	if err != nil || len(view) != l.RowSize() || cap(view) != l.RowSize() {
		t.Fatal(view, err)
	}
	view[0] = 42
	if b[l.RowSize()] != 42 {
		t.Fatal("row view does not alias payload")
	}
}
