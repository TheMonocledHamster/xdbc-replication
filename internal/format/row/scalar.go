package row

import (
	"fmt"
	"math"
	"regexp"
)

// Temporal infinity sentinels use PostgreSQL's signed integer extrema.
const (
	DateNegativeInfinity      int32 = math.MinInt32
	DateInfinity              int32 = math.MaxInt32
	TimestampNegativeInfinity int64 = math.MinInt64
	TimestampInfinity         int64 = math.MaxInt64
)

func (l *Layout) PutInt16(record []byte, col int, v int16) error {
	return l.putBits(record, col, Int16, uint64(uint16(v)))
}
func (l *Layout) GetInt16(record []byte, col int) (int16, error) {
	v, err := l.getBits(record, col, Int16)
	return int16(v), err
}
func (l *Layout) PutInt64(record []byte, col int, v int64) error {
	return l.putBits(record, col, Int64, uint64(v))
}
func (l *Layout) GetInt64(record []byte, col int) (int64, error) {
	v, err := l.getBits(record, col, Int64)
	return int64(v), err
}
func (l *Layout) PutReal(record []byte, col int, v float32) error {
	return l.putBits(record, col, Real, uint64(math.Float32bits(v)))
}
func (l *Layout) GetReal(record []byte, col int) (float32, error) {
	v, err := l.getBits(record, col, Real)
	return math.Float32frombits(uint32(v)), err
}
func (l *Layout) PutBool(record []byte, col int, v bool) error {
	var bits uint64
	if v {
		bits = 1
	}
	return l.putBits(record, col, Bool, bits)
}
func (l *Layout) GetBool(record []byte, col int) (bool, error) {
	v, err := l.getBits(record, col, Bool)
	if err != nil {
		return false, err
	}
	if v > 1 {
		return false, fmt.Errorf("invalid BOOLEAN byte %d", v)
	}
	return v == 1, nil
}

// PutDate writes days since 2000-01-01, or a named date infinity sentinel.
func (l *Layout) PutDate(record []byte, col int, days int32) error {
	return l.putBits(record, col, Date, uint64(uint32(days)))
}
func (l *Layout) GetDate(record []byte, col int) (int32, error) {
	v, err := l.getBits(record, col, Date)
	return int32(v), err
}

// PutTimestamp writes wall-clock microseconds since 2000-01-01, ignoring zones.
func (l *Layout) PutTimestamp(record []byte, col int, micros int64) error {
	return l.putBits(record, col, Timestamp, uint64(micros))
}
func (l *Layout) GetTimestamp(record []byte, col int) (int64, error) {
	v, err := l.getBits(record, col, Timestamp)
	return int64(v), err
}

// PutTimestampTZ writes UTC-instant microseconds since 2000-01-01T00:00:00Z.
// Both timestamp types also accept the named timestamp infinity sentinels.
func (l *Layout) PutTimestampTZ(record []byte, col int, micros int64) error {
	return l.putBits(record, col, TimestampTZ, uint64(micros))
}
func (l *Layout) GetTimestampTZ(record []byte, col int) (int64, error) {
	v, err := l.getBits(record, col, TimestampTZ)
	return int64(v), err
}

// Validate the spelling, not its floating-point approximation. Precision/scale
// enforcement belongs to PostgreSQL; this codec stores the value exactly as given.
var numericText = regexp.MustCompile(`^(?:[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|NaN|[+-]?Infinity)$`)

// PutNumeric writes exact decimal text without rounding or normalizing its scale.
func (l *Layout) PutNumeric(record []byte, col int, v string) error {
	if !numericText.MatchString(v) {
		return fmt.Errorf("invalid NUMERIC text %q", v)
	}
	return l.putText(record, col, Numeric, v)
}
func (l *Layout) GetNumeric(record []byte, col int) (string, error) {
	src, err := l.textBytes(record, col, Numeric)
	if err != nil {
		return "", err
	}
	v := string(src)
	if !numericText.MatchString(v) {
		return "", fmt.Errorf("invalid NUMERIC text %q", v)
	}
	return v, nil
}
