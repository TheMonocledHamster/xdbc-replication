package row

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

func (l *Layout) slot(
	row []byte, col int, dtype Dtype,
) ([]byte, error) {
	if len(row) != l.rowSize {
		return nil, fmt.Errorf("invalid row length")
	}
	if col < 0 || col >= len(l.fields) {
		return nil, fmt.Errorf("column index out of range")
	}
	if l.fields[col].Type != dtype {
		return nil, fmt.Errorf("unexpected column type")
	}

	start := l.offsets[col]
	end := start + l.fields[col].Size
	return row[start:end], nil
}

// Writes a signed 32-bit integer in little-endian order.
func (l *Layout) PutInt32(
	row []byte, col int, v int32,
) error {
	dst, err := l.slot(row, col, Int32)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(dst, uint32(v))
	return nil
}

// Reads a signed 32-bit integer in little-endian order.
func (l *Layout) GetInt32(
	row []byte, col int,
) (int32, error) {
	src, err := l.slot(row, col, Int32)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(src)), nil
}

// Writes the IEEE 754 bits of a float64 in little-endian order.
func (l *Layout) PutDouble(
	row []byte, col int, v float64,
) error {
	dst, err := l.slot(row, col, Double)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(dst, math.Float64bits(v))
	return nil
}

// Reads a float64, preserving its IEEE 754 bits.
func (l *Layout) GetDouble(
	row []byte, col int,
) (float64, error) {
	src, err := l.slot(row, col, Double)
	if err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(src)
	return math.Float64frombits(bits), nil
}

// Writes a single byte.
func (l *Layout) PutChar(
	row []byte, col int, v byte,
) error {
	dst, err := l.slot(row, col, Char)
	if err != nil {
		return err
	}
	dst[0] = v
	return nil
}

// Reads a single byte.
func (l *Layout) GetChar(
	row []byte, col int,
) (byte, error) {
	src, err := l.slot(row, col, Char)
	if err != nil {
		return 0, err
	}
	return src[0], nil
}

// Writes a string and clears trailing padding. It rejects embedded NUL
// and values exceeding the field width in bytes without modifying the row.
func (l *Layout) PutString(
	row []byte, col int, v string,
) error {
	dst, err := l.slot(row, col, String)
	if err != nil {
		return err
	}
	if len(v) > len(dst) || strings.IndexByte(v, 0) >= 0 {
		return fmt.Errorf("string cannot fit in field")
	}
	clear(dst)
	copy(dst, v)
	return nil
}

// Returns an owned copy of the string field.
func (l *Layout) GetString(row []byte, col int) (string, error) {
	src, err := l.GetStringBytes(row, col)
	if err != nil {
		return "", err
	}
	return string(src), nil
}

// Returns a borrowed view ending before the first NUL, or at the
// field width if there is no NUL. Its capacity equals its length. The view expires
// when row ownership transfers or the backing pool buffer is released.
func (l *Layout) GetStringBytes(row []byte, col int) ([]byte, error) {
	src, err := l.slot(row, col, String)
	if err != nil {
		return nil, err
	}
	n := bytes.IndexByte(src, 0)
	if n < 0 {
		n = len(src)
	}
	return src[:n:n], nil
}
