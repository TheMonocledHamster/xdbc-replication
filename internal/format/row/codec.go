package row

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

// ErrNull is returned by typed getters when the selected value is SQL NULL.
var ErrNull = errors.New("row value is NULL")

func (l *Layout) columnSlot(record []byte, col int) ([]byte, error) {
	if len(record) != l.rowSize {
		return nil, fmt.Errorf("invalid row length")
	}
	if col < 0 || col >= len(l.fields) {
		return nil, fmt.Errorf("column index out of range")
	}
	start := l.offsets[col]
	end := start + l.fields[col].Size
	return record[start:end:end], nil
}

func (l *Layout) slot(record []byte, col int, dtype Dtype) ([]byte, error) {
	dst, err := l.columnSlot(record, col)
	if err != nil {
		return nil, err
	}
	if l.fields[col].Type != dtype {
		return nil, fmt.Errorf("unexpected column type: got %s, want %s", l.fields[col].Type, dtype)
	}
	return dst, nil
}

func (l *Layout) readSlot(record []byte, col int, dtype Dtype) ([]byte, error) {
	src, err := l.slot(record, col, dtype)
	if err != nil {
		return nil, err
	}
	if record[col/8]&(1<<uint(col%8)) != 0 {
		return nil, ErrNull
	}
	return src, nil
}

// PutNull marks a column NULL and clears its entire fixed-width slot.
func (l *Layout) PutNull(record []byte, col int) error {
	dst, err := l.columnSlot(record, col)
	if err != nil {
		return err
	}
	clear(dst)
	record[col/8] |= 1 << uint(col%8)
	return nil
}

// IsNull reports the NULL bitmap bit after checking row length and column bounds.
func (l *Layout) IsNull(record []byte, col int) (bool, error) {
	if _, err := l.columnSlot(record, col); err != nil {
		return false, err
	}
	return record[col/8]&(1<<uint(col%8)) != 0, nil
}

func clearNull(record []byte, col int) { record[col/8] &^= 1 << uint(col%8) }

func (l *Layout) putBits(record []byte, col int, dtype Dtype, bits uint64) error {
	dst, err := l.slot(record, col, dtype)
	if err != nil {
		return err
	}
	switch len(dst) {
	case 1:
		dst[0] = byte(bits)
	case 2:
		binary.LittleEndian.PutUint16(dst, uint16(bits))
	case 4:
		binary.LittleEndian.PutUint32(dst, uint32(bits))
	case 8:
		binary.LittleEndian.PutUint64(dst, bits)
	default:
		return fmt.Errorf("invalid scalar width")
	}
	clearNull(record, col)
	return nil
}

func (l *Layout) getBits(record []byte, col int, dtype Dtype) (uint64, error) {
	src, err := l.readSlot(record, col, dtype)
	if err != nil {
		return 0, err
	}
	switch len(src) {
	case 1:
		return uint64(src[0]), nil
	case 2:
		return uint64(binary.LittleEndian.Uint16(src)), nil
	case 4:
		return uint64(binary.LittleEndian.Uint32(src)), nil
	case 8:
		return binary.LittleEndian.Uint64(src), nil
	}
	return 0, fmt.Errorf("invalid scalar width")
}

// PutInt32 writes a signed little-endian integer and clears its NULL bit.
func (l *Layout) PutInt32(record []byte, col int, v int32) error {
	return l.putBits(record, col, Int32, uint64(uint32(v)))
}
func (l *Layout) GetInt32(record []byte, col int) (int32, error) {
	v, err := l.getBits(record, col, Int32)
	return int32(v), err
}

// PutDouble preserves the IEEE 754 bits, including NaNs and signed zero.
func (l *Layout) PutDouble(record []byte, col int, v float64) error {
	return l.putBits(record, col, Double, math.Float64bits(v))
}
func (l *Layout) GetDouble(record []byte, col int) (float64, error) {
	v, err := l.getBits(record, col, Double)
	return math.Float64frombits(v), err
}
func (l *Layout) PutChar(record []byte, col int, v byte) error {
	return l.putBits(record, col, Char, uint64(v))
}
func (l *Layout) GetChar(record []byte, col int) (byte, error) {
	v, err := l.getBits(record, col, Char)
	return byte(v), err
}

func (l *Layout) putText(record []byte, col int, dtype Dtype, v string) error {
	dst, err := l.slot(record, col, dtype)
	if err != nil {
		return err
	}
	if len(v) > len(dst) || strings.IndexByte(v, 0) >= 0 {
		return fmt.Errorf("%s cannot fit in field", dtype)
	}
	clear(dst)
	copy(dst, v)
	clearNull(record, col)
	return nil
}

func (l *Layout) textBytes(record []byte, col int, dtype Dtype) ([]byte, error) {
	src, err := l.readSlot(record, col, dtype)
	if err != nil {
		return nil, err
	}
	n := bytes.IndexByte(src, 0)
	if n < 0 {
		n = len(src)
	}
	return src[:n:n], nil
}

// PutString rejects embedded NUL and oversized values without modifying the row.
func (l *Layout) PutString(record []byte, col int, v string) error {
	return l.putText(record, col, String, v)
}

// GetString returns an owned string; a NULL returns ErrNull.
func (l *Layout) GetString(record []byte, col int) (string, error) {
	src, err := l.GetStringBytes(record, col)
	return string(src), err
}

// GetStringBytes borrows bytes until row ownership transfers or the buffer is released.
func (l *Layout) GetStringBytes(record []byte, col int) ([]byte, error) {
	return l.textBytes(record, col, String)
}
