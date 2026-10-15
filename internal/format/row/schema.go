package row

import "fmt"

// Dtype identifies a V1 fixed-width field type.
type Dtype string

const (
	Int32  Dtype = "INT"
	Double Dtype = "DOUBLE"
	Char   Dtype = "CHAR"
	String Dtype = "STRING"
)

// Field describes a named column and its width in bytes.
type Field struct {
	Name string `json:"name"`
	Type Dtype  `json:"type"`
	Size int    `json:"size"`
}

// Layout is an immutable ordered schema. Construct it with NewLayout.
// TODO: Support NULL values
type Layout struct {
	fields  []Field
	offsets []int
	rowSize int
}

// NewLayout validates and copies fields, assigning contiguous offsets without padding.
func NewLayout(fields []Field) (*Layout, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("empty schema")
	}

	l := &Layout{
		fields:  append([]Field(nil), fields...),
		offsets: make([]int, len(fields)),
	}

	seen := make(map[string]bool)

	for i, f := range fields {
		if f.Name == "" || seen[f.Name] {
			return nil, fmt.Errorf("invalid/duplicate field %q", f.Name)
		}
		seen[f.Name] = true

		switch f.Type {
		case Int32:
			if f.Size != 4 {
				return nil, fmt.Errorf("%s: INT must be 4 bytes", f.Name)
			}
		case Double:
			if f.Size != 8 {
				return nil, fmt.Errorf("%s: DOUBLE must be 8 bytes", f.Name)
			}
		case Char:
			if f.Size != 1 {
				return nil, fmt.Errorf("%s: CHAR must be 1 byte", f.Name)
			}
		case String:
			if f.Size <= 0 {
				return nil, fmt.Errorf("%s: invalid string width", f.Name)
			}
		default:
			return nil, fmt.Errorf("unsupported type %q", f.Type)
		}

		maxInt := int(^uint(0) >> 1)
		if f.Size > maxInt-l.rowSize {
			return nil, fmt.Errorf("row size overflow")
		}

		l.offsets[i] = l.rowSize
		l.rowSize += f.Size
	}

	return l, nil
}

// RowSize returns the number of bytes in one encoded row.
func (l *Layout) RowSize() int {
	return l.rowSize
}
