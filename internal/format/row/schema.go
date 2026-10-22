package row

import "fmt"

// Dtype identifies a V1 fixed-width field type.
type Dtype string

const (
	Int32       Dtype = "INT"
	Double      Dtype = "DOUBLE"
	Char        Dtype = "CHAR"
	String      Dtype = "STRING"
	Int16       Dtype = "SMALLINT"
	Int64       Dtype = "BIGINT"
	Bool        Dtype = "BOOLEAN"
	Real        Dtype = "REAL"
	Numeric     Dtype = "NUMERIC"
	Date        Dtype = "DATE"
	Timestamp   Dtype = "TIMESTAMP"
	TimestampTZ Dtype = "TIMESTAMPTZ"
)

// Field describes a named column and its width in bytes.
type Field struct {
	Name string `json:"name"`
	Type Dtype  `json:"type"`
	Size int    `json:"size"`
}

// Layout is an immutable ordered schema. Construct it with NewLayout.
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
		rowSize: (len(fields)-1)/8 + 1,
	}

	seen := make(map[string]bool)
	widths := map[Dtype]int{Int16: 2, Int32: 4, Int64: 8, Bool: 1, Real: 4,
		Double: 8, Char: 1, Date: 4, Timestamp: 8, TimestampTZ: 8}

	for i, f := range fields {
		if f.Name == "" || seen[f.Name] {
			return nil, fmt.Errorf("invalid/duplicate field %q", f.Name)
		}
		seen[f.Name] = true

		if width, ok := widths[f.Type]; ok {
			if f.Size != width {
				return nil, fmt.Errorf("%s: %s must be %d bytes", f.Name, f.Type, width)
			}
		} else {
			switch f.Type {
			case String, Numeric:
				if f.Size <= 0 {
					return nil, fmt.Errorf("%s: invalid %s width", f.Name, f.Type)
				}
			default:
				return nil, fmt.Errorf("unsupported type %q", f.Type)
			}
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

// Fields returns an independent copy of the ordered schema.
func (l *Layout) Fields() []Field { return append([]Field(nil), l.fields...) }

// RowSize returns the number of bytes in one encoded row.
func (l *Layout) RowSize() int {
	return l.rowSize
}
