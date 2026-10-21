package postgres

import (
	"encoding/binary"
	"fmt"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type columnAdapter struct {
	destination any
	encode      func([]byte) error
	postScan    func([]byte)
}

// PostgreSQL OIDs, not the Go type returned by Values, identify each column.
// In particular, all three temporal types can otherwise become time.Time.
var sourceOIDs = map[row.Dtype][]uint32{
	row.Int16: {pgtype.Int2OID}, row.Int32: {pgtype.Int4OID}, row.Int64: {pgtype.Int8OID},
	row.Real: {pgtype.Float4OID}, row.Double: {pgtype.Float8OID}, row.Bool: {pgtype.BoolOID},
	row.String:  {pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID},
	row.Char:    {pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID},
	row.Numeric: {pgtype.NumericOID}, row.Date: {pgtype.DateOID},
	row.Timestamp: {pgtype.TimestampOID}, row.TimestampTZ: {pgtype.TimestamptzOID},
}

func prepareColumns(layout *row.Layout, descriptions []pgconn.FieldDescription) ([]columnAdapter, []any, error) {
	fields := layout.Fields()
	if len(fields) != len(descriptions) {
		return nil, nil, fmt.Errorf("query returns %d columns, layout requires %d", len(descriptions), len(fields))
	}
	adapters := make([]columnAdapter, len(fields))
	destinations := make([]any, len(fields))
	for col, field := range fields {
		oid := descriptions[col].DataTypeOID
		compatible := false
		for _, supported := range sourceOIDs[field.Type] {
			if oid == supported {
				compatible = true
				break
			}
		}
		if !compatible {
			return nil, nil, fmt.Errorf("column %d (%s): PostgreSQL OID %d is incompatible with %s", col, field.Name, oid, field.Type)
		}
		adapters[col] = makeColumn(layout, col, field.Type, oid, descriptions[col].Format)
		destinations[col] = adapters[col].destination
	}
	return adapters, destinations, nil
}

func makeColumn(layout *row.Layout, col int, dtype row.Dtype, oid uint32, format int16) columnAdapter {
	bind := func(dest any, valid *bool, write func([]byte) error) columnAdapter {
		return columnAdapter{destination: dest, encode: func(record []byte) error {
			if !*valid {
				return layout.PutNull(record, col)
			}
			return write(record)
		}}
	}
	switch dtype {
	case row.Int16:
		v := new(pgtype.Int2)
		return bind(v, &v.Valid, func(record []byte) error { return layout.PutInt16(record, col, v.Int16) })
	case row.Int32:
		v := new(pgtype.Int4)
		return bind(v, &v.Valid, func(record []byte) error { return layout.PutInt32(record, col, v.Int32) })
	case row.Int64:
		v := new(pgtype.Int8)
		return bind(v, &v.Valid, func(record []byte) error { return layout.PutInt64(record, col, v.Int64) })
	case row.Real:
		v := new(pgtype.Float4)
		return bind(v, &v.Valid, func(record []byte) error { return layout.PutReal(record, col, v.Float32) })
	case row.Double:
		v := new(pgtype.Float8)
		return bind(v, &v.Valid, func(record []byte) error { return layout.PutDouble(record, col, v.Float64) })
	case row.Bool:
		v := new(pgtype.Bool)
		return bind(v, &v.Valid, func(record []byte) error { return layout.PutBool(record, col, v.Bool) })
	case row.String, row.Char:
		v := new(pgtype.Text)
		return bind(v, &v.Valid, func(record []byte) error {
			if dtype == row.String {
				return layout.PutString(record, col, v.String)
			}
			if len(v.String) != 1 {
				return fmt.Errorf("CHAR requires exactly one byte, got %d", len(v.String))
			}
			return layout.PutChar(record, col, v.String[0])
		})
	case row.Numeric:
		v := new(pgtype.Numeric)
		adapter := bind(v, &v.Valid, func(record []byte) error {
			text, err := v.Value()
			if err != nil {
				return err
			}
			return layout.PutNumeric(record, col, text.(string))
		})
		if format == pgtype.BinaryFormatCode {
			adapter.postScan = func(raw []byte) {
				// pgx v5's zero-digit binary NUMERIC path drops dscale.
				// Restore it from the wire header so 0.00 does not become 0.
				// Other values already retain scale in their Int/Exp pair.
				if v.Valid && v.Int != nil && v.Int.Sign() == 0 && len(raw) >= 8 {
					v.Exp = -int32(binary.BigEndian.Uint16(raw[6:8]))
				}
			}
		}
		return adapter
	case row.Date:
		v := new(pgtype.Date)
		plan := (pgtype.DateCodec{}).PlanEncode(nil, oid, pgtype.BinaryFormatCode, v)
		var scratch [4]byte
		return bind(v, &v.Valid, func(record []byte) error {
			b, err := plan.Encode(v, scratch[:0])
			if err != nil {
				return err
			}
			return layout.PutDate(record, col, int32(binary.BigEndian.Uint32(b)))
		})
	case row.Timestamp:
		v := new(pgtype.Timestamp)
		plan := (&pgtype.TimestampCodec{}).PlanEncode(nil, oid, pgtype.BinaryFormatCode, v)
		var scratch [8]byte
		return bind(v, &v.Valid, func(record []byte) error {
			b, err := plan.Encode(v, scratch[:0])
			if err != nil {
				return err
			}
			return layout.PutTimestamp(record, col, int64(binary.BigEndian.Uint64(b)))
		})
	case row.TimestampTZ:
		v := new(pgtype.Timestamptz)
		plan := (&pgtype.TimestamptzCodec{}).PlanEncode(nil, oid, pgtype.BinaryFormatCode, v)
		var scratch [8]byte
		return bind(v, &v.Valid, func(record []byte) error {
			b, err := plan.Encode(v, scratch[:0])
			if err != nil {
				return err
			}
			return layout.PutTimestampTZ(record, col, int64(binary.BigEndian.Uint64(b)))
		})
	default:
		// prepareColumns rejects unsupported types before constructing an adapter.
		panic("unvalidated row type")
	}
}
