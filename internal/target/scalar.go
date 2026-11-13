package target

import (
	"encoding/binary"
	"fmt"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/jackc/pgx/v5/pgtype"
)

// decodeRow produces binary COPY values in one pass. CSV uses decodeRowValues
// below, which returns strings and time.Time values for formatting instead.
func (w *Writer) decodeRow(encoded []byte) ([]any, error) {
	values := make([]any, len(w.fields))
	for i, field := range w.fields {
		null, err := w.layout.IsNull(encoded, i)
		if err == nil && !null {
			values[i], err = decodeCopyField(w.layout, encoded, i, field.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("column %d (%s): %w", i, field.Name, err)
		}
	}
	return values, nil
}

// Numeric text must be parsed once. Temporal values are decoded straight from
// PostgreSQL's binary representation into the types expected by binary COPY.
func decodeCopyField(layout *row.Layout, encoded []byte, col int, dtype row.Dtype) (any, error) {
	switch dtype {
	case row.Int16:
		return layout.GetInt16(encoded, col)
	case row.Int32:
		return layout.GetInt32(encoded, col)
	case row.Int64:
		return layout.GetInt64(encoded, col)
	case row.Real:
		return layout.GetReal(encoded, col)
	case row.Double:
		return layout.GetDouble(encoded, col)
	case row.Bool:
		return layout.GetBool(encoded, col)
	case row.String:
		return layout.GetString(encoded, col)
	case row.Char:
		value, err := layout.GetChar(encoded, col)
		return string([]byte{value}), err
	case row.Numeric:
		text, err := layout.GetNumeric(encoded, col)
		if err != nil {
			return nil, err
		}
		var value pgtype.Numeric
		err = value.Scan(text)
		return value, err
	case row.Date:
		days, err := layout.GetDate(encoded, col)
		if err != nil {
			return nil, err
		}
		var data [4]byte
		binary.BigEndian.PutUint32(data[:], uint32(days))
		var value pgtype.Date
		plan := (pgtype.DateCodec{}).PlanScan(nil, pgtype.DateOID, pgtype.BinaryFormatCode, &value)
		err = plan.Scan(data[:], &value)
		return value, err
	case row.Timestamp:
		micros, err := layout.GetTimestamp(encoded, col)
		if err != nil {
			return nil, err
		}
		var data [8]byte
		binary.BigEndian.PutUint64(data[:], uint64(micros))
		var value pgtype.Timestamp
		plan := (&pgtype.TimestampCodec{}).PlanScan(nil, pgtype.TimestampOID, pgtype.BinaryFormatCode, &value)
		err = plan.Scan(data[:], &value)
		return value, err
	case row.TimestampTZ:
		micros, err := layout.GetTimestampTZ(encoded, col)
		if err != nil {
			return nil, err
		}
		var data [8]byte
		binary.BigEndian.PutUint64(data[:], uint64(micros))
		var value pgtype.Timestamptz
		plan := (&pgtype.TimestamptzCodec{}).PlanScan(nil, pgtype.TimestamptzOID, pgtype.BinaryFormatCode, &value)
		err = plan.Scan(data[:], &value)
		return value, err
	default:
		return nil, fmt.Errorf("unsupported field type %q", dtype)
	}
}

// decodeRowValues returns values for CSV formatting.
func decodeRowValues(layout *row.Layout, fields []row.Field, encoded []byte) ([]any, error) {
	values := make([]any, len(fields))
	for i, field := range fields {
		null, err := layout.IsNull(encoded, i)
		if err == nil && !null {
			values[i], err = decodeField(layout, encoded, i, field.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("column %d (%s): %w", i, field.Name, err)
		}
	}
	return values, nil
}

func decodeField(layout *row.Layout, encoded []byte, col int, dtype row.Dtype) (any, error) {
	switch dtype {
	case row.Int16:
		return layout.GetInt16(encoded, col)
	case row.Int32:
		return layout.GetInt32(encoded, col)
	case row.Int64:
		return layout.GetInt64(encoded, col)
	case row.Real:
		return layout.GetReal(encoded, col)
	case row.Double:
		return layout.GetDouble(encoded, col)
	case row.Bool:
		return layout.GetBool(encoded, col)
	case row.String:
		return layout.GetString(encoded, col)
	case row.Numeric:
		return layout.GetNumeric(encoded, col)
	case row.Char:
		v, err := layout.GetChar(encoded, col)
		return string([]byte{v}), err
	case row.Date:
		v, err := layout.GetDate(encoded, col)
		if err != nil {
			return nil, err
		}
		return decodeTemporal(dtype, int64(v))
	case row.Timestamp:
		v, err := layout.GetTimestamp(encoded, col)
		if err != nil {
			return nil, err
		}
		return decodeTemporal(dtype, v)
	case row.TimestampTZ:
		v, err := layout.GetTimestampTZ(encoded, col)
		if err != nil {
			return nil, err
		}
		return decodeTemporal(dtype, v)
	default:
		return nil, fmt.Errorf("unsupported field type %q", dtype)
	}
}

// pgx's temporal codecs understand PostgreSQL epochs, range limits, and infinity.
// Decode their binary representation into values for CSV formatting:
// finite values become time.Time; infinity becomes its PostgreSQL spelling.
// This avoids UnixNano overflow when handling wide PostgreSQL date ranges.
func decodeTemporal(dtype row.Dtype, value int64) (any, error) {
	var data [8]byte
	var codec pgtype.Codec
	var oid uint32
	size := 8
	switch dtype {
	case row.Date:
		codec, oid, size = pgtype.DateCodec{}, pgtype.DateOID, 4
		binary.BigEndian.PutUint32(data[:4], uint32(value))
	case row.Timestamp:
		codec, oid = &pgtype.TimestampCodec{}, pgtype.TimestampOID
		binary.BigEndian.PutUint64(data[:], uint64(value))
	case row.TimestampTZ:
		codec, oid = &pgtype.TimestamptzCodec{}, pgtype.TimestamptzOID
		binary.BigEndian.PutUint64(data[:], uint64(value))
	}
	return codec.DecodeDatabaseSQLValue(nil, oid, pgtype.BinaryFormatCode, data[:size])
}
