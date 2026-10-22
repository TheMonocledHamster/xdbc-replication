package target

import (
	"encoding/binary"
	"fmt"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/jackc/pgx/v5/pgtype"
)

func (w *Writer) decodeRow(encoded []byte) ([]any, error) {
	values := make([]any, len(w.fields))
	for i, field := range w.fields {
		null, err := w.layout.IsNull(encoded, i)
		if err == nil && !null {
			values[i], err = w.decodeField(encoded, i, field.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("column %d (%s): %w", i, field.Name, err)
		}
	}
	return values, nil
}

func (w *Writer) decodeField(encoded []byte, col int, dtype row.Dtype) (any, error) {
	switch dtype {
	case row.Int16:
		return w.layout.GetInt16(encoded, col)
	case row.Int32:
		return w.layout.GetInt32(encoded, col)
	case row.Int64:
		return w.layout.GetInt64(encoded, col)
	case row.Real:
		return w.layout.GetReal(encoded, col)
	case row.Double:
		return w.layout.GetDouble(encoded, col)
	case row.Bool:
		return w.layout.GetBool(encoded, col)
	case row.String:
		return w.layout.GetString(encoded, col)
	case row.Numeric:
		return w.layout.GetNumeric(encoded, col)
	case row.Char:
		v, err := w.layout.GetChar(encoded, col)
		return string([]byte{v}), err
	case row.Date:
		v, err := w.layout.GetDate(encoded, col)
		if err != nil {
			return nil, err
		}
		return decodeTemporal(dtype, int64(v))
	case row.Timestamp:
		v, err := w.layout.GetTimestamp(encoded, col)
		if err != nil {
			return nil, err
		}
		return decodeTemporal(dtype, v)
	case row.TimestampTZ:
		v, err := w.layout.GetTimestampTZ(encoded, col)
		if err != nil {
			return nil, err
		}
		return decodeTemporal(dtype, v)
	default:
		return nil, fmt.Errorf("unsupported field type %q", dtype)
	}
}

// pgx's temporal codecs understand PostgreSQL epochs, range limits, and infinity.
// Decode their binary representation into database/sql values for lib/pq COPY:
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
