package row

import "fmt"

// RowsPerBuffer returns how many complete rows fit in a payload capacity.
// A capacity that cannot hold at least one row is rejected.
func (l *Layout) RowsPerBuffer(payloadCap int) (int, error) {
	if payloadCap < l.rowSize {
		return 0, fmt.Errorf("payload capacity %d cannot hold a %d-byte row", payloadCap, l.rowSize)
	}
	return payloadCap / l.rowSize, nil
}

// RowAt returns a borrowed, capacity-capped view of a row in a used payload.
// The payload must contain only complete rows, excluding unused buffer capacity.
// The view expires when buffer ownership transfers or the buffer is released.
func (l *Layout) RowAt(payload []byte, index int) ([]byte, error) {
	if len(payload)%l.rowSize != 0 {
		return nil, fmt.Errorf("payload contains a partial row")
	}
	if index < 0 || index >= len(payload)/l.rowSize {
		return nil, fmt.Errorf("row index out of range")
	}
	start := index * l.rowSize
	end := start + l.rowSize
	return payload[start:end:end], nil
}

// ValidatePayload checks that a decoded payload contains exactly recordCount
// complete rows. An empty payload with zero records is valid.
func (l *Layout) ValidatePayload(payload []byte, recordCount uint32) error {
	if len(payload)%l.rowSize != 0 {
		return fmt.Errorf("payload contains a partial row")
	}
	if uint64(len(payload)/l.rowSize) != uint64(recordCount) {
		return fmt.Errorf("payload row count does not match record count %d", recordCount)
	}
	return nil
}
