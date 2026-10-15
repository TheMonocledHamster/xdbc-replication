
package postgres

import (
	"context"
	"fmt"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
	"github.com/jackc/pgx/v5"
)

// Read reads PostgreSQL rows, encodes them into pooled buffers,
// and sends completed buffer IDs to the output queue.
//
// The caller owns the database connection, pool, and queue.
// Read does not close the output queue.
func Read(
	ctx context.Context,
	conn *pgx.Conn,
	query string,
	layout *row.Layout,
	p *pool.Pool,
	out queue.Queue,
) error {
	// Figure out how many complete rows fit in one buffer.
	capacity, err := layout.RowsPerBuffer(p.PayloadCap())
	if err != nil {
		return err
	}

	// Execute the SQL query supplied by the caller.
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query PostgreSQL: %w", err)
	}
	defer rows.Close()

	// No buffer is currently owned by the reader.
	bufferID := -1
	var buffer []byte
	count := 0
	var sequence uint64
	var nextRecord uint64

	// If the reader exits while owning a buffer, return it.
	defer func() {
		if bufferID >= 0 {
			_ = p.Release(bufferID)
		}
	}()

	// Finish a buffer and hand it to the output queue.
	flush := func() error {
		if count == 0 {
			return nil
		}

		usedBytes := count * layout.RowSize()

		header := pool.BufferHeader{
			Seq:             sequence,
			NextRecord:      nextRecord + uint64(count),
			RecordCount:     uint32(count),
			PayloadLen:      uint32(usedBytes),
			UncompressedLen: uint32(usedBytes),
		}
		header.Encode(buffer)

		if err := out.Push(ctx, bufferID); err != nil {
			return fmt.Errorf("push buffer to queue: %w", err)
		}

		// Push succeeded: the consumer now owns this buffer.
		bufferID = -1
		buffer = nil
		nextRecord += uint64(count)
		sequence++
		count = 0
		return nil
	}

	for rows.Next() {
		// Read the actual PostgreSQL column values.
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("read PostgreSQL row: %w", err)
		}

		// Borrow a buffer only when we have a row to write.
		if bufferID == -1 {
			bufferID, err = p.Acquire(ctx)
			if err != nil {
				return fmt.Errorf("acquire buffer: %w", err)
			}

			buffer, err = p.Buffer(bufferID)
			if err != nil {
				return fmt.Errorf("access buffer: %w", err)
			}
		}

		// Find the next row slot inside the buffer payload.
		payload := buffer[pool.HeaderSize : pool.HeaderSize+capacity*layout.RowSize()]

		record, err := layout.RowAt(payload, count)
		if err != nil {
			return fmt.Errorf("get row slot: %w", err)
		}

		// Encode each column based on its PostgreSQL value type.
		for col, value := range values {
			switch v := value.(type) {
			case int32:
				err = layout.PutInt32(record, col, v)
			case float64:
				err = layout.PutDouble(record, col, v)
			case string:
				err = layout.PutString(record, col, v)
			case nil:
				return fmt.Errorf("column %d: NULL values not supported", col)
			default:
				return fmt.Errorf(
					"column %d: unsupported PostgreSQL value type %T",
					col, value,
				)
			}

			if err != nil {
				return fmt.Errorf("encode column %d: %w", col, err)
			}
		}

		count++

		// Once the buffer is full, send it downstream.
		if count == capacity {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate PostgreSQL rows: %w", err)
	}

	// Send the final buffer, even if only partially filled.
	return flush()
}
