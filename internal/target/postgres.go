package target

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
)

// OpenPostgres establishes a pgx connection owned by the caller.
func OpenPostgres(ctx context.Context, connectionString string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, connectionString)
	if err != nil {
		return nil, fmt.Errorf("target: connect to PostgreSQL: %w", err)
	}
	return conn, nil
}
