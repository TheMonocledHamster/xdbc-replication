package target

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// OpenPostgres opens and verifies a PostgreSQL connection using lib/pq.
// The returned database handle should be closed by the caller.
func OpenPostgres(ctx context.Context, connectionString string) (*sql.DB, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("target: open PostgreSQL connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("target: connect to PostgreSQL: %w", err)
	}
	return db, nil
}
