// The harness is deliberately fixture-specific: it prepares and compares small
// PostgreSQL tables without depending on XDBC's row encoder or decoder.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/transfer"
	"github.com/jackc/pgx/v5"
)

type harness struct {
	config               transfer.Config
	source, sink         *pgx.Conn
	clientURL, serverURL string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: harness seed|transfer|verify|e2e|health")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clientURL := transfer.Env("CLIENT_URL", "http://client:8080")
	if os.Args[1] == "health" {
		return health(ctx, clientURL)
	}
	c, err := transfer.LoadConfig(transfer.Env("CONFIG_PATH", "testdata/iot/transfer.json"))
	if err != nil {
		return err
	}
	source, err := pgx.Connect(ctx, os.Getenv("SOURCE_DATABASE_URL"))
	if err != nil {
		return err
	}
	defer source.Close(context.Background())
	sink, err := pgx.Connect(ctx, os.Getenv("TARGET_DATABASE_URL"))
	if err != nil {
		return err
	}
	defer sink.Close(context.Background())
	h := harness{c, source, sink, clientURL, transfer.Env("SERVER_URL", "http://server:8080")}
	switch os.Args[1] {
	case "seed":
		return h.seed(ctx)
	case "transfer":
		return h.transfer(ctx, http.StatusOK)
	case "verify":
		return h.verify(ctx)
	case "e2e":
		return h.e2e(ctx)
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}
func (h harness) seed(ctx context.Context) error {
	ddl, err := os.ReadFile("testdata/iot/schema.sql")
	if err != nil {
		return err
	}
	for _, db := range []*pgx.Conn{h.source, h.sink} {
		if _, err := db.Exec(ctx, string(ddl)); err != nil {
			return err
		}
		if _, err := db.Exec(ctx, "TRUNCATE TABLE "+h.config.Relation()); err != nil {
			return err
		}
	}
	if _, err := h.sink.Exec(ctx, "ALTER TABLE "+h.config.Relation()+" DROP CONSTRAINT IF EXISTS harness_reject"); err != nil {
		return err
	}
	file, err := os.Open("testdata/iot/sample.csv")
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = h.source.PgConn().CopyFrom(ctx, file, "COPY "+h.config.Relation()+" FROM STDIN WITH (FORMAT csv, HEADER true)")
	return err
}
func (h harness) transfer(ctx context.Context, status int) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.clientURL+"/transfer", nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode != status {
		return fmt.Errorf("transfer returned %d, expected %d: %s", response.StatusCode, status, body)
	}
	if status == http.StatusOK {
		var result transfer.Result
		if err := json.Unmarshal(body, &result); err != nil {
			return err
		}
		var count int64
		if err := h.source.QueryRow(ctx, "SELECT count(*) FROM "+h.config.Relation()).Scan(&count); err != nil {
			return err
		}
		if result.Rows != count {
			return fmt.Errorf("response reports %d rows, source has %d", result.Rows, count)
		}
	}
	fmt.Print(string(body))
	return nil
}
func (h harness) verify(ctx context.Context) error {
	// PostgreSQL renders every column, including NULL and floating-point special
	// values. Sorting the tiny fixture preserves duplicates without assuming a key.
	query := "SELECT row_to_json(t)::text FROM (" + h.config.Query() + ") t"
	read := func(db *pgx.Conn) ([]string, error) {
		rows, err := db.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var records []string
		for rows.Next() {
			var record string
			if err := rows.Scan(&record); err != nil {
				return nil, err
			}
			records = append(records, record)
		}
		slices.Sort(records)
		return records, rows.Err()
	}
	source, err := read(h.source)
	if err != nil {
		return err
	}
	sink, err := read(h.sink)
	if err != nil {
		return err
	}
	if !slices.Equal(source, sink) {
		return fmt.Errorf("source and sink differ (source=%d rows, sink=%d rows)", len(source), len(sink))
	}
	fmt.Printf("verified %d rows across %d columns\n", len(source), len(h.config.Fields))
	return nil
}
func health(ctx context.Context, url string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(url, "/")+"/healthz", nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s is not ready: %d", url, response.StatusCode)
	}
	return nil
}
func (h harness) e2e(ctx context.Context) error {
	if err := h.seed(ctx); err != nil {
		return err
	}
	// Assert the checked-in fixture really contains the documented 128 records.
	file, err := os.Open("testdata/iot/sample.csv")
	if err != nil {
		return err
	}
	records, err := csv.NewReader(file).ReadAll()
	file.Close()
	if err != nil {
		return err
	}
	if len(records) != 129 {
		return fmt.Errorf("expected header + 128 IoT records, got %d", len(records))
	}
	for i := 0; i < 2; i++ {
		if err := h.transfer(ctx, http.StatusOK); err != nil {
			return err
		}
		if err := h.verify(ctx); err != nil {
			return err
		}
	}
	// NOT VALID permits adding the constraint to a populated table; new writes
	// still fail. A failed request must not poison the persistent processes.
	constraint := "ALTER TABLE " + h.config.Relation()
	if _, err := h.sink.Exec(ctx, constraint+" ADD CONSTRAINT harness_reject CHECK (false) NOT VALID"); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.sink.Exec(cleanup, constraint+" DROP CONSTRAINT IF EXISTS harness_reject")
	}()
	if err := h.transfer(ctx, http.StatusInternalServerError); err != nil {
		return err
	}
	for _, url := range []string{h.clientURL, h.serverURL} {
		if err := health(ctx, url); err != nil {
			return err
		}
	}
	if _, err := h.sink.Exec(ctx, constraint+" DROP CONSTRAINT harness_reject"); err != nil {
		return err
	}
	if err := h.transfer(ctx, http.StatusOK); err != nil {
		return err
	}
	if err := h.verify(ctx); err != nil {
		return err
	}
	// Empty sources must still send END and clear the sink.
	if _, err := h.source.Exec(ctx, "TRUNCATE TABLE "+h.config.Relation()); err != nil {
		return err
	}
	if err := h.transfer(ctx, http.StatusOK); err != nil {
		return err
	}
	if err := h.verify(ctx); err != nil {
		return err
	}
	// Leave a useful, successfully transferred fixture behind.
	if err := h.seed(ctx); err != nil {
		return err
	}
	if err := h.transfer(ctx, http.StatusOK); err != nil {
		return err
	}
	return h.verify(ctx)
}
