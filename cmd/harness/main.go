// The harness loads, transfers, verifies, and exports the paper datasets.
// Small checked-in samples drive CI; full operations stream their data.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/dataset"
	"github.com/TheMonocledHamster/xdbc-replication/internal/transfer"
	"github.com/jackc/pgx/v5"
)

type harness struct {
	dataset              dataset.Dataset
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
		return errors.New("usage: harness seed|load|fetch|transfer|verify|export|e2e|health")
	}
	timeout, err := transfer.Timeout()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout+time.Minute)
	defer cancel()
	clientURL := transfer.Env("CLIENT_URL", "http://client:8080")
	if os.Args[1] == "health" {
		return health(ctx, clientURL)
	}
	d, err := dataset.Load(transfer.Env("CONFIG_PATH", "testdata/iot/transfer.json"))
	if err != nil {
		return err
	}
	if os.Args[1] == "fetch" {
		return d.Fetch(ctx, os.Stdout)
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
	h := harness{d, d.Config, source, sink, clientURL, transfer.Env("SERVER_URL", "http://server:8080")}
	switch os.Args[1] {
	case "seed":
		return h.seed(ctx)
	case "load":
		return h.load(ctx, transfer.Env("DATA_FILE", "/data/"+d.Name+".csv"), d.FullHeader, false)
	case "export":
		if _, err := h.sink.Exec(ctx, "SET TIME ZONE 'UTC'; SET DateStyle TO 'ISO'"); err != nil {
			return err
		}
		_, err := h.sink.PgConn().CopyTo(ctx, os.Stdout, d.CopyQuery())
		return err
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
	return h.load(ctx, filepath.Join(h.dataset.Directory, "sample.csv"), true, true)
}

// Loading replaces only the selected source relation. The sink is cleared by
// the transfer itself; a bad input file does not destroy its previous contents.
func (h harness) load(ctx context.Context, path string, header, sample bool) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	ddl, err := os.ReadFile(filepath.Join(h.dataset.Directory, "schema.sql"))
	if err != nil {
		return err
	}
	for _, db := range []*pgx.Conn{h.source, h.sink} {
		if _, err := db.Exec(ctx, string(ddl)); err != nil {
			return err
		}
	}
	if _, err := h.sink.Exec(ctx, "ALTER TABLE "+h.config.Relation()+" DROP CONSTRAINT IF EXISTS harness_reject"); err != nil {
		return err
	}
	tx, err := h.source.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "TRUNCATE TABLE "+h.config.Relation()); err != nil {
		return err
	}
	tag, err := tx.Conn().PgConn().CopyFrom(ctx, file, fmt.Sprintf("COPY %s FROM STDIN WITH (FORMAT csv, HEADER %t)", h.config.Relation(), header))
	if err != nil {
		return err
	}
	if sample && tag.RowsAffected() != h.dataset.SampleRows {
		return fmt.Errorf("%s sample has %d rows, expected %d", h.dataset.Name, tag.RowsAffected(), h.dataset.SampleRows)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	fmt.Printf("loaded %s: %d rows\n", h.dataset.Name, tag.RowsAffected())
	return nil
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
	fingerprint := func(db *pgx.Conn) (string, int64, error) {
		if _, err := db.Exec(ctx, "SET TIME ZONE 'UTC'; SET DateStyle TO 'ISO'"); err != nil {
			return "", 0, err
		}
		hash := sha256.New()
		tag, err := db.PgConn().CopyTo(ctx, hash, h.dataset.CopyQuery())
		return fmt.Sprintf("%x", hash.Sum(nil)), tag.RowsAffected(), err
	}
	source, sourceRows, err := fingerprint(h.source)
	if err != nil {
		return err
	}
	sink, sinkRows, err := fingerprint(h.sink)
	if err != nil {
		return err
	}
	if source != sink || sourceRows != sinkRows {
		return fmt.Errorf("%s source and sink differ (rows: %d/%d, SHA-256: %s/%s)", h.dataset.Name, sourceRows, sinkRows, source, sink)
	}
	fmt.Printf("verified %s: %d rows, %d columns, SHA-256 %s\n", h.dataset.Name, sourceRows, len(h.config.Fields), source)
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
	for i := 0; i < 2; i++ {
		if err := h.transfer(ctx, http.StatusOK); err != nil {
			return err
		}
		if err := h.verify(ctx); err != nil {
			return err
		}
	}
	// A wrong header setting must roll back source replacement, not leave the
	// previously loaded dataset empty after TRUNCATE.
	if err := h.load(ctx, filepath.Join(h.dataset.Directory, "sample.csv"), false, false); err == nil {
		return fmt.Errorf("headerless load unexpectedly accepted sample headers")
	}
	if err := h.verify(ctx); err != nil {
		return err
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
