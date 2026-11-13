package main

// bench-all is the one-command Fig. 9b benchmark (docker compose run --rm bench).
// Per dataset it downloads the CSV if missing, loads the source table, starts our
// server and two clients (PG and CSV sinks) as processes in this container, times
// every system, and verifies our PG -> PG copy. Only Docker is needed on the host.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/dataset"
	"github.com/TheMonocledHamster/xdbc-replication/internal/transfer"
	"github.com/jackc/pgx/v5"
)

const pageURL = "https://claude.ai/artifact/6d7yED3FfpTFSj7dHxYGBa"

func benchAll() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	datasets := strings.Fields(strings.ReplaceAll(transfer.Env("DATASETS", "iot"), ",", " "))
	if len(datasets) == 1 && datasets[0] == "all" {
		datasets = []string{"lineitem", "acs", "iot", "icu"}
	}
	systems := "ours,native"
	switch mode := os.Getenv("JDBC"); mode {
	case "", "off":
	case "faithful", "stream":
		systems += ",jdbc"
	default:
		return fmt.Errorf("JDBC must be off, faithful or stream, not %q", mode)
	}
	// Two machines: the source database and our server run on node A (SOURCE_HOST),
	// everything else here on node B, so every system's traffic crosses the network.
	if host := os.Getenv("SOURCE_HOST"); host != "" {
		if len(datasets) != 1 {
			return fmt.Errorf("with SOURCE_HOST, run one dataset at a time (node A's server is started for one dataset)")
		}
		os.Setenv("SOURCE_DATABASE_URL", fmt.Sprintf("postgres://postgres:postgres@%s:%s/xdbc_test?sslmode=disable", host, transfer.Env("SOURCE_PORT", "5435")))
		os.Setenv("BENCH_TWO_NODE", "1")
	}
	runID := transfer.Env("BENCH_RUN_ID", time.Now().UTC().Format("20060102T150405Z"))
	out := filepath.Join("/results", runID+".jsonl")
	// Friendly option names (docker compose run -e ROUNDS=1 ...) map onto the bench settings.
	for friendly, setting := range map[string]string{"ROUNDS": "BENCH_ROUNDS", "SINKS": "BENCH_SINKS",
		"MACHINE": "BENCH_MACHINE", "JDBC_HEAP_MB": "BENCH_JDBC_HEAP_MB", "QUERY_TIMEOUT": "BENCH_QUERY_TIMEOUT"} {
		if v := os.Getenv(friendly); v != "" {
			os.Setenv(setting, v)
		}
	}
	for k, v := range map[string]string{
		"BENCH_RUN_ID": runID, "BENCH_OUT": out, "BENCH_SYSTEMS": systems,
		"BENCH_JDBC_MODE": os.Getenv("JDBC"), "CLIENT_CSV_URL": "http://127.0.0.1:8092",
	} {
		os.Setenv(k, v)
	}
	if err := os.MkdirAll("/results", 0o755); err != nil {
		return err
	}
	defer chownLikeParent(out)

	mode := "one machine (databases, our server and clients all here)"
	if host := os.Getenv("SOURCE_HOST"); host != "" {
		mode = "two machines: source database and our server on " + host + ", everything else here"
	}
	fmt.Printf("Fig. 9b benchmark, run %s\n  MODE:     %s\n  datasets: %s\n  systems:  %s\n  rounds:   %s, sinks: %s\n",
		runID, mode, strings.Join(datasets, " "), systems, transfer.Env("BENCH_ROUNDS", "3"), transfer.Env("BENCH_SINKS", "pg,csv"))
	for _, name := range datasets {
		if err := benchDataset(ctx, name); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if err := benchSummary(); err != nil {
		return err
	}
	fmt.Printf("\nRaw results: bench-results/%s.jsonl\nTo see them as the Fig. 9b chart, open %s and drop that file on the page.\n", runID, pageURL)
	return nil
}

func benchDataset(ctx context.Context, name string) error {
	configPath := filepath.Join("testdata", name, "transfer.json")
	d, err := dataset.Load(configPath)
	if err != nil {
		return fmt.Errorf("unknown dataset (choose from lineitem acs iot icu): %w", err)
	}
	dataPath := filepath.Join("/data", d.Name+".csv")
	if err := ensureData(ctx, d, dataPath); err != nil {
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
	h := harness{d, d.Config, source, sink, "http://127.0.0.1:8091", "http://127.0.0.1:8090", "postgres", ""}

	fmt.Printf("\n== %s: loading source table\n", name)
	if err := h.load(ctx, dataPath, d.FullHeader, false); err != nil {
		return err
	}
	if _, err := source.Exec(ctx, "VACUUM ANALYZE "+d.Config.Relation()); err != nil {
		return err
	}

	rtt, err := roundTrip(ctx, source)
	if err != nil {
		return err
	}
	os.Setenv("BENCH_SOURCE_RTT_MS", fmt.Sprintf("%.3f", rtt))
	fmt.Printf("== %s: round trip to the source database %.3f ms\n", name, rtt)

	fmt.Printf("== %s: starting our server and clients\n", name)
	exports := filepath.Join("/exports", d.Name+".csv")
	defer os.Remove(exports)
	common := []string{"CONFIG_PATH=" + configPath, "COMPRESSION=" + transfer.Env("COMPRESSION", "snappy"), "TRANSFER_TIMEOUT=60m"}
	serverAddr, serverHealth := "127.0.0.1:9000", "http://127.0.0.1:8090"
	if host := os.Getenv("SOURCE_HOST"); host != "" {
		serverAddr, serverHealth = host+":9000", "http://"+host+":8090"
	}
	procs := []struct {
		name, bin string
		env       []string
		health    string
	}{
		{"server", "server", []string{"DATABASE_URL=" + os.Getenv("SOURCE_DATABASE_URL"), "LISTEN_ADDR=127.0.0.1:9000", "HTTP_ADDR=127.0.0.1:8090"}, "http://127.0.0.1:8090"},
		{"client-pg", "client", []string{"DATABASE_URL=" + os.Getenv("TARGET_DATABASE_URL"), "SERVER_ADDR=" + serverAddr, "SINK_TYPE=postgres", "HTTP_ADDR=127.0.0.1:8091"}, "http://127.0.0.1:8091"},
		{"client-csv", "client", []string{"SERVER_ADDR=" + serverAddr, "SINK_TYPE=csv", "CSV_OUTPUT_PATH=" + exports, "HTTP_ADDR=127.0.0.1:8092"}, "http://127.0.0.1:8092"},
	}
	if os.Getenv("SOURCE_HOST") != "" {
		// Node A runs our server; just make sure it is up.
		procs = procs[1:]
		if err := waitHealthy(ctx, serverHealth, 60*time.Second); err != nil {
			return fmt.Errorf("our server on node A is not reachable at %s; start it there with: DATASET=%s BENCH_BIND_IP=0.0.0.0 docker compose --profile bench up -d --build bench-source bench-server (%w)", serverHealth, name, err)
		}
		fmt.Printf("   using our server on node A (%s); it must have been started with DATASET=%s\n", serverAddr, name)
	}
	var running []*exec.Cmd
	defer func() {
		for _, c := range running {
			c.Process.Signal(syscall.SIGTERM)
		}
		for _, c := range running {
			c.Wait()
		}
	}()
	for _, p := range procs {
		log, err := os.Create(filepath.Join(os.TempDir(), p.name+".log"))
		if err != nil {
			return err
		}
		defer log.Close()
		c := exec.Command(p.bin)
		c.Env = append(append(os.Environ(), common...), p.env...)
		c.Stdout, c.Stderr = log, log
		if err := c.Start(); err != nil {
			return fmt.Errorf("start %s: %w", p.name, err)
		}
		running = append(running, c)
		if err := waitHealthy(ctx, p.health, 60*time.Second); err != nil {
			return fmt.Errorf("%s did not start (see %s): %w", p.name, log.Name(), err)
		}
	}

	fmt.Printf("== %s: timing\n", name)
	return h.bench(ctx)
}

// roundTrip is the median time of a trivial query to the source database: the
// network delay every postgres_fdw fetch pays.
func roundTrip(ctx context.Context, db *pgx.Conn) (float64, error) {
	var times []float64
	for i := 0; i < 31; i++ {
		start := time.Now()
		if _, err := db.Exec(ctx, "SELECT 1"); err != nil {
			return 0, err
		}
		times = append(times, float64(time.Since(start).Microseconds())/1000)
	}
	return median(times), nil
}

// ensureData downloads the dataset from the authors' TU Berlin share if it is missing.
func ensureData(ctx context.Context, d dataset.Dataset, path string) error {
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		return nil
	}
	fmt.Printf("\n== %s: downloading from the authors' TU Berlin share (first run only)\n", d.Name)
	part := path + ".part"
	f, err := os.Create(part)
	if err != nil {
		return err
	}
	if err := d.Fetch(ctx, f); err != nil {
		f.Close()
		os.Remove(part)
		return fmt.Errorf("download: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(part, path)
}

func waitHealthy(ctx context.Context, url string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		if err := health(ctx, url); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// chownLikeParent gives a file written as root to the owner of its directory,
// so results in a bind-mounted folder belong to the host user.
func chownLikeParent(path string) {
	info, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		os.Chown(path, int(st.Uid), int(st.Gid))
	}
}
