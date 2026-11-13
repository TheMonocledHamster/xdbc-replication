package main

// bench times the Fig. 9b systems on the loaded dataset: our transfer
// (PG -> PG and PG -> CSV), PostgreSQL's postgres_fdw ("native"), and
// jdbc_fdw ("jdbc"). The native and jdbc queries mirror the original
// authors' benchmark: COPY (SELECT * FROM <foreign table>) TO a file.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/transfer"
	"github.com/jackc/pgx/v5"
)

type benchResult struct {
	RunID    string  `json:"run_id"`
	Machine  string  `json:"machine,omitempty"`
	CPUs     int     `json:"cpus"`
	MemGB    float64 `json:"mem_gb"`
	Dataset  string  `json:"dataset"`
	System   string  `json:"system"`
	Sink     string  `json:"sink"`
	Round    int     `json:"round"`
	Seconds  float64 `json:"seconds"`
	Rows     int64   `json:"rows"`
	OK       bool    `json:"ok"`
	Note     string  `json:"note,omitempty"`
	JDBCMode string  `json:"jdbc_mode,omitempty"`
	Latency  string  `json:"latency_ms,omitempty"`
	RTTms    float64 `json:"source_rtt_ms,omitempty"`
	TwoNode  bool    `json:"two_node,omitempty"`
	Source   string  `json:"source_host,omitempty"`
	Time     string  `json:"time"`
}

type benchRun struct {
	h            harness
	rounds       int
	systems      map[string]bool
	sinks        map[string]bool
	csvClientURL string
	timeout      time.Duration
	out          *os.File
	base         benchResult
	sourceRows   int64
}

func (h harness) bench(ctx context.Context) error {
	rounds, err := strconv.Atoi(transfer.Env("BENCH_ROUNDS", "3"))
	if err != nil || rounds < 1 {
		return fmt.Errorf("BENCH_ROUNDS must be a positive integer")
	}
	timeout, err := time.ParseDuration(transfer.Env("BENCH_QUERY_TIMEOUT", "20m"))
	if err != nil {
		return fmt.Errorf("BENCH_QUERY_TIMEOUT: %w", err)
	}
	out, err := os.OpenFile(transfer.Env("BENCH_OUT", "/results/bench.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	b := benchRun{
		h: h, rounds: rounds, timeout: timeout, out: out,
		systems:      set(transfer.Env("BENCH_SYSTEMS", "ours,native")),
		sinks:        set(transfer.Env("BENCH_SINKS", "pg,csv")),
		csvClientURL: transfer.Env("CLIENT_CSV_URL", "http://client-csv:8080"),
		base: benchResult{
			RunID: transfer.Env("BENCH_RUN_ID", time.Now().UTC().Format("20060102T150405Z")), Machine: os.Getenv("BENCH_MACHINE"),
			CPUs: runtime.NumCPU(), MemGB: memGB(), Dataset: h.dataset.Name,
			JDBCMode: os.Getenv("BENCH_JDBC_MODE"), Latency: os.Getenv("BENCH_LATENCY_MS"),
			TwoNode: os.Getenv("BENCH_TWO_NODE") != "", Source: os.Getenv("SOURCE_HOST"),
		},
	}
	b.base.RTTms, _ = strconv.ParseFloat(os.Getenv("BENCH_SOURCE_RTT_MS"), 64)
	if err := h.source.QueryRow(ctx, "SELECT count(*) FROM "+h.config.Relation()).Scan(&b.sourceRows); err != nil {
		return err
	}
	fmt.Printf("bench %s: %d source rows, %d rounds, systems=%s sinks=%s\n", h.dataset.Name, b.sourceRows, rounds, transfer.Env("BENCH_SYSTEMS", "ours,native"), transfer.Env("BENCH_SINKS", "pg,csv"))
	if b.systems["native"] {
		if err := b.setupNative(ctx); err != nil {
			return fmt.Errorf("native setup: %w", err)
		}
	}
	if b.systems["jdbc"] {
		if err := b.setupJDBC(ctx); err != nil {
			return fmt.Errorf("jdbc setup: %w", err)
		}
	}
	// Order per round: ours -> CSV first, so a broken setup (for example node A's
	// server started for another dataset) stops the run in seconds; ours -> PG last,
	// so the target table it leaves behind is what the SHA-256 verification checks.
	for round := 1; round <= rounds; round++ {
		if b.systems["ours"] && b.sinks["csv"] {
			if !b.record(round, "ours", "csv", func() (int64, error) { return b.ours(ctx, b.csvClientURL) }) && round == 1 {
				return b.stopEarly()
			}
		}
		for _, system := range []string{"native", "jdbc"} {
			if !b.systems[system] {
				continue
			}
			if b.sinks["pg"] {
				b.record(round, system, "pg", func() (int64, error) { return b.foreignToTable(ctx, system) })
			}
			if b.sinks["csv"] {
				b.record(round, system, "csv", func() (int64, error) { return b.foreignToFile(ctx, system) })
			}
		}
		if b.systems["ours"] && b.sinks["pg"] {
			if !b.record(round, "ours", "pg", func() (int64, error) { return b.ours(ctx, h.clientURL) }) && round == 1 && !b.sinks["csv"] {
				return b.stopEarly()
			}
		}
	}
	// Empty the server-side CSV file the native/jdbc runs wrote.
	if b.systems["native"] || b.systems["jdbc"] {
		h.sink.Exec(ctx, "COPY (SELECT 1 WHERE false) TO '/tmp/bench.out'")
	}
	if b.systems["ours"] && b.sinks["pg"] {
		// Record the SHA-256 check as its own line so reports can show it.
		err := h.verify(ctx)
		r := b.base
		r.System, r.Sink, r.Rows, r.OK = "verify", "pg", b.sourceRows, err == nil
		r.Time = time.Now().UTC().Format(time.RFC3339)
		if err != nil {
			r.Note = truncate(err.Error(), 200)
		}
		line, _ := json.Marshal(r)
		b.out.Write(append(line, '\n'))
		return err
	}
	return nil
}

// stopEarly ends the run when our very first transfer fails: every later number
// would come from a broken setup.
func (b *benchRun) stopEarly() error {
	hint := "see the error above"
	if os.Getenv("SOURCE_HOST") != "" {
		hint = fmt.Sprintf("check that node A started bench-server with DATASET=%s", b.h.dataset.Name)
	}
	return fmt.Errorf("our first transfer failed, so the run stopped early (%s)", hint)
}

func (b *benchRun) record(round int, system, sink string, run func() (int64, error)) bool {
	start := time.Now()
	rows, err := run()
	r := b.base
	r.System, r.Sink, r.Round = system, sink, round
	r.Seconds = float64(time.Since(start).Milliseconds()) / 1000
	r.Rows = rows
	r.OK = err == nil && rows == b.sourceRows
	r.Time = time.Now().UTC().Format(time.RFC3339)
	switch {
	case err != nil && strings.Contains(err.Error(), "statement timeout"):
		r.Note = "timeout"
	case err != nil:
		r.Note = truncate(err.Error(), 200)
	case !r.OK:
		r.Note = fmt.Sprintf("copied %d of %d rows", rows, b.sourceRows)
	}
	line, _ := json.Marshal(r)
	b.out.Write(append(line, '\n'))
	status := "ok"
	if !r.OK {
		status = "FAILED: " + r.Note
	}
	fmt.Printf("  round %d  %-6s %s -> %-3s %8.2f s  %d rows  %s\n", round, system, "PG", strings.ToUpper(sink), r.Seconds, rows, status)
	return r.OK
}

func (b *benchRun) ours(ctx context.Context, clientURL string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, clientURL+"/transfer", nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("transfer returned %d: %s", resp.StatusCode, body)
	}
	var result transfer.Result
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}
	return result.Rows, nil
}

func (b *benchRun) foreignToTable(ctx context.Context, schema string) (int64, error) {
	rel := b.h.config.Relation()
	if _, err := b.h.sink.Exec(ctx, "TRUNCATE "+rel); err != nil {
		return 0, err
	}
	return b.timed(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", rel, pgx.Identifier{schema, b.h.config.Table}.Sanitize()))
}

func (b *benchRun) foreignToFile(ctx context.Context, schema string) (int64, error) {
	return b.timed(ctx, fmt.Sprintf("COPY (SELECT * FROM %s) TO '/tmp/bench.out' WITH CSV HEADER", pgx.Identifier{schema, b.h.config.Table}.Sanitize()))
}

func (b *benchRun) timed(ctx context.Context, sql string) (int64, error) {
	if _, err := b.h.sink.Exec(ctx, fmt.Sprintf("SET statement_timeout = %d", b.timeout.Milliseconds())); err != nil {
		return 0, err
	}
	defer b.h.sink.Exec(context.Background(), "RESET statement_timeout")
	tag, err := b.h.sink.Exec(ctx, sql)
	return tag.RowsAffected(), err
}

// setupNative mirrors pg_xdbc_fdw/experiments/setup_fdw.sql: postgres_fdw with
// default options (fetch_size 100), pointed at the source database.
func (b *benchRun) setupNative(ctx context.Context) error {
	src, err := pgx.ParseConfig(os.Getenv("SOURCE_DATABASE_URL"))
	if err != nil {
		return err
	}
	stmts := []string{
		"CREATE EXTENSION IF NOT EXISTS postgres_fdw",
		"DROP SERVER IF EXISTS bench_native CASCADE",
		fmt.Sprintf("CREATE SERVER bench_native FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %s, port %s, dbname %s)",
			quote(src.Host), quote(strconv.Itoa(int(src.Port))), quote(src.Database)),
		fmt.Sprintf("CREATE USER MAPPING FOR CURRENT_USER SERVER bench_native OPTIONS (user %s, password %s)", quote(src.User), quote(src.Password)),
		"CREATE SCHEMA IF NOT EXISTS native",
		fmt.Sprintf("DROP FOREIGN TABLE IF EXISTS native.%s", pgx.Identifier{b.h.config.Table}.Sanitize()),
		fmt.Sprintf("IMPORT FOREIGN SCHEMA %s LIMIT TO (%s) FROM SERVER bench_native INTO native",
			pgx.Identifier{b.h.config.Schema}.Sanitize(), pgx.Identifier{b.h.config.Table}.Sanitize()),
	}
	return b.exec(ctx, stmts)
}

// setupJDBC mirrors pg_xdbc_fdw/experiments/setup_fdw.sql:37-50 (jdbc_fdw,
// PostgreSQL JDBC 42.7.4, querytimeout 1200). Needs the bench/jdbc image.
func (b *benchRun) setupJDBC(ctx context.Context) error {
	var available bool
	if err := b.h.sink.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'jdbc_fdw')").Scan(&available); err != nil {
		return err
	}
	if !available {
		return fmt.Errorf("the target database has no jdbc_fdw; use the bench service (docker compose run --rm bench)")
	}
	src, err := pgx.ParseConfig(os.Getenv("SOURCE_DATABASE_URL"))
	if err != nil {
		return err
	}
	url := fmt.Sprintf("jdbc:postgresql://%s:%d/%s", src.Host, src.Port, src.Database)
	if os.Getenv("BENCH_JDBC_MODE") == "stream" {
		// Deviation from the original: fetch 10,000 rows at a time instead of the
		// whole table (bench/jdbc/fetch-size.patch turns autocommit off for this).
		url += "?defaultRowFetchSize=10000"
	}
	opts := fmt.Sprintf("drivername 'org.postgresql.Driver', url %s, querytimeout '1200', jarfile '/opt/jdbc/postgresql-42.7.4.jar'", quote(url))
	if heap := os.Getenv("BENCH_JDBC_HEAP_MB"); heap != "" {
		opts += ", maxheapsize " + quote(heap)
	}
	stmts := []string{
		"CREATE EXTENSION IF NOT EXISTS jdbc_fdw",
		"DROP SERVER IF EXISTS bench_jdbc CASCADE",
		"CREATE SERVER bench_jdbc FOREIGN DATA WRAPPER jdbc_fdw OPTIONS (" + opts + ")",
		fmt.Sprintf("CREATE USER MAPPING FOR CURRENT_USER SERVER bench_jdbc OPTIONS (username %s, password %s)", quote(src.User), quote(src.Password)),
		"CREATE SCHEMA IF NOT EXISTS jdbc",
	}
	if err := b.exec(ctx, stmts); err != nil {
		return err
	}
	// jdbc_fdw has no IMPORT FOREIGN SCHEMA for this case; copy the local table's columns.
	var cols string
	err = b.h.sink.QueryRow(ctx, `SELECT string_agg(format('%I %s', attname, format_type(atttypid, atttypmod)), ', ' ORDER BY attnum)
		FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped`, b.h.config.Relation()).Scan(&cols)
	if err != nil {
		return err
	}
	return b.exec(ctx, []string{fmt.Sprintf("CREATE FOREIGN TABLE jdbc.%s (%s) SERVER bench_jdbc OPTIONS (table_name %s)",
		pgx.Identifier{b.h.config.Table}.Sanitize(), cols, quote(b.h.config.Table))})
}

func (b *benchRun) exec(ctx context.Context, stmts []string) error {
	for _, s := range stmts {
		if _, err := b.h.sink.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", strings.SplitN(s, " OPTIONS", 2)[0], err)
		}
	}
	return nil
}

// benchSummary prints the median of each system/sink for one run as a table.
func benchSummary() error {
	path := transfer.Env("BENCH_OUT", "/results/bench.jsonl")
	runID := os.Getenv("BENCH_RUN_ID")
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	type key struct{ dataset, system, sink string }
	times := map[key][]float64{}
	failed := map[key]string{}
	var machine string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var r benchResult
		if json.Unmarshal(scanner.Bytes(), &r) != nil || (runID != "" && r.RunID != runID) {
			continue
		}
		machine = fmt.Sprintf("%s %d CPUs, %.0f GB visible to Docker", r.Machine, r.CPUs, r.MemGB)
		if r.TwoNode {
			machine += fmt.Sprintf(", source on another machine at %s (round trip %.2f ms)", r.Source, r.RTTms)
		}
		k := key{r.Dataset, r.System, r.Sink}
		if r.OK {
			times[k] = append(times[k], r.Seconds)
		} else if failed[k] == "" {
			failed[k] = r.Note
		}
	}
	columns := []struct{ system, sink, label string }{
		{"ours", "pg", "Ours PG→PG"}, {"native", "pg", "Native PG→PG"}, {"jdbc", "pg", "JDBC PG→PG"},
		{"ours", "csv", "Ours PG→CSV"}, {"native", "csv", "Native PG→CSV"}, {"jdbc", "csv", "JDBC PG→CSV"},
	}
	var used []int
	datasets := map[string]bool{}
	for k := range times {
		datasets[k.dataset] = true
	}
	for k := range failed {
		datasets[k.dataset] = true
	}
	for i, c := range columns {
		for d := range datasets {
			k := key{d, c.system, c.sink}
			if len(times[k]) > 0 || failed[k] != "" {
				used = append(used, i)
				break
			}
		}
	}
	fmt.Printf("\nMedian seconds, run %s (%s)\n\n| dataset |", runID, strings.TrimSpace(machine))
	for _, i := range used {
		fmt.Printf(" %s |", columns[i].label)
	}
	fmt.Print("\n|---|")
	for range used {
		fmt.Print("---|")
	}
	fmt.Println()
	for _, d := range []string{"lineitem", "acs", "iot", "icu"} {
		if !datasets[d] {
			continue
		}
		fmt.Printf("| %s |", d)
		for _, i := range used {
			k := key{d, columns[i].system, columns[i].sink}
			switch {
			case len(times[k]) > 0:
				fmt.Printf(" %.1f |", median(times[k]))
			case failed[k] == "timeout":
				fmt.Print(" timeout |")
			case failed[k] != "":
				fmt.Print(" failed |")
			default:
				fmt.Print(" – |")
			}
		}
		fmt.Println()
	}
	return scanner.Err()
}

func median(v []float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func memGB() float64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	var kb float64
	fmt.Sscanf(strings.TrimPrefix(strings.SplitN(string(b), "\n", 2)[0], "MemTotal:"), "%f", &kb)
	return float64(int(kb/1024/1024*10+0.5)) / 10
}

func set(csv string) map[string]bool {
	m := map[string]bool{}
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			m[s] = true
		}
	}
	return m
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
