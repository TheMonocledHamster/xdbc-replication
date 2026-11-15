package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/TheMonocledHamster/xdbc-replication/internal/target"
	"github.com/TheMonocledHamster/xdbc-replication/internal/transfer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("client stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	c, err := transfer.LoadConfig(transfer.Env("CONFIG_PATH", "testdata/iot/transfer.json"))
	if err != nil {
		return err
	}
	timeout, err := transfer.Timeout()
	if err != nil {
		return err
	}
	url := os.Getenv("DATABASE_URL")
	sinkType := transfer.Env("SINK_TYPE", "postgres")
	csvPath := os.Getenv("CSV_OUTPUT_PATH")
	if sinkType != "postgres" && sinkType != "csv" {
		return errors.New("SINK_TYPE must be postgres or csv")
	}
	if sinkType == "postgres" && url == "" {
		return errors.New("DATABASE_URL is required for the PostgreSQL sink")
	}
	if sinkType == "csv" && csvPath == "" {
		return errors.New("CSV_OUTPUT_PATH is required for the CSV sink")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ready := func(ctx context.Context) error {
		if sinkType == "csv" {
			return nil
		}
		db, err := target.OpenPostgres(ctx, url)
		if err != nil {
			return err
		}
		return db.Close(context.Background())
	}
	run := func(request context.Context) (transfer.Result, error) {
		var result transfer.Result
		var err error
		address := transfer.Env("SERVER_ADDR", "localhost:9000")
		if sinkType == "csv" {
			result, err = transfer.ReceiveTransferCSV(request, address, csvPath, c)
		} else {
			result, err = transfer.ReceiveTransfer(request, address, url, c)
		}
		if err != nil {
			slog.Error("transfer failed", "error", err)
		} else {
			slog.Info("transfer complete", "rows", result.Rows, "elapsed_ms", result.ElapsedMS)
		}
		return result, err
	}
	slog.Info("client listening", "compression", c.Compression, "sink", sinkType)
	return transfer.RunHTTP(ctx, transfer.Env("HTTP_ADDR", ":8080"), transfer.Handler(timeout, ready, run))
}
