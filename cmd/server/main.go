package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/TheMonocledHamster/xdbc-replication/internal/transfer"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
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
	if url == "" {
		return errors.New("DATABASE_URL is required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	listener, err := net.Listen("tcp", transfer.Env("LISTEN_ADDR", ":9000"))
	if err != nil {
		return err
	}
	defer listener.Close()
	g, ctx := errgroup.WithContext(ctx)
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	ready := func(ctx context.Context) error {
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			return err
		}
		defer conn.Close(context.Background())
		return conn.Ping(ctx)
	}
	g.Go(func() error {
		return transfer.RunHTTP(ctx, transfer.Env("HTTP_ADDR", ":8080"), transfer.Handler(timeout, ready, nil))
	})
	g.Go(func() error {
		slog.Info("server listening", "address", listener.Addr(), "compression", c.Compression)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			session, finish := context.WithTimeout(ctx, timeout)
			err = transfer.ServeTransfer(session, conn, url, c)
			finish()
			if err != nil {
				slog.Error("source transfer failed", "error", err)
			} else {
				slog.Info("source transfer sent")
			}
		}
	})
	return g.Wait()
}
