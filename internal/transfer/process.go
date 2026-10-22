package transfer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

func Env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// Timeout reads a positive transfer duration; smoke runs default to one minute.
func Timeout() (time.Duration, error) {
	timeout, err := time.ParseDuration(Env("TRANSFER_TIMEOUT", "1m"))
	if err != nil || timeout <= 0 {
		return 0, fmt.Errorf("TRANSFER_TIMEOUT must be a positive duration, such as 1m or 30m")
	}
	return timeout, nil
}

// RunHTTP shuts down handlers before the process exits, cancelling in-flight
// transfers through the process context supplied by each entrypoint.
func RunHTTP(ctx context.Context, address string, handler http.Handler) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			server.Close()
		}
	}()
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		<-done
		return nil
	}
	return err
}
