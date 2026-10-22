package transfer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandlerSerializesAndRecovers(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	calls := 0
	handler := Handler(time.Second, func(context.Context) error { return nil }, func(context.Context) (Result, error) {
		calls++
		if calls == 1 {
			close(started)
			<-release
			return Result{}, errors.New("sink unavailable")
		}
		return Result{Rows: 128}, nil
	})
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/transfer", nil))
		done <- w
	}()
	<-started
	busy := httptest.NewRecorder()
	handler.ServeHTTP(busy, httptest.NewRequest(http.MethodPost, "/transfer", nil))
	if busy.Code != http.StatusConflict {
		t.Fatalf("busy status: %d", busy.Code)
	}
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("health status: %d", ready.Code)
	}
	close(release)
	if w := <-done; w.Code != http.StatusInternalServerError {
		t.Fatalf("failure status: %d", w.Code)
	}
	retry := httptest.NewRecorder()
	handler.ServeHTTP(retry, httptest.NewRequest(http.MethodPost, "/transfer", nil))
	if retry.Code != http.StatusOK {
		t.Fatalf("retry: %d %s", retry.Code, retry.Body.String())
	}
}
func TestHandlerCancellationAndDeadline(t *testing.T) {
	for _, cancelRequest := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "cancellation"}[cancelRequest], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := Handler(10*time.Millisecond, func(context.Context) error { return nil }, func(ctx context.Context) (Result, error) {
				if cancelRequest {
					cancel()
				}
				<-ctx.Done()
				return Result{}, ctx.Err()
			})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/transfer", nil).WithContext(ctx))
			if w.Code != http.StatusGatewayTimeout {
				t.Fatalf("status: %d", w.Code)
			}
		})
	}
}
