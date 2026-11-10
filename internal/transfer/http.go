package transfer

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Handler serializes transfers; no work queue or background job state is needed.
func Handler(timeout time.Duration, ready func(context.Context) error, run func(context.Context) (Result, error)) http.Handler {
	mux := http.NewServeMux()
	var active sync.Mutex
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	if run != nil {
		mux.HandleFunc("POST /transfer", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if !active.TryLock() {
				w.WriteHeader(http.StatusConflict)
				json.NewEncoder(w).Encode(map[string]string{"error": "transfer already running"})
				return
			}
			defer active.Unlock()
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			result, err := run(ctx)
			if err != nil {
				status := http.StatusInternalServerError
				if ctx.Err() != nil {
					status = http.StatusGatewayTimeout
				}
				w.WriteHeader(status)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			json.NewEncoder(w).Encode(result)
		})
	}
	return mux
}
