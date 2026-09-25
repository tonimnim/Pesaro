package app

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	ledgerevent "github.com/tonimnim/Pesaro/contracts/events/ledger/v1"
	"github.com/tonimnim/Pesaro/internal/platform/eventtransport"
	"github.com/tonimnim/Pesaro/services/reconciliation/internal/inbox"
)

type Config struct {
	Mode             string                  `json:"mode"`
	Address          string                  `json:"address"`
	ProducerIdentity string                  `json:"producer_identity"`
	Books            []string                `json:"books"`
	TLS              eventtransport.TLSFiles `json:"tls"`
}

func loadConfig(path string) (Config, *tls.Config, error) {
	var config Config
	f, err := os.Open(path)
	if err != nil {
		return config, nil, eventtransport.ErrConfig
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return config, nil, eventtransport.ErrConfig
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var extra any
	if decoder.Decode(&config) != nil || decoder.Decode(&extra) != io.EOF || config.Mode != "synthetic" || !eventtransport.LoopbackAddress(config.Address) || len(config.Books) == 0 || len(config.Books) > 128 {
		return config, nil, eventtransport.ErrConfig
	}
	seen := map[string]bool{}
	for _, book := range config.Books {
		if !ledgerevent.ValidID(book) || seen[book] {
			return config, nil, eventtransport.ErrConfig
		}
		seen[book] = true
	}
	security, err := eventtransport.LoadTLS(path, config.TLS, true, config.ProducerIdentity)
	return config, security, err
}

type eventStore interface {
	Accept(context.Context, []byte) error
	Ready(context.Context) error
}

func eventHandler(store eventStore, config Config) http.Handler {
	books := map[string]bool{}
	for _, book := range config.Books {
		books[book] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if !eventtransport.Peer(r.TLS, config.ProducerIdentity) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if r.URL.Path == "/livez" || r.URL.Path == "/readyz" || r.URL.Path == "/info" {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			status := http.StatusOK
			if r.URL.Path == "/readyz" {
				ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
				defer cancel()
				if store.Ready(ctx) != nil {
					status = http.StatusServiceUnavailable
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			if r.Method != http.MethodHead {
				_, _ = io.WriteString(w, `{"service":"reconciliation","capability":"synthetic_event_inbox","production_ready":false}`)
			}
			return
		}
		if r.URL.Path != ledgerevent.Path {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" || r.URL.RawQuery != "" {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, ledgerevent.MaxBytes)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "event too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		event, _, err := ledgerevent.Decode(data)
		if err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		if !books[event.BookID] {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()
		err = store.Accept(ctx, data)
		if errors.Is(err, inbox.ErrConflict) {
			http.Error(w, "event identity conflict", http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, "event outcome unknown; retry original identity", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func runEvents(ctx context.Context, path, dsn string) error {
	config, security, err := loadConfig(path)
	if err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	store, err := inbox.Open(startup, dsn, false)
	if err != nil {
		return err
	}
	defer store.Close()
	if err = store.Ready(startup); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return errors.New("reconciliation event listener unavailable")
	}
	defer listener.Close()
	server := &http.Server{Handler: eventHandler(store, config), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.Serve(tls.NewListener(listener, security)) }()
	slog.Info("reconciliation event inbox listening", "stage", "synthetic_m1", "address", config.Address, "production_ready", false)
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if server.Shutdown(drain) != nil {
		_ = server.Close()
	}
	<-done
	return nil
}
