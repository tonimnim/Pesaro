package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/tonimnim/Pesaro/internal/platform/eventtransport"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/publisher"
	"github.com/tonimnim/Pesaro/services/ledger/internal/store/cockroach"
	"github.com/tonimnim/Pesaro/services/ledger/internal/transport"
	"google.golang.org/grpc"
)

// Runtime owns the configured synthetic Ledger, its pool and authenticated RPCs.
type Runtime struct {
	config  preparedConfig
	store   *cockroach.Store
	server  *grpc.Server
	serving atomic.Bool
	outbox  *eventtransport.Client
}

func Open(ctx context.Context, path, dsn string) (*Runtime, error) {
	config, err := loadConfig(path)
	if err != nil {
		return nil, err
	}
	if !runtimeURL(dsn) {
		return nil, errors.New("Ledger runtime requires its restricted database role and verified TLS")
	}
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	store, err := cockroach.Open(startup, dsn)
	if err != nil {
		return nil, err
	}
	if err = store.ReadyForBooks(startup, config.books); err != nil {
		store.Close()
		return nil, err
	}
	server, err := transport.NewGRPC(application.New(store, config.trust, nil), config.tls, config.rules)
	if err != nil {
		store.Close()
		return nil, err
	}
	r := &Runtime{config: config, store: store, server: server}
	if config.Outbox != nil {
		for _, book := range config.books {
			if err = store.BindOutboxConsumer(startup, book, config.Outbox.ConsumerIdentity); err != nil {
				r.Close()
				return nil, errors.New("Ledger outbox consumer binding requires review")
			}
		}
		r.outbox = eventtransport.NewClient(config.Outbox.Endpoint, config.outboxTLS)
	}
	return r, nil
}
func (r *Runtime) Close() {
	r.serving.Store(false)
	r.server.Stop()
	if r.outbox != nil {
		r.outbox.Close()
	}
	r.store.Close()
}

// Serve accepts already-bound listeners, allowing tests to use ephemeral ports.
// Callers own Close; Serve always drains/stops both listeners before returning.
func (r *Runtime) Serve(ctx context.Context, rpcListener, opsListener net.Listener) error {
	defer rpcListener.Close()
	defer opsListener.Close()
	if ctx.Err() != nil {
		return nil
	}
	worker, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	if r.outbox == nil {
		close(workerDone)
	} else {
		go func() {
			defer close(workerDone)
			var lastWarning time.Time
			_ = publisher.Run(worker, r.store, r.config.books, r.config.Outbox.Options, r.outbox.Send, func(error) {
				if time.Since(lastWarning) >= 30*time.Second {
					slog.Warn("ledger event delivery pending; original facts retained")
					lastWarning = time.Now()
				}
			})
		}()
	}
	defer func() { stopWorker(); <-workerDone }()
	httpServer := &http.Server{Handler: r.operations(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	stopped := make(chan error, 2)
	r.serving.Store(true)
	go func() { stopped <- r.server.Serve(rpcListener) }()
	go func() { stopped <- httpServer.Serve(opsListener) }()
	var result error
	completed := 0
	select {
	case <-ctx.Done():
	case err := <-stopped:
		completed = 1
		if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			result = err
		}
	}
	r.serving.Store(false)
	drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	grpcDone := make(chan struct{})
	go func() { r.server.GracefulStop(); close(grpcDone) }()
	if httpServer.Shutdown(drain) != nil {
		_ = httpServer.Close()
	}
	select {
	case <-grpcDone:
	case <-drain.Done():
		r.server.Stop()
		<-grpcDone
	}
	for ; completed < 2; completed++ {
		<-stopped
	}
	return result
}
func (r *Runtime) operations() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/livez" && req.URL.Path != "/readyz" && req.URL.Path != "/info" {
			http.NotFound(w, req)
			return
		}
		if req.Method != http.MethodGet && req.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		status := http.StatusOK
		body := map[string]string{"status": "alive"}
		switch req.URL.Path {
		case "/info":
			body = map[string]string{"product": "Pesar", "service": "ledger", "stage": "synthetic_m1", "production_ready": "false"}
		case "/readyz":
			probe, cancel := context.WithTimeout(req.Context(), 2*time.Second)
			defer cancel()
			if !r.serving.Load() || r.store.ReadyForBooks(probe, r.config.books) != nil {
				status = http.StatusServiceUnavailable
				body = map[string]string{"status": "not_ready", "capability": "synthetic_ledger"}
			} else {
				body = map[string]string{"status": "ready", "capability": "synthetic_ledger"}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(status)
		if req.Method != http.MethodHead {
			_ = json.NewEncoder(w).Encode(body)
		}
	})
	return mux
}
func runConfigured(ctx context.Context, path, dsn string) error {
	runtime, err := Open(ctx, path, dsn)
	if err != nil {
		return err
	}
	defer runtime.Close()
	rpcListener, err := net.Listen("tcp", runtime.config.GRPCAddress)
	if err != nil {
		return errors.New("Ledger RPC listener unavailable")
	}
	defer rpcListener.Close()
	opsListener, err := net.Listen("tcp", runtime.config.HTTPAddress)
	if err != nil {
		return errors.New("Ledger operations listener unavailable")
	}
	defer opsListener.Close()
	slog.Info("ledger listening", "product", "Pesar", "stage", "synthetic_m1", "grpc", rpcListener.Addr().String(), "http", opsListener.Addr().String(), "production_ready", false)
	return runtime.Serve(ctx, rpcListener, opsListener)
}
