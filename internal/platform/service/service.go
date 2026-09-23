// Package service provides operational scaffolding, never business behavior.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// Definition describes one independently deployed service.
type Definition struct {
	Name        string
	DefaultPort int
}

// Address accepts an explicit host:port and otherwise binds only to loopback.
func Address(def Definition, override string) (string, error) {
	addr := override
	if addr == "" {
		addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(def.DefaultPort))
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return "", fmt.Errorf("PESARO_HTTP_ADDR must contain an explicit host and port")
	}
	value, err := strconv.Atoi(port)
	if err != nil || value < 1 || value > 65535 {
		return "", fmt.Errorf("PESARO_HTTP_ADDR port must be between 1 and 65535")
	}
	return addr, nil
}

// Handler has no business routes and cannot report business readiness.
func Handler(def Definition) http.Handler {
	mux := http.NewServeMux()
	add := func(path string, status int, body map[string]string) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				w.Header().Set("Allow", "GET, HEAD")
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(status)
			if r.Method != http.MethodHead {
				_ = json.NewEncoder(w).Encode(body)
			}
		})
	}
	add("/livez", http.StatusOK, map[string]string{"status": "alive"})
	add("/readyz", http.StatusServiceUnavailable, map[string]string{
		"status": "not_ready", "reason": "business_capability_not_implemented",
	})
	add("/info", http.StatusOK, map[string]string{
		"service": def.Name, "stage": "scaffold",
	})
	return mux
}

// Run starts one service and drains HTTP requests when ctx is cancelled.
func Run(ctx context.Context, def Definition) error {
	addr, err := Address(def, os.Getenv("PESARO_HTTP_ADDR"))
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen for %s: %w", def.Name, err)
	}
	slog.Info("service listening", "service", def.Name, "address", listener.Addr().String(), "stage", "scaffold", "ready", false)
	return serve(ctx, listener, Handler(def))
}

func serve(ctx context.Context, listener net.Listener, handler http.Handler) error {
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	select {
	case err := <-stopped:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			<-stopped
			return fmt.Errorf("drain server: %w", err)
		}
		err := <-stopped
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// Process configures logging and signal handling for each service binary.
func Process(run func(context.Context) error) int {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("service stopped", "error", err)
		return 1
	}
	return 0
}
