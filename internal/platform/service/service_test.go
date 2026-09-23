package service

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestScaffoldCannotAdvertisePaymentReadiness(t *testing.T) {
	handler := Handler(Definition{Name: "ledger", DefaultPort: 8105})
	cases := []struct {
		path   string
		status int
	}{
		{"/livez", 200}, {"/readyz", 503}, {"/info", 200},
		{"/v1/payments", 404}, {"/readyz/anything", 404},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			result := httptest.NewRecorder()
			handler.ServeHTTP(result, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if result.Code != tc.status {
				t.Fatalf("status = %d, want %d", result.Code, tc.status)
			}
			if tc.path == "/readyz" {
				var body map[string]string
				if err := json.Unmarshal(result.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if body["reason"] != "business_capability_not_implemented" {
					t.Fatalf("unexpected readiness: %v", body)
				}
			}
		})
	}
	result := httptest.NewRecorder()
	handler.ServeHTTP(result, httptest.NewRequest(http.MethodPost, "/livez", nil))
	if result.Code != 405 || result.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("unsupported method accepted: %d", result.Code)
	}
}

func TestExplicitBindingAndInvalidConfiguration(t *testing.T) {
	def := Definition{Name: "ledger", DefaultPort: 8105}
	got, err := Address(def, "")
	if err != nil || got != "127.0.0.1:8105" {
		t.Fatalf("unsafe default %q, %v", got, err)
	}
	for _, addr := range []string{":8080", "localhost", "127.0.0.1:0", "127.0.0.1:-1", "127.0.0.1:65536", "127.0.0.1:http"} {
		if _, err := Address(def, addr); err == nil {
			t.Errorf("accepted invalid address %q", addr)
		}
	}
	for _, addr := range []string{"0.0.0.0:8080", "[::1]:8080"} {
		if _, err := Address(def, addr); err != nil {
			t.Errorf("rejected explicit address %q: %v", addr, err)
		}
	}
}

func TestServerServesAndClosesListenerOnCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- serve(ctx, listener, Handler(Definition{Name: "payments"})) }()
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatalf("readiness = %d", response.StatusCode)
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("server did not drain")
	}
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err == nil {
		_ = connection.Close()
		t.Fatal("listener still accepts connections after shutdown")
	}
}
