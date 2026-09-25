package eventtransport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestAcknowledgementAndRedirectPolicy(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1); w.WriteHeader(204) }))
	defer target.Close()
	var status atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status.Load() == 307 {
			w.Header().Set("Location", target.URL)
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	roots.AddCert(target.Certificate())
	client := NewClient(server.URL, &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots})
	defer client.Close()
	for _, code := range []int32{200, 202, 204, 307, 409, 503} {
		status.Store(code)
		err := client.Send(context.Background(), []byte(`{}`))
		if (err == nil) != (code == 204) {
			t.Fatal("incorrect acknowledgement", code, err)
		}
	}
	if redirected.Load() != 0 {
		t.Fatal("event was forwarded by a redirect")
	}
}

func TestSyntheticEndpointPolicy(t *testing.T) {
	path := "/internal/events/ledger/v1"
	for _, endpoint := range []string{"http://127.0.0.1:8443" + path, "https://localhost:8443" + path, "https://10.0.0.1:8443" + path, "https://127.0.0.1:8443" + path + "?extra=true", "https://user@127.0.0.1:8443" + path, "https://127.0.0.1:8443/other"} {
		if Endpoint(endpoint, path) {
			t.Fatal("unsafe/ambiguous synthetic endpoint", endpoint)
		}
	}
	if !Endpoint("https://127.0.0.1:8443"+path, path) {
		t.Fatal("valid loopback endpoint rejected")
	}
}
