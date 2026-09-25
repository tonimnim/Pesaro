package app

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// All credentials are generated per test and confined to t.TempDir.
type testPKI struct {
	config                Config
	path                  string
	clients               map[string]*tls.Config
	grantKey, evidenceKey ed25519.PrivateKey
}

func pki(t *testing.T, book string) testPKI {
	t.Helper()
	dir := t.TempDir()
	caPub, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Pesar isolated test CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPub, caKey)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("CA")
	}
	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cert := func(name, identity string, server bool, serial int64) (tls.Certificate, string, string) {
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			leaf.DNSNames = []string{"localhost"}
			leaf.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}
		if identity != "" {
			u, err := url.Parse(identity)
			if err != nil {
				t.Fatal(err)
			}
			leaf.URIs = []*url.URL{u}
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		cp := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		kp := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		pair, err := tls.X509KeyPair(cp, kp)
		if err != nil {
			t.Fatal(err)
		}
		return pair, write(name+".crt", cp), write(name+".key", kp)
	}
	_, serverCert, serverKey := cert("server", "", true, 2)
	grants, gkey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	evidence, ekey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	result := testPKI{path: filepath.Join(dir, "ledger.json"), clients: map[string]*tls.Config{}, grantKey: gkey, evidenceKey: ekey}
	result.config = Config{Mode: "synthetic", TLS: TLSFiles{serverCert, serverKey, write("ca.crt", caPEM)}, GrantKeys: map[string]string{"grant-test": base64.RawURLEncoding.EncodeToString(grants)}, EvidenceKeys: map[string]string{"evidence-test": base64.RawURLEncoding.EncodeToString(evidence)}}
	for i, name := range []string{"payments", "reader", "outsider", "no-uri"} {
		identity := "spiffe://pesar.test/" + name
		if name == "no-uri" {
			identity = ""
		}
		pair, _, _ := cert(name, identity, false, int64(i+3))
		result.clients[name] = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{pair}, ServerName: "localhost"}
		if name == "payments" || name == "reader" {
			permissions := []string{"read"}
			if name == "payments" {
				permissions = []string{"spend", "resolve", "control", "provision", "read", "verify"}
			}
			result.config.Callers = append(result.config.Callers, CallerRule{Identity: identity, Books: []string{book}, Permissions: permissions, AllSubjects: true})
		}
	}
	result.save(t)
	return result
}
func (p testPKI) save(t *testing.T) {
	t.Helper()
	b, e := json.Marshal(p.config)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(p.path, b, 0600); e != nil {
		t.Fatal(e)
	}
}
func TestConfigurationFailsClosed(t *testing.T) {
	p := pki(t, "00000000-0000-4000-8000-000000000001")
	if _, err := loadConfig(p.path); err != nil {
		t.Fatal(err)
	}
	original := p.config
	for name, change := range map[string]func(*Config){
		"production":    func(c *Config) { c.Mode = "production" },
		"public rpc":    func(c *Config) { c.GRPCAddress = "0.0.0.0:9105" },
		"public ops":    func(c *Config) { c.HTTPAddress = "0.0.0.0:8105" },
		"empty callers": func(c *Config) { c.Callers = nil },
		"implicit subject scope": func(c *Config) {
			c.Callers = []CallerRule{{Identity: "spiffe://pesar.test/payments", Books: original.Callers[0].Books, Permissions: []string{"spend"}}}
		},
		"unknown permission": func(c *Config) {
			c.Callers = []CallerRule{{Identity: "spiffe://pesar.test/payments", Books: original.Callers[0].Books, Permissions: []string{"admin"}, AllSubjects: true}}
		},
		"bad public key": func(c *Config) { c.GrantKeys = map[string]string{"issuer": "secret"} },
		"missing tls":    func(c *Config) { c.TLS = TLSFiles{} },
	} {
		t.Run(name, func(t *testing.T) {
			p.config = original
			change(&p.config)
			p.save(t)
			if _, err := loadConfig(p.path); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
	p.config = original
	p.save(t)
	b, err := os.ReadFile(p.path)
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, []byte("\n{}")...)
	if err = os.WriteFile(p.path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadConfig(p.path); err == nil {
		t.Fatal("accepted trailing JSON")
	}
	for _, dsn := range []string{
		"postgresql://root@localhost:26277/pesaro_ledger?sslmode=verify-full",
		"postgresql://ledger_runtime@localhost:26277/pesaro_ledger?sslmode=disable",
		"postgresql://ledger_runtime@localhost:26277/pesaro_ledger?sslmode=verify-full&options=role%3Droot",
		"postgresql://ledger_runtime@localhost:26277/other?sslmode=verify-full",
	} {
		if runtimeURL(dsn) {
			t.Fatal("unsafe runtime URL accepted")
		}
	}
	if !runtimeURL("postgresql://ledger_runtime@localhost:26277/pesaro_ledger?sslmode=verify-full") {
		t.Fatal("valid runtime URL rejected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(ctx, p.path, "postgresql://root:do-not-log@localhost/db"); err == nil || strings.Contains(err.Error(), "do-not-log") {
		t.Fatal("invalid config/secret exposure")
	}
}
