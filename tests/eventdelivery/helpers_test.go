// Package eventdelivery_test is an external synthetic failure harness. It uses
// executable binaries, public wire contracts and read-only SQL observations;
// it imports neither service's private implementation. Runtime processes each
// receive only their own credentials. Cross-store reads exist only here.
package eventdelivery_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tonimnim/Pesaro/internal/platform/eventtransport"
)

const producer = "spiffe://pesar.test/ledger-events"
const consumer = "spiffe://pesar.test/reconciliation-events"

func id() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func required(t *testing.T) {
	t.Helper()
	for _, name := range []string{"PESARO_LEDGER_TEST_ADMIN_URL", "PESARO_LEDGER_TEST_RUNTIME_URL", "PESAR_RECONCILIATION_TEST_RUNTIME_URL"} {
		if os.Getenv(name) == "" {
			t.Skip("requires prepared synthetic Ledger and Reconciliation stores")
		}
	}
}
func root(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root absent")
		}
		dir = parent
	}
}
func jsonFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func address(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func cleanEnv(values map[string]string) []string {
	var result []string
	for _, entry := range os.Environ() {
		name := strings.ToUpper(strings.SplitN(entry, "=", 2)[0])
		if !strings.HasPrefix(name, "PESAR_") && !strings.HasPrefix(name, "PESARO_") {
			result = append(result, entry)
		}
	}
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}

func binaries(t *testing.T) map[string]string {
	t.Helper()
	dir := t.TempDir()
	result := map[string]string{}
	for name, path := range map[string]string{"ledger": "./services/ledger/cmd/ledger", "ledger-admin": "./services/ledger/cmd/ledger-admin", "reconciliation": "./services/reconciliation/cmd/reconciliation", "verify": "./services/reconciliation/cmd/ledger-verify"} {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		args := []string{"build"}
		if raceBuild {
			args = append(args, "-race")
		}
		result[name] = filepath.Join(dir, name+".exe")
		args = append(args, "-o", result[name], path)
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = root(t)
		output, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("build %s: %v %s", name, err, output)
		}
	}
	return result
}

func fixture(t *testing.T, binary string) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-synthetic", "-action=fixture")
	cmd.Env = cleanEnv(map[string]string{"PESAR_LEDGER_ADMIN_URL": os.Getenv("PESARO_LEDGER_TEST_ADMIN_URL")})
	data, err := cmd.Output()
	if err != nil {
		t.Fatal("fixture command", err)
	}
	var result map[string]string
	if json.Unmarshal(data, &result) != nil {
		t.Fatal("fixture encoding")
	}
	return result
}

type process struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	log  *os.File
	once sync.Once
}

func start(t *testing.T, binary string, env map[string]string) *process {
	t.Helper()
	log, err := os.CreateTemp(t.TempDir(), "process-*.log")
	if err != nil {
		t.Fatal(err)
	}
	p := &process{cmd: exec.Command(binary), done: make(chan struct{}), log: log}
	p.cmd.Env = cleanEnv(env)
	p.cmd.Stdout, p.cmd.Stderr = log, log
	if err = p.cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { p.kill(t) })
	return p
}
func (p *process) kill(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		if err := p.cmd.Process.Kill(); err != nil {
			t.Error("process exited before expected kill", err)
		}
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("killed process failed to exit")
		}
		_ = p.log.Close()
		data, _ := os.ReadFile(p.log.Name())
		if strings.Contains(string(data), "WARNING: DATA RACE") {
			t.Errorf("child process race: %s", data)
		}
	})
}
func (p *process) check(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		data, _ := os.ReadFile(p.log.Name())
		t.Fatalf("child exited: %v %s", p.err, data)
	default:
	}
}

func eventually(t *testing.T, label string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("deadline waiting for", label)
}
func ready(t *testing.T, p *process, endpoint string, client *http.Client) {
	t.Helper()
	eventually(t, "readiness", func() bool {
		p.check(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		res, err := client.Do(req)
		if err != nil {
			return false
		}
		defer res.Body.Close()
		return res.StatusCode == 200
	})
}

type pki struct {
	dir   string
	files map[string]eventtransport.TLSFiles
	roots *x509.CertPool
	keys  map[string]ed25519.PrivateKey
}

func testPKI(t *testing.T) pki {
	t.Helper()
	dir := t.TempDir()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Pesar event test CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	caBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	caPath := filepath.Join(dir, "ca.crt")
	if os.WriteFile(caPath, caBytes, 0600) != nil {
		t.Fatal("CA file")
	}
	p := pki{dir: dir, files: map[string]eventtransport.TLSFiles{}, roots: x509.NewCertPool(), keys: map[string]ed25519.PrivateKey{}}
	p.roots.AppendCertsFromPEM(caBytes)
	for index, name := range []string{"producer", "consumer", "payments", "ledger", "outsider"} {
		identity := "spiffe://pesar.test/" + name
		server := name == "consumer" || name == "ledger"
		if name == "consumer" {
			identity = consumer
		}
		if name == "producer" {
			identity = producer
		}
		uri, _ := url.Parse(identity)
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		p.keys[name] = private
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(index + 2)), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{uri}}
		if server {
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			leaf.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			leaf.DNSNames = []string{"localhost"}
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, public, key)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		files := eventtransport.TLSFiles{Certificate: filepath.Join(dir, name+".crt"), Key: filepath.Join(dir, name+".key"), CA: caPath}
		if os.WriteFile(files.Certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600) != nil || os.WriteFile(files.Key, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600) != nil {
			t.Fatal("certificate files")
		}
		p.files[name] = files
	}
	return p
}
func (p pki) security(t *testing.T, name string) *tls.Config {
	t.Helper()
	pair, err := tls.LoadX509KeyPair(p.files[name].Certificate, p.files[name].Key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: p.roots, Certificates: []tls.Certificate{pair}}
}

func observer(t *testing.T, name string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, os.Getenv(name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}
