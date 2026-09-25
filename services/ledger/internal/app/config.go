package app

import (
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	ledgerevent "github.com/tonimnim/Pesaro/contracts/events/ledger/v1"
	"github.com/tonimnim/Pesaro/internal/platform/eventtransport"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
	"github.com/tonimnim/Pesaro/services/ledger/internal/publisher"
)

// Config contains paths and public authorization policy, never issuer private keys.
// The database URL is supplied separately through the runtime environment.
type Config struct {
	Mode         string            `json:"mode"`
	GRPCAddress  string            `json:"grpc_address"`
	HTTPAddress  string            `json:"http_address"`
	TLS          TLSFiles          `json:"tls"`
	Callers      []CallerRule      `json:"callers"`
	GrantKeys    map[string]string `json:"grant_keys"`
	EvidenceKeys map[string]string `json:"evidence_keys"`
	Outbox       *OutboxConfig     `json:"outbox,omitempty"`
}
type OutboxConfig struct {
	Endpoint         string                  `json:"endpoint"`
	ConsumerIdentity string                  `json:"consumer_identity"`
	TLS              eventtransport.TLSFiles `json:"tls"`
	Options          publisher.Options       `json:"worker"`
}
type TLSFiles struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	ClientCA    string `json:"client_ca"`
}
type CallerRule struct {
	Identity    string   `json:"identity"`
	Books       []string `json:"books"`
	Permissions []string `json:"permissions"`
	Subjects    []string `json:"subjects"`
	AllSubjects bool     `json:"all_subjects"`
}
type preparedConfig struct {
	Config
	tls       *tls.Config
	rules     map[string]application.Caller
	trust     application.Trust
	books     []domain.ID
	outboxTLS *tls.Config
}

var errConfig = errors.New("invalid Ledger configuration; synthetic mode, loopback listeners, TLS and explicit scoped policy required")

func loadConfig(path string) (preparedConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return preparedConfig{}, errConfig
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return preparedConfig{}, errConfig
	}
	var c Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if dec.Decode(&c) != nil {
		return preparedConfig{}, errConfig
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return preparedConfig{}, errConfig
	}
	if c.Mode != "synthetic" {
		return preparedConfig{}, errConfig
	}
	if c.GRPCAddress == "" {
		c.GRPCAddress = "127.0.0.1:9105"
	}
	if c.HTTPAddress == "" {
		c.HTTPAddress = "127.0.0.1:8105"
	}
	if !loopbackAddress(c.GRPCAddress) || !loopbackAddress(c.HTTPAddress) || c.GRPCAddress == c.HTTPAddress {
		return preparedConfig{}, errConfig
	}
	p := preparedConfig{Config: c, rules: map[string]application.Caller{}}
	bookSet := map[domain.ID]bool{}
	allowed := map[string]bool{"spend": true, "resolve": true, "control": true, "provision": true, "read": true, "verify": true}
	if len(c.Callers) == 0 || len(c.Callers) > 128 {
		return preparedConfig{}, errConfig
	}
	for _, rule := range c.Callers {
		identity, err := url.Parse(rule.Identity)
		if err != nil || identity.Scheme != "spiffe" || identity.Host == "" || identity.User != nil || identity.RawQuery != "" || identity.Fragment != "" || identity.Path == "" || strings.Contains(rule.Identity, "%") {
			return preparedConfig{}, errConfig
		}
		if _, duplicate := p.rules[rule.Identity]; duplicate {
			return preparedConfig{}, errConfig
		}
		if len(rule.Books) == 0 || len(rule.Permissions) == 0 || rule.AllSubjects == (len(rule.Subjects) > 0) {
			return preparedConfig{}, errConfig
		}
		caller := application.Caller{Identity: rule.Identity, Books: map[domain.ID]bool{}, Permissions: map[string]bool{}, Subjects: map[domain.ID]bool{}}
		for _, value := range rule.Books {
			id, err := domain.ParseID(value)
			if err != nil || caller.Books[id] {
				return preparedConfig{}, errConfig
			}
			caller.Books[id] = true
			bookSet[id] = true
		}
		for _, permission := range rule.Permissions {
			if !allowed[permission] || caller.Permissions[permission] {
				return preparedConfig{}, errConfig
			}
			caller.Permissions[permission] = true
		}
		if caller.Permissions["verify"] && !rule.AllSubjects {
			return preparedConfig{}, errConfig
		}
		for _, value := range rule.Subjects {
			id, err := domain.ParseID(value)
			if err != nil || caller.Subjects[id] {
				return preparedConfig{}, errConfig
			}
			caller.Subjects[id] = true
		}
		p.rules[rule.Identity] = caller
	}
	for book := range bookSet {
		p.books = append(p.books, book)
	}
	keys := func(encoded map[string]string) (map[string]ed25519.PublicKey, error) {
		if len(encoded) == 0 || len(encoded) > 32 {
			return nil, errConfig
		}
		result := map[string]ed25519.PublicKey{}
		for issuer, value := range encoded {
			key, err := base64.RawURLEncoding.DecodeString(value)
			if err != nil || len(key) != ed25519.PublicKeySize || len(issuer) == 0 || len(issuer) > 64 {
				return nil, errConfig
			}
			result[issuer] = ed25519.PublicKey(key)
		}
		return result, nil
	}
	if p.trust.GrantKeys, err = keys(c.GrantKeys); err != nil {
		return preparedConfig{}, err
	}
	if p.trust.EvidenceKeys, err = keys(c.EvidenceKeys); err != nil {
		return preparedConfig{}, err
	}
	resolve := func(name string) string {
		if filepath.IsAbs(name) {
			return name
		}
		return filepath.Join(filepath.Dir(path), name)
	}
	cert, err := tls.LoadX509KeyPair(resolve(c.TLS.Certificate), resolve(c.TLS.Key))
	if err != nil {
		return preparedConfig{}, errConfig
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	now := time.Now()
	if err != nil || now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return preparedConfig{}, errConfig
	}
	// A runtime restart is required for certificate/policy rotation in M1.
	ca, err := os.ReadFile(resolve(c.TLS.ClientCA))
	if err != nil {
		return preparedConfig{}, errConfig
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return preparedConfig{}, errConfig
	}
	p.tls = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	if c.Outbox != nil {
		if !eventtransport.Endpoint(c.Outbox.Endpoint, ledgerevent.Path) {
			return preparedConfig{}, errConfig
		}
		if p.Outbox.Options, err = c.Outbox.Options.Defaults(); err != nil {
			return preparedConfig{}, errConfig
		}
		if p.outboxTLS, err = eventtransport.LoadTLS(path, c.Outbox.TLS, false, c.Outbox.ConsumerIdentity); err != nil {
			return preparedConfig{}, errConfig
		}
	}
	return p, nil
}
func loopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	n, parseErr := strconv.Atoi(port)
	return err == nil && parseErr == nil && n > 0 && n <= 65535 && ip != nil && ip.IsLoopback()
}
func runtimeURL(dsn string) bool {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme != "postgresql" || u.User == nil || u.User.Username() != "ledger_runtime" || u.Path != "/pesaro_ledger" || u.Host == "" || u.Fragment != "" {
		return false
	}
	if u.Query().Get("sslmode") != "verify-full" {
		return false
	}
	allowed := map[string]bool{"sslmode": true, "sslrootcert": true, "sslcert": true, "sslkey": true}
	for key, values := range u.Query() {
		if !allowed[key] || len(values) != 1 {
			return false
		}
	}
	return true
}
