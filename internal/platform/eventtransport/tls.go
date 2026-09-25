// Package eventtransport supplies transport security for the synthetic M1
// point-to-point event path. It contains no financial state or money rules.
package eventtransport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var ErrConfig = errors.New("invalid synthetic event transport configuration")

type TLSFiles struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	CA          string `json:"ca"`
}

func ValidIdentity(identity string) bool {
	u, err := url.Parse(identity)
	return err == nil && u.Scheme == "spiffe" && u.Host != "" && u.User == nil && u.Path != "" && u.RawQuery == "" && u.Fragment == "" && !strings.Contains(identity, "%")
}

func Peer(state *tls.ConnectionState, identity string) bool {
	return state != nil && len(state.VerifiedChains) > 0 && len(state.PeerCertificates) > 0 && len(state.PeerCertificates[0].URIs) == 1 && state.PeerCertificates[0].URIs[0].String() == identity
}

func LoadTLS(configPath string, files TLSFiles, server bool, peer string) (*tls.Config, error) {
	resolve := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(filepath.Dir(configPath), path)
	}
	if files.Certificate == "" || files.Key == "" || files.CA == "" || !ValidIdentity(peer) {
		return nil, ErrConfig
	}
	cert, err := tls.LoadX509KeyPair(resolve(files.Certificate), resolve(files.Key))
	if err != nil {
		return nil, ErrConfig
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil || time.Now().Before(leaf.NotBefore) || !time.Now().Before(leaf.NotAfter) {
		return nil, ErrConfig
	}
	data, err := os.ReadFile(resolve(files.CA))
	if err != nil {
		return nil, ErrConfig
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, ErrConfig
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: roots}
	if server {
		config.ClientCAs, config.ClientAuth = roots, tls.RequireAndVerifyClientCert
	}
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if !Peer(&state, peer) {
			return ErrConfig
		}
		return nil
	}
	return config, nil
}

func LoopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	n, parseErr := strconv.Atoi(port)
	return err == nil && parseErr == nil && n > 0 && n <= 65535 && ip != nil && ip.IsLoopback()
}

func Endpoint(raw, path string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.Path == path && u.RawPath == "" && LoopbackAddress(u.Host)
}
