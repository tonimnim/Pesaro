package eventtransport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"
)

var ErrDelivery = errors.New("event delivery not acknowledged")

type Client struct {
	endpoint  string
	http      *http.Client
	transport *http.Transport
}

func NewClient(endpoint string, security *tls.Config) *Client {
	transport := &http.Transport{TLSClientConfig: security.Clone(), Proxy: nil,
		DialContext:         (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
		TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 3 * time.Second,
		IdleConnTimeout: 30 * time.Second, MaxIdleConns: 4, MaxConnsPerHost: 4}
	return &Client{endpoint: endpoint, transport: transport, http: &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (c *Client) Close() { c.transport.CloseIdleConnections() }

func (c *Client) Send(ctx context.Context, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(data))
	if err != nil {
		return ErrDelivery
	}
	req.Header.Set("Content-Type", "application/json")
	// No Idempotency-Key header: net/http must not autonomously replay a POST.
	// The durable publisher owns retry and retains the event's wire identity.
	response, err := c.http.Do(req)
	if err != nil {
		return ErrDelivery
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return ErrDelivery
	}
	return nil
}
