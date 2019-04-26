package http

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// NewDefaultTransport creates a new transport with sane defaults.
func NewDefaultTransport() *http.Transport {
	// These defaults are copied from http.DefaultTransport.
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// Below are changes from http.DefaultTransport
		MaxIdleConnsPerHost: 100, // increased from 2, services tend to connect to a single host
	}
}

// NewDefaultTransportWithTLS creates a new transport with the specified TLS configuration.
func NewDefaultTransportWithTLS(tlsConfig *tls.Config) *http.Transport {
	t := NewDefaultTransport()
	t.TLSClientConfig = tlsConfig
	return t
}
